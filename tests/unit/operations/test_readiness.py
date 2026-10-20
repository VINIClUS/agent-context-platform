"""The `/health/ready` probe with fakes: states, timeouts, single flight and no leaks."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from sqlalchemy.exc import OperationalError, ProgrammingError

from agent_context_platform.operations import readiness
from agent_context_platform.operations.readiness import (
    Components,
    ReadinessProbe,
    ReadinessReport,
)
from agent_context_platform.projection.neo4j import Neo4jHealth
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit

HEAD = "20260929_0001"
SECRET_HOST = "db.secret.example.test"


class FakeOrig(Exception):
    def __init__(self, sqlstate: str | None) -> None:
        super().__init__(f"connection to {SECRET_HOST} failed")
        self.sqlstate = sqlstate


class FakeResult:
    def __init__(self, rows: list[str]) -> None:
        self._rows = rows

    def scalars(self) -> list[str]:
        return self._rows


class FakeConnection:
    def __init__(self, engine: FakeEngine) -> None:
        self._engine = engine

    async def __aenter__(self) -> FakeConnection:
        if self._engine.connect_error is not None:
            raise self._engine.connect_error
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, statement: Any) -> FakeResult:
        self._engine.statements.append(str(statement))
        if "alembic_version" in str(statement):
            if self._engine.query_error is not None:
                raise self._engine.query_error
            if self._engine.delay:
                await asyncio.sleep(self._engine.delay)
        return FakeResult(self._engine.rows)

    async def rollback(self) -> None:
        self._engine.rollbacks += 1


class FakeEngine:
    def __init__(
        self,
        rows: list[str] | None = None,
        *,
        connect_error: Exception | None = None,
        query_error: Exception | None = None,
        delay: float = 0.0,
    ) -> None:
        self.rows = [HEAD] if rows is None else rows
        self.connect_error = connect_error
        self.query_error = query_error
        self.delay = delay
        self.statements: list[str] = []
        self.rollbacks = 0

    def connect(self) -> FakeConnection:
        return FakeConnection(self)


def _sql_error(kind: type[Exception], sqlstate: str | None) -> Exception:
    return kind("SELECT version_num FROM public.alembic_version", {}, FakeOrig(sqlstate))


async def _postgresql(engine: FakeEngine, head: str | None = HEAD) -> Components:
    check, _ = readiness.postgresql_check(engine, head)  # type: ignore[arg-type]
    return await check()


@pytest.mark.parametrize(
    ("engine", "head", "expected"),
    [
        (FakeEngine(), HEAD, {"postgresql": "ok", "schema": "ok"}),
        (FakeEngine(["20260823_0001"]), HEAD, {"postgresql": "ok", "schema": "mismatch"}),
        (FakeEngine([]), HEAD, {"postgresql": "ok", "schema": "mismatch"}),
        (FakeEngine([HEAD, "other"]), HEAD, {"postgresql": "ok", "schema": "mismatch"}),
        (FakeEngine(), None, {"postgresql": "ok", "schema": "unconfigured"}),
        (
            FakeEngine(query_error=_sql_error(ProgrammingError, "42501")),
            HEAD,
            {"postgresql": "ok", "schema": "forbidden"},
        ),
        (
            FakeEngine(query_error=_sql_error(ProgrammingError, "42P01")),
            HEAD,
            {"postgresql": "ok", "schema": "mismatch"},
        ),
        (
            FakeEngine(query_error=_sql_error(OperationalError, "57P01")),
            HEAD,
            {"postgresql": "unavailable", "schema": "unknown"},
        ),
        (
            FakeEngine(connect_error=_sql_error(OperationalError, None)),
            HEAD,
            {"postgresql": "unavailable", "schema": "unknown"},
        ),
        (
            FakeEngine(connect_error=ConnectionRefusedError(SECRET_HOST)),
            HEAD,
            {"postgresql": "unavailable", "schema": "unknown"},
        ),
    ],
)
def test_postgresql_states(engine: FakeEngine, head: str | None, expected: Components) -> None:
    assert asyncio.run(_postgresql(engine, head)) == expected


def test_postgresql_check_only_reads_in_a_read_only_transaction() -> None:
    engine = FakeEngine()

    asyncio.run(_postgresql(engine))

    assert engine.statements == [
        "SET TRANSACTION READ ONLY",
        "SELECT version_num FROM public.alembic_version",
    ]
    assert engine.rollbacks == 1


class FakeStore:
    def __init__(self, health: Neo4jHealth, delay: float = 0.0) -> None:
        self._health = health
        self._delay = delay

    async def verify_connectivity(self) -> Neo4jHealth:
        await asyncio.sleep(self._delay)
        return self._health


@pytest.mark.parametrize(
    ("health", "expected"),
    [
        (Neo4jHealth("ok"), "ok"),
        (Neo4jHealth("error", "authentication"), "forbidden"),
        (Neo4jHealth("error", "configuration"), "unconfigured"),
        (Neo4jHealth("error", "unavailable"), "unavailable"),
    ],
)
def test_neo4j_states(health: Neo4jHealth, expected: str) -> None:
    check, _ = readiness.neo4j_check(FakeStore(health))  # type: ignore[arg-type]

    assert asyncio.run(check()) == {"neo4j": expected}


class FakeS3:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def head_bucket(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {}


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": SECRET_HOST}}, "HeadBucket")


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (None, "ok"),
        (_client_error("403"), "forbidden"),
        (_client_error("AccessDenied"), "forbidden"),
        (_client_error("404"), "unavailable"),
        (EndpointConnectionError(endpoint_url=f"http://{SECRET_HOST}"), "unavailable"),
    ],
)
def test_s3_states(error: Exception | None, expected: str) -> None:
    client = FakeS3(error)
    check, _ = readiness.s3_check(client, "agent-context-content")

    assert asyncio.run(check()) == {"s3": expected}
    assert client.calls == [{"Bucket": "agent-context-content"}]


def _static(components: Components, delay: float = 0.0) -> readiness.Check:
    async def check() -> Components:
        await asyncio.sleep(delay)
        return dict(components)

    return check


def test_the_report_is_ready_only_when_every_component_is_ok() -> None:
    assert ReadinessReport({"a": "ok", "b": "ok"}).to_dict() == {"status": "ready"}
    assert ReadinessReport({"b": "timeout", "a": "ok"}).to_dict() == {
        "status": "not_ready",
        "components": {"a": "ok", "b": "timeout"},
    }
    assert not ReadinessReport({}).ready


def test_checks_run_concurrently_and_a_slow_one_times_out() -> None:
    probe = ReadinessProbe(
        {
            "fast": (_static({"fast": "ok"}, 0.05), {"fast": "timeout"}),
            "slow": (_static({"slow": "ok"}, 5.0), {"slow": "timeout", "child": "unknown"}),
            "other": (_static({"other": "ok"}, 0.05), {"other": "timeout"}),
        },
        timeout_seconds=0.3,
    )

    async def timed() -> tuple[ReadinessReport, float]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        report = await probe.probe()
        return report, loop.time() - started

    report, elapsed = asyncio.run(timed())

    assert dict(report.components) == {
        "fast": "ok",
        "other": "ok",
        "slow": "timeout",
        "child": "unknown",
    }
    assert elapsed < 1.0


def test_an_unexpected_failure_is_unavailable_without_its_text() -> None:
    async def explode() -> Components:
        raise RuntimeError(f"postgresql://user:hunter2@{SECRET_HOST}/db")

    probe = ReadinessProbe({"pg": (explode, {"postgresql": "timeout", "schema": "unknown"})})

    report = asyncio.run(probe.probe())

    assert report.to_dict() == {
        "status": "not_ready",
        "components": {"postgresql": "unavailable", "schema": "unknown"},
    }


def test_concurrent_requests_share_one_in_flight_probe() -> None:
    calls = 0

    async def counted() -> Components:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return {"x": "ok"}

    probe = ReadinessProbe({"x": (counted, {"x": "timeout"})})

    async def burst() -> list[ReadinessReport]:
        first = await asyncio.gather(*(probe.probe() for _ in range(20)))
        second = await probe.probe()
        return [*first, second]

    reports = asyncio.run(burst())

    assert calls == 2
    assert all(report.ready for report in reports)


def test_a_cancelled_waiter_does_not_cancel_the_shared_probe() -> None:
    async def slow() -> Components:
        await asyncio.sleep(0.1)
        return {"x": "ok"}

    probe = ReadinessProbe({"x": (slow, {"x": "timeout"})})

    async def scenario() -> ReadinessReport:
        impatient = asyncio.create_task(probe.probe())
        patient = asyncio.create_task(probe.probe())
        await asyncio.sleep(0.01)
        impatient.cancel()
        return await patient

    assert asyncio.run(scenario()).ready


def test_build_probe_reports_missing_dependencies_as_unconfigured() -> None:
    probe = readiness.build_probe(Settings(environment="test"), None, None, HEAD)

    report = asyncio.run(probe.probe())

    assert report.to_dict() == {
        "status": "not_ready",
        "components": {
            "neo4j": "unconfigured",
            "postgresql": "unconfigured",
            "s3": "unconfigured",
            "schema": "unconfigured",
        },
    }


def test_build_probe_wires_every_configured_check(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        environment="test",
        s3={
            "endpoint_url": "http://garage.example.test:3900",
            "access_key_id": "GKexample",
            "secret_access_key": "example-secret",
            "bucket_name": "bucket",
        },
    )
    client = FakeS3()
    captured: dict[str, Any] = {}

    def fake_client(s3: Any, timeout: float) -> FakeS3:
        captured["timeout"] = timeout
        return client

    monkeypatch.setattr(readiness, "s3_client", fake_client)
    probe = readiness.build_probe(
        settings,
        FakeEngine(),  # type: ignore[arg-type]
        FakeStore(Neo4jHealth("ok")),  # type: ignore[arg-type]
        HEAD,
        timeout_seconds=1.5,
    )

    assert asyncio.run(probe.probe()).to_dict() == {"status": "ready"}
    assert captured == {"timeout": 1.5}
    assert client.calls == [{"Bucket": "bucket"}]


def test_the_s3_client_never_retries_and_honours_the_probe_timeout() -> None:
    settings = Settings(
        environment="test",
        s3={"endpoint_url": "http://garage.example.test:3900"},
    ).s3

    client = readiness.s3_client(settings, 1.5)

    config = client.meta.config
    assert (config.connect_timeout, config.read_timeout) == (1.5, 1.5)
    assert config.retries["total_max_attempts"] == 1
    assert client.meta.endpoint_url.rstrip("/") == "http://garage.example.test:3900"
