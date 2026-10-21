"""Gauge sampler, index runs and graph retrievals: bounded results, durations and spans."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from opentelemetry.trace import StatusCode

from agent_context_platform.indexing import pipeline
from agent_context_platform.indexing.emitter import IndexingError
from agent_context_platform.indexing.pipeline import IndexReport, IndexRequest, index_checkout
from agent_context_platform.indexing.scanner import ScanError, ScanFailure
from agent_context_platform.observability import contract
from agent_context_platform.observability.projection import ProjectionSampler
from agent_context_platform.retrieval.graph import GraphScope, GraphTraversalService
from agent_context_platform.settings import Settings

from ._capture import Canaries, Capture

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 1, tzinfo=UTC)


# --- projection gauge sampler --------------------------------------------------------------------


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def one(self) -> tuple[Any, ...]:
        return self._rows[0]

    def all(self) -> list[tuple[Any, ...]]:
        return self._rows


class _Session:
    def __init__(self, results: list[list[tuple[Any, ...]]]) -> None:
        self._results = results
        self.settings: list[dict[str, Any]] = []

    async def execute(self, statement: Any, parameters: Any = None) -> _Result:
        if "set_config" in str(statement):
            self.settings.append(dict(parameters))
            return _Result([])
        return _Result(self._results.pop(0))

    async def rollback(self) -> None:
        return None


def _factory(*results: list[tuple[Any, ...]], error: Exception | None = None) -> Any:
    @asynccontextmanager
    async def open_session() -> AsyncIterator[_Session]:
        if error is not None:
            raise error
        session = _Session(list(results))
        open_session.sessions.append(session)  # type: ignore[attr-defined]
        yield session

    open_session.sessions = []  # type: ignore[attr-defined]
    return open_session


def test_the_sampler_reports_backlog_lag_and_dead_letters(telemetry: Capture) -> None:
    sampler = ProjectionSampler(
        _factory(
            [(3, NOW - timedelta(seconds=42))],
            [("agent", 2), ("<projection.runtime>", 1), ("retired-projector", 5)],
        ),
        ["agent", "code", "agent"],
        clock=lambda: NOW,
    )

    assert asyncio.run(sampler.refresh()) is True

    assert telemetry.value(contract.PROJECTION_OUTBOX_PENDING) == 3
    assert telemetry.value(contract.PROJECTION_LAG, projector="agent") == 42
    assert telemetry.value(contract.PROJECTION_LAG, projector="code") == 42
    assert telemetry.points(contract.PROJECTION_DEAD_LETTERS) == {
        (("projector", "agent"),): 2,
        (("projector", "unattributed"),): 1,
        (("projector", "other"),): 5,
    }


def test_the_sampler_bounds_its_queries_with_a_statement_timeout(telemetry: Capture) -> None:
    factory = _factory([(0, None)], [])
    sampler = ProjectionSampler(factory, ["agent"], statement_timeout_seconds=1.5)

    assert asyncio.run(sampler.refresh()) is True

    (session,) = factory.sessions
    assert session.settings == [{"timeout": "1500ms"}]


def test_an_empty_backlog_has_no_lag(telemetry: Capture) -> None:
    sampler = ProjectionSampler(_factory([(0, None)], []), ["agent"], clock=lambda: NOW)
    assert asyncio.run(sampler.refresh()) is True
    assert telemetry.value(contract.PROJECTION_LAG, projector="agent") == 0
    assert telemetry.points(contract.PROJECTION_DEAD_LETTERS) == {}


def test_a_failed_refresh_stops_the_gauges_instead_of_reporting_stale_values(
    telemetry: Capture,
) -> None:
    good = ProjectionSampler(_factory([(1, NOW)], []), ["agent"], clock=lambda: NOW)
    assert asyncio.run(good.refresh()) is True
    assert telemetry.value(contract.PROJECTION_OUTBOX_PENDING) == 1

    broken = ProjectionSampler(_factory(error=ConnectionError("db down")), ["agent"])
    assert asyncio.run(broken.refresh()) is False
    assert telemetry.points(contract.PROJECTION_OUTBOX_PENDING) == {}


def test_the_sampler_loop_stops_and_clears_its_gauges(telemetry: Capture) -> None:
    sampler = ProjectionSampler(
        _factory([(1, NOW)], [], [(1, NOW)], []), ["agent"], clock=lambda: NOW
    )

    async def run() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(sampler.run(stop, 0.01))
        await asyncio.sleep(0.03)
        assert telemetry.value(contract.PROJECTION_OUTBOX_PENDING) == 1
        stop.set()
        await task

    asyncio.run(run())
    assert telemetry.points(contract.PROJECTION_OUTBOX_PENDING) == {}


# --- index runs ------------------------------------------------------------------------------------


class _IndexService:
    def __init__(self, repository_id: str = "repo-1") -> None:
        self.config = SimpleNamespace(repository_id=repository_id)

    async def ingest(self, drafts: Any) -> Any:
        return SimpleNamespace(submitted=len(drafts), existing=0)


def _draft(event_type: str, payload: dict[str, Any]) -> Any:
    return SimpleNamespace(event_type=event_type, payload=payload)


@pytest.mark.parametrize(
    ("build", "service", "result", "error_class"),
    [
        (
            lambda *_: (
                [_draft("code.index.completed", {"success": True, "index_id": "i1"})],
                Counter(),
            ),
            _IndexService(),
            "success",
            None,
        ),
        (None, _IndexService("another-repo"), "refused", "repository_mismatch"),
        ("scan", _IndexService(), "refused", "scan_failed"),
        ("indexing", _IndexService(), "failed", "shallow_history"),
        (
            lambda *_: ([_draft("code.index.completed", {"success": False})], Counter()),
            _IndexService(),
            "failed",
            None,
        ),
    ],
)
def test_index_runs_are_counted_by_bounded_result(
    telemetry: Capture,
    canaries: Canaries,
    monkeypatch: pytest.MonkeyPatch,
    build: Any,
    service: Any,
    result: str,
    error_class: str | None,
) -> None:
    def fail_scan(*_args: Any) -> Any:
        raise ScanError(next(iter(ScanFailure)))

    def fail_indexing(*_args: Any) -> Any:
        raise IndexingError("shallow_history")

    replacement = {"scan": fail_scan, "indexing": fail_indexing}.get(build, build)
    if replacement is not None:
        monkeypatch.setattr(pipeline, "_build", replacement)
    request = IndexRequest("repo-1", Path(canaries.path))

    report = asyncio.run(index_checkout(request, Settings(environment="test"), service))

    assert isinstance(report, IndexReport)
    assert report.error_class == error_class
    assert telemetry.points(contract.INDEX_RUNS) == {(("result", result),): 1}
    assert telemetry.value(contract.INDEX_DURATION, result=result) == 1
    span = telemetry.span("agent_context.index.run")
    assert (span.attributes or {})["agent_context.result"] == result
    expected = StatusCode.UNSET if result == "success" else StatusCode.ERROR
    assert span.status.status_code is expected
    canaries.assert_absent(telemetry.dump())


def test_a_usage_error_still_counts_a_failed_run(
    telemetry: Capture, monkeypatch: pytest.MonkeyPatch
) -> None:
    def missing(*_args: Any) -> Any:
        raise FileNotFoundError("no checkout")

    monkeypatch.setattr(pipeline, "_build", missing)
    with pytest.raises(FileNotFoundError):
        asyncio.run(
            index_checkout(
                IndexRequest("repo-1", Path("/nowhere")),
                Settings(environment="test"),
                _IndexService(),  # type: ignore[arg-type]
            )
        )
    assert telemetry.points(contract.INDEX_RUNS) == {(("result", "failed"),): 1}


def test_run_result_mapping() -> None:
    def report(success: bool, error: str | None) -> IndexReport:
        return IndexReport("r", success, error)

    assert pipeline.run_result(report(True, None)) == "success"
    assert pipeline.run_result(report(False, "structural_landlock_unavailable")) == "refused"
    assert pipeline.run_result(report(False, "repository_mismatch")) == "refused"
    assert pipeline.run_result(report(False, "scip_unreadable")) == "failed"
    assert pipeline.run_result(report(False, None)) == "failed"


# --- graph retrieval -------------------------------------------------------------------------------


class _Driver:
    """A driver whose read transaction answers ``result`` or raises ``error``."""

    def __init__(self, *, result: Any = None, error: BaseException | None = None) -> None:
        self._result = result
        self._error = error

    def session(self, **_kwargs: Any) -> Any:
        driver = self

        class _Session:
            async def __aenter__(self) -> Any:
                return self

            async def __aexit__(self, *_args: Any) -> None:
                return None

            async def execute_read(self, _work: Any) -> Any:
                if driver._error is not None:
                    raise driver._error
                return driver._result

        return _Session()


_SCOPE = GraphScope(repository_id="repo-1")


@pytest.mark.parametrize(
    ("driver", "call", "outcome"),
    [
        (_Driver(result="neighborhood"), lambda s: s.neighborhood(_SCOPE, "a"), "ok"),
        (_Driver(), lambda s: s.neighborhood(_SCOPE, "a", depth=9), "client_error"),
        (_Driver(), lambda s: s.callers(_SCOPE, "a", valid_at=NOW), "client_error"),
        (_Driver(error=TimeoutError()), lambda s: s.callees(_SCOPE, "a"), "deadline"),
        (
            _Driver(error=RuntimeError("driver exploded")),
            lambda s: s.dependencies(_SCOPE, "a"),
            "server_error",
        ),
        (
            _Driver(result="paths"),
            lambda s: s.dependency_paths(_SCOPE, "a", "b", max_paths=1),
            "ok",
        ),
    ],
)
def test_graph_traversals_record_operation_graph(
    telemetry: Capture, driver: _Driver, call: Any, outcome: str
) -> None:
    service = GraphTraversalService(driver, database="neo4j")  # type: ignore[arg-type]

    async def run() -> None:
        if outcome == "ok":
            await call(service)
        else:
            with pytest.raises(Exception):  # noqa: B017 - each failure has its own typed class
                await call(service)

    asyncio.run(run())

    assert telemetry.points(contract.RETRIEVAL_DURATION) == {
        (("operation", "graph"), ("outcome", outcome)): 1
    }
    span = telemetry.span("agent_context.retrieval.graph")
    assert (span.attributes or {})["agent_context.outcome"] == outcome
    failed = outcome in {"server_error", "deadline"}
    assert (span.status.status_code is StatusCode.ERROR) is failed


def test_a_retrieval_can_report_a_returned_failure(telemetry: Capture) -> None:
    from agent_context_platform.observability import retrieval_span

    with retrieval_span("context_for_task") as observation:
        observation.set_outcome("client_error")
    with retrieval_span("search") as observation:
        observation.set_outcome("made-up")

    assert telemetry.points(contract.RETRIEVAL_DURATION) == {
        (("operation", "context_for_task"), ("outcome", "client_error")): 1,
        (("operation", "search"), ("outcome", "other")): 1,
    }
