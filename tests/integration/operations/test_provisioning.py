"""`agent-context producer|mcp-token` against the real schema, roles and both auth planes.

The CLI runs as the schema owner (its `postgresql.admin_dsn`); the credentials it issues are then
presented to the real app, whose authenticators read as the least-privileged API role.
"""

from __future__ import annotations

import asyncio
import json
import logging
import stat
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx2
import pytest
from agent_context_sdk import (
    AcceptedEventV1,
    ContentDisposition,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    IngestBatchResponseV1,
    ProducerV1,
)
from agent_context_sdk.ids import new_uuid7
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.app import create_app
from agent_context_platform.db import session_factory
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import ProducerAuthenticator, SqlProducerLookup
from agent_context_platform.ledger.service import IngestOutcome
from agent_context_platform.mcp.auth import (
    McpAuthenticator,
    McpAuthRuntime,
    PrincipalCache,
    SqlMcpTokenLookup,
    TokenBucketLimiter,
)
from agent_context_platform.operations import provisioning
from agent_context_platform.security.bearer import Argon2Verifier
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.integration

COST = {"time_cost": 1, "memory_cost_kib": 8, "parallelism": 1}
OBSERVED = datetime(2026, 1, 1, tzinfo=UTC)
MCP_HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
    "mcp-protocol-version": "2026-07-28",
    "mcp-method": "server/discover",
}
MCP_BODY = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "server/discover",
    "params": {
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
        }
    },
}


class StubService:
    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        accepted = tuple(
            AcceptedEventV1(event_id=e.event_id, status="accepted", stream_sequence=i + 1)
            for i, e in enumerate(batch.events)
        )
        return IngestOutcome(200, IngestBatchResponseV1(batch_id=batch.batch_id, accepted=accepted))


def _verifier() -> Argon2Verifier:
    return Argon2Verifier(**COST, max_concurrency=2, max_queue_depth=8)


def _app(api: AsyncEngine) -> Any:
    """A fresh app (and so a fresh MCP principal cache) reading as the API role."""
    sessions = session_factory(api)
    clock = lambda: datetime.now(UTC)  # noqa: E731
    mcp = McpAuthRuntime(
        McpAuthenticator(
            SqlMcpTokenLookup(sessions),
            _verifier(),
            PrincipalCache(b"k" * 32, ttl=timedelta(seconds=30), max_entries=8, clock=clock),
        ),
        TokenBucketLimiter(rate_per_second=100, burst=100),
    )
    ingestion = IngestionRuntime(
        ProducerAuthenticator(SqlProducerLookup(sessions), _verifier()),
        StubService(),  # type: ignore[arg-type]
        1_000_000,
    )
    return create_app(Settings(environment="test"), ingestion=ingestion, mcp_auth=mcp)


async def _post_mcp(app: Any, token: str) -> int:
    async with (
        app.router.lifespan_context(app),
        httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
        ) as client,
    ):
        response = await client.post(
            "/mcp", headers={**MCP_HEADERS, "authorization": f"Bearer {token}"}, json=MCP_BODY
        )
    return response.status_code


async def _post_ingest(app: Any, token: str, producer_id: str) -> int:
    event = EventDraftV1(
        event_type="agent.session.started",
        stream_id="s-provisioned",
        occurred_at=OBSERVED,
        observed_at=OBSERVED,
        producer=ProducerV1(producer_id=producer_id, name="Codex", version="1.0.0"),
        payload={"source": "codex", "session_id": "session-1"},
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
        ),
        idempotency_key=f"key-{uuid.uuid4().hex}",
    )
    batch = IngestBatchRequestV1(batch_id=new_uuid7(), events=(event,))
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        response = await client.post(
            "/v1/ingestion/batches",
            content=batch.model_dump_json(),
            headers={
                "authorization": f"Bearer {token}",
                "idempotency-key": str(batch.batch_id),
                "content-type": "application/json",
            },
        )
    return response.status_code


def _env(dsn: str, *, admin: bool = False) -> dict[str, str]:
    key = "ADMIN_DSN" if admin else "DSN"
    return {
        f"AGENT_CONTEXT_POSTGRESQL__{key}": dsn,
        "AGENT_CONTEXT_INGESTION__ARGON2_TIME_COST": "1",
        "AGENT_CONTEXT_INGESTION__ARGON2_MEMORY_COST_KIB": "8",
        "AGENT_CONTEXT_INGESTION__ARGON2_PARALLELISM": "1",
    }


@pytest.fixture
def operator(owner_engine: AsyncEngine, postgres_dsn: str) -> dict[str, str]:
    return _env(postgres_dsn)


def run(env: dict[str, str], *args: str) -> Any:
    return CliRunner().invoke(cli.app, list(args), env=env)


def register(env: dict[str, str], producer_id: str, *extra: str) -> Any:
    return run(
        env, "producer", "register", "--producer-id", producer_id, "--expires-in", "30",
        "--json", *extra,
    )  # fmt: skip


def create(env: dict[str, str], principal: str, *extra: str) -> Any:
    return run(
        env, "mcp-token", "create", "--principal", principal, "--scope", "memory:read",
        "--expires-in", "30", "--json", *extra,
    )  # fmt: skip


def test_a_registered_producer_authenticates_and_is_bound_to_its_id(
    operator: dict[str, str], api_engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    result = register(operator, "p039b-register")
    assert result.exit_code == 0, result.output
    issued = json.loads(result.stdout)
    token = issued["token"]
    assert issued["scope"] == "events:ingest" and issued["id"] == "p039b-register"
    assert issued["prefix"].startswith("prd_") and token.startswith(issued["prefix"] + ".")
    assert token not in caplog.text and token not in result.stderr

    async def exercise() -> None:
        assert await _post_ingest(_app(api_engine), token, "p039b-register") == 200
        # Bound to exactly one producer id, and unusable on the MCP plane.
        assert await _post_ingest(_app(api_engine), token, "someone-else") == 403
        assert await _post_mcp(_app(api_engine), token) == 401

    asyncio.run(exercise())


def test_revoked_and_expired_producers_are_rejected(
    operator: dict[str, str], owner_engine: AsyncEngine, api_engine: AsyncEngine
) -> None:
    token = json.loads(register(operator, "p039b-revoked").stdout)["token"]
    assert asyncio.run(_post_ingest(_app(api_engine), token, "p039b-revoked")) == 200
    revoked = run(operator, "producer", "revoke", "p039b-revoked", "--json")
    assert revoked.exit_code == 0 and json.loads(revoked.stdout)["changed"] is True
    assert asyncio.run(_post_ingest(_app(api_engine), token, "p039b-revoked")) == 401
    again = run(operator, "producer", "revoke", "p039b-revoked", "--json")
    assert again.exit_code == 0 and json.loads(again.stdout)["changed"] is False
    assert run(operator, "producer", "revoke", "p039b-unknown").exit_code == 2

    async def expire() -> str:
        engine = create_async_engine(operator["AGENT_CONTEXT_POSTGRESQL__DSN"], poolclass=NullPool)
        try:
            expired = await provisioning.register_producer(
                session_factory(engine),
                producer_id="p039b-expired",
                expires_in_days=1,
                cost=provisioning.HashCost(**COST),
                now=datetime.now(UTC) - timedelta(days=3),
            )
        finally:
            await engine.dispose()
        return expired.token

    expired_token = asyncio.run(expire())
    assert asyncio.run(_post_ingest(_app(api_engine), expired_token, "p039b-expired")) == 401


def test_register_refuses_an_active_id_and_rotate_keeps_it(
    operator: dict[str, str], owner_engine: AsyncEngine, api_engine: AsyncEngine
) -> None:
    first = json.loads(register(operator, "p039b-rotate").stdout)
    refused = register(operator, "p039b-rotate")
    assert refused.exit_code == 2 and "--rotate" in refused.stderr
    assert first["token"] not in refused.output

    rotated = register(operator, "p039b-rotate", "--rotate")
    assert rotated.exit_code == 0, rotated.output
    second = json.loads(rotated.stdout)
    assert second["rotated"] is True and second["id"] == "p039b-rotate"
    assert second["prefix"] != first["prefix"]

    async def exercise() -> None:
        assert await _post_ingest(_app(api_engine), first["token"], "p039b-rotate") == 401
        assert await _post_ingest(_app(api_engine), second["token"], "p039b-rotate") == 200
        async with owner_engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(
                        "SELECT count(*), bool_and(created_at <= updated_at) FROM "
                        "operations.registered_producers WHERE producer_id = 'p039b-rotate'"
                    )
                )
            ).one()
        assert tuple(rows) == (1, True)

    asyncio.run(exercise())
    # A revoked id is an operator decision: only --rotate may undo it.
    run(operator, "producer", "revoke", "p039b-rotate")
    assert register(operator, "p039b-rotate").exit_code == 2
    assert register(operator, "p039b-rotate", "--rotate").exit_code == 0


def test_an_expired_only_id_re_registers_without_rotate(operator: dict[str, str]) -> None:
    async def expire() -> None:
        engine = create_async_engine(operator["AGENT_CONTEXT_POSTGRESQL__DSN"], poolclass=NullPool)
        try:
            await provisioning.register_producer(
                session_factory(engine),
                producer_id="p039b-lapsed",
                expires_in_days=1,
                cost=provisioning.HashCost(**COST),
                now=datetime.now(UTC) - timedelta(days=3),
            )
        finally:
            await engine.dispose()

    asyncio.run(expire())
    assert register(operator, "p039b-lapsed").exit_code == 0
    # Now active again: refused without --rotate.
    assert register(operator, "p039b-lapsed").exit_code == 2


def test_a_failed_output_write_is_reported_and_cleaned_up(
    operator: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(self: cli.SecretFile, token: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(cli.SecretFile, "write", broken)
    path = tmp_path / "full.token"
    result = register(operator, "p039b-undelivered", "--output", str(path))
    assert result.exit_code == 1
    assert "NOT delivered" in result.stderr and "--rotate" in result.stderr
    assert not path.exists()
    monkeypatch.undo()
    # The credential exists, so a plain retry is refused and --rotate recovers.
    assert register(operator, "p039b-undelivered").exit_code == 2
    assert register(operator, "p039b-undelivered", "--rotate").exit_code == 0


def test_mcp_tokens_authenticate_only_on_mcp(
    operator: dict[str, str], api_engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    created = create(operator, "p039b-reader")
    assert created.exit_code == 0, created.output
    issued = json.loads(created.stdout)
    mcp_token = issued["token"]
    assert issued["prefix"].startswith("mcp_") and issued["scope"] == "memory:read"
    producer_token = json.loads(register(operator, "p039b-planes").stdout)["token"]

    async def before() -> None:
        assert await _post_mcp(_app(api_engine), mcp_token) == 200
        assert await _post_mcp(_app(api_engine), producer_token) == 401
        assert await _post_ingest(_app(api_engine), mcp_token, "p039b-planes") == 401
        assert await _post_ingest(_app(api_engine), producer_token, "p039b-planes") == 200

    asyncio.run(before())
    revoked = run(operator, "mcp-token", "revoke", issued["prefix"], "--json")
    assert revoked.exit_code == 0 and json.loads(revoked.stdout)["changed"] is True
    # A fresh app: a live replica may serve a revoked token for up to the cache TTL.
    assert asyncio.run(_post_mcp(_app(api_engine), mcp_token)) == 401
    assert run(operator, "mcp-token", "revoke", "mcp_unknown0001").exit_code == 2
    assert mcp_token not in caplog.text and producer_token not in caplog.text


def test_listing_never_prints_verifiers_or_tokens(operator: dict[str, str]) -> None:
    producer = json.loads(register(operator, "p039b-list").stdout)
    token = json.loads(create(operator, "p039b-list-reader").stdout)
    for args in (("producer", "list"), ("mcp-token", "list")):
        for flags in ((), ("--json",)):
            listed = run(operator, *args, *flags)
            assert listed.exit_code == 0, listed.output
            assert "argon2" not in listed.output and producer["token"] not in listed.output
            assert token["token"] not in listed.output
    assert "p039b-list" in run(operator, "producer", "list").stdout
    assert token["prefix"] in run(operator, "mcp-token", "list").stdout


def test_output_is_private_new_and_the_token_is_not_on_stdout(
    operator: dict[str, str], tmp_path: Path
) -> None:
    path = tmp_path / "producer.token"
    result = register(operator, "p039b-output", "--output", str(path))
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    token = path.read_text().strip()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "token" not in document and token not in result.output
    assert token.startswith(document["prefix"] + ".")

    # Never overwritten: refused before any credential is issued or changed.
    again = register(operator, "p039b-output", "--rotate", "--output", str(path))
    assert again.exit_code == 2 and path.read_text().strip() == token

    # A refused or failed run leaves no empty file behind.
    failed = tmp_path / "failed.token"
    assert register(operator, "p039b-output", "--output", str(failed)).exit_code == 2
    assert not failed.exists()

    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    inside = register(operator, "p039b-output2", "--output", str(checkout / "t"))
    assert inside.exit_code == 2 and not (checkout / "t").exists()

    mcp_path = tmp_path / "mcp.token"
    made = create(operator, "p039b-output-reader", "--output", str(mcp_path))
    assert made.exit_code == 0 and stat.S_IMODE(mcp_path.stat().st_mode) == 0o600
    assert mcp_path.read_text().strip() not in made.output


def test_text_output_puts_only_the_token_on_stdout(operator: dict[str, str]) -> None:
    result = run(
        operator, "producer", "register", "--producer-id", "p039b-text", "--expires-in", "5"
    )
    assert result.exit_code == 0
    assert len(result.stdout.split()) == 1 and result.stdout.startswith("prd_")
    assert "p039b-text" in result.stderr and result.stdout.strip() not in result.stderr


def test_the_roles_cannot_provision_and_the_preflight_names_the_grant(
    operator: dict[str, str], owner_engine: AsyncEngine, postgres_dsn: str
) -> None:
    suffix = uuid.uuid4().hex[:8]
    password = uuid.uuid4().hex
    names = {"api": f"p039b_api_{suffix}", "projector": f"p039b_projector_{suffix}"}
    parents = {"api": "agent_context_api", "projector": "agent_context_projector"}

    async def sql(statement: str) -> list[tuple[Any, ...]]:
        async with owner_engine.connect() as connection:
            result = await connection.execute(text(statement))
            rows = [tuple(row) for row in result] if result.returns_rows else []
            await connection.commit()
            return rows

    for key, name in names.items():
        asyncio.run(sql(f"CREATE ROLE {name} LOGIN PASSWORD '{password}' IN ROLE {parents[key]}"))
    try:
        base = make_url(postgres_dsn)
        for key, name in names.items():
            dsn = base.set(username=name, password=password).render_as_string(hide_password=False)
            env = {**_env(postgres_dsn), "AGENT_CONTEXT_POSTGRESQL__ADMIN_DSN": dsn}
            denied = register(env, f"p039b-denied-{key}")
            assert denied.exit_code == 1, denied.output
            expected = (
                "INSERT on operations.registered_producers" if key == "api" else "USAGE on schema"
            )
            assert expected in denied.stderr
            assert password not in denied.output
            assert run(env, "mcp-token", "list").exit_code == (0 if key == "api" else 1)
            assert create(env, "p039b-denied").exit_code == 1
        written = asyncio.run(
            sql(
                "SELECT count(*) FROM operations.registered_producers WHERE producer_id LIKE "
                "'p039b-denied-%'"
            )
        )
        assert written == [(0,)]
        # The migration grants nothing on either table to the projector, SELECT only to the API.
        grants = asyncio.run(
            sql(
                "SELECT grantee, table_name, privilege_type FROM "
                "information_schema.role_table_grants WHERE table_schema = 'operations' "
                "AND table_name IN ('registered_producers', 'mcp_tokens') "
                "AND grantee IN ('agent_context_api', 'agent_context_projector')"
            )
        )
        assert {row for row in grants} == {
            ("agent_context_api", "registered_producers", "SELECT"),
            ("agent_context_api", "mcp_tokens", "SELECT"),
        }
    finally:
        for name in names.values():
            asyncio.run(sql(f"DROP ROLE IF EXISTS {name}"))


def test_admin_dsn_is_used_and_falls_back_to_dsn(
    owner_engine: AsyncEngine, postgres_dsn: str
) -> None:
    only_admin = {
        **_env(postgres_dsn, admin=True),
        "AGENT_CONTEXT_POSTGRESQL__DSN": "postgresql+psycopg://nobody:x@127.0.0.1:1/none",
    }
    assert register(only_admin, "p039b-admin").exit_code == 0
    assert register(_env(postgres_dsn), "p039b-fallback").exit_code == 0


def test_a_path_swapped_during_provisioning_never_receives_the_token(
    operator: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "swapped.token"
    real = provisioning.register_producer

    async def swapping(*args: Any, **kwargs: Any) -> Any:
        path.rename(tmp_path / "original")
        path.write_text("attacker")
        return await real(*args, **kwargs)

    monkeypatch.setattr(provisioning, "register_producer", swapping)
    result = register(operator, "p039b-swap", "--output", str(path))
    assert result.exit_code == 0, result.output
    token = (tmp_path / "original").read_text().strip()
    assert token.startswith("prd_") and token not in result.output
    assert path.read_text() == "attacker"

    async def failing(*args: Any, **kwargs: Any) -> Any:
        path2 = tmp_path / "second.token"
        path2.unlink()
        path2.write_text("attacker")
        raise provisioning.ProvisioningError("producer_conflict")

    monkeypatch.setattr(provisioning, "register_producer", failing)
    second = register(operator, "p039b-swap2", "--output", str(tmp_path / "second.token"))
    assert second.exit_code == 2
    assert (tmp_path / "second.token").read_text() == "attacker"
