"""MCP authentication against the real ``operations.mcp_tokens`` table and both credential planes."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx2
import pytest
from agent_context_sdk import (
    ContentDisposition,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    ProducerV1,
)
from agent_context_sdk.ids import new_uuid7
from argon2 import PasswordHasher
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.app import create_app
from agent_context_platform.db import session_factory
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import ProducerAuthenticator, SqlProducerLookup
from agent_context_platform.mcp import auth as mcp_auth
from agent_context_platform.mcp.auth import (
    McpAuthenticator,
    McpAuthRuntime,
    PrincipalCache,
    SqlMcpTokenLookup,
    TokenBucketLimiter,
)
from agent_context_platform.security.bearer import Argon2Verifier
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.integration

_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)
SECRET = "s" * 43
HMAC_KEY = "integration-hmac-key-0123456789abcdef"
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


def _verifier() -> Argon2Verifier:
    return Argon2Verifier(
        time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2, max_queue_depth=8
    )


class NeverIngest:
    async def ingest(self, batch: object) -> object:
        raise AssertionError("an unauthenticated batch reached the ingestion service")


async def _insert_token(
    owner: AsyncEngine,
    prefix: str,
    *,
    principal: str = "codex-reader",
    scopes: str = "{memory:read}",
    revoked: bool = False,
    expires_in: timedelta = timedelta(days=1),
) -> str:
    now = datetime.now(UTC)
    async with owner.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO operations.mcp_tokens (token_id, token_prefix, token_verifier, "
                "principal, scopes, created_at, expires_at, revoked_at) VALUES "
                "(uuidv7(), :p, :v, :principal, CAST(:scopes AS text[]), :created, :exp, :rev)"
            ),
            {
                "p": prefix,
                "v": _HASHER.hash(f"{prefix}.{SECRET}"),
                "principal": principal,
                "scopes": scopes,
                "created": now + min(expires_in, timedelta(0)) - timedelta(hours=1),
                "exp": now + expires_in,
                "rev": now - timedelta(minutes=1) if revoked else None,
            },
        )
    return f"{prefix}.{SECRET}"


async def _insert_producer(owner: AsyncEngine, prefix: str) -> str:
    now = datetime.now(UTC)
    async with owner.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO operations.registered_producers (producer_id, token_prefix, "
                "token_verifier, scope, expires_at, created_at, updated_at) "
                "VALUES ('auth-producer', :p, :v, 'events:ingest', :exp, :created, :now)"
            ),
            {
                "p": prefix,
                "v": _HASHER.hash(f"{prefix}.{SECRET}"),
                "exp": now + timedelta(days=1),
                "created": now - timedelta(hours=1),
                "now": now,
            },
        )
    return f"{prefix}.{SECRET}"


def _application(api: AsyncEngine) -> Any:
    sessions = session_factory(api)
    clock = lambda: datetime.now(UTC)  # noqa: E731
    runtime = McpAuthRuntime(
        McpAuthenticator(
            SqlMcpTokenLookup(sessions),
            _verifier(),
            PrincipalCache(b"k" * 32, ttl=timedelta(seconds=30), max_entries=8, clock=clock),
        ),
        TokenBucketLimiter(rate_per_second=100, burst=100),
    )
    ingestion = IngestionRuntime(
        ProducerAuthenticator(SqlProducerLookup(sessions), _verifier()),
        NeverIngest(),  # type: ignore[arg-type]
        1_000_000,
    )
    return create_app(Settings(environment="test"), ingestion=ingestion, mcp_auth=runtime)


async def _post_mcp(app: Any, token: str) -> httpx2.Response:
    async with (
        app.router.lifespan_context(app),
        httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
        ) as client,
    ):
        return await client.post(
            "/mcp", headers={**MCP_HEADERS, "authorization": f"Bearer {token}"}, json=MCP_BODY
        )


async def _post_ingest(app: Any, token: str) -> httpx2.Response:
    event = EventDraftV1(
        event_type="agent.session.started",
        stream_id="s-auth",
        occurred_at=OBSERVED,
        observed_at=OBSERVED,
        producer=ProducerV1(producer_id="auth-producer", name="Codex", version="1.0.0"),
        payload={"source": "codex", "session_id": "session-1"},
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
        ),
        idempotency_key="auth-key",
    )
    batch = IngestBatchRequestV1(batch_id=new_uuid7(), events=(event,))
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        return await client.post(
            "/v1/ingestion/batches",
            content=batch.model_dump_json(),
            headers={
                "authorization": f"Bearer {token}",
                "idempotency-key": str(batch.batch_id),
                "content-type": "application/json",
            },
        )


def test_memory_read_token_reads_and_bad_tokens_are_401(
    owner_engine: AsyncEngine, api_engine: AsyncEngine
) -> None:
    async def exercise() -> None:
        good = await _insert_token(owner_engine, "mcp_good0001")
        revoked = await _insert_token(owner_engine, "mcp_revoked1", revoked=True)
        expired = await _insert_token(owner_engine, "mcp_expired1", expires_in=-timedelta(hours=1))
        app = _application(api_engine)

        assert (await _post_mcp(app, good)).status_code == 200
        for token in (revoked, expired, f"mcp_good0001.{'x' * 43}", f"mcp_nope0001.{SECRET}"):
            response = await _post_mcp(_application(api_engine), token)
            assert response.status_code == 401
            assert response.headers["www-authenticate"] == "Bearer"

    asyncio.run(exercise())


def test_scope_check_constraint_rejects_unknown_and_empty_scopes(owner_engine: AsyncEngine) -> None:
    async def insert(prefix: str, scopes: str) -> None:
        await _insert_token(owner_engine, prefix, scopes=scopes)

    for scopes in ("{events:ingest}", "{}", "{memory:read,events:ingest}"):
        with pytest.raises(DBAPIError):
            asyncio.run(insert("mcp_scopechk1", scopes))
    with pytest.raises(DBAPIError):
        asyncio.run(insert("prod_prefix1", "{memory:read}"))


def test_the_api_role_can_read_but_never_write_tokens(
    owner_engine: AsyncEngine, api_engine: AsyncEngine
) -> None:
    async def exercise() -> None:
        await _insert_token(owner_engine, "mcp_grants01")
        async with api_engine.connect() as connection:
            assert await connection.scalar(
                text(
                    "SELECT count(*) FROM operations.mcp_tokens WHERE token_prefix = 'mcp_grants01'"
                )
            )
        for statement in (
            "UPDATE operations.mcp_tokens SET revoked_at = NULL",
            "DELETE FROM operations.mcp_tokens",
        ):
            async with api_engine.connect() as connection:
                with pytest.raises(DBAPIError):
                    await connection.execute(text(statement))

    asyncio.run(exercise())


def test_credentials_are_unusable_on_the_other_plane(
    owner_engine: AsyncEngine, api_engine: AsyncEngine
) -> None:
    async def exercise() -> None:
        mcp_token = await _insert_token(owner_engine, "mcp_planes01")
        ingest_token = await _insert_producer(owner_engine, "prod_plane01")

        # events:ingest producer token on /mcp
        assert (await _post_mcp(_application(api_engine), ingest_token)).status_code in (401, 403)
        # memory:read token on the ingestion plane
        assert (await _post_ingest(_application(api_engine), mcp_token)).status_code in (401, 403)
        # each token works on its own plane's authenticator
        assert (await _post_mcp(_application(api_engine), mcp_token)).status_code == 200

    asyncio.run(exercise())


def test_minted_token_round_trips_through_the_real_app(
    owner_engine: AsyncEngine, postgres_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(mcp_auth.DSN_ENVIRONMENT_VARIABLE, postgres_dsn)
    assert (
        mcp_auth._main(["mint", "--principal", "x", "--expires", "2000-01-01T00:00:00+00:00"]) == 2
    )

    expires = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    captured: list[str] = []
    monkeypatch.setattr("builtins.print", lambda *a, **_k: captured.append(str(a[0])))
    assert mcp_auth._main(["mint", "--principal", "minted-reader", "--expires", expires]) == 0
    token = captured[-1]
    assert token.startswith("mcp_")

    settings = Settings(
        environment="test",
        postgresql={"dsn": postgres_dsn},
        mcp={"token_hmac_key": HMAC_KEY},
    )
    with TestClient(create_app(settings), base_url="http://127.0.0.1:8000") as client:
        ok = client.post(
            "/mcp", headers={**MCP_HEADERS, "authorization": f"Bearer {token}"}, json=MCP_BODY
        )
        missing = client.post("/mcp", headers=MCP_HEADERS, json=MCP_BODY)

    assert ok.status_code == 200
    assert missing.status_code == 401

    async def stored() -> tuple[str, str]:
        async with owner_engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT token_verifier, principal FROM operations.mcp_tokens "
                        "WHERE principal = 'minted-reader'"
                    )
                )
            ).one()
            return row[0], row[1]

    verifier, principal = asyncio.run(stored())
    assert principal == "minted-reader"
    assert token.partition(".")[2] not in verifier
    assert verifier.startswith("$argon2id$")
