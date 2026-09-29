"""Integration tests: producer authentication against the real ``registered_producers`` table.

The lookup runs as the least-privileged ``agent_context_api`` role; registrations are
inserted by the schema owner. A stub service stands in for ingestion, so only the
authentication and producer-binding path is exercised.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
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
from argon2 import PasswordHasher
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.app import create_app
from agent_context_platform.db import session_factory
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    InsufficientScopeError,
    InvalidCredentialError,
    ProducerAuthenticator,
    SqlProducerLookup,
)
from agent_context_platform.ledger.service import IngestOutcome
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.integration

SECRET = "s" * 43
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)
OBSERVED = datetime(2026, 1, 1, tzinfo=UTC)


class StubService:
    def __init__(self) -> None:
        self.calls = 0

    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        self.calls += 1
        accepted = tuple(
            AcceptedEventV1(event_id=e.event_id, status="accepted", stream_sequence=i + 1)
            for i, e in enumerate(batch.events)
        )
        return IngestOutcome(200, IngestBatchResponseV1(batch_id=batch.batch_id, accepted=accepted))


class Harness:
    def __init__(self, owner: AsyncEngine, api: AsyncEngine) -> None:
        self.owner = owner
        self.authenticator = ProducerAuthenticator(
            SqlProducerLookup(session_factory(api)),
            Argon2Verifier(time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2),
        )
        self.service = StubService()
        runtime = IngestionRuntime(self.authenticator, self.service, 1_000_000)  # type: ignore[arg-type]
        self.app = create_app(Settings(environment="test"), ingestion=runtime)

    async def register(
        self,
        prefix: str,
        *,
        producer_id: str = "auth-producer",
        scope: str = "events:ingest",
        expires_in: timedelta = timedelta(days=1),
        revoked: bool = False,
    ) -> str:
        now = datetime.now(UTC)
        async with self.owner.begin() as connection:
            await connection.execute(
                text(
                    "DELETE FROM operations.registered_producers "
                    "WHERE token_prefix = :p OR producer_id = :id"
                ),
                {"p": prefix, "id": producer_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO operations.registered_producers (producer_id, token_prefix, "
                    "token_verifier, scope, expires_at, revoked_at, created_at, updated_at) "
                    "VALUES (:id, :p, :v, :scope, :exp, :rev, :created, :now)"
                ),
                {
                    "id": producer_id,
                    "p": prefix,
                    "v": _HASHER.hash(f"{prefix}.{SECRET}"),
                    "scope": scope,
                    "exp": now + expires_in,
                    "rev": now - timedelta(minutes=1) if revoked else None,
                    "now": now,
                    "created": now - timedelta(hours=1),
                },
            )
        return f"{prefix}.{SECRET}"

    async def post(self, token: str, producer_id: str) -> httpx2.Response:
        event = EventDraftV1(
            event_type="agent.session.started",
            stream_id="s-auth",
            occurred_at=OBSERVED,
            observed_at=OBSERVED,
            producer=ProducerV1(producer_id=producer_id, name="Codex", version="1.0.0"),
            payload={"source": "codex", "session_id": "session-1"},
            redaction=EventRedactionSummaryV1(
                policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
            ),
            idempotency_key="auth-key",
        )
        batch = IngestBatchRequestV1(batch_id=new_uuid7(), events=(event,))
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=self.app), base_url="http://127.0.0.1:8000"
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


@pytest.fixture
def harness(ledger_engine: AsyncEngine, postgres_dsn: str) -> Iterator[Harness]:
    api = create_async_engine(
        postgres_dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    yield Harness(ledger_engine, api)
    asyncio.run(api.dispose())


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_valid_credential_authenticates_and_binds_the_producer(harness: Harness) -> None:
    async def exercise() -> None:
        token = await harness.register("validpfx1")

        principal = await harness.authenticator.authenticate(f"Bearer {token}")
        response = await harness.post(token, "auth-producer")

        assert principal.producer_id == "auth-producer"
        assert response.status_code == 200
        assert harness.service.calls == 1

    _run(exercise())


@pytest.mark.parametrize("case", ["unknown", "wrong_secret", "revoked", "expired"])
def test_bad_credentials_are_indistinguishable_and_never_reach_the_service(
    harness: Harness, case: str
) -> None:
    async def exercise() -> None:
        token = await harness.register(
            "badpfx001",
            revoked=case == "revoked",
            expires_in=timedelta(minutes=-5) if case == "expired" else timedelta(days=1),
        )
        if case == "unknown":
            token = f"nopfx0001.{SECRET}"
        elif case == "wrong_secret":
            token = f"badpfx001.{'x' * 43}"

        with pytest.raises(InvalidCredentialError):
            await harness.authenticator.authenticate(f"Bearer {token}")
        response = await harness.post(token, "auth-producer")

        assert response.status_code == 401
        assert response.json()["error"]["code"] == "invalid_credential"
        assert SECRET not in response.text
        assert harness.service.calls == 0

    _run(exercise())


def test_producer_mismatch_is_forbidden(harness: Harness) -> None:
    async def exercise() -> None:
        token = await harness.register("mismatch1")

        response = await harness.post(token, "someone-else")

        assert response.status_code == 403
        assert response.json()["error"]["code"] == "producer_mismatch"
        assert harness.service.calls == 0

    _run(exercise())


def test_scope_check_constraint_blocks_read_registrations_at_the_database(
    harness: Harness,
) -> None:
    async def exercise() -> None:
        with pytest.raises(DBAPIError):
            await harness.register("readpfx01", scope="memory:read")

    _run(exercise())


def test_insufficient_scope_error_is_a_403_type() -> None:
    assert InsufficientScopeError.status_code == 403
