"""Integration tests: the batch ingestion route is atomic against real PostgreSQL and S3.

The full stack runs in process: ASGI app, real producer authentication, the real
``IngestionService`` over ``ContentService``/``S3BlobStore`` (Garage) and the
migrated schema. The application connects as the least-privileged
``agent_context_api`` role, so a missing grant fails these tests.

Every test uses unique streams, keys and content (a ``time.time_ns()`` nonce, never
a hex string, which the redaction engine may treat as a secret).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx2
import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    IngestBatchResponseV1,
    ProducerV1,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
)
from agent_context_sdk.ids import new_uuid7
from argon2 import PasswordHasher
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.app import create_app
from agent_context_platform.content.blob_store import S3BlobStore
from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    SqlProducerLookup,
)
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.settings import S3Settings, Settings

pytestmark = pytest.mark.integration

PRODUCER_ID = "atomicity-producer"
PREFIX = "atomic01"
TOKEN = f"{PREFIX}.{'a' * 43}"
CANARY = "AGENT_CONTEXT_CANARY_atomic_91c2"
OBSERVED = datetime(2026, 1, 1, tzinfo=UTC)

_S3_VARIABLES = {
    "endpoint_url": "AGENT_CONTEXT_TEST_S3_ENDPOINT_URL",
    "region_name": "AGENT_CONTEXT_TEST_S3_REGION_NAME",
    "bucket_name": "AGENT_CONTEXT_TEST_S3_BUCKET_NAME",
    "access_key_id": "AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID",
    "secret_access_key": "AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY",
}


@pytest.fixture(scope="module")
def s3_settings() -> S3Settings:
    values = {name: os.getenv(variable) for name, variable in _S3_VARIABLES.items()}
    if not any(values.values()):
        pytest.skip("ingestion integration tests require AGENT_CONTEXT_TEST_S3_* configuration")
    missing = [_S3_VARIABLES[name] for name, value in values.items() if not value]
    if missing:
        pytest.fail("partial S3 configuration; missing: " + ", ".join(missing))
    return S3Settings(**values)  # type: ignore[arg-type]


@pytest.fixture
def stack(
    ledger_engine: AsyncEngine, postgres_dsn: str, s3_settings: S3Settings
) -> Iterator[Stack]:
    """Register the producer (as owner) and serve the app as ``agent_context_api``."""
    now = datetime.now(UTC)

    async def register() -> None:
        async with ledger_engine.begin() as connection:
            await connection.execute(
                text("DELETE FROM operations.registered_producers WHERE producer_id = :id"),
                {"id": PRODUCER_ID},
            )
            await connection.execute(
                text(
                    "INSERT INTO operations.registered_producers (producer_id, token_prefix, "
                    "token_verifier, scope, expires_at, created_at, updated_at) VALUES "
                    "(:id, :prefix, :verifier, 'events:ingest', :expires, :now, :now)"
                ),
                {
                    "id": PRODUCER_ID,
                    "prefix": PREFIX,
                    "verifier": PasswordHasher(time_cost=1, memory_cost=8, parallelism=1).hash(
                        TOKEN
                    ),
                    "expires": now + timedelta(days=1),
                    "now": now,
                },
            )

    asyncio.run(register())
    api_engine = create_async_engine(
        postgres_dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    sessions = session_factory(api_engine)
    blob_store = S3BlobStore.from_settings(s3_settings)
    runtime = IngestionRuntime(
        authenticator=ProducerAuthenticator(
            SqlProducerLookup(sessions),
            Argon2Verifier(time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2),
        ),
        service=IngestionService(ContentService(blob_store, RedactionPolicyV1()), sessions),
        max_request_body_bytes=8_000_000,
    )
    yield Stack(
        ledger_engine, blob_store, create_app(Settings(environment="test"), ingestion=runtime)
    )
    asyncio.run(api_engine.dispose())


class Stack:
    def __init__(self, owner: AsyncEngine, blob_store: S3BlobStore, app: Any) -> None:
        self.owner = owner
        self.blob_store = blob_store
        self.app = app

    async def post(self, batch: IngestBatchRequestV1, token: str = TOKEN) -> httpx2.Response:
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

    async def scalar(self, sql: str, **params: Any) -> Any:
        async with self.owner.connect() as connection:
            return await connection.scalar(text(sql), params)

    async def count(self, table: str, where: str, **params: Any) -> int:
        return int(await self.scalar(f"SELECT count(*) FROM {table} WHERE {where}", **params))


def _content(size: int) -> tuple[bytes, ContentClaimV1, SanitizedContentItemV1]:
    nonce = time.time_ns()
    prefix = f"nonce={nonce} size={size} ".encode("ascii")
    data = prefix + b"a" * (size - len(prefix))
    claim = ContentClaimV1(
        content_id=f"content-{nonce}",
        content_sha256=hashlib.sha256(data).hexdigest(),
        media_type="text/plain",
        uncompressed_bytes=len(data),
    )
    item = SanitizedContentItemV1(
        claim=claim,
        sanitized_bytes_base64=base64.b64encode(data).decode("ascii"),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )
    return data, claim, item


def _draft(
    key: str,
    *,
    stream_id: str,
    claims: tuple[ContentClaimV1, ...] = (),
    session_id: str = "session-1",
    event_id: UUID | None = None,
) -> EventDraftV1:
    extra = {} if event_id is None else {"event_id": event_id}
    return EventDraftV1(
        event_type="agent.session.started",
        stream_id=stream_id,
        occurred_at=OBSERVED,
        observed_at=OBSERVED,
        producer=ProducerV1(producer_id=PRODUCER_ID, name="Codex", version="1.0.0"),
        payload={"source": "codex", "session_id": session_id},
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
        ),
        idempotency_key=key,
        content_claims=claims,
        **extra,
    )


def _batch(
    events: tuple[EventDraftV1, ...], items: tuple[SanitizedContentItemV1, ...] = ()
) -> IngestBatchRequestV1:
    return IngestBatchRequestV1(batch_id=new_uuid7(), events=events, content_items=items)


def _statuses(response: httpx2.Response) -> dict[str, str]:
    body = IngestBatchResponseV1.model_validate(response.json())
    return {str(item.event_id): item.status for item in body.accepted}


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_batch_with_inline_and_object_content_commits_everything(stack: Stack) -> None:
    run = uuid4().hex
    inline_data, inline_claim, inline_item = _content(2_000)
    object_data, object_claim, object_item = _content(INLINE_MAX_BYTES + 1_000)
    events = (
        _draft(f"a-{run}", stream_id=f"s-{run}", claims=(inline_claim, object_claim)),
        _draft(f"b-{run}", stream_id=f"s-{run}"),
    )
    batch = _batch(events, (inline_item, object_item))

    async def exercise() -> None:
        response = await stack.post(batch)
        assert response.status_code == 200, response.text
        assert set(_statuses(response).values()) == {"accepted"}
        ids = [str(event.event_id) for event in events]
        assert await stack.count("ledger.events", "stream_id = :s", s=f"s-{run}") == 2
        assert await stack.count("ledger.event_content_refs", "event_id = :e", e=ids[0]) == 2
        assert (
            await stack.count("projection.outbox", "event_id = ANY(:ids)", ids=events_ids(events))
            == 2
        )
        assert (
            await stack.count(
                "catalog.inline_contents", "content_sha256 = :d", d=inline_claim.content_sha256
            )
            == 1
        )
        assert (
            await stack.count(
                "catalog.content_objects", "content_sha256 = :d", d=object_claim.content_sha256
            )
            == 1
        )

    _run(exercise())
    assert inline_data and object_data


def events_ids(events: tuple[EventDraftV1, ...]) -> list[UUID]:
    return [event.event_id for event in events]


def test_partial_duplicate_batch_reports_existing_and_inserts_only_new_events(
    stack: Stack,
) -> None:
    run = uuid4().hex
    stream = f"s-{run}"
    first = _draft(f"k1-{run}", stream_id=stream)
    second = _draft(f"k2-{run}", stream_id=stream)
    third = _draft(f"k3-{run}", stream_id=stream)
    retried_second = _draft(f"k2-{run}", stream_id=stream)  # fresh event_id, same key

    async def exercise() -> None:
        seeded = await stack.post(_batch((first, second)))
        assert seeded.status_code == 200
        stored_second = str(second.event_id)

        response = await stack.post(_batch((first, retried_second, third)))

        assert response.status_code == 200, response.text
        statuses = _statuses(response)
        assert statuses == {
            str(first.event_id): "existing",
            stored_second: "existing",
            str(third.event_id): "accepted",
        }
        accepted = {
            item["event_id"]: item["stream_sequence"] for item in response.json()["accepted"]
        }
        assert accepted[str(first.event_id)] == 1
        assert accepted[stored_second] == 2
        assert accepted[str(third.event_id)] == 3
        assert await stack.count("ledger.events", "stream_id = :s", s=stream) == 3
        assert (
            await stack.count(
                "projection.outbox",
                "event_id = ANY(:ids)",
                ids=[first.event_id, second.event_id, third.event_id, retried_second.event_id],
            )
            == 3
        )

    _run(exercise())


def test_identical_batches_racing_report_exactly_one_writer(stack: Stack) -> None:
    run = uuid4().hex
    events = tuple(_draft(f"k{i}-{run}", stream_id=f"s-{run}") for i in range(5))
    batch = _batch(events)

    async def exercise() -> None:
        responses = await asyncio.gather(*(stack.post(batch) for _ in range(6)))

        assert [response.status_code for response in responses] == [200] * 6
        all_statuses = [set(_statuses(response).values()) for response in responses]
        assert all_statuses.count({"accepted"}) == 1
        assert all_statuses.count({"existing"}) == 5
        assert await stack.count("ledger.events", "stream_id = :s", s=f"s-{run}") == 5

    _run(exercise())


def _nothing_committed(stack: Stack, *, stream_ids: list[str], claims: list[ContentClaimV1]) -> Any:
    async def check() -> None:
        assert await stack.count("ledger.events", "stream_id = ANY(:s)", s=stream_ids) == 0
        assert await stack.count("ledger.event_streams", "stream_id = ANY(:s)", s=stream_ids) == 0
        for claim in claims:
            digest = claim.content_sha256
            assert (
                await stack.count("ledger.event_content_refs", "content_sha256 = :d", d=digest) == 0
            )
            assert (
                await stack.count("catalog.inline_contents", "content_sha256 = :d", d=digest) == 0
            )
            assert (
                await stack.count("catalog.content_objects", "content_sha256 = :d", d=digest) == 0
            )

    return check()


def test_last_event_conflict_commits_nothing_and_orphans_the_object_for_the_sweeper(
    stack: Stack,
) -> None:
    run = uuid4().hex
    _, inline_claim, inline_item = _content(1_500)
    _, object_claim, object_item = _content(INLINE_MAX_BYTES + 2_000)
    seeded = _draft(f"taken-{run}", stream_id=f"seed-{run}", session_id="original")
    good_a = _draft(f"a-{run}", stream_id=f"a-{run}", claims=(inline_claim,))
    good_b = _draft(f"b-{run}", stream_id=f"b-{run}", claims=(object_claim,))
    conflicting = _draft(f"taken-{run}", stream_id=f"seed-{run}", session_id="different")
    batch = _batch((good_a, good_b, conflicting), (inline_item, object_item))

    async def exercise() -> None:
        assert (await stack.post(_batch((seeded,)))).status_code == 200

        response = await stack.post(batch)

        assert response.status_code == 409
        body = IngestBatchResponseV1.model_validate(response.json())
        assert not body.accepted
        codes = {str(item.event_id): item.error_code for item in body.rejected}
        assert codes == {
            str(good_a.event_id): "batch_rejected",
            str(good_b.event_id): "batch_rejected",
            str(conflicting.event_id): "idempotency_conflict",
        }
        for event in (good_a, good_b, conflicting):
            assert await stack.count("ledger.events", "event_id = :e", e=event.event_id) == 0
        assert (
            await stack.count(
                "projection.outbox",
                "event_id = ANY(:ids)",
                ids=events_ids((good_a, good_b, conflicting)),
            )
            == 0
        )
        await _nothing_committed(
            stack, stream_ids=[f"a-{run}", f"b-{run}"], claims=[inline_claim, object_claim]
        )
        # The object was uploaded before the transaction; only the sweeper reclaims it.
        key = f"sha256/{object_claim.content_sha256[:2]}/{object_claim.content_sha256[2:4]}/{object_claim.content_sha256}.zst"
        assert await stack.blob_store.get_verified(key, object_claim.content_sha256)
        assert await stack.count("ledger.events", "stream_id = :s", s=f"seed-{run}") == 1

    _run(exercise())


def test_quarantined_last_stream_commits_nothing(stack: Stack) -> None:
    run = uuid4().hex
    _, claim, item = _content(1_200)
    good = _draft(f"a-{run}", stream_id=f"a-{run}", claims=(claim,))
    stuck = _draft(f"b-{run}", stream_id=f"z-{run}")

    async def exercise() -> None:
        async with stack.owner.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO ledger.event_streams (stream_id, last_sequence, status, "
                    "quarantined_at, created_at, updated_at) "
                    "VALUES (:s, 0, 'quarantined', now(), now(), now())"
                ),
                {"s": f"z-{run}"},
            )

        response = await stack.post(_batch((good, stuck), (item,)))

        assert response.status_code == 409
        codes = {item["error_code"] for item in response.json()["rejected"]}
        assert codes == {"batch_rejected", "stream_quarantined"}
        await _nothing_committed(stack, stream_ids=[f"a-{run}"], claims=[claim])
        assert await stack.count("ledger.events", "stream_id = :s", s=f"z-{run}") == 0

    _run(exercise())


def test_content_requiring_redaction_rejects_the_batch_without_leaking_it(
    stack: Stack, caplog: pytest.LogCaptureFixture
) -> None:
    run = uuid4().hex
    secret = b"password=" + CANARY.encode() + b" api_key=AKIAIOSFODNN7EXAMPLE"
    claim = ContentClaimV1(
        content_id=f"c-{run}",
        content_sha256=hashlib.sha256(secret).hexdigest(),
        media_type="text/plain",
        uncompressed_bytes=len(secret),
    )
    item = SanitizedContentItemV1(
        claim=claim,
        sanitized_bytes_base64=base64.b64encode(secret).decode(),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )
    event = _draft(f"a-{run}", stream_id=f"a-{run}", claims=(claim,), session_id=CANARY)
    caplog.set_level(logging.DEBUG)

    async def exercise() -> None:
        response = await stack.post(_batch((event,), (item,)))

        assert response.status_code == 422
        assert response.json()["rejected"][0]["error_code"] == "content_requires_redaction"
        assert CANARY not in response.text
        assert TOKEN not in response.text
        await _nothing_committed(stack, stream_ids=[f"a-{run}"], claims=[claim])

    _run(exercise())
    assert CANARY not in caplog.text
    assert "a" * 43 not in caplog.text
    assert "AKIAIOSFODNN7EXAMPLE" not in caplog.text
