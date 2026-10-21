"""Crash the real API process at every documented ingestion fault point, then recover.

Each case runs ``uvicorn`` as a subprocess armed with one label from
``docs/operations/fault-injection.md``, posts a batch with object-storage content, and
checks the process died with exit code 137 at that point. A second process without the
label then accepts the same batch. At every step: no committed reference to a missing
object, no duplicate sequence, a contiguous hash chain, and convergence to exactly one
stored copy of each event and one outbox row each.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx2
import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    ProducerV1,
    RedactionReportV1,
    SanitizedContentItemV1,
)
from agent_context_sdk.ids import new_uuid7
from argon2 import PasswordHasher
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.content.blob_store import BlobNotFoundError, S3BlobStore
from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.settings import S3Settings

from ..fault_harness import CRASH_EXIT_CODE, ApiServer, process_env

pytestmark = pytest.mark.integration

PRODUCER_ID = "fault-producer"
PREFIX = "fault001"
TOKEN = f"{PREFIX}.{'f' * 43}"
OBSERVED = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class Case:
    label: str
    committed_before_restart: bool
    object_stored_before_restart: bool


CASES = [
    Case("content.before_s3_put", False, False),
    Case("content.after_s3_put", False, True),
    Case("content.after_head_verification", False, True),
    Case("ledger.before_db_transaction", False, True),
    Case("ledger.after_event_before_outbox", False, True),
    Case("ledger.before_commit", False, True),
    Case("ledger.after_commit_before_response", True, True),
]


@pytest.fixture
def producer(ledger_engine: AsyncEngine) -> Iterator[None]:
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
    yield


def _object_content() -> tuple[ContentClaimV1, SanitizedContentItemV1]:
    nonce = time.time_ns()
    size = INLINE_MAX_BYTES + 1_000
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
    return claim, item


def _draft(key: str, stream_id: str, claims: tuple[ContentClaimV1, ...] = ()) -> EventDraftV1:
    return EventDraftV1(
        event_type="agent.session.started",
        stream_id=stream_id,
        occurred_at=OBSERVED,
        observed_at=OBSERVED,
        producer=ProducerV1(producer_id=PRODUCER_ID, name="Codex", version="1.0.0"),
        payload={"source": "codex", "session_id": "session-1"},
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
        ),
        idempotency_key=key,
        content_claims=claims,
    )


class Observer:
    """Reads the datastores the way an E2E scenario would: SQL and S3 only."""

    def __init__(self, owner: AsyncEngine, blob_store: S3BlobStore, stream_id: str) -> None:
        self.owner = owner
        self.blob_store = blob_store
        self.stream_id = stream_id

    async def rows(self, sql: str, **params: Any) -> list[Any]:
        async with self.owner.connect() as connection:
            return list((await connection.execute(text(sql), params)).all())

    async def event_count(self) -> int:
        rows = await self.rows(
            "SELECT count(*) FROM ledger.events WHERE stream_id = :s", s=self.stream_id
        )
        return int(rows[0][0])

    async def object_stored(self, digest: str) -> bool:
        key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"
        try:
            await self.blob_store.get_verified(key, digest)
        except BlobNotFoundError:
            return False
        return True

    async def assert_ledger_invariants(self, expected_events: int) -> None:
        events = await self.rows(
            "SELECT stream_sequence, previous_event_sha256, event_sha256 FROM ledger.events "
            "WHERE stream_id = :s ORDER BY stream_sequence",
            s=self.stream_id,
        )
        assert len(events) == expected_events
        sequences = [row[0] for row in events]
        assert sequences == list(range(1, expected_events + 1)), "gap or duplicate sequence"
        previous = None
        for _sequence, previous_hash, event_hash in events:
            assert previous_hash == previous, "broken hash chain"
            previous = event_hash
        if expected_events:
            head = await self.rows(
                "SELECT last_sequence, last_event_sha256 FROM ledger.event_streams "
                "WHERE stream_id = :s",
                s=self.stream_id,
            )
            assert head == [(expected_events, previous)]
        outbox = await self.rows(
            "SELECT count(*) FROM projection.outbox o JOIN ledger.events e USING (event_id) "
            "WHERE e.stream_id = :s",
            s=self.stream_id,
        )
        assert outbox[0][0] == expected_events, "outbox must hold one row per stored event"

    async def assert_no_dangling_object_reference(self) -> None:
        refs = await self.rows(
            "SELECT r.object_key, r.content_sha256, "
            "EXISTS (SELECT 1 FROM catalog.content_objects c WHERE c.object_key = r.object_key) "
            "FROM ledger.event_content_refs r JOIN ledger.events e USING (event_id) "
            "WHERE e.stream_id = :s AND r.storage = 'object'",
            s=self.stream_id,
        )
        for _key, digest, cataloged in refs:
            assert cataloged, "committed reference without a catalog row"
            assert await self.object_stored(digest), "committed reference to a missing object"


@pytest.mark.parametrize("case", CASES, ids=[case.label for case in CASES])
def test_crash_at_label_then_restart_recovers(
    case: Case, ledger_engine: AsyncEngine, postgres_dsn: str, producer: None
) -> None:
    run = uuid4().hex
    stream_id = f"fault-{run}"
    claim, item = _object_content()
    batch = IngestBatchRequestV1(
        batch_id=new_uuid7(),
        events=(
            _draft(f"a-{run}", stream_id, (claim,)),
            _draft(f"b-{run}", stream_id),
        ),
        content_items=(item,),
    )
    body = batch.model_dump_json()
    batch_id = str(batch.batch_id)

    blob_store = S3BlobStore.from_settings(_s3_settings())
    observer = Observer(ledger_engine, blob_store, stream_id)

    with ApiServer(process_env(postgres_dsn, crash_at=case.label)) as crashing:
        with pytest.raises(httpx2.TransportError):
            crashing.post_batch(TOKEN, batch_id, body)
        assert crashing.wait() == CRASH_EXIT_CODE
        assert f"fault_injected label={case.label}" in crashing.stderr

    async def after_crash() -> None:
        assert await observer.event_count() == (2 if case.committed_before_restart else 0)
        assert (
            await observer.object_stored(claim.content_sha256) is case.object_stored_before_restart
        )
        await observer.assert_no_dangling_object_reference()
        await observer.assert_ledger_invariants(2 if case.committed_before_restart else 0)

    asyncio.run(after_crash())

    with ApiServer(process_env(postgres_dsn)) as recovered:
        response = recovered.post_batch(TOKEN, batch_id, body)
        assert response.status_code == 200, response.text
        statuses = {entry["status"] for entry in response.json()["accepted"]}
        assert statuses == ({"existing"} if case.committed_before_restart else {"accepted"})

        replay = recovered.post_batch(TOKEN, batch_id, body)
        assert replay.status_code == 200
        assert {entry["status"] for entry in replay.json()["accepted"]} == {"existing"}

    async def after_recovery() -> None:
        await observer.assert_ledger_invariants(2)
        await observer.assert_no_dangling_object_reference()
        assert await observer.object_stored(claim.content_sha256)
        refs = await observer.rows(
            "SELECT count(*) FROM ledger.event_content_refs r JOIN ledger.events e USING "
            "(event_id) WHERE e.stream_id = :s",
            s=stream_id,
        )
        assert refs[0][0] == 1

    asyncio.run(after_recovery())


def _s3_settings() -> S3Settings:
    return S3Settings.model_validate(
        {
            "endpoint_url": os.environ["AGENT_CONTEXT_TEST_S3_ENDPOINT_URL"],
            "region_name": os.environ["AGENT_CONTEXT_TEST_S3_REGION_NAME"],
            "bucket_name": os.environ["AGENT_CONTEXT_TEST_S3_BUCKET_NAME"],
            "access_key_id": os.environ["AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY"],
        }
    )
