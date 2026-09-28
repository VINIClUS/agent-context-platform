"""Integration tests for ledger integrity: verification and least privilege.

Covers the card's three integrity requirements against real PostgreSQL:
untampered events verify via the SDK's ``verify_event``; a superuser mutation
of a stored payload breaks verification; and the entire ``append`` write set
(stream row, event, content ref, redaction report, outbox row) succeeds under
``SET LOCAL ROLE agent_context_api`` while that role can neither UPDATE nor
DELETE ``ledger.events``.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from agent_context_sdk import (  # type: ignore[import-untyped]
    ContentClaimV1,
    ContentRefV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    RedactionReportV1,
    verify_event,
)
from agent_context_sdk.content.models import ContentDisposition as SdkContentDisposition
from agent_context_sdk.content.models import ContentStorage as SdkContentStorage
from agent_context_sdk.redaction.models import RedactionFindingSummaryV1
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_context_platform.catalog.models import ContentObjectRow
from agent_context_platform.ledger.models import EventRow, RedactionReportRow
from agent_context_platform.ledger.repository import LedgerRepository, ResolvedEvent

pytestmark = pytest.mark.integration

_OBSERVED_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _unique(label: str) -> str:
    return f"{label}-{uuid4()}"


def _producer(producer_id: str = "agent-1") -> ProducerV1:
    return ProducerV1(producer_id=producer_id, name="Agent One", version="1.0.0")


def _redaction_summary() -> EventRedactionSummaryV1:
    return EventRedactionSummaryV1(
        policy_version="policy-v1",
        disposition=SdkContentDisposition.SANITIZED,
        finding_counts={},
    )


def _draft(
    *,
    stream_id: str,
    idempotency_key: str,
    claims: tuple[ContentClaimV1, ...] = (),
) -> EventDraftV1:
    return EventDraftV1(
        event_type="test.event",
        stream_id=stream_id,
        occurred_at=_OBSERVED_AT,
        observed_at=_OBSERVED_AT,
        producer=_producer(),
        payload={"hello": "world"},
        redaction=_redaction_summary(),
        idempotency_key=idempotency_key,
        content_claims=claims,
    )


def _sha256(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _object_key(sha256: str) -> str:
    return f"sha256/{sha256[:2]}/{sha256[2:4]}/{sha256}.zst"


def test_untampered_reconstructed_event_verifies(ledger_engine: AsyncEngine) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        stream_id = _unique("stream")
        key = _unique("key")

        async with factory() as session:
            await LedgerRepository.append(
                session, [ResolvedEvent(draft=_draft(stream_id=stream_id, idempotency_key=key))]
            )
            await session.commit()

        async with factory() as session:
            [stored] = (
                await LedgerRepository.get_by_idempotency_keys(session, [("agent-1", key)])
            ).values()

        assert verify_event(stored) is True

    asyncio.run(exercise())


def test_tampering_with_the_stored_payload_breaks_verification(ledger_engine: AsyncEngine) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        stream_id = _unique("stream")
        key = _unique("key")

        async with factory() as session:
            [stored] = await LedgerRepository.append(
                session, [ResolvedEvent(draft=_draft(stream_id=stream_id, idempotency_key=key))]
            )
            await session.commit()
            event_id = stored.event_id

        # Tamper directly, as the superuser test connection -- bypassing the
        # repository entirely -- to prove verification (not application
        # logic) is what catches this. No trigger guards ledger.events.
        async with factory() as session:
            await session.execute(
                update(EventRow)
                .where(EventRow.event_id == event_id)
                .values(payload={"hello": "tampered"})
            )
            await session.commit()

        async with factory() as session:
            [reconstructed] = (
                await LedgerRepository.get_by_idempotency_keys(session, [("agent-1", key)])
            ).values()

        assert verify_event(reconstructed) is False

    asyncio.run(exercise())


def test_agent_context_api_role_can_append_but_not_update_or_delete_events(
    ledger_engine: AsyncEngine,
) -> None:
    """The whole one-transaction write set -- stream row, event, content ref,
    redaction report, outbox row -- succeeds under the restricted
    ``agent_context_api`` role, including the SAVEPOINT idempotency-recovery
    path on replay, while that role is denied UPDATE/DELETE on
    ``ledger.events``.
    """

    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        stream_id = _unique("stream")
        key = _unique("key")
        seed = _unique("content")
        sha = _sha256(seed)
        object_key = _object_key(sha)

        async with factory() as session:
            session.add(
                ContentObjectRow(
                    content_sha256=sha,
                    object_key=object_key,
                    media_type="text/plain",
                    compressed_bytes=5,
                    uncompressed_bytes=10,
                )
            )
            await session.commit()

        finding = RedactionFindingSummaryV1(
            detector="email", detector_version="1.0.0", category="pii", count=1
        )
        report = RedactionReportV1(
            policy_version="policy-v1",
            disposition=SdkContentDisposition.SANITIZED,
            findings=(finding,),
        )
        claim = ContentClaimV1(
            content_id="a", content_sha256=sha, media_type="text/plain", uncompressed_bytes=10
        )
        ref = ContentRefV1(
            content_id="a",
            content_sha256=sha,
            media_type="text/plain",
            uncompressed_bytes=10,
            disposition=SdkContentDisposition.SANITIZED,
            storage=SdkContentStorage.OBJECT,
            object_key=object_key,
            encoding="zstd",
        )
        draft = _draft(stream_id=stream_id, idempotency_key=key, claims=(claim,))
        resolved = ResolvedEvent(draft=draft, content_refs=(ref,), redaction_reports={"a": report})

        async with factory() as session:
            await session.execute(text("SET LOCAL ROLE agent_context_api"))
            current_user = await session.scalar(text("SELECT current_user"))
            assert current_user == "agent_context_api"

            [stored] = await LedgerRepository.append(session, [resolved])
            await session.commit()
            event_id = stored.event_id

        # Replay the identical draft under the same restricted role: this
        # exercises the SAVEPOINT idempotency-conflict recovery SELECT
        # (_recover_existing) with the restricted grants too.
        async with factory() as session:
            await session.execute(text("SET LOCAL ROLE agent_context_api"))
            [replayed] = await LedgerRepository.append(session, [resolved])
            await session.commit()
        assert replayed.event_id == event_id

        async with factory() as session:
            event = await session.get(EventRow, event_id)
            assert event is not None
            assert event.stream_sequence == 1

            report_count = await session.scalar(
                select(func.count())
                .select_from(RedactionReportRow)
                .where(RedactionReportRow.event_id == event_id)
            )
            assert report_count == 1

        async with factory() as session:
            await session.execute(text("SET LOCAL ROLE agent_context_api"))
            with pytest.raises(DBAPIError, match="permission denied"):
                await session.execute(
                    update(EventRow)
                    .where(EventRow.event_id == event_id)
                    .values(payload={"hacked": True})
                )

        async with factory() as session:
            await session.execute(text("SET LOCAL ROLE agent_context_api"))
            with pytest.raises(DBAPIError, match="permission denied"):
                await session.execute(
                    text("DELETE FROM ledger.events WHERE event_id = :event_id"),
                    {"event_id": event_id},
                )

    asyncio.run(exercise())
