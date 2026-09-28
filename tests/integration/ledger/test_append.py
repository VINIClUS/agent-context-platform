"""Integration tests for ``LedgerRepository.append`` against real PostgreSQL.

Every test uses a fresh, globally-unique ``stream_id``, idempotency key, and
content seed (all derived from ``uuid4()``) because ``ledger_engine`` is a
module-scoped fixture: all tests in this file share one live database, and
the idempotency constraint is ``(producer_id, idempotency_key)`` -- not
scoped by stream.

Content refs use OBJECT storage only, with a matching ``catalog.content_objects``
row inserted (and committed) in its own session *before* ``append`` runs.
INLINE refs are never fabricated here: the inline table arrives with
PLATFORM-021. Seeding in a separate, already-committed transaction also keeps
these tests correct once PLATFORM-021 adds a real FK from
``event_content_refs.object_key`` to ``catalog.content_objects.object_key``.
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
    verify_event,
)
from agent_context_sdk.content.models import ContentDisposition as SdkContentDisposition
from agent_context_sdk.content.models import ContentStorage as SdkContentStorage
from agent_context_sdk.events.envelope import new_uuid7
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from agent_context_platform.catalog.models import ContentObjectRow
from agent_context_platform.ledger.models import (
    EventContentRefRow,
    EventRow,
    EventStreamRow,
    StreamStatus,
)
from agent_context_platform.ledger.repository import (
    IdempotencyConflictError,
    LedgerRepository,
    ResolvedEvent,
    StreamQuarantinedError,
)
from agent_context_platform.projection.models import OutboxRow

pytestmark = pytest.mark.integration

_OBSERVED_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _unique(label: str) -> str:
    return f"{label}-{uuid4()}"


def _producer(producer_id: str) -> ProducerV1:
    return ProducerV1(producer_id=producer_id, name="Agent", version="1.0.0")


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
    producer_id: str = "agent-1",
    claims: tuple[ContentClaimV1, ...] = (),
    payload: dict[str, object] | None = None,
    event_id: object | None = None,
) -> EventDraftV1:
    kwargs: dict[str, object] = {
        "event_type": "test.event",
        "stream_id": stream_id,
        "occurred_at": _OBSERVED_AT,
        "observed_at": _OBSERVED_AT,
        "producer": _producer(producer_id),
        "payload": payload if payload is not None else {"hello": "world"},
        "redaction": _redaction_summary(),
        "idempotency_key": idempotency_key,
        "content_claims": claims,
    }
    if event_id is not None:
        kwargs["event_id"] = event_id
    return EventDraftV1(**kwargs)


def _sha256(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _object_key(sha256: str) -> str:
    return f"sha256/{sha256[:2]}/{sha256[2:4]}/{sha256}.zst"


def _claim(content_id: str, seed: str) -> ContentClaimV1:
    return ContentClaimV1(
        content_id=content_id,
        content_sha256=_sha256(seed),
        media_type="text/plain",
        uncompressed_bytes=10,
    )


def _content_ref(content_id: str, seed: str) -> ContentRefV1:
    sha = _sha256(seed)
    return ContentRefV1(
        content_id=content_id,
        content_sha256=sha,
        media_type="text/plain",
        uncompressed_bytes=10,
        disposition=SdkContentDisposition.SANITIZED,
        storage=SdkContentStorage.OBJECT,
        object_key=_object_key(sha),
        encoding="zstd",
    )


async def _seed_content_object(factory: async_sessionmaker, seed: str) -> None:
    """Insert and commit one ``catalog.content_objects`` row in its own transaction.

    Kept separate from any ``append`` transaction: ``ContentObjectRow`` lives
    on ``CatalogBase`` while ledger rows live on the shared ``db.Base``, and
    there is no FK between them yet, so nothing enforces insert ordering
    within a shared flush today. Committing the seed first is what keeps this
    test correct once PLATFORM-021 adds the FK.
    """
    sha = _sha256(seed)
    async with factory() as session:
        session.add(
            ContentObjectRow(
                content_sha256=sha,
                object_key=_object_key(sha),
                media_type="text/plain",
                compressed_bytes=5,
                uncompressed_bytes=10,
            )
        )
        await session.commit()


def test_append_creates_stream_event_and_outbox_row(ledger_engine: AsyncEngine) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        stream_id = _unique("stream")
        draft = _draft(stream_id=stream_id, idempotency_key=_unique("key"))

        async with factory() as session:
            [stored] = await LedgerRepository.append(session, [ResolvedEvent(draft=draft)])
            await session.commit()

        async with factory() as session:
            stream = await session.get(EventStreamRow, stream_id)
            assert stream is not None
            assert stream.last_sequence == 1
            assert stream.last_event_id == stored.event_id

            event_row = await session.get(EventRow, stored.event_id)
            assert event_row is not None
            assert event_row.stream_sequence == 1
            assert event_row.previous_event_sha256 is None

            outbox_count = await session.scalar(
                select(func.count())
                .select_from(OutboxRow)
                .where(OutboxRow.event_id == stored.event_id)
            )
            assert outbox_count == 1

    asyncio.run(exercise())


def test_append_is_idempotent_across_one_hundred_concurrent_callers_with_out_of_order_claims(
    ledger_engine: AsyncEngine, postgres_dsn: str
) -> None:
    """100 concurrent callers submit the literal same draft (fixed
    ``event_id``, per the card's "100 coroutines submitting the same draft")
    -- half with content claims/refs ordered [a, b], half as [b, a] -- and
    must converge on exactly one stored event (with that same ``event_id``),
    one outbox row, one allocated sequence number, and exactly two content
    ref rows reconstructed in canonical ``content_id`` order regardless of
    which submission order "won" the race.
    """

    async def exercise() -> None:
        stream_id = _unique("stream")
        key = _unique("key")
        seed_a, seed_b = _unique("a"), _unique("b")
        shared_event_id = new_uuid7()
        pool_engine = create_async_engine(
            postgres_dsn, pool_size=10, max_overflow=0, pool_timeout=120
        )
        try:
            factory = async_sessionmaker(pool_engine, expire_on_commit=False)
            await _seed_content_object(factory, seed_a)
            await _seed_content_object(factory, seed_b)

            draft_ab = _draft(
                stream_id=stream_id,
                idempotency_key=key,
                claims=(_claim("a", seed_a), _claim("b", seed_b)),
                event_id=shared_event_id,
            )
            draft_ba = _draft(
                stream_id=stream_id,
                idempotency_key=key,
                claims=(_claim("b", seed_b), _claim("a", seed_a)),
                event_id=shared_event_id,
            )
            refs_ab = (_content_ref("a", seed_a), _content_ref("b", seed_b))
            refs_ba = (_content_ref("b", seed_b), _content_ref("a", seed_a))

            async def call(index: int) -> object:
                draft, refs = (draft_ab, refs_ab) if index % 2 == 0 else (draft_ba, refs_ba)
                async with factory() as session:
                    [stored] = await LedgerRepository.append(
                        session, [ResolvedEvent(draft=draft, content_refs=refs)]
                    )
                    await session.commit()
                    return stored.event_id

            event_ids = await asyncio.wait_for(
                asyncio.gather(*(call(i) for i in range(100))), timeout=60
            )

            assert len(set(event_ids)) == 1
            assert event_ids[0] == shared_event_id

            async with factory() as session:
                stream = await session.get(EventStreamRow, stream_id)
                assert stream is not None
                assert stream.last_sequence == 1

                event_count = await session.scalar(
                    select(func.count())
                    .select_from(EventRow)
                    .where(EventRow.stream_id == stream_id)
                )
                assert event_count == 1

                outbox_count = await session.scalar(
                    select(func.count())
                    .select_from(OutboxRow)
                    .where(OutboxRow.event_id == event_ids[0])
                )
                assert outbox_count == 1

                ref_count = await session.scalar(
                    select(func.count())
                    .select_from(EventContentRefRow)
                    .where(EventContentRefRow.event_id == event_ids[0])
                )
                assert ref_count == 2

                [reconstructed] = (
                    await LedgerRepository.get_by_idempotency_keys(session, [("agent-1", key)])
                ).values()
                assert [ref.content_id for ref in reconstructed.content_refs] == ["a", "b"]
                assert verify_event(reconstructed) is True
        finally:
            await pool_engine.dispose()

    asyncio.run(exercise())


def test_out_of_order_claims_are_stored_reconstructed_and_verify(
    ledger_engine: AsyncEngine,
) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        stream_id = _unique("stream")
        key = _unique("key")
        seed_a, seed_b = _unique("a"), _unique("b")
        await _seed_content_object(factory, seed_a)
        await _seed_content_object(factory, seed_b)

        draft = _draft(
            stream_id=stream_id,
            idempotency_key=key,
            claims=(_claim("b", seed_b), _claim("a", seed_a)),
        )
        refs = (_content_ref("b", seed_b), _content_ref("a", seed_a))

        async with factory() as session:
            [stored] = await LedgerRepository.append(
                session, [ResolvedEvent(draft=draft, content_refs=refs)]
            )
            await session.commit()
            first_event_id = stored.event_id
            first_sequence = stored.stream_sequence

        async with factory() as session:
            [reconstructed] = (
                await LedgerRepository.get_by_idempotency_keys(session, [("agent-1", key)])
            ).values()
        assert [ref.content_id for ref in reconstructed.content_refs] == ["a", "b"]
        assert verify_event(reconstructed) is True

        # Resubmitting with claims/refs reordered as [a, b] must be recognized
        # as the SAME draft: same event ID, no new sequence allocated.
        reordered_draft = _draft(
            stream_id=stream_id,
            idempotency_key=key,
            claims=(_claim("a", seed_a), _claim("b", seed_b)),
        )
        reordered_refs = (_content_ref("a", seed_a), _content_ref("b", seed_b))
        async with factory() as session:
            [resubmitted] = await LedgerRepository.append(
                session, [ResolvedEvent(draft=reordered_draft, content_refs=reordered_refs)]
            )
            await session.commit()

        assert resubmitted.event_id == first_event_id
        assert resubmitted.stream_sequence == first_sequence

        async with factory() as session:
            stream = await session.get(EventStreamRow, stream_id)
            assert stream is not None
            assert stream.last_sequence == first_sequence

    asyncio.run(exercise())


def test_conflicting_event_mid_batch_undoes_the_whole_batch_on_rollback(
    ledger_engine: AsyncEngine,
) -> None:
    """A batch's second event conflicts with an already-committed event under
    the same idempotency key but a different payload. The batch's first event
    (a brand new event on an unrelated stream) is flushed before the conflict
    is detected -- proving the caller's rollback undoes the *whole* batch, not
    just the failing event.
    """

    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        conflict_stream = _unique("stream")
        other_stream = _unique("stream")
        shared_key = _unique("key")

        async with factory() as session:
            await LedgerRepository.append(
                session,
                [
                    ResolvedEvent(
                        draft=_draft(
                            stream_id=conflict_stream,
                            idempotency_key=shared_key,
                            payload={"a": 1},
                        )
                    )
                ],
            )
            await session.commit()

        async with factory() as session:
            batch = [
                ResolvedEvent(
                    draft=_draft(stream_id=other_stream, idempotency_key=_unique("unrelated"))
                ),
                ResolvedEvent(
                    draft=_draft(
                        stream_id=conflict_stream,
                        idempotency_key=shared_key,
                        payload={"a": 2},
                    )
                ),
            ]
            with pytest.raises(IdempotencyConflictError):
                await LedgerRepository.append(session, batch)
            await session.rollback()

        async with factory() as session:
            other_stream_row = await session.get(EventStreamRow, other_stream)
            assert other_stream_row is None
            other_event_count = await session.scalar(
                select(func.count()).select_from(EventRow).where(EventRow.stream_id == other_stream)
            )
            assert other_event_count == 0

    asyncio.run(exercise())


def test_get_by_idempotency_keys_omits_unmatched_pairs(ledger_engine: AsyncEngine) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        stream_id = _unique("stream")
        key = _unique("key")
        draft = _draft(stream_id=stream_id, idempotency_key=key)

        async with factory() as session:
            await LedgerRepository.append(session, [ResolvedEvent(draft=draft)])
            await session.commit()

        async with factory() as session:
            found = await LedgerRepository.get_by_idempotency_keys(
                session, [("agent-1", key), ("agent-1", _unique("absent"))]
            )

        assert set(found) == {("agent-1", key)}

    asyncio.run(exercise())


def test_append_refuses_a_quarantined_stream(ledger_engine: AsyncEngine) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(ledger_engine, expire_on_commit=False)
        stream_id = _unique("stream")
        first = _draft(stream_id=stream_id, idempotency_key=_unique("key"))
        async with factory() as session:
            await LedgerRepository.append(session, [ResolvedEvent(draft=first)])
            await session.commit()

        async with factory() as session:
            await session.execute(
                update(EventStreamRow)
                .where(EventStreamRow.stream_id == stream_id)
                .values(status=StreamStatus.QUARANTINED, quarantined_at=_OBSERVED_AT)
            )
            await session.commit()

        blocked = _draft(stream_id=stream_id, idempotency_key=_unique("key"))
        async with factory() as session:
            with pytest.raises(StreamQuarantinedError) as caught:
                await LedgerRepository.append(session, [ResolvedEvent(draft=blocked)])
            await session.rollback()
        assert caught.value.stream_id == stream_id

        async with factory() as session:
            stream = await session.get(EventStreamRow, stream_id)
            assert stream is not None
            assert stream.last_sequence == 1
            assert await session.get(EventRow, blocked.event_id) is None

    asyncio.run(exercise())
