"""Integration tests proving cross-batch deadlock avoidance under load.

``LedgerRepository.append`` locks every stream touched by a batch in sorted
``stream_id`` order internally (see ``_lock_streams``), regardless of the
order events were submitted in. The first test hammers two shared streams
with many concurrent batches submitted in *opposite* relative order and
asserts Postgres never reports a deadlock, and that both streams end up
with contiguous, gap-free sequences. The second covers what stream locks
cannot: batches on disjoint streams that reuse the same idempotency keys in
opposite order (see ``_lock_idempotency_keys``).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from agent_context_sdk import (  # type: ignore[import-untyped]
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
)
from agent_context_sdk.content.models import ContentDisposition as SdkContentDisposition
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

import agent_context_platform.ledger.repository as ledger_repository
from agent_context_platform.ledger.models import EventRow, EventStreamRow
from agent_context_platform.ledger.repository import (
    IdempotencyConflictError,
    LedgerRepository,
    ResolvedEvent,
)

pytestmark = pytest.mark.integration

_OBSERVED_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _producer(producer_id: str) -> ProducerV1:
    return ProducerV1(producer_id=producer_id, name="Agent", version="1.0.0")


def _redaction_summary() -> EventRedactionSummaryV1:
    return EventRedactionSummaryV1(
        policy_version="policy-v1",
        disposition=SdkContentDisposition.SANITIZED,
        finding_counts={},
    )


def _draft(*, stream_id: str, idempotency_key: str, producer_id: str) -> EventDraftV1:
    return EventDraftV1(
        event_type="test.event",
        stream_id=stream_id,
        occurred_at=_OBSERVED_AT,
        observed_at=_OBSERVED_AT,
        producer=_producer(producer_id),
        payload={"hello": "world"},
        redaction=_redaction_summary(),
        idempotency_key=idempotency_key,
    )


def test_opposite_order_two_stream_batches_never_deadlock_under_load(
    ledger_engine: AsyncEngine, postgres_dsn: str
) -> None:
    async def exercise() -> None:
        stream_x = f"stream-x-{uuid4()}"
        stream_y = f"stream-y-{uuid4()}"
        rounds = 8
        batches_per_round = 10
        pool_engine = create_async_engine(
            postgres_dsn, pool_size=10, max_overflow=0, pool_timeout=120
        )
        try:
            factory = async_sessionmaker(pool_engine, expire_on_commit=False)

            async def run_batch(round_index: int, batch_index: int, forward: bool) -> None:
                producer_id = f"p-{round_index}-{batch_index}"
                first = ResolvedEvent(
                    draft=_draft(
                        stream_id=stream_x,
                        idempotency_key=f"{producer_id}-x",
                        producer_id=producer_id,
                    )
                )
                second = ResolvedEvent(
                    draft=_draft(
                        stream_id=stream_y,
                        idempotency_key=f"{producer_id}-y",
                        producer_id=producer_id,
                    )
                )
                batch = [first, second] if forward else [second, first]
                async with factory() as session:
                    await LedgerRepository.append(session, batch)
                    await session.commit()

            for round_index in range(rounds):
                await asyncio.wait_for(
                    asyncio.gather(
                        *(
                            run_batch(round_index, batch_index, batch_index % 2 == 0)
                            for batch_index in range(batches_per_round)
                        )
                    ),
                    timeout=60,
                )

            async with factory() as session:
                for stream_id in (stream_x, stream_y):
                    stream = await session.get(EventStreamRow, stream_id)
                    assert stream is not None
                    assert stream.last_sequence == rounds * batches_per_round

                    sequences = (
                        await session.scalars(
                            select(EventRow.stream_sequence)
                            .where(EventRow.stream_id == stream_id)
                            .order_by(EventRow.stream_sequence)
                        )
                    ).all()
                    assert sequences == list(range(1, rounds * batches_per_round + 1))
        finally:
            await pool_engine.dispose()

    asyncio.run(exercise())


def test_opposite_key_order_batches_on_disjoint_streams_resolve_without_deadlock(
    ledger_engine: AsyncEngine, postgres_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One batch commits and the other gets a clean conflict, never a deadlock.

    The two batches share a producer and both idempotency keys but touch
    disjoint streams, so stream locks do not serialize them. The patched
    insert step forces the dangerous schedule: after its first insert, each
    batch waits (bounded) for the other's first insert before continuing,
    so both would hold one uncommitted key and wait on the other's.
    """
    original_insert = ledger_repository._insert_or_recover
    first_inserted: dict[str, asyncio.Event] = {}

    async def interleaved_insert(*args: object) -> object:
        stored = await original_insert(*args)  # type: ignore[arg-type]
        stream_id = args[2].stream_id  # type: ignore[attr-defined]
        mine = first_inserted[stream_id]
        if not mine.is_set():
            mine.set()
            (other,) = (event for key, event in first_inserted.items() if key != stream_id)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(other.wait(), timeout=2)
        return stored

    monkeypatch.setattr(ledger_repository, "_insert_or_recover", interleaved_insert)

    async def exercise() -> None:
        producer_id = f"p-{uuid4()}"
        stream_a = f"stream-a-{uuid4()}"
        stream_b = f"stream-b-{uuid4()}"
        first_inserted[stream_a] = asyncio.Event()
        first_inserted[stream_b] = asyncio.Event()

        def resolved(stream_id: str, key: str) -> ResolvedEvent:
            return ResolvedEvent(
                draft=_draft(
                    stream_id=stream_id,
                    idempotency_key=f"{producer_id}-{key}",
                    producer_id=producer_id,
                )
            )

        pool_engine = create_async_engine(postgres_dsn, pool_size=2, max_overflow=0)
        try:
            factory = async_sessionmaker(pool_engine, expire_on_commit=False)

            async def run_batch(batch: list[ResolvedEvent]) -> list[object]:
                async with factory() as session:
                    stored = await LedgerRepository.append(session, batch)
                    await session.commit()
                    return list(stored)

            outcomes = await asyncio.wait_for(
                asyncio.gather(
                    run_batch([resolved(stream_a, "k1"), resolved(stream_a, "k2")]),
                    run_batch([resolved(stream_b, "k2"), resolved(stream_b, "k1")]),
                    return_exceptions=True,
                ),
                timeout=60,
            )

            committed = [outcome for outcome in outcomes if isinstance(outcome, list)]
            conflicts = [
                outcome for outcome in outcomes if isinstance(outcome, IdempotencyConflictError)
            ]
            assert len(committed) == 1, outcomes
            assert len(conflicts) == 1, outcomes

            async with factory() as session:
                streams = (
                    await session.scalars(
                        select(EventRow.stream_id).where(EventRow.producer_id == producer_id)
                    )
                ).all()
            assert len(streams) == 2
            assert len(set(streams)) == 1
        finally:
            await pool_engine.dispose()

    asyncio.run(exercise())
