"""Integration tests proving cross-batch deadlock avoidance under load.

``LedgerRepository.append`` locks every stream touched by a batch in sorted
``stream_id`` order internally (see ``_lock_streams``), regardless of the
order events were submitted in. This test hammers two shared streams with
many concurrent batches submitted in *opposite* relative order and asserts
Postgres never reports a deadlock, and that both streams end up with
contiguous, gap-free sequences.
"""

from __future__ import annotations

import asyncio
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

from agent_context_platform.ledger.models import EventRow, EventStreamRow
from agent_context_platform.ledger.repository import LedgerRepository, ResolvedEvent

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
