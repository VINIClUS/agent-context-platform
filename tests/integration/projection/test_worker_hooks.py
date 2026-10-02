"""`ProjectionRunner.backlog` and `release_leases`, the hooks the projection worker uses (039D)."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.db import session_factory
from agent_context_platform.projection.models import OutboxRow, OutboxStatus
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.runtime import OutboxBacklog, ProjectionRunner

from .conftest import (
    MutableClock,
    neo4j_integration_settings,
    reset_projection_state,
    seed_pending_event,
)

pytestmark = pytest.mark.integration


def test_release_leases_returns_only_this_workers_rows_to_pending(
    projection_engine: AsyncEngine,
) -> None:
    async def exercise() -> None:
        await reset_projection_state(projection_engine)
        factory = session_factory(projection_engine)
        clock = MutableClock()
        async with factory() as session:
            ids = [(await seed_pending_event(session, now=clock()))[1] for _ in range(3)]
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            mine = ProjectionRunner(factory, store, [], worker_id="worker-mine", clock=clock)
            other = ProjectionRunner(factory, store, [], worker_id="worker-other", clock=clock)
            assert len(await mine._claim_batch(2)) == 2
            assert len(await other._claim_batch(1)) == 1
            assert await mine.backlog() == OutboxBacklog(leased=3)
            assert (await mine.backlog()).outstanding == 3

            assert await mine.release_leases() == 2
            assert await mine.release_leases() == 0  # idempotent
            assert await mine.backlog() == OutboxBacklog(pending=2, leased=1)

            async with factory() as session:
                rows = [await session.get(OutboxRow, outbox_id) for outbox_id in ids]
            released = [row for row in rows if row and row.status == OutboxStatus.PENDING]
            assert len(released) == 2
            assert all(row.lease_owner is None and row.lease_expires_at is None for row in released)
            assert all(row.retry_count == 0 for row in released)  # a release is not an attempt
            kept = [row for row in rows if row and row.status == OutboxStatus.LEASED]
            assert [row.lease_owner for row in kept] == ["worker-other"]

    asyncio.run(exercise())
