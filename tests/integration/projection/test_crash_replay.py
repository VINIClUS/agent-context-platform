"""Crash-safety proof: replay after a crash between Neo4j commit and finalize.

Simulates a worker process dying after its Neo4j write transaction commits
but before it finalizes the outbox row / advances the checkpoint, by
monkey-patching one worker's own `_finalize_success` to capture its
arguments and then raise instead of running. `runtime.py`'s
`_process_claimed_row` wraps only the Neo4j write in `except Exception` --
nothing wraps the `_finalize_success` call -- so the raised exception
propagates out of `run_once` exactly like an uncaught process crash would.
A second worker then reclaims the expired lease and replays the same
event for real; a graph-state digest (hashing every property of every
test-labelled node) proves the replay converges to the same state (MERGE
idempotency), and calling the crashed worker's real `_finalize_success`
afterward with its stale captured lease proves it is correctly fenced out
(returns `False`, does not double-count the checkpoint).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.db import session_factory
from agent_context_platform.projection.checkpoints import CheckpointRepository
from agent_context_platform.projection.models import OutboxRow, OutboxStatus
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.runtime import ProjectionRunner

from .conftest import (
    GraphNodeProjector,
    MutableClock,
    cleanup_test_nodes,
    graph_digest,
    neo4j_integration_settings,
    new_run_id,
    reset_projection_state,
    seed_pending_event,
)

pytestmark = pytest.mark.integration


class _SimulatedCrash(BaseException):
    """Deliberately not an `Exception`.

    `runtime.py`'s `_process_claimed_row` only wraps the Neo4j write in
    `except Exception`; nothing wraps the `_finalize_success` call. A real
    process crash (SIGKILL) is not a catchable `Exception` either --
    subclassing `BaseException` directly keeps this simulation from ever
    being swallowed by the runtime's own error handling, so it must
    propagate all the way out of `run_once`, matching a real crash.
    """


def test_replay_after_a_crash_between_neo4j_commit_and_finalize_converges(
    projection_engine: AsyncEngine,
) -> None:
    run_id = new_run_id()

    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()
        lease_duration = timedelta(seconds=5)

        async with factory() as session:
            sealed, outbox_id = await seed_pending_event(session, now=clock())
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            try:
                projector = GraphNodeProjector(run_id, name="test.crash_replay")
                worker_a = ProjectionRunner(
                    factory,
                    store,
                    [projector],
                    worker_id="worker-a-crash",
                    clock=clock,
                    lease_duration=lease_duration,
                )

                captured: dict[str, object] = {}

                async def crashing_finalize(
                    *, row: object, event: object, matching: object
                ) -> bool:
                    captured["row"] = row
                    captured["event"] = event
                    captured["matching"] = matching
                    raise _SimulatedCrash("simulated crash after neo4j commit, before finalize")

                worker_a._finalize_success = crashing_finalize  # type: ignore[method-assign]

                with pytest.raises(_SimulatedCrash):
                    await worker_a.run_once(1)

                assert "row" in captured, (
                    "crash must happen after the neo4j write, not before claiming"
                )
                captured_event = captured["event"]
                assert captured_event.event_id == sealed.event_id  # type: ignore[attr-defined]

                digest_after_crash, count_after_crash = await graph_digest(store, run_id)
                assert count_after_crash == 1

                # The outbox row is still LEASED to worker_a -- finalize never
                # ran. Advance the clock past the lease so worker_b can
                # legitimately reclaim it, exactly like a real operator's
                # replacement worker would after the lease timeout elapses.
                clock.advance(lease_duration + timedelta(seconds=1))

                worker_b = ProjectionRunner(
                    factory,
                    store,
                    [projector],
                    worker_id="worker-b-replay",
                    clock=clock,
                    lease_duration=lease_duration,
                )
                report_b = await worker_b.run_once(1)

                assert report_b.claimed == 1
                assert report_b.delivered == 1
                assert report_b.lost_leases == 0

                digest_after_replay, count_after_replay = await graph_digest(store, run_id)
                assert count_after_replay == 1
                assert digest_after_replay == digest_after_crash

                async with factory() as session:
                    row = await session.get(OutboxRow, outbox_id)
                    assert row is not None
                    assert row.status == OutboxStatus.DELIVERED
                    assert row.lease_owner is None

                    checkpoint = await CheckpointRepository.get(
                        session,
                        projector_name=projector.name,
                        projector_version=projector.version,
                    )
                    assert checkpoint is not None
                    assert checkpoint.processed_count == 1
                    assert checkpoint.last_outbox_id == outbox_id
                    assert checkpoint.last_event_id == sealed.event_id

                # The crashed worker's real finalize, replayed late with its
                # stale captured lease, must be fenced out -- it lost the
                # race to worker_b and must not double-count anything.
                applied = await ProjectionRunner._finalize_success(
                    worker_a,
                    row=captured["row"],
                    event=captured["event"],
                    matching=captured["matching"],
                )
                assert applied is False

                async with factory() as session:
                    checkpoint_after = await CheckpointRepository.get(
                        session,
                        projector_name=projector.name,
                        projector_version=projector.version,
                    )
                    assert checkpoint_after is not None
                    assert checkpoint_after.processed_count == 1
            finally:
                await cleanup_test_nodes(store, run_id)

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())
