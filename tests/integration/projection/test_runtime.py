"""Integration tests for `ProjectionRunner` against real PostgreSQL + Neo4j.

Covers: ordered leases, genuine `SKIP LOCKED` concurrency, exponential
retry backoff, poison-event dead-lettering without leaking payload or
exception-message content, monotonic checkpoint advancement (including
the null-starting-checkpoint case), two competing workers claiming
disjoint work, and running the whole pipeline under
`SET ROLE agent_context_projector`. Crash-safety across the
Neo4j-commit/finalize seam is proven separately in `test_crash_replay.py`.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import inspect, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.db import session_factory
from agent_context_platform.projection.checkpoints import CheckpointRepository
from agent_context_platform.projection.models import (
    DeadLetterRow,
    DeadLetterStatus,
    OutboxRow,
    OutboxStatus,
    ProjectionCheckpointRow,
    ProjectionState,
)
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.runtime import ProjectionRunner

from .conftest import (
    GraphNodeProjector,
    MutableClock,
    PoisonProjector,
    cleanup_test_nodes,
    graph_digest,
    neo4j_integration_settings,
    new_run_id,
    reset_projection_state,
    role_scoped_session_factory,
    seed_pending_event,
)

pytestmark = pytest.mark.integration


def _all_column_text(row: object) -> str:
    """Flatten every mapped column's value into one searchable string.

    Proves a leak-check holds against *every* persisted column, not just
    the hand-picked ones a narrower assertion might check.
    """
    mapper = inspect(row).mapper
    return " ".join(str(getattr(row, attr.key)) for attr in mapper.column_attrs)


def test_run_once_claims_projects_and_delivers_a_single_event(
    projection_engine: AsyncEngine,
) -> None:
    run_id = new_run_id()

    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()

        async with factory() as session:
            sealed, outbox_id = await seed_pending_event(session, now=clock())
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            try:
                projector = GraphNodeProjector(run_id)
                runner = ProjectionRunner(
                    factory, store, [projector], worker_id="worker-smoke", clock=clock
                )

                report = await runner.run_once(10)

                assert report.claimed == 1
                assert report.delivered == 1
                assert report.retried == 0
                assert report.dead_lettered == 0
                assert report.lost_leases == 0

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

                digest, count = await graph_digest(store, run_id)
                assert count == 1
                assert digest
            finally:
                await cleanup_test_nodes(store, run_id)

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_claim_batch_claims_the_oldest_pending_rows_first(
    projection_engine: AsyncEngine,
) -> None:
    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()

        outbox_ids: list[int] = []
        async with factory() as session:
            for _ in range(3):
                _, outbox_id = await seed_pending_event(session, now=clock())
                outbox_ids.append(outbox_id)
            await session.commit()

        runner = ProjectionRunner(
            factory,
            object(),  # type: ignore[arg-type]
            [],
            worker_id="worker-order",
            clock=clock,
        )

        claimed = await runner._claim_batch(2)

        assert [row.outbox_id for row in claimed] == sorted(outbox_ids)[:2]

        async with factory() as session:
            for claimed_row in claimed:
                db_row = await session.get(OutboxRow, claimed_row.outbox_id)
                assert db_row is not None
                assert db_row.status == OutboxStatus.LEASED
                assert db_row.lease_owner == "worker-order"
                assert db_row.lease_expires_at == clock() + timedelta(seconds=30)

            untouched = await session.get(OutboxRow, sorted(outbox_ids)[2])
            assert untouched is not None
            assert untouched.status == OutboxStatus.PENDING

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_claim_batch_skips_a_row_locked_by_another_transaction(
    projection_engine: AsyncEngine,
) -> None:
    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()

        async with factory() as session:
            _, outbox_id_a = await seed_pending_event(session, now=clock())
            _, outbox_id_b = await seed_pending_event(session, now=clock())
            await session.commit()

        locking_session = factory()
        try:
            await locking_session.execute(
                select(OutboxRow).where(OutboxRow.outbox_id == outbox_id_a).with_for_update()
            )

            runner = ProjectionRunner(
                factory,
                object(),  # type: ignore[arg-type]
                [],
                worker_id="worker-skip-locked",
                clock=clock,
            )
            claimed = await asyncio.wait_for(runner._claim_batch(10), timeout=5)

            claimed_ids = {row.outbox_id for row in claimed}
            assert outbox_id_a not in claimed_ids
            assert outbox_id_b in claimed_ids
        finally:
            await locking_session.rollback()
            await locking_session.close()

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_finalize_failure_backs_off_exponentially_then_dead_letters_without_leaking_the_message(
    projection_engine: AsyncEngine,
) -> None:
    # Opaque sentinels, not real-looking secrets: a credentialed-DSN-shaped
    # string here would look like a genuine leaked secret to scanners even
    # though it is test fixture data.
    sensitive_message = "SENTINEL-EXC-MSG-7f3a"
    sensitive_payload_marker = "SENTINEL-PAYLOAD-2c9e"

    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()
        base_delay = timedelta(seconds=1)

        async with factory() as session:
            sealed, outbox_id = await seed_pending_event(
                session,
                now=clock(),
                event_type="test.poison.happened",
                payload={"marker": sensitive_payload_marker},
            )
            await session.commit()

        # A real `Neo4jStore` is required here, not a placeholder: whenever
        # any projector matches, `_process_claimed_row` unconditionally
        # calls `self._neo4j.execute_write(...)` before running any
        # projector callback, regardless of whether that projector's
        # `project()` itself touches `tx`. `PoisonProjector` never writes,
        # but the write-transaction plumbing it runs inside of is real.
        async with Neo4jStore(neo4j_integration_settings()) as store:
            projector = PoisonProjector(message=sensitive_message)
            runner = ProjectionRunner(
                factory,
                store,
                [projector],
                worker_id="worker-poison",
                clock=clock,
                max_attempts=3,
                base_retry_delay=base_delay,
            )

            failure_time_1 = clock()
            report_1 = await runner.run_once(1)
            assert report_1.retried == 1
            assert report_1.dead_lettered == 0

            async with factory() as session:
                row = await session.get(OutboxRow, outbox_id)
                assert row is not None
                assert row.status == OutboxStatus.PENDING
                assert row.retry_count == 1
                assert row.available_at == failure_time_1 + base_delay
                assert row.last_error_class == "PoisonError"

            # Backoff must be *enforced* at claim time, not merely recorded
            # in `available_at`: with no clock advance, the row is still in
            # the future and must not be claimable yet.
            still_backing_off = await runner.run_once(1)
            assert still_backing_off.claimed == 0

            clock.advance(base_delay + timedelta(milliseconds=1))

            failure_time_2 = clock()
            report_2 = await runner.run_once(1)
            assert report_2.retried == 1
            assert report_2.dead_lettered == 0

            async with factory() as session:
                row = await session.get(OutboxRow, outbox_id)
                assert row is not None
                assert row.retry_count == 2
                assert row.available_at == failure_time_2 + base_delay * 2

            # Same proof at the 2x backoff step: a partial advance (past the
            # 1x window but short of the 2x window) must still not claim.
            clock.advance(base_delay + timedelta(milliseconds=1))
            partially_backed_off = await runner.run_once(1)
            assert partially_backed_off.claimed == 0

            clock.advance(base_delay)

            report_3 = await runner.run_once(1)
            assert report_3.retried == 0
            assert report_3.dead_lettered == 1

            async with factory() as session:
                row = await session.get(OutboxRow, outbox_id)
                assert row is not None
                assert row.status == OutboxStatus.DEAD_LETTERED
                assert row.retry_count == 3
                assert row.last_error_class == "PoisonError"

                dead_letter = await session.get(DeadLetterRow, outbox_id)
                assert dead_letter is not None
                assert dead_letter.event_id == sealed.event_id
                assert dead_letter.attempts == 3
                assert dead_letter.error_class == "PoisonError"
                assert dead_letter.status == DeadLetterStatus.OPEN
                assert dead_letter.projector_name == projector.name
                assert dead_letter.projector_version == projector.version

                # Scan *every* column of both rows -- proves the DLQ path
                # leaks neither the raw exception message nor the event
                # payload anywhere, not just in the hand-picked columns
                # above.
                leak_scan = f"{_all_column_text(row)} {_all_column_text(dead_letter)}"
                assert sensitive_message not in leak_scan
                assert sensitive_payload_marker not in leak_scan

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_finalize_success_advances_a_checkpoint_from_its_null_starting_state(
    projection_engine: AsyncEngine,
) -> None:
    run_id = new_run_id()

    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()
        projector = GraphNodeProjector(run_id, name="test.null_checkpoint", version="1")

        async with factory() as session:
            sealed, outbox_id = await seed_pending_event(session, now=clock())
            session.add(
                ProjectionCheckpointRow(
                    projector_name=projector.name,
                    projector_version=projector.version,
                    last_outbox_id=None,
                    last_event_id=None,
                    processed_count=0,
                    state=ProjectionState.ACTIVE,
                    updated_at=clock(),
                )
            )
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            try:
                runner = ProjectionRunner(
                    factory, store, [projector], worker_id="worker-null-checkpoint", clock=clock
                )
                report = await runner.run_once(1)
                assert report.delivered == 1

                async with factory() as session:
                    checkpoint = await CheckpointRepository.get(
                        session,
                        projector_name=projector.name,
                        projector_version=projector.version,
                    )
                    assert checkpoint is not None
                    assert checkpoint.processed_count == 1
                    assert checkpoint.last_outbox_id == outbox_id
                    assert checkpoint.last_event_id == sealed.event_id
            finally:
                await cleanup_test_nodes(store, run_id)

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_two_competing_workers_each_claim_and_deliver_distinct_events(
    projection_engine: AsyncEngine,
) -> None:
    run_id = new_run_id()
    total_events = 6

    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()

        event_ids = []
        async with factory() as session:
            for _ in range(total_events):
                sealed, _ = await seed_pending_event(session, now=clock())
                event_ids.append(sealed.event_id)
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            try:
                projector = GraphNodeProjector(run_id, name="test.competing_workers")
                worker_a = ProjectionRunner(
                    factory, store, [projector], worker_id="worker-competing-a", clock=clock
                )
                worker_b = ProjectionRunner(
                    factory, store, [projector], worker_id="worker-competing-b", clock=clock
                )

                # Each worker asks for exactly half: with `FOR UPDATE SKIP
                # LOCKED` claiming committed atomically inside one UPDATE
                # statement, and demand exactly matching supply (2 workers
                # x `per_worker` == `total_events`), the split is
                # deterministic -- whichever worker's claim commits first
                # takes the lowest `per_worker` rows, and the other worker
                # either skips their now-locked rows or (if it runs after
                # the first commits) never sees them as claimable at all,
                # so it always gets the complementary half. A limit of
                # `total_events` for both workers would let whichever one
                # claims first grab everything, proving nothing about
                # competition.
                per_worker = total_events // 2
                report_a, report_b = await asyncio.gather(
                    worker_a.run_once(per_worker), worker_b.run_once(per_worker)
                )

                assert report_a.claimed == per_worker
                assert report_b.claimed == per_worker
                assert report_a.delivered == per_worker
                assert report_b.delivered == per_worker
                assert report_a.lost_leases == 0
                assert report_b.lost_leases == 0

                async with factory() as session:
                    rows = (
                        await session.scalars(
                            select(OutboxRow).where(OutboxRow.event_id.in_(event_ids))
                        )
                    ).all()
                    assert len(rows) == total_events
                    assert all(row.status == OutboxStatus.DELIVERED for row in rows)
                    assert all(row.lease_owner is None for row in rows)

                    checkpoint = await CheckpointRepository.get(
                        session,
                        projector_name=projector.name,
                        projector_version=projector.version,
                    )
                    assert checkpoint is not None
                    assert checkpoint.processed_count == total_events

                digest, count = await graph_digest(store, run_id)
                assert count == total_events
                assert digest
            finally:
                await cleanup_test_nodes(store, run_id)

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_projection_runner_operates_correctly_under_set_role_agent_context_projector(
    projection_engine: AsyncEngine, postgres_dsn: str
) -> None:
    run_id = new_run_id()
    sensitive_message = "must-not-leak-under-set-role"

    async def exercise() -> None:
        role_factory = role_scoped_session_factory(postgres_dsn, "agent_context_projector")

        # Non-vacuous proof 1: the role switch genuinely took effect. If the
        # connecting test user were not a superuser (or SET ROLE silently
        # reverted), this would read back the login role instead.
        async with role_factory() as session:
            current_user = (await session.execute(text("SELECT current_user"))).scalar_one()
        assert current_user == "agent_context_projector"

        # Non-vacuous proof 2: a column this role was deliberately NOT
        # granted UPDATE on genuinely fails. Proves the grants are real
        # column-level restrictions, not merely "connects as superuser."
        async with role_factory() as session:
            with pytest.raises(DBAPIError) as exc_info:
                await session.execute(
                    update(OutboxRow)
                    .where(OutboxRow.outbox_id == -1)
                    .values(outbox_id=OutboxRow.outbox_id)
                )
                await session.commit()
            await session.rollback()
        message = str(exc_info.value).lower()
        assert "permission denied" in message or "insufficient" in message

        clock = MutableClock()
        plain_factory = session_factory(projection_engine)

        async with plain_factory() as session:
            _, success_outbox_id = await seed_pending_event(session, now=clock())
            _, poison_outbox_id = await seed_pending_event(
                session, now=clock(), event_type="test.poison.happened"
            )
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            try:
                success_projector = GraphNodeProjector(run_id, name="test.set_role.success")
                poison_projector = PoisonProjector(
                    message=sensitive_message, name="test.set_role.poison"
                )
                runner = ProjectionRunner(
                    role_factory,
                    store,
                    [success_projector, poison_projector],
                    worker_id="worker-set-role",
                    clock=clock,
                    max_attempts=1,
                )

                report = await runner.run_once(2)
                assert report.claimed == 2
                assert report.delivered == 1
                assert report.dead_lettered == 1

                async with plain_factory() as session:
                    success_row = await session.get(OutboxRow, success_outbox_id)
                    assert success_row is not None
                    assert success_row.status == OutboxStatus.DELIVERED

                    poison_row = await session.get(OutboxRow, poison_outbox_id)
                    assert poison_row is not None
                    assert poison_row.status == OutboxStatus.DEAD_LETTERED
                    assert poison_row.last_error_class == "PoisonError"

                    dead_letter = await session.get(DeadLetterRow, poison_outbox_id)
                    assert dead_letter is not None
                    assert dead_letter.error_class == "PoisonError"
                    assert sensitive_message not in dead_letter.error_class

                digest, count = await graph_digest(store, run_id)
                assert count == 1
                assert digest
            finally:
                await cleanup_test_nodes(store, run_id)

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())
