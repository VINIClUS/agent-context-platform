"""Projection telemetry against real PostgreSQL + Neo4j: events, durations, integrity, gauges.

Counts are attributed per projector with bounded labels (registered names, ``none``,
``unattributed`` or ``other``); spans carry event and outbox ids only. A poison projector's
exception message and the event payload are canaries that must never reach telemetry. The gauge
sampler runs as the least-privileged projector role.
"""

from __future__ import annotations

import asyncio
import secrets
from datetime import timedelta

import pytest
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncEngine
from unit.observability._capture import capture_telemetry

from agent_context_platform.db import session_factory
from agent_context_platform.ledger.models import EventRow
from agent_context_platform.observability import contract
from agent_context_platform.observability.projection import ProjectionSampler
from agent_context_platform.projection.models import OutboxRow
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.runtime import ProjectionRunner

from .conftest import (
    GraphNodeProjector,
    MutableClock,
    PoisonProjector,
    cleanup_test_nodes,
    neo4j_integration_settings,
    new_run_id,
    reset_projection_state,
    role_scoped_session_factory,
    seed_pending_event,
)

pytestmark = pytest.mark.integration


def test_projection_events_durations_and_spans_are_recorded_without_content(
    projection_engine: AsyncEngine,
) -> None:
    run_id = new_run_id()
    canary = f"CANARYprojection{secrets.token_hex(8)}"

    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()
        async with factory() as session:
            delivered, delivered_outbox = await seed_pending_event(
                session, now=clock(), payload={"marker": canary}
            )
            await seed_pending_event(session, now=clock(), event_type="test.unhandled.happened")
            poisoned, _ = await seed_pending_event(
                session, now=clock(), event_type="test.poison.happened", payload={"m": canary}
            )
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            try:
                runner = ProjectionRunner(
                    factory,
                    store,
                    [
                        # Registered names keep their label; the poison one is attributed too.
                        GraphNodeProjector(run_id, name="agent"),
                        PoisonProjector(message=f"secret {canary}", name="git"),
                    ],
                    worker_id="worker-telemetry",
                    max_attempts=2,
                    base_retry_delay=timedelta(seconds=1),
                    clock=clock,
                )
                with capture_telemetry() as telemetry:
                    first = await runner.run_once(10)
                    clock.advance(timedelta(seconds=5))
                    second = await runner.run_once(10)

                    assert (first.delivered, first.retried, second.dead_lettered) == (2, 1, 1)
                    events = telemetry.points(contract.PROJECTION_EVENTS)
                    assert events == {
                        (("projector", "agent"), ("result", "delivered")): 1,
                        (("projector", "none"), ("result", "delivered")): 1,
                        (("projector", "git"), ("result", "retried")): 1,
                        (("projector", "git"), ("result", "dead_lettered")): 1,
                    }
                    assert telemetry.value(contract.PROJECTION_DURATION, projector="agent") == 1

                    batches = telemetry.spans("agent_context.projection.batch")
                    assert [dict(span.attributes or {}) for span in batches] == [
                        {
                            "agent_context.claimed": 3,
                            "agent_context.delivered": 2,
                            "agent_context.retried": 1,
                            "agent_context.dead_lettered": 0,
                        },
                        {
                            "agent_context.claimed": 1,
                            "agent_context.delivered": 0,
                            "agent_context.retried": 0,
                            "agent_context.dead_lettered": 1,
                        },
                    ]
                    per_event = {
                        (span.attributes or {})["agent_context.event_id"]: span
                        for span in telemetry.spans("agent_context.projection.event")
                    }
                    ok = per_event[str(delivered.event_id)]
                    assert dict(ok.attributes or {}) == {
                        "agent_context.event_id": str(delivered.event_id),
                        "agent_context.outbox_id": delivered_outbox,
                        "agent_context.result": "delivered",
                    }
                    assert ok.parent is not None
                    assert ok.parent.span_id == batches[0].context.span_id
                    assert str(poisoned.event_id) in per_event
                    dump = telemetry.dump()
                    assert canary not in dump
                    assert "PoisonError" not in dump  # the class is persisted, not exported
            finally:
                await cleanup_test_nodes(store, run_id)

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_a_tampered_event_counts_one_digest_mismatch_per_detection(
    projection_engine: AsyncEngine,
) -> None:
    async def exercise() -> None:
        factory = session_factory(projection_engine)
        clock = MutableClock()
        async with factory() as session:
            sealed, _ = await seed_pending_event(session, now=clock(), payload={"v": 1})
            await session.commit()
        async with projection_engine.begin() as connection:
            await connection.execute(
                update(EventRow)
                .where(EventRow.event_id == sealed.event_id)
                .values(payload={"v": 2})
            )

        async with Neo4jStore(neo4j_integration_settings()) as store:
            runner = ProjectionRunner(
                factory, store, [GraphNodeProjector(new_run_id())], worker_id="w", clock=clock
            )
            with capture_telemetry() as telemetry:
                report = await runner.run_once(10)

                assert report.retried == 1
                assert telemetry.points(contract.LEDGER_INTEGRITY_FAILURES) == {
                    (("check", "digest_mismatch"),): 1
                }
                assert (
                    telemetry.value(
                        contract.PROJECTION_EVENTS, projector="unattributed", result="retried"
                    )
                    == 1
                )

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())


def test_the_sampler_feeds_the_gauges_as_the_projector_role(
    projection_engine: AsyncEngine, postgres_dsn: str
) -> None:
    async def exercise() -> None:
        owner = session_factory(projection_engine)
        clock = MutableClock()
        async with owner() as session:
            for _ in range(2):
                await seed_pending_event(session, now=clock() - timedelta(seconds=120))
            dead, _ = await seed_pending_event(
                session, now=clock(), event_type="test.poison.happened"
            )
            await session.commit()

        async with Neo4jStore(neo4j_integration_settings()) as store:
            runner = ProjectionRunner(
                owner,
                store,
                [PoisonProjector(message="x", name="git")],
                worker_id="w-dead",
                max_attempts=1,
                clock=clock,
            )
            # Only the poison event is claimable now; the two older rows stay outstanding.
            async with owner() as session:
                await session.execute(
                    update(OutboxRow)
                    .where(OutboxRow.event_id != dead.event_id)
                    .values(available_at=clock() + timedelta(hours=1))
                )
                await session.commit()
            assert (await runner.run_once(10)).dead_lettered == 1

        projector_sessions = role_scoped_session_factory(postgres_dsn, "agent_context_projector")
        sampler = ProjectionSampler(projector_sessions, ["git", "agent"], clock=clock)
        with capture_telemetry() as telemetry:
            assert await sampler.refresh() is True

            assert telemetry.value(contract.PROJECTION_OUTBOX_PENDING) == 2
            for projector in ("git", "agent"):
                lag = telemetry.value(contract.PROJECTION_LAG, projector=projector)
                assert 119 <= lag <= 121
            assert telemetry.points(contract.PROJECTION_DEAD_LETTERS) == {
                (("projector", "git"),): 1
            }

            async with owner() as session:
                await session.execute(text("DELETE FROM projection.dead_letters"))
                await session.execute(text("DELETE FROM projection.outbox"))
                await session.commit()
            assert await sampler.refresh() is True
            assert telemetry.value(contract.PROJECTION_OUTBOX_PENDING) == 0
            assert telemetry.value(contract.PROJECTION_LAG, projector="git") == 0

    async def run_with_cleanup() -> None:
        try:
            await exercise()
        finally:
            await reset_projection_state(projection_engine)

    asyncio.run(run_with_cleanup())
