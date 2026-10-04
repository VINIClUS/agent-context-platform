"""Crash the real projection worker at each documented fault point, then recover.

``agent-context projection run --once`` runs as a subprocess armed with one label from
``docs/operations/fault-injection.md`` and must die with exit code 137 there. The worker
that restarts without the label reclaims the abandoned lease and converges: the outbox row
is delivered once, the checkpoint advances once and the graph holds exactly one node for
the event, with no duplicate.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import LiteralString
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.db import session_factory
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction

from ..fault_harness import CRASH_EXIT_CODE, process_env, run_cli
from .conftest import neo4j_integration_settings, reset_projection_state, seed_pending_event

pytestmark = pytest.mark.integration

_COUNT_SESSIONS: LiteralString = "MATCH (s:Session {session_id: $sid}) RETURN count(s) AS n"
_DELETE_SESSIONS: LiteralString = "MATCH (s:Session {session_id: $sid}) DETACH DELETE s"


@dataclass(frozen=True)
class Case:
    label: str
    graph_nodes_after_crash: int


CASES = [
    # The write transaction never commits, so Neo4j rolls the mutation back.
    Case("projection.during_mutation", 0),
    # Neo4j committed; only the outbox/checkpoint finalize is lost, so the replay must merge.
    Case("projection.before_checkpoint", 1),
]


async def _session_nodes(store: Neo4jStore, session_id: str) -> int:
    async def count(tx: Neo4jTransaction) -> int:
        result = await tx.run(_COUNT_SESSIONS, parameters={"sid": session_id})
        return int(result.records[0]["n"])

    return await store.execute_read(count)


async def _expire_lease(engine: AsyncEngine, outbox_id: int) -> None:
    """Make the crashed worker's lease claimable now instead of after its 30 s duration."""
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE projection.outbox SET lease_expires_at = now() - interval '1 second' "
                "WHERE outbox_id = :id AND status = 'leased'"
            ),
            {"id": outbox_id},
        )


async def _outbox(engine: AsyncEngine, outbox_id: int) -> tuple[str, int]:
    async with engine.connect() as connection:
        status = await connection.scalar(
            text("SELECT status::text FROM projection.outbox WHERE outbox_id = :id"),
            {"id": outbox_id},
        )
        checkpointed = await connection.scalar(
            text("SELECT coalesce(sum(processed_count), 0) FROM projection.projection_checkpoints")
        )
    return str(status), int(checkpointed)


@pytest.mark.parametrize("case", CASES, ids=[case.label for case in CASES])
def test_crash_at_label_then_restart_converges(
    case: Case, projection_engine: AsyncEngine, postgres_dsn: str
) -> None:
    session_id = f"fault-session-{uuid4().hex}"
    settings = neo4j_integration_settings()
    args = ["projection", "run", "--once", "--max-seconds", "90"]

    async def seed() -> int:
        await reset_projection_state(projection_engine)
        async with session_factory(projection_engine)() as session:
            _sealed, outbox_id = await seed_pending_event(
                session,
                now=datetime.now(UTC),
                event_type="agent.session.started",
                payload={"source": "codex", "session_id": session_id},
            )
            await session.commit()
        return outbox_id

    async def inspect(outbox_id: int) -> tuple[tuple[str, int], int]:
        async with Neo4jStore(settings) as store:
            return await _outbox(projection_engine, outbox_id), await _session_nodes(
                store, session_id
            )

    async def cleanup() -> None:
        async with Neo4jStore(settings) as store:

            async def delete(tx: Neo4jTransaction) -> None:
                await tx.run(_DELETE_SESSIONS, parameters={"sid": session_id})

            await store.execute_write(delete)

    outbox_id = asyncio.run(seed())
    try:
        code, stderr = run_cli(args, process_env(postgres_dsn, crash_at=case.label))
        assert code == CRASH_EXIT_CODE, stderr
        assert f"fault_injected label={case.label}" in stderr

        (status, checkpointed), nodes = asyncio.run(inspect(outbox_id))
        assert status == "leased", "the lease is abandoned, never delivered"
        assert checkpointed == 0, "no checkpoint may advance past an unfinalized event"
        assert nodes == case.graph_nodes_after_crash

        asyncio.run(_expire_lease(projection_engine, outbox_id))
        code, stderr = run_cli(args, process_env(postgres_dsn))
        assert code == 0, stderr

        (status, checkpointed), nodes = asyncio.run(inspect(outbox_id))
        assert status == "delivered"
        assert checkpointed >= 1
        assert nodes == 1

        # Idempotent: another drain neither redelivers nor duplicates.
        code, stderr = run_cli(args, process_env(postgres_dsn))
        assert code == 0, stderr
        assert asyncio.run(inspect(outbox_id)) == ((status, checkpointed), 1)
    finally:
        asyncio.run(cleanup())
