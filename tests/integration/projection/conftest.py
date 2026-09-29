"""Shared fixtures and test doubles for the projection runtime's integration suite.

Mirrors `tests/integration/catalog/test_repository.py`'s module-scoped
`NullPool` engine pattern for PostgreSQL: `NullPool` opens a fresh physical
connection on every checkout, so sharing one `AsyncEngine` across many
independently-`asyncio.run()`-wrapped test functions never risks binding a
pooled connection to a stale event loop. Schema setup/teardown mirrors
`tests/integration/test_migrations.py`'s alembic upgrade/downgrade
try/finally pattern.

Neo4j resources deliberately stay per-test (built and closed inside each
test's own `asyncio.run()` body): no precedent in this repository proves
the async Neo4j driver is safe to share across separate `asyncio.run()`
calls the way `NullPool` makes sharing a Postgres `AsyncEngine` safe.

Fixtures here do NOT stub or pre-implement the ledger repository
(PLATFORM-020 owns that): `insert_ledger_event` writes directly into the
same `ledger.event_streams` / `ledger.events` rows `_load_event` reads
back from, using real SDK-sealed `StoredEventV1` instances built through
`agent_context_sdk.seal_event`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, LiteralString
from uuid import UUID, uuid4

import pytest
from agent_context_sdk import (
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content import ContentDisposition
from alembic.config import Config
from sqlalchemy import delete, event, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.db import session_factory
from agent_context_platform.ledger.models import EventRow, EventStreamRow, StreamStatus
from agent_context_platform.projection.models import (
    DeadLetterRow,
    OutboxRow,
    OutboxStatus,
    ProjectionCheckpointRow,
)
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.settings import Neo4jSettings
from alembic import command

PROJECT_ROOT = Path(__file__).parents[3]

_NEO4J_URI_VARIABLE = "AGENT_CONTEXT_TEST_NEO4J_URI"
_NEO4J_USERNAME_VARIABLE = "AGENT_CONTEXT_TEST_NEO4J_USERNAME"
_NEO4J_PASSWORD_VARIABLE = "AGENT_CONTEXT_TEST_NEO4J_PASSWORD"
_NEO4J_DATABASE_VARIABLE = "AGENT_CONTEXT_TEST_NEO4J_DATABASE"

TEST_NODE_LABEL = "ProjectionRuntimeTestNode"

_ALLOWED_SCOPED_ROLES = frozenset({"agent_context_projector"})

_CLEANUP_QUERY: LiteralString = (
    "MATCH (n:ProjectionRuntimeTestNode {run_id: $run_id}) DETACH DELETE n"
)
_DIGEST_QUERY: LiteralString = (
    "MATCH (n:ProjectionRuntimeTestNode {run_id: $run_id}) RETURN properties(n) AS props"
)
_MERGE_NODE_QUERY: LiteralString = (
    "MERGE (n:ProjectionRuntimeTestNode {event_id: $event_id}) "
    "SET n.run_id = $run_id, n.event_type = $event_type, "
    "n.stream_id = $stream_id, n.payload_marker = $payload_marker"
)


async def _run_alembic(connection: Any, operation: Callable[..., None], revision: str) -> None:
    def run(sync_connection: Any) -> None:
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.attributes["connection"] = sync_connection
        operation(config, revision)

    await connection.run_sync(run)


@pytest.fixture(scope="module")
def projection_engine(postgres_dsn: str) -> Iterator[AsyncEngine]:
    """Module-scoped Postgres engine with the full ledger/projection schema applied.

    Schema is fixed for this task (PLATFORM-024 may not add migrations):
    the whole `alembic upgrade head` is applied once per module, not a
    hand-picked subset, since the outbox/checkpoint/dead-letter tables and
    their role grants all come from the same migration.
    """
    engine = create_async_engine(postgres_dsn, poolclass=NullPool)

    async def upgrade() -> None:
        async with engine.connect() as connection:
            await _run_alembic(connection, command.upgrade, "head")
            await connection.commit()

    async def downgrade_and_dispose() -> None:
        try:
            async with engine.connect() as connection:
                await connection.rollback()
                await _run_alembic(connection, command.downgrade, "base")
                await connection.commit()
        finally:
            await engine.dispose()

    asyncio.run(upgrade())
    try:
        yield engine
    finally:
        asyncio.run(downgrade_and_dispose())


async def reset_projection_state(engine: AsyncEngine) -> None:
    """Wipe all ledger/projection rows between tests in this module.

    Safe because `projection_engine` owns a throwaway, module-exclusive
    database (`postgres_dsn` mints a fresh `agent_context_test_{uuid}`
    database per module) -- nothing outside this test module's own tests
    can observe or depend on rows left behind here. `ProjectionRunner`'s
    claim query has no per-test scoping mechanism of its own (it claims
    *any* available outbox row), so without this, one test's leftover
    pending/leased rows silently contaminate every later test in the
    same module.
    """
    async with engine.begin() as connection:
        await connection.execute(delete(DeadLetterRow))
        await connection.execute(delete(ProjectionCheckpointRow))
        await connection.execute(delete(OutboxRow))
        await connection.execute(
            update(EventStreamRow).values(
                last_event_id=None, last_event_sha256=None, last_sequence=0
            )
        )
        await connection.execute(delete(EventRow))
        await connection.execute(delete(EventStreamRow))


def _jsonable(value: object) -> object:
    """Coerce SDK values into plain JSON-native types for a JSONB column.

    `StoredEventV1` freezes its dict-typed fields (`context`, `payload`,
    `trace`) as read-only `mappingproxy` objects and nests real Pydantic
    submodels (`producer`, `redaction`) -- neither is directly acceptable
    to psycopg's JSON dumper, which wants a plain, mutable dict.
    """
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return dump(mode="json")
    if isinstance(value, Mapping):
        return dict(value)
    return value


def build_event(
    *,
    stream_id: str | None = None,
    event_type: str = "test.projection.happened",
    payload: dict[str, object] | None = None,
    producer_id: str = "projection-test-producer",
    occurred_at: datetime | None = None,
) -> StoredEventV1:
    """Build a real, SDK-sealed `StoredEventV1` for integration fixtures.

    Every event gets its own fresh `stream_id` (sequence 1, no previous
    hash) so ledger fixtures never need to satisfy multi-event stream
    head-advance invariants -- this suite tests the projection runtime,
    not the ledger append path (PLATFORM-020's territory).
    """
    moment = occurred_at or datetime.now(UTC)
    stream = stream_id or f"projection-test-{uuid4().hex}"
    draft = EventDraftV1(
        event_type=event_type,
        stream_id=stream,
        occurred_at=moment,
        observed_at=moment,
        producer=ProducerV1(
            producer_id=producer_id, name="projection-test-harness", version="1.0.0"
        ),
        payload=payload or {},
        redaction=EventRedactionSummaryV1(
            policy_version="test-policy-v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"{stream}-1-{uuid4().hex}",
    )
    return seal_event(draft, [], 1, None)


async def insert_ledger_event(
    session: AsyncSession, sealed: StoredEventV1, *, now: datetime
) -> None:
    """Write one SDK-sealed event into the same ledger rows `_load_event` reads.

    Respects the circular FK between `event_streams.last_event_id` and
    `events.event_id`: the stream row is created with no head, the event
    row is inserted referencing it, then the stream's head is advanced.
    """
    session.add(
        EventStreamRow(
            stream_id=sealed.stream_id,
            last_sequence=0,
            last_event_id=None,
            last_event_sha256=None,
            status=StreamStatus.ACTIVE,
            quarantined_at=None,
            created_at=now,
            updated_at=now,
        )
    )
    await session.flush()

    session.add(
        EventRow(
            event_id=sealed.event_id,
            event_type=sealed.event_type,
            schema_version=sealed.schema_version,
            stream_id=sealed.stream_id,
            stream_sequence=sealed.stream_sequence,
            producer_id=sealed.producer.producer_id,
            idempotency_key=sealed.idempotency_key,
            occurred_at=sealed.occurred_at,
            observed_at=sealed.observed_at,
            recorded_at=now,
            producer=_jsonable(sealed.producer),
            context=_jsonable(sealed.context) or {},
            trace=_jsonable(sealed.trace),
            payload=_jsonable(sealed.payload),
            redaction=_jsonable(sealed.redaction),
            payload_sha256=sealed.integrity.payload_sha256,
            previous_event_sha256=sealed.integrity.previous_event_sha256,
            event_sha256=sealed.integrity.event_sha256,
        )
    )
    await session.flush()

    await session.execute(
        update(EventStreamRow)
        .where(EventStreamRow.stream_id == sealed.stream_id)
        .values(
            last_sequence=sealed.stream_sequence,
            last_event_id=sealed.event_id,
            last_event_sha256=sealed.integrity.event_sha256,
            updated_at=now,
        )
    )


async def insert_outbox_row(session: AsyncSession, *, event_id: UUID, now: datetime) -> int:
    row = OutboxRow(
        event_id=event_id,
        status=OutboxStatus.PENDING,
        retry_count=0,
        available_at=now,
        lease_owner=None,
        lease_expires_at=None,
        last_error_class=None,
        delivered_at=None,
        dead_lettered_at=None,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    await session.flush()
    return row.outbox_id


async def seed_pending_event(
    session: AsyncSession, *, now: datetime, **event_kwargs: object
) -> tuple[StoredEventV1, int]:
    """Convenience: build + insert one ready-to-claim event and outbox row."""
    sealed = build_event(**event_kwargs)  # type: ignore[arg-type]
    await insert_ledger_event(session, sealed, now=now)
    outbox_id = await insert_outbox_row(session, event_id=sealed.event_id, now=now)
    return sealed, outbox_id


class MutableClock:
    """A manually-advanced, monotonic clock for deterministic lease/retry tests."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime.now(UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        if delta <= timedelta(0):
            raise ValueError("advance() must move the clock forward")
        self._now += delta


def role_scoped_engine(dsn: str, role: str) -> AsyncEngine:
    """Build an engine whose every connection runs `SET ROLE <role>` first.

    `role` is restricted to a closed allow-list rather than accepting
    arbitrary interpolated input. `NullPool` means every checkout is a
    fresh physical connection, so the connect listener fires every time.
    `SET ROLE` is committed as its own standalone transaction immediately
    after connecting -- before SQLAlchemy opens its own logical
    transaction on the connection -- so a later `ROLLBACK` on that
    connection can never silently revert it back to the login role.
    """
    if role not in _ALLOWED_SCOPED_ROLES:
        raise ValueError(f"role must be one of {sorted(_ALLOWED_SCOPED_ROLES)}")

    engine = create_async_engine(dsn, poolclass=NullPool)

    @event.listens_for(engine.sync_engine, "connect")
    def _set_role(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"SET ROLE {role}")
        finally:
            cursor.close()
        dbapi_connection.commit()

    return engine


def role_scoped_session_factory(dsn: str, role: str) -> Any:
    return session_factory(role_scoped_engine(dsn, role))


def neo4j_integration_settings() -> Neo4jSettings:
    uri = os.environ.get(_NEO4J_URI_VARIABLE)
    username = os.environ.get(_NEO4J_USERNAME_VARIABLE)
    password = os.environ.get(_NEO4J_PASSWORD_VARIABLE)
    if not uri or not username or not password:
        pytest.skip(f"{_NEO4J_URI_VARIABLE} and friends are required for this test")
    return Neo4jSettings.model_validate(
        {
            "uri": uri,
            "username": username,
            "password": password,
            "database": os.environ.get(_NEO4J_DATABASE_VARIABLE, "neo4j"),
        }
    )


def new_run_id() -> str:
    return uuid4().hex


async def cleanup_test_nodes(store: Neo4jStore, run_id: str) -> None:
    async def _delete(tx: Neo4jTransaction) -> None:
        await tx.run(_CLEANUP_QUERY, parameters={"run_id": run_id})

    await store.execute_write(_delete)


async def graph_digest(store: Neo4jStore, run_id: str) -> tuple[str, int]:
    """Hash sorted node properties for every test-labelled node in this run.

    Hashing `properties(n)` wholesale (rather than a hand-picked field
    tuple) means the digest notices *any* property drift between the two
    passes it is used to compare, not just the fields the test author
    thought to check.
    """

    async def _read(tx: Neo4jTransaction) -> list[dict[str, object]]:
        result = await tx.run(_DIGEST_QUERY, parameters={"run_id": run_id})
        return [dict(record["props"]) for record in result.records]

    records = await store.execute_read(_read)
    canonical = sorted(json.dumps(record, sort_keys=True, default=str) for record in records)
    digest = hashlib.sha256("\n".join(canonical).encode()).hexdigest()
    return digest, len(records)


class GraphNodeProjector:
    """Test-only projector: MERGEs one node per event, keyed by event_id.

    Idempotent by construction -- MERGE plus a deterministic SET of the
    same parameters always converges to the same end state for the same
    event, which is exactly the property crash-safety relies on: a worker
    that crashes after this commits but before the outbox/checkpoint
    finalize forces some other worker to replay the same event later.
    """

    def __init__(
        self,
        run_id: str,
        *,
        name: str = "test.graph_node",
        version: str = "1",
        event_type: str = "test.projection.happened",
    ) -> None:
        self.name = name
        self.version = version
        self._run_id = run_id
        self._event_type = event_type

    def handles(self, event_type: str) -> bool:
        return event_type == self._event_type

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        marker = event.payload.get("marker") if isinstance(event.payload, Mapping) else None
        await tx.run(
            _MERGE_NODE_QUERY,
            parameters={
                "event_id": str(event.event_id),
                "run_id": self._run_id,
                "event_type": event.event_type,
                "stream_id": event.stream_id,
                "payload_marker": marker,
            },
        )


class PoisonError(RuntimeError):
    """Deliberately sensitive-looking message that must never be persisted."""


class PoisonProjector:
    """Test-only projector that always raises, to exercise the DLQ path.

    Never touches `tx`: the outbox/dead-letter bookkeeping this exercises
    doesn't depend on Neo4j at all.
    """

    def __init__(
        self,
        *,
        message: str,
        name: str = "test.poison",
        version: str = "1",
        event_type: str = "test.poison.happened",
    ) -> None:
        self.name = name
        self.version = version
        self._event_type = event_type
        self._message = message

    def handles(self, event_type: str) -> bool:
        return event_type == self._event_type

    async def project(self, _tx: Neo4jTransaction, _event: StoredEventV1) -> None:
        raise PoisonError(self._message)


AsyncSessionFactory = Callable[[], AsyncSession]
WriteCallback = Callable[[Neo4jTransaction], Awaitable[object]]
