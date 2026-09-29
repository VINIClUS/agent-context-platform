"""Crash-safe outbox-driven projection runtime."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID

from agent_context_sdk import StoredEventV1
from sqlalchemy import and_, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.ledger.models import EventContentRefRow, EventRow
from agent_context_platform.projection.checkpoints import CheckpointRepository
from agent_context_platform.projection.models import (
    DeadLetterRow,
    DeadLetterStatus,
    OutboxRow,
    OutboxStatus,
)
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction

Clock = Callable[[], datetime]
ClaimOutcome = Literal["delivered", "retried", "dead_lettered", "lost_lease"]

_UNATTRIBUTED_PROJECTOR_NAME = "<projection.runtime>"
_UNATTRIBUTED_PROJECTOR_VERSION = "<unattributed>"


class OrphanedOutboxRowError(LookupError):
    """A claimed outbox row's event_id has no matching ledger event.

    The outbox's `event_id` column carries a foreign key into
    `ledger.events`, so this should never happen in a healthy system. It is
    handled defensively (retried, then dead-lettered) rather than allowed to
    crash the whole run, so one corrupt row cannot block every other claimed
    row in the same batch.
    """


@runtime_checkable
class Projector(Protocol):
    """A single idempotent projection from the event ledger into the graph."""

    name: str
    version: str

    def handles(self, event_type: str) -> bool:
        """Return whether this projector participates in a given event type."""
        ...

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        """Apply one event's effect within an already-open Neo4j write transaction.

        Must be idempotent: replaying the same event against the same graph
        state must converge to the same result, since a crash between the
        Neo4j commit and the outbox/checkpoint finalize forces a replay.
        """
        ...


@dataclass(frozen=True, slots=True)
class ProjectionRunReport:
    """Outcome tally for one `ProjectionRunner.run_once` call."""

    claimed: int = 0
    delivered: int = 0
    retried: int = 0
    dead_lettered: int = 0
    lost_leases: int = 0


@dataclass(frozen=True, slots=True)
class ClaimedOutboxRow:
    """An immutable snapshot of one outbox row this worker currently leases.

    `_claim_batch` copies the handful of fields the rest of the pipeline
    needs out of the ORM row *before* committing the claiming transaction,
    rather than passing the `OutboxRow` instance itself across the later
    session boundaries in `_process_claimed_row` / `_finalize_success` /
    `_finalize_failure`. Those methods each open their own new session, so a
    live ORM instance loaded by a different, already-committed (and
    possibly expiring) session would risk a `DetachedInstanceError` the
    moment an unloaded attribute were touched -- a risk that depends on the
    caller's `session_factory` having `expire_on_commit=False`, which
    `ProjectionRunner` accepts no guarantee of. A plain frozen dataclass has
    no such session affinity.
    """

    outbox_id: int
    event_id: UUID
    retry_count: int
    lease_expires_at: datetime


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


async def _load_event(session: AsyncSession, event_id: UUID) -> StoredEventV1:
    """Reconstruct the SDK's canonical `StoredEventV1` from ledger rows.

    Uses the real SDK contract model rather than a hand-rolled projection
    dataclass, so downstream projectors observe exactly the same shape a
    producer sealed and the ledger stored.
    """
    event_row = await session.get(EventRow, event_id)
    if event_row is None:
        raise OrphanedOutboxRowError(f"ledger event missing for outbox claim: {event_id}")

    content_ref_rows = (
        await session.scalars(
            select(EventContentRefRow)
            .where(EventContentRefRow.event_id == event_id)
            .order_by(EventContentRefRow.content_id)
        )
    ).all()

    return StoredEventV1.model_validate(
        {
            "event_id": event_row.event_id,
            "event_type": event_row.event_type,
            "schema_version": event_row.schema_version,
            "stream_id": event_row.stream_id,
            "stream_sequence": event_row.stream_sequence,
            "occurred_at": _as_utc(event_row.occurred_at),
            "observed_at": _as_utc(event_row.observed_at),
            "producer": event_row.producer,
            "context": event_row.context,
            "trace": event_row.trace,
            "payload": event_row.payload,
            "redaction": event_row.redaction,
            "idempotency_key": event_row.idempotency_key,
            "content_refs": [
                {
                    "content_id": ref.content_id,
                    "content_sha256": ref.content_sha256,
                    "media_type": ref.media_type,
                    "uncompressed_bytes": ref.uncompressed_bytes,
                    "disposition": ref.disposition.value,
                    "storage": ref.storage.value,
                    "inline_id": ref.inline_id,
                    "object_key": ref.object_key,
                    "encoding": ref.encoding,
                }
                for ref in content_ref_rows
            ],
            "integrity": {
                "payload_sha256": event_row.payload_sha256,
                "previous_event_sha256": event_row.previous_event_sha256,
                "event_sha256": event_row.event_sha256,
            },
        }
    )


class ProjectionRunner:
    """Claims outbox rows and drives them through registered projectors.

    Crash safety comes from two things working together:

    * Claiming is lease-based (`FOR UPDATE SKIP LOCKED` + a `lease_expires_at`
      column), so a crashed worker's claimed rows become reclaimable once
      the lease expires rather than being stuck forever.
    * Every finalize step (`_finalize_success` / `_finalize_failure`) is
      fenced: its `UPDATE` only matches when the row is still leased to
      *this* worker under the *exact* lease it observed at claim time. A
      worker that crashed after committing to Neo4j but before finalizing,
      then wakes up and tries to finalize late, always loses that race once
      another worker has reclaimed the row -- its fenced update matches zero
      rows and is treated as a no-op (`lost_lease`), never clobbering the
      reclaiming worker's state.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        neo4j: Neo4jStore,
        projectors: Sequence[Projector],
        *,
        worker_id: str,
        lease_duration: timedelta = timedelta(seconds=30),
        max_attempts: int = 5,
        base_retry_delay: timedelta = timedelta(seconds=1),
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if base_retry_delay <= timedelta(0):
            raise ValueError("base_retry_delay must be positive")
        identities = [(projector.name, projector.version) for projector in projectors]
        if len(identities) != len(set(identities)):
            raise ValueError("projectors must have unique (name, version) pairs")

        self._session_factory = session_factory
        self._neo4j = neo4j
        self._projectors = tuple(projectors)
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._max_attempts = max_attempts
        self._base_retry_delay = base_retry_delay
        self._clock = clock

    async def run_once(self, limit: int) -> ProjectionRunReport:
        if limit < 1:
            raise ValueError("limit must be at least 1")

        claimed_rows = await self._claim_batch(limit)
        outcomes = {"delivered": 0, "retried": 0, "dead_lettered": 0, "lost_lease": 0}
        for row in claimed_rows:
            outcome = await self._process_claimed_row(row)
            outcomes[outcome] += 1

        return ProjectionRunReport(
            claimed=len(claimed_rows),
            delivered=outcomes["delivered"],
            retried=outcomes["retried"],
            dead_lettered=outcomes["dead_lettered"],
            lost_leases=outcomes["lost_lease"],
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("clock() must return a UTC-aware datetime")
        return value

    async def _claim_batch(self, limit: int) -> list[ClaimedOutboxRow]:
        now = self._now()
        lease_expires_at = now + self._lease_duration

        async with self._session_factory() as session:
            claimable = (
                select(OutboxRow.outbox_id)
                .where(
                    or_(
                        and_(
                            OutboxRow.status == OutboxStatus.PENDING,
                            OutboxRow.available_at <= now,
                        ),
                        and_(
                            OutboxRow.status == OutboxStatus.LEASED,
                            OutboxRow.lease_expires_at <= now,
                        ),
                    )
                )
                .order_by(OutboxRow.outbox_id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            claim_statement = (
                update(OutboxRow)
                .where(OutboxRow.outbox_id.in_(claimable))
                .values(
                    status=OutboxStatus.LEASED,
                    lease_owner=self._worker_id,
                    lease_expires_at=lease_expires_at,
                    updated_at=now,
                )
                .returning(OutboxRow)
            )
            claimed = await session.scalars(
                claim_statement.execution_options(populate_existing=True)
            )
            # Copy the fields the rest of the pipeline needs into plain,
            # session-independent `ClaimedOutboxRow` snapshots *before*
            # committing -- once committed, this session may expire or
            # detach these ORM instances, and downstream steps always run
            # against freshly opened sessions of their own.
            claimed_rows = [
                ClaimedOutboxRow(
                    outbox_id=row.outbox_id,
                    event_id=row.event_id,
                    retry_count=row.retry_count,
                    lease_expires_at=lease_expires_at,
                )
                for row in claimed.all()
            ]
            await session.commit()
            return sorted(claimed_rows, key=lambda row: row.outbox_id)

    async def _process_claimed_row(self, row: ClaimedOutboxRow) -> ClaimOutcome:
        async with self._session_factory() as session:
            try:
                event = await _load_event(session, row.event_id)
            except OrphanedOutboxRowError as error:
                return await self._finalize_failure(
                    row=row,
                    event_id=row.event_id,
                    failing_projector=None,
                    error=error,
                )

        matching = [
            projector for projector in self._projectors if projector.handles(event.event_type)
        ]
        if not matching:
            applied = await self._finalize_success(row=row, event=event, matching=())
            return "delivered" if applied else "lost_lease"

        failing_projector: Projector | None = None

        async def run_matching(tx: Neo4jTransaction) -> None:
            nonlocal failing_projector
            for projector in matching:
                failing_projector = projector
                await projector.project(tx, event)
            failing_projector = None

        try:
            await self._neo4j.execute_write(run_matching)
        except Exception as error:
            return await self._finalize_failure(
                row=row,
                event_id=event.event_id,
                failing_projector=failing_projector,
                error=error,
            )

        applied = await self._finalize_success(row=row, event=event, matching=matching)
        return "delivered" if applied else "lost_lease"

    async def _fenced_update(
        self,
        session: AsyncSession,
        *,
        outbox_id: int,
        claimed_lease_expires_at: datetime,
        values: dict[str, object],
    ) -> bool:
        statement = (
            update(OutboxRow)
            .where(
                OutboxRow.outbox_id == outbox_id,
                OutboxRow.status == OutboxStatus.LEASED,
                OutboxRow.lease_owner == self._worker_id,
                OutboxRow.lease_expires_at == claimed_lease_expires_at,
            )
            .values(**values)
            .returning(OutboxRow.outbox_id)
        )
        result = await session.execute(statement)
        return result.scalar_one_or_none() is not None

    async def _finalize_success(
        self,
        *,
        row: ClaimedOutboxRow,
        event: StoredEventV1,
        matching: Sequence[Projector],
    ) -> bool:
        now = self._now()
        async with self._session_factory() as session:
            applied = await self._fenced_update(
                session,
                outbox_id=row.outbox_id,
                claimed_lease_expires_at=row.lease_expires_at,
                values={
                    "status": OutboxStatus.DELIVERED,
                    "delivered_at": now,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "updated_at": now,
                },
            )
            if not applied:
                await session.rollback()
                return False

            for projector in matching:
                await CheckpointRepository.advance(
                    session,
                    projector_name=projector.name,
                    projector_version=projector.version,
                    outbox_id=row.outbox_id,
                    event_id=event.event_id,
                    now=now,
                )
            await session.commit()
            return True

    async def _finalize_failure(
        self,
        *,
        row: ClaimedOutboxRow,
        event_id: UUID,
        failing_projector: Projector | None,
        error: BaseException,
    ) -> ClaimOutcome:
        now = self._now()
        error_class = type(error).__name__
        new_retry_count = row.retry_count + 1

        async with self._session_factory() as session:
            if new_retry_count >= self._max_attempts:
                applied = await self._fenced_update(
                    session,
                    outbox_id=row.outbox_id,
                    claimed_lease_expires_at=row.lease_expires_at,
                    values={
                        "status": OutboxStatus.DEAD_LETTERED,
                        "retry_count": new_retry_count,
                        "dead_lettered_at": now,
                        "last_error_class": error_class,
                        "lease_owner": None,
                        "lease_expires_at": None,
                        "updated_at": now,
                    },
                )
                if not applied:
                    await session.rollback()
                    return "lost_lease"

                projector_name = (
                    failing_projector.name
                    if failing_projector is not None
                    else _UNATTRIBUTED_PROJECTOR_NAME
                )
                projector_version = (
                    failing_projector.version
                    if failing_projector is not None
                    else _UNATTRIBUTED_PROJECTOR_VERSION
                )
                dlq_insert = insert(DeadLetterRow).values(
                    outbox_id=row.outbox_id,
                    event_id=event_id,
                    projector_name=projector_name,
                    projector_version=projector_version,
                    attempts=new_retry_count,
                    error_class=error_class,
                    status=DeadLetterStatus.OPEN,
                    first_failed_at=now,
                    updated_at=now,
                )
                dlq_statement = dlq_insert.on_conflict_do_update(
                    index_elements=[DeadLetterRow.outbox_id],
                    set_={
                        "attempts": new_retry_count,
                        "error_class": error_class,
                        "status": DeadLetterStatus.OPEN,
                        "updated_at": now,
                    },
                )
                await session.execute(dlq_statement)
                await session.commit()
                return "dead_lettered"

            applied = await self._fenced_update(
                session,
                outbox_id=row.outbox_id,
                claimed_lease_expires_at=row.lease_expires_at,
                values={
                    "status": OutboxStatus.PENDING,
                    "retry_count": new_retry_count,
                    "available_at": now
                    + self._retry_delay(self._base_retry_delay, new_retry_count),
                    "last_error_class": error_class,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "updated_at": now,
                },
            )
            if not applied:
                await session.rollback()
                return "lost_lease"
            await session.commit()
            return "retried"

    @staticmethod
    def _retry_delay(base_delay: timedelta, retry_count: int) -> timedelta:
        multiplier: float = 2 ** (retry_count - 1)
        return timedelta(seconds=base_delay.total_seconds() * multiplier)
