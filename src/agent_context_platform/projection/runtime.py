"""Crash-safe outbox-driven projection runtime."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID

from agent_context_sdk import StoredEventV1, verify_event
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# Module import (not `from ... import`): ledger.repository imports projection.models,
# so binding the attribute at call time keeps either import order cycle-free.
import agent_context_platform.ledger.repository as ledger_repository
from agent_context_platform.operations.faults import fault_point
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
_PUBLIC_ERROR_CLASS = re.compile(r"^[A-Za-z][A-Za-z0-9_.]{0,127}$")
_FALLBACK_ERROR_CLASS = "UnnamedProjectionError"


def _public_error_class(error: BaseException) -> str:
    """Return a constraint-safe public error class for ``error``.

    Outbox and dead-letter rows only accept ``^[A-Za-z][A-Za-z0-9_.]{0,127}$``.
    A projector may raise an exception whose class name is non-ASCII, starts
    with an underscore or is too long; persisting it unchanged would fail the
    finalize transaction on every attempt and keep the poison row from ever
    reaching the dead-letter queue.
    """
    name = type(error).__name__
    if _PUBLIC_ERROR_CLASS.fullmatch(name):
        return name
    sanitized = re.sub(r"[^A-Za-z0-9_.]", "_", name).lstrip("_.0123456789")[:128]
    return sanitized if _PUBLIC_ERROR_CLASS.fullmatch(sanitized) else _FALLBACK_ERROR_CLASS


class OrphanedOutboxRowError(LookupError):
    """A claimed outbox row's event_id has no matching ledger event.

    The outbox's `event_id` column carries a foreign key into
    `ledger.events`, so this should never happen in a healthy system. It is
    handled defensively (retried, then dead-lettered) rather than allowed to
    crash the whole run, so one corrupt row cannot block every other claimed
    row in the same batch.
    """


class EventIntegrityError(ValueError):
    """A stored event no longer matches its own payload/envelope digests.

    Projection of the event is blocked: the failure goes through the bounded
    retries into the dead-letter queue, and the event never reaches the graph.
    Quarantining the stream is the integrity verifier's job (PLATFORM-039); the
    projector role cannot write stream state.
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
class OutboxBacklog:
    """Counts of the outbox rows that are not delivered: what a long-running worker reports as lag.

    `pending` includes rows still waiting out a retry backoff, which are not claimable yet.
    """

    pending: int = 0
    leased: int = 0
    dead_lettered: int = 0

    @property
    def outstanding(self) -> int:
        """Rows that are neither delivered nor dead-lettered: work that can still happen."""
        return self.pending + self.leased


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


async def _load_event(session: AsyncSession, event_id: UUID) -> StoredEventV1:
    """Reconstruct the SDK's canonical `StoredEventV1` through the ledger.

    Delegates to `LedgerRepository.get_event`, the ledger's single
    reconstruction path, which orders content refs exactly as the event was
    sealed (Python code-point order by `content_id`) instead of trusting SQL
    collation. Projectors therefore observe the same shape a producer sealed
    and the ledger stored.
    """
    event = await ledger_repository.LedgerRepository.get_event(session, event_id)
    if event is None:
        raise OrphanedOutboxRowError(f"ledger event missing for outbox claim: {event_id}")
    if not verify_event(event):
        raise EventIntegrityError(f"stored event failed integrity verification: {event_id}")
    return event


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
        max_retry_delay: timedelta = timedelta(hours=1),
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if len(worker_id) > 255:
            # `projection.outbox.lease_owner` is varchar(255): a longer id would
            # make every claim transaction fail.
            raise ValueError("worker_id must be at most 255 characters")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if base_retry_delay <= timedelta(0):
            raise ValueError("base_retry_delay must be positive")
        if max_retry_delay < base_retry_delay:
            raise ValueError("max_retry_delay must not be shorter than base_retry_delay")
        identities = [(projector.name, projector.version) for projector in projectors]
        for name, version in identities:
            # Checkpoint and dead-letter columns cap these at 255 and 64: an
            # identity that cannot be persisted would make every finalize fail
            # and the row replay forever instead of reaching a terminal state.
            if not name.strip() or len(name) > 255:
                raise ValueError("projector name must be 1-255 characters")
            if not version.strip() or len(version) > 64:
                raise ValueError("projector version must be 1-64 characters")
        if len(identities) != len(set(identities)):
            raise ValueError("projectors must have unique (name, version) pairs")

        self._session_factory = session_factory
        self._neo4j = neo4j
        self._projectors = tuple(projectors)
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._max_attempts = max_attempts
        self._base_retry_delay = base_retry_delay
        self._max_retry_delay = max_retry_delay
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

    async def backlog(self) -> OutboxBacklog:
        """Count the pending, leased and dead-lettered rows (content-free).

        Delivered rows, the bulk of the table, are never counted: the status filter is served by
        `ix_outbox_status` (status, available_at, outbox_id), so the cost follows the backlog and
        not the total number of events.
        """
        async with self._session_factory() as session:
            rows = await session.execute(
                select(OutboxRow.status, func.count())
                .where(
                    OutboxRow.status.in_(
                        (OutboxStatus.PENDING, OutboxStatus.LEASED, OutboxStatus.DEAD_LETTERED)
                    )
                )
                .group_by(OutboxRow.status)
            )
            counts = {OutboxStatus(status): int(count) for status, count in rows.all()}
        return OutboxBacklog(
            pending=counts.get(OutboxStatus.PENDING, 0),
            leased=counts.get(OutboxStatus.LEASED, 0),
            dead_lettered=counts.get(OutboxStatus.DEAD_LETTERED, 0),
        )

    async def release_leases(self) -> int:
        """Return every row still leased to this worker to `pending`; return how many.

        `run_once` finalizes every row it claims, so this only matters after a run was cut short
        (an unexpected error mid-batch): a worker shutting down calls it so that no lease outlives
        the process and the rows are claimable at once instead of after `lease_duration`. A
        release is not a failed attempt, so `retry_count` is left alone.
        """
        now = self._now()
        async with self._session_factory() as session:
            released = await session.execute(
                update(OutboxRow)
                .where(
                    OutboxRow.status == OutboxStatus.LEASED,
                    OutboxRow.lease_owner == self._worker_id,
                )
                .values(
                    status=OutboxStatus.PENDING,
                    lease_owner=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
                .returning(OutboxRow.outbox_id)
            )
            count = len(released.all())
            await session.commit()
            return count

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
        load_error: Exception | None = None
        async with self._session_factory() as session:
            try:
                event = await _load_event(session, row.event_id)
            except Exception as error:
                # Orphaned rows and events the current SDK model can no longer
                # validate are deterministic failures: count them against the
                # bounded retry budget instead of aborting the run and leaving
                # the row to be reclaimed forever without progress.
                load_error = error
        if load_error is not None:
            # Finalize only after the load session is released, so a failure
            # never holds one pooled connection while waiting for another.
            return await self._finalize_failure(
                row=row,
                event_id=row.event_id,
                failing_projector=None,
                error=load_error,
            )

        matching: list[Projector] = []
        for projector in self._projectors:
            try:
                handles = projector.handles(event.event_type)
            except Exception as error:
                # A projector that cannot classify this event is a poison
                # failure like any other: count it against the retry budget.
                return await self._finalize_failure(
                    row=row,
                    event_id=event.event_id,
                    failing_projector=projector,
                    error=error,
                )
            if handles:
                matching.append(projector)
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
            fault_point("projection.during_mutation")

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

            fault_point("projection.before_checkpoint")
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
        error_class = _public_error_class(error)
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
                        "projector_name": projector_name,
                        "projector_version": projector_version,
                        "attempts": new_retry_count,
                        "error_class": error_class,
                        "status": DeadLetterStatus.OPEN,
                        # Reopening a requeued or resolved dead letter must clear
                        # its resolution, or the status/resolution check fails.
                        "resolved_at": None,
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
                    + self._retry_delay(
                        self._base_retry_delay, new_retry_count, self._max_retry_delay
                    ),
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
    def _retry_delay(
        base_delay: timedelta, retry_count: int, max_delay: timedelta = timedelta(hours=1)
    ) -> timedelta:
        """Exponential backoff capped at ``max_delay``.

        The exponent is capped before the multiplication, so a large retry
        count can never overflow ``timedelta`` and leave a row stuck short of
        the dead-letter queue.
        """
        multiplier: float = 2 ** min(retry_count - 1, 62)
        seconds = min(base_delay.total_seconds() * multiplier, max_delay.total_seconds())
        return timedelta(seconds=seconds)
