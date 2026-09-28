"""Append-only ledger repository: hash-chained, idempotent event persistence.

``LedgerRepository.append`` is the only way new events enter the ledger. It
allocates per-stream sequence numbers under row-level locks (acquired in
sorted ``stream_id`` order, so concurrent multi-stream batches never
deadlock against each other), seals each event through the SDK's
``seal_event`` (hash-chained against the stream's previous head), and is
idempotent on ``(producer_id, idempotency_key)``: replaying the exact same
draft -- whether resubmitted with the same ``event_id`` (e.g. concurrent
callers racing on one literal draft) or a fresh one (e.g. a retried call
that built a new draft after a timeout) -- returns the original stored
event without allocating a new sequence, while reusing a key for a
materially different draft raises ``IdempotencyConflictError``. The
repository never commits or rolls back the caller's transaction itself, so a
raised conflict aborts the whole batch once the caller rolls back.

Stored events carry content refs in canonical content_id order: before
sealing, ``append`` replaces a draft's ``content_claims`` with the same
claims sorted by ``content_id`` in Python code-point order (see
``_canonicalize_draft``). Producer-supplied claim order carries no semantic
meaning -- payloads address content by ``content_id``, and producers never
compute event hashes -- and ``ledger.event_content_refs`` has no ordinal
column, so canonicalizing order before sealing is what makes every stored
event exactly reconstructible from the database. Every read path
(``get_by_idempotency_keys``, idempotent-replay recovery, and
``_row_to_stored_event`` generally) re-sorts fetched content refs by
``content_id`` in Python, never via SQL ``ORDER BY``, since collation can
differ from Python's code-point order.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID as PythonUUID

from agent_context_sdk import (  # type: ignore[import-untyped]
    ContentRefV1,
    EventDraftV1,
    RedactionReportV1,
    StoredEventV1,
    seal_event,
)
from sqlalchemy import func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from agent_context_platform.ledger.models import (
    ContentDisposition,
    ContentStorage,
    EventContentRefRow,
    EventRow,
    EventStreamRow,
    MetadataOnlyReason,
    RedactionReportRow,
    StreamStatus,
)
from agent_context_platform.projection.models import OutboxRow, OutboxStatus

_IDEMPOTENCY_CONSTRAINT = "uq_events_producer_id"
_EVENT_PK_CONSTRAINT = "pk_events"


@dataclass(frozen=True, slots=True)
class ResolvedEvent:
    """One producer draft plus its server-resolved content, ready to append.

    ``redaction_reports`` carries the detailed per-content redaction report
    for any content ref that has one, keyed by ``content_id``; every key must
    also appear in ``content_refs``.
    """

    draft: EventDraftV1
    content_refs: tuple[ContentRefV1, ...] = ()
    redaction_reports: Mapping[str, RedactionReportV1] = field(default_factory=dict)


class IdempotencyConflictError(ValueError):
    """Raised when an idempotency key is reused by a materially different draft.

    Aborts the whole ``append`` batch: the repository never overwrites an
    existing event, and the caller is expected to roll back its transaction
    on this error.
    """

    def __init__(
        self, *, producer_id: str, idempotency_key: str, existing_event_id: PythonUUID
    ) -> None:
        super().__init__(
            "idempotency key already used by a different draft: "
            f"producer_id={producer_id!r} idempotency_key={idempotency_key!r} "
            f"existing_event_id={existing_event_id}"
        )
        self.producer_id = producer_id
        self.idempotency_key = idempotency_key
        self.existing_event_id = existing_event_id


@dataclass(slots=True)
class _StreamState:
    sequence: int
    head_event_id: PythonUUID | None
    head_sha256: str | None


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _canonicalize_draft(draft: EventDraftV1) -> EventDraftV1:
    """Return ``draft`` with ``content_claims`` sorted by ``content_id``.

    Sort key is plain Python string ``<`` (code-point order). Claim order
    carries no meaning: the payload addresses content by ``content_id``, not
    by position, and producers never compute event hashes.
    """
    ordered = tuple(sorted(draft.content_claims, key=lambda claim: claim.content_id))
    return draft.model_copy(update={"content_claims": ordered})


def _draft_identity(draft: EventDraftV1) -> dict[str, Any]:
    """Canonical, event_id-independent draft fingerprint for idempotency checks.

    Always canonicalizes ``content_claims`` first (see
    ``_canonicalize_draft``), so two submissions that differ only in claim
    order compare equal here; any other difference compares unequal.
    """
    canonical = _canonicalize_draft(draft)
    return cast(dict[str, Any], canonical.model_dump(mode="json", exclude={"event_id"}))


def _existing_draft_identity(
    event: EventRow, content_refs: Sequence[EventContentRefRow]
) -> dict[str, Any]:
    reconstructed = EventDraftV1.model_validate(
        {
            "event_type": event.event_type,
            "schema_version": event.schema_version,
            "stream_id": event.stream_id,
            "occurred_at": event.occurred_at.astimezone(UTC),
            "observed_at": event.observed_at.astimezone(UTC),
            "producer": event.producer,
            "context": event.context,
            "trace": event.trace,
            "payload": event.payload,
            "redaction": event.redaction,
            "idempotency_key": event.idempotency_key,
            "content_claims": [
                {
                    "content_id": ref.content_id,
                    "content_sha256": ref.content_sha256,
                    "media_type": ref.media_type,
                    "uncompressed_bytes": ref.uncompressed_bytes,
                }
                for ref in content_refs
            ],
        }
    )
    return _draft_identity(reconstructed)


def _plan_batch(resolved_events: Sequence[ResolvedEvent]) -> list[int]:
    """Resolve which batch position owns the real insert attempt for each index.

    Returns, for each index ``i``, the index that should actually be
    inserted: ``i`` itself for the first occurrence of its idempotency key in
    the batch, or an earlier index ``j < i`` when ``i`` repeats ``j``'s exact
    draft under the same key (comparison uses the canonicalized draft, so
    claim order never causes a false conflict). Raises
    ``IdempotencyConflictError`` immediately -- before touching the database
    -- when the same key is reused by a materially different draft within
    one batch.
    """
    representative: list[int] = []
    first_index_by_key: dict[tuple[str, str], int] = {}
    for i, resolved in enumerate(resolved_events):
        draft = resolved.draft
        key = (draft.producer.producer_id, draft.idempotency_key)
        first = first_index_by_key.get(key)
        if first is None:
            first_index_by_key[key] = i
            representative.append(i)
            continue
        if _draft_identity(draft) != _draft_identity(resolved_events[first].draft):
            raise IdempotencyConflictError(
                producer_id=key[0],
                idempotency_key=key[1],
                existing_event_id=resolved_events[first].draft.event_id,
            )
        representative.append(first)
    return representative


def _validate_resolved_event(resolved: ResolvedEvent) -> None:
    content_ids = {ref.content_id for ref in resolved.content_refs}
    unknown = set(resolved.redaction_reports) - content_ids
    if unknown:
        raise ValueError(
            "redaction_reports references content_id(s) not present in "
            f"content_refs: {sorted(unknown)}"
        )


def _violated_constraint_name(error: IntegrityError) -> str | None:
    diagnostics = getattr(error.orig, "diag", None)
    return getattr(diagnostics, "constraint_name", None)


def _content_ref_from_row(row: EventContentRefRow) -> dict[str, Any]:
    return {
        "content_id": row.content_id,
        "content_sha256": row.content_sha256,
        "media_type": row.media_type,
        "uncompressed_bytes": row.uncompressed_bytes,
        "disposition": row.disposition.value,
        "storage": row.storage.value,
        "inline_id": row.inline_id,
        "object_key": row.object_key,
        "encoding": row.encoding,
    }


def _row_to_stored_event(
    event: EventRow, content_refs: Sequence[EventContentRefRow]
) -> StoredEventV1:
    """Reconstruct a ``StoredEventV1`` from its stored rows.

    Content refs are always re-sorted by ``content_id`` here (Python
    code-point order via plain ``sorted()``, never SQL ``ORDER BY``),
    matching the canonical order every event was sealed in -- see the module
    docstring and ``_canonicalize_draft``.
    """
    ordered_refs = sorted(content_refs, key=lambda ref: ref.content_id)
    return StoredEventV1.model_validate(
        {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "schema_version": event.schema_version,
            "stream_id": event.stream_id,
            "occurred_at": event.occurred_at.astimezone(UTC),
            "observed_at": event.observed_at.astimezone(UTC),
            "producer": event.producer,
            "context": event.context,
            "trace": event.trace,
            "payload": event.payload,
            "redaction": event.redaction,
            "idempotency_key": event.idempotency_key,
            "stream_sequence": event.stream_sequence,
            "content_refs": [_content_ref_from_row(ref) for ref in ordered_refs],
            "integrity": {
                "payload_sha256": event.payload_sha256,
                "previous_event_sha256": event.previous_event_sha256,
                "event_sha256": event.event_sha256,
            },
        }
    )


def _event_row_from_sealed(sealed: StoredEventV1, *, recorded_at: datetime) -> EventRow:
    return EventRow(
        event_id=sealed.event_id,
        event_type=sealed.event_type,
        schema_version=sealed.schema_version,
        stream_id=sealed.stream_id,
        stream_sequence=sealed.stream_sequence,
        producer_id=sealed.producer.producer_id,
        idempotency_key=sealed.idempotency_key,
        occurred_at=sealed.occurred_at,
        observed_at=sealed.observed_at,
        recorded_at=recorded_at,
        producer=sealed.producer.model_dump(mode="json"),
        context=sealed.context.model_dump(mode="json"),
        trace=sealed.trace.model_dump(mode="json") if sealed.trace is not None else None,
        payload=sealed.model_dump(mode="json", include={"payload"})["payload"],
        redaction=sealed.redaction.model_dump(mode="json"),
        payload_sha256=sealed.integrity.payload_sha256,
        previous_event_sha256=sealed.integrity.previous_event_sha256,
        event_sha256=sealed.integrity.event_sha256,
    )


def _content_ref_rows_from_sealed(sealed: StoredEventV1) -> list[EventContentRefRow]:
    return [
        EventContentRefRow(
            event_id=sealed.event_id,
            content_id=ref.content_id,
            content_sha256=ref.content_sha256,
            media_type=ref.media_type,
            uncompressed_bytes=ref.uncompressed_bytes,
            disposition=ContentDisposition(ref.disposition.value),
            storage=ContentStorage(ref.storage.value),
            inline_id=ref.inline_id,
            object_key=ref.object_key,
            encoding=ref.encoding,
        )
        for ref in sealed.content_refs
    ]


def _redaction_report_rows(
    event_id: PythonUUID, reports: Mapping[str, RedactionReportV1]
) -> list[RedactionReportRow]:
    return [
        RedactionReportRow(
            event_id=event_id,
            content_id=content_id,
            policy_version=report.policy_version,
            disposition=ContentDisposition(report.disposition.value),
            findings=[
                {
                    "detector": finding.detector,
                    "detector_version": finding.detector_version,
                    "category": finding.category,
                    "count": finding.count,
                    "correlation_labels": list(finding.correlation_labels),
                }
                for finding in report.findings
            ],
            metadata_only_reason=(
                MetadataOnlyReason(report.metadata_only_reason.value)
                if report.metadata_only_reason is not None
                else None
            ),
            error_class=report.error_class,
            failed_detector=report.failed_detector,
            failed_detector_version=report.failed_detector_version,
        )
        for content_id, report in reports.items()
    ]


def _outbox_row(event_id: PythonUUID, *, now: datetime) -> OutboxRow:
    return OutboxRow(
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


async def _lock_streams(
    session: AsyncSession, stream_ids: Iterable[str]
) -> dict[str, _StreamState]:
    """Create-if-missing and row-lock every distinct stream, in sorted order.

    Locks are always acquired in sorted ``stream_id`` order across the whole
    batch, regardless of the events' original submission order, so that two
    concurrent multi-stream batches can never deadlock against each other.
    """
    state: dict[str, _StreamState] = {}
    for stream_id in sorted(set(stream_ids)):
        seed_time = _utcnow()
        await session.execute(
            pg_insert(EventStreamRow)
            .values(
                stream_id=stream_id,
                last_sequence=0,
                last_event_id=None,
                last_event_sha256=None,
                status=StreamStatus.ACTIVE,
                quarantined_at=None,
                created_at=seed_time,
                updated_at=seed_time,
            )
            .on_conflict_do_nothing(index_elements=[EventStreamRow.stream_id])
        )
        sequence, head_event_id, head_sha256 = (
            await session.execute(
                select(
                    EventStreamRow.last_sequence,
                    EventStreamRow.last_event_id,
                    EventStreamRow.last_event_sha256,
                )
                .where(EventStreamRow.stream_id == stream_id)
                .with_for_update()
            )
        ).one()
        state[stream_id] = _StreamState(sequence, head_event_id, head_sha256)
    return state


async def _recover_existing(
    session: AsyncSession, draft: EventDraftV1, *, event_id: PythonUUID | None = None
) -> StoredEventV1:
    """Recover the event that caused a unique/PK conflict and verify identity.

    Looks up by explicit ``event_id`` when the violated constraint was the
    ``events`` primary key -- a literal same-``event_id`` resubmission, as in
    the card's "100 coroutines submitting the same draft" scenario -- or by
    ``(producer_id, idempotency_key)`` otherwise, the general
    fresh-``event_id``-retry case. Either way, the recovered row's identity
    is compared against ``draft`` (via ``_draft_identity``, which always
    excludes ``event_id``) before it is trusted: a same-key-or-same-id row
    that does not match raises ``IdempotencyConflictError`` rather than being
    silently treated as a successful replay.
    """
    if event_id is not None:
        existing = (
            await session.execute(select(EventRow).where(EventRow.event_id == event_id))
        ).scalar_one()
    else:
        existing = (
            await session.execute(
                select(EventRow).where(
                    EventRow.producer_id == draft.producer.producer_id,
                    EventRow.idempotency_key == draft.idempotency_key,
                )
            )
        ).scalar_one()
    existing_refs = (
        (
            await session.execute(
                select(EventContentRefRow).where(EventContentRefRow.event_id == existing.event_id)
            )
        )
        .scalars()
        .all()
    )

    if _draft_identity(draft) != _existing_draft_identity(existing, existing_refs):
        raise IdempotencyConflictError(
            producer_id=draft.producer.producer_id,
            idempotency_key=draft.idempotency_key,
            existing_event_id=existing.event_id,
        )
    return _row_to_stored_event(existing, existing_refs)


async def _insert_or_recover(
    session: AsyncSession,
    resolved: ResolvedEvent,
    canonical_draft: EventDraftV1,
    stream_state: dict[str, _StreamState],
    touched_streams: set[str],
) -> StoredEventV1:
    state = stream_state[canonical_draft.stream_id]
    sequence = state.sequence + 1
    previous_hash = state.head_sha256

    sealed = seal_event(canonical_draft, resolved.content_refs, sequence, previous_hash)
    insertion_time = _utcnow()

    try:
        async with session.begin_nested():
            # Inserted and flushed in three stages, one per foreign-key
            # dependency level (events -> event_content_refs ->
            # redaction_reports; outbox only depends on events). Neither
            # EventRow nor its dependents declare an ORM ``relationship()``
            # (see the module docstring), so the unit of work has no
            # dependency edge to order these tables' INSERTs by -- a single
            # flush can legally send them in mapper-registration order,
            # which is not the same as foreign-key order.
            session.add(_event_row_from_sealed(sealed, recorded_at=insertion_time))
            await session.flush()

            session.add_all(_content_ref_rows_from_sealed(sealed))
            await session.flush()

            session.add_all(_redaction_report_rows(sealed.event_id, resolved.redaction_reports))
            session.add(_outbox_row(sealed.event_id, now=insertion_time))
            await session.flush()
    except IntegrityError as error:
        constraint = _violated_constraint_name(error)
        if constraint == _IDEMPOTENCY_CONSTRAINT:
            return await _recover_existing(session, canonical_draft)
        if constraint == _EVENT_PK_CONSTRAINT:
            return await _recover_existing(session, canonical_draft, event_id=sealed.event_id)
        raise

    state.sequence = sequence
    state.head_event_id = sealed.event_id
    state.head_sha256 = sealed.integrity.event_sha256
    touched_streams.add(canonical_draft.stream_id)
    return sealed


async def _update_stream_head(session: AsyncSession, stream_id: str, state: _StreamState) -> None:
    now = _utcnow()
    await session.execute(
        update(EventStreamRow)
        .where(EventStreamRow.stream_id == stream_id)
        .values(
            last_sequence=state.sequence,
            last_event_id=state.head_event_id,
            last_event_sha256=state.head_sha256,
            updated_at=func.greatest(EventStreamRow.updated_at, now),
        )
    )


class LedgerRepository:
    """Transaction-neutral, least-privilege-safe append-only ledger operations.

    Every operation only ``flush``es; callers own the transaction boundary
    (commit or rollback). ``append`` never partially commits a batch: any
    unrecovered failure propagates out of the (still open) transaction, so
    the whole batch is undone by the caller's rollback.
    """

    @staticmethod
    async def append(
        session: AsyncSession, resolved_events: Sequence[ResolvedEvent]
    ) -> list[StoredEventV1]:
        """Append events, allocating per-stream sequence numbers atomically.

        Stored events carry content refs in canonical content_id order:
        before sealing, each draft's ``content_claims`` are replaced with the
        same claims sorted by ``content_id`` (Python code-point order,
        ``_canonicalize_draft``). Producer claim order carries no meaning and
        is never part of the idempotency comparison.

        Idempotent per ``(producer_id, idempotency_key)``: replaying the
        exact same draft returns the original stored event and allocates no
        new sequence; reusing a key for a materially different draft raises
        ``IdempotencyConflictError``. Results are positional: ``results[i]``
        corresponds to ``resolved_events[i]``, including for batch-local and
        cross-call idempotent replays.
        """
        if not resolved_events:
            return []

        for resolved in resolved_events:
            _validate_resolved_event(resolved)

        plan = _plan_batch(resolved_events)

        stream_ids = {resolved.draft.stream_id for resolved in resolved_events}
        stream_state = await _lock_streams(session, stream_ids)

        touched_streams: set[str] = set()
        results: list[StoredEventV1 | None] = [None] * len(resolved_events)
        for i, resolved in enumerate(resolved_events):
            if plan[i] != i:
                results[i] = results[plan[i]]
                continue
            canonical_draft = _canonicalize_draft(resolved.draft)
            results[i] = await _insert_or_recover(
                session, resolved, canonical_draft, stream_state, touched_streams
            )

        for stream_id in touched_streams:
            await _update_stream_head(session, stream_id, stream_state[stream_id])

        return cast(list[StoredEventV1], results)

    @staticmethod
    async def get_by_idempotency_keys(
        session: AsyncSession, keys: Sequence[tuple[str, str]]
    ) -> dict[tuple[str, str], StoredEventV1]:
        """Look up already-appended events by ``(producer_id, idempotency_key)``.

        Pairs with no matching stored event are omitted from the result.
        Content refs are reconstructed in canonical content_id order (see the
        module docstring).
        """
        unique_keys = sorted(set(keys))
        if not unique_keys:
            return {}

        rows = (
            (
                await session.execute(
                    select(EventRow).where(
                        tuple_(EventRow.producer_id, EventRow.idempotency_key).in_(unique_keys)
                    )
                )
            )
            .scalars()
            .all()
        )
        if not rows:
            return {}

        event_ids = [row.event_id for row in rows]
        content_refs = (
            (
                await session.execute(
                    select(EventContentRefRow).where(EventContentRefRow.event_id.in_(event_ids))
                )
            )
            .scalars()
            .all()
        )
        refs_by_event: dict[PythonUUID, list[EventContentRefRow]] = {}
        for ref in content_refs:
            refs_by_event.setdefault(ref.event_id, []).append(ref)

        return {
            (row.producer_id, row.idempotency_key): _row_to_stored_event(
                row, refs_by_event.get(row.event_id, [])
            )
            for row in rows
        }
