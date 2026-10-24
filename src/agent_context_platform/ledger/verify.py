"""Read-only integrity verification of the event ledger (PLATFORM-056).

`verify_ledger` walks every stream (or the requested ones) in `stream_sequence` order and checks,
for each stored event, exactly what sealing guaranteed (design spec section 6.2):

1. `sequence`: sequences are contiguous from 1 (or from `since_sequence`), with no gap and no
   duplicate.
2. `previous_event_sha256`: the stored link is null for the first event and otherwise equals the
   previous event's STORED `event_sha256` (the chain as it was sealed).
3. `event_decode`: the rows rebuild into a valid `StoredEventV1` through the repository's own
   reconstruction (`_row_to_stored_event`, content refs in canonical order).
4. `payload_sha256`: recomputed from the stored payload with the SDK's seal-time functions.
5. `event_sha256`: recomputed from the canonical envelope (RFC 8785 JCS, `integrity.event_sha256`
   excluded) with the SDK's seal-time function.

and, once a stream's events are read, `stream_head`: `ledger.event_streams` names the last event
(its sequence, ID and digest). A requested stream that does not exist fails `stream_exists`.

The canonicalization is never reimplemented: `_serialized_payload` and `_event_sha256` are the
functions `seal_event` itself uses (the SDK is pinned), so a verifier and a sealer can never
disagree about the bytes. A middle event whose `event_sha256` was replaced, with the next event's
link repaired to match, is caught by check 5 on the edited event.

Purge (design spec section 16) appends a `content.purged` tombstone and deletes stored bytes; it
never rewrites an event or its content refs, which keep their digest metadata. The verifier reads
only the sealed envelope, never content bytes, so purged content verifies like any other.

Bounded and read-only. The whole run is one `REPEATABLE READ, READ ONLY` transaction (a
consistent snapshot; PostgreSQL refuses any write). Streams are paged by keyset, and each stream's
events are streamed through a server-side cursor in batches of `batch_size`, with their content
refs read per batch, so memory holds one batch and one result line per stream, never the ledger.
The first failure of a stream ends its walk (later checks would only repeat it). Output carries
IDs, sequences, check names and digests, never payload or content.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from agent_context_sdk.canonical import canonical_json_bytes, sha256_hex
from agent_context_sdk.events.envelope import _event_sha256, _serialized_payload
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.ledger.models import EventContentRefRow, EventRow, EventStreamRow
from agent_context_platform.ledger.repository import _row_to_stored_event

DEFAULT_BATCH_SIZE: Final = 500
STREAM_PAGE_SIZE: Final = 1000
CHECKS: Final = (
    "stream_exists",
    "sequence",
    "previous_event_sha256",
    "event_decode",
    "payload_sha256",
    "event_sha256",
    "stream_head",
)
_READ_ONLY: Final = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"


@dataclass(frozen=True, slots=True)
class IntegrityFailure:
    """The first check a stream failed; digests and sequences only, never content."""

    check: str
    stream_sequence: int | None = None
    event_id: str | None = None
    expected: str | None = None
    actual: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "stream_sequence": self.stream_sequence,
            "event_id": self.event_id,
            "expected": self.expected,
            "actual": self.actual,
        }


@dataclass(slots=True)
class StreamVerification:
    stream_id: str
    events: int = 0
    first_sequence: int | None = None
    last_sequence: int | None = None
    failure: IntegrityFailure | None = None

    @property
    def ok(self) -> bool:
        return self.failure is None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "ok": self.ok,
            "events": self.events,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "failure": None if self.failure is None else self.failure.to_dict(),
        }


@dataclass(slots=True)
class LedgerVerification:
    streams: list[StreamVerification] = field(default_factory=list)
    batches: int = 0  # event batches fetched (bounded-memory evidence; not part of the output)

    @property
    def ok(self) -> bool:
        return all(stream.ok for stream in self.streams)

    def to_dict(self) -> dict[str, Any]:
        """The stable machine-readable report (`ledger verify --json`)."""
        return {
            "ok": self.ok,
            "checks": list(CHECKS),
            "totals": {
                "streams": len(self.streams),
                "streams_failed": sum(1 for stream in self.streams if not stream.ok),
                "events": sum(stream.events for stream in self.streams),
            },
            "streams": [stream.to_dict() for stream in self.streams],
        }


async def verify_ledger(
    sessions: async_sessionmaker[AsyncSession],
    *,
    streams: Sequence[str] = (),
    since_sequence: int = 1,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> LedgerVerification:
    """Verify every stream (or `streams`) from `since_sequence` on; read-only, one snapshot."""
    if since_sequence < 1:
        raise ValueError("since_sequence must be at least 1")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    report = LedgerVerification()
    async with sessions() as session, session.begin():
        await session.execute(text(_READ_ONLY))
        if streams:
            for stream_id in sorted(set(streams)):
                head = await session.get(EventStreamRow, stream_id)
                if head is None:
                    report.streams.append(
                        StreamVerification(stream_id, failure=IntegrityFailure("stream_exists"))
                    )
                    continue
                report.streams.append(
                    await _verify_stream(session, head, since_sequence, batch_size, report)
                )
        else:
            after = ""
            while True:
                page = (
                    await session.scalars(
                        select(EventStreamRow)
                        .where(EventStreamRow.stream_id > after)
                        .order_by(EventStreamRow.stream_id)
                        .limit(STREAM_PAGE_SIZE)
                    )
                ).all()
                if not page:
                    break
                for head in page:
                    report.streams.append(
                        await _verify_stream(session, head, since_sequence, batch_size, report)
                    )
                    session.expunge(head)
                after = page[-1].stream_id
    return report


async def _verify_stream(
    session: AsyncSession,
    head: EventStreamRow,
    since_sequence: int,
    batch_size: int,
    report: LedgerVerification,
) -> StreamVerification:
    result = StreamVerification(head.stream_id)
    expected_sequence = since_sequence
    previous_digest: str | None = None
    if since_sequence > 1:
        previous_digest = await session.scalar(
            select(EventRow.event_sha256).where(
                EventRow.stream_id == head.stream_id,
                EventRow.stream_sequence == since_sequence - 1,
            )
        )
        if previous_digest is None:
            later = await session.scalar(
                select(EventRow.stream_sequence)
                .where(
                    EventRow.stream_id == head.stream_id,
                    EventRow.stream_sequence >= since_sequence,
                )
                .order_by(EventRow.stream_sequence)
                .limit(1)
            )
            if later is None:  # a stream that ends before the range: only its head to check
                result.failure = _check_head(head, None, since_sequence)
                return result
            result.failure = IntegrityFailure(
                "sequence",
                stream_sequence=later,
                expected=str(since_sequence - 1),
                actual=str(later),
            )
            return result
    last: EventRow | None = None
    rows = await session.stream_scalars(
        select(EventRow)
        .where(EventRow.stream_id == head.stream_id, EventRow.stream_sequence >= since_sequence)
        .order_by(EventRow.stream_sequence)
        .execution_options(yield_per=batch_size)
    )
    try:
        async for batch in rows.partitions(batch_size):
            report.batches += 1
            refs = await _content_refs(session, [row.event_id for row in batch])
            for row in batch:
                failure = _check_event(row, expected_sequence, previous_digest, refs)
                if failure is not None:
                    result.failure = failure
                    return result
                if result.first_sequence is None:
                    result.first_sequence = row.stream_sequence
                result.last_sequence = row.stream_sequence
                result.events += 1
                expected_sequence = row.stream_sequence + 1
                previous_digest = row.event_sha256
                last = row
            for row in batch:
                session.expunge(row)
    finally:
        await rows.close()
    result.failure = _check_head(head, last, since_sequence)
    return result


async def _content_refs(
    session: AsyncSession, event_ids: Sequence[Any]
) -> dict[Any, list[EventContentRefRow]]:
    found: dict[Any, list[EventContentRefRow]] = {event_id: [] for event_id in event_ids}
    for ref in await session.scalars(
        select(EventContentRefRow).where(EventContentRefRow.event_id.in_(event_ids))
    ):
        found[ref.event_id].append(ref)
        session.expunge(ref)
    return found


def _check_event(
    row: EventRow,
    expected_sequence: int,
    previous_digest: str | None,
    refs: dict[Any, list[EventContentRefRow]],
) -> IntegrityFailure | None:
    event_id = str(row.event_id)

    def fail(check: str, expected: object, actual: object) -> IntegrityFailure:
        return IntegrityFailure(
            check,
            stream_sequence=row.stream_sequence,
            event_id=event_id,
            expected=None if expected is None else str(expected),
            actual=None if actual is None else str(actual),
        )

    if row.stream_sequence != expected_sequence:
        return fail("sequence", expected_sequence, row.stream_sequence)
    if row.previous_event_sha256 != previous_digest:
        return fail("previous_event_sha256", previous_digest, row.previous_event_sha256)
    try:
        event = _row_to_stored_event(row, refs.get(row.event_id, []))
    except (ValidationError, ValueError, TypeError):
        return fail("event_decode", None, None)
    payload_digest = sha256_hex(canonical_json_bytes(_serialized_payload(event)))
    if payload_digest != row.payload_sha256:
        return fail("payload_sha256", payload_digest, row.payload_sha256)
    event_digest = _event_sha256(event)
    if event_digest != row.event_sha256:
        return fail("event_sha256", event_digest, row.event_sha256)
    return None


def _check_head(
    head: EventStreamRow, last: EventRow | None, since_sequence: int
) -> IntegrityFailure | None:
    """The stream head names the last event read (or none, for an empty stream)."""
    if last is None:
        if since_sequence == 1 and head.last_sequence != 0:
            return IntegrityFailure("stream_head", expected="0", actual=str(head.last_sequence))
        if since_sequence > 1 and head.last_sequence >= since_sequence:
            return IntegrityFailure(
                "stream_head",
                stream_sequence=since_sequence,
                expected=str(since_sequence - 1),
                actual=str(head.last_sequence),
            )
        return None
    if head.last_sequence != last.stream_sequence:
        return IntegrityFailure(
            "stream_head",
            stream_sequence=last.stream_sequence,
            event_id=str(last.event_id),
            expected=str(last.stream_sequence),
            actual=str(head.last_sequence),
        )
    if head.last_event_id != last.event_id or head.last_event_sha256 != last.event_sha256:
        return IntegrityFailure(
            "stream_head",
            stream_sequence=last.stream_sequence,
            event_id=str(last.event_id),
            expected=last.event_sha256,
            actual=head.last_event_sha256,
        )
    return None
