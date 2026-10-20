"""Atomic batch ingestion: prepare content, then append events in one transaction.

Order of operations for one batch (authentication and SDK schema validation
happen before this module is entered):

1. ``ContentService.prepare`` validates and uploads content outside any
   database transaction. Uploaded objects that a later failure abandons are
   left for the orphan sweeper.
2. One database transaction runs ``ContentService.attach`` and then
   ``LedgerRepository.append`` and commits. Any failure rolls the whole batch
   back: no event, content reference, outbox row, inline content or catalog
   row from the batch survives.

Batch replay needs no ``projection.ingestion_batches`` row. Idempotency is per
event, ``(producer_id, idempotency_key)``, and a rejected batch commits
nothing, so resubmitting the same batch is always safe: events that were
already stored come back as ``existing`` and are never reinserted.

An event is reported ``accepted`` only when this transaction created it. The
repository says so explicitly (``LedgerRepository.append_with_outcome``): it
knows whether it inserted a row or recovered an earlier one, including when a
concurrent identical batch commits first and the per-key advisory locks turn the
race into a wait.

On any rejection or transient outage the events already stored are still
reported ``existing`` (a read-only lookup by ``(producer_id, idempotency_key)``
in its own short transaction); only the new events are rejected, with
``retryable=true`` for outages.

Every public failure is content-free: responses carry event IDs, a stable
``error_code`` and ``retryable``, never request content or exception text.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Any, Final
from uuid import UUID

from agent_context_sdk import (  # type: ignore[import-untyped, unused-ignore]
    AcceptedEventV1,
    EventDraftV1,
    IngestBatchRequestV1,
    IngestBatchResponseV1,
    RejectedEventV1,
    StoredEventV1,
)
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.blob_store import BlobStoreError
from agent_context_platform.content.service import (
    ContentService,
    ContentServiceError,
    PreparedContent,
)
from agent_context_platform.ledger.repository import (
    IdempotencyConflictError,
    LedgerRepository,
    ResolvedEvent,
    StreamQuarantinedError,
)
from agent_context_platform.operations.faults import fault_point

IDEMPOTENCY_CONFLICT: Final = "idempotency_conflict"
STREAM_QUARANTINED: Final = "stream_quarantined"
DUPLICATE_IDEMPOTENCY_KEY: Final = "duplicate_idempotency_key"
BATCH_REJECTED: Final = "batch_rejected"
SERVICE_UNAVAILABLE: Final = "service_unavailable"

_TRANSIENT_ERRORS: Final = (BlobStoreError, OperationalError, InterfaceError, PoolTimeoutError)

_TRANSIENT_RETRY_AFTER_SECONDS: Final = 5


@dataclass(frozen=True, slots=True)
class IngestOutcome:
    """The HTTP status, the SDK response body and an optional retry hint."""

    http_status: int
    response: IngestBatchResponseV1
    retry_after_seconds: int | None = None


class IngestionService:
    """Ingest one already-authenticated, schema-valid batch atomically."""

    def __init__(
        self, content: ContentService, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        self._content = content
        self._session_factory = session_factory

    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        if _has_duplicate_idempotency_key(batch.events):
            return await self._reject(batch, 422, DUPLICATE_IDEMPOTENCY_KEY)

        try:
            prepared = await self._content.prepare(batch.content_items)
            return await self._commit(batch, prepared)
        except ContentServiceError as error:
            return await self._reject(batch, 422, error.error_code)
        except IdempotencyConflictError as error:
            offenders = {
                event.event_id
                for event in batch.events
                if (event.producer.producer_id, event.idempotency_key)
                == (error.producer_id, error.idempotency_key)
            }
            return await self._reject(batch, 409, IDEMPOTENCY_CONFLICT, offenders)
        except StreamQuarantinedError as error:
            offenders = {
                event.event_id for event in batch.events if event.stream_id == error.stream_id
            }
            return await self._reject(batch, 409, STREAM_QUARANTINED, offenders)
        except _TRANSIENT_ERRORS:
            return await self._reject(
                batch,
                503,
                SERVICE_UNAVAILABLE,
                retryable=True,
                retry_after=_TRANSIENT_RETRY_AFTER_SECONDS,
            )

    async def _commit(
        self, batch: IngestBatchRequestV1, prepared: PreparedContent
    ) -> IngestOutcome:
        fault_point("ledger.before_db_transaction")
        async with self._session_factory() as session, session.begin():
            await self._content.attach(session, prepared)
            resolved = [_resolve(event, prepared) for event in batch.events]
            outcomes = await LedgerRepository.append_with_outcome(session, resolved)
            fault_point("ledger.before_commit")
        fault_point("ledger.after_commit_before_response")
        accepted = tuple(
            AcceptedEventV1(
                event_id=outcome.stored.event_id,
                status="accepted" if outcome.created else "existing",
                stream_sequence=outcome.stored.stream_sequence,
            )
            for outcome in outcomes
        )
        return IngestOutcome(200, IngestBatchResponseV1(batch_id=batch.batch_id, accepted=accepted))

    async def _reject(
        self,
        batch: IngestBatchRequestV1,
        http_status: int,
        error_code: str,
        offenders: Collection[UUID] | None = None,
        *,
        retryable: bool = False,
        retry_after: int | None = None,
    ) -> IngestOutcome:
        """Reject the whole batch; events already stored stay reported as ``existing``.

        ``offenders`` are the events the error is attributable to; the other new
        events are rejected as ``batch_rejected``. ``None`` means the error is
        not attributable to a single event (for example a content failure), so
        every new event carries ``error_code``.
        """
        existing = await self._existing_events(batch.events)
        accepted: list[AcceptedEventV1] = []
        rejected: list[RejectedEventV1] = []
        for event in batch.events:
            stored = existing.get(event.event_id)
            if stored is not None:
                accepted.append(
                    AcceptedEventV1(
                        event_id=stored.event_id,
                        status="existing",
                        stream_sequence=stored.stream_sequence,
                    )
                )
                continue
            code = (
                error_code if offenders is None or event.event_id in offenders else BATCH_REJECTED
            )
            rejected.append(
                RejectedEventV1(event_id=event.event_id, error_code=code, retryable=retryable)
            )
        return IngestOutcome(
            http_status,
            IngestBatchResponseV1(
                batch_id=batch.batch_id, accepted=tuple(accepted), rejected=tuple(rejected)
            ),
            retry_after_seconds=retry_after,
        )

    async def _existing_events(self, events: Sequence[EventDraftV1]) -> dict[UUID, StoredEventV1]:
        """Stored events identical to a submitted draft, keyed by the draft's ``event_id``.

        Informational and monotonic: a stored row never disappears, so a match
        stays true whatever races with this read. Best effort: if the read
        fails the batch is simply reported without ``existing`` entries.
        """
        keys = [(event.producer.producer_id, event.idempotency_key) for event in events]
        try:
            async with self._session_factory() as session:
                stored_by_key = await LedgerRepository.get_by_idempotency_keys(session, keys)
        except SQLAlchemyError:
            return {}
        matches: dict[UUID, StoredEventV1] = {}
        for event in events:
            stored = stored_by_key.get((event.producer.producer_id, event.idempotency_key))
            if stored is not None and _same_event(event, stored):
                matches[event.event_id] = stored
        return matches


def _has_duplicate_idempotency_key(events: Sequence[EventDraftV1]) -> bool:
    keys = [(event.producer.producer_id, event.idempotency_key) for event in events]
    return len(keys) != len(set(keys))


def _resolve(event: EventDraftV1, prepared: PreparedContent) -> ResolvedEvent:
    """Build the repository input from ``PreparedContent`` only; refs and reports are never re-derived."""
    content_refs = prepared.resolve(event.content_claims)
    return ResolvedEvent(
        draft=event,
        content_refs=content_refs,
        redaction_reports={
            ref.content_id: prepared.report_for(ref.content_id) for ref in content_refs
        },
    )


_IDENTITY_FIELDS: Final = frozenset(
    {
        "event_type",
        "schema_version",
        "stream_id",
        "occurred_at",
        "observed_at",
        "producer",
        "context",
        "trace",
        "payload",
        "redaction",
        "idempotency_key",
    }
)


def _same_event(draft: EventDraftV1, stored: StoredEventV1) -> bool:
    """Whether ``stored`` is a replay of ``draft``, ignoring ``event_id`` and claim order."""
    fields: dict[str, Any] = draft.model_dump(mode="json", include=set(_IDENTITY_FIELDS))
    stored_fields: dict[str, Any] = stored.model_dump(mode="json", include=set(_IDENTITY_FIELDS))
    claims = sorted(
        (claim.content_id, claim.content_sha256, claim.media_type, claim.uncompressed_bytes)
        for claim in draft.content_claims
    )
    refs = sorted(
        (ref.content_id, ref.content_sha256, ref.media_type, ref.uncompressed_bytes)
        for ref in stored.content_refs
    )
    return fields == stored_fields and claims == refs
