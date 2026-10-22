"""Ledger-backed evidence for context items (PLATFORM-043): references, snippets, ranking.

Every `EvidenceRefV1` a context package cites is built here from a REAL stored event, loaded through
`_row_to_stored_event` (the reconstruction `LedgerRepository.get_event` uses), in batches, and integrity-checked with
the SDK's `verify_event`; an event that is missing, not yet delivered by the projection runner
(pending or dead-lettered), fails verification, has a non-UUIDv7 ID or lies outside the request
scope is never cited.

Content reference. `EvidenceRefV1.content_ref` is required by the SDK. When the cited event carries
content (an `event_content_refs` row that is `sanitized` and not purged: the one the item is about,
else the event's first by `content_id`), that real `ContentRefV1` is cited. Otherwise (code-index, run and failure events
carry no content; purged content is never pointed at) the event itself is cited in ONE
metadata-only form, documented in `docs/data/context-evidence.md`:

- `content_id = "event:<event_id>"`, `disposition = metadata_only`, `storage = inline`,
  `inline_id = <event_id>`, `media_type = EVENT_METADATA_MEDIA_TYPE`;
- `content_sha256` is the stored event's `integrity.event_sha256` and `uncompressed_bytes` the
  length of its preimage (the SDK canonical JSON of the event without `event_sha256`), so the
  reference names a real, verifiable byte string: the event's canonical metadata.

Every reference also carries an opaque `resource_uri` (`agent-context://events/<event id>` or
`.../contents/<sha256 of the content ID>`): no path, host, DSN or raw content ID.

Content reads. `ContentService.read` performs no scope check, so `snippet` reads only after every
gate passes: the event is inside the request scope, its reference for that content ID is
`sanitized`, the content ID is named by no `content.purged` event in the ledger (authoritative,
so no projection lag can expose purged bytes), the media type is text or JSON, and the content is
at most `MAX_READ_BYTES` (larger evidence is never read: the item carries the `resource_uri` only).
The snippet is at most `MAX_SNIPPET_CHARS` characters with control and format characters removed;
it is untrusted evidence and is only ever placed in an item's `content`.

Lexical ranking for the history tools. `rank_documents` ranks the PLATFORM-042 lexical material
(`retrieval.search_documents`) of an explicit set of event IDs that a scoped read already returned,
with the same `simple` text-search configuration and delivered-only rule as `SearchService`; the
set of IDs is the scope, so no document outside it can be ranked.

Nothing here logs; errors are content-free.
"""

from __future__ import annotations

import hashlib
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final
from uuid import UUID

from agent_context_sdk import (
    ContentDisposition,
    ContentRefV1,
    ContentStorage,
    StoredEventV1,
    verify_event,
)
from agent_context_sdk.canonical import canonical_json_bytes
from agent_context_sdk.contracts.query import EvidenceRefV1
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.service import ContentService, ContentServiceError
from agent_context_platform.ledger.models import EventContentRefRow, EventRow
from agent_context_platform.ledger.repository import _row_to_stored_event
from agent_context_platform.projection.projectors.search import extract_text
from agent_context_platform.retrieval.models import TEXT_SEARCH_CONFIG

EVENT_METADATA_MEDIA_TYPE: Final = "application/vnd.agent-context.event-metadata+json"
RESOURCE_SCHEME: Final = "agent-context://"
MAX_READ_BYTES: Final = INLINE_MAX_BYTES
MAX_SNIPPET_CHARS: Final = 480
MAX_EVENTS_PER_LOAD: Final = 1000
DEFAULT_STATEMENT_TIMEOUT_SECONDS: Final = 5.0
EXECUTION_EVENT_PREFIX: Final = "quality."
FAILURE_OBSERVED_TYPE: Final = "knowledge.failure.observed"
_READABLE_MEDIA_TYPES: Final = frozenset({"text/plain", "application/json"})

_PURGED: Final[str] = (
    "SELECT DISTINCT item ->> 'content_id' AS content_id FROM ledger.events e, "
    "jsonb_array_elements(e.payload -> 'purged_contents') AS item "
    "WHERE e.event_type = 'content.purged' AND item ->> 'content_id' = ANY(:content_ids)"
)
# Only events every projector has taken (delivered) are cited: a dead-lettered or pending event
# never reached the graph or the search material, so nothing the package says rests on it.
_DELIVERED: Final[str] = (
    "SELECT event_id FROM projection.outbox WHERE event_id = ANY(CAST(:ids AS uuid[])) "
    "AND status = 'delivered' AND (CAST(:max AS bigint) IS NULL OR outbox_id <= CAST(:max AS bigint))"
)
_TIMEOUT: Final[str] = "SELECT set_config('statement_timeout', :millis, true)"
_RANK: Final[str] = f"""
WITH q AS (SELECT websearch_to_tsquery('{TEXT_SEARCH_CONFIG}', :query) AS tsq)
SELECT d.source_event_ids AS source_event_ids, ts_rank_cd(d.tsv, q.tsq) AS score
FROM retrieval.search_documents d, q
WHERE d.source_event_ids && CAST(:event_ids AS uuid[]) AND d.tsv @@ q.tsq
  AND EXISTS (SELECT 1 FROM projection.outbox o
              WHERE o.event_id = ANY(d.source_event_ids) AND o.status = 'delivered')
ORDER BY score DESC, d.content_id
LIMIT :limit
"""


@dataclass(frozen=True, slots=True)
class EvidenceScope:
    """What a cited event must satisfy beyond existing, being delivered and verifying.

    - Its envelope `context` matches every given scope ID.
    - `before` (exclusive) bounds its `occurred_at`, `recorded_before` its `observed_at`: a
      revision's window end bounds both (a fact first recorded after the window is later
      knowledge, whenever it says it happened); `failure_history` bounds valid time only.
    - In a revision query (`revision_mode`), an EXECUTION event (`quality.*`: CI and test runs,
      findings) whose payload names a workspace snapshot is dirty evidence unless it is the
      requested snapshot (`snapshot_id`; `None` for a commit): output of a dirty workspace never
      backs a commit answer. Code events are NOT filtered this way: a code claim names the run
      that first observed it, so an unchanged symbol committed after being indexed in a dirty
      snapshot is a replayed claim whose only event names that snapshot. Whether a code fact
      belongs to the revision is decided by the revision's membership (`revision.py`).
    - `excluded_runs` are the CI and test runs the answer dropped (dirty runs): a failure
      observation (`knowledge.failure.observed`) made in one of them is not cited either.
    - `max_outbox_id` bounds its outbox position to the package's `as_of_event_id`, so an event
      delivered after the snapshot (while an earlier one was still pending) is never cited.
    """

    project_id: str | None = None
    repository_id: str | None = None
    before: datetime | None = None
    recorded_before: datetime | None = None
    revision_mode: bool = False
    snapshot_id: str | None = None
    excluded_runs: frozenset[str] = frozenset()
    max_outbox_id: int | None = None

    def __post_init__(self) -> None:
        if self.project_id is None and self.repository_id is None:
            raise ValueError("an evidence scope needs a project or a repository")

    def admits(self, event: StoredEventV1) -> bool:
        context = event.context
        if self.project_id is not None and context.project_id != self.project_id:
            return False
        if self.repository_id is not None and context.repository_id != self.repository_id:
            return False
        if self.before is not None and event.occurred_at >= self.before:
            return False
        if self.recorded_before is not None and event.observed_at >= self.recorded_before:
            return False
        payload = event.payload
        if self.revision_mode and event.event_type.startswith(EXECUTION_EVENT_PREFIX):
            named = payload.get("snapshot_id")
            if named is not None and str(named) != self.snapshot_id:
                return False
        if self.excluded_runs and event.event_type == FAILURE_OBSERVED_TYPE:
            runs = {str(payload[k]) for k in ("ci_run_id", "test_run_id") if payload.get(k)}
            if runs & self.excluded_runs:
                return False
        return True


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    """One verified stored event and what citing it needs."""

    event: StoredEventV1
    preimage_bytes: int
    purged: frozenset[str]

    @property
    def event_id(self) -> str:
        return str(self.event.event_id)

    def content_ref(self, content_id: str) -> ContentRefV1 | None:
        for ref in self.event.content_refs:
            if ref.content_id == content_id:
                return ref
        return None

    def citable_content(self, content_id: str | None) -> ContentRefV1 | None:
        """The event's sanitized, unpurged reference for `content_id` (`None`: its first one)."""
        if content_id is None:
            for ref in sorted(self.event.content_refs, key=lambda r: r.content_id):
                if self.citable_content(ref.content_id) is not None:
                    return ref
            return None
        if content_id in self.purged:
            return None
        ref = self.content_ref(content_id)
        if ref is None or ref.disposition is not ContentDisposition.SANITIZED:
            return None
        return ref


@dataclass(frozen=True, slots=True)
class Citation:
    """What a candidate says about one source event (resolved into an `EvidenceRefV1`)."""

    event_id: str
    evidence_kind: str
    confidence: float
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    revision_id: str | None = None
    content_id: str | None = None


def metadata_ref(item: LedgerEvent) -> ContentRefV1:
    """The metadata-only reference of a stored event (see the module docstring)."""
    return ContentRefV1(
        content_id=f"event:{item.event_id}",
        content_sha256=item.event.integrity.event_sha256,
        media_type=EVENT_METADATA_MEDIA_TYPE,
        uncompressed_bytes=item.preimage_bytes,
        disposition=ContentDisposition.METADATA_ONLY,
        storage=ContentStorage.INLINE,
        inline_id=item.event_id,
    )


def resource_uri(event_id: str, content_id: str | None = None) -> str:
    if content_id is None:
        return f"{RESOURCE_SCHEME}events/{event_id}"
    digest = hashlib.sha256(content_id.encode()).hexdigest()
    return f"{RESOURCE_SCHEME}events/{event_id}/contents/{digest}"


def evidence_ref(item: LedgerEvent, citation: Citation) -> EvidenceRefV1 | None:
    """The SDK reference for `citation`, or `None` when the SDK would reject it (never coerced)."""
    content = item.citable_content(citation.content_id)
    valid_from = citation.valid_from or item.event.occurred_at
    valid_to = citation.valid_to
    if valid_to is not None and valid_to <= valid_from:
        valid_to = None  # an empty interval says nothing more than an open one starting there
    try:
        return EvidenceRefV1(
            source_event_id=item.event.event_id,
            evidence_kind=citation.evidence_kind,
            confidence=float(citation.confidence),
            valid_from=valid_from,
            valid_to=valid_to,
            content_ref=content or metadata_ref(item),
            revision_id=citation.revision_id,
            resource_uri=resource_uri(
                item.event_id, None if content is None else content.content_id
            ),
        )
    except ValidationError:
        return None


def clean_text(value: str, limit: int) -> str:
    """Printable text only: no control or format characters (bidi overrides included)."""
    kept = (
        " " if unicodedata.category(char)[0] in {"C", "Z"} else char for char in value[: limit * 4]
    )
    collapsed = " ".join("".join(kept).split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 1].rstrip() + "…"


class LoadedEvents(dict[str, "LedgerEvent"]):
    """Loaded events by ID; `capped` says the request named more than `MAX_EVENTS_PER_LOAD`."""

    capped: bool = False


async def _purged(session: AsyncSession, content_ids: Sequence[str]) -> set[str]:
    rows = await session.execute(text(_PURGED), {"content_ids": list(content_ids)})
    return {str(row.content_id) for row in rows}


def _uuid(value: str) -> UUID | None:
    try:
        return UUID(value)
    except ValueError:
        return None


class EvidenceLedger:
    """Loads cited events and gated content through one PostgreSQL role (the API role suffices)."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        content: ContentService,
        *,
        statement_timeout_seconds: float = DEFAULT_STATEMENT_TIMEOUT_SECONDS,
    ) -> None:
        if not statement_timeout_seconds > 0:
            raise ValueError("statement_timeout_seconds must be positive")
        self._sessions = sessions
        self._content = content
        self._timeout = str(max(1, int(statement_timeout_seconds * 1000)))

    async def _bound(self, session: AsyncSession) -> None:
        """Bound every statement of the session's transaction (the request deadline bounds all)."""
        await session.execute(text(_TIMEOUT), {"millis": self._timeout})

    async def events(self, event_ids: Iterable[str], scope: EvidenceScope) -> LoadedEvents:
        """The verified, in-scope events among `event_ids` (others are silently not citable).

        At most `MAX_EVENTS_PER_LOAD` are loaded: the NEWEST (UUIDv7 order), with `capped` set
        so the caller reports it. One transaction, a fixed number of statements: delivery, the
        event rows, their content references (reconstructed by the ledger's own
        `_row_to_stored_event`) and the purge check.
        """
        wanted = sorted({uid for uid in (_uuid(e) for e in event_ids) if uid is not None})
        loaded = LoadedEvents()
        if len(wanted) > MAX_EVENTS_PER_LOAD:
            wanted = wanted[-MAX_EVENTS_PER_LOAD:]
            loaded.capped = True
        if not wanted:
            return loaded
        async with self._sessions() as session:
            await self._bound(session)
            rows = await session.execute(
                text(_DELIVERED), {"ids": [str(e) for e in wanted], "max": scope.max_outbox_id}
            )
            delivered = sorted(UUID(str(row.event_id)) for row in rows)
            if not delivered:
                return loaded
            event_rows = (
                await session.scalars(select(EventRow).where(EventRow.event_id.in_(delivered)))
            ).all()
            ref_rows = (
                await session.scalars(
                    select(EventContentRefRow).where(EventContentRefRow.event_id.in_(delivered))
                )
            ).all()
            refs: dict[UUID, list[EventContentRefRow]] = {}
            for ref in ref_rows:
                refs.setdefault(ref.event_id, []).append(ref)
            events: list[StoredEventV1] = []
            for row in sorted(event_rows, key=lambda r: r.event_id):
                event = _row_to_stored_event(row, refs.get(row.event_id, []))
                if event.event_id.version != 7 or not verify_event(event):
                    continue
                if scope.admits(event):
                    events.append(event)
            content_ids = sorted({ref.content_id for e in events for ref in e.content_refs})
            purged: set[str] = set()
            if content_ids:
                purged = await _purged(session, content_ids)
        for event in events:
            preimage = canonical_json_bytes(
                event.model_dump(mode="json", exclude={"integrity": {"event_sha256"}})
            )
            loaded[str(event.event_id)] = LedgerEvent(
                event,
                len(preimage),
                frozenset(ref.content_id for ref in event.content_refs if ref.content_id in purged),
            )
        return loaded

    async def snippet(self, item: LedgerEvent, content_id: str | None) -> str | None:
        """A bounded excerpt of gated content, or `None` when any gate refuses (see module)."""
        return (await self.snippets([(item, content_id)])).get((item.event_id, content_id))

    async def snippets(
        self, requests: Sequence[tuple[LedgerEvent, str | None]]
    ) -> dict[tuple[str, str | None], str | None]:
        """Excerpts for several (event, content ID) pairs, read in ONE transaction whose purge
        re-check (a purge recorded since `events` loaded the event must still win) covers them all.
        """
        readable: dict[tuple[str, str | None], ContentRefV1] = {}
        found: dict[tuple[str, str | None], str | None] = {}
        for item, content_id in requests:
            key = (item.event_id, content_id)
            found[key] = None
            ref = item.citable_content(content_id)
            if ref is None:
                continue
            media = ref.media_type.split(";", 1)[0].strip().lower()
            if media in _READABLE_MEDIA_TYPES and ref.uncompressed_bytes <= MAX_READ_BYTES:
                readable[key] = ref
        if not readable:
            return found
        async with self._sessions() as session:
            await self._bound(session)
            purged = await _purged(session, sorted({r.content_id for r in readable.values()}))
            for key, ref in sorted(readable.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")):
                if ref.content_id in purged:
                    continue
                try:
                    data = await self._content.read(session, ref)
                except ContentServiceError:
                    continue
                media = ref.media_type.split(";", 1)[0].strip().lower()
                found[key] = clean_text(extract_text(media, data), MAX_SNIPPET_CHARS) or None
        return found

    async def rank_documents(
        self, event_ids: Sequence[str], query: str, *, limit: int
    ) -> Mapping[str, float]:
        """`ts_rank_cd` of the lexical documents of `event_ids` matching `query`, per event ID."""
        ids = sorted({str(uid) for uid in (_uuid(e) for e in event_ids) if uid is not None})
        if not ids or not query.strip():
            return {}
        async with self._sessions() as session:
            await self._bound(session)
            rows = await session.execute(
                text(_RANK), {"query": query, "event_ids": ids, "limit": limit}
            )
            scores: dict[str, float] = {}
            wanted = set(ids)
            for row in rows:
                for event_id in row.source_event_ids:
                    key = str(event_id)
                    if key in wanted:
                        scores[key] = max(scores.get(key, 0.0), float(row.score))
        return scores
