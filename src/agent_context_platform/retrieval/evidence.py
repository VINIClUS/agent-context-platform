"""Ledger-backed evidence for context items (PLATFORM-043): references, snippets, ranking.

Every `EvidenceRefV1` a context package cites is built here from a REAL stored event, loaded through
`LedgerRepository.get_event` (the ledger's single reconstruction path) and integrity-checked with
the SDK's `verify_event`; an event that is missing, fails verification, has a non-UUIDv7 ID or lies
outside the request scope is never cited.

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
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.service import ContentService, ContentServiceError
from agent_context_platform.ledger.repository import LedgerRepository
from agent_context_platform.projection.projectors.search import extract_text
from agent_context_platform.retrieval.models import TEXT_SEARCH_CONFIG

EVENT_METADATA_MEDIA_TYPE: Final = "application/vnd.agent-context.event-metadata+json"
RESOURCE_SCHEME: Final = "agent-context://"
MAX_READ_BYTES: Final = INLINE_MAX_BYTES
MAX_SNIPPET_CHARS: Final = 480
MAX_EVENTS_PER_LOAD: Final = 1000
_READABLE_MEDIA_TYPES: Final = frozenset({"text/plain", "application/json"})

_PURGED: Final[str] = (
    "SELECT DISTINCT item ->> 'content_id' AS content_id FROM ledger.events e, "
    "jsonb_array_elements(e.payload -> 'purged_contents') AS item "
    "WHERE e.event_type = 'content.purged' AND item ->> 'content_id' = ANY(:content_ids)"
)
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
    """The scope a cited event must belong to: its envelope context must match every given ID."""

    project_id: str | None = None
    repository_id: str | None = None
    # Exclusive upper bound on the cited events' `occurred_at` (a revision's window end): a fact
    # first recorded by a later event is never cited, whatever the candidate claims.
    before: datetime | None = None

    def __post_init__(self) -> None:
        if self.project_id is None and self.repository_id is None:
            raise ValueError("an evidence scope needs a project or a repository")

    def admits(self, event: StoredEventV1) -> bool:
        context = event.context
        if self.project_id is not None and context.project_id != self.project_id:
            return False
        if self.before is not None and event.occurred_at >= self.before:
            return False
        return self.repository_id is None or context.repository_id == self.repository_id


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


def _uuid(value: str) -> UUID | None:
    try:
        return UUID(value)
    except ValueError:
        return None


class EvidenceLedger:
    """Loads cited events and gated content through one PostgreSQL role (the API role suffices)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], content: ContentService) -> None:
        self._sessions = sessions
        self._content = content

    async def events(
        self, event_ids: Iterable[str], scope: EvidenceScope
    ) -> dict[str, LedgerEvent]:
        """The verified, in-scope events among `event_ids` (others are silently not citable)."""
        wanted = sorted({uid for uid in (_uuid(e) for e in event_ids) if uid is not None})
        if len(wanted) > MAX_EVENTS_PER_LOAD:
            wanted = wanted[:MAX_EVENTS_PER_LOAD]
        loaded: dict[str, LedgerEvent] = {}
        async with self._sessions() as session:
            events: list[StoredEventV1] = []
            for event_id in wanted:
                event = await LedgerRepository.get_event(session, event_id)
                if event is None or event.event_id.version != 7 or not verify_event(event):
                    continue
                if not scope.admits(event):
                    continue
                events.append(event)
            content_ids = sorted({ref.content_id for e in events for ref in e.content_refs})
            purged: set[str] = set()
            if content_ids:
                rows = await session.execute(text(_PURGED), {"content_ids": content_ids})
                purged = {str(row.content_id) for row in rows}
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
        ref = item.citable_content(content_id)
        if ref is None:
            return None
        media = ref.media_type.split(";", 1)[0].strip().lower()
        if media not in _READABLE_MEDIA_TYPES or ref.uncompressed_bytes > MAX_READ_BYTES:
            return None
        async with self._sessions() as session:
            try:
                data = await self._content.read(session, ref)
            except ContentServiceError:
                return None
        excerpt = clean_text(extract_text(media, data), MAX_SNIPPET_CHARS)
        return excerpt or None

    async def rank_documents(
        self, event_ids: Sequence[str], query: str, *, limit: int
    ) -> Mapping[str, float]:
        """`ts_rank_cd` of the lexical documents of `event_ids` matching `query`, per event ID."""
        ids = sorted({str(uid) for uid in (_uuid(e) for e in event_ids) if uid is not None})
        if not ids or not query.strip():
            return {}
        async with self._sessions() as session:
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
