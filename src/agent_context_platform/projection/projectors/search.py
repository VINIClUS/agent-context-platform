"""Index sanitized content for lexical and vector retrieval (PLATFORM-042).

For every event that references stored content, `SearchProjector` reads the SANITIZED bytes through
`ContentService.read` (never raw hook input), then writes two projections of them:

* PostgreSQL `retrieval.search_documents`: scope, provenance and a `simple` `tsvector`. The text is
  not stored again; a snippet is fetched later through the content service.
* A Neo4j `ContentEmbedding` node: the L2-normalized MiniLM vector (the `embedding` property the
  cosine vector index covers), the model identity and the scope used to filter vector hits.

Both are projections. They are rebuildable from the ledger and never a source of truth.

Delivery is at-least-once and every write is an idempotent upsert. The PostgreSQL write is NOT
atomic with the Neo4j transaction or the checkpoint (the runner's Neo4j transaction commits first,
the checkpoint after it): a crash between them replays the event, and replaying converges to the
same rows and node, so no atomicity is needed. The slow work (reading content, embedding,
the PostgreSQL write) runs BEFORE the first Neo4j statement, so no graph lock or transaction
timeout is spent waiting on inference or object storage.

Purge: `content.purged` writes a tombstone and deletes the search rows and the `ContentEmbedding`
nodes for every purged `content_id`. Indexing checks the tombstone, so a redelivered original event
cannot resurrect purged content; a `content.purged` that is projected before the original event
(delivery is unordered) has the same effect. The vector search additionally requires the
PostgreSQL row, so a node a concurrent index-and-purge race leaves behind is never returned and the
next rebuild removes it.

Only events scoped to both a project and a repository are indexed: retrieval is always scoped,
so an unscoped document could never be returned.

`embedding` is excluded from the P039 graph digest (`VOLATILE_PROPERTIES`); the model identity and
revision stay in it.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Final, LiteralString

from agent_context_sdk import EVENT_PAYLOAD_MODELS, ContentPurgedV1, ContentRefV1, StoredEventV1
from agent_context_sdk.content import ContentDisposition
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.service import ContentService
from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors import (
    assert_link,
    event_lock_keys,
    lock_event_nodes,
    relationship_statement,
)
from agent_context_platform.retrieval.embeddings import EmbeddingProvider
from agent_context_platform.retrieval.models import TEXT_SEARCH_CONFIG

CONTENT_PURGED_TYPE: Final = "content.purged"
# Dimensions of the cosine vector index (`projection.schema`); a provider must match.
EMBEDDING_DIMENSIONS: Final = 384
# Upper bound on the text one document indexes: far below the 1 MiB `tsvector` limit.
MAX_INDEXED_CHARS: Final = 200_000
_INDEXABLE_MEDIA_TYPES: Final = frozenset({"text/plain", "application/json"})
_OWNER_LABELS: Final = (
    ("ToolCall", "tool_call_id"),
    ("Turn", "turn_id"),
    ("Session", "session_id"),
)


def _carries_content(model: type[Any]) -> bool:
    return any(name.endswith("content_id") for name in model.model_fields)


#: Event types whose payload names a content object, derived from the SDK registry so a new
#: content-carrying event type is picked up with the SDK. Purges are handled separately.
INDEXED_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    event_type
    for (event_type, _version), model in EVENT_PAYLOAD_MODELS.items()
    if _carries_content(model)
)
_HANDLED: Final = INDEXED_EVENT_TYPES | {CONTENT_PURGED_TYPE}


class SearchBackendNotConfiguredError(RuntimeError):
    """The projector was asked to index content but has no embedding model or content access."""


class SearchScopeConflictError(RuntimeError):
    """A content ID is already indexed under a different project or repository."""


class EmbeddingDimensionError(RuntimeError):
    """The embedding provider's vectors do not match the vector index."""


@dataclass(frozen=True, slots=True)
class SearchBackend:
    """What indexing needs: the projector-role PostgreSQL, content access and an embedder."""

    sessions: async_sessionmaker[AsyncSession]
    content: ContentService
    embeddings: EmbeddingProvider
    # False for a replay into a scratch or standby graph (`verify --replay-check`, a standby
    # rebuild): content is still read, tombstones checked and vectors written to the target graph,
    # but the live PostgreSQL rows and tombstones are never written or deleted.
    write_documents: bool = True


@dataclass(frozen=True, slots=True)
class PurgeAccess:
    """What purging needs, and nothing more: PostgreSQL (no embedder, no content reader).

    Purges must work on a deployment with no model, otherwise purged content would stay searchable.
    """

    sessions: async_sessionmaker[AsyncSession]
    write_documents: bool = True


@dataclass(frozen=True, slots=True)
class _Document:
    ref: ContentRefV1
    text: str


# Indexing and purging one content ID serialize on this transaction-scoped lock (taken in the index
# transaction BEFORE its tombstone check, and in the purge transaction), so a purge that commits
# between an index's tombstone check and its upsert can never be overwritten.
CONTENT_LOCK_PREFIX: Final = "agent-context.search.content:"
_LOCK_CONTENT: Final[str] = "SELECT pg_advisory_xact_lock(hashtextextended(CAST(:key AS text), 0))"
_LEDGER_PURGES: Final[str] = (
    "SELECT DISTINCT item ->> 'content_id' FROM ledger.events e, "
    "jsonb_array_elements(e.payload -> 'purged_contents') AS item "
    "WHERE e.event_type = 'content.purged'"
)
_TOMBSTONED: Final[str] = (
    "SELECT 1 FROM retrieval.content_tombstones WHERE content_id = :content_id"
)
_UPSERT_DOCUMENT: Final[str] = f"""
INSERT INTO retrieval.search_documents AS d
    (content_id, event_id, source_event_ids, event_type, project_id, repository_id,
     session_id, occurred_at, redaction, tsv)
VALUES
    (:content_id, CAST(:event_id AS uuid), ARRAY[CAST(:event_id AS uuid)], :event_type,
     :project_id, :repository_id, :session_id, :occurred_at, :redaction,
     to_tsvector('{TEXT_SEARCH_CONFIG}', CAST(:body AS text)))
ON CONFLICT (content_id) DO UPDATE SET
    event_id = CASE WHEN (excluded.occurred_at, excluded.event_id) < (d.occurred_at, d.event_id)
        THEN excluded.event_id ELSE d.event_id END,
    event_type = CASE WHEN (excluded.occurred_at, excluded.event_id) < (d.occurred_at, d.event_id)
        THEN excluded.event_type ELSE d.event_type END,
    session_id = CASE WHEN (excluded.occurred_at, excluded.event_id) < (d.occurred_at, d.event_id)
        THEN excluded.session_id ELSE d.session_id END,
    redaction = CASE WHEN (excluded.occurred_at, excluded.event_id) < (d.occurred_at, d.event_id)
        THEN excluded.redaction ELSE d.redaction END,
    occurred_at = LEAST(d.occurred_at, excluded.occurred_at),
    source_event_ids = (
        SELECT array_agg(DISTINCT source_id ORDER BY source_id)
        FROM unnest(d.source_event_ids || excluded.source_event_ids) AS source_id
    ),
    tsv = excluded.tsv,
    indexed_at = now()
WHERE d.project_id = excluded.project_id AND d.repository_id = excluded.repository_id
RETURNING content_id
"""
_INSERT_TOMBSTONE: Final[str] = (
    "INSERT INTO retrieval.content_tombstones (content_id, purged_event_id, purged_at) "
    "VALUES (:content_id, CAST(:event_id AS uuid), :purged_at) ON CONFLICT (content_id) DO NOTHING"
)
_DELETE_DOCUMENTS: Final[str] = (
    "DELETE FROM retrieval.search_documents WHERE content_id = ANY(:content_ids)"
)

# `$vector` goes through `db.create.setNodeVectorProperty` (compact float32 storage for the index).
_UPSERT_EMBEDDING: Final[LiteralString] = (
    "MERGE (n:ContentEmbedding {content_id: $content_id}) "
    "SET n.model_id = $model_id, n.model_revision = $model_revision, "
    "n.content_sha256 = $content_sha256, n.truncated = $truncated, "
    "n.project_id = coalesce(n.project_id, $project_id), "
    "n.repository_id = coalesce(n.repository_id, $repository_id) "
    "WITH n "
    "UNWIND coalesce(n.source_event_ids, []) + [$event_id] AS source_event_id "
    "WITH DISTINCT n, source_event_id ORDER BY source_event_id "
    "WITH n, collect(source_event_id) AS source_event_ids "
    "SET n.source_event_ids = source_event_ids "
    "WITH n CALL db.create.setNodeVectorProperty(n, 'embedding', $vector)"
)
_DELETE_EMBEDDINGS: Final[LiteralString] = (
    "MATCH (n:ContentEmbedding) WHERE n.content_id IN $content_ids DETACH DELETE n"
)
_OWNER_LINKS: Final = {
    label: relationship_statement(label, key, "HAS_EMBEDDING", "ContentEmbedding", "content_id")
    for label, key in _OWNER_LABELS
}


async def lock_content(session: AsyncSession, content_ids: Iterable[str]) -> None:
    """Take the per-content advisory locks, in sorted order so two writers cannot deadlock."""
    for content_id in sorted(set(content_ids)):
        await session.execute(text(_LOCK_CONTENT), {"key": CONTENT_LOCK_PREFIX + content_id})


def _flatten_json(value: object, out: list[str]) -> None:
    """Collect string values of a JSON document, depth-first, without recursion."""
    stack: list[object] = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            stack.extend(reversed(list(item.values())))
        elif isinstance(item, list):
            stack.extend(reversed(item))


def extract_text(media_type: str, data: bytes) -> str:
    """The searchable text of sanitized content: the string values of JSON, or the text itself."""
    decoded = data.decode("utf-8", errors="replace")
    if media_type.split(";", 1)[0].strip().lower() == "application/json":
        try:
            parsed = json.loads(decoded)
        except ValueError:
            return decoded[:MAX_INDEXED_CHARS]
        parts: list[str] = []
        _flatten_json(parsed, parts)
        decoded = "\n".join(parts)
    return decoded[:MAX_INDEXED_CHARS]


def _indexable(ref: ContentRefV1) -> bool:
    media = ref.media_type.split(";", 1)[0].strip().lower()
    return ref.disposition is ContentDisposition.SANITIZED and media in _INDEXABLE_MEDIA_TYPES


def _owner(event: StoredEventV1) -> tuple[str, str] | None:
    """The agent node that owns the event's content: its tool call, else turn, else session."""
    locked = set(event_lock_keys(event))
    for label, _key in _OWNER_LABELS:
        for locked_label, node_id in sorted(locked):
            if locked_label == label:
                return label, node_id
    return None


class SearchProjector:
    """Projects sanitized content into PostgreSQL full-text rows and Neo4j embedding nodes."""

    name = "search"
    version = "1"

    def __init__(
        self, backend: SearchBackend | None = None, purge: PurgeAccess | None = None
    ) -> None:
        self._backend = backend
        self._purge_access = (
            PurgeAccess(backend.sessions, backend.write_documents) if backend else purge
        )
        # Purges seen by THIS instance while it replays into a graph-only target
        # (`write_documents=False`): the live tombstone table is never written then, so a purge
        # that precedes its original event in the replay is remembered here instead.
        self._replayed_purges: set[str] = set()

    def with_backend(self, backend: SearchBackend) -> SearchProjector:
        return SearchProjector(backend)

    def with_purge(self, purge: PurgeAccess) -> SearchProjector:
        return SearchProjector(purge=purge)

    def handles(self, event_type: str) -> bool:
        return event_type in _HANDLED

    async def seed_purges(self, sessions: async_sessionmaker[AsyncSession]) -> int:
        """Remember every `content.purged` ID in the ledger, before a replay.

        An in-place rebuild empties the tombstone table, and the purged bytes are already gone, so
        an original event replayed before its later purge would fail to read them. The ledger is
        authoritative: content it records as purged is never read or embedded by this instance.
        """
        async with sessions() as session:
            rows = await session.execute(text(_LEDGER_PURGES))
            self._replayed_purges.update(str(content_id) for content_id in rows.scalars())
        return len(self._replayed_purges)

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        if event.event_type == CONTENT_PURGED_TYPE:
            await self._purge(tx, event)
            return
        scope = _scope(event)
        refs = sorted((ref for ref in event.content_refs if _indexable(ref)), key=_content_id)
        if scope is None or not refs:
            return
        project_id, repository_id = scope
        backend = self._require_backend()
        indexed = await self._index_documents(backend, event, refs, project_id, repository_id)
        if not indexed:
            return
        await lock_event_nodes(tx, event)
        owner = _owner(event)
        for document, vector, truncated in indexed:
            await tx.run(
                _UPSERT_EMBEDDING,
                parameters={
                    "content_id": document.ref.content_id,
                    "model_id": backend.embeddings.model_id,
                    "model_revision": backend.embeddings.model_revision,
                    "content_sha256": document.ref.content_sha256,
                    "truncated": truncated,
                    "project_id": project_id,
                    "repository_id": repository_id,
                    "event_id": str(event.event_id),
                    "vector": list(vector),
                },
            )
            if owner is not None:
                await assert_link(
                    tx, _OWNER_LINKS[owner[0]], event, owner[1], document.ref.content_id
                )

    def _require_backend(self) -> SearchBackend:
        if self._backend is None:
            raise SearchBackendNotConfiguredError(
                "search indexing needs AGENT_CONTEXT_SEARCH__MODEL_DIR and content access"
            )
        return self._backend

    def _require_purge_access(self) -> PurgeAccess:
        if self._purge_access is None:
            raise SearchBackendNotConfiguredError("purging needs a PostgreSQL connection")
        return self._purge_access

    async def _index_documents(
        self,
        backend: SearchBackend,
        event: StoredEventV1,
        refs: Sequence[ContentRefV1],
        project_id: str,
        repository_id: str,
    ) -> list[tuple[_Document, tuple[float, ...], bool]]:
        """Read, embed and upsert the PostgreSQL rows; return what to write to the graph."""
        documents: list[_Document] = []
        async with backend.sessions() as session:
            tombstoned = await _tombstoned(session, [ref.content_id for ref in refs])
            tombstoned |= self._replayed_purges
            for ref in refs:
                if ref.content_id in tombstoned:
                    continue
                data = await backend.content.read(session, ref)
                body = extract_text(ref.media_type, data)
                if body.strip():
                    documents.append(_Document(ref, body))
        if not documents:
            return []
        embedded = await backend.embeddings.embed([document.text for document in documents])
        if len(embedded) != len(documents) or any(
            len(item.vector) != EMBEDDING_DIMENSIONS for item in embedded
        ):
            raise EmbeddingDimensionError("embedding provider output does not match the index")
        written: list[tuple[_Document, tuple[float, ...], bool]] = []
        if not backend.write_documents:
            return [
                (document, item.vector, item.truncated)
                for document, item in zip(documents, embedded, strict=True)
            ]
        async with backend.sessions() as session, session.begin():
            # Serialized with a purge of the same content, then re-checked right before the upsert.
            await lock_content(session, [d.ref.content_id for d in documents])
            tombstoned = await _tombstoned(session, [d.ref.content_id for d in documents])
            for document, item in zip(documents, embedded, strict=True):
                if document.ref.content_id in tombstoned:
                    continue
                row = await session.execute(
                    text(_UPSERT_DOCUMENT),
                    {
                        "content_id": document.ref.content_id,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "project_id": project_id,
                        "repository_id": repository_id,
                        "session_id": event.context.session_id,
                        "occurred_at": event.occurred_at,
                        "redaction": document.ref.disposition.value,
                        "body": document.text,
                    },
                )
                if row.first() is None:
                    raise SearchScopeConflictError(
                        "content is already indexed under another project or repository"
                    )
                written.append((document, item.vector, item.truncated))
        return written

    async def _purge(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        purged = ContentPurgedV1.model_validate(dict(event.payload))
        content_ids = sorted(item.content_id for item in purged.purged_contents)
        access = self._require_purge_access()
        if not access.write_documents:
            self._replayed_purges.update(content_ids)
        else:
            async with access.sessions() as session, session.begin():
                await lock_content(session, content_ids)
                for content_id in content_ids:
                    await session.execute(
                        text(_INSERT_TOMBSTONE),
                        {
                            "content_id": content_id,
                            "event_id": str(event.event_id),
                            "purged_at": purged.purged_at,
                        },
                    )
                await session.execute(text(_DELETE_DOCUMENTS), {"content_ids": content_ids})
        await lock_event_nodes(tx, event)
        await tx.run(_DELETE_EMBEDDINGS, parameters={"content_ids": content_ids})


def _content_id(ref: ContentRefV1) -> str:
    return str(ref.content_id)


def _scope(event: StoredEventV1) -> tuple[str, str] | None:
    project_id, repository_id = event.context.project_id, event.context.repository_id
    if project_id is None or repository_id is None:
        return None
    return project_id, repository_id


async def _tombstoned(session: AsyncSession, content_ids: Iterable[str]) -> set[str]:
    found: set[str] = set()
    for content_id in content_ids:
        if (await session.execute(text(_TOMBSTONED), {"content_id": content_id})).first():
            found.add(content_id)
    return found
