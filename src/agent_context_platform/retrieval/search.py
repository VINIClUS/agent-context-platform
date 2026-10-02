"""Scoped lexical and vector retrieval over sanitized content (PLATFORM-042).

Both searches return content IDs with a score, scope metadata and the source event IDs; the text is
fetched later through the content service. Every call needs a `SearchScope` (a project AND a
repository): there is no unscoped search. Limits are bounded (`MAX_LIMIT`) and every call runs under
a deadline.

* `lexical` runs `websearch_to_tsquery('simple', ...)` ranked by `ts_rank_cd` over the PostgreSQL
  `retrieval.search_documents` tsvector (the `simple` configuration: the content is Portuguese and
  English, so no stemming).
* `vector` embeds the query and calls the Neo4j vector index `db.index.vector.queryNodes`, which
  has no filter of its own: it over-fetches candidates, filters them by project, repository and
  model revision in the query, and widens the candidate count until `limit` hits survive or the
  cap is reached. A hit must also have its PostgreSQL row, which is also where the metadata comes
  from, so a purged document is never returned even if a stale node lingers.

Both searches return only documents whose source event was delivered (see `_DELIVERED`).

Recall limit, not a leak: the Neo4j index returns its top-k over the WHOLE corpus and the scope
filter runs afterwards, on at most `MAX_CANDIDATES` (1000) candidates. A small scope in a large
corpus can therefore return fewer than `limit` hits (or none) even when matching documents exist;
what is returned is always inside the scope.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, LiteralString

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.projection.neo4j import Neo4jReadFacade, Neo4jTransaction
from agent_context_platform.retrieval.embeddings import EmbeddingProvider
from agent_context_platform.retrieval.models import TEXT_SEARCH_CONFIG

MAX_LIMIT: Final = 100
MAX_QUERY_CHARS: Final = 1_000
DEFAULT_DEADLINE_SECONDS: Final = 5.0
# First candidate count is `limit * OVERFETCH_FACTOR`, widened by the same factor up to the cap.
OVERFETCH_FACTOR: Final = 4
MAX_CANDIDATES: Final = 1_000
VECTOR_INDEX_NAME: Final = "vx_content_embedding_embedding"


class SearchRequestError(ValueError):
    """The scope, query or limit of a search is invalid."""


class SearchTimeoutError(TimeoutError):
    """A search did not finish within its deadline."""


@dataclass(frozen=True, slots=True)
class SearchScope:
    """The project and repository a search is confined to; both are mandatory."""

    project_id: str
    repository_id: str

    def __post_init__(self) -> None:
        if not self.project_id.strip() or not self.repository_id.strip():
            raise SearchRequestError("a search needs a project_id and a repository_id")


@dataclass(frozen=True, slots=True)
class SearchHit:
    content_id: str
    score: float
    project_id: str
    repository_id: str
    session_id: str | None
    event_id: str
    event_type: str
    occurred_at: datetime
    source_event_ids: tuple[str, ...]
    redaction: str


_COLUMNS: Final = (
    "d.content_id, d.project_id, d.repository_id, d.session_id, d.event_id, d.event_type, "
    "d.occurred_at, d.source_event_ids, d.redaction"
)
# A row is searchable only once one of its source events is DELIVERED (every projector, the graph
# write included, committed). A row left by an event that was dead-lettered, or whose graph
# commit has not happened yet (a crash after the PostgreSQL commit), stays invisible.
_DELIVERED: Final = (
    "EXISTS (SELECT 1 FROM projection.outbox o "
    "WHERE o.event_id = ANY(d.source_event_ids) AND o.status = 'delivered')"
)
_LEXICAL: Final[str] = f"""
WITH q AS (SELECT websearch_to_tsquery('{TEXT_SEARCH_CONFIG}', :query) AS tsq)
SELECT {_COLUMNS}, ts_rank_cd(d.tsv, q.tsq) AS score
FROM retrieval.search_documents d, q
WHERE d.project_id = :project_id AND d.repository_id = :repository_id AND d.tsv @@ q.tsq
  AND {_DELIVERED}
ORDER BY score DESC, d.content_id
LIMIT :limit
"""
_METADATA: Final[str] = f"""
SELECT {_COLUMNS} FROM retrieval.search_documents d
WHERE d.content_id = ANY(:content_ids)
  AND d.project_id = :project_id AND d.repository_id = :repository_id
  AND {_DELIVERED}
"""
_VECTOR: Final[LiteralString] = (
    "CALL db.index.vector.queryNodes('vx_content_embedding_embedding', $k, $vector) "
    "YIELD node, score "
    "WITH collect({content_id: node.content_id, score: score, project_id: node.project_id, "
    "repository_id: node.repository_id, model_revision: node.model_revision}) AS raw "
    "UNWIND raw AS row "
    "WITH raw, row WHERE row.project_id = $project_id AND row.repository_id = $repository_id "
    "AND row.model_revision = $model_revision "
    "RETURN size(raw) AS raw_count, row.content_id AS content_id, row.score AS score "
    "ORDER BY score DESC, content_id LIMIT $limit"
)


def _hit(row: Any, score: float) -> SearchHit:
    return SearchHit(
        content_id=row.content_id,
        score=score,
        project_id=row.project_id,
        repository_id=row.repository_id,
        session_id=row.session_id,
        event_id=str(row.event_id),
        event_type=row.event_type,
        occurred_at=row.occurred_at,
        source_event_ids=tuple(str(item) for item in row.source_event_ids),
        redaction=row.redaction,
    )


def _validate(query: str, limit: int) -> str:
    cleaned = query.strip()
    if not cleaned or len(cleaned) > MAX_QUERY_CHARS:
        raise SearchRequestError(f"a query must be 1 to {MAX_QUERY_CHARS} characters")
    if not 1 <= limit <= MAX_LIMIT:
        raise SearchRequestError(f"limit must be between 1 and {MAX_LIMIT}")
    return cleaned


class SearchService:
    """Scoped lexical and vector search; `sessions` is the API-role PostgreSQL connection."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        graph: Neo4jReadFacade | None = None,
        embeddings: EmbeddingProvider | None = None,
        *,
        deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
    ) -> None:
        if deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")
        self._sessions = sessions
        self._graph = graph
        self._embeddings = embeddings
        self._deadline = deadline_seconds

    async def lexical(self, query: str, scope: SearchScope, limit: int) -> list[SearchHit]:
        """Documents matching `query`, best `ts_rank_cd` first, confined to `scope`."""
        cleaned = _validate(query, limit)
        try:
            async with asyncio.timeout(self._deadline):
                async with self._sessions() as session, session.begin():
                    await self._bound_statement(session)
                    rows = await session.execute(
                        text(_LEXICAL),
                        {
                            "query": cleaned,
                            "project_id": scope.project_id,
                            "repository_id": scope.repository_id,
                            "limit": limit,
                        },
                    )
                    return [_hit(row, float(row.score)) for row in rows]
        except TimeoutError:
            raise SearchTimeoutError("search deadline exceeded") from None

    async def vector(self, query: str, scope: SearchScope, limit: int) -> list[SearchHit]:
        """Documents nearest to `query` by cosine similarity, confined to `scope`."""
        cleaned = _validate(query, limit)
        if self._graph is None or self._embeddings is None:
            raise SearchRequestError("vector search needs a graph and an embedding provider")
        try:
            async with asyncio.timeout(self._deadline):
                embedded = (await self._embeddings.embed([cleaned]))[0]
                return await self._expand(list(embedded.vector), scope, limit)
        except TimeoutError:
            raise SearchTimeoutError("search deadline exceeded") from None

    async def _expand(self, vector: list[float], scope: SearchScope, limit: int) -> list[SearchHit]:
        assert self._graph is not None and self._embeddings is not None
        revision = self._embeddings.model_revision
        candidates = min(limit * OVERFETCH_FACTOR, MAX_CANDIDATES)
        while True:
            scored, raw_count = await self._candidates(vector, scope, revision, candidates)
            hits = await self._with_metadata(scored, scope)
            exhausted = raw_count is not None and raw_count < candidates
            if len(hits) >= limit or exhausted or candidates >= MAX_CANDIDATES:
                return hits[:limit]
            candidates = min(candidates * OVERFETCH_FACTOR, MAX_CANDIDATES)

    async def _candidates(
        self, vector: list[float], scope: SearchScope, revision: str, candidates: int
    ) -> tuple[list[tuple[str, float]], int | None]:
        assert self._graph is not None

        async def read(tx: Neo4jTransaction) -> tuple[list[tuple[str, float]], int | None]:
            result = await tx.run(
                _VECTOR,
                parameters={
                    "k": candidates,
                    "vector": vector,
                    "project_id": scope.project_id,
                    "repository_id": scope.repository_id,
                    "model_revision": revision,
                    # Candidates may still lose their PostgreSQL row (stale or undelivered), so
                    # return every filtered candidate of this round, not just `limit` of them.
                    "limit": candidates,
                },
            )
            rows = result.records
            raw = int(rows[0]["raw_count"]) if rows else None
            return [(str(row["content_id"]), float(row["score"])) for row in rows], raw

        return await self._graph.execute_read(read)

    async def _with_metadata(
        self, scored: Sequence[tuple[str, float]], scope: SearchScope
    ) -> list[SearchHit]:
        if not scored:
            return []
        async with self._sessions() as session, session.begin():
            await self._bound_statement(session)
            rows = await session.execute(
                text(_METADATA),
                {
                    "content_ids": [content_id for content_id, _ in scored],
                    "project_id": scope.project_id,
                    "repository_id": scope.repository_id,
                },
            )
            by_id = {row.content_id: row for row in rows}
        return [_hit(by_id[cid], score) for cid, score in scored if cid in by_id]

    async def _bound_statement(self, session: AsyncSession) -> None:
        await session.execute(
            text("SELECT set_config('statement_timeout', :millis, true)"),
            {"millis": str(max(1, int(self._deadline * 1000)))},
        )
