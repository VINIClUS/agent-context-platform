"""Lexical and vector retrieval (PLATFORM-042) over really ingested, really projected content.

The only fake is the embedder: its vectors are chosen so cosine orders are known.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.retrieval.search import (
    SearchHit,
    SearchRequestError,
    SearchScope,
    SearchService,
    SearchTimeoutError,
)

from .fake_embeddings import FakeEmbeddingProvider, unit
from .search_harness import SearchWorld, with_world

pytestmark = pytest.mark.integration

SCOPE = SearchScope("project-1", "repo-1")


def _service(world: SearchWorld, embeddings: FakeEmbeddingProvider | None = None) -> SearchService:
    return SearchService(world.api, world.store.read_only(), embeddings or world.embeddings)


def _ids(hits: list[SearchHit]) -> list[str]:
    return [hit.content_id for hit in hits]


def test_lexical_search_ranks_sanitized_content_and_returns_provenance(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        both = await world.ingest("the migration lock was held so the migration failed", "c-both")
        one = await world.ingest("added a search index migration", "c-one")
        await world.ingest("unrelated css tweak for the footer", "c-none")
        pt = await world.ingest("a migração do banco falhou", "c-pt")
        report = await world.project()
        assert report.delivered == 4 and report.dead_lettered == 0

        search = _service(world)
        assert _ids(await search.lexical("migration lock", SCOPE, 10)) == ["c-both"]  # AND
        hits = await search.lexical("migration or lock", SCOPE, 10)
        assert _ids(hits) == ["c-both", "c-one"]
        assert hits[0].score > hits[1].score > 0
        assert hits[0].event_id == str(both) and hits[0].source_event_ids == (str(both),)
        assert (hits[0].project_id, hits[0].repository_id) == ("project-1", "repo-1")
        assert (hits[0].session_id, hits[0].event_type) == ("session-1", "agent.turn.started")
        assert hits[0].redaction == "sanitized" and hits[1].event_id == str(one)

        # websearch syntax: exclusion works; the `simple` config matches Portuguese words as-is.
        assert _ids(await search.lexical("migration -lock", SCOPE, 10)) == ["c-one"]
        assert _ids(await search.lexical("migração", SCOPE, 10)) == ["c-pt"]
        assert (await search.lexical("migração", SCOPE, 10))[0].event_id == str(pt)
        assert await search.lexical("nonexistent", SCOPE, 10) == []
        assert len(await search.lexical("migration", SCOPE, 1)) == 1

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_json_content_is_indexed_by_its_string_values(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.ingest(
            '{"command":"pytest -q","output":{"summary":"two tests flaked"},"exit":1}',
            "c-json",
            media_type="application/json",
        )
        await world.project()

        search = _service(world)
        assert _ids(await search.lexical("flaked", SCOPE, 5)) == ["c-json"]
        assert await search.lexical("exit", SCOPE, 5) == []  # keys are not indexed

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_vector_search_ranks_by_cosine_and_returns_the_same_provenance(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        provider = FakeEmbeddingProvider(
            {
                "alpha doc": unit(1, 0),
                "beta doc": unit(0.8, 0.6),
                "gamma doc": unit(0.3, 0.95),
                "delta doc": unit(-1, 0),
                "the query": unit(1, 0),
            }
        )
        ids = {}
        for name in ("gamma", "alpha", "delta", "beta"):  # not in rank order
            ids[name] = await world.ingest(f"{name} doc", f"c-{name}")
        await world.project(provider)

        hits = await _service(world, provider).vector("the query", SCOPE, 10)

        assert _ids(hits) == ["c-alpha", "c-beta", "c-gamma", "c-delta"]
        scores = [hit.score for hit in hits]
        assert scores == sorted(scores, reverse=True)
        assert scores[0] == pytest.approx(1.0, abs=1e-4)
        assert hits[0].source_event_ids == (str(ids["alpha"]),)
        assert (hits[0].project_id, hits[0].repository_id) == ("project-1", "repo-1")
        assert _ids(await _service(world, provider).vector("the query", SCOPE, 2)) == [
            "c-alpha",
            "c-beta",
        ]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_search_never_crosses_a_repository_or_project(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        provider = FakeEmbeddingProvider({"shared words": unit(1, 0), "query": unit(1, 0)})
        await world.ingest("shared words", "c-mine")
        await world.ingest("shared words", "c-other-repo", repository="repo-2")
        await world.ingest("shared words", "c-other-project", project="project-2")
        await world.ingest("shared words", "c-unscoped", project=None, repository=None)
        await world.ingest("shared words", "c-no-repo", repository=None)
        await world.project(provider)

        search = _service(world, provider)
        assert await world.documents() == ["c-mine", "c-other-project", "c-other-repo"]
        for hits in (
            await search.lexical("shared", SCOPE, 10),
            await search.vector("query", SCOPE, 10),
        ):
            assert _ids(hits) == ["c-mine"]
        other = SearchScope("project-1", "repo-2")
        assert _ids(await search.lexical("shared", other, 10)) == ["c-other-repo"]
        assert _ids(await search.vector("query", other, 10)) == ["c-other-repo"]
        project_two = SearchScope("project-2", "repo-1")
        assert _ids(await search.lexical("shared", project_two, 10)) == ["c-other-project"]
        assert _ids(await search.vector("query", project_two, 10)) == ["c-other-project"]
        nobody = SearchScope("project-2", "repo-2")  # a pair no event carries
        assert await search.lexical("shared", nobody, 10) == []
        assert await search.vector("query", nobody, 10) == []

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_vector_search_over_fetches_until_the_scope_filter_leaves_enough_hits(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        fixed = {"query": unit(1, 0), "target doc": unit(0.1, 0.99)}
        for number in range(9):  # nearer to the query than the target, in another repository
            fixed[f"foreign {number}"] = unit(1, 0.01 * (number + 1))
            await world.ingest(f"foreign {number}", f"c-foreign-{number}", repository="repo-2")
        await world.ingest("target doc", "c-target")
        provider = FakeEmbeddingProvider(fixed)
        await world.project(provider)

        # limit 1 fetches 4 candidates first: all foreign; the search must widen to find the target.
        hits = await _service(world, provider).vector("query", SCOPE, 1)

        assert _ids(hits) == ["c-target"]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_purge_removes_lexical_and_vector_material_and_blocks_resurrection(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        provider = FakeEmbeddingProvider({"purge me": unit(1, 0), "keep me": unit(0.9, 0.4)})
        original = await world.ingest("purge me", "c-purged")
        await world.ingest("keep me", "c-kept")
        await world.project(provider)
        search = _service(world, provider)
        assert set(_ids(await search.lexical("me", SCOPE, 10))) == {"c-purged", "c-kept"}
        assert "c-purged" in _ids(await search.vector("purge me", SCOPE, 10))

        await world.purge("c-purged")
        report = await world.project(provider)
        assert report.delivered == 1 and report.dead_lettered == 0

        assert _ids(await search.lexical("me", SCOPE, 10)) == ["c-kept"]
        assert _ids(await search.vector("purge me", SCOPE, 10)) == ["c-kept"]
        assert await world.documents() == ["c-kept"]
        assert await world.embedding_ids() == ["c-kept"]

        # The original event is delivered again (at-least-once): nothing comes back.
        await world.redeliver(original, provider)
        assert await world.documents() == ["c-kept"]
        assert await world.embedding_ids() == ["c-kept"]
        assert _ids(await search.lexical("purge", SCOPE, 10)) == []

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_requests_are_bounded_and_scoped() -> None:
    with pytest.raises(SearchRequestError):
        SearchScope("", "repo-1")
    with pytest.raises(SearchRequestError):
        SearchScope("project-1", "  ")


def test_a_slow_embedder_hits_the_deadline(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    class Slow(FakeEmbeddingProvider):
        async def embed(self, texts):  # type: ignore[no-untyped-def]
            await asyncio.sleep(2)
            return await super().embed(texts)

    async def body(world: SearchWorld) -> None:
        search = SearchService(world.api, world.store.read_only(), Slow(), deadline_seconds=0.05)
        with pytest.raises(SearchTimeoutError):
            await search.vector("anything", SCOPE, 5)

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


class _Boom:
    """Runs after the search projector and fails, so its event is dead-lettered."""

    name = "boom"
    version = "1"

    def handles(self, event_type: str) -> bool:
        return event_type == "agent.turn.started"

    async def project(self, _tx: object, _event: object) -> None:
        raise RuntimeError("graph write failed")


def test_rows_of_dead_lettered_or_undelivered_events_are_never_searchable(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        provider = FakeEmbeddingProvider({"half indexed": unit(1, 0), "query": unit(1, 0)})
        await world.ingest("half indexed", "c-half")

        report = await world.project(provider, extra_projectors=[_Boom()])

        # The PostgreSQL row was committed before the later failure, but the event never delivered.
        assert report.dead_lettered == 1
        assert await world.documents() == ["c-half"]
        search = _service(world, provider)
        assert await search.lexical("half", SCOPE, 10) == []
        assert await search.vector("query", SCOPE, 10) == []
        # A row whose event is merely not delivered yet (a crash before the graph commit) is hidden.
        async with world.owner.begin() as connection:
            await connection.execute(
                text("UPDATE projection.outbox SET status = 'pending', dead_lettered_at = NULL")
            )
        assert await search.lexical("half", SCOPE, 10) == []
        async with world.owner.begin() as connection:
            await connection.execute(
                text("UPDATE projection.outbox SET status = 'delivered', delivered_at = now()")
            )
        assert _ids(await search.lexical("half", SCOPE, 10)) == ["c-half"]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)
