"""The search projector against real PostgreSQL and Neo4j (PLATFORM-042).

Idempotent replay, purge ordering, scope safety, fail-closed configuration and the P039
rebuild/verify contract with the projector registered.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.projection.projectors.search import (
    INDEXED_EVENT_TYPES,
    EmbeddingDimensionError,
    SearchProjector,
)
from agent_context_platform.projection.registry import (
    projectors_with_search,
    registered_projectors,
)
from agent_context_platform.projection.runtime import ProjectionRunner
from agent_context_platform.projection.schema import ensure_schema
from agent_context_platform.projection.verify import (
    compute_graph_digest,
    replay_ledger,
    verify_projections,
    wipe_graph,
)
from agent_context_platform.retrieval.embeddings import EmbeddedText
from agent_context_platform.retrieval.search import SearchScope, SearchService

from .fake_embeddings import FakeEmbeddingProvider, unit
from .search_harness import SearchWorld, with_world

pytestmark = pytest.mark.integration

SCOPE = SearchScope("project-1", "repo-1")


async def _rows(world: SearchWorld, query: str, **params: Any) -> list[Any]:
    async with world.owner.connect() as connection:
        return list((await connection.execute(text(query), params)).all())


async def _cypher(world: SearchWorld, query: Any, **params: Any) -> list[dict[str, Any]]:
    async def read(tx: Any) -> list[dict[str, Any]]:
        return [record.data() for record in (await tx.run(query, parameters=params)).records]

    return await world.store.execute_read(read)


async def _node_count(world: SearchWorld) -> int:
    return int((await _cypher(world, "MATCH (n) RETURN count(n) AS n"))[0]["n"])


def test_the_projector_handles_content_events_and_purges_and_is_registered() -> None:
    projector = SearchProjector()

    assert {"agent.turn.started", "agent.tool_call.completed", "knowledge.summary.recorded"} <= (
        INDEXED_EVENT_TYPES
    )
    assert all(projector.handles(kind) for kind in INDEXED_EVENT_TYPES)
    assert projector.handles("content.purged")
    assert not projector.handles("agent.session.started")
    assert not projector.handles("code.file.indexed")
    assert [item.name for item in registered_projectors()][-1] == "search"


def test_a_projected_event_leaves_one_row_and_one_node_without_the_text(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        secret_free = "plain searchable sentence"
        event_id = await world.ingest(secret_free, "c-1")
        await world.project()

        (row,) = await _rows(
            world,
            "SELECT content_id, event_id, source_event_ids, event_type, project_id, "
            "repository_id, session_id, redaction, tsv::text FROM retrieval.search_documents",
        )
        assert (row.content_id, str(row.event_id)) == ("c-1", str(event_id))
        assert [str(item) for item in row.source_event_ids] == [str(event_id)]
        assert (row.project_id, row.repository_id, row.session_id) == (
            "project-1",
            "repo-1",
            "session-1",
        )
        assert row.redaction == "sanitized"
        assert "searchable" in row.tsv  # lexemes, not the stored text
        nodes = await _cypher(world, "MATCH (n:ContentEmbedding) RETURN properties(n) AS p")
        (props,) = [item["p"] for item in nodes]
        assert set(props) == {
            "content_id",
            "model_id",
            "model_revision",
            "content_sha256",
            "truncated",
            "project_id",
            "repository_id",
            "source_event_ids",
            "embedding",
        }
        assert secret_free not in str(props)
        assert props["model_revision"] == world.embeddings.model_revision
        assert len(props["embedding"]) == 384
        links = await _cypher(
            world,
            "MATCH (o:Turn)-[r:HAS_EMBEDDING]->(n:ContentEmbedding {content_id: 'c-1'}) "
            "RETURN r.source_event_ids AS sources",
        )
        assert links == [{"sources": [str(event_id)]}]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_replaying_an_unchanged_event_creates_no_duplicates(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        first = await world.ingest("replayed content", "c-replay")
        await world.project()
        digest = (await compute_graph_digest(world.store)).digest
        nodes = await _node_count(world)

        for _ in range(3):
            await world.redeliver(first)

        assert await world.documents() == ["c-replay"]
        assert await world.embedding_ids() == ["c-replay"]
        assert await _node_count(world) == nodes
        assert (await compute_graph_digest(world.store)).digest == digest
        (row,) = await _rows(world, "SELECT source_event_ids FROM retrieval.search_documents")
        assert len(row.source_event_ids) == 1

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_one_content_object_in_two_events_merges_provenance_whatever_the_order(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        from .search_harness import BASE_TIME

        later = await world.ingest(
            "shared body", "c-shared", session="session-late", at=BASE_TIME.replace(hour=12)
        )
        earlier = await world.ingest(
            "shared body", "c-shared", session="session-early", at=BASE_TIME
        )
        await world.project()

        (row,) = await _rows(
            world,
            "SELECT event_id, source_event_ids, session_id, occurred_at "
            "FROM retrieval.search_documents",
        )
        assert str(row.event_id) == str(earlier)  # the earliest event's metadata wins
        assert row.session_id == "session-early"
        assert row.occurred_at == BASE_TIME
        assert {str(item) for item in row.source_event_ids} == {str(earlier), str(later)}
        (node,) = await _cypher(world, "MATCH (n:ContentEmbedding) RETURN n.source_event_ids AS s")
        assert node["s"] == sorted([str(earlier), str(later)])

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_content_already_indexed_under_another_scope_is_refused_not_merged(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.ingest("collision body", "c-collision", repository="repo-1")
        await world.project()
        await world.ingest("collision body", "c-collision", repository="repo-2")

        report = await world.project()

        assert report.dead_lettered == 1
        (row,) = await _rows(
            world,
            "SELECT repository_id, cardinality(source_event_ids) AS n FROM retrieval.search_documents",
        )
        assert (row.repository_id, row.n) == ("repo-1", 1)
        (node,) = await _cypher(world, "MATCH (n:ContentEmbedding) RETURN n.repository_id AS r")
        assert node["r"] == "repo-1"

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_a_purge_delivered_before_the_original_event_still_wins(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.purge("c-early")  # first in the outbox
        await world.ingest("arrives after its purge", "c-early")

        report = await world.project()

        assert report.delivered == 2 and report.dead_lettered == 0
        assert await world.documents() == []
        assert await world.embedding_ids() == []
        tombstones = await _rows(world, "SELECT content_id FROM retrieval.content_tombstones")
        assert [row.content_id for row in tombstones] == ["c-early"]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_purging_content_that_was_never_indexed_is_a_clean_no_op(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.purge("c-never", "c-never-either")
        report = await world.project()
        assert report.delivered == 1 and report.dead_lettered == 0
        await world.project()  # nothing pending
        tombstones = await _rows(world, "SELECT count(*) AS n FROM retrieval.content_tombstones")
        assert tombstones[0].n == 2

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_unconfigured_search_fails_closed_instead_of_skipping(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.ingest("needs an embedder", "c-unconfigured")
        runner = ProjectionRunner(
            world.projector_sessions,
            world.store,
            registered_projectors(),
            worker_id="unconfigured",
            max_attempts=1,
        )

        report = await runner.run_once(10)

        assert (report.claimed, report.dead_lettered) == (1, 1)
        assert await world.documents() == []
        letters = await _rows(
            world, "SELECT error_class, projector_name FROM projection.dead_letters"
        )
        assert [(row.error_class, row.projector_name) for row in letters] == [
            ("SearchBackendNotConfiguredError", "search")
        ]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_a_provider_with_the_wrong_vector_size_is_rejected_before_any_write(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    class Short(FakeEmbeddingProvider):
        async def embed(self, texts: Sequence[str]) -> list[EmbeddedText]:
            return [EmbeddedText(vector=(1.0, 0.0), truncated=False) for _ in texts]

    async def body(world: SearchWorld) -> None:
        await world.ingest("short vectors", "c-short")

        report = await world.project(Short())

        assert report.dead_lettered == 1
        assert await world.documents() == [] and await world.embedding_ids() == []
        assert EmbeddingDimensionError.__name__ in str(
            await _rows(world, "SELECT error_class FROM projection.dead_letters")
        )

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_events_without_a_full_scope_or_text_are_not_indexed(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.ingest("no project", "c-a", project=None)
        await world.ingest("   ", "c-blank")
        report = await world.project()
        assert report.delivered == 2
        assert await world.documents() == [] and await world.embedding_ids() == []

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_the_rebuilt_graph_matches_with_the_search_projector_registered(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        provider = FakeEmbeddingProvider({"kept text": unit(1, 0)})
        await world.ingest("kept text", "c-kept")
        await world.ingest("second text", "c-second", session="session-2", turn="turn-2")
        gone = await world.ingest("will be purged", "c-gone")
        await world.purge("c-gone")
        report = await world.project(provider)
        assert report.dead_lettered == 0
        assert gone

        from agent_context_platform.projection.registry import projectors_with_search

        verification = await verify_projections(
            world.projector_sessions,
            world.store,
            projectors_with_search(world.backend(provider)),
            require_caught_up=True,
            replay_target=world.store,
            wipe_replay_target=True,
        )

        assert verification.ok(require_caught_up=True), verification.to_dict(require_caught_up=True)
        assert verification.replay_digest == verification.graph.digest
        assert verification.graph.nodes["ContentEmbedding"] == 2
        assert await world.embedding_ids() == ["c-kept", "c-second"]  # rebuilt, minus the purge

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_the_digest_ignores_vector_floats_but_not_the_model_revision(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.ingest("digest subject", "c-digest")
        await world.project()
        before = (await compute_graph_digest(world.store)).digest

        async def jitter(tx: Any) -> None:
            await tx.run(
                "MATCH (n:ContentEmbedding) "
                "CALL db.create.setNodeVectorProperty(n, 'embedding', $vector)",
                parameters={"vector": list(unit(0.5, 0.5))},
            )

        await world.store.execute_write(jitter)
        assert (await compute_graph_digest(world.store)).digest == before

        async def other_model(tx: Any) -> None:
            await tx.run("MATCH (n:ContentEmbedding) SET n.model_revision = 'other'", parameters={})

        await world.store.execute_write(other_model)
        assert (await compute_graph_digest(world.store)).digest != before

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_a_replay_check_or_standby_rebuild_never_writes_live_search_tables(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.ingest("kept text", "c-kept")
        await world.ingest("deleted live row", "c-lost")
        await world.ingest("purged later", "c-gone")
        await world.purge("c-gone")
        assert (await world.project()).dead_lettered == 0
        async with world.owner.begin() as connection:
            await connection.execute(
                text("DELETE FROM retrieval.search_documents WHERE content_id = 'c-lost'")
            )

        async def live_state() -> tuple[list[Any], list[Any]]:
            docs = await _rows(
                world, "SELECT content_id, indexed_at FROM retrieval.search_documents ORDER BY 1"
            )
            tombs = await _rows(world, "SELECT * FROM retrieval.content_tombstones ORDER BY 1")
            return [tuple(r) for r in docs], [tuple(r) for r in tombs]

        before = await live_state()
        projectors = projectors_with_search(world.backend(write_documents=False))

        verification = await verify_projections(
            world.projector_sessions,
            world.store,
            projectors,
            replay_target=world.store,
            wipe_replay_target=True,
        )
        assert verification.replay_digest is not None
        assert await live_state() == before  # replay-check: row still absent, nothing touched

        await world.purge("c-kept")  # still pending: a standby replay must not apply it live
        await wipe_graph(world.store)  # what a standby rebuild runs
        await ensure_schema(world.store)
        await replay_ledger(world.projector_sessions, world.store, projectors, delivered_only=False)
        assert await live_state() == before  # no tombstone, no delete, no new indexed_at
        # In the target only: c-kept purged by the pending event, c-gone tombstoned, c-lost embedded.
        assert await world.embedding_ids() == ["c-lost"]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_every_search_grant_is_checked_by_preflight_before_anything_is_mutated(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    from agent_context_platform import cli
    from agent_context_platform.projection.verify import MissingGrantError, preflight
    from agent_context_platform.settings import Settings

    grants = cli._search_grants(Settings(search={"model_dir": "/models"}), write_documents=True)
    assert len(grants) >= 12

    async def body(world: SearchWorld) -> None:
        await world.ingest("graph must survive", "c-intact")
        await world.project()
        graph = await world.embedding_ids()
        checkpoints = await _rows(world, "SELECT * FROM projection.projection_checkpoints")
        assert graph == ["c-intact"] and checkpoints
        await preflight(world.projector_sessions, None, write_checkpoints=True, extra_grants=grants)

        for item in grants:
            target = "agent_context_projector"
            privilege = f"{item.privilege} ({item.column})" if item.column else item.privilege
            async with world.owner.begin() as connection:
                await connection.execute(text(f"REVOKE {privilege} ON {item.obj} FROM {target}"))
            try:
                with pytest.raises(MissingGrantError) as raised:
                    await preflight(
                        world.projector_sessions, None, write_checkpoints=True, extra_grants=grants
                    )
                assert item.obj in str(raised.value)
                assert await world.embedding_ids() == graph  # nothing was wiped or reset
                assert len(
                    await _rows(world, "SELECT * FROM projection.projection_checkpoints")
                ) == len(checkpoints)
            finally:
                async with world.owner.begin() as connection:
                    await connection.execute(text(f"GRANT {privilege} ON {item.obj} TO {target}"))

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_indexing_waits_for_a_purge_of_the_same_content_and_then_skips_it(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    import asyncio

    from agent_context_platform.projection.projectors.search import CONTENT_LOCK_PREFIX

    async def body(world: SearchWorld) -> None:
        original = await world.ingest("racy content", "c-race")
        async with world.owner.connect() as purge:
            # The purge transaction holds the content lock; the index passes its first tombstone
            # check (none yet), reads and embeds, then must wait before writing.
            await purge.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": CONTENT_LOCK_PREFIX + "c-race"},
            )
            index = asyncio.create_task(world.redeliver(original))
            done, _pending = await asyncio.wait({index}, timeout=2)
            assert not done  # blocked behind the purge, not racing it
            await purge.execute(
                text(
                    "INSERT INTO retrieval.content_tombstones "
                    "VALUES ('c-race', gen_random_uuid(), now())"
                )
            )
            await purge.commit()
        await asyncio.wait_for(index, 30)

        assert await world.documents() == [] and await world.embedding_ids() == []

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_a_graph_only_replay_honours_a_purge_that_precedes_its_original_event(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: SearchWorld) -> None:
        await world.purge("c-early")
        await world.ingest("arrives after its purge", "c-early")  # nothing is projected live
        projectors = projectors_with_search(world.backend(write_documents=False))

        await replay_ledger(world.projector_sessions, world.store, projectors, delivered_only=False)

        assert await world.embedding_ids() == []
        assert await _rows(world, "SELECT 1 FROM retrieval.content_tombstones") == []

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_an_in_place_rebuild_resets_the_search_tables_and_rebuilds_them_from_the_ledger(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    from agent_context_platform import cli
    from agent_context_platform.projection.verify import rebuild_projections

    async def body(world: SearchWorld) -> None:
        await world.ingest("first text", "c-a")
        await world.ingest("second text", "c-b", session="session-2", turn="turn-2")
        await world.ingest("to be purged", "c-gone")
        await world.purge("c-gone")
        assert (await world.project()).dead_lettered == 0
        expected = await _rows(
            world,
            "SELECT content_id, source_event_ids, event_type, tsv::text FROM retrieval.search_documents ORDER BY 1",
        )
        async with world.owner.begin() as connection:  # drift: an orphan row and a stale tombstone
            await connection.execute(
                text(
                    "INSERT INTO retrieval.search_documents (content_id, event_id, "
                    "source_event_ids, event_type, project_id, repository_id, occurred_at, "
                    "redaction, tsv) SELECT 'c-orphan', event_id, ARRAY[event_id], 'x', 'p', 'r', "
                    "now(), 'sanitized', to_tsvector('simple', 'orphan') FROM ledger.events LIMIT 1"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO retrieval.content_tombstones "
                    "VALUES ('c-stale', gen_random_uuid(), now())"
                )
            )

        projectors = projectors_with_search(world.backend())
        report = await rebuild_projections(
            world.projector_sessions,
            world.store,
            projectors,
            target_description="live",
            in_place=True,
            runner_quiet_seconds=0,
            before_replay=lambda: cli._prepare_search_replay(projectors, world.projector_sessions),
        )

        assert report.ok
        assert await world.documents() == ["c-a", "c-b"]  # orphan gone, purged content stays out
        rebuilt = await _rows(
            world,
            "SELECT content_id, source_event_ids, event_type, tsv::text FROM retrieval.search_documents ORDER BY 1",
        )
        assert [tuple(r) for r in rebuilt] == [tuple(r) for r in expected]
        tombstones = await _rows(world, "SELECT content_id FROM retrieval.content_tombstones")
        assert [r.content_id for r in tombstones] == ["c-gone"]  # from the replayed purge event
        assert await world.embedding_ids() == ["c-a", "c-b"]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_purges_work_on_a_deployment_with_no_embedding_model(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    from agent_context_platform.projection.projectors.search import PurgeAccess
    from agent_context_platform.projection.registry import projectors_with_purge

    async def body(world: SearchWorld) -> None:
        await world.ingest("searchable until purged", "c-purge")
        await world.ingest("stays", "c-stay")
        await world.project()
        assert await world.documents() == ["c-purge", "c-stay"]
        await world.purge("c-purge")

        report = await world.project(
            projectors=projectors_with_purge(PurgeAccess(world.projector_sessions))
        )

        assert (report.delivered, report.dead_lettered) == (1, 0)
        assert await world.documents() == ["c-stay"]
        assert await world.embedding_ids() == ["c-stay"]
        tombstones = await _rows(world, "SELECT content_id FROM retrieval.content_tombstones")
        assert [r.content_id for r in tombstones] == ["c-purge"]

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def test_an_in_place_rebuild_survives_content_whose_bytes_were_really_purged(
    postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    from agent_context_platform import cli
    from agent_context_platform.projection.verify import rebuild_projections

    async def body(world: SearchWorld) -> None:
        await world.ingest("kept text", "c-kept")
        await world.ingest("bytes will vanish", "c-gone")
        await world.purge("c-gone")
        assert (await world.project()).dead_lettered == 0
        gone_digest = hashlib.sha256(b"bytes will vanish").hexdigest()
        async with world.owner.begin() as connection:  # what a real purge does to the bytes
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(
                text("DELETE FROM catalog.inline_contents WHERE content_sha256 = :d"),
                {"d": gone_digest},
            )
        projectors = projectors_with_search(world.backend())

        report = await rebuild_projections(
            world.projector_sessions,
            world.store,
            projectors,
            target_description="live",
            in_place=True,
            runner_quiet_seconds=0,
            before_replay=lambda: cli._prepare_search_replay(projectors, world.projector_sessions),
        )

        assert report.ok  # the original event was replayed before its purge, without reading bytes
        assert await world.documents() == ["c-kept"]
        assert await world.embedding_ids() == ["c-kept"]
        tombstones = await _rows(world, "SELECT content_id FROM retrieval.content_tombstones")
        assert [r.content_id for r in tombstones] == ["c-gone"]  # rebuilt from the purge event
        assert await _service(world).lexical("vanish", SCOPE, 5) == []

    with_world(body, dsn=postgres_dsn, owner=projection_engine)


def _service(world: SearchWorld) -> SearchService:
    return SearchService(world.api, world.store.read_only(), world.embeddings)
