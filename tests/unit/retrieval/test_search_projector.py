"""The search projector's decisions, against doubles: what it indexes, skips and refuses."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from typing import Any

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    ContentRefV1,
    ContentStorage,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)

from agent_context_platform.content.service import ContentUnavailableError
from agent_context_platform.projection.projectors import search
from agent_context_platform.projection.projectors.search import (
    EmbeddingDimensionError,
    SearchBackend,
    SearchBackendNotConfiguredError,
    SearchContentConflictError,
    SearchProjector,
    SearchScopeConflictError,
    extract_text,
)
from agent_context_platform.projection.registry import projectors_with_search, registered_projectors

from .fakes import FakeSessions, FakeTx, StubEmbedder, row

pytestmark = pytest.mark.unit

MOMENT = datetime(2026, 9, 1, 9, tzinfo=UTC)


def _ref(
    content_id: str,
    data: bytes,
    *,
    media_type: str = "text/plain",
    disposition: ContentDisposition = ContentDisposition.SANITIZED,
) -> ContentRefV1:
    digest = hashlib.sha256(data).hexdigest()
    return ContentRefV1(
        content_id=content_id,
        content_sha256=digest,
        media_type=media_type,
        uncompressed_bytes=len(data),
        disposition=disposition,
        storage=ContentStorage.INLINE,
        inline_id=digest,
    )


def _event(
    refs: list[ContentRefV1],
    *,
    event_type: str = "agent.turn.started",
    payload: dict[str, Any] | None = None,
    context: EventContextV1 | None = None,
) -> StoredEventV1:
    draft = EventDraftV1(
        event_type=event_type,
        stream_id="stream-1",
        occurred_at=MOMENT,
        observed_at=MOMENT,
        producer=ProducerV1(producer_id="p", name="Codex", version="1.0.0"),
        payload=payload
        if payload is not None
        else {"source": "codex", "session_id": "s-1", "turn_id": "t-1"},
        context=context
        or EventContextV1(project_id="proj", repository_id="repo", session_id="s-1", turn_id="t-1"),
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key="k-1",
        content_claims=tuple(
            ContentClaimV1(
                content_id=ref.content_id,
                content_sha256=ref.content_sha256,
                media_type=ref.media_type,
                uncompressed_bytes=ref.uncompressed_bytes,
            )
            for ref in sorted(refs, key=lambda item: item.content_id)
        ),
    )
    return seal_event(draft, refs, 1, None)


class FakeContent:
    def __init__(self, blobs: dict[str, bytes]) -> None:
        self.blobs = blobs
        self.reads: list[str] = []

    async def read(self, _session: Any, ref: ContentRefV1) -> bytes:
        self.reads.append(ref.content_id)
        if ref.content_id not in self.blobs:
            raise ContentUnavailableError("gone")
        return self.blobs[ref.content_id]


class Rig:
    def __init__(
        self,
        blobs: dict[str, bytes],
        *,
        tombstoned: frozenset[str] = frozenset(),
        conflict: bool = False,
        existing: Any = None,
        embedder: Any = None,
        write_documents: bool = True,
        lexical: bool = False,
    ) -> None:
        def script(sql: str, params: dict[str, Any]) -> list[Any]:
            if "content_tombstones WHERE" in sql:
                return [row(one=1)] if params["content_id"] in tombstoned else []
            if "INSERT INTO retrieval.search_documents" in sql:
                return [] if conflict else [row(content_id=params["content_id"])]
            if "SELECT project_id, repository_id FROM retrieval.search_documents" in sql:
                return [existing] if existing is not None else []
            return []

        self.sessions = FakeSessions(script)
        self.content = FakeContent(blobs)
        self.embedder = None if lexical else embedder or StubEmbedder()
        self.projector = SearchProjector(
            SearchBackend(self.sessions, self.content, self.embedder, write_documents)  # type: ignore[arg-type]
        )

    def run(self, event: StoredEventV1, graph: Any = None) -> FakeTx:
        tx = FakeTx(graph)
        asyncio.run(self.projector.project(tx, event))  # type: ignore[arg-type]
        return tx


def test_a_content_event_is_upserted_then_embedded_then_linked_to_its_turn() -> None:
    data = b"hello search"
    rig = Rig({"c-1": data})

    tx = rig.run(_event([_ref("c-1", data)]))

    (upsert,) = rig.sessions.statements("INSERT INTO retrieval.search_documents")
    assert (upsert["content_id"], upsert["project_id"], upsert["repository_id"]) == (
        "c-1",
        "proj",
        "repo",
    )
    assert (upsert["session_id"], upsert["body"], upsert["redaction"]) == (
        "s-1",
        "hello search",
        "sanitized",
    )
    (embedding,) = tx.with_fragment("setNodeVectorProperty")
    assert (embedding["content_id"], embedding["model_revision"]) == ("c-1", "f" * 40)
    assert len(embedding["vector"]) == 384
    (link,) = tx.with_fragment("HAS_EMBEDDING")
    assert link["target_id"] == "c-1" and link["source_id"].startswith("turn:")
    assert tx.queries.index(next(q for q in tx.queries if "setNodeVectorProperty" in q[0])) > 0


def test_a_purged_content_id_is_skipped_everywhere() -> None:
    data = b"hello"
    rig = Rig({"c-1": data}, tombstoned=frozenset({"c-1"}))

    tx = rig.run(_event([_ref("c-1", data)]))

    assert rig.content.reads == []  # never even read
    assert rig.sessions.statements("INSERT INTO retrieval.search_documents") == []
    assert tx.queries == []


def test_a_tombstone_written_during_embedding_is_honoured_in_the_writing_transaction() -> None:
    data = b"racy"
    seen = {"lookups": 0}

    def script(sql: str, params: dict[str, Any]) -> list[Any]:
        if "content_tombstones WHERE" in sql:
            seen["lookups"] += 1
            return [row(one=1)] if seen["lookups"] > 1 else []  # purged after the first check
        return [row(content_id="c-1")]

    rig = Rig({"c-1": data})
    rig.sessions.script = script

    tx = rig.run(_event([_ref("c-1", data)]))

    assert rig.sessions.statements("INSERT INTO retrieval.search_documents") == []
    assert tx.queries == []


@pytest.mark.parametrize(
    "case",
    [
        "unscoped_project",
        "unscoped_repository",
        "no_refs",
        "metadata_only",
        "purged_disposition",
        "unsupported_media",
        "blank_text",
    ],
)
def test_nothing_is_indexed_without_scope_sanitized_text(case: str) -> None:
    data = b"  " if case == "blank_text" else b"text"
    kwargs: dict[str, Any] = {}
    context = EventContextV1(
        project_id="proj", repository_id="repo", session_id="s-1", turn_id="t-1"
    )
    if case == "unscoped_project":
        context = EventContextV1(repository_id="repo", session_id="s-1", turn_id="t-1")
    if case == "unscoped_repository":
        context = EventContextV1(project_id="proj", session_id="s-1", turn_id="t-1")
    if case == "metadata_only":
        kwargs["disposition"] = ContentDisposition.METADATA_ONLY
    if case == "purged_disposition":
        kwargs["disposition"] = ContentDisposition.PURGED
    if case == "unsupported_media":
        kwargs["media_type"] = "application/octet-stream"
    refs = [] if case == "no_refs" else [_ref("c-1", data, **kwargs)]
    rig = Rig({"c-1": data})

    tx = rig.run(_event(refs, context=context))

    assert tx.queries == []
    assert rig.sessions.statements("INSERT INTO retrieval.search_documents") == []


def test_content_ids_are_indexed_in_order_in_one_embedding_batch() -> None:
    blobs = {"c-b": b"second", "c-a": b"first"}
    rig = Rig(blobs)
    embedder = StubEmbedder()
    calls: list[list[str]] = []
    original = embedder.embed

    async def spy(texts: Any) -> Any:
        calls.append(list(texts))
        return await original(texts)

    embedder.embed = spy  # type: ignore[method-assign]
    rig.embedder = embedder
    rig.projector = SearchProjector(SearchBackend(rig.sessions, rig.content, embedder))  # type: ignore[arg-type]

    tx = rig.run(_event([_ref("c-b", blobs["c-b"]), _ref("c-a", blobs["c-a"])]))

    assert calls == [["first", "second"]]
    assert [p["content_id"] for p in tx.with_fragment("setNodeVectorProperty")] == ["c-a", "c-b"]


def test_a_non_agent_event_writes_the_embedding_without_an_owner_link() -> None:
    data = b"summary text"
    event = _event(
        [_ref("c-s", data)],
        event_type="knowledge.summary.recorded",
        payload={
            "summary_id": "sum-1",
            "subjects": ["topic"],
            "content_id": "c-s",
            "valid_from": MOMENT.isoformat(),
            "recorded_at": MOMENT.isoformat(),
            "source_event_ids": ["0190f5e0-0000-7000-8000-000000000001"],
        },
        context=EventContextV1(project_id="proj", repository_id="repo"),
    )

    tx = Rig({"c-s": data}).run(event)

    assert len(tx.with_fragment("setNodeVectorProperty")) == 1
    assert tx.with_fragment("HAS_EMBEDDING") == []


def test_unreadable_content_is_an_error_not_a_silent_skip() -> None:
    with pytest.raises(ContentUnavailableError):
        Rig({}).run(_event([_ref("c-1", b"x")]))


def test_a_scope_collision_in_the_database_is_refused_before_the_graph_is_touched() -> None:
    rig = Rig({"c-1": b"x"}, conflict=True)

    with pytest.raises(SearchScopeConflictError):
        rig.run(_event([_ref("c-1", b"x")]))


@pytest.mark.parametrize(
    ("existing", "error"),
    [
        (row(project_id="proj", repository_id="repo"), SearchContentConflictError),
        (row(project_id="proj", repository_id="other-repo"), SearchScopeConflictError),
        (None, SearchScopeConflictError),
    ],
)
def test_a_refused_upsert_is_named_as_a_content_or_a_scope_collision_without_content(
    existing: Any, error: type[Exception]
) -> None:
    rig = Rig({"c-1": b"secret body"}, conflict=True, existing=existing)

    with pytest.raises(error) as raised:
        rig.run(_event([_ref("c-1", b"secret body")]))

    assert "secret" not in str(raised.value) and "c-1" not in str(raised.value)
    (upsert,) = rig.sessions.statements("INSERT INTO retrieval.search_documents")
    assert upsert["content_sha256"] == hashlib.sha256(b"secret body").hexdigest()
    assert upsert["media_type"] == "text/plain"


@pytest.mark.parametrize(
    ("record", "error"),
    [
        ({"same_scope": True, "same_content": False}, SearchContentConflictError),
        ({"same_scope": False, "same_content": True}, SearchScopeConflictError),
        ({"same_scope": True, "same_content": None}, SearchContentConflictError),
    ],
)
def test_an_embedding_node_of_other_content_or_scope_is_never_overwritten(
    record: dict[str, Any], error: type[Exception]
) -> None:
    def graph(query: str, _parameters: dict[str, Any]) -> list[dict[str, Any]]:
        return [record] if "same_content" in query else []

    rig = Rig({"c-1": b"x"}, write_documents=False)  # a replay: no PostgreSQL check runs

    with pytest.raises(error):
        rig.run(_event([_ref("c-1", b"x")]), graph)
    tx = FakeTx(graph)
    with pytest.raises(error):
        asyncio.run(rig.projector.project(tx, _event([_ref("c-1", b"x")])))  # type: ignore[arg-type]
    assert tx.with_fragment("setNodeVectorProperty") == []


def test_the_same_content_under_the_same_scope_is_merged_in_the_graph() -> None:
    def graph(query: str, _parameters: dict[str, Any]) -> list[dict[str, Any]]:
        return [{"same_scope": True, "same_content": True}] if "same_content" in query else []

    tx = Rig({"c-1": b"x"}).run(_event([_ref("c-1", b"x")]), graph)

    (identity,) = tx.with_fragment("same_content")
    assert identity["content_sha256"] == hashlib.sha256(b"x").hexdigest()
    (upsert,) = tx.with_fragment("setNodeVectorProperty")
    assert upsert["media_type"] == "text/plain"


def test_resetting_the_tables_is_one_call_of_the_reset_and_reseed_function() -> None:
    sessions = FakeSessions()

    async def reset() -> int:
        async with sessions() as session:
            return await search.reset_search_tables(session)  # type: ignore[arg-type]

    asyncio.run(reset())

    # One SECURITY DEFINER call: the projector role holds no TRUNCATE and no tombstone DELETE.
    assert [sql for sql, _params in sessions.log] == ["SELECT retrieval.reset_search_projection()"]


def test_vectors_of_the_wrong_size_are_refused() -> None:
    class Short(StubEmbedder):
        async def embed(self, texts: Any) -> Any:
            from agent_context_platform.retrieval.embeddings import EmbeddedText

            return [EmbeddedText((1.0,), truncated=False) for _ in texts]

    class Fewer(StubEmbedder):
        async def embed(self, texts: Any) -> Any:
            return []

    for embedder in (Short(), Fewer()):
        with pytest.raises(EmbeddingDimensionError):
            Rig({"c-1": b"x"}, embedder=embedder).run(_event([_ref("c-1", b"x")]))


def test_an_unconfigured_projector_fails_closed_on_content_and_purges_but_not_on_empty_events() -> (
    None
):
    projector = SearchProjector()
    tx = FakeTx()

    with pytest.raises(SearchBackendNotConfiguredError):
        asyncio.run(projector.project(tx, _event([_ref("c-1", b"x")])))  # type: ignore[arg-type]
    purge = _event(
        [],
        event_type="content.purged",
        payload=_purge_payload("c-1"),
        context=EventContextV1(),
    )
    with pytest.raises(SearchBackendNotConfiguredError):
        asyncio.run(projector.project(tx, purge))  # type: ignore[arg-type]
    asyncio.run(projector.project(tx, _event([])))  # type: ignore[arg-type]  # nothing to index
    assert tx.queries == []


def _purge_payload(*content_ids: str) -> dict[str, Any]:
    return {
        "purge_request_id": "req-1",
        "purged_contents": [
            {"content_id": cid, "content_sha256": hashlib.sha256(cid.encode()).hexdigest()}
            for cid in sorted(content_ids)
        ],
        "reason": "operator_request",
        "purged_at": MOMENT.isoformat(),
        "affected_event_count": 1,
        "storage_kinds": ["inline"],
    }


def test_a_purge_tombstones_deletes_rows_then_deletes_nodes() -> None:
    rig = Rig({})
    event = _event(
        [],
        event_type="content.purged",
        payload=_purge_payload("c-2", "c-1"),
        context=EventContextV1(),
    )

    tx = rig.run(event)

    assert [
        p["content_id"] for p in rig.sessions.statements("INSERT INTO retrieval.content_tombstones")
    ] == [
        "c-1",
        "c-2",
    ]
    (delete,) = rig.sessions.statements("DELETE FROM retrieval.search_documents")
    assert delete == {"content_ids": ["c-1", "c-2"]}
    (nodes,) = tx.with_fragment("DETACH DELETE")
    assert nodes == {"content_ids": ["c-1", "c-2"]}


def test_the_registry_binds_a_backend_to_the_search_projector_only() -> None:
    rig = Rig({})

    bound = projectors_with_search(rig.projector._backend)  # type: ignore[arg-type]

    assert [item.name for item in bound] == [item.name for item in registered_projectors()]
    assert next(item for item in bound if item.name == "search") is not registered_projectors()[-1]
    assert bound[:-1] == registered_projectors()[:-1]
    assert bound[-1]._backend is rig.projector._backend  # type: ignore[attr-defined]
    assert registered_projectors()[-1]._backend is None  # type: ignore[attr-defined]


def test_text_extraction_flattens_json_strings_and_bounds_the_result() -> None:
    document = b'{"a":"one","b":["two",{"c":"three"},4],"d":null,"e":{"f":"four"}}'

    assert extract_text("application/json; charset=utf-8", document) == "one\ntwo\nthree\nfour"
    assert extract_text("application/json", b"not json") == "not json"
    assert extract_text("text/plain", "ação".encode()) == "ação"
    assert extract_text("text/plain", b"bad \xff byte") == "bad � byte"
    # PostgreSQL text cannot hold NUL: raw or JSON-escaped, it is indexed as a space.
    assert extract_text("text/plain", b"a\x00b") == "a b"
    assert extract_text("application/json", b'{"out": "x\\u0000y"}') == "x y"
    assert extract_text("application/json", b"not\x00json") == "not json"
    assert len(extract_text("text/plain", b"x" * (search.MAX_INDEXED_CHARS + 5))) == (
        search.MAX_INDEXED_CHARS
    )
    deep = b"[" * 5000 + b'"x"' + b"]" * 5000
    assert extract_text("application/json", deep)  # no recursion limit is hit


def test_a_replay_target_embeds_and_checks_tombstones_but_never_writes_live_postgresql() -> None:
    data = b"replayed text"
    rig = Rig(
        {"c-1": data, "c-2": b"purged text"}, tombstoned=frozenset({"c-2"}), write_documents=False
    )

    tx = rig.run(_event([_ref("c-1", data), _ref("c-2", b"purged text")]))

    assert [p["content_id"] for p in tx.with_fragment("setNodeVectorProperty")] == ["c-1"]
    assert rig.sessions.statements("INSERT INTO") == []
    assert rig.sessions.statements("DELETE FROM") == []
    purge = _event(
        [], event_type="content.purged", payload=_purge_payload("c-1"), context=EventContextV1()
    )
    tx = rig.run(purge)
    assert tx.with_fragment("DETACH DELETE") == [{"content_ids": ["c-1"]}]
    assert rig.sessions.statements("INSERT INTO") == []
    assert rig.sessions.statements("DELETE FROM") == []


def test_a_replay_remembers_a_purge_that_precedes_its_original_event() -> None:
    data = b"purged first"
    rig = Rig({"c-1": data}, write_documents=False)
    purge = _event(
        [], event_type="content.purged", payload=_purge_payload("c-1"), context=EventContextV1()
    )

    rig.run(purge)
    tx = rig.run(_event([_ref("c-1", data)]))

    assert tx.with_fragment("setNodeVectorProperty") == []
    assert rig.content.reads == []
    assert rig.sessions.statements("INSERT INTO") == []
    # Another projector instance (a fresh replay) starts with no memory of it.
    other = Rig({"c-1": data}, write_documents=False)
    assert len(other.run(_event([_ref("c-1", data)])).with_fragment("setNodeVectorProperty")) == 1


def test_indexing_and_purging_take_the_content_lock_before_checking_or_writing() -> None:
    rig = Rig({"c-b": b"b", "c-a": b"a"})

    rig.run(_event([_ref("c-b", b"b"), _ref("c-a", b"a")]))

    statements = [sql for sql, _params in rig.sessions.log]
    locks = [i for i, sql in enumerate(statements) if "pg_advisory_xact_lock" in sql]
    checks = [i for i, sql in enumerate(statements) if "content_tombstones WHERE" in sql]
    upsert = next(
        i for i, sql in enumerate(statements) if "INSERT INTO retrieval.search_documents" in sql
    )
    assert [p["key"] for _sql, p in rig.sessions.log if "pg_advisory" in _sql] == [
        "agent-context.search.content:c-a",
        "agent-context.search.content:c-b",
    ]
    assert max(locks) < upsert and checks[-1] > locks[0]

    purge = Rig({})
    purge.run(
        _event(
            [],
            event_type="content.purged",
            payload=_purge_payload("c-2", "c-1"),
            context=EventContextV1(),
        )
    )
    purge_statements = [sql for sql, _params in purge.sessions.log]
    assert "pg_advisory_xact_lock" in purge_statements[0]
    assert "INSERT INTO retrieval.content_tombstones" in purge_statements[2]


def test_an_unconfigured_projector_neither_purges_nor_indexes() -> None:
    projector = SearchProjector()
    purge = _event(
        [], event_type="content.purged", payload=_purge_payload("c-1"), context=EventContextV1()
    )

    for event in (purge, _event([_ref("c-1", b"x")])):
        with pytest.raises(SearchBackendNotConfiguredError):
            asyncio.run(projector.project(FakeTx(), event))  # type: ignore[arg-type]


def test_seeded_ledger_purges_are_neither_read_nor_embedded() -> None:
    data = b"bytes already deleted"
    sessions = FakeSessions(
        lambda sql, _p: [row_id for row_id in ("c-1",)] if "jsonb_array_elements" in sql else []
    )
    rig = Rig({}, write_documents=True)  # the content reader would raise: no blob exists
    seeded = asyncio.run(rig.projector.seed_purges(sessions))  # type: ignore[arg-type]

    tx = rig.run(_event([_ref("c-1", data)]))

    assert seeded == 1
    assert rig.content.reads == [] and tx.queries == []
    assert rig.sessions.statements("INSERT INTO") == []


def test_lexical_only_writes_checked_rows_and_no_graph_node(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rig = Rig({"c-1": b"secret body", "c-2": b"other body"}, lexical=True)

    with caplog.at_level("WARNING"):
        tx = rig.run(_event([_ref("c-1", b"secret body")]))
        rig.run(_event([_ref("c-2", b"other body")]))

    upserts = rig.sessions.statements("INSERT INTO retrieval.search_documents")
    assert [p["content_id"] for p in upserts] == ["c-1", "c-2"]
    assert upserts[0]["content_sha256"] == hashlib.sha256(b"secret body").hexdigest()
    assert rig.sessions.statements("content_tombstones WHERE")  # tombstones are honoured
    assert tx.queries == []  # no embedding, no node, no link
    assert rig.projector.unembedded_events == 2
    assert len(caplog.records) == 1 and "secret" not in caplog.text and "c-1" not in caplog.text


def test_lexical_only_dead_letters_conflicts_and_unreadable_content_like_a_full_index() -> None:
    conflicting = Rig(
        {"c-1": b"x"},
        lexical=True,
        conflict=True,
        existing=row(project_id="proj", repository_id="repo"),
    )
    with pytest.raises(SearchContentConflictError):
        conflicting.run(_event([_ref("c-1", b"x")]))
    with pytest.raises(ContentUnavailableError):
        Rig({}, lexical=True).run(_event([_ref("c-1", b"x")]))
    assert Rig({}, lexical=True).projector.unembedded_events == 0


def test_lexical_only_still_purges_rows_tombstones_and_nodes() -> None:
    rig = Rig({}, lexical=True)
    tx = FakeTx()
    purge = _event(
        [], event_type="content.purged", payload=_purge_payload("c-1"), context=EventContextV1()
    )

    asyncio.run(rig.projector.project(tx, purge))  # type: ignore[arg-type]

    assert [
        p["content_id"] for p in rig.sessions.statements("INSERT INTO retrieval.content_tombstones")
    ] == ["c-1"]
    assert tx.with_fragment("DETACH DELETE") == [{"content_ids": ["c-1"]}]


def test_a_graph_replay_without_an_embedder_is_refused_at_construction() -> None:
    with pytest.raises(ValueError, match="embedding provider"):
        SearchBackend(FakeSessions(), FakeContent({}), None, write_documents=False)  # type: ignore[arg-type]


def test_the_search_projector_is_never_named_in_the_history_marker() -> None:
    from agent_context_platform.projection.schema import history_projectors

    for projector in (Rig({}).projector, Rig({}, lexical=True).projector, SearchProjector()):
        assert history_projectors([projector]) == []
