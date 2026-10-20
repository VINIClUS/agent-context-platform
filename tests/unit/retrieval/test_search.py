"""Search request rules and the vector over-fetch loop, against doubles."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from agent_context_platform.retrieval.search import (
    MAX_CANDIDATES,
    MAX_LIMIT,
    SearchRequestError,
    SearchScope,
    SearchService,
    SearchTimeoutError,
)

from .fakes import FakeGraph, FakeSessions, FakeTx, StubEmbedder, row

pytestmark = pytest.mark.unit

SCOPE = SearchScope("project-1", "repo-1")


def _document(content_id: str, **changes: Any) -> Any:
    values: dict[str, Any] = {
        "content_id": content_id,
        "project_id": "project-1",
        "repository_id": "repo-1",
        "session_id": "session-1",
        "event_id": uuid4(),
        "event_type": "agent.turn.started",
        "occurred_at": datetime(2026, 9, 1, tzinfo=UTC),
        "source_event_ids": [uuid4()],
        "redaction": "sanitized",
        "score": 0.5,
    }
    values.update(changes)
    return row(**values)


@pytest.mark.parametrize(
    ("query", "limit"),
    [("", 5), ("   ", 5), ("x" * 1001, 5), ("ok", 0), ("ok", MAX_LIMIT + 1), ("ok", -1)],
)
def test_bad_queries_and_limits_are_rejected_before_any_io(query: str, limit: int) -> None:
    sessions = FakeSessions()
    service = SearchService(sessions)

    with pytest.raises(SearchRequestError):
        asyncio.run(service.lexical(query, SCOPE, limit))
    with pytest.raises(SearchRequestError):
        asyncio.run(service.vector(query, SCOPE, limit))
    assert sessions.log == []


def test_a_search_needs_both_a_project_and_a_repository() -> None:
    with pytest.raises(SearchRequestError):
        SearchScope(" ", "repo-1")
    with pytest.raises(SearchRequestError):
        SearchScope("project-1", "")
    with pytest.raises(ValueError, match="deadline"):
        SearchService(FakeSessions(), deadline_seconds=0)


def test_vector_search_needs_a_graph_and_an_embedder() -> None:
    with pytest.raises(SearchRequestError, match="needs a graph"):
        asyncio.run(SearchService(FakeSessions()).vector("hello", SCOPE, 5))


def test_lexical_search_binds_scope_limit_and_deadline_and_maps_rows() -> None:
    document = _document("c-1", score=0.25)
    sessions = FakeSessions(
        lambda sql, _params: [document] if "websearch_to_tsquery" in sql else []
    )
    service = SearchService(sessions, deadline_seconds=2.5)

    (hit,) = asyncio.run(service.lexical("  migration  ", SCOPE, 7))

    assert (hit.content_id, hit.score, hit.redaction) == ("c-1", 0.25, "sanitized")
    assert hit.source_event_ids == tuple(str(item) for item in document.source_event_ids)
    (query,) = sessions.statements("websearch_to_tsquery")
    assert query == {
        "query": "migration",
        "project_id": "project-1",
        "repository_id": "repo-1",
        "limit": 7,
    }
    (timeout,) = sessions.statements("statement_timeout")
    assert timeout == {"millis": "2500"}


def _vector_service(
    sessions: FakeSessions, graph_rows: list[list[dict[str, Any]]], **options: Any
) -> tuple[SearchService, FakeTx]:
    answers = iter(graph_rows)
    tx = FakeTx(lambda _query, _parameters: next(answers))
    service = SearchService(sessions, FakeGraph(tx), StubEmbedder(), **options)  # type: ignore[arg-type]
    return service, tx


def _candidates(*ids: str, raw: int) -> list[dict[str, Any]]:
    return [
        {"raw_count": raw, "content_id": cid, "score": 0.9 - i / 10} for i, cid in enumerate(ids)
    ]


def test_vector_search_keeps_graph_order_and_drops_hits_without_a_document() -> None:
    documents = {"c-1": _document("c-1"), "c-3": _document("c-3")}  # c-2 was purged
    sessions = FakeSessions(
        lambda sql, params: (
            [documents[c] for c in params["content_ids"] if c in documents]
            if "content_ids" in params
            else []
        )
    )
    service, tx = _vector_service(sessions, [_candidates("c-1", "c-2", "c-3", raw=3)])

    hits = asyncio.run(service.vector("hello", SCOPE, 5))

    assert [(hit.content_id, hit.score) for hit in hits] == [
        ("c-1", 0.9),
        ("c-3", pytest.approx(0.7)),
    ]
    (parameters,) = tx.with_fragment("queryNodes")
    assert (parameters["k"], parameters["project_id"], parameters["repository_id"]) == (
        20,
        "project-1",
        "repo-1",
    )
    assert parameters["model_revision"] == "f" * 40
    assert len(parameters["vector"]) == 384


def test_the_candidate_count_widens_until_enough_hits_or_the_cap() -> None:
    sessions = FakeSessions()  # no document ever exists: every candidate is filtered out
    service, tx = _vector_service(sessions, [[], [], [], [], []])

    assert asyncio.run(service.vector("hello", SCOPE, 10)) == []

    assert [parameters["k"] for parameters in tx.with_fragment("queryNodes")] == [
        40,
        160,
        640,
        MAX_CANDIDATES,
    ]


def test_widening_stops_when_the_index_has_nothing_more() -> None:
    sessions = FakeSessions()
    service, tx = _vector_service(sessions, [_candidates("c-1", raw=3)])  # 3 raw < 20 requested

    assert asyncio.run(service.vector("hello", SCOPE, 5)) == []

    assert len(tx.with_fragment("queryNodes")) == 1


def test_widening_fills_the_limit_from_a_later_round() -> None:
    documents = {"c-9": _document("c-9")}
    sessions = FakeSessions(
        lambda _sql, params: [documents[c] for c in params.get("content_ids", []) if c in documents]
    )
    service, tx = _vector_service(
        sessions, [_candidates("c-1", raw=20), _candidates("c-9", raw=80)]
    )

    hits = asyncio.run(service.vector("hello", SCOPE, 1))

    assert [hit.content_id for hit in hits] == ["c-9"]
    assert [parameters["k"] for parameters in tx.with_fragment("queryNodes")] == [4, 16]


def test_a_slow_embedder_raises_a_search_timeout() -> None:
    class Slow(StubEmbedder):
        async def embed(self, texts: Any) -> Any:
            await asyncio.sleep(1)

    service = SearchService(
        FakeSessions(),
        FakeGraph(FakeTx()),
        Slow(),
        deadline_seconds=0.01,  # type: ignore[arg-type]
    )

    with pytest.raises(SearchTimeoutError):
        asyncio.run(service.vector("hello", SCOPE, 5))


def test_a_slow_database_raises_a_search_timeout() -> None:
    class Slow(FakeSessions):
        def __call__(self) -> Any:
            raise TimeoutError

    with pytest.raises(SearchTimeoutError):
        asyncio.run(SearchService(Slow(), deadline_seconds=0.01).lexical("hello", SCOPE, 5))


def test_stale_candidates_do_not_hide_a_valid_hit_further_down_the_same_round() -> None:
    documents = {"c-5": _document("c-5")}  # c-1..c-4 are stale: they have no searchable row
    sessions = FakeSessions(
        lambda _sql, params: [documents[c] for c in params.get("content_ids", []) if c in documents]
    )
    service, tx = _vector_service(
        sessions, [_candidates("c-1", "c-2", "c-3", "c-4", "c-5", raw=20)]
    )

    hits = asyncio.run(service.vector("hello", SCOPE, 1))

    assert [hit.content_id for hit in hits] == ["c-5"]
    (parameters,) = tx.with_fragment("queryNodes")
    assert parameters["k"] == parameters["limit"] == 4  # every candidate of the round comes back
