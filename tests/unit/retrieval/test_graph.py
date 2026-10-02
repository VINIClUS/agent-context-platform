"""Graph traversal request validation and the transaction boundary, without a database."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from types import SimpleNamespace, TracebackType
from typing import Any

import pytest
from agent_context_sdk import CommitRevisionV1
from neo4j.exceptions import ClientError, ConfigurationError, Neo4jError

from agent_context_platform.retrieval import graph
from agent_context_platform.retrieval.graph import (
    GraphRequestError,
    GraphScope,
    GraphTemporalScopeUnsupported,
    GraphTraversalService,
    RetrievalDeadlineExceeded,
)
from agent_context_platform.settings import Neo4jSettings

pytestmark = pytest.mark.unit

Behaviour = Callable[[Callable[[Any], Awaitable[Any]]], Awaitable[Any]]


class FakeSession:
    def __init__(self, driver: FakeDriver) -> None:
        self._driver = driver

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def execute_read(self, work: Callable[[Any], Awaitable[Any]]) -> Any:
        self._driver.timeouts.append(getattr(work, "timeout", None))
        return await self._driver.behaviour(work)


class FakeDriver:
    def __init__(self, behaviour: Behaviour) -> None:
        self.behaviour = behaviour
        self.timeouts: list[float | None] = []
        self.databases: list[str] = []
        self.closed = False

    def session(self, *, database: str) -> FakeSession:
        self.databases.append(database)
        return FakeSession(self)

    async def close(self) -> None:
        self.closed = True


class ServerError(ClientError):
    """A server failure with a chosen Neo4j status code."""

    def __init__(self, status: str) -> None:
        super().__init__("server said no")
        self._status = status

    @property
    def code(self) -> str:
        return self._status


def server_error(code: str) -> Neo4jError:
    return ServerError(code)


def service(driver: FakeDriver, **kwargs: Any) -> GraphTraversalService:
    return GraphTraversalService(driver, database="graph", **kwargs)  # type: ignore[arg-type]


def test_scope_needs_a_repository() -> None:
    assert GraphScope("repo").repository_id == "repo"
    for blank in ("", "   "):
        with pytest.raises(GraphRequestError):
            GraphScope(blank)


@pytest.mark.parametrize("depth", [0, -1, 5, 99, True, 2.0, "2"])
def test_depth_outside_one_to_four_is_an_error_not_a_clamp(depth: Any) -> None:
    async def never(_work: Any) -> Any:
        raise AssertionError("no transaction may start for an invalid request")

    driver = FakeDriver(never)

    async def run() -> None:
        calls = (
            service(driver).neighborhood(GraphScope("r"), "x", depth),
            service(driver).callers(GraphScope("r"), "x", depth),
            service(driver).callees(GraphScope("r"), "x", depth),
            service(driver).dependency_paths(GraphScope("r"), "x", "y", max_depth=depth),
        )
        for call in calls:
            with pytest.raises(GraphRequestError):
                await call

    asyncio.run(run())
    assert driver.timeouts == []


def test_path_and_deadline_arguments_are_validated() -> None:
    async def never(_work: Any) -> Any:
        raise AssertionError("no transaction may start")

    driver = FakeDriver(never)

    async def run() -> None:
        scope = GraphScope("r")
        with pytest.raises(GraphRequestError):
            await service(driver).dependency_paths(scope, "x", "y", max_paths=0)
        with pytest.raises(GraphRequestError):
            await service(driver).dependency_paths(scope, "x", "y", max_paths=graph.MAX_PATHS + 1)
        with pytest.raises(GraphRequestError):
            await service(driver).dependencies(scope, "x", deadline_seconds=0)
        with pytest.raises(GraphRequestError):
            await service(driver).neighborhood(scope, "x", 1, deadline_seconds=-1)
        with pytest.raises(GraphRequestError):
            service(driver, default_deadline_seconds=0)

    asyncio.run(run())


def test_each_request_is_one_read_transaction_with_the_deadline_as_driver_timeout() -> None:
    async def finish(_work: Any) -> Any:
        raise ClientError("stop")  # not a timeout: it must surface unchanged

    driver = FakeDriver(finish)
    graph_service = service(driver, default_deadline_seconds=3.0)

    async def run() -> None:
        with pytest.raises(ClientError):
            await graph_service.neighborhood(GraphScope("r"), "x", 1)
        with pytest.raises(ClientError):
            await graph_service.callers(GraphScope("r"), "x", 1, deadline_seconds=1.5)

    asyncio.run(run())
    assert driver.timeouts == [3.0, 1.5]
    assert driver.databases == ["graph", "graph"]


def test_a_stalled_query_hits_the_client_deadline() -> None:
    async def stall(_work: Any) -> Any:
        await asyncio.sleep(30)

    async def run() -> None:
        with pytest.raises(RetrievalDeadlineExceeded):
            await service(FakeDriver(stall)).callees(GraphScope("r"), "x", 1, deadline_seconds=0.05)

    asyncio.run(run())


@pytest.mark.parametrize(
    "code",
    [
        "Neo.ClientError.Transaction.TransactionTimedOut",
        "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
    ],
)
def test_a_server_side_transaction_timeout_is_the_typed_deadline_error(code: str) -> None:
    async def expire(_work: Any) -> Any:
        raise server_error(code)

    async def run() -> None:
        with pytest.raises(RetrievalDeadlineExceeded):
            await service(FakeDriver(expire)).neighborhood(GraphScope("r"), "x", 1)

    asyncio.run(run())


def test_other_database_errors_are_not_reported_as_deadlines() -> None:
    async def fail(_work: Any) -> Any:
        raise server_error("Neo.ClientError.Statement.SyntaxError")

    async def run() -> None:
        with pytest.raises(Neo4jError):
            await service(FakeDriver(fail)).neighborhood(GraphScope("r"), "x", 1)

    asyncio.run(run())


def test_the_service_closes_only_a_driver_it_owns() -> None:
    async def run() -> tuple[bool, bool]:
        borrowed, owned = (
            FakeDriver(lambda _w: asyncio.sleep(0)),
            FakeDriver(lambda _w: asyncio.sleep(0)),
        )
        async with service(borrowed):
            pass
        async with GraphTraversalService(owned, database="g", owns_driver=True):  # type: ignore[arg-type]
            pass
        return borrowed.closed, owned.closed

    assert asyncio.run(run()) == (False, True)


def test_from_settings_needs_complete_connection_settings() -> None:
    async def run() -> None:
        with pytest.raises(ConfigurationError):
            GraphTraversalService.from_settings(Neo4jSettings())
        complete = Neo4jSettings.model_validate(
            {"uri": "bolt://127.0.0.1:1", "username": "u", "password": "p"}
        )
        async with GraphTraversalService.from_settings(complete):  # opens no connection
            pass

    asyncio.run(run())


def test_cypher_is_static_and_uses_closed_tables() -> None:
    queries = [
        graph._RESOLVE,
        *graph._LEVEL.values(),
        *graph._EDGES.values(),
        *graph._NODE_EVIDENCE.values(),
        *graph._PATHS.values(),
        *graph._IMPORTS.values(),
        *graph._MEMBERS_CUT.values(),
        *graph._EXTERNAL.values(),
        graph._ALTERNATIVES,
    ]
    allowed = {
        "id", "ids", "repository_id", "visited", "limit",
        "source_id", "target_id", "predicates",
    }  # fmt: skip
    for query in queries:
        assert set(re.findall(r"\$(\w+)", query)) <= allowed, query
        assert "'" not in query.replace("'relation'", "").replace("'dependency'", "").replace(
            "'repository'", ""
        ).replace("'module'", "").replace("'file'", "").replace("'symbol'", ""), query
    assert {kind.value for kind in graph.EdgeKind} >= {"CALLS", "IMPORTS", "DEFINES"}
    assert {"*1..1]", "*1..2]", "*1..3]", "*1..4]"} == {
        next(m for m in ("*1..1]", "*1..2]", "*1..3]", "*1..4]") if m in q)
        for q in graph._PATHS.values()
    }


@pytest.mark.parametrize("field", ["revision", "valid_at", "recorded_at"])
def test_revision_and_as_of_scopes_are_rejected_before_any_transaction(field: str) -> None:
    async def never(_work: Any) -> Any:
        raise AssertionError("no transaction may start")

    value: Any = (
        CommitRevisionV1(kind="commit", commit_id="a" * 40)
        if field == "revision"
        else datetime(2026, 3, 1, tzinfo=UTC)
    )
    driver = FakeDriver(never)

    async def run() -> None:
        graph_service, scope = service(driver), GraphScope("r")
        extra = {field: value}
        for call in (
            graph_service.neighborhood(scope, "x", 1, **extra),
            graph_service.callers(scope, "x", **extra),
            graph_service.callees(scope, "x", **extra),
            graph_service.dependency_paths(scope, "x", "y", **extra),
            graph_service.dependencies(scope, "x", **extra),
        ):
            with pytest.raises(GraphTemporalScopeUnsupported):
                await call

    asyncio.run(run())
    assert driver.timeouts == []
    assert issubclass(GraphTemporalScopeUnsupported, GraphRequestError)


_REPO_LABELS = {"Repository", "Module", "File", "Symbol", "Assertion", "Dependency"}
_NODE_PATTERN = re.compile(r"(?<![\w])\(\s*(\w+)\s*(?::\s*([\w|]+))?\s*(\{[^}]*\})?\s*\)")


def _all_queries() -> list[str]:
    return [
        graph._RESOLVE,
        *graph._LEVEL.values(),
        *graph._EDGES.values(),
        *graph._NODE_EVIDENCE.values(),
        *graph._PATHS.values(),
        *graph._IMPORTS.values(),
        *graph._MEMBERS_CUT.values(),
        *graph._EXTERNAL.values(),
        graph._ALTERNATIVES,
    ]


def test_every_bound_node_variable_is_scoped_to_the_repository() -> None:
    """Isolation by construction: no matched node is trusted just because a neighbour is scoped."""
    checked = 0
    for query in _all_queries():
        labels: dict[str, set[str]] = {}
        scoped: set[str] = set()
        for name, label, props in _NODE_PATTERN.findall(query):
            labels.setdefault(name, set()).update(label.split("|") if label else ())
            if "repository_id: $repository_id" in props:
                scoped.add(name)
        for name in set(re.findall(r"(\w+)\.repository_id = \$repository_id", query)):
            scoped.add(name)
        variables = set(labels) | set(re.findall(r"UNWIND \w+ AS (\w+)", query))
        variables |= set(re.findall(r"(\w+) IN (?:ns|subjects)\b", query))
        for name in variables:
            known = labels.get(name, set())
            if known and not known & _REPO_LABELS:
                continue  # revisions and run targets have no repository_id: reached through scoped nodes
            assert name in scoped, (name, query)
            checked += 1
    assert checked > 40


# --- the request flows, against a scripted transaction (no database) ---


class ScriptedTransaction:
    """Answers each static query from a table; unknown queries return no rows."""

    def __init__(
        self, answers: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]]
    ) -> None:
        self.answers = answers
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, *, parameters: dict[str, Any]) -> Any:
        self.calls.append((query, parameters))
        rows = self.answers.get(query, lambda _p: [])(parameters)

        async def eager() -> Any:
            return SimpleNamespace(records=rows)

        return SimpleNamespace(to_eager_result=eager)


def scripted(
    answers: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]],
) -> tuple[GraphTraversalService, ScriptedTransaction]:
    raw = ScriptedTransaction(answers)

    async def behaviour(work: Callable[[Any], Awaitable[Any]]) -> Any:
        return await work(raw)

    return service(FakeDriver(behaviour)), raw


def node_row(node_id: str, name: str | None = None, events: Any = ("ev",)) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "name": name,
        "revision_id": "rev-" + node_id,
        "extractor_name": "x",
        "extractor_version": "1",
        "valid_from": "t0",
        "valid_to": None,
        "events": [list(events)],
    }


def edge_row(kind: str, a: str, b: str, **props: Any) -> dict[str, Any]:
    base = {"source_event_ids": ["ev-" + a], "evidence_kind": "scip", "confidence": 1.0}
    return {
        "type": kind,
        "source_id": a,
        "target_id": b,
        "props": base | props,
        "revisions": ["fr-2", "fr-1"],
    }


SYMBOL = graph.NodeKind.SYMBOL


def test_neighborhood_flow_walks_levels_truncates_and_lists_alternatives() -> None:
    levels = iter([[{"kind": "File", "node_id": "f1"}, {"kind": "Symbol", "node_id": "b"}]])
    mode = graph._Mode.NEIGHBORHOOD
    answers: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
        graph._RESOLVE: lambda _p: [{"kind": "symbol"}],
        graph._LEVEL[(mode, SYMBOL)]: lambda _p: next(levels, []),
        graph._NODE_EVIDENCE[SYMBOL]: lambda p: [node_row(i, f"n-{i}") for i in p["ids"]],
        graph._NODE_EVIDENCE[graph.NodeKind.FILE]: lambda p: [node_row(i, "f.py") for i in p["ids"]],
        graph._EDGES[(mode, SYMBOL)]: lambda _p: [edge_row("CALLS", "a", "b")],
        graph._EDGES[(mode, graph.NodeKind.FILE)]: lambda _p: [
            edge_row("IN_MODULE", "f1", "a"),
            edge_row("DEFINES", "f1", "a"),
        ],
        graph._ALTERNATIVES: lambda _p: [
            {
                "assertion_id": "as-low", "predicate": "CALLS", "subject_id": "a",
                "object_id": "b", "current": True, "props": {"source_event_ids": ["e"]},
                "revisions": [],
            },
            {
                "assertion_id": "as-old", "predicate": "CALLS", "subject_id": "a",
                "object_id": "b", "current": False, "props": {}, "revisions": [],
            },
        ],
    }  # fmt: skip
    graph_service, raw = scripted(answers)
    result = asyncio.run(
        graph_service.neighborhood(GraphScope("r"), "a", 2, include_alternatives=True)
    )
    assert [n.node_id for n in result.nodes] == ["a", "b", "f1"]
    assert [(e.kind.value, e.source_id) for e in result.edges] == [
        ("CALLS", "a"), ("DEFINES", "f1"), ("IN_MODULE", "f1"),
    ]  # fmt: skip
    assert result.edges[0].evidence.revision_id == "fr-1"
    assert [(a.evidence.assertion_id, a.status) for a in result.alternatives] == [
        ("as-low", "lower_evidence"),
        ("as-old", "not_current"),
    ]
    assert not result.truncated
    assert all(parameters.get("repository_id") == "r" for _q, parameters in raw.calls)


def test_neighborhood_flow_reports_every_cut() -> None:
    mode = graph._Mode.CALLERS
    many = [{"kind": "Symbol", "node_id": f"c{n:03d}"} for n in range(graph.MAX_NODES + 1)]
    answers: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
        graph._RESOLVE: lambda _p: [{"kind": "symbol"}],
        graph._LEVEL[(mode, SYMBOL)]: lambda p: many[: p["limit"]],
        graph._NODE_EVIDENCE[SYMBOL]: lambda p: [node_row(i) for i in p["ids"]],
        graph._EDGES[(mode, SYMBOL)]: lambda p: [
            edge_row("CALLS", f"c{n:03d}", "a") for n in range(p["limit"])
        ],
    }
    graph_service, _ = scripted(answers)
    result = asyncio.run(graph_service.callers(GraphScope("r"), "a", 3))
    assert len(result.nodes) == graph.MAX_NODES and result.truncated
    assert len(result.edges) == graph.MAX_EDGES


def test_flow_errors_are_typed() -> None:
    graph_service, _ = scripted({})
    scope = GraphScope("r")
    with pytest.raises(graph.GraphAnchorNotFound):
        asyncio.run(graph_service.neighborhood(scope, "nope", 1))
    only = {graph._RESOLVE: lambda _p: [{"kind": "module"}]}
    graph_service, _ = scripted(only)
    for call in (
        graph_service.callers(scope, "m"),
        graph_service.callees(scope, "m"),
        graph_service.dependency_paths(scope, "m", "m"),
    ):
        with pytest.raises(GraphRequestError):
            asyncio.run(call)
    graph_service, _ = scripted({graph._RESOLVE: lambda _p: [{"kind": "symbol"}]})
    with pytest.raises(GraphRequestError):
        asyncio.run(graph_service.dependencies(scope, "s"))


def test_dependency_paths_flow() -> None:
    path = SYMBOL, SYMBOL, 2
    rel = {"type": "CALLS", "props": {"source_event_ids": ["e"]}, "revisions": ["fr"]}
    rows = [
        {"ids": ["a", "b"], "labels": ["Symbol", "Symbol"], "rels": [rel]},
        {"ids": ["a", "c", "b"], "labels": ["Symbol"] * 3, "rels": [rel, rel]},
    ]
    answers: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
        graph._RESOLVE: lambda _p: [{"kind": "symbol"}],
        graph._PATHS[path]: lambda p: rows[: p["limit"]],
        graph._NODE_EVIDENCE[SYMBOL]: lambda p: [node_row(i) for i in p["ids"]],
    }
    graph_service, _ = scripted(answers)
    found = asyncio.run(graph_service.dependency_paths(GraphScope("r"), "a", "b", max_depth=2))
    assert [[n.node_id for n in p.nodes] for p in found.paths] == [["a", "b"], ["a", "c", "b"]]
    assert not found.truncated
    capped = asyncio.run(
        graph_service.dependency_paths(GraphScope("r"), "a", "b", max_depth=2, max_paths=1)
    )
    assert len(capped.paths) == 1 and capped.truncated


@pytest.mark.parametrize("members", [500, 501])
def test_dependencies_flow_flags_a_cut_even_without_imports(members: int) -> None:
    kind = graph.NodeKind.FILE
    answers: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
        graph._RESOLVE: lambda _p: [{"kind": "file"}],
        graph._IMPORTS[kind]: lambda _p: [],
        graph._MEMBERS_CUT[kind]: lambda _p: [{"members": members}],
        graph._NODE_EVIDENCE[kind]: lambda p: [node_row(i, "f.py") for i in p["ids"]],
        graph._EXTERNAL[kind]: lambda _p: [
            {
                "dependency_id": "json", "revision_id": "fr-1",
                "props": {"dependency_kind": "observed", "assertion_id": "dep-1",
                          "source_event_ids": ["e"]},
            }
        ],
    }  # fmt: skip
    graph_service, _ = scripted(answers)
    result = asyncio.run(graph_service.dependencies(GraphScope("r"), "f1"))
    assert result.truncated is (members > graph.MAX_NODES)
    assert result.edges == () and [d.dependency_id for d in result.external] == ["json"]
    assert result.external[0].evidence.revision_id == "fr-1"


def test_dependencies_flow_caps_targets_and_external() -> None:
    kind = graph.NodeKind.MODULE
    imports = [
        {"source_id": "s", "target_id": f"t{n:03d}", "props": {"source_event_ids": ["e"]},
         "revisions": []} for n in range(graph.MAX_NODES)
    ]  # fmt: skip
    external = [
        {"dependency_id": f"d{n:03d}", "revision_id": None, "props": {}}
        for n in range(graph.MAX_NODES + 1)
    ]
    answers: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
        graph._RESOLVE: lambda _p: [{"kind": "module"}],
        graph._IMPORTS[kind]: lambda p: imports[: p["limit"]],
        graph._MEMBERS_CUT[kind]: lambda _p: [{"members": 3}],
        graph._NODE_EVIDENCE[kind]: lambda p: [node_row(i) for i in p["ids"]],
        graph._NODE_EVIDENCE[SYMBOL]: lambda p: [node_row(i) for i in p["ids"]],
        graph._EXTERNAL[kind]: lambda p: external[: p["limit"]],
    }
    graph_service, _ = scripted(answers)
    result = asyncio.run(graph_service.dependencies(GraphScope("r"), "m"))
    assert len(result.targets) == graph.MAX_NODES and len(result.edges) == graph.MAX_NODES - 1
    assert len(result.external) == graph.MAX_NODES and result.truncated
