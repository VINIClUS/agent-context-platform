"""Bounded code-graph traversal (PLATFORM-040) over the graph the `CodeProjector` writes.

The end-to-end test projects real `IndexingService` output (a temp git repository) and traverses
it. The bound, isolation, injection and evidence-ranking tests use seeded graphs of the same
shape (`GraphSeed`) because an index run cannot produce a 600-way fan-out or a second repository
on demand.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.retrieval.graph import (
    MAX_NODES,
    DependencyPaths,
    EdgeKind,
    GraphAnchorNotFound,
    GraphEdge,
    GraphNeighborhood,
    GraphRequestError,
    GraphScope,
    GraphTraversalService,
    NodeKind,
    OutgoingDependencies,
    RetrievalDeadlineExceeded,
)

from ..projection.projectors.test_code import REPO, Timeline, deliver
from .conftest import GraphSeed, with_graph

pytestmark = pytest.mark.integration

R1 = GraphScope("repo-1")
R2 = GraphScope("repo-2")
A_SOURCE = "import json\n\n\ndef f():\n    return json.dumps(1)\n\n\nclass K:\n    def m(self):\n        return f()\n"
B_SOURCE = "from .a import f\n\n\ndef g():\n    return f()\n"


def sym(n: int) -> str:
    return f"s{n:05d}"


def names(result: GraphNeighborhood) -> list[tuple[str, str | None]]:
    return [(node.kind.value, node.name) for node in result.nodes]


def assert_edge_evidence(edge: GraphEdge) -> None:
    assert edge.evidence.source_event_ids, edge
    if edge.kind in {EdgeKind.CALLS, EdgeKind.IMPORTS, EdgeKind.DEFINES, EdgeKind.REFERENCES}:
        assert edge.evidence.evidence_kind is not None, edge
        assert edge.evidence.confidence is not None, edge
        assert edge.evidence.valid_from is not None, edge


def assert_evidenced(result: GraphNeighborhood | DependencyPaths | OutgoingDependencies) -> None:
    """Every node, edge, alternative and external dependency of ANY result carries evidence."""
    if isinstance(result, DependencyPaths):
        nodes = [n for p in result.paths for n in p.nodes]
        edges = [e for p in result.paths for e in p.edges]
    elif isinstance(result, OutgoingDependencies):
        nodes, edges = list(result.targets), list(result.edges)
        assert all(x.evidence.source_event_ids for x in result.external)
    else:
        nodes, edges = list(result.nodes), list(result.edges)
        assert all(a.evidence.source_event_ids for a in result.alternatives)
    for node in nodes:
        assert node.evidence.source_event_ids, node
    for edge in edges:
        assert_edge_evidence(edge)


async def real_graph(store: Neo4jStore) -> dict[str, str]:
    """Index two Python files (one imports and calls the other) and project them."""
    with tempfile.TemporaryDirectory() as tmp:
        line = Timeline(Path(tmp))
        line.repo.write("pkg/a.py", A_SOURCE)
        line.repo.write("pkg/b.py", B_SOURCE)
        line.repo.commit()
        await deliver(store, line.index(1))

    async def read(tx: Neo4jTransaction) -> dict[str, str]:
        files = (
            await tx.run(
                "MATCH (f:File) RETURN f.file_id AS id, f.current_path AS p", parameters={}
            )
        ).records
        symbols = (
            await tx.run(
                "MATCH (s:Symbol)-[:CURRENT_REVISION]->(r:SymbolRevision) "
                "RETURN s.symbol_id AS id, r.qualified_name AS p",
                parameters={},
            )
        ).records
        return {str(r["p"]): str(r["id"]) for r in [*files, *symbols]}

    return await store.execute_read(read)


def test_real_index_neighborhoods_callers_callees_and_dependencies() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        ids = await real_graph(store)
        scope = GraphScope(REPO)
        a_file, b_file = ids["pkg/a.py"], ids["pkg/b.py"]
        f, g = ids["pkg.a.f"], ids["pkg.b.g"]

        repo_1 = await service.neighborhood(scope, REPO, 1)
        assert repo_1.anchor.kind is NodeKind.REPOSITORY
        assert {n.kind for n in repo_1.nodes} == {
            NodeKind.REPOSITORY,
            NodeKind.MODULE,
            NodeKind.FILE,
        }
        assert {n.name for n in repo_1.nodes if n.kind is NodeKind.FILE} == {"pkg/a.py", "pkg/b.py"}
        repo_2 = await service.neighborhood(scope, REPO, 2)
        assert {"pkg.a.f", "pkg.b.g", "pkg.a.K.m"} <= {n.name for n in repo_2.nodes}
        assert [n.distance for n in repo_2.nodes] == sorted(n.distance for n in repo_2.nodes)

        module = next(n.node_id for n in repo_1.nodes if n.kind is NodeKind.MODULE)
        in_module = await service.neighborhood(scope, module, 1)
        assert {n.name for n in in_module.nodes if n.kind is NodeKind.FILE} == {
            "pkg/a.py",
            "pkg/b.py",
        }

        file_view = await service.neighborhood(scope, b_file, 2)
        assert {"pkg/a.py", "pkg.a.f", "pkg.b.g"} <= {n.name for n in file_view.nodes}

        symbol_view = await service.neighborhood(scope, f, 1)
        assert {"pkg/a.py", "pkg.b.g", "pkg.a.K.m", "pkg.b"} <= {n.name for n in symbol_view.nodes}

        callers = await service.callers(scope, f)
        assert {n.name for n in callers.nodes} == {"pkg.a.f", "pkg.b.g", "pkg.a.K.m"}
        assert {e.kind for e in callers.edges} == {EdgeKind.CALLS}
        callees = await service.callees(scope, g)
        assert {n.name for n in callees.nodes} == {"pkg.b.g", "pkg.a.f"}  # across files

        paths = await service.dependency_paths(scope, g, f)
        assert [[n.name for n in p.nodes] for p in paths.paths] == [["pkg.b.g", "pkg.a.f"]]
        assert paths.paths[0].edges[0].kind is EdgeKind.CALLS

        b_deps = await service.dependencies(scope, b_file)
        assert [e.kind for e in b_deps.edges] == [EdgeKind.IMPORTS]
        assert b_deps.edges[0].target_id == f
        assert b_deps.external == ()
        a_deps = await service.dependencies(scope, a_file)
        assert [(d.dependency_kind) for d in a_deps.external] == ["observed"]
        assert a_deps.external[0].evidence.source_event_ids
        assert a_deps.external[0].evidence.revision_id is not None
        module_deps = await service.dependencies(scope, module)
        assert len(module_deps.external) == 1 and len(module_deps.edges) == 1

        for result in (repo_1, repo_2, in_module, file_view, symbol_view, callers, callees):
            assert_evidenced(result)
        edge = next(e for e in callers.edges if e.target_id == f and e.source_id == g)
        assert edge.evidence.evidence_kind == "tree_sitter"
        assert edge.evidence.revision_id is not None and edge.evidence.assertion_id is not None
        file_node = next(n for n in file_view.nodes if n.node_id == b_file)
        assert file_node.evidence.revision_id is not None and file_node.evidence.valid_from

        assert_evidenced(paths)
        for deps in (b_deps, a_deps, module_deps):
            assert_evidenced(deps)

        for anchor in (REPO, module, a_file, f):  # every anchor kind at every depth
            previous: set[str] = set()
            for depth in range(1, 5):
                result = await service.neighborhood(scope, anchor, depth)
                reached = {n.node_id for n in result.nodes}
                assert result.anchor.node_id == anchor and result.anchor.distance == 0
                assert max(n.distance for n in result.nodes) <= depth
                assert previous <= reached  # a deeper walk only adds
                assert not result.truncated
                assert_evidenced(result)
                previous = reached

        again = await service.neighborhood(scope, REPO, 2)
        assert again == repo_2  # the same graph gives the same output

    with_graph(body)


def test_depth_bounds_and_neighborhoods_at_depth_one_to_four() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        ids = [sym(n) for n in range(8)]
        await seed.symbols("repo-1", ids)
        await seed.calls([(ids[n], ids[n + 1]) for n in range(7)])
        for depth in range(1, 5):
            result = await service.neighborhood(R1, ids[0], depth)
            assert [n.node_id for n in result.nodes] == ids[: depth + 1]
            assert [n.distance for n in result.nodes] == list(range(depth + 1))
            assert len(result.edges) == depth
            assert not result.truncated
            assert_evidenced(result)
        middle = await service.neighborhood(R1, ids[3], 2)  # undirected
        assert [n.node_id for n in middle.nodes] == [ids[3], ids[2], ids[4], ids[1], ids[5]]
        for bad in (0, -1, 5, 100):
            with pytest.raises(GraphRequestError):
                await service.neighborhood(R1, ids[0], bad)
            with pytest.raises(GraphRequestError):
                await service.callers(R1, ids[0], bad)
            with pytest.raises(GraphRequestError):
                await service.dependency_paths(R1, ids[0], ids[1], max_depth=bad)

    with_graph(body)


def test_dependency_path_is_bounded_simple_and_shortest_first() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        ids = [sym(n) for n in range(6)]
        await seed.symbols("repo-1", ids)
        await seed.calls([(ids[n], ids[n + 1]) for n in range(5)] + [(ids[3], ids[0])])  # a cycle
        await seed.calls([(ids[0], ids[3])])  # a shortcut
        found = await service.dependency_paths(R1, ids[0], ids[3])
        assert [[n.node_id for n in p.nodes] for p in found.paths] == [
            [ids[0], ids[3]],
            [ids[0], ids[1], ids[2], ids[3]],
        ]
        assert all(len(p.edges) == len(p.nodes) - 1 for p in found.paths)
        assert_evidenced(found)
        assert (await service.dependency_paths(R1, ids[0], ids[3], max_depth=1)).paths[0].edges
        assert len((await service.dependency_paths(R1, ids[0], ids[3], max_depth=2)).paths) == 1
        assert (await service.dependency_paths(R1, ids[0], ids[5], max_depth=2)).paths == ()
        deep = await service.dependency_paths(R1, ids[0], ids[5])  # 0-3-4-5; the 5-hop walk is out
        assert [[n.node_id for n in p.nodes] for p in deep.paths] == [
            [ids[0], ids[3], ids[4], ids[5]]
        ]
        capped = await service.dependency_paths(R1, ids[0], ids[3], max_paths=1)
        assert len(capped.paths) == 1 and capped.truncated
        assert (await service.dependency_paths(R1, ids[3], ids[3])).paths == ()
        assert (await service.dependency_paths(R1, ids[5], ids[0])).paths == ()  # directed
        with pytest.raises(GraphRequestError):
            await service.dependency_paths(R1, ids[0], ids[3], max_paths=0)
        with pytest.raises(GraphRequestError):
            await service.dependency_paths(R1, ids[0], ids[3], max_paths=51)

    with_graph(body)


def test_node_limit_is_enforced_in_the_database_and_flagged() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        hub, leaves = "hub", [sym(n) for n in range(MAX_NODES + 100)]
        await seed.symbols("repo-1", [hub, *leaves])
        await seed.calls([(hub, leaf) for leaf in leaves])
        for result in (
            await service.neighborhood(R1, hub, 1),
            await service.callees(R1, hub),
            await service.neighborhood(R1, hub, 4),
        ):
            assert len(result.nodes) == MAX_NODES
            assert result.truncated
            kept = [n.node_id for n in result.nodes if n.node_id != hub]
            assert kept == leaves[: MAX_NODES - 1]  # deterministic cut: the lowest IDs
            assert all(e.source_id == hub for e in result.edges)
        # Exactly at the limit is not truncation.
        await seed.symbols("repo-1", ["hub2", *[f"x{n:03d}" for n in range(MAX_NODES - 1)]])
        await seed.calls([("hub2", f"x{n:03d}") for n in range(MAX_NODES - 1)])
        exact = await service.callees(R1, "hub2")
        assert len(exact.nodes) == MAX_NODES and not exact.truncated

    with_graph(body)


def test_deadline_raises_a_typed_error_and_the_service_recovers() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        ids = [sym(n) for n in range(2500)]
        await seed.symbols("repo-1", [*ids, "island"])
        await seed.calls([(ids[n], ids[(n + k) % 2500]) for n in range(2500) for k in range(1, 9)])
        with pytest.raises(RetrievalDeadlineExceeded):
            await service.neighborhood(R1, ids[0], 4, deadline_seconds=0.001)
        with pytest.raises(RetrievalDeadlineExceeded):  # ~4,000 walks, none reaches the island
            await service.dependency_paths(R1, ids[0], "island", deadline_seconds=0.001)
        ok = await service.callees(R1, ids[0], 1, deadline_seconds=30)
        assert len(ok.nodes) == 9

    with_graph(body)


def test_repository_isolation_with_two_repositories_in_one_graph() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        one, two = ["a1", "a2", "a3"], ["b1", "b2", "b3"]
        await seed.symbols("repo-1", one)
        await seed.symbols("repo-2", two)
        await seed.file("repo-1", "file-1", "x.py", "mod-1")
        await seed.file("repo-2", "file-2", "y.py", "mod-2")
        await seed.calls([("a1", "a2"), ("a2", "a3"), ("b1", "b2"), ("b2", "b3")])
        await seed.calls([("a2", "b1"), ("b3", "a1")])  # stray edges across repositories
        for depth in (1, 4):
            first = await service.neighborhood(R1, "a1", depth)
            assert {n.node_id for n in first.nodes} <= {*one}
            assert all(e.source_id in one and e.target_id in one for e in first.edges)
            assert_evidenced(first)
        assert {n.node_id for n in (await service.callees(R1, "a2")).nodes} == {"a2", "a3"}
        assert {n.node_id for n in (await service.callers(R2, "b1")).nodes} == {"b1"}
        repo = await service.neighborhood(R1, "repo-1", 4)
        assert {n.node_id for n in repo.nodes} <= {"repo-1", "file-1", "mod-1", *one}
        for anchor in ("b1", "file-2", "mod-2", "repo-2"):
            with pytest.raises(GraphAnchorNotFound):
                await service.neighborhood(R1, anchor, 2)
            with pytest.raises(GraphAnchorNotFound):
                await service.dependency_paths(R1, "a1", anchor)
        isolated = await service.dependency_paths(R1, "a1", "a3")
        assert isolated.paths
        assert_evidenced(isolated)
        with pytest.raises(GraphAnchorNotFound):
            await service.dependency_paths(R1, "a1", "b2")  # reachable only through repo-2
        with pytest.raises(GraphRequestError):
            GraphScope("  ")

    with_graph(body)


def test_injection_shaped_ids_are_data() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        evil = ['x"}) DETACH DELETE (n) //', "' OR 1=1 //", "a1' RETURN 1 AS kind //", "$id", "*"]
        await seed.symbols("repo-1", ["a1", "a2", *evil])
        await seed.calls([("a1", "a2"), ("a2", evil[0]), (evil[1], "a1")])

        async def count(tx: Neo4jTransaction) -> int:
            return int(
                (await tx.run("MATCH (n) RETURN count(n) AS c", parameters={})).records[0]["c"]
            )

        before = await store.execute_read(count)
        for anchor in (
            'zz"}) DETACH DELETE (n) //',
            "' OR 1=1 //x",
            "1; MATCH (n) DETACH DELETE n",
        ):
            with pytest.raises(GraphAnchorNotFound):
                await service.neighborhood(R1, anchor, 2)
            with pytest.raises(GraphAnchorNotFound):
                await service.callers(R1, anchor)
            with pytest.raises(GraphAnchorNotFound):
                await service.dependencies(R1, anchor)
        # An ID that merely looks like injection is a plain ID.
        found = await service.neighborhood(R1, evil[0], 1)
        assert {n.node_id for n in found.nodes} == {evil[0], "a2"}
        assert_evidenced(found)
        assert (await service.callers(R1, "a1")).nodes[1].node_id == evil[1]
        wild = await service.neighborhood(R1, "*", 4)
        assert [n.node_id for n in wild.nodes] == ["*"]
        assert_evidenced(wild)
        assert (await store.execute_read(count)) == before

    with_graph(body)


def test_resolved_current_edges_by_default_and_alternatives_on_request() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        await seed.symbols("repo-1", ["caller", "callee", "gone"], current=True)
        await seed.symbols("repo-1", ["stale"], current=False)
        await seed.calls(
            [("caller", "callee")],
            evidence_kind="scip",
            confidence=1.0,
            lower=["as-low", "as-heur"],
        )
        await seed.calls([("caller", "stale")])  # to a symbol that is not current any more
        win, low, heur, old = "as-caller>callee", "as-low", "as-heur", "as-old"
        for assertion, kind, confidence, current, obj in (
            (win, "scip", 1.0, True, "callee"),
            (low, "tree_sitter", 0.9, True, "callee"),
            (heur, "tree_sitter", 0.5, True, "callee"),
            (old, "tree_sitter", 0.5, False, "gone"),
        ):
            await seed.assertion(
                "repo-1", assertion, "caller", obj, current=current,
                evidence_kind=kind, confidence=confidence,
            )  # fmt: skip

        default = await service.callees(R1, "caller")
        assert [n.node_id for n in default.nodes] == ["caller", "callee"]  # no stale, no gone
        assert default.alternatives == ()
        (edge,) = default.edges
        assert (edge.evidence.evidence_kind, edge.evidence.confidence) == ("scip", 1.0)
        assert edge.evidence.assertion_id == win

        full = await service.callees(R1, "caller", include_alternatives=True)
        assert full.nodes == default.nodes and full.edges == default.edges
        assert {(a.evidence.assertion_id, a.status) for a in full.alternatives} == {
            (low, "lower_evidence"),
            (heur, "lower_evidence"),
        }  # "gone" is not among the returned nodes, so its assertion is not either
        assert all(a.evidence.source_event_ids for a in full.alternatives)
        wide = await service.neighborhood(R1, "caller", 1, include_alternatives=True)
        assert all(a.object_id == "callee" for a in wide.alternatives)

        await seed.symbols("repo-1", ["gone"])
        await seed.calls([("caller", "gone")], lower=[old])
        again = await service.callees(R1, "caller", include_alternatives=True)
        assert {(a.evidence.assertion_id, a.status) for a in again.alternatives} == {
            (low, "lower_evidence"),
            (heur, "lower_evidence"),
            (old, "not_current"),
        }

    with_graph(body)


def test_non_current_anchors_are_not_found_and_kinds_are_checked() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        await seed.symbols("repo-1", ["live"])
        await seed.symbols("repo-1", ["dead"], current=False)
        await seed.file("repo-1", "file-1", "x.py", "mod-1")
        await seed.defines("file-1", ["live"])
        with pytest.raises(GraphAnchorNotFound):
            await service.neighborhood(R1, "dead", 1)
        with pytest.raises(GraphAnchorNotFound):
            await service.neighborhood(R1, "missing", 1)
        with pytest.raises(GraphRequestError):
            await service.callers(R1, "mod-1")  # a module has no callers
        with pytest.raises(GraphRequestError):
            await service.dependencies(R1, "live")  # dependencies are of modules and files
        with pytest.raises(GraphRequestError):
            await service.dependency_paths(R1, "mod-1", "live")
        defined = await service.neighborhood(R1, "file-1", 1)
        assert [(n.kind.value, n.node_id) for n in defined.nodes] == [
            ("file", "file-1"),
            ("symbol", "live"),  # by distance, then ID
            ("module", "mod-1"),
        ]
        assert [e.kind for e in defined.edges] == [EdgeKind.DEFINES, EdgeKind.IN_MODULE]
        # Module membership carries the file's evidence; IN_REPOSITORY its own events.
        assert_evidenced(defined)

    with_graph(body)


def test_outgoing_dependencies_are_bounded_and_scoped() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        targets = [f"t{n:04d}" for n in range(MAX_NODES + 50)]
        await seed.symbols("repo-1", ["owner", *targets])
        await seed.symbols("repo-2", ["foreign"])
        await seed.file("repo-1", "file-1", "x.py", "mod-1")
        await seed.defines("file-1", ["owner"])
        await seed.imports([("owner", t) for t in targets[:3]] + [("owner", "foreign")])
        small = await service.dependencies(R1, "file-1")
        assert [e.target_id for e in small.edges] == targets[:3]  # never repo-2's symbol
        assert [n.node_id for n in small.targets] == ["file-1", *targets[:3]]
        assert not small.truncated and small.external == ()
        assert_evidenced(small)
        assert (await service.dependencies(R1, "mod-1")).edges == small.edges

        await seed.imports([("owner", t) for t in targets[3:]])
        big = await service.dependencies(R1, "file-1")
        assert len(big.targets) == MAX_NODES and big.truncated
        assert [e.target_id for e in big.edges] == targets[: MAX_NODES - 1]
        assert_evidenced(big)
        with pytest.raises(GraphAnchorNotFound):
            await service.dependencies(R1, "file-2")

    with_graph(body)


def test_foreign_dependency_nodes_and_emptied_modules_are_not_returned() -> None:
    async def body(store: Neo4jStore, service: GraphTraversalService, seed: GraphSeed) -> None:
        await seed.file("repo-1", "file-1", "x.py", "mod-1")

        async def write(tx: Neo4jTransaction) -> None:
            await tx.run(
                "MATCH (f:File {file_id: 'file-1'}) "
                "CREATE (f)-[:CURRENT_REVISION]->(fr:FileRevision {file_revision_id: 'fr-1'}) "
                "CREATE (a1:Assertion {assertion_id: 'dep-own', repository_id: 'repo-1', "
                "family: 'dependency', current: true, dependency_kind: 'observed', "
                "evidence_kind: 'tree_sitter', confidence: 0.9, source_event_ids: ['ev-1']}) "
                "CREATE (a2:Assertion {assertion_id: 'dep-stray', repository_id: 'repo-1', "
                "family: 'dependency', current: true, dependency_kind: 'observed', "
                "evidence_kind: 'tree_sitter', confidence: 0.9, source_event_ids: ['ev-2']}) "
                "CREATE (a1)-[:OBSERVED_IN]->(fr) CREATE (a2)-[:OBSERVED_IN]->(fr) "
                "CREATE (a1)-[:DEPENDS_ON]->(:Dependency "
                "{dependency_id: 'json', repository_id: 'repo-1'}) "
                "CREATE (a2)-[:DEPENDS_ON]->(:Dependency "
                "{dependency_id: 'foreign-dep', repository_id: 'repo-2'})",
                parameters={},
            )

        await store.execute_write(write)
        for anchor in ("file-1", "mod-1"):
            found = await service.dependencies(R1, anchor)
            assert [d.dependency_id for d in found.external] == ["json"]

        async def empty(tx: Neo4jTransaction) -> None:
            await tx.run("MATCH (:File)-[r:IN_MODULE]->(:Module) DELETE r", parameters={})

        await store.execute_write(empty)  # the module's last file moved away
        with pytest.raises(GraphAnchorNotFound):
            await service.neighborhood(R1, "mod-1", 1)
        with pytest.raises(GraphAnchorNotFound):
            await service.dependencies(R1, "mod-1")

    with_graph(body)
