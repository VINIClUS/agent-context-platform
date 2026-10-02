"""Fixtures for the graph traversal suite (PLATFORM-040).

Real `IndexingService` output is projected by the P038 `CodeProjector` for the end-to-end checks.
Bounds, isolation and evidence rules need shapes an index run would not produce on demand (a
600-way fan-out, a second repository, losing assertions), so `GraphSeed` writes the same labels,
flags and relationship properties the projector writes. Seeding is test-only Cypher, always
parameterized.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator, Sequence
from typing import Any, LiteralString

import pytest

from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.schema import SCHEMA_STATEMENTS, ensure_schema
from agent_context_platform.retrieval.graph import GraphTraversalService
from agent_context_platform.settings import Neo4jSettings

from ..projection.conftest import neo4j_integration_settings

BATCH = 1000

_WIPE: LiteralString = "MATCH (n) WITH n LIMIT 2000 DETACH DELETE n RETURN count(*) AS deleted"  # in slices: one transaction over a 50k-edge graph is too big for a loaded test database
_SYMBOLS: LiteralString = (
    "UNWIND $rows AS row MERGE (s:Symbol {symbol_id: row.id}) "
    "SET s.repository_id = row.repository_id, s.current = row.current "
    "MERGE (r:Repository {repository_id: row.repository_id}) "
    "MERGE (s)-[m:IN_REPOSITORY]->(r) SET m.source_event_ids = ['ev-' + row.id]"
)
_FILES: LiteralString = (
    "UNWIND $rows AS row MERGE (f:File {file_id: row.id}) "
    "SET f.repository_id = row.repository_id, f.current = row.current, f.current_path = row.path "
    "MERGE (r:Repository {repository_id: row.repository_id}) "
    "MERGE (f)-[m:IN_REPOSITORY]->(r) SET m.source_event_ids = ['ev-' + row.id] "
    "MERGE (d:Module {module_id: row.module_id}) SET d.repository_id = row.repository_id "
    "MERGE (d)-[dm:IN_REPOSITORY]->(r) SET dm.source_event_ids = ['ev-' + row.module_id] "
    "MERGE (f)-[:IN_MODULE]->(d)"
)
_EDGES: LiteralString = (
    "UNWIND $rows AS row MATCH (a:Symbol {symbol_id: row.source}) "
    "MATCH (b:Symbol {symbol_id: row.target}) "
    "CREATE (a)-[r:CALLS]->(b) SET r += row.props"
)
_IMPORTS: LiteralString = (
    "UNWIND $rows AS row MATCH (a:Symbol {symbol_id: row.source}) "
    "MATCH (b:Symbol {symbol_id: row.target}) CREATE (a)-[r:IMPORTS]->(b) SET r += row.props"
)
_DEFINES: LiteralString = (
    "UNWIND $rows AS row MATCH (a:File {file_id: row.source}) "
    "MATCH (b:Symbol {symbol_id: row.target}) CREATE (a)-[r:DEFINES]->(b) SET r += row.props"
)
_ASSERTIONS: LiteralString = (
    "UNWIND $rows AS row MERGE (a:Assertion {assertion_id: row.assertion_id}) SET a += row"
)


def edge_props(
    edge_id: str,
    *,
    evidence_kind: str = "tree_sitter",
    confidence: float = 0.5,
    lower: Sequence[str] = (),
) -> dict[str, Any]:
    """The properties `resolve_edges` writes on a resolved edge (the ones traversal reads)."""
    return {
        "source_event_ids": [f"ev-{edge_id}"],
        "evidence_kind": evidence_kind,
        "confidence": confidence,
        "valid_from": "2026-03-01T12:00:00.000000Z",
        "valid_to": None,
        "resolved": True,
        "resolved_assertion_id": f"as-{edge_id}",
        "lower_evidence_assertion_ids": list(lower),
    }


class GraphSeed:
    """Write P038-shaped nodes and edges in batches."""

    def __init__(self, store: Neo4jStore) -> None:
        self._store = store

    async def _batches(self, query: LiteralString, rows: Sequence[dict[str, Any]]) -> None:
        for start in range(0, len(rows), BATCH):
            chunk = list(rows[start : start + BATCH])

            async def run(tx: Neo4jTransaction, chunk: list[dict[str, Any]] = chunk) -> None:
                await tx.run(query, parameters={"rows": chunk})

            await self._store.execute_write(run)

    async def symbols(
        self, repository_id: str, ids: Sequence[str], *, current: bool = True
    ) -> None:
        await self._batches(
            _SYMBOLS,
            [{"id": i, "repository_id": repository_id, "current": current} for i in ids],
        )

    async def file(self, repository_id: str, file_id: str, path: str, module_id: str) -> None:
        row = {
            "id": file_id,
            "repository_id": repository_id,
            "current": True,
            "path": path,
            "module_id": module_id,
        }
        await self._batches(_FILES, [row])

    async def files(self, repository_id: str, count: int, module_id: str) -> None:
        rows = [
            {
                "id": f"{repository_id}-file-{n:04d}",
                "repository_id": repository_id,
                "current": True,
                "path": f"f{n}.py",
                "module_id": module_id,
            }
            for n in range(count)
        ]
        await self._batches(_FILES, rows)

    async def calls(self, pairs: Sequence[tuple[str, str]], **evidence: Any) -> None:
        rows = [
            {"source": a, "target": b, "props": edge_props(f"{a}>{b}", **evidence)}
            for a, b in pairs
        ]
        await self._batches(_EDGES, rows)

    async def imports(self, pairs: Sequence[tuple[str, str]]) -> None:
        rows = [
            {"source": a, "target": b, "props": edge_props(f"{a}>{b}", confidence=0.9)}
            for a, b in pairs
        ]
        await self._batches(_IMPORTS, rows)

    async def defines(self, file_id: str, symbols: Sequence[str]) -> None:
        rows = [
            {"source": file_id, "target": s, "props": edge_props(f"{file_id}>{s}", confidence=0.9)}
            for s in symbols
        ]
        await self._batches(_DEFINES, rows)

    async def assertion(
        self,
        repository_id: str,
        assertion_id: str,
        subject: str,
        obj: str,
        *,
        current: bool,
        evidence_kind: str,
        confidence: float,
        predicate: str = "CALLS",
    ) -> None:
        row = {
            "assertion_id": assertion_id,
            "repository_id": repository_id,
            "family": "relation",
            "subject_id": subject,
            "object_id": obj,
            "predicate": predicate,
            "current": current,
            "evidence_kind": evidence_kind,
            "confidence": confidence,
            "source_event_ids": [f"ev-{assertion_id}"],
            "valid_from": "2026-03-01T12:00:00.000000Z",
        }
        await self._batches(_ASSERTIONS, [row])


async def wipe(store: Neo4jStore) -> None:
    async def run(tx: Neo4jTransaction) -> int:
        return int((await tx.run(_WIPE, parameters={})).records[0]["deleted"])

    while await store.execute_write(run):
        pass


async def _set_schema(*, present: bool) -> None:
    async with Neo4jStore(neo4j_integration_settings()) as store:
        if present:
            await ensure_schema(store)
            return

        async def drop(tx: Neo4jTransaction) -> None:
            for statement in SCHEMA_STATEMENTS:
                kind = "CONSTRAINT" if "CONSTRAINT" in statement.query else "INDEX"
                await tx.run(f"DROP {kind} {statement.name} IF EXISTS", parameters={})  # type: ignore[arg-type]

        await store.execute_write(drop)


@pytest.fixture(scope="module", autouse=True)
def retrieval_graph_schema() -> Iterator[None]:
    """The projection schema (unique keys), created once and dropped again like the P038 suite."""
    asyncio.run(_set_schema(present=True))
    try:
        yield
    finally:
        asyncio.run(_set_schema(present=False))


def with_graph(
    body: Callable[[Neo4jStore, GraphTraversalService, GraphSeed], Awaitable[None]],
    *,
    settings: Neo4jSettings | None = None,
) -> None:
    """Run `body` against an empty graph with a traversal service; leave it empty again."""

    async def run() -> None:
        config = settings or neo4j_integration_settings()
        async with Neo4jStore(config) as store:
            await wipe(store)
            service = GraphTraversalService.from_settings(config)
            try:
                await body(store, service, GraphSeed(store))
            finally:
                await service.close()
                await wipe(store)

    asyncio.run(run())
