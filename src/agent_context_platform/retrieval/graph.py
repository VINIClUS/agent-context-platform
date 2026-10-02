"""Bounded, parameterized code-graph traversal over the graph PLATFORM-038 projects.

The graph is read, never re-derived: `File`/`Symbol` nodes carry the `current` flag the
`CodeProjector` derives, `(File|Symbol)-[:CALLS|REFERENCES|IMPORTS|DEFINES]->(Symbol)` are its
resolved edges (one per triple, winning evidence on the edge, losers listed by assertion ID),
`(File)-[:IN_MODULE]->(Module)` exists for current files only, and every member points at its
`Repository` with `IN_REPOSITORY`. Nothing here recomputes identity, currentness or evidence rank.

Contract
--------
- Scope: every call takes a `GraphScope` with a mandatory `repository_id`. Every node a query
  touches is matched on `repository_id`, so an ID of another repository is "not found", never data.
- Current only. `revision` (SDK `RevisionScopeV1`), `valid_at` and `recorded_at` are part of every
  signature but any non-`None` value raises `GraphTemporalScopeUnsupported` before a transaction
  opens. P038 materializes `current` flags, resolved edges and their evidence winners for the
  present only; the per-entity intervals can answer "as of" through `entity_as_of`, which
  re-derives from the stored facts in Python (`load_facts`, `derive_*`, `resolve_edges`) for one
  entity at a time. An as-of or revision-pinned traversal would have to run that per candidate
  node and edge, which no database `LIMIT` or timeout can bound, so it is not offered.
- Bounds are enforced by the database: depth is validated to [1, 4] (above 4 is an error, never
  clamped), every query carries a `LIMIT` (500 nodes, with `truncated=True` when it is hit), and
  one read transaction per request carries the driver timeout. The same deadline bounds the client
  with `asyncio.timeout` (the server checks transaction timeouts only periodically, so the client
  clock is what makes short deadlines exact; the driver timeout stops what the client abandoned).
  Expiry raises `RetrievalDeadlineExceeded`.
- All Cypher is static text. Labels and relationship types come from closed tables below, limits are
  parameters, and variable-length paths use one static query per depth (`*1..1` to `*1..4`),
  chosen from a closed table. No input is ever interpolated.
- Order is deterministic: nodes by `(distance, node_id)`, edges by `(type, source, target)`,
  paths by `(length, node ids)`. When the node limit cuts a level, nodes are kept in
  `(kind, node_id)` order.
- Evidence: every node and edge has an `EvidenceRef` (see below). Only resolved, current edges are
  returned unless `include_alternatives=True`, which adds the assertions that lost to the winning
  evidence (`lower_evidence`) or are no longer current (`not_current`) between the returned nodes. Losers
  are read from P038's stored flags, never inferred from the capped output.

Mapping to the SDK `EvidenceRefV1` (PLATFORM-043)
-------------------------------------------------
`EvidenceRefV1` needs a `content_ref`; code events have none, so this internal type keeps the
event IDs instead. P043 must resolve `source_event_ids` through the ledger (`event_content_refs`)
to a content reference, or choose a metadata-only form, per event. `valid_from`/`valid_to` and
`recorded` times are the P038 fixed-width UTC strings (`YYYY-MM-DDTHH:MM:SS.ffffffZ`);
`evidence_kind` is `scip`, `tree_sitter`, `git` or `test` as stored. SCIP and `git` evidence is
deterministic (confidence 1.0); `tree_sitter` evidence is inferred, and `confidence` tells
syntactic facts (0.9) from heuristic ones (0.5).
Structural nodes and `IN_MODULE`/`IN_REPOSITORY` edges have `evidence_kind=None` and no confidence.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Final, LiteralString, Self, TypeVar

from agent_context_sdk import RevisionScopeV1
from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncManagedTransaction, Record, unit_of_work
from neo4j.exceptions import ConfigurationError, Neo4jError

from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.settings import Neo4jSettings

MAX_DEPTH: Final = 4
MAX_NODES: Final = 500
MAX_EDGES: Final = 2000
MAX_PATHS: Final = 50
DEFAULT_DEADLINE_SECONDS: Final = 5.0

ResultT = TypeVar("ResultT")


class RetrievalError(Exception):
    """Base class of typed retrieval failures."""


class RetrievalDeadlineExceeded(RetrievalError):
    """The request did not finish inside its deadline."""


class GraphRequestError(RetrievalError, ValueError):
    """The request is malformed or exceeds a hard bound (for example depth above 4)."""


class GraphTemporalScopeUnsupported(GraphRequestError):
    """A revision or as-of time was requested; only the current graph can be traversed."""


class GraphAnchorNotFound(RetrievalError, LookupError):
    """The ID is not a current repository, module, file or symbol of the scoped repository."""


class NodeKind(StrEnum):
    REPOSITORY = "repository"
    MODULE = "module"
    FILE = "file"
    SYMBOL = "symbol"


class EdgeKind(StrEnum):
    """The relationship types a result can contain (a closed set)."""

    CALLS = "CALLS"
    REFERENCES = "REFERENCES"
    IMPORTS = "IMPORTS"
    DEFINES = "DEFINES"
    IN_MODULE = "IN_MODULE"
    IN_REPOSITORY = "IN_REPOSITORY"


_KIND_ORDER: Final = (NodeKind.REPOSITORY, NodeKind.MODULE, NodeKind.FILE, NodeKind.SYMBOL)


@dataclass(frozen=True, slots=True)
class GraphScope:
    """The repository every node of a request must belong to."""

    repository_id: str

    def __post_init__(self) -> None:
        if not self.repository_id.strip():
            raise GraphRequestError("repository_id is required")


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    """Where a returned fact comes from (internal; P043 maps it to the SDK type)."""

    source_event_ids: tuple[str, ...]
    evidence_kind: str | None = None
    confidence: float | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    revision_id: str | None = None
    assertion_id: str | None = None
    extractor_name: str | None = None
    extractor_version: str | None = None


@dataclass(frozen=True, slots=True)
class GraphNode:
    kind: NodeKind
    node_id: str
    distance: int
    name: str | None
    evidence: EvidenceRef


@dataclass(frozen=True, slots=True)
class GraphEdge:
    kind: EdgeKind
    source_id: str
    target_id: str
    evidence: EvidenceRef


@dataclass(frozen=True, slots=True)
class GraphAlternative:
    """An assertion that did not become the resolved edge."""

    predicate: str
    subject_id: str
    object_id: str
    status: str  # lower_evidence | not_current
    current: bool
    evidence: EvidenceRef


@dataclass(frozen=True, slots=True)
class GraphNeighborhood:
    repository_id: str
    anchor: GraphNode
    depth: int
    nodes: tuple[GraphNode, ...]  # includes the anchor
    edges: tuple[GraphEdge, ...]
    alternatives: tuple[GraphAlternative, ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class GraphPath:
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]


@dataclass(frozen=True, slots=True)
class DependencyPaths:
    repository_id: str
    source_id: str
    target_id: str
    paths: tuple[GraphPath, ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class ExternalDependency:
    dependency_id: str
    dependency_kind: str | None
    requirement: str | None
    resolved_version: str | None
    evidence: EvidenceRef


@dataclass(frozen=True, slots=True)
class OutgoingDependencies:
    """What a module or file imports.

    `targets` are the imported symbols (with the anchor first, at most `MAX_NODES` nodes);
    an edge's `source_id` is the anchor or a file/symbol inside it. `external` are the current
    dependency assertions of the anchor's current file revisions.
    """

    repository_id: str
    anchor: GraphNode
    targets: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    external: tuple[ExternalDependency, ...]
    truncated: bool


# --------------------------------------------------------------------------------------------
# Cypher: static text only
# --------------------------------------------------------------------------------------------

_NODE_ID: Final[LiteralString] = "coalesce(n.symbol_id, n.file_id, n.module_id, n.repository_id)"
_END_ID: Final[LiteralString] = "coalesce(b.symbol_id, b.file_id, b.module_id, b.repository_id)"

_RESOLVE: Final[LiteralString] = (
    "MATCH (n:Repository {repository_id: $id}) WHERE n.repository_id = $repository_id "
    "RETURN 'repository' AS kind "
    "UNION MATCH (n:Module {module_id: $id}) WHERE n.repository_id = $repository_id "
    "AND EXISTS { (n)<-[:IN_MODULE]-(:File {current: true, repository_id: $repository_id}) } "
    "RETURN 'module' AS kind "
    "UNION MATCH (n:File {file_id: $id}) "
    "WHERE n.repository_id = $repository_id AND n.current = true RETURN 'file' AS kind "
    "UNION MATCH (n:Symbol {symbol_id: $id}) "
    "WHERE n.repository_id = $repository_id AND n.current = true RETURN 'symbol' AS kind"
)

_FRONTIER: Final[dict[NodeKind, LiteralString]] = {
    NodeKind.REPOSITORY: "MATCH (a:Repository) WHERE a.repository_id IN $ids "
    "AND a.repository_id = $repository_id ",
    NodeKind.MODULE: "MATCH (a:Module) WHERE a.module_id IN $ids "
    "AND a.repository_id = $repository_id ",
    NodeKind.FILE: "MATCH (a:File) WHERE a.file_id IN $ids AND a.repository_id = $repository_id ",
    NodeKind.SYMBOL: "MATCH (a:Symbol) WHERE a.symbol_id IN $ids "
    "AND a.repository_id = $repository_id ",
}
_CURRENT_CODE: Final[LiteralString] = (
    "WHERE (n:Symbol OR n:File) AND n.repository_id = $repository_id AND n.current = true "
)
_REACHED: Final[LiteralString] = (
    f"WITH DISTINCT n, {_NODE_ID} AS node_id WHERE NOT node_id IN $visited "
    "RETURN labels(n)[0] AS kind, node_id ORDER BY node_id LIMIT $limit"
)


class _Mode(StrEnum):
    NEIGHBORHOOD = "neighborhood"
    CALLERS = "callers"
    CALLEES = "callees"


_STEP: Final[dict[tuple[_Mode, NodeKind], LiteralString]] = {
    (_Mode.NEIGHBORHOOD, NodeKind.SYMBOL): (
        "MATCH (a)-[:CALLS|REFERENCES|IMPORTS|DEFINES]-(n) " + _CURRENT_CODE
    ),
    (_Mode.NEIGHBORHOOD, NodeKind.FILE): (
        "MATCH (a)-[:CALLS|REFERENCES|IMPORTS|DEFINES|IN_MODULE]-(n) "
        "WHERE n.repository_id = $repository_id "
        "AND (n:Module OR ((n:Symbol OR n:File) AND n.current = true)) "
    ),
    (_Mode.NEIGHBORHOOD, NodeKind.MODULE): (
        "MATCH (a)<-[:IN_MODULE]-(n:File) "
        "WHERE n.repository_id = $repository_id AND n.current = true "
    ),
    (_Mode.NEIGHBORHOOD, NodeKind.REPOSITORY): (
        "MATCH (a)<-[:IN_REPOSITORY]-(n) WHERE n.repository_id = $repository_id "
        "AND ((n:File AND n.current = true) "
        "OR (n:Module AND EXISTS { "
        "(n)<-[:IN_MODULE]-(:File {current: true, repository_id: $repository_id}) })) "
    ),
    (_Mode.CALLERS, NodeKind.SYMBOL): "MATCH (a)<-[:CALLS]-(n) " + _CURRENT_CODE,
    (_Mode.CALLERS, NodeKind.FILE): "MATCH (a)<-[:CALLS]-(n) " + _CURRENT_CODE,
    (_Mode.CALLEES, NodeKind.SYMBOL): "MATCH (a)-[:CALLS]->(n) " + _CURRENT_CODE,
    (_Mode.CALLEES, NodeKind.FILE): "MATCH (a)-[:CALLS]->(n) " + _CURRENT_CODE,
}
_LEVEL: Final[dict[tuple[_Mode, NodeKind], LiteralString]] = {
    (mode, kind): _FRONTIER[kind] + step + _REACHED for (mode, kind), step in _STEP.items()
}

_EDGE_SOURCES: Final[tuple[tuple[NodeKind, LiteralString, LiteralString], ...]] = (
    (NodeKind.MODULE, "Module", "module_id"),
    (NodeKind.FILE, "File", "file_id"),
    (NodeKind.SYMBOL, "Symbol", "symbol_id"),
)


def _edge_query(label: LiteralString, key: LiteralString, types: LiteralString) -> LiteralString:
    return (
        f"MATCH (a:{label}) WHERE a.{key} IN $ids AND a.repository_id = $repository_id "
        f"MATCH (a)-[r:{types}]->(b) "
        f"WHERE b.repository_id = $repository_id AND {_END_ID} IN $ids "
        "OPTIONAL MATCH (x:Assertion {assertion_id: r.resolved_assertion_id, "
        "repository_id: $repository_id})-[:OBSERVED_IN]->(fr:FileRevision) "
        f"WITH a, b, r, collect(fr.file_revision_id) AS revisions "
        f"RETURN type(r) AS type, a.{key} AS source_id, {_END_ID} AS target_id, "
        "properties(r) AS props, revisions ORDER BY type, source_id, target_id LIMIT $limit"
    )


_EDGE_TYPES: Final[dict[_Mode, LiteralString]] = {
    _Mode.NEIGHBORHOOD: "CALLS|REFERENCES|IMPORTS|DEFINES|IN_MODULE|IN_REPOSITORY",
    _Mode.CALLERS: "CALLS",
    _Mode.CALLEES: "CALLS",
}
_EDGES: Final[dict[tuple[_Mode, NodeKind], LiteralString]] = {
    (mode, kind): _edge_query(label, key, types)
    for mode, types in _EDGE_TYPES.items()
    for kind, label, key in _EDGE_SOURCES
}
_PREDICATES: Final[dict[_Mode, list[str]]] = {
    _Mode.NEIGHBORHOOD: ["CALLS", "REFERENCES", "IMPORTS", "DEFINES"],
    _Mode.CALLERS: ["CALLS"],
    _Mode.CALLEES: ["CALLS"],
}

_NODE_EVIDENCE: Final[dict[NodeKind, LiteralString]] = {
    NodeKind.SYMBOL: (
        "MATCH (n:Symbol) WHERE n.symbol_id IN $ids AND n.repository_id = $repository_id "
        "OPTIONAL MATCH (n)-[:CURRENT_REVISION]->(rev:SymbolRevision) "
        "OPTIONAL MATCH (n)-[h:HAS_REVISION]->(rev) WHERE h.valid_to IS NULL "
        "RETURN n.symbol_id AS node_id, rev.qualified_name AS name, "
        "rev.symbol_revision_id AS revision_id, rev.extractor_name AS extractor_name, "
        "rev.extractor_version AS extractor_version, h.valid_from AS valid_from, "
        "h.valid_to AS valid_to, "
        "[(n)-[i:IN_REPOSITORY]->(:Repository {repository_id: $repository_id}) "
        "| i.source_event_ids] "
        "+ [(rev)-[d:DEFINED_IN]->() | d.source_event_ids] AS events"
    ),
    NodeKind.FILE: (
        "MATCH (n:File) WHERE n.file_id IN $ids AND n.repository_id = $repository_id "
        "OPTIONAL MATCH (n)-[:CURRENT_REVISION]->(rev:FileRevision) "
        "OPTIONAL MATCH (n)-[h:HAS_REVISION]->(rev) WHERE h.valid_to IS NULL "
        "RETURN n.file_id AS node_id, n.current_path AS name, "
        "rev.file_revision_id AS revision_id, rev.extractor_name AS extractor_name, "
        "rev.extractor_version AS extractor_version, h.valid_from AS valid_from, "
        "h.valid_to AS valid_to, "
        "[(n)-[i:IN_REPOSITORY]->(:Repository {repository_id: $repository_id}) "
        "| i.source_event_ids] "
        "+ [(rev)-[m:MEMBER_OF]->(:Commit|WorkspaceSnapshot {repository_id: $repository_id}) "
        "| m.source_event_ids] AS events"
    ),
    NodeKind.MODULE: (
        "MATCH (n:Module) WHERE n.module_id IN $ids AND n.repository_id = $repository_id "
        "RETURN n.module_id AS node_id, null AS name, null AS revision_id, "
        "null AS extractor_name, null AS extractor_version, null AS valid_from, "
        "null AS valid_to, "
        "[(n)-[i:IN_REPOSITORY]->(:Repository {repository_id: $repository_id}) "
        "| i.source_event_ids] AS events"
    ),
    NodeKind.REPOSITORY: (
        "MATCH (n:Repository) WHERE n.repository_id IN $ids "
        "AND n.repository_id = $repository_id "
        "RETURN n.repository_id AS node_id, null AS name, null AS revision_id, "
        "null AS extractor_name, null AS extractor_version, null AS valid_from, "
        "null AS valid_to, "
        "[(n)-[i:INDEXED]->(:Commit|WorkspaceSnapshot {repository_id: $repository_id}) "
        "| i.source_event_ids] AS events"
    ),
}

# A loser is read from what P038 stored, never inferred from the (capped) output: a non-current
# assertion, or a current one listed in `lower_evidence_assertion_ids` of the resolved edge of its
# own triple. The resolved winner is on neither list, so it can never be reported as an alternative.
_ALTERNATIVES: Final[LiteralString] = (
    "MATCH (a:Assertion {repository_id: $repository_id, family: 'relation'}) "
    "WHERE a.subject_id IN $ids AND a.object_id IN $ids AND a.predicate IN $predicates "
    "AND (a.current IS NULL OR a.current = false "
    "OR EXISTS { (s:Symbol {symbol_id: a.subject_id, repository_id: $repository_id})"
    "-[r]->(o:Symbol {symbol_id: a.object_id, repository_id: $repository_id}) "
    "WHERE type(r) = a.predicate AND a.assertion_id IN r.lower_evidence_assertion_ids } "
    "OR EXISTS { (s:File {file_id: a.subject_id, repository_id: $repository_id})"
    "-[r]->(o:Symbol {symbol_id: a.object_id, repository_id: $repository_id}) "
    "WHERE type(r) = a.predicate AND a.assertion_id IN r.lower_evidence_assertion_ids }) "
    "RETURN a.assertion_id AS assertion_id, a.predicate AS predicate, "
    "a.subject_id AS subject_id, a.object_id AS object_id, a.current AS current, "
    "properties(a) AS props, "
    "[(a)-[:OBSERVED_IN]->(fr:FileRevision) | fr.file_revision_id] AS revisions "
    "ORDER BY assertion_id LIMIT $limit"
)

_PATH_KINDS: Final = {
    NodeKind.FILE: ("File", "file_id"),
    NodeKind.SYMBOL: ("Symbol", "symbol_id"),
}


def _path_query(
    source_label: LiteralString,
    source_key: LiteralString,
    target_label: LiteralString,
    target_key: LiteralString,
    hops: LiteralString,
) -> LiteralString:
    return (
        f"MATCH (a:{source_label} {{{source_key}: $source_id}}) "
        f"MATCH (b:{target_label} {{{target_key}: $target_id}}) "
        "WHERE a.repository_id = $repository_id AND b.repository_id = $repository_id "
        "AND a.current = true AND b.current = true "
        f"MATCH p = (a)-[:CALLS|REFERENCES|IMPORTS*{hops}]->(b) "
        "WITH p, nodes(p) AS ns "
        "WHERE all(x IN ns WHERE x.repository_id = $repository_id AND x.current = true) "
        "AND all(i IN range(0, size(ns) - 2) WHERE NOT ns[i] IN ns[i + 1..]) "
        "WITH p, ns, [x IN ns | coalesce(x.symbol_id, x.file_id)] AS ids "
        "RETURN ids, [x IN ns | labels(x)[0]] AS labels, "
        "[r IN relationships(p) | {type: type(r), props: properties(r), "
        "revisions: [(x:Assertion {assertion_id: r.resolved_assertion_id, "
        "repository_id: $repository_id})-[:OBSERVED_IN]->(fr:FileRevision) "
        "| fr.file_revision_id]}] AS rels "
        "ORDER BY length(p), ids LIMIT $limit"
    )


_HOPS: Final[dict[int, LiteralString]] = {1: "1..1", 2: "1..2", 3: "1..3", 4: "1..4"}
_PATHS: Final[dict[tuple[NodeKind, NodeKind, int], LiteralString]] = {
    (source, target, depth): _path_query(*_PATH_KINDS[source], *_PATH_KINDS[target], hops)
    for source in _PATH_KINDS
    for target in _PATH_KINDS
    for depth, hops in _HOPS.items()
}

_IMPORT_TAIL: Final[LiteralString] = (
    "UNWIND subjects AS s MATCH (s)-[r:IMPORTS]->(t:Symbol) "
    "WHERE s.repository_id = $repository_id AND t.repository_id = $repository_id "
    "AND t.current = true "
    "OPTIONAL MATCH (x:Assertion {assertion_id: r.resolved_assertion_id, "
    "repository_id: $repository_id})-[:OBSERVED_IN]->(fr:FileRevision) "
    "WITH s, r, t, collect(fr.file_revision_id) AS revisions "
    "RETURN coalesce(s.symbol_id, s.file_id) AS source_id, t.symbol_id AS target_id, "
    "properties(r) AS props, revisions ORDER BY target_id, source_id LIMIT $limit"
)
# The subjects of an import query (the anchor's files and the symbols they define) are cut inside
# the database, so memory is bounded by the cap and not by the anchor's size. Each cut is paired
# with `_MEMBERS_CUT`, the same walk with one row more: when it sees more than the cap, the
# result is `truncated` even if the cut subjects have no imports left to show.
_FILE_SUBJECTS: Final[LiteralString] = (
    "MATCH (f:File {file_id: $id}) WHERE f.repository_id = $repository_id "
    "AND f.current = true "
    "OPTIONAL MATCH (f)-[:DEFINES]->(d:Symbol) "
    "WHERE d.repository_id = $repository_id AND d.current = true "
)
_MODULE_SUBJECTS: Final[LiteralString] = (
    "MATCH (m:Module {module_id: $id}) WHERE m.repository_id = $repository_id "
    "MATCH (m)<-[:IN_MODULE]-(f:File) "
    "WHERE f.repository_id = $repository_id AND f.current = true "
    "WITH f ORDER BY f.file_id LIMIT {FILES} "
    "OPTIONAL MATCH (f)-[:DEFINES]->(d:Symbol) "
    "WHERE d.repository_id = $repository_id AND d.current = true "
)
_IMPORTS: Final[dict[NodeKind, LiteralString]] = {
    NodeKind.FILE: (
        _FILE_SUBJECTS + "WITH f, d ORDER BY d.symbol_id LIMIT 500 "
        "WITH f, collect(d) + [f] AS subjects " + _IMPORT_TAIL
    ),
    NodeKind.MODULE: (
        _MODULE_SUBJECTS.replace("{FILES}", "500") + "WITH f, d ORDER BY f.file_id, d.symbol_id "
        "LIMIT 500 WITH collect(DISTINCT f) + collect(DISTINCT d) AS subjects " + _IMPORT_TAIL
    ),
}
_MEMBERS_CUT: Final[dict[NodeKind, LiteralString]] = {
    NodeKind.FILE: (
        _FILE_SUBJECTS + "WITH f, d ORDER BY d.symbol_id LIMIT 501 RETURN count(*) AS members"
    ),
    NodeKind.MODULE: (
        _MODULE_SUBJECTS.replace("{FILES}", "501")
        + "WITH f, d ORDER BY f.file_id, d.symbol_id LIMIT 501 RETURN count(*) AS members"
    ),
}
_EXTERNAL_TAIL: Final[LiteralString] = (
    "MATCH (fr)<-[:OBSERVED_IN]-(a:Assertion {family: 'dependency'}) "
    "WHERE a.repository_id = $repository_id AND a.current = true "
    "MATCH (a)-[:DEPENDS_ON]->(dep:Dependency) WHERE dep.repository_id = $repository_id "
    "RETURN dep.dependency_id AS dependency_id, fr.file_revision_id AS revision_id, "
    "properties(a) AS props ORDER BY dependency_id, a.assertion_id LIMIT $limit"
)
_EXTERNAL: Final[dict[NodeKind, LiteralString]] = {
    NodeKind.FILE: (
        "MATCH (f:File {file_id: $id})-[:CURRENT_REVISION]->(fr:FileRevision) "
        "WHERE f.repository_id = $repository_id AND f.current = true " + _EXTERNAL_TAIL
    ),
    NodeKind.MODULE: (
        "MATCH (m:Module {module_id: $id})<-[:IN_MODULE]-(f:File)"
        "-[:CURRENT_REVISION]->(fr:FileRevision) "
        "WHERE m.repository_id = $repository_id AND f.repository_id = $repository_id "
        "AND f.current = true " + _EXTERNAL_TAIL
    ),
}


# --------------------------------------------------------------------------------------------
# Service
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Placed:
    kind: NodeKind
    node_id: str
    distance: int


class GraphTraversalService:
    """Typed read-only traversal of the projected code graph."""

    def __init__(
        self,
        driver: AsyncDriver,
        *,
        database: str,
        default_deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
        owns_driver: bool = False,
    ) -> None:
        if default_deadline_seconds <= 0:
            raise GraphRequestError("deadline must be positive")
        self._driver = driver
        self._database = database
        self._default_deadline = default_deadline_seconds
        self._owns_driver = owns_driver

    @classmethod
    def from_settings(
        cls,
        settings: Neo4jSettings,
        *,
        default_deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
    ) -> Self:
        """Open a driver the service owns (closed by `close`), like `Neo4jStore` does."""
        if (
            settings.uri is None
            or settings.username is None
            or not settings.username.strip()
            or settings.password is None
            or not settings.password.get_secret_value().strip()
            or not settings.database.strip()
        ):
            raise ConfigurationError("Neo4j connection settings are incomplete")
        driver = AsyncGraphDatabase.driver(
            str(settings.uri),
            auth=(settings.username, settings.password.get_secret_value()),
            connection_timeout=settings.connection_timeout,
            connection_acquisition_timeout=settings.connection_acquisition_timeout,
            max_transaction_retry_time=settings.max_transaction_retry_time,
            notifications_min_severity="OFF",  # unknown-label hints are normal on a sparse graph
        )
        return cls(
            driver,
            database=settings.database,
            default_deadline_seconds=default_deadline_seconds,
            owns_driver=True,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_driver:
            await self._driver.close()

    # --- operations ---

    async def neighborhood(
        self,
        scope: GraphScope,
        anchor: str,
        depth: int = 2,
        *,
        include_alternatives: bool = False,
        revision: RevisionScopeV1 | None = None,
        valid_at: datetime | None = None,
        recorded_at: datetime | None = None,
        deadline_seconds: float | None = None,
    ) -> GraphNeighborhood:
        """Nodes within `depth` hops of a repository, module, file or symbol, undirected."""
        _current_only(revision, valid_at, recorded_at)
        _check_depth(depth)
        return await self._run(
            deadline_seconds,
            lambda tx: _walk(tx, scope, anchor, depth, _Mode.NEIGHBORHOOD, include_alternatives),
        )

    async def callers(
        self,
        scope: GraphScope,
        symbol: str,
        depth: int = 1,
        *,
        include_alternatives: bool = False,
        revision: RevisionScopeV1 | None = None,
        valid_at: datetime | None = None,
        recorded_at: datetime | None = None,
        deadline_seconds: float | None = None,
    ) -> GraphNeighborhood:
        """Who calls `symbol`, transitively up to `depth` (a caller can be a file)."""
        _current_only(revision, valid_at, recorded_at)
        _check_depth(depth)
        return await self._run(
            deadline_seconds,
            lambda tx: _walk(tx, scope, symbol, depth, _Mode.CALLERS, include_alternatives),
        )

    async def callees(
        self,
        scope: GraphScope,
        symbol: str,
        depth: int = 1,
        *,
        include_alternatives: bool = False,
        revision: RevisionScopeV1 | None = None,
        valid_at: datetime | None = None,
        recorded_at: datetime | None = None,
        deadline_seconds: float | None = None,
    ) -> GraphNeighborhood:
        """What `symbol` calls, transitively up to `depth`."""
        _current_only(revision, valid_at, recorded_at)
        _check_depth(depth)
        return await self._run(
            deadline_seconds,
            lambda tx: _walk(tx, scope, symbol, depth, _Mode.CALLEES, include_alternatives),
        )

    async def dependency_paths(
        self,
        scope: GraphScope,
        source: str,
        target: str,
        *,
        max_depth: int = MAX_DEPTH,
        max_paths: int = 10,
        revision: RevisionScopeV1 | None = None,
        valid_at: datetime | None = None,
        recorded_at: datetime | None = None,
        deadline_seconds: float | None = None,
    ) -> DependencyPaths:
        """Simple directed paths of CALLS/REFERENCES/IMPORTS edges between two files or symbols."""
        _current_only(revision, valid_at, recorded_at)
        _check_depth(max_depth)
        if not 1 <= max_paths <= MAX_PATHS:
            raise GraphRequestError(f"max_paths must be within [1, {MAX_PATHS}]")
        return await self._run(
            deadline_seconds,
            lambda tx: _paths(tx, scope, source, target, max_depth, max_paths),
        )

    async def dependencies(
        self,
        scope: GraphScope,
        anchor: str,
        *,
        revision: RevisionScopeV1 | None = None,
        valid_at: datetime | None = None,
        recorded_at: datetime | None = None,
        deadline_seconds: float | None = None,
    ) -> OutgoingDependencies:
        """Outgoing imports and external dependencies of a module or file."""
        _current_only(revision, valid_at, recorded_at)
        return await self._run(deadline_seconds, lambda tx: _outgoing(tx, scope, anchor))

    # --- transaction boundary ---

    async def _run(
        self,
        deadline_seconds: float | None,
        work: Callable[[Neo4jTransaction], Awaitable[ResultT]],
    ) -> ResultT:
        deadline = self._default_deadline if deadline_seconds is None else deadline_seconds
        if deadline <= 0:
            raise GraphRequestError("deadline must be positive")

        async def execute(transaction: AsyncManagedTransaction) -> ResultT:
            return await work(Neo4jTransaction(transaction))

        async def attempt() -> ResultT:
            async with self._driver.session(database=self._database) as session:
                return await session.execute_read(unit_of_work(timeout=deadline)(execute))

        try:
            async with asyncio.timeout(deadline):
                return await attempt()
        except TimeoutError as error:
            raise RetrievalDeadlineExceeded("graph request exceeded its deadline") from error
        except Neo4jError as error:
            if "TransactionTimedOut" in (error.code or ""):
                raise RetrievalDeadlineExceeded("graph request exceeded its deadline") from error
            raise


def _current_only(
    revision: RevisionScopeV1 | None, valid_at: datetime | None, recorded_at: datetime | None
) -> None:
    if revision is not None or valid_at is not None or recorded_at is not None:
        raise GraphTemporalScopeUnsupported(
            "only the current graph can be traversed; revision, valid_at and recorded_at "
            "are not supported"
        )


def _check_depth(depth: int) -> None:
    if isinstance(depth, bool) or not isinstance(depth, int):
        raise GraphRequestError("depth must be an integer")
    if not 1 <= depth <= MAX_DEPTH:
        raise GraphRequestError(f"depth must be within [1, {MAX_DEPTH}], got {depth}")


# --------------------------------------------------------------------------------------------
# Query steps (all take the read transaction of one request)
# --------------------------------------------------------------------------------------------


async def _resolve(tx: Neo4jTransaction, scope: GraphScope, node_id: str) -> NodeKind:
    records = (
        await tx.run(_RESOLVE, parameters={"id": node_id, "repository_id": scope.repository_id})
    ).records
    found = {NodeKind(str(record["kind"])) for record in records}
    for kind in _KIND_ORDER:
        if kind in found:
            return kind
    raise GraphAnchorNotFound("no current node with that ID in the repository")


async def _walk(
    tx: Neo4jTransaction,
    scope: GraphScope,
    anchor_id: str,
    depth: int,
    mode: _Mode,
    include_alternatives: bool,
) -> GraphNeighborhood:
    anchor_kind = await _resolve(tx, scope, anchor_id)
    if (mode, anchor_kind) not in _LEVEL:
        raise GraphRequestError(f"{mode.value} needs a symbol or file, got a {anchor_kind.value}")
    placed = [_Placed(anchor_kind, anchor_id, 0)]
    seen = {anchor_id}
    frontier = {anchor_kind: [anchor_id]}
    truncated = False
    for distance in range(1, depth + 1):
        level: list[_Placed] = []
        for kind in _KIND_ORDER:
            ids = frontier.get(kind)
            if not ids:
                continue
            room = MAX_NODES - len(placed) - len(level)
            rows = (
                await tx.run(
                    _LEVEL[(mode, kind)],
                    parameters={
                        "ids": ids,
                        "repository_id": scope.repository_id,
                        "visited": sorted(seen | {item.node_id for item in level}),
                        "limit": room + 1,
                    },
                )
            ).records
            if len(rows) > room:
                truncated = True
                rows = rows[:room]
            level.extend(
                _Placed(NodeKind(str(row["kind"]).lower()), str(row["node_id"]), distance)
                for row in rows
            )
        if not level:
            break
        placed.extend(level)
        seen.update(item.node_id for item in level)
        frontier = {}
        for item in level:
            frontier.setdefault(item.kind, []).append(item.node_id)
        if truncated:
            break
    nodes = await _materialize(tx, scope, placed)
    edges, edges_cut = await _edges(tx, scope, mode, nodes)
    alternatives: tuple[GraphAlternative, ...] = ()
    alternatives_cut = False
    if include_alternatives:
        alternatives, alternatives_cut = await _alternatives(tx, scope, mode, nodes)
    return GraphNeighborhood(
        repository_id=scope.repository_id,
        anchor=nodes[0],
        depth=depth,
        nodes=tuple(nodes),
        edges=tuple(edges),
        alternatives=alternatives,
        truncated=truncated or edges_cut or alternatives_cut,
    )


def _evidence(
    props: Mapping[str, Any],
    revisions: Iterable[str] = (),
    *,
    events: Iterable[str] | None = None,
    assertion_id: str | None = None,
) -> EvidenceRef:
    source = props.get("source_event_ids") if events is None else events
    return EvidenceRef(
        source_event_ids=tuple(sorted({str(event) for event in source or ()})),
        evidence_kind=props.get("evidence_kind"),
        confidence=props.get("confidence"),
        valid_from=props.get("valid_from"),
        valid_to=props.get("valid_to"),
        revision_id=min(revisions, default=None),
        assertion_id=assertion_id or props.get("resolved_assertion_id"),
        extractor_name=props.get("extractor_name"),
        extractor_version=props.get("extractor_version"),
    )


async def _materialize(
    tx: Neo4jTransaction, scope: GraphScope, placed: Sequence[_Placed]
) -> list[GraphNode]:
    """Nodes with evidence, anchor (or first node) first, then by `(distance, node_id)`."""
    by_kind: dict[NodeKind, list[str]] = {}
    for item in placed:
        by_kind.setdefault(item.kind, []).append(item.node_id)
    rows: dict[str, Record] = {}
    for kind, ids in by_kind.items():
        result = await tx.run(
            _NODE_EVIDENCE[kind],
            parameters={"ids": sorted(ids), "repository_id": scope.repository_id},
        )
        for found in result.records:
            rows.setdefault(str(found["node_id"]), found)
    nodes: list[GraphNode] = []
    for item in sorted(placed, key=lambda p: (p.distance, p.node_id)):
        record = rows.get(item.node_id)
        if record is None:
            continue
        events = [event for batch in record["events"] or () for event in batch or ()]
        nodes.append(
            GraphNode(
                kind=item.kind,
                node_id=item.node_id,
                distance=item.distance,
                name=record["name"],
                evidence=_evidence(
                    {
                        "valid_from": record["valid_from"],
                        "valid_to": record["valid_to"],
                        "extractor_name": record["extractor_name"],
                        "extractor_version": record["extractor_version"],
                    },
                    [record["revision_id"]] if record["revision_id"] else [],
                    events=events,
                ),
            )
        )
    return nodes


async def _edges(
    tx: Neo4jTransaction, scope: GraphScope, mode: _Mode, nodes: Sequence[GraphNode]
) -> tuple[list[GraphEdge], bool]:
    ids = sorted(node.node_id for node in nodes)
    by_id = {node.node_id: node for node in nodes}
    edges: list[GraphEdge] = []
    cut = False
    for kind, _label, _key in _EDGE_SOURCES:
        room = MAX_EDGES - len(edges)
        rows = (
            await tx.run(
                _EDGES[(mode, kind)],
                parameters={"ids": ids, "repository_id": scope.repository_id, "limit": room + 1},
            )
        ).records
        if len(rows) > room:
            cut = True
            rows = rows[:room]
        for row in rows:
            edge_kind = EdgeKind(str(row["type"]))
            props: Mapping[str, Any] = row["props"]
            if edge_kind is EdgeKind.IN_MODULE:
                # Membership is derived from the file's current revision: its evidence is the file's.
                evidence = by_id[str(row["source_id"])].evidence
            else:
                evidence = _evidence(props, row["revisions"])
            edges.append(
                GraphEdge(edge_kind, str(row["source_id"]), str(row["target_id"]), evidence)
            )
    edges.sort(key=lambda e: (e.kind.value, e.source_id, e.target_id))
    return edges, cut


async def _alternatives(
    tx: Neo4jTransaction,
    scope: GraphScope,
    mode: _Mode,
    nodes: Sequence[GraphNode],
) -> tuple[tuple[GraphAlternative, ...], bool]:
    rows = (
        await tx.run(
            _ALTERNATIVES,
            parameters={
                "ids": sorted(node.node_id for node in nodes),
                "repository_id": scope.repository_id,
                "predicates": _PREDICATES[mode],
                "limit": MAX_EDGES + 1,
            },
        )
    ).records
    cut = len(rows) > MAX_EDGES
    rows = rows[:MAX_EDGES]
    found: list[GraphAlternative] = []
    for row in rows:
        assertion_id = str(row["assertion_id"])
        current = bool(row["current"])
        status = "lower_evidence" if current else "not_current"  # the query returns no others
        found.append(
            GraphAlternative(
                predicate=str(row["predicate"]),
                subject_id=str(row["subject_id"]),
                object_id=str(row["object_id"]),
                status=status,
                current=current,
                evidence=_evidence(row["props"], row["revisions"], assertion_id=assertion_id),
            )
        )
    return tuple(found), cut


async def _paths(
    tx: Neo4jTransaction,
    scope: GraphScope,
    source: str,
    target: str,
    max_depth: int,
    max_paths: int,
) -> DependencyPaths:
    kinds: list[NodeKind] = []
    for node_id in (source, target):
        kind = await _resolve(tx, scope, node_id)
        if kind not in _PATH_KINDS:
            raise GraphRequestError(f"paths connect files and symbols, got a {kind.value}")
        kinds.append(kind)
    rows = (
        await tx.run(
            _PATHS[(kinds[0], kinds[1], max_depth)],
            parameters={
                "source_id": source,
                "target_id": target,
                "repository_id": scope.repository_id,
                "limit": max_paths + 1,
            },
        )
    ).records
    truncated = len(rows) > max_paths
    rows = rows[:max_paths]
    placed = {
        str(node_id): _Placed(NodeKind(str(label).lower()), str(node_id), 0)
        for row in rows
        for node_id, label in zip(row["ids"], row["labels"], strict=True)
    }
    nodes = {node.node_id: node for node in await _materialize(tx, scope, list(placed.values()))}
    paths: list[GraphPath] = []
    for row in rows:
        ids = [str(i) for i in row["ids"]]
        edges = tuple(
            GraphEdge(
                EdgeKind(str(rel["type"])),
                ids[index],
                ids[index + 1],
                _evidence(rel["props"], rel["revisions"]),
            )
            for index, rel in enumerate(row["rels"])
        )
        paths.append(GraphPath(tuple(nodes[i] for i in ids if i in nodes), edges))
    return DependencyPaths(scope.repository_id, source, target, tuple(paths), truncated)


async def _outgoing(tx: Neo4jTransaction, scope: GraphScope, anchor: str) -> OutgoingDependencies:
    kind = await _resolve(tx, scope, anchor)
    if kind not in _IMPORTS:
        raise GraphRequestError(f"dependencies need a module or file, got a {kind.value}")
    params: dict[str, object] = {"id": anchor, "repository_id": scope.repository_id}
    # Edges are capped one below the node cap, so anchor plus targets never exceeds MAX_NODES.
    rows = (await tx.run(_IMPORTS[kind], parameters=params | {"limit": MAX_NODES})).records
    truncated = len(rows) >= MAX_NODES
    members = (await tx.run(_MEMBERS_CUT[kind], parameters=params)).records[0]["members"]
    truncated = truncated or int(members) > MAX_NODES  # a cut of the subjects is never silent
    rows = rows[: MAX_NODES - 1]
    targets = sorted({str(row["target_id"]) for row in rows})
    placed = [_Placed(kind, anchor, 0), *(_Placed(NodeKind.SYMBOL, t, 1) for t in targets)]
    nodes = await _materialize(tx, scope, placed)
    edges = tuple(
        sorted(
            (
                GraphEdge(
                    EdgeKind.IMPORTS,
                    str(row["source_id"]),
                    str(row["target_id"]),
                    _evidence(row["props"], row["revisions"]),
                )
                for row in rows
            ),
            key=lambda e: (e.target_id, e.source_id),
        )
    )
    external_rows = (
        await tx.run(_EXTERNAL[kind], parameters=params | {"limit": MAX_NODES + 1})
    ).records
    if len(external_rows) > MAX_NODES:
        truncated = True
        external_rows = external_rows[:MAX_NODES]
    external = tuple(
        ExternalDependency(
            dependency_id=str(row["dependency_id"]),
            dependency_kind=row["props"].get("dependency_kind"),
            requirement=row["props"].get("requirement"),
            resolved_version=row["props"].get("resolved_version"),
            evidence=_evidence(
                row["props"],
                [row["revision_id"]] if row["revision_id"] else [],
                assertion_id=row["props"].get("assertion_id"),
            ),
        )
        for row in external_rows
    )
    return OutgoingDependencies(
        repository_id=scope.repository_id,
        anchor=nodes[0],
        targets=tuple(nodes),
        edges=edges,
        external=external,
        truncated=truncated,
    )
