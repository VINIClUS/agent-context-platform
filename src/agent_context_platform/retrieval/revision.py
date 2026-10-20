"""The code graph AS INDEXED AT one revision: a commit or a workspace snapshot (PLATFORM-043).

`GraphTraversalService` (PLATFORM-040) reads the CURRENT graph only, and "current" follows the
latest index run, which may be a dirty workspace snapshot. A commit query that trusted it could
see uncommitted or later code. This module therefore derives the state of one index run from the
immutable facts the code projector stores, with the projector's OWN pure helpers
(`load_facts`, `derive_file`, `derive_symbol`, `derive_assertion`, `interval_at`), so what a
revision contains agrees with the projector by construction (replayed claims, renames that keep
a file's logical ID, path-bound symbol identities, coverage-gated absence):

1. The runs: `(Repository)-[:INDEXED {index_id}]->(Commit|WorkspaceSnapshot)` and their latest
   `COMPLETED` outcome. A revision resolves to the latest run of its target that completed
   successfully, else to its latest run (`complete=False`, a warning for the caller).
2. The candidates: the files that are `MEMBER_OF` that run, and the symbols and assertions claimed
   on their file revisions (`DEFINED_IN`, `OBSERVED_IN`) by any run.
3. The derivation: each candidate's validity intervals over every run, read at the run's start
   (`interval_at`). An unchanged symbol is a replayed claim of an earlier run; it is present
   because its file revision is a member of this run, exactly as the projector decides.

A revision's time window is `[its run's start, the start of the repository's next run)`, open
when it is the latest run. Valid time here is index-run time (the events carry no commit time),
so re-indexing an old commit later makes it the latest run.

Every query is static, parameterized Cypher in one read transaction through the read-only facade,
matched on `repository_id`, with a `LIMIT` (`truncated` says a cap was hit).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final, LiteralString

from agent_context_sdk import CommitRevisionV1, WorkspaceSnapshotRevisionV1

from agent_context_platform.projection.neo4j import Neo4jReadFacade, Neo4jTransaction
from agent_context_platform.projection.projectors import code
from agent_context_platform.projection.projectors.git import commit_node_id
from agent_context_platform.retrieval.graph import RetrievalError

MAX_RUNS: Final = 1000
MAX_FILES: Final = 2000
MAX_SYMBOLS: Final = 5000
MAX_ASSERTIONS: Final = 10000
MAX_HISTORY: Final = 100
# Deterministic evidence outranks syntactic, which outranks heuristic (as the code projector ranks).
EVIDENCE_RANK: Final = {"scip": 3, "git": 3, "test": 3, "tree_sitter": 2}


class ScopeNotFound(RetrievalError, LookupError):
    """A repository, project, revision or symbol of the request does not exist in scope."""


class RevisionNotFound(ScopeNotFound):
    """The revision was never indexed for the repository."""


@dataclass(frozen=True, slots=True)
class Run:
    index_id: str
    label: str  # Commit | WorkspaceSnapshot
    target_id: str
    occurred_at: str
    source_event_ids: tuple[str, ...]
    complete: bool

    @property
    def key(self) -> str:
        """The code projector's run key."""
        return f"{self.index_id}|{self.target_id}"

    @property
    def order(self) -> tuple[str, str]:
        return (self.occurred_at, self.key)


@dataclass(frozen=True, slots=True)
class FileFact:
    file_id: str
    file_revision_id: str
    path: str
    module_id: str
    language: str | None
    extractor_name: str | None
    extractor_version: str | None
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SymbolFact:
    symbol_id: str
    symbol_revision_id: str
    qualified_name: str
    kind: str | None
    file_id: str
    file_revision_id: str
    extractor_name: str | None
    extractor_version: str | None
    scip_symbol: str | None
    valid_from: str
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AssertionFact:
    assertion_id: str
    family: str
    subject_id: str
    predicate: str | None
    object_id: str | None
    evidence_kind: str | None
    confidence: float | None
    extractor_name: str | None
    extractor_version: str | None
    review_status: str | None
    valid_from: str | None
    file_revision_id: str | None
    dependency_id: str | None
    dependency_kind: str | None
    requirement: str | None
    resolved_version: str | None
    source_event_ids: tuple[str, ...]

    @property
    def rank(self) -> tuple[int, float]:
        return (EVIDENCE_RANK.get(self.evidence_kind or "", 0), float(self.confidence or 0.0))


@dataclass(frozen=True, slots=True)
class SymbolRevisionFact:
    """One revision a symbol had up to the requested run, with the run that opened it."""

    symbol_revision_id: str
    qualified_name: str
    first_seen: str
    until: str | None  # when a later run (up to the requested one) replaced it
    run_label: str
    run_target_id: str
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RevisionView:
    repository_id: str
    run: Run
    previous: Run | None  # the latest earlier commit run
    window_end: str | None  # start of the repository's next run, exclusive; None: still open
    history_runs: tuple[Run, ...]  # every run up to and including `run`, in run order
    files: tuple[FileFact, ...]
    symbols: tuple[SymbolFact, ...]
    assertions: tuple[AssertionFact, ...]
    truncated: bool

    def relations(self) -> tuple[AssertionFact, ...]:
        """One winning relation assertion per (subject, predicate, object) between members."""
        members = {item.file_id for item in self.files} | {s.symbol_id for s in self.symbols}
        best: dict[tuple[str, str, str], AssertionFact] = {}
        for fact in self.assertions:
            if fact.family != "relation" or fact.predicate is None or fact.object_id is None:
                continue
            if fact.subject_id not in members or fact.object_id not in members:
                continue
            key = (fact.subject_id, fact.predicate, fact.object_id)
            held = best.get(key)
            if held is None or (fact.rank, held.assertion_id) > (held.rank, fact.assertion_id):
                best[key] = fact
        return tuple(best[key] for key in sorted(best))

    def dependencies(self) -> tuple[AssertionFact, ...]:
        found = {
            fact.assertion_id: fact
            for fact in self.assertions
            if fact.family == "dependency" and fact.dependency_id is not None
        }
        return tuple(found[key] for key in sorted(found))


# --------------------------------------------------------------------------------------------
# Static Cypher
# --------------------------------------------------------------------------------------------

_REPOSITORY: Final[LiteralString] = (
    "MATCH (r:Repository {repository_id: $repository_id}) RETURN r.repository_id AS id LIMIT 1"
)
_PROJECT: Final[LiteralString] = (
    "MATCH (p:Project {project_id: $project_id}) RETURN p.project_id AS id LIMIT 1"
)
_PROJECTS: Final[LiteralString] = (
    "MATCH (p:Project)-[:USES_REPOSITORY]->(:Repository {repository_id: $repository_id}) "
    "RETURN DISTINCT p.project_id AS id ORDER BY id LIMIT $limit"
)
_RUNS: Final[LiteralString] = (
    "MATCH (:Repository {repository_id: $repository_id})-[i:INDEXED]->(t) "
    "WHERE (t:Commit OR t:WorkspaceSnapshot) AND t.repository_id = $repository_id "
    "OPTIONAL MATCH (:Repository {repository_id: $repository_id})"
    "-[c:COMPLETED {index_id: i.index_id}]->(t) "
    "WITH i, t, c ORDER BY c.event_order DESC "
    "WITH i, t, collect(c)[0] AS outcome "
    "RETURN i.index_id AS index_id, CASE WHEN t:Commit THEN 'Commit' "
    "ELSE 'WorkspaceSnapshot' END AS label, coalesce(t.commit_id, t.snapshot_id) AS target_id, "
    "i.occurred_at AS occurred_at, i.source_event_ids AS events, "
    "outcome.success AS success "
    "ORDER BY occurred_at DESC, index_id DESC LIMIT $limit"
)


def _files_query(label: LiteralString, key: LiteralString) -> LiteralString:
    return (
        f"MATCH (t:{label} {{{key}: $target_id}}) WHERE t.repository_id = $repository_id "
        "MATCH (fr:FileRevision)-[m:MEMBER_OF {index_id: $index_id}]->(t) "
        "RETURN fr.file_revision_id AS revision_id, coalesce(m.file_id, fr.file_id) AS file_id, "
        "m.path AS path, m.module_id AS module_id, fr.language AS language, "
        "fr.extractor_name AS extractor_name, fr.extractor_version AS extractor_version, "
        "m.source_event_ids AS events ORDER BY path, revision_id LIMIT $limit"
    )


_FILES: Final[dict[str, LiteralString]] = {
    "Commit": _files_query("Commit", "commit_id"),
    "WorkspaceSnapshot": _files_query("WorkspaceSnapshot", "snapshot_id"),
}
_CANDIDATE_SYMBOLS: Final[LiteralString] = (
    "UNWIND $revisions AS revision_id "
    "MATCH (sr:SymbolRevision)-[:DEFINED_IN]->(:FileRevision {file_revision_id: revision_id}) "
    "MATCH (s:Symbol {symbol_id: sr.symbol_id}) WHERE s.repository_id = $repository_id "
    "RETURN DISTINCT sr.symbol_id AS id ORDER BY id LIMIT $limit"
)
_CANDIDATE_ASSERTIONS: Final[LiteralString] = (
    "UNWIND $revisions AS revision_id "
    "MATCH (a:Assertion)-[:OBSERVED_IN]->(:FileRevision {file_revision_id: revision_id}) "
    "WHERE a.repository_id = $repository_id "
    "RETURN DISTINCT a.assertion_id AS id ORDER BY id LIMIT $limit"
)
# The properties and provenance of the symbol revisions present at the run, through their
# `DEFINED_IN` claims on the run's member file revisions.
_SYMBOL_REVISIONS: Final[LiteralString] = (
    "UNWIND $ids AS id "
    "MATCH (sr:SymbolRevision {symbol_revision_id: id})-[d:DEFINED_IN]->(fr:FileRevision) "
    "WHERE fr.file_revision_id IN $revisions "
    "RETURN properties(sr) AS props, fr.file_revision_id AS revision_id, fr.file_id AS file_id, "
    "d.source_event_ids AS events ORDER BY id, revision_id"
)
# Properties are read as maps: optional ones (a dependency's `requirement`, a symbol's
# `scip_symbol`) may be absent, and naming an absent key makes the server log a notification.
_ASSERTION_PROPS: Final[LiteralString] = (
    "UNWIND $ids AS id MATCH (a:Assertion {assertion_id: id}) "
    "WHERE a.repository_id = $repository_id "
    "OPTIONAL MATCH (a)-[:OBSERVED_IN]->(fr:FileRevision) WHERE fr.file_revision_id IN $revisions "
    "OPTIONAL MATCH (a)-[:DEPENDS_ON]->(dep:Dependency) WHERE dep.repository_id = $repository_id "
    "WITH a, dep, min(fr.file_revision_id) AS revision_id "
    "RETURN properties(a) AS props, revision_id, dep.dependency_id AS dependency_id "
    "ORDER BY a.assertion_id"
)
_SYMBOL_CLAIMS: Final[LiteralString] = (
    "UNWIND $ids AS id "
    "MATCH (sr:SymbolRevision {symbol_revision_id: id})-[d:DEFINED_IN]->(:FileRevision) "
    "WHERE d.occurred_at <= $at "
    "RETURN sr.symbol_revision_id AS symbol_revision_id, sr.qualified_name AS qualified_name, "
    "d.source_event_ids AS events ORDER BY symbol_revision_id"
)


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(sorted({str(item) for item in value if item is not None}))


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def target_id(repository_id: str, revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1) -> str:
    if isinstance(revision, CommitRevisionV1):
        return commit_node_id(repository_id, revision.commit_id)
    return str(revision.snapshot_id)


class RevisionReader:
    """Reads revision views and scope facts with the read-only graph facade."""

    def __init__(self, graph: Neo4jReadFacade) -> None:
        self._graph = graph

    async def repository_exists(self, repository_id: str) -> bool:
        async def read(tx: Neo4jTransaction) -> bool:
            result = await tx.run(_REPOSITORY, parameters={"repository_id": repository_id})
            return bool(result.records)

        return await self._graph.execute_read(read)

    async def project_exists(self, project_id: str) -> bool:
        async def read(tx: Neo4jTransaction) -> bool:
            result = await tx.run(_PROJECT, parameters={"project_id": project_id})
            return bool(result.records)

        return await self._graph.execute_read(read)

    async def projects_of(self, repository_id: str, limit: int) -> list[str]:
        """The projects that use the repository (portfolio links), sorted, at most `limit`."""

        async def read(tx: Neo4jTransaction) -> list[str]:
            result = await tx.run(
                _PROJECTS, parameters={"repository_id": repository_id, "limit": limit}
            )
            return [str(row["id"]) for row in result.records]

        return await self._graph.execute_read(read)

    async def view(
        self, repository_id: str, revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1
    ) -> RevisionView:
        label = "Commit" if isinstance(revision, CommitRevisionV1) else "WorkspaceSnapshot"
        target = target_id(repository_id, revision)

        async def read(tx: Neo4jTransaction) -> RevisionView:
            return await _view(tx, repository_id, label, target)

        return await self._graph.execute_read(read)

    async def run_files(self, repository_id: str, run: Run) -> tuple[FileFact, ...]:
        """The member files of another run (capped at `MAX_FILES`), e.g. the previous one."""

        async def read(tx: Neo4jTransaction) -> tuple[FileFact, ...]:
            return (await _members(tx, repository_id, run))[0]

        return await self._graph.execute_read(read)

    async def symbol_history(
        self, view: RevisionView, symbol_id: str
    ) -> tuple[SymbolRevisionFact, ...]:
        """The revisions `symbol_id` had in the runs up to and including the view's run."""

        async def read(tx: Neo4jTransaction) -> tuple[SymbolRevisionFact, ...]:
            facts = await code.load_facts(tx, view.repository_id, symbols=[symbol_id])
            return await _history(tx, view, symbol_id, facts)

        return await self._graph.execute_read(read)


async def _history(
    tx: Neo4jTransaction, view: RevisionView, symbol_id: str, facts: code.Facts
) -> tuple[SymbolRevisionFact, ...]:
    runs = {run.key: run for run in view.history_runs}
    intervals = [item for item in code.derive_symbol(facts, symbol_id) if item.start.key in runs]
    intervals = intervals[-MAX_HISTORY:]
    ids = sorted({item.key for item in intervals})
    rows = (
        await tx.run(_SYMBOL_CLAIMS, parameters={"ids": ids, "at": view.run.occurred_at})
    ).records
    names: dict[str, str] = {}
    events: dict[str, set[str]] = {}
    for row in rows:
        key = str(row["symbol_revision_id"])
        names.setdefault(key, str(row["qualified_name"]))
        events.setdefault(key, set()).update(_strings(row["events"]))
    found = []
    for item in intervals:
        opened = runs[item.start.key]
        closed = None if item.end is None else runs.get(item.end.key)
        found.append(
            SymbolRevisionFact(
                symbol_revision_id=item.key,
                qualified_name=names.get(item.key, symbol_id),
                first_seen=opened.occurred_at,
                until=None if closed is None else closed.occurred_at,
                run_label=opened.label,
                run_target_id=opened.target_id,
                source_event_ids=tuple(sorted(events.get(item.key, ()))),
            )
        )
    return tuple(found)


async def _view(tx: Neo4jTransaction, repository_id: str, label: str, target: str) -> RevisionView:
    runs, runs_cut = await read_runs(tx, repository_id)
    mine = [run for run in runs if run.label == label and run.target_id == target]
    if not mine:
        raise RevisionNotFound("the revision was never indexed for this repository")
    run = ([item for item in mine if item.complete] or mine)[-1]
    later = sorted(other.occurred_at for other in runs if other.occurred_at > run.occurred_at)
    # A commit is compared with the previous commit, never with a dirty snapshot in between.
    earlier = [other for other in runs if other.order < run.order and other.label == "Commit"]
    members, truncated = await _members(tx, repository_id, run)
    revisions = sorted({item.file_revision_id for item in members})
    base = {"repository_id": repository_id, "revisions": revisions}
    symbol_ids, symbols_cut = await _ids(tx, _CANDIDATE_SYMBOLS, base, MAX_SYMBOLS)
    assertion_ids, assertions_cut = await _ids(tx, _CANDIDATE_ASSERTIONS, base, MAX_ASSERTIONS)
    facts = await code.load_facts(
        tx,
        repository_id,
        files=[item.file_id for item in members],
        symbols=symbol_ids,
        assertions=assertion_ids,
    )
    at = run.occurred_at
    files = _present_files(facts, members, at)
    present: dict[str, str] = {}  # symbol revision -> the start of its interval
    for symbol_id in symbol_ids:
        found = code.interval_at(code.derive_symbol(facts, symbol_id), at)
        if found is not None:
            present[found.key] = found.start.occurred_at
    live = [
        assertion_id
        for assertion_id in assertion_ids
        if assertion_id in facts.assertions
        and code.interval_at(code.derive_assertion(facts, assertion_id), at) is not None
    ]
    return RevisionView(
        repository_id=repository_id,
        run=run,
        previous=earlier[-1] if earlier else None,
        window_end=later[0] if later else None,
        history_runs=tuple(other for other in runs if other.order <= run.order),
        files=files,
        symbols=await _symbols(tx, present, [item.file_revision_id for item in files]),
        assertions=await _assertions(tx, repository_id, live, revisions),
        truncated=truncated or runs_cut or symbols_cut or assertions_cut,
    )


def _present_files(facts: code.Facts, members: Sequence[FileFact], at: str) -> tuple[FileFact, ...]:
    """The run's member files the derivation agrees are present (the member revision holds)."""
    kept = []
    for item in members:
        found = code.interval_at(code.derive_file(facts, item.file_id), at)
        if found is not None and found.key == item.file_revision_id:
            kept.append(item)
    return tuple(kept)


async def _ids(
    tx: Neo4jTransaction, statement: LiteralString, base: Mapping[str, object], cap: int
) -> tuple[list[str], bool]:
    rows = (await tx.run(statement, parameters={**base, "limit": cap + 1})).records
    return [str(row["id"]) for row in rows[:cap]], len(rows) > cap


async def _symbols(
    tx: Neo4jTransaction, present: Mapping[str, str], revisions: Iterable[str]
) -> tuple[SymbolFact, ...]:
    rows = (
        await tx.run(
            _SYMBOL_REVISIONS, parameters={"ids": sorted(present), "revisions": sorted(revisions)}
        )
    ).records
    found: dict[str, SymbolFact] = {}
    for row in rows:
        props: Mapping[str, object] = row["props"]
        symbol_revision_id = str(props["symbol_revision_id"])
        if symbol_revision_id in found:
            continue  # one defining file revision per symbol revision in a run
        found[symbol_revision_id] = SymbolFact(
            symbol_id=str(props["symbol_id"]),
            symbol_revision_id=symbol_revision_id,
            qualified_name=str(props.get("qualified_name")),
            kind=_text(props.get("kind")),
            file_id=str(row["file_id"]),
            file_revision_id=str(row["revision_id"]),
            extractor_name=_text(props.get("extractor_name")),
            extractor_version=_text(props.get("extractor_version")),
            scip_symbol=_text(props.get("scip_symbol")),
            valid_from=present[symbol_revision_id],
            source_event_ids=_strings(row["events"]),
        )
    return tuple(sorted(found.values(), key=lambda s: (s.qualified_name, s.symbol_id)))


async def _assertions(
    tx: Neo4jTransaction, repository_id: str, ids: Sequence[str], revisions: Sequence[str]
) -> tuple[AssertionFact, ...]:
    rows = (
        await tx.run(
            _ASSERTION_PROPS,
            parameters={"ids": sorted(ids), "repository_id": repository_id, "revisions": revisions},
        )
    ).records
    return tuple(_assertion(row["props"], row["revision_id"], row["dependency_id"]) for row in rows)


def _assertion(
    props: Mapping[str, object], revision_id: object, dependency_id: object
) -> AssertionFact:
    confidence = props.get("confidence")
    return AssertionFact(
        assertion_id=str(props["assertion_id"]),
        family=str(props.get("family")),
        subject_id=str(props.get("subject_id")),
        predicate=_text(props.get("predicate")),
        object_id=_text(props.get("object_id")),
        evidence_kind=_text(props.get("evidence_kind")),
        confidence=float(confidence) if isinstance(confidence, int | float) else None,
        extractor_name=_text(props.get("extractor_name")),
        extractor_version=_text(props.get("extractor_version")),
        review_status=_text(props.get("review_status")),
        valid_from=_text(props.get("valid_from")),
        file_revision_id=_text(revision_id),
        dependency_id=_text(dependency_id),
        dependency_kind=_text(props.get("dependency_kind")),
        requirement=_text(props.get("requirement")),
        resolved_version=_text(props.get("resolved_version")),
        source_event_ids=_strings(props.get("source_event_ids")),
    )


async def _members(
    tx: Neo4jTransaction, repository_id: str, run: Run
) -> tuple[tuple[FileFact, ...], bool]:
    params = {
        "repository_id": repository_id,
        "target_id": run.target_id,
        "index_id": run.index_id,
        "limit": MAX_FILES + 1,
    }
    rows = (await tx.run(_FILES[run.label], parameters=params)).records
    files = tuple(
        FileFact(
            file_id=str(row["file_id"]),
            file_revision_id=str(row["revision_id"]),
            path=str(row["path"]),
            module_id=str(row["module_id"]),
            language=_text(row["language"]),
            extractor_name=_text(row["extractor_name"]),
            extractor_version=_text(row["extractor_version"]),
            source_event_ids=_strings(row["events"]),
        )
        for row in rows[:MAX_FILES]
    )
    return files, len(rows) > MAX_FILES


async def read_runs(tx: Neo4jTransaction, repository_id: str) -> tuple[list[Run], bool]:
    """The repository's newest `MAX_RUNS` runs, oldest first (a cut drops the oldest)."""
    rows = (
        await tx.run(_RUNS, parameters={"repository_id": repository_id, "limit": MAX_RUNS + 1})
    ).records
    runs = [
        Run(
            index_id=str(row["index_id"]),
            label=str(row["label"]),
            target_id=str(row["target_id"]),
            occurred_at=str(row["occurred_at"]),
            source_event_ids=_strings(row["events"]),
            complete=row["success"] is True,
        )
        for row in rows[:MAX_RUNS]
    ]
    runs.sort(key=lambda run: run.order)
    return runs, len(rows) > MAX_RUNS


def window_bound(value: str | None) -> datetime | None:
    """A stored fixed-width UTC timestamp (`YYYY-MM-DDTHH:MM:SS.ffffffZ`) as a datetime."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
