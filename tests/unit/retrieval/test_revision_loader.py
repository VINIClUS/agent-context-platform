"""Revision views read through the reader's static Cypher, without a graph database (PLATFORM-043).

`Graph` is an in-memory code graph: declared index runs, memberships, symbol claims and
assertions. It answers the reader's statements and the code projector's `load_facts` statements
(matched by identity, filtered by their parameters), so every view below is derived by the
projector's own helpers, exactly as against Neo4j.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from agent_context_sdk import CommitRevisionV1, WorkspaceSnapshotRevisionV1

from agent_context_platform.projection.projectors import code
from agent_context_platform.projection.projectors.git import commit_node_id
from agent_context_platform.retrieval import revision
from agent_context_platform.retrieval.revision import (
    AssertionFact,
    FileFact,
    RevisionNotFound,
    RevisionReader,
    RevisionView,
    Run,
    ScopeNotFound,
    SymbolFact,
    SymbolRevisionFact,
    read_runs,
    target_id,
)

from .fakes import FakeGraph, FakeTx

pytestmark = pytest.mark.unit

REPO = "repo-1"
C1, C2, C3 = "1" * 40, "2" * 40, "3" * 40
T0 = datetime(2026, 9, 1, 9, tzinfo=UTC)
Rows = list[dict[str, Any]]


def ts(hours: float) -> str:
    return code.timestamp(T0 + timedelta(hours=hours))


def node(oid: str) -> str:
    return commit_node_id(REPO, oid)


def commit(oid: str) -> CommitRevisionV1:
    return CommitRevisionV1(kind="commit", commit_id=oid)


def snapshot(snapshot_id: str) -> WorkspaceSnapshotRevisionV1:
    return WorkspaceSnapshotRevisionV1(kind="workspace_snapshot", snapshot_id=snapshot_id)


# --- the in-memory graph ---


@dataclass(frozen=True)
class IndexRun:
    """`(Repository)-[:INDEXED {index_id}]->(target)` and its `COMPLETED` outcome (`None`: none)."""

    index_id: str
    label: str
    target_id: str
    hours: float
    success: bool | None = True
    events: tuple[str | None, ...] | None = ()

    @property
    def at(self) -> str:
        return ts(self.hours)

    def expected(self) -> Run:
        return Run(
            index_id=self.index_id,
            label=self.label,
            target_id=self.target_id,
            occurred_at=self.at,
            source_event_ids=tuple(sorted({e for e in self.events or () if e is not None})),
            complete=self.success is True,
        )


@dataclass(frozen=True)
class Membership:
    """`(FileRevision)-[:MEMBER_OF {index_id}]->(target)`, its coverage reported complete."""

    run: IndexRun
    file_id: str
    revision_id: str
    path: str
    module_id: str = "mod:pkg"
    language: str | None = "python"
    events: tuple[str | None, ...] | None = ("ev-member",)


@dataclass(frozen=True)
class Definition:
    """`(SymbolRevision)-[:DEFINED_IN]->(FileRevision)`, claimed by `run`."""

    symbol_id: str
    symbol_revision_id: str
    qualified_name: str
    file_id: str
    revision_id: str
    run: IndexRun
    scip_symbol: str | None = None
    kind: str | None = "function"
    events: tuple[str, ...] = ()

    def props(self) -> dict[str, object]:
        props: dict[str, object] = {
            "symbol_id": self.symbol_id,
            "symbol_revision_id": self.symbol_revision_id,
            "qualified_name": self.qualified_name,
            "kind": self.kind,
            "extractor_name": "tree_sitter",
            "extractor_version": "0.23",
        }
        if self.scip_symbol is not None:  # an absent optional property is no key at all
            props["scip_symbol"] = self.scip_symbol
        return props


@dataclass(frozen=True)
class Observation:
    """`(Assertion)-[:OBSERVED_IN]->(FileRevision)`, claimed by `run`."""

    file_id: str
    revision_id: str
    run: IndexRun


@dataclass(frozen=True)
class Assertion:
    props: Mapping[str, object]
    observed: tuple[Observation, ...]
    dependency_id: str | None = None

    @property
    def assertion_id(self) -> str:
        return str(self.props["assertion_id"])


@dataclass
class Graph:
    repository_id: str = REPO
    projects: dict[str, set[str]] = field(default_factory=dict)
    runs: list[IndexRun] = field(default_factory=list)
    members: list[Membership] = field(default_factory=list)
    defs: list[Definition] = field(default_factory=list)
    assertions: list[Assertion] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.tx = FakeTx(self.answer)
        self.reader = RevisionReader(FakeGraph(self.tx))  # type: ignore[arg-type]
        self._answers: dict[str, Callable[[dict[str, Any]], Rows]] = {
            revision._REPOSITORY: self._repository,
            revision._PROJECT: self._project,
            revision._PROJECTS: self._projects,
            revision._RUNS: self._index_runs,
            revision._FILES["Commit"]: self._files("Commit"),
            revision._FILES["WorkspaceSnapshot"]: self._files("WorkspaceSnapshot"),
            revision._CANDIDATE_SYMBOLS: self._candidate_symbols,
            revision._CANDIDATE_ASSERTIONS: self._candidate_assertions,
            revision._SYMBOL_REVISIONS: self._symbol_revisions,
            revision._ASSERTION_PROPS: self._assertion_props,
            revision._SYMBOL_CLAIMS: self._symbol_claims,
            code._RUNS: self._code_runs,
            code._OUTCOMES: self._code_outcomes,
            code._MEMBERS: self._code_members,
            code._COVERAGE: self._code_coverage,
            code._CLAIMS: self._code_claims,
            code._DEFS: self._code_defs,
            code._ASSERTIONS: self._code_assertions,
        }

    # declaring the graph

    def run(self, index_id: str, label: str, target: str, hours: float, **kw: Any) -> IndexRun:
        item = IndexRun(index_id, label, target, hours, **kw)
        self.runs.append(item)
        return item

    def member(self, run: IndexRun, file_id: str, revision_id: str, path: str, **kw: Any) -> None:
        self.members.append(Membership(run, file_id, revision_id, path, **kw))

    def define(
        self,
        symbol_id: str,
        symbol_revision_id: str,
        name: str,
        file_id: str,
        revision_id: str,
        run: IndexRun,
        **kw: Any,
    ) -> None:
        self.defs.append(
            Definition(symbol_id, symbol_revision_id, name, file_id, revision_id, run, **kw)
        )

    def assert_(
        self, observed: tuple[Observation, ...], dependency_id: str | None = None, **props: Any
    ) -> None:
        self.assertions.append(Assertion(props, observed, dependency_id))

    # reading it

    def view(self, revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1) -> RevisionView:
        return asyncio.run(self.reader.view(REPO, revision))

    def statements(self, query: str) -> list[dict[str, Any]]:
        return [parameters for sent, parameters in self.tx.queries if sent == query]

    def answer(self, query: str, parameters: dict[str, Any]) -> Rows:
        handler = self._answers.get(query)
        if handler is None:
            raise AssertionError(f"unexpected statement: {query[:80]}")
        return handler(parameters)

    def _mine(self, parameters: dict[str, Any]) -> bool:
        return parameters.get("repository_id") == self.repository_id

    def _repository(self, p: dict[str, Any]) -> Rows:
        return [{"id": self.repository_id}] if self._mine(p) else []

    def _project(self, p: dict[str, Any]) -> Rows:
        return [{"id": p["project_id"]}] if p["project_id"] in self.projects else []

    def _projects(self, p: dict[str, Any]) -> Rows:
        found = sorted(k for k, repos in self.projects.items() if p["repository_id"] in repos)
        return [{"id": project_id} for project_id in found][: p["limit"]]

    def _index_runs(self, p: dict[str, Any]) -> Rows:
        if not self._mine(p):
            return []
        rows = [
            {
                "index_id": r.index_id,
                "label": r.label,
                "target_id": r.target_id,
                "occurred_at": r.at,
                "events": None if r.events is None else list(r.events),
                "success": r.success,
            }
            for r in self.runs
        ]
        rows.sort(key=lambda row: (row["occurred_at"], row["index_id"]), reverse=True)
        return rows[: p["limit"]]

    def _files(self, label: str) -> Callable[[dict[str, Any]], Rows]:
        def answer(p: dict[str, Any]) -> Rows:
            if not self._mine(p):
                return []
            rows = [
                {
                    "revision_id": m.revision_id,
                    "file_id": m.file_id,
                    "path": m.path,
                    "module_id": m.module_id,
                    "language": m.language,
                    "extractor_name": "tree_sitter",
                    "extractor_version": "0.23",
                    "events": None if m.events is None else list(m.events),
                }
                for m in self.members
                if (m.run.label, m.run.target_id, m.run.index_id)
                == (label, p["target_id"], p["index_id"])
            ]
            rows.sort(key=lambda row: (row["path"], row["revision_id"]))
            return rows[: p["limit"]]

        return answer

    def _candidate_symbols(self, p: dict[str, Any]) -> Rows:
        ids = sorted({d.symbol_id for d in self.defs if d.revision_id in p["revisions"]})
        return [{"id": i} for i in ids][: p["limit"]] if self._mine(p) else []

    def _candidate_assertions(self, p: dict[str, Any]) -> Rows:
        ids = sorted(
            {
                a.assertion_id
                for a in self.assertions
                if any(o.revision_id in p["revisions"] for o in a.observed)
            }
        )
        return [{"id": i} for i in ids][: p["limit"]] if self._mine(p) else []

    def _symbol_revisions(self, p: dict[str, Any]) -> Rows:
        found = [
            d
            for d in self.defs
            if d.symbol_revision_id in p["ids"] and d.revision_id in p["revisions"]
        ]
        found.sort(key=lambda d: (d.symbol_revision_id, d.revision_id))
        return [
            {
                "props": d.props(),
                "revision_id": d.revision_id,
                "file_id": d.file_id,
                "events": list(d.events),
            }
            for d in found
        ]

    def _assertion_props(self, p: dict[str, Any]) -> Rows:
        if not self._mine(p):
            return []
        rows = []
        for a in sorted(self.assertions, key=lambda a: a.assertion_id):
            if a.assertion_id in p["ids"]:
                seen = [o.revision_id for o in a.observed if o.revision_id in p["revisions"]]
                rows.append(
                    {
                        "props": dict(a.props),
                        "revision_id": min(seen, default=None),
                        "dependency_id": a.dependency_id,
                    }
                )
        return rows

    def _symbol_claims(self, p: dict[str, Any]) -> Rows:
        found = [d for d in self.defs if d.symbol_revision_id in p["ids"] and d.run.at <= p["at"]]
        found.sort(key=lambda d: d.symbol_revision_id)
        return [
            {
                "symbol_revision_id": d.symbol_revision_id,
                "qualified_name": d.qualified_name,
                "events": list(d.events),
            }
            for d in found
        ]

    # the code projector's fact loading

    def _code_runs(self, p: dict[str, Any]) -> Rows:
        return [
            {"index_id": r.index_id, "target_id": r.target_id, "occurred_at": r.at}
            | {"observed_at": r.at}
            for r in self.runs
            if self._mine(p)
        ]

    def _code_outcomes(self, p: dict[str, Any]) -> Rows:
        return [
            {"index_id": r.index_id, "target_id": r.target_id, "error_class": None}
            | {"event_order": r.at, "observed_at": r.at}
            for r in self.runs
            if self._mine(p) and r.success is not None
        ]

    def _membership_rows(self, p: dict[str, Any]) -> Rows:
        return [
            {
                "revision_id": m.revision_id,
                "file_id": m.file_id,
                "index_id": m.run.index_id,
                "target_id": m.run.target_id,
                "path": m.path,
                "module_id": m.module_id,
                "observed_at": m.run.at,
                "complete": True,
                "event_order": m.run.at,
            }
            for m in self.members
            if m.file_id in p["files"]
        ]

    def _code_members(self, p: dict[str, Any]) -> Rows:
        return self._membership_rows(p)

    def _code_coverage(self, p: dict[str, Any]) -> Rows:
        return self._membership_rows(p)

    def _code_claims(self, p: dict[str, Any]) -> Rows:
        claims = {
            (d.revision_id, d.run.index_id, d.run.target_id, d.run.at)
            for d in self.defs
            if d.file_id in p["files"]
        } | {
            (o.revision_id, o.run.index_id, o.run.target_id, o.run.at)
            for a in self.assertions
            for o in a.observed
            if o.file_id in p["files"]
        }
        keys = ("revision_id", "index_id", "target_id", "observed_at")
        return [dict(zip(keys, claim, strict=True)) for claim in sorted(claims)]

    def _code_defs(self, p: dict[str, Any]) -> Rows:
        return [
            {
                "symbol_id": d.symbol_id,
                "symbol_revision_id": d.symbol_revision_id,
                "revision_id": d.revision_id,
                "file_id": d.file_id,
                "observed_at": d.run.at,
                "since": d.run.at,
                "index_id": d.run.index_id,
                "target_id": d.run.target_id,
                "scip_symbol": d.scip_symbol,
            }
            for d in self.defs
            if d.symbol_id in p["symbols"]
        ]

    def _code_assertions(self, p: dict[str, Any]) -> Rows:
        rows: Rows = []
        for a in self.assertions:
            if a.assertion_id not in p["ids"]:
                continue
            props = a.props
            base = {
                "assertion_id": a.assertion_id,
                "family": props.get("family"),
                "subject_id": props.get("subject_id"),
                "predicate": props.get("predicate"),
                "object_id": props.get("object_id"),
                "valid_from": props.get("valid_from"),
                "valid_to": props.get("asserted_valid_to"),
                "recorded_from": props.get("recorded_from"),
            }
            observed = [
                {
                    "revision_id": o.revision_id,
                    "file_id": o.file_id,
                    "observed_at": o.run.at,
                    "since": o.run.at,
                    "index_id": o.run.index_id,
                    "target_id": o.run.target_id,
                }
                for o in a.observed
            ]
            empty = dict.fromkeys(("revision_id", "file_id", "observed_at", "since", "index_id"))
            # OPTIONAL MATCH: an assertion observed nowhere is still one row.
            rows += [base | item for item in observed or [empty | {"target_id": None}]]
        return rows


def world() -> tuple[Graph, IndexRun, IndexRun, IndexRun]:
    """C1, a dirty workspace snapshot S of its working tree, then C2, an hour apart.

    `add.py` never changes (one revision, claimed once by C1: later runs replay that claim);
    `report.py` changes in S and again in C2; `scratch.py` exists only in the dirty snapshot.
    """
    g = Graph()
    c1 = g.run("idx-1", "Commit", node(C1), 1, events=("ev-c1", "ev-c1-b", "ev-c1", None))
    s = g.run("idx-2", "WorkspaceSnapshot", "snap-1", 2, events=("ev-s",))
    c2 = g.run("idx-3", "Commit", node(C2), 3, events=None)
    for run in (c1, s, c2):
        g.member(run, "file:add", "fr:add:1", "src/pkg/add.py")
    g.member(c1, "file:report", "fr:report:1", "src/pkg/report.py", events=None)
    g.member(s, "file:report", "fr:report:s", "src/pkg/report.py", language=None)
    g.member(c2, "file:report", "fr:report:2", "src/pkg/report.py", events=("b", None, "a", "b"))
    g.member(s, "file:scratch", "fr:scratch:s", "src/pkg/scratch.py")
    g.define("sym:add", "sr:add:1", "pkg.add.add", "file:add", "fr:add:1", c1, events=("ev-a1",))
    for rev, run in (("1", c1), ("s", s), ("2", c2)):
        g.define(
            "sym:avg",
            f"sr:avg:{rev}",
            "pkg.report.average",
            "file:report",
            f"fr:report:{rev}",
            run,
            events=(f"ev-avg-{rev}",),
        )
    g.define("sym:tmp", "sr:tmp:s", "pkg.scratch.helper", "file:scratch", "fr:scratch:s", s)
    return g, c1, s, c2


def file_fact(revision_id: str, path: str, **changes: Any) -> FileFact:
    values: dict[str, Any] = {
        "file_id": "file:" + path.rsplit("/", 1)[-1].removesuffix(".py"),
        "file_revision_id": revision_id,
        "path": path,
        "module_id": "mod:pkg",
        "language": "python",
        "extractor_name": "tree_sitter",
        "extractor_version": "0.23",
        "source_event_ids": ("ev-member",),
    }
    return FileFact(**(values | changes))


# --- which run a revision is, and its window ---


def test_a_commit_view_is_its_own_run_compared_with_the_previous_commit_never_a_snapshot() -> None:
    g, c1, s, c2 = world()

    view = g.view(commit(C2))

    assert view.repository_id == REPO
    assert view.run == Run(
        index_id="idx-3",
        label="Commit",
        target_id=f"commit:{len(REPO)}:{REPO}:{C2}",
        occurred_at=ts(3),
        source_event_ids=(),
        complete=True,
    )
    assert view.previous == c1.expected()  # the dirty snapshot in between is skipped
    assert view.window_end is None  # the latest run: still open
    assert view.history_runs == (c1.expected(), s.expected(), c2.expected())
    assert view.truncated is False
    assert view.files == (
        file_fact("fr:add:1", "src/pkg/add.py"),
        file_fact("fr:report:2", "src/pkg/report.py", source_event_ids=("a", "b")),
    )
    # The unchanged symbol is a replayed claim of C1: present from C1's start.
    assert view.symbols == (
        SymbolFact(
            symbol_id="sym:add",
            symbol_revision_id="sr:add:1",
            qualified_name="pkg.add.add",
            kind="function",
            file_id="file:add",
            file_revision_id="fr:add:1",
            extractor_name="tree_sitter",
            extractor_version="0.23",
            scip_symbol=None,
            valid_from=ts(1),
            source_event_ids=("ev-a1",),
        ),
        SymbolFact(
            symbol_id="sym:avg",
            symbol_revision_id="sr:avg:2",
            qualified_name="pkg.report.average",
            kind="function",
            file_id="file:report",
            file_revision_id="fr:report:2",
            extractor_name="tree_sitter",
            extractor_version="0.23",
            scip_symbol=None,
            valid_from=ts(3),
            source_event_ids=("ev-avg-2",),
        ),
    )
    # Only the run's own members are read: the files query is the commit's, at this run.
    assert g.statements(revision._FILES["Commit"]) == [
        {
            "repository_id": REPO,
            "target_id": node(C2),
            "index_id": "idx-3",
            "limit": revision.MAX_FILES + 1,
        }
    ]
    assert g.statements(revision._SYMBOL_REVISIONS) == [
        {"ids": ["sr:add:1", "sr:avg:2"], "revisions": ["fr:add:1", "fr:report:2"]}
    ]


def test_a_snapshot_view_sees_uncommitted_code_and_closes_at_the_next_run() -> None:
    g, c1, s, _ = world()

    view = g.view(snapshot("snap-1"))

    assert view.run == s.expected()
    assert view.previous == c1.expected()
    assert view.window_end == ts(3)  # C2's start, exclusive
    assert view.history_runs == (c1.expected(), s.expected())
    assert [(f.path, f.file_revision_id, f.language) for f in view.files] == [
        ("src/pkg/add.py", "fr:add:1", "python"),
        ("src/pkg/report.py", "fr:report:s", None),
        ("src/pkg/scratch.py", "fr:scratch:s", "python"),
    ]
    assert [(x.qualified_name, x.symbol_revision_id, x.valid_from) for x in view.symbols] == [
        ("pkg.add.add", "sr:add:1", ts(1)),
        ("pkg.report.average", "sr:avg:s", ts(2)),
        ("pkg.scratch.helper", "sr:tmp:s", ts(2)),
    ]
    files = g.statements(revision._FILES["WorkspaceSnapshot"])
    assert [(p["target_id"], p["index_id"]) for p in files] == [("snap-1", "idx-2")]
    assert not g.statements(revision._FILES["Commit"])


def test_the_first_commit_has_no_previous_run_and_its_window_ends_at_the_snapshot() -> None:
    g, c1, _, _ = world()

    view = g.view(commit(C1))

    assert view.run == c1.expected()
    assert view.run.source_event_ids == ("ev-c1", "ev-c1-b")  # deduplicated, sorted, no nulls
    assert view.previous is None
    assert view.window_end == ts(2)
    assert view.history_runs == (c1.expected(),)
    assert [f.file_revision_id for f in view.files] == ["fr:add:1", "fr:report:1"]
    assert view.files[1].source_event_ids == ()  # no provenance recorded
    assert [s.symbol_revision_id for s in view.symbols] == ["sr:add:1", "sr:avg:1"]


def test_a_revision_is_its_latest_completed_run_else_its_latest_run() -> None:
    g = Graph()
    done = g.run("idx-a", "Commit", node(C1), 1)
    failed = g.run("idx-b", "Commit", node(C1), 2, success=False)
    g.run("idx-c", "Commit", node(C1), 4, success=None)  # still running: no outcome
    g.member(done, "file:add", "fr:add:1", "src/pkg/add.py")
    g.member(failed, "file:add", "fr:add:2", "src/pkg/add.py")

    view = g.view(commit(C1))

    assert view.run.index_id == "idx-a" and view.run.complete
    assert view.window_end == ts(2)  # a later run of the same commit still closes the window
    assert [f.file_revision_id for f in view.files] == ["fr:add:1"]

    unfinished = Graph()
    unfinished.run("idx-b", "Commit", node(C1), 2, success=False)
    unfinished.run("idx-c", "Commit", node(C1), 4, success=None)

    latest = unfinished.view(commit(C1))

    assert (latest.run.index_id, latest.run.complete) == ("idx-c", False)
    assert latest.files == () and latest.symbols == () and latest.assertions == ()


def test_a_revision_never_indexed_for_the_repository_is_not_found() -> None:
    g, _, _, _ = world()

    with pytest.raises(RevisionNotFound) as missing:
        g.view(commit(C3))
    assert isinstance(missing.value, ScopeNotFound) and isinstance(missing.value, LookupError)
    # A snapshot whose ID spells a commit's node ID is not that commit.
    with pytest.raises(RevisionNotFound):
        g.view(snapshot(node(C1)))
    # The same commit asked for in another repository: nothing of repo-1 is read.
    with pytest.raises(RevisionNotFound):
        asyncio.run(g.reader.view("repo-2", commit(C1)))
    assert g.statements(revision._RUNS)[-1] == {
        "repository_id": "repo-2",
        "limit": revision.MAX_RUNS + 1,
    }
    assert not g.statements(revision._FILES["Commit"])


def test_every_statement_is_matched_on_the_repository_and_bounded() -> None:
    g, _, _, _ = world()

    view = g.view(commit(C2))
    asyncio.run(g.reader.symbol_history(view, "sym:avg"))

    scoped = [p for query, p in g.tx.queries if "$repository_id" in query]
    assert scoped and all(p["repository_id"] == REPO for p in scoped)
    for query in (
        revision._RUNS,
        *revision._FILES.values(),
        revision._CANDIDATE_SYMBOLS,
        revision._CANDIDATE_ASSERTIONS,
    ):
        assert "LIMIT $limit" in query and "$repository_id" in query
    assert [p["limit"] for p in g.statements(revision._CANDIDATE_SYMBOLS)] == [
        revision.MAX_SYMBOLS + 1
    ]
    assert [p["limit"] for p in g.statements(revision._CANDIDATE_ASSERTIONS)] == [
        revision.MAX_ASSERTIONS + 1
    ]


# --- what a run contains ---


def test_a_member_revision_the_projector_does_not_hold_is_dropped() -> None:
    g, c1, _, _ = world()
    # A second membership of add.py in C1: the projector holds the smaller revision ID only.
    g.member(c1, "file:add", "fr:add:0", "src/pkg/add.py")

    view = g.view(commit(C1))

    assert [f.file_revision_id for f in view.files] == ["fr:add:0", "fr:report:1"]
    assert "fr:add:1" not in {s.file_revision_id for s in view.symbols}


def test_a_claim_made_by_a_later_run_is_not_part_of_an_earlier_one() -> None:
    g, c1, _, c2 = world()
    # C2's index claims a new symbol on the unchanged add.py revision C1 already held.
    g.define("sym:new", "sr:new:1", "pkg.add.new", "file:add", "fr:add:1", c2)

    first = g.view(commit(C1))
    latest = g.view(commit(C2))

    assert "sym:new" not in {s.symbol_id for s in first.symbols}
    # A candidate at C1 (claimed on a member revision), but its claim is not part of C1.
    assert g.statements(revision._SYMBOL_REVISIONS)[0]["ids"] == ["sr:add:1", "sr:avg:1"]
    new = next(s for s in latest.symbols if s.symbol_id == "sym:new")
    assert (new.valid_from, new.file_revision_id) == (c2.at, "fr:add:1")
    assert c1.at < new.valid_from


def test_a_symbol_revision_is_reported_once_and_symbols_sort_by_name_then_id() -> None:
    g, c1, _, _ = world()
    # One symbol revision defined in two member files (a path-independent SCIP identity).
    for file_id, revision_id in (("file:report", "fr:report:1"), ("file:add", "fr:add:1")):
        g.define(
            "sym:shared",
            "sr:shared:1",
            "pkg.shared",
            file_id,
            revision_id,
            c1,
            scip_symbol="scip-go gomod example v1 `example`/Shared().",
            kind=None,
            events=(f"ev-{file_id}",),
        )
    # Another symbol with the same qualified name as `pkg.add.add`, but a smaller ID.
    g.define("sym:aaa", "sr:aaa:1", "pkg.add.add", "file:report", "fr:report:1", c1)

    view = g.view(commit(C1))

    assert [(s.qualified_name, s.symbol_id) for s in view.symbols] == [
        ("pkg.add.add", "sym:aaa"),
        ("pkg.add.add", "sym:add"),
        ("pkg.report.average", "sym:avg"),
        ("pkg.shared", "sym:shared"),
    ]
    shared = view.symbols[-1]
    # The first defining revision by ID is the one reported, with its own claim's provenance.
    assert (shared.file_id, shared.file_revision_id) == ("file:add", "fr:add:1")
    assert shared.source_event_ids == ("ev-file:add",)
    assert shared.kind is None
    assert shared.scip_symbol == "scip-go gomod example v1 `example`/Shared()."


def _with_assertions() -> tuple[Graph, IndexRun, IndexRun, IndexRun]:
    g, c1, s, c2 = world()
    on_report_2 = (Observation("file:report", "fr:report:2", c2),)
    on_add = (Observation("file:add", "fr:add:1", c1),)
    relation = {
        "family": "relation",
        "repository_id": REPO,
        "subject_id": "sym:avg",
        "predicate": "CALLS",
        "object_id": "sym:add",
        "valid_from": c2.at,
        "recorded_from": c2.at,
        "review_status": "unreviewed",
    }
    g.assert_(
        on_report_2,
        **relation,
        assertion_id="as:calls:ts",
        evidence_kind="tree_sitter",
        confidence=0.6,
        extractor_name="tree_sitter",
        extractor_version="0.23",
        source_event_ids=["ev-ts"],
    )
    g.assert_(
        on_report_2,
        **relation,
        assertion_id="as:calls:scip",
        evidence_kind="scip",
        confidence=1,
        extractor_name="scip-python",
        extractor_version="0.6",
        source_event_ids=["ev-scip-2", "ev-scip-1", "ev-scip-2"],
    )
    dependency = {
        "family": "dependency",
        "repository_id": REPO,
        "subject_id": "mod:pkg",
        "predicate": "DEPENDS_ON",
        "evidence_kind": "manifest",
        "valid_from": c1.at,
        "recorded_from": c1.at,
        "dependency_kind": "runtime",
    }
    g.assert_(
        on_add,
        "dep:pypi:httpx",
        **dependency,
        assertion_id="as:dep:httpx",
        object_id="dep:pypi:httpx",
        confidence="high",  # not a number: no confidence
        requirement=">=0.27",
        resolved_version="0.27.2",
        source_event_ids=["ev-dep"],
    )
    g.assert_(
        on_add,
        "dep:pypi:six",
        **dependency,
        assertion_id="as:dep:six",
        object_id="dep:pypi:six",
        asserted_valid_to=ts(2.5),  # declared to end between S and C2
    )
    g.assert_(on_add, assertion_id="as:stub", family="relation")  # an observation stub only
    return g, c1, s, c2


def test_assertions_map_their_stored_properties_and_only_live_ones_are_read() -> None:
    g, c1, _, c2 = _with_assertions()

    view = g.view(commit(C2))

    assert view.assertions == (
        AssertionFact(
            assertion_id="as:calls:scip",
            family="relation",
            subject_id="sym:avg",
            predicate="CALLS",
            object_id="sym:add",
            evidence_kind="scip",
            confidence=1.0,
            extractor_name="scip-python",
            extractor_version="0.6",
            review_status="unreviewed",
            valid_from=c2.at,
            file_revision_id="fr:report:2",
            dependency_id=None,
            dependency_kind=None,
            requirement=None,
            resolved_version=None,
            source_event_ids=("ev-scip-1", "ev-scip-2"),
        ),
        AssertionFact(
            assertion_id="as:calls:ts",
            family="relation",
            subject_id="sym:avg",
            predicate="CALLS",
            object_id="sym:add",
            evidence_kind="tree_sitter",
            confidence=0.6,
            extractor_name="tree_sitter",
            extractor_version="0.23",
            review_status="unreviewed",
            valid_from=c2.at,
            file_revision_id="fr:report:2",
            dependency_id=None,
            dependency_kind=None,
            requirement=None,
            resolved_version=None,
            source_event_ids=("ev-ts",),
        ),
        AssertionFact(
            assertion_id="as:dep:httpx",
            family="dependency",
            subject_id="mod:pkg",
            predicate="DEPENDS_ON",
            object_id="dep:pypi:httpx",
            evidence_kind="manifest",
            confidence=None,
            extractor_name=None,
            extractor_version=None,
            review_status=None,
            valid_from=c1.at,
            file_revision_id="fr:add:1",
            dependency_id="dep:pypi:httpx",
            dependency_kind="runtime",
            requirement=">=0.27",
            resolved_version="0.27.2",
            source_event_ids=("ev-dep",),
        ),
    )
    # Every candidate observed in the run is weighed; the expired one and the stub are not read.
    assert g.statements(revision._ASSERTION_PROPS) == [
        {
            "ids": ["as:calls:scip", "as:calls:ts", "as:dep:httpx"],
            "repository_id": REPO,
            "revisions": ["fr:add:1", "fr:report:2"],
        }
    ]
    assert [a.assertion_id for a in view.relations()] == ["as:calls:scip"]  # SCIP outranks
    assert [a.assertion_id for a in view.dependencies()] == ["as:dep:httpx"]


def test_an_assertion_is_held_only_within_its_declared_validity() -> None:
    g, _, _, _ = _with_assertions()

    first = g.view(commit(C1))
    dirty = g.view(snapshot("snap-1"))

    assert [a.assertion_id for a in first.dependencies()] == ["as:dep:httpx", "as:dep:six"]
    six = first.dependencies()[1]
    assert (six.requirement, six.resolved_version, six.confidence) == (None, None, None)
    assert six.source_event_ids == ()
    assert [a.assertion_id for a in dirty.dependencies()] == ["as:dep:httpx", "as:dep:six"]
    # The relation is observed on C2's report.py only: not in C1 nor in the snapshot.
    assert first.relations() == () and dirty.relations() == ()


@pytest.mark.parametrize(
    ("cap", "files", "symbols", "assertions"),
    [
        ("MAX_FILES", ["fr:add:1"], ["sr:add:1"], ["as:dep:httpx", "as:dep:six"]),
        ("MAX_SYMBOLS", ["fr:add:1", "fr:report:1"], ["sr:add:1"], ["as:dep:httpx", "as:dep:six"]),
        ("MAX_ASSERTIONS", ["fr:add:1", "fr:report:1"], ["sr:add:1", "sr:avg:1"], ["as:dep:httpx"]),
    ],
)
def test_a_cap_keeps_the_first_rows_and_marks_the_view_truncated(
    monkeypatch: pytest.MonkeyPatch,
    cap: str,
    files: list[str],
    symbols: list[str],
    assertions: list[str],
) -> None:
    g, _, _, _ = _with_assertions()
    monkeypatch.setattr(revision, cap, 1)

    view = g.view(commit(C1))

    assert view.truncated
    assert [f.file_revision_id for f in view.files] == files
    assert [s.symbol_revision_id for s in view.symbols] == symbols
    assert [a.assertion_id for a in view.assertions] == assertions
    limits = [p["limit"] for query, p in g.tx.queries if "LIMIT $limit" in query]
    assert 2 in limits  # the cap is read one past itself to detect the cut


def test_the_newest_runs_are_read_oldest_first_and_a_cut_drops_the_oldest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    g, _, s, c2 = world()
    tie = g.run("idx-0", "WorkspaceSnapshot", "snap-0", 3)  # same start as C2: ordered by key

    async def read(tx: Any) -> tuple[list[Run], bool]:
        return await read_runs(tx, REPO)

    runs, cut = asyncio.run(read(g.tx))
    assert [r.index_id for r in runs] == ["idx-1", "idx-2", "idx-0", "idx-3"] and not cut
    assert tie.expected().order < c2.expected().order

    monkeypatch.setattr(revision, "MAX_RUNS", 2)
    runs, cut = asyncio.run(read(g.tx))
    assert runs == [tie.expected(), c2.expected()] and cut
    assert g.statements(revision._RUNS)[-1] == {"repository_id": REPO, "limit": 3}

    monkeypatch.setattr(revision, "MAX_RUNS", 3)
    view = g.view(commit(C2))
    assert view.truncated
    assert view.previous is None  # C1 is beyond the cut
    assert view.history_runs == (s.expected(), tie.expected(), c2.expected())
    with pytest.raises(RevisionNotFound):
        g.view(commit(C1))


def test_the_files_of_another_run_are_its_own_members(monkeypatch: pytest.MonkeyPatch) -> None:
    g, c1, _, _ = world()
    view = g.view(commit(C2))
    assert view.previous is not None

    before = asyncio.run(g.reader.run_files(REPO, view.previous))

    assert before == (
        file_fact("fr:add:1", "src/pkg/add.py"),
        file_fact("fr:report:1", "src/pkg/report.py", source_event_ids=()),
    )
    assert g.statements(revision._FILES["Commit"])[-1]["index_id"] == c1.index_id
    monkeypatch.setattr(revision, "MAX_FILES", 1)
    assert [f.path for f in asyncio.run(g.reader.run_files(REPO, c1.expected()))] == [
        "src/pkg/add.py"
    ]


# --- a symbol's history up to the revision ---


def test_symbol_history_lists_each_revision_with_the_run_that_opened_it() -> None:
    g, _, _, _ = world()
    view = g.view(snapshot("snap-1"))

    history = asyncio.run(g.reader.symbol_history(view, "sym:avg"))

    assert history == (
        SymbolRevisionFact(
            symbol_revision_id="sr:avg:1",
            qualified_name="pkg.report.average",
            first_seen=ts(1),
            until=ts(2),
            run_label="Commit",
            run_target_id=node(C1),
            source_event_ids=("ev-avg-1",),
        ),
        # C2 replaced it later, but C2 is after the view: it is still held at the snapshot.
        SymbolRevisionFact(
            symbol_revision_id="sr:avg:s",
            qualified_name="pkg.report.average",
            first_seen=ts(2),
            until=None,
            run_label="WorkspaceSnapshot",
            run_target_id="snap-1",
            source_event_ids=("ev-avg-s",),
        ),
    )
    assert g.statements(revision._SYMBOL_CLAIMS) == [{"ids": ["sr:avg:1", "sr:avg:s"], "at": ts(2)}]


def test_a_commit_history_is_its_commit_lineage_never_a_dirty_snapshot() -> None:
    g, c1, _, c2 = world()
    view = g.view(commit(C2))

    history = asyncio.run(g.reader.symbol_history(view, "sym:avg"))

    # The snapshot's revision sr:avg:s was never committed: C1's revision is held until C2.
    assert [(f.symbol_revision_id, f.first_seen, f.until, f.run_label) for f in history] == [
        ("sr:avg:1", c1.at, c2.at, "Commit"),
        ("sr:avg:2", c2.at, None, "Commit"),
    ]
    assert g.statements(revision._SYMBOL_CLAIMS) == [{"ids": ["sr:avg:1", "sr:avg:2"], "at": c2.at}]


def test_a_commit_that_adopts_a_snapshot_revision_lists_it_from_that_commit() -> None:
    g, c1, s, c2 = world()
    # C2 commits exactly what the snapshot held: report.py and average() keep S's revisions.
    g.members = [m for m in g.members if not (m.run is c2 and m.file_id == "file:report")]
    g.defs = [d for d in g.defs if d.symbol_revision_id != "sr:avg:2"]
    g.member(c2, "file:report", "fr:report:s", "src/pkg/report.py")

    history = asyncio.run(g.reader.symbol_history(g.view(commit(C2)), "sym:avg"))

    assert [(f.symbol_revision_id, f.first_seen, f.until, f.run_label) for f in history] == [
        ("sr:avg:1", c1.at, c2.at, "Commit"),
        ("sr:avg:s", c2.at, None, "Commit"),
    ]
    assert history[1].source_event_ids == ("ev-avg-s",)  # the replayed claim is cited
    assert s.at < c2.at


def test_an_unchanged_symbol_has_one_revision_with_the_claims_known_at_the_view() -> None:
    g, c1, _, c2 = world()
    # C2 changes add.py elsewhere: the same symbol revision is claimed again on the new file.
    g.members = [m for m in g.members if not (m.run is c2 and m.file_id == "file:add")]
    g.member(c2, "file:add", "fr:add:2", "src/pkg/add.py")
    g.define("sym:add", "sr:add:1", "pkg.add.add", "file:add", "fr:add:2", c2, events=("ev-a2",))

    latest = g.view(commit(C2))
    first = g.view(commit(C1))
    now = asyncio.run(g.reader.symbol_history(latest, "sym:add"))
    then = asyncio.run(g.reader.symbol_history(first, "sym:add"))

    assert [(f.symbol_revision_id, f.first_seen, f.until) for f in now] == [
        ("sr:add:1", c1.at, None)
    ]
    assert now[0].run_target_id == node(C1)
    assert now[0].source_event_ids == ("ev-a1", "ev-a2")
    assert [f.source_event_ids for f in then] == [("ev-a1",)]  # C2's claim is later knowledge
    assert [p["at"] for p in g.statements(revision._SYMBOL_CLAIMS)] == [c2.at, c1.at]


def test_symbol_history_keeps_the_latest_revisions(monkeypatch: pytest.MonkeyPatch) -> None:
    g, _, _, _ = world()
    view = g.view(snapshot("snap-1"))
    monkeypatch.setattr(revision, "MAX_HISTORY", 1)

    history = asyncio.run(g.reader.symbol_history(view, "sym:avg"))

    assert [f.symbol_revision_id for f in history] == ["sr:avg:s"]
    assert g.statements(revision._SYMBOL_CLAIMS)[-1]["ids"] == ["sr:avg:s"]


# --- scope lookups ---


def test_scope_lookups_answer_for_the_requested_ids_only() -> None:
    g = Graph(projects={"prj-b": {REPO}, "prj-a": {REPO}, "prj-c": {"repo-2"}})

    assert asyncio.run(g.reader.repository_exists(REPO)) is True
    assert asyncio.run(g.reader.repository_exists("repo-2")) is False
    assert asyncio.run(g.reader.project_exists("prj-a")) is True
    assert asyncio.run(g.reader.project_exists("prj-z")) is False
    assert asyncio.run(g.reader.projects_of(REPO, 10)) == ["prj-a", "prj-b"]
    assert asyncio.run(g.reader.projects_of(REPO, 1)) == ["prj-a"]
    assert g.statements(revision._REPOSITORY) == [
        {"repository_id": REPO},
        {"repository_id": "repo-2"},
    ]
    assert g.statements(revision._PROJECT) == [{"project_id": "prj-a"}, {"project_id": "prj-z"}]
    assert g.statements(revision._PROJECTS) == [
        {"repository_id": REPO, "limit": 10},
        {"repository_id": REPO, "limit": 1},
    ]


def test_a_commit_target_is_its_repository_bound_node_id_and_a_snapshot_its_own_id() -> None:
    assert target_id(REPO, commit(C1)) == f"commit:6:repo-1:{C1}"
    assert target_id("repo-22", commit(C1)) != target_id(REPO, commit(C1))
    assert target_id(REPO, snapshot("snap-1")) == "snap-1"


# --- relations and dependencies of a view ---


def relation(assertion_id: str, subject: str, obj: str | None, **changes: Any) -> AssertionFact:
    values: dict[str, Any] = {
        "assertion_id": assertion_id,
        "family": "relation",
        "subject_id": subject,
        "predicate": "CALLS",
        "object_id": obj,
        "evidence_kind": "tree_sitter",
        "confidence": 0.5,
        "extractor_name": None,
        "extractor_version": None,
        "review_status": None,
        "valid_from": None,
        "file_revision_id": None,
        "dependency_id": None,
        "dependency_kind": None,
        "requirement": None,
        "resolved_version": None,
        "source_event_ids": (),
    }
    return AssertionFact(**(values | changes))


def symbol_fact(symbol_id: str) -> SymbolFact:
    return SymbolFact(
        symbol_id, f"sr:{symbol_id}", symbol_id, None, "f", "fr", None, None, None, "", ()
    )


def view_of(*assertions: AssertionFact) -> RevisionView:
    run = Run("idx", "Commit", node(C1), ts(1), (), True)
    member = FileFact("f", "fr", "f.py", "mod", None, None, None, ())
    symbols = tuple(symbol_fact(s) for s in ("a", "b", "c"))
    return RevisionView(REPO, run, None, None, (run,), (member,), symbols, assertions, False)


def test_one_relation_wins_per_triple_by_evidence_then_confidence_then_id() -> None:
    facts = [
        relation("r-heur", "a", "b", evidence_kind="heuristic", confidence=1.0),
        relation("r-ts-9", "a", "b", confidence=0.9),
        relation("r-scip-low", "a", "b", evidence_kind="scip", confidence=0.1),
        relation("r-ts-low", "a", "c", confidence=0.2),
        relation("r-ts-high", "a", "c", confidence=0.8),
        relation("r-z", "b", "c", confidence=None),
        relation("r-y", "b", "c", confidence=None),
        relation("r-file", "f", "a", evidence_kind="git"),  # a file is a member too
        relation("r-out", "a", "outside"),
        relation("r-in", "outside", "a"),
        relation("r-open", "a", None),
        relation("r-nopred", "a", "b", predicate=None, evidence_kind="scip", confidence=1.0),
        relation("r-dep", "a", "b", family="dependency", evidence_kind="scip", confidence=1.0),
    ]

    picked = [r.assertion_id for r in view_of(*facts).relations()]

    assert picked == ["r-scip-low", "r-ts-high", "r-y", "r-file"]  # sorted by triple
    assert [r.assertion_id for r in view_of(*reversed(facts)).relations()] == picked
    assert relation("x", "a", "b", evidence_kind="git", confidence=0.4).rank == (3, 0.4)
    assert relation("x", "a", "b", evidence_kind=None, confidence=None).rank == (0, 0.0)


def test_dependencies_are_the_bound_dependency_assertions_once_each_by_id() -> None:
    dep = {"family": "dependency", "predicate": "DEPENDS_ON"}
    facts = [
        relation("d-2", "mod", "dep:b", **dep, dependency_id="dep:b"),
        relation("d-1", "mod", "dep:a", **dep, dependency_id="dep:a"),
        relation("d-1", "mod", "dep:a", **dep, dependency_id="dep:a"),
        relation("d-0", "mod", "dep:c", **dep),  # its Dependency node is not in the repository
        relation("r-1", "a", "b", dependency_id="dep:x"),  # a relation is never a dependency
    ]

    assert [d.assertion_id for d in view_of(*facts).dependencies()] == ["d-1", "d-2"]
    assert view_of().dependencies() == () and view_of().relations() == ()
