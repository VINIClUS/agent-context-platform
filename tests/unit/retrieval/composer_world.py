"""A composer world without services (PLATFORM-043): in-memory doubles of its collaborators.

`seeded()` builds one repository with two commits and a dirty workspace snapshot, its decisions,
failures and searchable content, and the ledger events that back them. Every double answers from
data a test can change and records the arguments it received, so a test asserts both the package
and what the composer asked for. The revision reader is replaced as a whole: its Cypher is
`revision.py`'s to test.

Timeline (hours after `T0`): C1 indexed at 0, C2 at 1, the snapshot (an incomplete index of C2's
tree plus `pkg/draft.py`) at 2, so C2's window is [1, 2) and the snapshot's is still open. The
projection snapshot's own time is 3.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import Coroutine, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from agent_context_sdk import (
    CommitRevisionV1,
    ContentDisposition,
    ContentRefV1,
    ContentStorage,
    ContextPackageV1,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    WorkspaceSnapshotRevisionV1,
    seal_event,
)
from agent_context_sdk.contracts.mcp import (
    ContextForTaskInputV1,
    DecisionHistoryInputV1,
    FailureHistoryInputV1,
    RepositoryOverviewInputV1,
    SymbolContextInputV1,
)

from agent_context_platform.projection.projectors.git import commit_node_id
from agent_context_platform.retrieval.composer import ContextComposer, RequestContext
from agent_context_platform.retrieval.cursor import CursorCodec
from agent_context_platform.retrieval.evidence import EvidenceScope, LedgerEvent, LoadedEvents
from agent_context_platform.retrieval.freshness import Freshness
from agent_context_platform.retrieval.graph import (
    EvidenceRef,
    GraphAnchorNotFound,
    GraphNeighborhood,
    GraphNode,
    GraphScope,
    NodeKind,
)
from agent_context_platform.retrieval.revision import (
    AssertionFact,
    FileFact,
    RevisionNotFound,
    RevisionView,
    Run,
    SymbolFact,
    SymbolRevisionFact,
)
from agent_context_platform.retrieval.search import SearchHit, SearchRequestError, SearchScope
from agent_context_platform.retrieval.temporal import (
    DecisionRecord,
    DecisionsResult,
    DecisionStanding,
    FailureRecord,
    FailuresResult,
    FixPath,
    FixResolution,
    FixStatus,
    HistoricalReason,
    ObservedRun,
    RunKind,
    TemporalProjectionOutdated,
    TemporalScope,
)

REPO = "repo-1"
PROJECT = "prj-1"
C1 = "a" * 40
C2 = "b" * 40
SNAP = "snap-1"
T0 = datetime(2026, 9, 1, 9, 0, tzinfo=UTC)
PRINCIPAL = RequestContext("unit-principal")
WIDE = {"max_items": 60, "max_bytes": 400_000, "max_tokens": 100_000}
AS_OF = "0198a4b1-98c0-7c28-ae3f-0000000fffff"
AS_OF_OUTBOX = 1000


def at(hours: float) -> datetime:
    return T0 + timedelta(hours=hours)


def stamp(moment: datetime) -> str:
    """The stored fixed-width UTC form the revision facts carry."""
    return f"{moment.astimezone(UTC):%Y-%m-%dT%H:%M:%S.%fZ}"


NOW = at(3) + timedelta(minutes=1)


def answer[T](call: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(call)


def _base_event() -> StoredEventV1:
    draft = EventDraftV1(
        event_id=uuid.UUID("0198a4b1-98c0-7c28-ae3f-000000000000"),
        event_type="code.index.started",
        stream_id="unit-composer",
        occurred_at=T0,
        observed_at=T0,
        producer=ProducerV1(producer_id="unit", name="unit", version="1"),
        context=EventContextV1(project_id=PROJECT, repository_id=REPO),
        payload={},
        redaction=EventRedactionSummaryV1(
            policy_version="1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key="unit-composer-0",
    )
    return seal_event(draft, [], 1, None)


_BASE: StoredEventV1 = _base_event()


# --------------------------------------------------------------------------------------------
# Doubles
# --------------------------------------------------------------------------------------------


class FakeLedger:
    """`EvidenceLedger`: stored events with an outbox position, filtered by the request scope."""

    def __init__(self) -> None:
        self.stored: dict[str, LedgerEvent] = {}
        self.outbox: dict[str, int] = {}
        self.keys: dict[str, str] = {}
        self.texts: dict[str, str] = {}
        self.ranks: dict[str, dict[str, float]] = {}  # query word -> event ID -> rank
        self.cap: int | None = None
        self.loads: list[tuple[frozenset[str], EvidenceScope]] = []
        self.snippet_requests: list[tuple[str, str | None]] = []
        self.rank_calls: list[tuple[tuple[str, ...], str, int]] = []

    def add(
        self,
        key: str,
        event_type: str,
        occurred: datetime,
        payload: dict[str, Any] | None = None,
        *,
        observed: datetime | None = None,
        repository: str | None = REPO,
        project: str | None = PROJECT,
        content_id: str | None = None,
        text: str | None = None,
        outbox: int | None = None,
    ) -> str:
        event_id = uuid.UUID(f"0198a4b1-98c0-7c28-ae3f-{len(self.keys) + 1:012x}")
        refs = ()
        if content_id is not None:
            refs = (
                ContentRefV1(
                    content_id=content_id,
                    content_sha256="0" * 64,
                    media_type="text/plain",
                    uncompressed_bytes=len(text or ""),
                    disposition=ContentDisposition.SANITIZED,
                    storage=ContentStorage.INLINE,
                    inline_id=f"inline-{content_id}",
                ),
            )
        event = _BASE.model_copy(
            update={
                "event_id": event_id,
                "event_type": event_type,
                "occurred_at": occurred,
                "observed_at": observed or occurred,
                "context": EventContextV1(project_id=project, repository_id=repository),
                "payload": payload or {},
                "content_refs": refs,
            }
        )
        key_id = str(event_id)
        self.keys[key] = key_id
        self.stored[key_id] = LedgerEvent(event, 100, frozenset())
        self.outbox[key_id] = len(self.keys) if outbox is None else outbox
        if text is not None:
            self.texts[key_id] = text
        return key_id

    def id(self, key: str) -> str:
        return self.keys[key]

    def key_of(self, event_id: str) -> str:
        return next(key for key, value in self.keys.items() if value == event_id)

    async def events(self, event_ids: Iterable[str], scope: EvidenceScope) -> LoadedEvents:
        wanted = frozenset(event_ids)
        self.loads.append((wanted, scope))
        found = LoadedEvents()
        for event_id in sorted(wanted):
            item = self.stored.get(event_id)
            if item is None:
                continue
            if scope.max_outbox_id is not None and self.outbox[event_id] > scope.max_outbox_id:
                continue  # delivered after the package's snapshot
            if scope.admits(item.event):
                found[event_id] = item
        found.capped = self.cap is not None and len(wanted) > self.cap
        return found

    async def snippets(
        self, requests: Sequence[tuple[LedgerEvent, str | None]]
    ) -> dict[tuple[str, str | None], str | None]:
        found: dict[tuple[str, str | None], str | None] = {}
        for item, content_id in requests:
            self.snippet_requests.append((item.event_id, content_id))
            found[(item.event_id, content_id)] = self.texts.get(item.event_id)
        return found

    async def rank_documents(
        self, event_ids: Sequence[str], query: str, *, limit: int
    ) -> dict[str, float]:
        self.rank_calls.append((tuple(event_ids), query, limit))
        found: dict[str, float] = {}
        for word in query.split(" or "):
            for event_id, score in self.ranks.get(word.strip(), {}).items():
                if event_id in event_ids:
                    found[event_id] = max(found.get(event_id, 0.0), score)
        return found


class FakeFreshness:
    def __init__(self, fresh: Freshness) -> None:
        self.fresh = fresh
        self.calls: list[dict[str, Any]] = []

    async def read(
        self,
        projectors: Sequence[tuple[str, str]],
        now: datetime,
        *,
        project_id: str | None = None,
        repository_id: str | None = None,
    ) -> Freshness:
        self.calls.append(
            {
                "projectors": tuple(projectors),
                "now": now,
                "project_id": project_id,
                "repository_id": repository_id,
            }
        )
        return self.fresh


class FakeRevisions:
    """`RevisionReader`: views by commit or snapshot ID, and the other runs' files."""

    def __init__(self) -> None:
        self.repositories = {REPO}
        self.projects = {PROJECT}
        self.links: dict[str, list[str]] = {REPO: [PROJECT]}
        self.views: dict[str, RevisionView] = {}
        self.run_members: dict[str, tuple[FileFact, ...]] = {}
        self.histories: dict[str, tuple[SymbolRevisionFact, ...]] = {}
        self.calls: list[tuple[str, Any]] = []

    async def repository_exists(self, repository_id: str) -> bool:
        self.calls.append(("repository_exists", repository_id))
        return repository_id in self.repositories

    async def project_exists(self, project_id: str) -> bool:
        self.calls.append(("project_exists", project_id))
        return project_id in self.projects

    async def projects_of(self, repository_id: str, limit: int) -> list[str]:
        self.calls.append(("projects_of", (repository_id, limit)))
        return sorted(self.links.get(repository_id, []))[:limit]

    async def view(
        self, repository_id: str, revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1
    ) -> RevisionView:
        key = revision.commit_id if isinstance(revision, CommitRevisionV1) else revision.snapshot_id
        self.calls.append(("view", (repository_id, key)))
        if key not in self.views:
            raise RevisionNotFound("the revision was never indexed for the repository")
        return self.views[key]

    async def run_files(self, repository_id: str, run: Run) -> tuple[FileFact, ...]:
        self.calls.append(("run_files", (repository_id, run.index_id)))
        return self.run_members[run.key]

    async def symbol_history(
        self, view: RevisionView, symbol_id: str
    ) -> tuple[SymbolRevisionFact, ...]:
        self.calls.append(("symbol_history", (view.run.index_id, symbol_id)))
        return self.histories.get(symbol_id, ())

    def called(self, name: str) -> list[Any]:
        return [args for called, args in self.calls if called == name]


class FakeTemporal:
    def __init__(self) -> None:
        self.decision_records: list[DecisionRecord] = []
        self.failure_records: list[FailureRecord] = []
        self.truncated = False
        self.outdated = False
        self.calls: list[tuple[str, TemporalScope, datetime, datetime, bool]] = []

    async def decisions(
        self, scope: TemporalScope, valid_at: datetime, recorded_at: datetime, history: bool
    ) -> DecisionsResult:
        self.calls.append(("decisions", scope, valid_at, recorded_at, history))
        if self.outdated:
            raise TemporalProjectionOutdated("the temporal history is not complete")
        records = tuple(self.decision_records)
        return DecisionsResult(scope, valid_at, recorded_at, records, self.truncated)

    async def failures(
        self, scope: TemporalScope, valid_at: datetime, recorded_at: datetime, history: bool
    ) -> FailuresResult:
        self.calls.append(("failures", scope, valid_at, recorded_at, history))
        if self.outdated:
            raise TemporalProjectionOutdated("the temporal history is not complete")
        records = tuple(self.failure_records)
        return FailuresResult(scope, valid_at, recorded_at, records, self.truncated)


class FakeSearch:
    def __init__(self) -> None:
        self.lexical_hits: list[SearchHit] = []
        self.vector_hits: list[SearchHit] = []
        self.vector_fails = False
        self.calls: list[tuple[str, str, SearchScope, int]] = []

    async def lexical(self, query: str, scope: SearchScope, limit: int) -> list[SearchHit]:
        self.calls.append(("lexical", query, scope, limit))
        return [hit for hit in self.lexical_hits if hit.project_id == scope.project_id]

    async def vector(self, query: str, scope: SearchScope, limit: int) -> list[SearchHit]:
        self.calls.append(("vector", query, scope, limit))
        if self.vector_fails:
            raise SearchRequestError("no embedding provider is configured")
        return [hit for hit in self.vector_hits if hit.project_id == scope.project_id]


class FakeGraph:
    """`GraphTraversalService.neighborhood` over the CURRENT graph, by anchor."""

    def __init__(self) -> None:
        self.reached: dict[str, GraphNeighborhood] = {}
        self.calls: list[tuple[GraphScope, str, int]] = []

    async def neighborhood(self, scope: GraphScope, anchor: str, depth: int) -> GraphNeighborhood:
        self.calls.append((scope, anchor, depth))
        if anchor not in self.reached:
            raise GraphAnchorNotFound("the anchor is not in the current graph")
        return self.reached[anchor]


def neighborhood(
    anchor: str, nodes: Sequence[tuple[str, int]], *, truncated: bool = False
) -> GraphNeighborhood:
    def node(node_id: str, distance: int) -> GraphNode:
        return GraphNode(NodeKind.FILE, node_id, distance, None, EvidenceRef(()))

    return GraphNeighborhood(
        REPO,
        node(anchor, 0),
        2,
        tuple(node(node_id, distance) for node_id, distance in nodes),
        (),
        (),
        truncated,
    )


# --------------------------------------------------------------------------------------------
# Records
# --------------------------------------------------------------------------------------------


def decision(
    decision_id: str,
    events: Sequence[str],
    *,
    subjects: Sequence[str] = (),
    standing: DecisionStanding = DecisionStanding.ACTIVE,
    reason: HistoricalReason | None = None,
    superseded_by: str | None = None,
    supersedes: str | None = None,
    status: str = "accepted",
    valid_from: datetime | None = None,
    effective_valid_to: datetime | None = None,
) -> DecisionRecord:
    start = valid_from or at(0.5)
    return DecisionRecord(
        decision_id=decision_id,
        standing=standing,
        reason=reason,
        superseded_by=superseded_by,
        status=status,
        subjects=tuple(subjects),
        valid_from=start,
        valid_to=effective_valid_to,
        effective_valid_to=effective_valid_to,
        supersedes_id=supersedes,
        recorded_at=start,
        source_event_ids=tuple(events),
    )


def ci_run(
    run_id: str,
    status: str,
    completed: datetime,
    events: Sequence[str],
    *,
    commit: str | None = None,
    snapshot: str | None = None,
    workflow: str = "ci",
    job: str = "test",
) -> ObservedRun:
    return ObservedRun(
        RunKind.CI_RUN, run_id, status, workflow, job, completed, commit, snapshot, tuple(events)
    )


def failure(
    failure_id: str,
    events: Sequence[str],
    runs: Sequence[ObservedRun],
    resolution: FixResolution,
    *,
    component: str = "pytest",
    operation: str = "test_add",
    error_class: str = "AssertionError",
    fingerprint: str | None = None,
    valid_from: datetime | None = None,
) -> FailureRecord:
    start = valid_from or at(1.2)
    return FailureRecord(
        failure_id=failure_id,
        component=component,
        operation=operation,
        error_class=error_class,
        fingerprint_sha256=fingerprint,
        valid_from=start,
        recorded_at=start,
        observed_in=tuple(runs),
        resolution=resolution,
        source_event_ids=tuple(events),
    )


def hit(
    ledger: FakeLedger,
    key: str,
    content_id: str,
    score: float,
    *,
    session: str | None = None,
    project: str = PROJECT,
) -> SearchHit:
    """A search row for the content of the ledger event `key` (its type and time are the event's)."""
    event_id = ledger.id(key)
    event = ledger.stored[event_id].event
    return SearchHit(
        content_id,
        score,
        project,
        REPO,
        session,
        event_id,
        event.event_type,
        event.occurred_at,
        (event_id,),
        "sanitized",
    )


# --------------------------------------------------------------------------------------------
# The seeded world
# --------------------------------------------------------------------------------------------


@dataclass
class World:
    ledger: FakeLedger
    freshness: FakeFreshness
    revisions: FakeRevisions
    temporal: FakeTemporal
    search: FakeSearch
    graph: FakeGraph
    cursor: CursorCodec = field(default_factory=CursorCodec.ephemeral)
    runs: dict[str, Run] = field(default_factory=dict)

    def composer(
        self, *, clock: datetime = NOW, vector_search: bool = True, deadline: float = 10.0
    ) -> ContextComposer:
        doubles: dict[str, Any] = {
            "graph": self.graph,
            "temporal": self.temporal,
            "search": self.search,
            "ledger": self.ledger,
            "freshness": self.freshness,
        }
        composer = ContextComposer(
            graph_reader=cast(Any, None),
            cursor=self.cursor,
            clock=lambda: clock,
            default_deadline_seconds=deadline,
            vector_search=vector_search,
            **doubles,
        )
        composer._revisions = cast(Any, self.revisions)
        return composer

    def id(self, key: str) -> str:
        return self.ledger.id(key)


def content_item(content_id: str) -> str:
    """The item ID of a search hit's content (its content ID is never shown)."""
    return "content:" + hashlib.sha256(content_id.encode()).hexdigest()[:32]


def cited(page: ContextPackageV1) -> set[str]:
    return {str(ref.source_event_id) for item in page.items for ref in item.evidence}


def items_by_id(page: ContextPackageV1) -> dict[str, Any]:
    return {item.item_id: item for item in page.items}


def _file(
    ledger: FakeLedger,
    at_run: str,
    file_id: str,
    revision: str,
    path: str,
    module: str,
    language: str | None,
    occurred: datetime,
) -> FileFact:
    event = ledger.add(f"{at_run}:file:{file_id}", "code.file.indexed", occurred)
    return FileFact(file_id, revision, path, module, language, "scanner", "1", (event,))


def _symbol(
    ledger: FakeLedger,
    at_run: str,
    symbol_id: str,
    revision: str,
    name: str,
    file_id: str,
    file_revision: str,
    scip: str | None,
    occurred: datetime,
) -> SymbolFact:
    event = ledger.add(f"{at_run}:symbol:{symbol_id}", "code.symbol.indexed", occurred)
    extractor = "scip-python" if scip else "tree-sitter"
    return SymbolFact(
        symbol_id,
        revision,
        name,
        "function",
        file_id,
        file_revision,
        extractor,
        "1",
        scip,
        stamp(occurred),
        (event,),
    )


def _assertion(
    assertion_id: str, family: str, subject: str, event: str, **fields: Any
) -> AssertionFact:
    empty = dict.fromkeys(
        (
            "predicate",
            "object_id",
            "evidence_kind",
            "confidence",
            "extractor_name",
            "review_status",
            "file_revision_id",
            "dependency_id",
            "dependency_kind",
            "requirement",
            "resolved_version",
        )
    )
    return AssertionFact(
        assertion_id=assertion_id,
        family=family,
        subject_id=subject,
        extractor_version="1",
        valid_from=stamp(at(1)),
        source_event_ids=(event,),
        **(empty | fields),
    )


def _relation(
    ledger: FakeLedger,
    assertion_id: str,
    subject: str,
    predicate: str,
    target: str,
    kind: str,
    confidence: float,
    *,
    review: str | None = None,
) -> AssertionFact:
    event = ledger.add(f"c2:relation:{assertion_id}", "code.relation.asserted", at(1))
    extractor = "scip-python" if kind == "scip" else "tree-sitter"
    return _assertion(
        assertion_id,
        "relation",
        subject,
        event,
        predicate=predicate,
        object_id=target,
        evidence_kind=kind,
        confidence=confidence,
        extractor_name=extractor,
        review_status=review,
    )


def _dependency(
    ledger: FakeLedger,
    assertion_id: str,
    subject: str,
    dependency: str,
    confidence: float,
    *,
    requirement: str | None,
    resolved: str | None,
) -> AssertionFact:
    event = ledger.add(f"c2:dependency:{assertion_id}", "code.dependency.asserted", at(1))
    return _assertion(
        assertion_id,
        "dependency",
        subject,
        event,
        evidence_kind="tree_sitter",
        confidence=confidence,
        extractor_name="manifest",
        dependency_id=dependency,
        dependency_kind="pypi",
        requirement=requirement,
        resolved_version=resolved,
    )


def seeded() -> World:
    ledger = FakeLedger()
    run1 = Run(
        "idx-1",
        "Commit",
        commit_node_id(REPO, C1),
        stamp(at(0)),
        (ledger.add("run:c1", "code.index.started", at(0)),),
        True,
    )
    run2 = Run(
        "idx-2",
        "Commit",
        commit_node_id(REPO, C2),
        stamp(at(1)),
        (ledger.add("run:c2", "code.index.started", at(1)),),
        True,
    )
    run3 = Run(
        "idx-3",
        "WorkspaceSnapshot",
        SNAP,
        stamp(at(2)),
        (ledger.add("run:snap", "code.index.started", at(2)),),
        False,
    )

    # C1: calc, legacy, util and the README.
    c1_files = (
        _file(ledger, "c1", "f1", "fr1a", "pkg/calc.py", "m1", "python", at(0)),
        _file(ledger, "c1", "f3", "fr3", "pkg/legacy.py", "m1", "python", at(0)),
        _file(ledger, "c1", "f4", "fr4a", "pkg/util.py", "m1", "python", at(0)),
        _file(ledger, "c1", "f5", "fr5", "README.md", "m0", "markdown", at(0)),
    )
    c1_add = _symbol(ledger, "c1", "s1", "sr1a", "pkg.calc.add", "f1", "fr1a", "scip:add", at(0))
    # C2: calc modified, report added, legacy removed, util renamed to helpers (and changed).
    c2_files = (
        _file(ledger, "c2", "f1", "fr1b", "pkg/calc.py", "m1", "python", at(1)),
        _file(ledger, "c2", "f2", "fr2", "pkg/report.py", "m1", "python", at(1)),
        _file(ledger, "c2", "f4", "fr4b", "pkg/helpers.py", "m1", "python", at(1)),
        c1_files[3],  # the README is unchanged: C2 replays C1's claim
    )
    add = _symbol(ledger, "c2", "s1", "sr1b", "pkg.calc.add", "f1", "fr1b", "scip:add", at(1))
    total = _symbol(ledger, "c2", "s2", "sr2", "pkg.report.total", "f2", "fr2", None, at(1))
    half = _symbol(ledger, "c2", "s4", "sr4", "pkg.helpers.round_half", "f4", "fr4b", None, at(1))
    assertions = (
        _relation(ledger, "r1", "s2", "CALLS", "s1", "scip", 1.0),
        _relation(ledger, "r2", "f2", "IMPORTS", "f4", "tree_sitter", 0.95, review="accepted"),
        _relation(ledger, "r3", "s4", "REFERENCES", "s1", "tree_sitter", 0.5),
        _dependency(ledger, "d1", "f1", "numpy", 1.0, requirement=">=2", resolved="2.1.0"),
        _dependency(ledger, "d2", "f2", "requests", 0.6, requirement=">=2.31", resolved=None),
    )
    # The snapshot: C2's tree plus an uncommitted draft.
    draft_file = _file(ledger, "snap", "f6", "fr6", "pkg/draft.py", "m1", "python", at(2))
    average = _symbol(ledger, "snap", "s6", "sr6", "pkg.draft.average", "f6", "fr6", None, at(2))

    revisions = FakeRevisions()
    revisions.views[C1] = RevisionView(
        REPO, run1, None, stamp(at(1)), (run1,), c1_files, (c1_add,), (), False
    )
    revisions.views[C2] = RevisionView(
        REPO,
        run2,
        run1,
        stamp(at(2)),
        (run1, run2),
        c2_files,
        (add, total, half),
        assertions,
        False,
    )
    revisions.views[SNAP] = RevisionView(
        REPO,
        run3,
        run2,
        None,
        (run1, run2, run3),
        (*c2_files, draft_file),
        (add, total, half, average),
        assertions,
        False,
    )
    revisions.run_members[run1.key] = c1_files

    # Decisions: one about `add`, one unrelated and only proposed.
    ledger.add(
        "dec:round",
        "knowledge.decision.recorded",
        at(0.5),
        {"decision_id": "dec_round"},
        text="Round half even in add.",
    )
    ledger.add(
        "dec:logging",
        "knowledge.decision.recorded",
        at(0.6),
        {"decision_id": "dec_log"},
        text="Log as structured JSON.",
    )
    temporal = FakeTemporal()
    temporal.decision_records = [
        decision("dec_round", [ledger.id("dec:round")], subjects=["pkg.calc.add"]),
        decision(
            "dec_logging", [ledger.id("dec:logging")], subjects=["pkg.log"], status="proposed"
        ),
    ]

    # Failures: a drift seen in a clean C2 run AND in a run of the dirty snapshot (its dirty
    # observation listed first), and a flaky lint timeout that a re-run on the same commit passed.
    ledger.add(
        "ci:clean", "quality.ci_run.completed", at(1.2), {"ci_run_id": "ci-clean", "commit_id": C2}
    )
    ledger.add(
        "ci:dirty",
        "quality.ci_run.completed",
        at(1.5),
        {"ci_run_id": "ci-dirty", "snapshot_id": SNAP},
    )
    ledger.add(
        "obs:dirty",
        "knowledge.failure.observed",
        at(1.5),
        {"ci_run_id": "ci-dirty"},
        text="DIRTYMARK drift details from the dirty workspace",
    )
    ledger.add(
        "obs:clean",
        "knowledge.failure.observed",
        at(1.2),
        {"ci_run_id": "ci-clean"},
        text="clean drift details",
    )
    ledger.add(
        "ci:flaky-fail",
        "quality.ci_run.completed",
        at(1.3),
        {"ci_run_id": "ci-flaky-1", "commit_id": C2},
    )
    ledger.add(
        "ci:flaky-pass",
        "quality.ci_run.completed",
        at(1.4),
        {"ci_run_id": "ci-flaky-2", "commit_id": C2},
    )
    ledger.add(
        "obs:flaky",
        "knowledge.failure.observed",
        at(1.3),
        {"ci_run_id": "ci-flaky-1"},
        text="lint timed out",
    )
    drift = failure(
        "fail_drift",
        [ledger.id("obs:dirty"), ledger.id("obs:clean")],
        [
            ci_run("ci-clean", "failure", at(1.2), [ledger.id("ci:clean")], commit=C2),
            ci_run("ci-dirty", "failure", at(1.5), [ledger.id("ci:dirty")], snapshot=SNAP),
        ],
        FixResolution(FixStatus.UNRESOLVED, "no_passing_run"),
        fingerprint="1" * 64,
    )
    flaky = failure(
        "fail_flaky",
        [ledger.id("obs:flaky")],
        [
            ci_run(
                "ci-flaky-1",
                "failure",
                at(1.3),
                [ledger.id("ci:flaky-fail")],
                commit=C2,
                workflow="lint",
                job="ruff",
            )
        ],
        FixResolution(
            FixStatus.RESOLVED,
            None,
            FixPath(
                "fail_flaky",
                "ci-flaky-1",
                "ci-flaky-2",
                "lint",
                "ruff",
                at(1.3),
                at(1.4),
                C2,
                None,
                False,
                (ledger.id("ci:flaky-pass"),),
            ),
        ),
        component="lint",
        operation="timeout",
        error_class="TimeoutError",
        fingerprint="2" * 64,
        valid_from=at(1.3),
    )
    temporal.failure_records = [drift, flaky]

    # Searchable content: a turn in C2's window, a later turn, the dirty snapshot's CI output,
    # C2's own CI output, a finding, a decision's own text, a decision the temporal read did not
    # return, and a commit message found by vector only (plus a vector hit below the floor).
    ledger.add(
        "turn:1",
        "agent.turn.completed",
        at(1.5),
        content_id="c-turn",
        text="We should round half even in add.",
    )
    ledger.add(
        "turn:late",
        "agent.turn.completed",
        at(2.5),
        content_id="c-late",
        text="A later note about rounding.",
    )
    ledger.add(
        "ci:dirty-output",
        "quality.test_run.completed",
        at(1.6),
        {"test_run_id": "t-dirty", "snapshot_id": SNAP},
        content_id="c-dirty-output",
        text="DIRTYMARK dirty workspace test output",
    )
    ledger.add(
        "ci:clean-output",
        "quality.ci_run.completed",
        at(1.25),
        {"ci_run_id": "ci-clean", "commit_id": C2},
        content_id="c-clean-output",
        text="rounding drift in add: expected 0.3",
    )
    ledger.add(
        "finding:1",
        "quality.finding.observed",
        at(1.7),
        {"finding_id": "fi-1"},
        content_id="c-finding",
        text="rounding helper lacks a test",
    )
    ledger.add(
        "dec:hidden",
        "knowledge.decision.recorded",
        at(1.1),
        {"decision_id": "dec_x"},
        content_id="c-hidden",
        text="HIDDEN rounding decision",
    )
    ledger.add(
        "commit:c2", "git.commit.observed", at(1), content_id="c-commit", text="Fix rounding in add"
    )
    ledger.add("turn:noise", "agent.turn.started", at(1.5), content_id="c-noise", text="hello")
    search = FakeSearch()
    search.lexical_hits = [
        hit(ledger, "turn:1", "c-turn", 8.0, session="sess-1"),
        hit(ledger, "turn:late", "c-late", 6.0, session="sess-1"),
        hit(ledger, "ci:dirty-output", "c-dirty-output", 4.0),
        hit(ledger, "ci:clean-output", "c-clean-output", 4.0),
        hit(ledger, "finding:1", "c-finding", 2.0),
        hit(ledger, "dec:round", "c-dec-round", 2.0),
        hit(ledger, "dec:hidden", "c-hidden", 2.0),
    ]
    search.vector_hits = [
        hit(ledger, "commit:c2", "c-commit", 0.9),
        hit(ledger, "turn:1", "c-turn", 0.5, session="sess-1"),  # weaker than its lexical match
        hit(ledger, "turn:noise", "c-noise", 0.2, session="sess-1"),  # below the cosine floor
    ]

    freshness = FakeFreshness(
        Freshness(
            AS_OF, AS_OF_OUTBOX, at(3), None, "pc1-unit", 0, ("projection_dead_letters:code",)
        )
    )
    return World(
        ledger,
        freshness,
        revisions,
        temporal,
        search,
        FakeGraph(),
        runs={"c1": run1, "c2": run2, "snap": run3},
    )


# --------------------------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------------------------


def commit(oid: str = C2) -> dict[str, str]:
    return {"kind": "commit", "commit_id": oid}


def snapshot(snapshot_id: str = SNAP) -> dict[str, str]:
    return {"kind": "workspace_snapshot", "snapshot_id": snapshot_id}


def task_input(
    text: str,
    revision: dict[str, str] | None = None,
    *,
    budget: dict[str, int] | None = None,
    **extra: Any,
) -> ContextForTaskInputV1:
    return ContextForTaskInputV1.model_validate(
        {
            "repository_id": REPO,
            "revision": revision or commit(),
            "task": text,
            "budget": budget or WIDE,
        }
        | extra
    )


def overview_input(
    oid: str = C2, *, budget: dict[str, int] | None = None, **extra: Any
) -> RepositoryOverviewInputV1:
    return RepositoryOverviewInputV1.model_validate(
        {"repository_id": REPO, "commit_id": oid, "budget": budget or WIDE} | extra
    )


def symbol_input(
    oid: str = C2, *, budget: dict[str, int] | None = None, **extra: Any
) -> SymbolContextInputV1:
    return SymbolContextInputV1.model_validate(
        {"repository_id": REPO, "commit_id": oid, "budget": budget or WIDE} | extra
    )


def decision_input(
    query: str,
    *,
    scope: dict[str, str] | None = None,
    valid_at: datetime | None = None,
    budget: dict[str, int] | None = None,
    **extra: Any,
) -> DecisionHistoryInputV1:
    return DecisionHistoryInputV1.model_validate(
        {
            "scope": scope or {"kind": "project", "project_id": PROJECT},
            "query": query,
            "valid_at": (valid_at or at(3)).isoformat(),
            "budget": budget or WIDE,
        }
        | extra
    )


def failure_input(
    *,
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
    budget: dict[str, int] | None = None,
    **extra: Any,
) -> FailureHistoryInputV1:
    return FailureHistoryInputV1.model_validate(
        {
            "repository_id": REPO,
            "valid_from": (valid_from or at(0)).isoformat(),
            "valid_to": (valid_to or at(4)).isoformat(),
            "budget": budget or WIDE,
        }
        | extra
    )
