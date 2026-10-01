"""Project the temporal code graph (design 9, 10) from the canonical `code.*` events.

Two layers keep every write idempotent and order-independent:

1. Immutable FACTS, one idempotent write per event. FileRevision, SymbolRevision and Assertion
   nodes are keyed by their logical ID and filled once (`coalesce`, so a stub made by an earlier
   event is completed by a later one, and a replay changes nothing). Membership, coverage and
   binding facts are relationships carrying a sorted `source_event_ids` list and the envelope
   `occurred_at`/`observed_at`:

   - `(Repository)-[:INDEXED {index_id}]->(Commit|WorkspaceSnapshot)`: one index run (the target
     node is keyed by the commit or by the opaque `snapshot_id`). It carries the run time (the
     smallest `occurred_at`, the valid time: code events have no commit time).
   - `(Repository)-[:COMPLETED {index_id, event_id}]->(target)`: one immutable observation per
     `code.index.completed` event; the latest visible one (by `(occurred_at, event_id)`, within
     the transaction-time cutoff) is the run's outcome, so a retry never rewrites history.
   - `(FileRevision)-[:MEMBER_OF {index_id, path, module_id}]->(target)`: file membership. The
     path is NOT part of a file revision (a rename keeps the revision), so it lives here.
   - `(FileRevision)-[:COVERED_IN {index_id, event_id}]->(target)`: one immutable coverage report
     (`complete`, `losses`) per event; the latest visible one wins, like the outcome.
   - `(SymbolRevision)-[:DEFINED_IN]->(FileRevision)`: symbol membership follows file revisions.
   - `(Assertion)-[:OBSERVED_IN]->(FileRevision)`: the source file revision of an assertion.
   - `(Assertion)-[:DEPENDS_ON]->(Dependency)`: the external module of a dependency assertion.

2. DERIVED state (current pointers, validity intervals, `current` flags, resolved edges): a pure
   function of the stored facts (`derive_*`), recomputed inside the same transaction for the
   entities an event can affect and written with replace semantics. Nothing here closes an
   interval because "the next event said so": a closure is always a consequence of the facts, so
   any delivery order, and duplicates, converge on the same graph. Absence is inferred only as the
   card states: a run whose `code.index.completed` is missing or `scan_incomplete` /
   `snapshot_incomplete` infers none; per file revision only complete coverage does.

   - `(File)-[:HAS_REVISION {path, module_id, valid_from, valid_to, recorded_from, recorded_to}]->`
     `(FileRevision)` and `(File)-[:CURRENT_REVISION]->(FileRevision)`; same for `Symbol`.
   - `Assertion.current`, `valid_to`, `recorded_to`, `valid_intervals`.
   - `(Symbol)-[:CALLS|REFERENCES|IMPORTS|DEFINES]->(Symbol)` and
     `(File)-[:POSSIBLY_SUPERSEDES]->(File)` for current assertions, carrying the evidence,
     extractor, confidence, validity, review status and source events of the winning assertion
     (SCIP outranks syntactic outranks heuristic); lower-ranked assertions are kept on their nodes.

Valid time is the `occurred_at` of the index run; transaction time is its `observed_at`.
`entity_as_of` answers "current as of (valid, recorded)" by re-deriving from the facts that were
recorded by then, so it matches a replay in any order. No source text is stored, only names,
paths, digests and IDs.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Final, Literal, LiteralString

from agent_context_sdk import (
    CodeAssertionObservedV1,
    CodeDependencyAssertedV1,
    CodeFileCoverageReportedV1,
    CodeFileIndexedV1,
    CodeIndexCompletedV1,
    CodeIndexStartedV1,
    CodeRelationAssertedV1,
    CodeSymbolIndexedV1,
    StoredEventV1,
)

from agent_context_platform.indexing.tree_sitter._common import module_name
from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors import (
    event_order,
    fill_once,
    lock_event_nodes,
    min_non_null,
    node_statement,
    relationship_statement,
)
from agent_context_platform.projection.projectors.git import commit_node_id

BLOCKING_ERROR_CLASSES: Final = frozenset({"scan_incomplete", "snapshot_incomplete"})
SUPERSEDES: Final = "POSSIBLY_SUPERSEDES"
_EVIDENCE_RANK: Final = {"scip": 3, "git": 3, "test": 3, "tree_sitter": 2}

_RUN_TYPES: Final = {
    "code.index.started": CodeIndexStartedV1,
    "code.index.completed": CodeIndexCompletedV1,
    "code.file.indexed": CodeFileIndexedV1,
    "code.file.coverage_reported": CodeFileCoverageReportedV1,
    "code.symbol.indexed": CodeSymbolIndexedV1,
    "code.assertion.observed": CodeAssertionObservedV1,
}
_ASSERTION_TYPES: Final = {
    "code.relation.asserted": CodeRelationAssertedV1,
    "code.dependency.asserted": CodeDependencyAssertedV1,
}
CODE_EVENT_TYPES: Final = frozenset({*_RUN_TYPES, *_ASSERTION_TYPES})


def timestamp(moment: datetime) -> str:
    """Fixed-width UTC text, so string order is time order (like `event_order`)."""
    return f"{moment.astimezone(UTC):%Y-%m-%dT%H:%M:%S.%fZ}"


# --------------------------------------------------------------------------------------------
# Derivation: pure functions of the stored facts
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Run:
    """One index run (an `index_id` over one target)."""

    key: str
    occurred_at: str
    observed_at: str
    completed: bool
    error_class: str | None
    outcome_observed_at: str = ""  # when the outcome that `completed` reports was observed

    @property
    def permits_absence(self) -> bool:
        return self.completed and self.error_class not in BLOCKING_ERROR_CLASSES

    @property
    def order(self) -> tuple[str, str]:
        return (self.occurred_at, self.key)


@dataclass(frozen=True, slots=True)
class Member:
    revision_id: str
    path: str
    module_id: str
    observed_at: str = ""


@dataclass(frozen=True, slots=True)
class AssertionFact:
    family: str
    subject_id: str
    predicate: str
    object_id: str | None
    valid_from: str
    valid_to: str | None
    observed_at: str
    revisions: Mapping[str, str]  # source file revision -> when first observed


@dataclass(slots=True)
class Facts:
    """Everything the derivation needs, already filtered by transaction time."""

    runs: list[Run] = field(default_factory=list)
    members: dict[str, dict[str, Member]] = field(default_factory=dict)
    coverage: dict[str, dict[str, dict[str, bool]]] = field(default_factory=dict)
    coverage_observed: dict[tuple[str, str], str] = field(default_factory=dict)  # (run, rev)
    revision_file: dict[str, str] = field(default_factory=dict)
    defs: dict[str, dict[str, str]] = field(default_factory=dict)  # symbol -> revision -> SR
    since: dict[tuple[str, str], str] = field(default_factory=dict)  # (item, revision) -> time
    claim_run: dict[tuple[str, str], str] = field(default_factory=dict)  # (item, revision) -> run
    claim_observed: dict[tuple[str, str], str] = field(default_factory=dict)  # (item, rev) -> tx
    claimed: dict[str, set[str]] = field(default_factory=dict)  # revision -> runs that claim for it
    path_bound: set[str] = field(default_factory=set)  # symbols named from the path (no SCIP ID)
    scips: dict[str, set[str]] = field(default_factory=dict)  # symbol -> its SCIP symbol strings
    item_symbols: dict[str, tuple[str, ...]] = field(default_factory=dict)  # assertion -> ends
    assertions: dict[str, AssertionFact] = field(default_factory=dict)

    @property
    def horizon(self) -> str:
        """The latest valid time the graph knows: the newest run's."""
        return max((run.occurred_at for run in self.runs), default="")


@dataclass(frozen=True, slots=True)
class Interval:
    """A validity interval `[start, end)` of `key` (a revision ID, or the assertion itself).

    `recorded_from`/`recorded_to` are the transaction times at which the evidence that opened
    and closed it was observed (membership, claim, outcome, coverage), not the run's own.
    """

    key: str
    extra: tuple[str, ...]
    start: Run
    end: Run | None
    recorded_from: str = ""
    recorded_to: str | None = None


type Status = (
    tuple[Literal["present"], str, tuple[str, ...], str]
    | tuple[Literal["absent"], str]
    | tuple[Literal["unknown"]]
)
_UNKNOWN: Final[Status] = ("unknown",)


def _timeline(runs: Iterable[Run], status: Callable[[Run], Status]) -> list[Interval]:
    """Fold per-run statuses (in run order) into intervals; `unknown` leaves them untouched."""
    done: list[Interval] = []
    open_: Interval | None = None
    for run in runs:
        state = status(run)
        if state[0] == "unknown":
            continue
        if state[0] == "absent":
            if open_ is not None:
                done.append(replace(open_, end=run, recorded_to=state[1]))
                open_ = None
            continue
        _, key, extra, seen_at = state
        if open_ is not None and (open_.key, open_.extra) == (key, extra):
            continue
        if open_ is not None:
            done.append(replace(open_, end=run, recorded_to=seen_at))
        open_ = Interval(key, extra, run, None, seen_at)
    if open_ is not None:
        done.append(open_)
    return done


def _descriptors(path: str) -> set[str]:
    """How an indexer spells the module of `path`: `src/c` (scip-typescript), `pkg.c`
    (scip-python and the emitter's own dotted module name)."""
    directory, _, base = path.rpartition("/")
    stem = base.rsplit(".", 1)[0] if "." in base else base
    slash = f"{directory}/{stem}" if directory else stem
    dotted = {module_name(path), module_name(path, package_files=("__init__", "index"))}
    return {d for d in {slash, *dotted} if d}


def _symbol_path_bound(facts: Facts, symbol_id: str, path: str) -> bool:
    """Does the identity of `symbol_id` derive from `path`?

    A symbol with no SCIP symbol was named from the module path by the structural parser. A SCIP
    symbol is path-bound only if it spells the module of the claim path (`pkg.c`, `src/c`, as
    scip-python and scip-typescript do); one without it (Go's package-path symbols) is not changed
    by a file rename within its package.
    """
    if symbol_id in facts.path_bound:
        return True
    found = _descriptors(path)
    return any(
        re.search(rf"(?<![A-Za-z0-9_]){re.escape(d)}(?![A-Za-z0-9_])", scip)
        for scip in facts.scips.get(symbol_id, ())
        for d in found
    )


def _path_matches(facts: Facts, item: str, member: Member) -> bool:
    """Does `item`'s claim on `member`'s file revision belong to the path it has now?

    The path is not part of a file revision (a rename keeps it), but identities derived from the
    path are claimed anew under the new path. So an item whose own identity is path-derived
    (`_symbol_path_bound`; an assertion inherits it from its subject or object) holds only while
    the file is at the path it was claimed at, once its revision is claimed under more than one
    path. Items with a path-independent identity keep their claim; so does anything on a
    revision claimed under a single path.
    """
    file_id = facts.revision_file[member.revision_id]

    def path_of(run_key: str) -> str | None:
        held = facts.members.get(run_key, {}).get(file_id)
        return held.path if held is not None and held.revision_id == member.revision_id else None

    paths = {path_of(k) for k in facts.claimed.get(member.revision_id, ())} - {None}
    own = facts.claim_run.get((item, member.revision_id))
    claimed_at = None if own is None else path_of(own)
    if len(paths) <= 1 or claimed_at is None or claimed_at == member.path:
        return True
    return not any(
        _symbol_path_bound(facts, symbol, claimed_at)
        for symbol in facts.item_symbols.get(item, (item,))
    )


def _live(facts: Facts, run: Run, item: str, owners: Mapping[str, str]) -> list[str]:
    """The owning file revisions whose claim on `item` holds in `run`'s membership."""
    in_run = facts.members.get(run.key, {})
    live: list[str] = []
    for revision in sorted(owners):
        member = in_run.get(facts.revision_file.get(revision, ""))
        if (
            member is not None
            and member.revision_id == revision
            and owners[revision] <= run.occurred_at
            and _path_matches(facts, item, member)
        ):
            live.append(revision)
    return live


def _owner_status(
    facts: Facts, run: Run, item: str, owners: Mapping[str, str]
) -> tuple[Literal["present", "absent", "unknown"], str]:
    """Is `item`, claimed by the file revisions `owners`, in `run`'s membership?

    `owners` maps each owning file revision to when the claim was first observed: a claim made
    later than the run is not part of it. Present when a claim holds (`_live`). Otherwise absent
    only when absence may be inferred for the run AND every owning file is gone from it or has a
    member revision whose coverage is complete; anything else (a degraded or uncovered file) is
    unknown. The second value is the latest transaction time of the evidence used.
    """
    live = _live(facts, run, item, owners)
    in_run = facts.members.get(run.key, {})
    if live:
        revision = live[0]
        holder = in_run[facts.revision_file[revision]]
        return "present", max(holder.observed_at, facts.claim_observed.get((item, revision), ""))
    if not run.permits_absence:
        return "unknown", ""
    covered = facts.coverage.get(run.key, {})
    evidence = run.outcome_observed_at
    for file_id in {facts.revision_file[r] for r in owners if r in facts.revision_file}:
        member = in_run.get(file_id)
        if member is not None:
            if not covered.get(file_id, {}).get(member.revision_id, False):
                return "unknown", ""
            evidence = max(
                evidence,
                member.observed_at,
                facts.coverage_observed.get((run.key, member.revision_id), ""),
            )
        elif file_id in covered:
            return "unknown", ""  # seen but not indexed: never an absence
    return "absent", evidence


def derive_file(facts: Facts, file_id: str) -> list[Interval]:
    def status(run: Run) -> Status:
        member = facts.members.get(run.key, {}).get(file_id)
        if member is not None:
            return (
                "present",
                member.revision_id,
                (member.path, member.module_id),
                member.observed_at,
            )
        if file_id in facts.coverage.get(run.key, {}) or not run.permits_absence:
            return _UNKNOWN
        return ("absent", run.outcome_observed_at)

    return _timeline(facts.runs, status)


def _symbol_present(facts: Facts, symbol_id: str, run: Run) -> Status:
    owners = facts.defs.get(symbol_id, {})
    since = {r: facts.since[(symbol_id, r)] for r in owners}
    state, evidence = _owner_status(facts, run, symbol_id, since)
    if state == "unknown":
        return _UNKNOWN
    if state == "absent":
        return ("absent", evidence)
    live = sorted(owners[r] for r in _live(facts, run, symbol_id, since))
    return ("present", live[0], (), evidence)


def derive_symbol(facts: Facts, symbol_id: str) -> list[Interval]:
    return _timeline(facts.runs, lambda run: _symbol_present(facts, symbol_id, run))


def _clip(intervals: list[Interval], fact: AssertionFact) -> list[Interval]:
    """Honor what the assertion itself declares: nothing is valid before `valid_from` or at or
    after `valid_to` (an interval opened by an earlier run starts at `valid_from`)."""
    clipped: list[Interval] = []
    for item in intervals:
        if fact.valid_to is not None:
            if item.start.occurred_at >= fact.valid_to:
                continue
            if item.end is None or item.end.occurred_at > fact.valid_to:
                end = Run("", fact.valid_to, fact.observed_at, True, None)
                item = replace(item, end=end, recorded_to=fact.observed_at)
        if item.start.occurred_at < fact.valid_from:
            if item.end is not None and item.end.occurred_at <= fact.valid_from:
                continue
            start = Run("", fact.valid_from, fact.observed_at, True, None)
            item = replace(
                item, start=start, recorded_from=max(item.recorded_from, fact.observed_at)
            )
        clipped.append(item)
    return clipped


def derive_assertion(facts: Facts, assertion_id: str) -> list[Interval]:
    """Validity intervals of a file-bound assertion; the key is the assertion itself.

    Current at T iff observed for a source revision in T's membership and (relations) its target
    symbol is in T's membership; stale otherwise. Supersessions are not file-bound (always open).
    The assertion's own declared `valid_from`/`valid_to` clamp every interval.
    """
    fact = facts.assertions[assertion_id]
    if fact.predicate == SUPERSEDES:
        start = Run("", fact.valid_from, fact.observed_at, True, None)
        return _clip([Interval(assertion_id, (), start, None, fact.observed_at)], fact)

    def status(run: Run) -> Status:
        state, evidence = _owner_status(facts, run, assertion_id, fact.revisions)
        if state == "absent":
            return ("absent", evidence)
        if state == "unknown":
            return _UNKNOWN
        evidence = max(evidence, fact.observed_at)
        if fact.family == "relation" and fact.object_id is not None:
            target = _symbol_present(facts, fact.object_id, run)
            if target[0] == "unknown":
                return _UNKNOWN  # only an explicit absence closes
            if target[0] == "absent":
                return ("absent", max(evidence, target[1]))
            evidence = max(evidence, target[3])
        return ("present", assertion_id, (), evidence)

    return _clip(_timeline(facts.runs, status), fact)


def is_current(intervals: list[Interval], horizon: str) -> bool:
    """Valid at the latest valid time the graph knows (`horizon`). An end declared in the future,
    past every run, is not a closure yet."""
    return bool(intervals) and (
        intervals[-1].end is None or intervals[-1].end.occurred_at > horizon
    )


def interval_at(intervals: Iterable[Interval], valid_at: str) -> Interval | None:
    for item in intervals:
        if item.start.occurred_at <= valid_at and (
            item.end is None or valid_at < item.end.occurred_at
        ):
            return item
    return None


# --------------------------------------------------------------------------------------------
# Loading facts
# --------------------------------------------------------------------------------------------

_RUNS: Final[LiteralString] = (
    "MATCH (:Repository {repository_id: $repository_id})-[r:INDEXED]->(t) "
    "RETURN r.index_id AS index_id, coalesce(t.commit_id, t.snapshot_id) AS target_id, "
    "r.occurred_at AS occurred_at, r.observed_at AS observed_at"
)
_OUTCOMES: Final[LiteralString] = (
    "MATCH (:Repository {repository_id: $repository_id})-[c:COMPLETED]->(t) "
    "RETURN c.index_id AS index_id, coalesce(t.commit_id, t.snapshot_id) AS target_id, "
    "c.error_class AS error_class, c.event_order AS event_order, c.observed_at AS observed_at"
)
_MEMBERS: Final[LiteralString] = (
    "MATCH (fr:FileRevision)-[m:MEMBER_OF]->(t) WHERE fr.file_id IN $files "
    "RETURN fr.file_revision_id AS revision_id, fr.file_id AS file_id, m.index_id AS index_id, "
    "coalesce(t.commit_id, t.snapshot_id) AS target_id, m.path AS path, "
    "m.module_id AS module_id, m.observed_at AS observed_at"
)
_COVERAGE: Final[LiteralString] = (
    "MATCH (fr:FileRevision)-[c:COVERED_IN]->(t) WHERE fr.file_id IN $files "
    "RETURN fr.file_revision_id AS revision_id, fr.file_id AS file_id, c.index_id AS index_id, "
    "coalesce(t.commit_id, t.snapshot_id) AS target_id, c.complete AS complete, "
    "c.observed_at AS observed_at, c.event_order AS event_order"
)
_DEFS: Final[LiteralString] = (
    "MATCH (sr:SymbolRevision)-[d:DEFINED_IN]->(fr:FileRevision) WHERE sr.symbol_id IN $symbols "
    "RETURN sr.symbol_id AS symbol_id, sr.symbol_revision_id AS symbol_revision_id, "
    "fr.file_revision_id AS revision_id, fr.file_id AS file_id, d.observed_at AS observed_at, d.occurred_at AS since, "
    "d.index_id AS index_id, d.claim_target AS target_id, sr.scip_symbol AS scip_symbol"
)
_ASSERTIONS: Final[LiteralString] = (
    "MATCH (a:Assertion) WHERE a.assertion_id IN $ids "
    "OPTIONAL MATCH (a)-[o:OBSERVED_IN]->(fr:FileRevision) "
    "RETURN a.assertion_id AS assertion_id, a.family AS family, a.subject_id AS subject_id, "
    "a.predicate AS predicate, a.object_id AS object_id, a.valid_from AS valid_from, "
    "a.asserted_valid_to AS valid_to, "
    "a.recorded_from AS recorded_from, fr.file_revision_id AS revision_id, "
    "fr.file_id AS file_id, o.observed_at AS observed_at, o.occurred_at AS since, "
    "o.index_id AS index_id, o.claim_target AS target_id"
)
_CLAIMS: Final[LiteralString] = (
    "MATCH ()-[d:DEFINED_IN|OBSERVED_IN]->(fr:FileRevision) WHERE fr.file_id IN $files "
    "RETURN DISTINCT fr.file_revision_id AS revision_id, d.index_id AS index_id, "
    "d.claim_target AS target_id, d.observed_at AS observed_at"
)


def _run_key(index_id: str, target_id: str) -> str:
    return f"{index_id}|{target_id}"


async def load_facts(
    tx: Neo4jTransaction,
    repository_id: str,
    *,
    files: Iterable[str] = (),
    symbols: Iterable[str] = (),
    assertions: Iterable[str] = (),
    recorded_at: str | None = None,
) -> Facts:
    """Load the facts needed to derive the given entities, to a fixed point.

    An assertion needs its source revisions' files and its target symbol; a symbol needs the
    files of its defining revisions. `recorded_at` drops everything observed after it.
    """

    def seen(observed_at: object) -> bool:
        return recorded_at is None or str(observed_at) <= recorded_at

    facts = Facts()
    runs = (await tx.run(_RUNS, parameters={"repository_id": repository_id})).records
    # Every completion is its own immutable observation; the latest one visible at the cutoff
    # (by `(occurred_at, event_id)`) is the run's outcome, so a retry never rewrites history.
    outcomes: dict[str, tuple[str, str | None, str]] = {}
    for row in (await tx.run(_OUTCOMES, parameters={"repository_id": repository_id})).records:
        outcome_key = _run_key(row["index_id"], row["target_id"])
        if seen(row["observed_at"]) and (
            outcome_key not in outcomes or row["event_order"] > outcomes[outcome_key][0]
        ):
            outcomes[outcome_key] = (row["event_order"], row["error_class"], row["observed_at"])
    for row in runs:
        if not seen(row["observed_at"]):
            continue
        run_key = _run_key(row["index_id"], row["target_id"])
        outcome = outcomes.get(run_key)
        facts.runs.append(
            Run(
                run_key,
                row["occurred_at"],
                row["observed_at"],
                outcome is not None,
                None if outcome is None else outcome[1],
                "" if outcome is None else outcome[2],
            )
        )
    facts.runs.sort(key=lambda run: run.order)
    visible = {run.key for run in facts.runs}

    want_files, want_symbols, want_assertions = set(files), set(symbols), set(assertions)
    done_files: set[str] = set()
    done_symbols: set[str] = set()
    done_assertions: set[str] = set()
    pending: dict[str, dict[str, Any]] = {}
    latest: dict[tuple[str, str], str] = {}  # newest coverage report seen per (run, revision)
    while (
        want_files - done_files or want_symbols - done_symbols or want_assertions - done_assertions
    ):
        new_assertions = sorted(want_assertions - done_assertions)
        done_assertions |= set(new_assertions)
        if new_assertions:
            rows = (await tx.run(_ASSERTIONS, parameters={"ids": new_assertions})).records
            for row in rows:
                item = pending.setdefault(row["assertion_id"], {"row": row, "revisions": {}})
                if row["revision_id"] is not None and seen(row["observed_at"]):
                    held = item["revisions"].get(row["revision_id"])
                    item["revisions"][row["revision_id"]] = min(held or row["since"], row["since"])
                    if held is None or row["since"] < held:
                        facts.claim_observed[(row["assertion_id"], row["revision_id"])] = row[
                            "observed_at"
                        ]
                        facts.claim_run[(row["assertion_id"], row["revision_id"])] = _run_key(
                            row["index_id"], row["target_id"]
                        )
                    facts.revision_file[row["revision_id"]] = row["file_id"]
                    want_files.add(row["file_id"])
            for item in pending.values():
                row = item["row"]
                if row["valid_from"] is None or not seen(row["recorded_from"]):
                    continue
                if row["family"] == "relation" and row["object_id"] is not None:
                    want_symbols.add(row["object_id"])
                want_symbols.add(row["subject_id"])  # to know whether its identity is path-derived
        new_symbols = sorted(want_symbols - done_symbols)
        done_symbols |= set(new_symbols)
        if new_symbols:
            rows = (await tx.run(_DEFS, parameters={"symbols": new_symbols})).records
            for row in rows:
                if not seen(row["observed_at"]):
                    continue
                key = (row["symbol_id"], row["revision_id"])
                if (
                    row["scip_symbol"] is None
                ):  # named from the module path by the structural parser
                    facts.path_bound.add(row["symbol_id"])
                else:
                    facts.scips.setdefault(row["symbol_id"], set()).add(row["scip_symbol"])
                if key not in facts.since or row["since"] < facts.since[key]:
                    facts.since[key] = row["since"]
                    facts.claim_run[key] = _run_key(row["index_id"], row["target_id"])
                    facts.claim_observed[key] = row["observed_at"]
                owners = facts.defs.setdefault(row["symbol_id"], {})
                current = owners.get(row["revision_id"])
                if current is None or row["symbol_revision_id"] < current:
                    owners[row["revision_id"]] = row["symbol_revision_id"]
                facts.revision_file[row["revision_id"]] = row["file_id"]
                want_files.add(row["file_id"])
        new_files = sorted(want_files - done_files)
        done_files |= set(new_files)
        if new_files:
            for row in (await tx.run(_MEMBERS, parameters={"files": new_files})).records:
                run_key = _run_key(row["index_id"], row["target_id"])
                if run_key not in visible or not seen(row["observed_at"]):
                    continue
                facts.revision_file[row["revision_id"]] = row["file_id"]
                held = facts.members.setdefault(run_key, {}).get(row["file_id"])
                if held is None or row["revision_id"] < held.revision_id:
                    facts.members[run_key][row["file_id"]] = Member(
                        row["revision_id"], row["path"], row["module_id"], row["observed_at"]
                    )
            for row in (await tx.run(_CLAIMS, parameters={"files": new_files})).records:
                if seen(row["observed_at"]):
                    facts.claimed.setdefault(row["revision_id"], set()).add(
                        _run_key(row["index_id"], row["target_id"])
                    )
            for row in (await tx.run(_COVERAGE, parameters={"files": new_files})).records:
                run_key = _run_key(row["index_id"], row["target_id"])
                if run_key not in visible or not seen(row["observed_at"]):
                    continue
                facts.revision_file[row["revision_id"]] = row["file_id"]
                reported = facts.coverage.setdefault(run_key, {}).setdefault(row["file_id"], {})
                coverage_key = (run_key, row["revision_id"])
                if coverage_key not in latest or row["event_order"] > latest[coverage_key]:
                    latest[coverage_key] = row["event_order"]
                    reported[row["revision_id"]] = bool(row["complete"])
                    facts.coverage_observed[coverage_key] = row["observed_at"]
    for assertion_id, item in pending.items():
        row = item["row"]
        if row["valid_from"] is None or not seen(row["recorded_from"]):
            continue  # only a stub from an observation: the assertion itself is not recorded
        facts.item_symbols[assertion_id] = tuple(
            x
            for x in (row["subject_id"], row["object_id"] if row["family"] == "relation" else None)
            if x is not None
        )
        facts.assertions[assertion_id] = AssertionFact(
            row["family"],
            row["subject_id"],
            row["predicate"],
            row["object_id"],
            row["valid_from"],
            row["valid_to"],
            row["recorded_from"],
            item["revisions"],
        )
    return facts


# --------------------------------------------------------------------------------------------
# Fact writes
# --------------------------------------------------------------------------------------------

_PROVENANCE: Final[LiteralString] = (
    " WITH n UNWIND coalesce(n.source_event_ids, []) + [$event_id] AS source_event_id "
    "WITH DISTINCT n, source_event_id ORDER BY source_event_id "
    "WITH n, collect(source_event_id) AS source_event_ids "
    "SET n.source_event_ids = source_event_ids"
)


def _target_statement(label: LiteralString, key: LiteralString) -> LiteralString:
    return f"MERGE (n:{label} {{{key}: $node_id}}) WITH n " + min_non_null("repository_id")


def _fact_statement(
    source_label: LiteralString,
    source_key: LiteralString,
    rel_type: LiteralString,
    target_label: LiteralString,
    target_key: LiteralString,
    keyed: LiteralString,
    body: LiteralString,
) -> LiteralString:
    """`MERGE` a fact relationship `n` on `keyed` properties, then `body` and provenance."""
    return (
        f"MATCH (a:{source_label} {{{source_key}: $source_id}}) "
        f"MATCH (b:{target_label} {{{target_key}: $target_id}}) "
        f"MERGE (a)-[n:{rel_type} {{{keyed}}}]->(b) WITH n " + body + _PROVENANCE
    )


def _run_statement(label: LiteralString, key: LiteralString) -> LiteralString:
    return (
        (
            "MATCH (a:Repository {repository_id: $source_id}) "
            f"MATCH (b:{label} {{{key}: $target_id}}) "
            "MERGE (a)-[n:INDEXED {index_id: $index_id}]->(b) WITH n "
        )
        + min_non_null("occurred_at", "observed_at")
        + _PROVENANCE
    )


def _completed_statement(label: LiteralString, key: LiteralString) -> LiteralString:
    """One immutable observation per `code.index.completed` event."""
    return (
        "MATCH (a:Repository {repository_id: $source_id}) "
        f"MATCH (b:{label} {{{key}: $target_id}}) "
        "MERGE (a)-[n:COMPLETED {index_id: $index_id, event_id: $event_id}]->(b) WITH n "
        + fill_once("success", "error_class", "event_order", "occurred_at", "observed_at")
        + _PROVENANCE
    )


_FILE_REVISION: Final = node_statement("FileRevision", "file_revision_id") + fill_once("file_id")
_FILE_REVISION_FULL: Final = node_statement("FileRevision", "file_revision_id") + fill_once(
    "file_id", "content_sha256", "language", "extractor_name", "extractor_version"
)
_FILE: Final = node_statement("File", "file_id") + fill_once("repository_id")
_MODULE: Final = node_statement("Module", "module_id") + fill_once("repository_id")
_SYMBOL: Final = node_statement("Symbol", "symbol_id") + fill_once("repository_id")
_SYMBOL_REVISION: Final = node_statement("SymbolRevision", "symbol_revision_id") + fill_once(
    "symbol_id",
    "qualified_name",
    "kind",
    "signature_fingerprint_sha256",
    "semantic_fingerprint_sha256",
    "extractor_name",
    "extractor_version",
    "scip_symbol",
)
_ASSERTION_STUB: Final = node_statement("Assertion", "assertion_id") + fill_once(
    "family", "repository_id"
)
_ASSERTION: Final = (
    node_statement("Assertion", "assertion_id")
    + fill_once(
        "family",
        "repository_id",
        "subject_id",
        "predicate",
        "object_id",
        "evidence_kind",
        "deterministic",
        "extractor_name",
        "extractor_version",
        "confidence",
        "valid_from",
        "asserted_valid_to",
        "review_status",
        "dependency_kind",
        "requirement",
        "resolved_version",
    )
    + ", "
    + min_non_null("recorded_from", "source_event_id")[4:]
    + _PROVENANCE
)
_DEPENDENCY: Final = node_statement("Dependency", "dependency_id") + fill_once("repository_id")

_IN_REPOSITORY: Final = {
    label: relationship_statement(label, key, "IN_REPOSITORY", "Repository", "repository_id")
    for label, key in (
        ("File", "file_id"),
        ("Module", "module_id"),
        ("Symbol", "symbol_id"),
    )
}
_DEPENDS_ON: Final = relationship_statement(
    "Assertion", "assertion_id", "DEPENDS_ON", "Dependency", "dependency_id"
)


def _target_of(payload: Any, repository_id: str) -> tuple[LiteralString, LiteralString, str]:
    if payload.commit_id is not None:
        return "Commit", "commit_id", commit_node_id(repository_id, payload.commit_id)
    return "WorkspaceSnapshot", "snapshot_id", str(payload.snapshot_id)


def _repository_of(event: StoredEventV1) -> str:
    repository_id = event.context.repository_id
    if repository_id is None:
        raise ValueError("a code event needs context.repository_id")
    return str(repository_id)


def lock_keys(event: StoredEventV1) -> list[tuple[str, str]]:
    """Nodes `CodeProjector` locks for `event`: the repository (it serializes the derivation)
    and the run target. Everything else is only written under that repository lock."""
    if event.event_type not in CODE_EVENT_TYPES:
        return []
    repository_id = _repository_of(event)
    keys = [("Repository", repository_id)]
    if event.event_type in _RUN_TYPES:
        payload = _RUN_TYPES[event.event_type].model_validate(dict(event.payload))
        label, _, node_id = _target_of(payload, repository_id)
        keys.append((label, node_id))
    return sorted(keys)


@dataclass(slots=True)
class _Scope:
    everything: bool = False
    files: set[str] = field(default_factory=set)
    symbols: set[str] = field(default_factory=set)
    assertions: set[str] = field(default_factory=set)


async def _write_run_event(
    tx: Neo4jTransaction, event: StoredEventV1, repository_id: str, scope: _Scope
) -> None:
    payload: Any = _RUN_TYPES[event.event_type].model_validate(dict(event.payload))
    label, key, target = _target_of(payload, repository_id)
    base = {"event_id": str(event.event_id), "repository_id": repository_id}
    occurred, observed = timestamp(event.occurred_at), timestamp(event.observed_at)
    await tx.run(_MERGE_REPOSITORY, parameters={"node_id": repository_id})
    await tx.run(
        _target_statement(label, key),
        parameters={"node_id": target, "repository_id": repository_id},
    )
    kind = event.event_type
    run = {**base, "source_id": repository_id, "target_id": target, "index_id": payload.index_id}
    run |= {"occurred_at": occurred, "observed_at": observed}
    await tx.run(_run_statement(label, key), parameters=run)
    if kind in ("code.index.started", "code.index.completed"):
        if kind == "code.index.completed":
            await tx.run(
                _completed_statement(label, key),
                parameters=run
                | {
                    "event_order": event_order(event),
                    "success": payload.success,
                    "error_class": payload.error_class,
                },
            )
        scope.everything = True
        return
    link = {
        **base,
        "source_id": payload.file_revision_id,
        "target_id": target,
        "index_id": payload.index_id,
        "occurred_at": occurred,
        "observed_at": observed,
    }
    if kind == "code.file.indexed":
        await _write_file(tx, payload, repository_id, link)
        scope.files.add(payload.file_id)
    elif kind == "code.file.coverage_reported":
        await tx.run(
            _FILE_REVISION,
            parameters={"node_id": payload.file_revision_id, "file_id": payload.file_id},
        )
        await tx.run(_FILE, parameters={"node_id": payload.file_id, "repository_id": repository_id})
        await tx.run(
            _fact_statement(
                "FileRevision",
                "file_revision_id",
                "COVERED_IN",
                label,
                key,
                "index_id: $index_id, event_id: $event_id",
                fill_once("complete", "losses", "event_order")
                + " WITH n "
                + min_non_null("observed_at"),
            ),
            parameters={
                **link,
                "event_order": event_order(event),
                "complete": payload.complete,
                "losses": sorted(str(loss.value) for loss in payload.losses),
            },
        )
        scope.files.add(payload.file_id)
    elif kind == "code.symbol.indexed":
        await _write_symbol(tx, event, payload, repository_id)
        scope.files.add(payload.file_id)
        scope.symbols.add(payload.symbol_id)
    else:
        await tx.run(
            _ASSERTION_STUB,
            parameters={
                "node_id": payload.assertion_id,
                "family": payload.assertion_family.value,
                "repository_id": repository_id,
            },
        )
        await tx.run(
            _FILE_REVISION,
            parameters={"node_id": payload.file_revision_id, "file_id": payload.file_id},
        )
        await tx.run(
            _fact_statement(
                "Assertion",
                "assertion_id",
                "OBSERVED_IN",
                "FileRevision",
                "file_revision_id",
                "index_id: $index_id",
                fill_once("claim_target") + " WITH n " + min_non_null("observed_at", "occurred_at"),
            ),
            parameters={
                **base,
                "index_id": payload.index_id,
                "claim_target": target,
                "source_id": payload.assertion_id,
                "target_id": payload.file_revision_id,
                "observed_at": observed,
                "occurred_at": occurred,
            },
        )
        scope.files.add(payload.file_id)
        scope.assertions.add(payload.assertion_id)


_MERGE_REPOSITORY: Final = "MERGE (n:Repository {repository_id: $node_id})"


async def _write_file(
    tx: Neo4jTransaction, payload: CodeFileIndexedV1, repository_id: str, link: dict[str, object]
) -> None:
    await tx.run(
        _FILE_REVISION_FULL,
        parameters={
            "node_id": payload.file_revision_id,
            "file_id": payload.file_id,
            "content_sha256": payload.content_sha256,
            "language": payload.language,
            "extractor_name": payload.extractor_name,
            "extractor_version": payload.extractor_version,
        },
    )
    await tx.run(_FILE, parameters={"node_id": payload.file_id, "repository_id": repository_id})
    await tx.run(_MODULE, parameters={"node_id": payload.module_id, "repository_id": repository_id})
    for label, node in (("File", payload.file_id), ("Module", payload.module_id)):
        await tx.run(
            _IN_REPOSITORY[label],
            parameters={
                "event_id": link["event_id"],
                "source_id": node,
                "target_id": repository_id,
            },
        )
    label, key, _ = _target_of(payload, repository_id)
    await tx.run(
        _fact_statement(
            "FileRevision",
            "file_revision_id",
            "MEMBER_OF",
            label,
            key,
            "index_id: $index_id",
            fill_once("file_id", "path", "module_id")
            + " WITH n "
            + min_non_null("occurred_at", "observed_at"),
        ),
        parameters={
            **link,
            "file_id": payload.file_id,
            "path": payload.path,
            "module_id": payload.module_id,
        },
    )


async def _write_symbol(
    tx: Neo4jTransaction,
    event: StoredEventV1,
    payload: CodeSymbolIndexedV1,
    repository_id: str,
) -> None:
    await tx.run(_SYMBOL, parameters={"node_id": payload.symbol_id, "repository_id": repository_id})
    await tx.run(
        _IN_REPOSITORY["Symbol"],
        parameters={
            "event_id": str(event.event_id),
            "source_id": payload.symbol_id,
            "target_id": repository_id,
        },
    )
    await tx.run(
        _SYMBOL_REVISION,
        parameters={
            "node_id": payload.symbol_revision_id,
            "symbol_id": payload.symbol_id,
            "qualified_name": payload.qualified_name,
            "kind": payload.kind,
            "signature_fingerprint_sha256": payload.signature_fingerprint_sha256,
            "semantic_fingerprint_sha256": payload.semantic_fingerprint_sha256,
            "extractor_name": payload.extractor_name,
            "extractor_version": payload.extractor_version,
            "scip_symbol": payload.scip_symbol,
        },
    )
    await tx.run(
        _FILE_REVISION,
        parameters={"node_id": payload.file_revision_id, "file_id": payload.file_id},
    )
    await tx.run(
        _fact_statement(
            "SymbolRevision",
            "symbol_revision_id",
            "DEFINED_IN",
            "FileRevision",
            "file_revision_id",
            "index_id: $index_id",
            fill_once("claim_target") + " WITH n " + min_non_null("observed_at", "occurred_at"),
        ),
        parameters={
            "event_id": str(event.event_id),
            "index_id": payload.index_id,
            "claim_target": _target_of(payload, repository_id)[2],
            "source_id": payload.symbol_revision_id,
            "target_id": payload.file_revision_id,
            "observed_at": timestamp(event.observed_at),
            "occurred_at": timestamp(event.occurred_at),
        },
    )


async def _write_assertion(
    tx: Neo4jTransaction, event: StoredEventV1, repository_id: str, scope: _Scope
) -> None:
    payload: Any = _ASSERTION_TYPES[event.event_type].model_validate(dict(event.payload))
    relation = event.event_type == "code.relation.asserted"
    props: dict[str, object] = {
        "node_id": payload.assertion_id,
        "event_id": str(event.event_id),
        "source_event_id": str(event.event_id),
        "family": "relation" if relation else "dependency",
        "repository_id": repository_id,
        "subject_id": payload.subject_id if relation else payload.dependent_id,
        "predicate": payload.predicate.value if relation else "DEPENDS_ON",
        "object_id": payload.object_id if relation else payload.dependency_id,
        "evidence_kind": payload.evidence_kind.value,
        "deterministic": payload.deterministic,
        "extractor_name": payload.extractor_name,
        "extractor_version": payload.extractor_version,
        "confidence": payload.confidence,
        "valid_from": timestamp(payload.valid_from),
        "asserted_valid_to": None if payload.valid_to is None else timestamp(payload.valid_to),
        "review_status": payload.review_status.value,
        "dependency_kind": None if relation else payload.dependency_kind.value,
        "requirement": None if relation else payload.requirement,
        "resolved_version": None if relation else payload.resolved_version,
        "recorded_from": timestamp(event.observed_at),
    }
    await tx.run(_ASSERTION, parameters=props)
    if not relation:
        await tx.run(
            _DEPENDENCY,
            parameters={"node_id": payload.dependency_id, "repository_id": repository_id},
        )
        await tx.run(
            _DEPENDS_ON,
            parameters={
                "event_id": str(event.event_id),
                "source_id": payload.assertion_id,
                "target_id": payload.dependency_id,
            },
        )
    scope.assertions.add(payload.assertion_id)


# --------------------------------------------------------------------------------------------
# Derived writes
# --------------------------------------------------------------------------------------------

_DROP_FILE: Final[LiteralString] = (
    "MATCH (n:File {file_id: $id}) OPTIONAL MATCH (n)-[r:HAS_REVISION|CURRENT_REVISION|IN_MODULE]->() "
    "DELETE r"
)
_FILE_ROW: Final[LiteralString] = (
    "MATCH (n:File {file_id: $id}) MATCH (fr:FileRevision {file_revision_id: $key}) "
    "CREATE (n)-[:HAS_REVISION {path: $path, module_id: $module_id, valid_from: $valid_from, "
    "valid_to: $valid_to, recorded_from: $recorded_from, recorded_to: $recorded_to}]->(fr)"
)
_FILE_CURRENT: Final[LiteralString] = (
    "MATCH (n:File {file_id: $id}) MATCH (fr:FileRevision {file_revision_id: $key}) "
    "CREATE (n)-[:CURRENT_REVISION]->(fr) WITH n "
    "OPTIONAL MATCH (m:Module {module_id: $module_id}) "
    "FOREACH (x IN CASE WHEN m IS NULL THEN [] ELSE [m] END | CREATE (n)-[:IN_MODULE]->(x))"
)
_FILE_FLAGS: Final[LiteralString] = (
    "MATCH (n:File {file_id: $id}) SET n.current = $current, n.current_path = $path, "
    "n.valid_to = $valid_to"
)
_DROP_SYMBOL: Final[LiteralString] = (
    "MATCH (n:Symbol {symbol_id: $id}) OPTIONAL MATCH (n)-[r:HAS_REVISION|CURRENT_REVISION]->() "
    "DELETE r"
)
_SYMBOL_ROW: Final[LiteralString] = (
    "MATCH (n:Symbol {symbol_id: $id}) MATCH (sr:SymbolRevision {symbol_revision_id: $key}) "
    "CREATE (n)-[:HAS_REVISION {valid_from: $valid_from, valid_to: $valid_to, "
    "recorded_from: $recorded_from, recorded_to: $recorded_to}]->(sr)"
)
_SYMBOL_CURRENT: Final[LiteralString] = (
    "MATCH (n:Symbol {symbol_id: $id}) MATCH (sr:SymbolRevision {symbol_revision_id: $key}) "
    "CREATE (n)-[:CURRENT_REVISION]->(sr)"
)
_SYMBOL_FLAGS: Final[LiteralString] = (
    "MATCH (n:Symbol {symbol_id: $id}) SET n.current = $current, n.valid_to = $valid_to"
)
_ASSERTION_FLAGS: Final[LiteralString] = (
    "MATCH (n:Assertion {assertion_id: $id}) SET n.current = $current, n.valid_to = $valid_to, "
    "n.recorded_to = $recorded_to, n.valid_intervals = $intervals"
)


def _row(item: Interval) -> dict[str, object]:
    return {
        "key": item.key,
        "valid_from": item.start.occurred_at,
        "recorded_from": item.recorded_from,
        "valid_to": None if item.end is None else item.end.occurred_at,
        "recorded_to": item.recorded_to,
    }


async def _write_file_state(tx: Neo4jTransaction, facts: Facts, file_id: str) -> None:
    intervals = derive_file(facts, file_id)
    await tx.run(_DROP_FILE, parameters={"id": file_id})
    for item in intervals:
        await tx.run(
            _FILE_ROW,
            parameters={"id": file_id, "path": item.extra[0], "module_id": item.extra[1]}
            | _row(item),
        )
    last = intervals[-1] if intervals else None
    current = last is not None and last.end is None
    if current and last is not None:
        await tx.run(
            _FILE_CURRENT,
            parameters={"id": file_id, "key": last.key, "module_id": last.extra[1]},
        )
    await tx.run(
        _FILE_FLAGS,
        parameters={
            "id": file_id,
            "current": current,
            "path": last.extra[0] if current and last is not None else None,
            "valid_to": None if last is None or last.end is None else last.end.occurred_at,
        },
    )


async def _write_symbol_state(tx: Neo4jTransaction, facts: Facts, symbol_id: str) -> None:
    intervals = derive_symbol(facts, symbol_id)
    await tx.run(_DROP_SYMBOL, parameters={"id": symbol_id})
    for item in intervals:
        await tx.run(_SYMBOL_ROW, parameters={"id": symbol_id} | _row(item))
    last = intervals[-1] if intervals else None
    current = last is not None and last.end is None
    if current and last is not None:
        await tx.run(_SYMBOL_CURRENT, parameters={"id": symbol_id, "key": last.key})
    await tx.run(
        _SYMBOL_FLAGS,
        parameters={
            "id": symbol_id,
            "current": current,
            "valid_to": None if last is None or last.end is None else last.end.occurred_at,
        },
    )


def _text(item: Interval) -> str:
    return f"{item.start.occurred_at}/{'' if item.end is None else item.end.occurred_at}"


async def _write_assertion_state(tx: Neo4jTransaction, facts: Facts, assertion_id: str) -> None:
    if assertion_id not in facts.assertions:
        return
    intervals = derive_assertion(facts, assertion_id)
    last = intervals[-1] if intervals else None
    current = is_current(intervals, facts.horizon)
    await tx.run(
        _ASSERTION_FLAGS,
        parameters={
            "id": assertion_id,
            "current": current,
            "valid_to": None if last is None or last.end is None else last.end.occurred_at,
            "recorded_to": None if last is None else last.recorded_to,
            "intervals": [_text(item) for item in intervals],
        },
    )


_DROP_EDGES: Final[LiteralString] = (
    "MATCH (s)-[r:CALLS|REFERENCES|IMPORTS|DEFINES]->() "
    "WHERE s.symbol_id IN $subjects OR s.file_id IN $subjects DELETE r"
)
_DROP_SUPERSESSIONS: Final[LiteralString] = (
    "MATCH (s:File)-[r:POSSIBLY_SUPERSEDES]->() WHERE s.file_id IN $subjects DELETE r"
)
_CURRENT_BY_SUBJECT: Final[LiteralString] = (
    "MATCH (a:Assertion {repository_id: $repository_id, family: 'relation'}) "
    "WHERE a.current = true AND a.subject_id IN $subjects "
    "RETURN properties(a) AS props"
)
_EDGES: Final[dict[str, LiteralString]] = {
    predicate: (
        "MATCH (s) WHERE (s:Symbol AND s.symbol_id = $subject_id) "
        "OR (s:File AND s.file_id = $subject_id) "
        f"MATCH (o:{label} {{{key}: $object_id}}) "
        f"CREATE (s)-[r:{predicate}]->(o) SET r += $props"
    )
    for predicate, label, key in (
        ("CALLS", "Symbol", "symbol_id"),
        ("REFERENCES", "Symbol", "symbol_id"),
        ("IMPORTS", "Symbol", "symbol_id"),
        ("DEFINES", "Symbol", "symbol_id"),
        (SUPERSEDES, "File", "file_id"),
    )
}


def _rank(props: dict[str, Any]) -> tuple[int, float]:
    return (_EVIDENCE_RANK.get(props["evidence_kind"], 1), float(props["confidence"]))


async def _write_edges(tx: Neo4jTransaction, repository_id: str, subjects: set[str]) -> None:
    """Replace the resolved edges of `subjects` from their current assertions."""
    if not subjects:
        return
    ordered = sorted(subjects)
    await tx.run(_DROP_EDGES, parameters={"subjects": ordered})
    await tx.run(_DROP_SUPERSESSIONS, parameters={"subjects": ordered})
    rows = (
        await tx.run(
            _CURRENT_BY_SUBJECT,
            parameters={"repository_id": repository_id, "subjects": ordered},
        )
    ).records
    for (subject, predicate, obj), props in resolve_edges([row["props"] for row in rows]).items():
        await tx.run(
            _EDGES[predicate],
            parameters={"subject_id": subject, "object_id": obj, "props": props},
        )


def resolve_edges(
    current: Iterable[dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """One resolved edge per (subject, predicate, object) among the current assertions.

    Evidence competes only within a triple: CALLS, IMPORTS and DEFINES are not functional, so a
    SCIP edge to `g` says nothing against a heuristic edge to `h`. Within a triple the best
    evidence (SCIP, then syntactic, then heuristic; then confidence) decides the edge; the lower
    ones are kept on their nodes and listed as `lower_evidence_assertion_ids`.
    """
    triples: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for props in current:
        if props["predicate"] in _EDGES and props.get("object_id") is not None:
            triples.setdefault(
                (props["subject_id"], props["predicate"], props["object_id"]), []
            ).append(props)
    edges: dict[tuple[str, str, str], dict[str, Any]] = {}
    for triple in sorted(triples):
        group = triples[triple]
        winner = min(group, key=lambda p: (-_rank(p)[0], -_rank(p)[1], p["assertion_id"]))
        edges[triple] = {
            "assertion_ids": sorted(p["assertion_id"] for p in group),
            "source_event_ids": sorted({e for p in group for e in p.get("source_event_ids", [])}),
            "source_event_id": winner.get("source_event_id"),
            "evidence_kind": winner["evidence_kind"],
            "deterministic": winner["deterministic"],
            "extractor_name": winner["extractor_name"],
            "extractor_version": winner["extractor_version"],
            "confidence": winner["confidence"],
            "valid_from": winner["valid_from"],
            "recorded_from": winner.get("recorded_from"),
            "review_status": winner["review_status"],
            "resolved": True,
            "resolved_assertion_id": winner["assertion_id"],
            "lower_evidence_assertion_ids": sorted(
                p["assertion_id"] for p in group if p is not winner
            ),
        }
    return edges


_SCOPE_OF_FILES: Final[LiteralString] = (
    "MATCH (fr:FileRevision) WHERE fr.file_id IN $files "
    "OPTIONAL MATCH (sr:SymbolRevision)-[:DEFINED_IN]->(fr) "
    "OPTIONAL MATCH (a:Assertion)-[:OBSERVED_IN]->(fr) "
    "RETURN collect(DISTINCT sr.symbol_id) AS symbols, collect(DISTINCT a.assertion_id) AS assertions"
)
_ASSERTIONS_OF_SYMBOLS: Final[LiteralString] = (
    "MATCH (a:Assertion {repository_id: $repository_id}) "
    "WHERE a.subject_id IN $symbols OR a.object_id IN $symbols "
    "RETURN collect(a.assertion_id) AS assertions"
)
_REPOSITORY_SCOPE: Final[LiteralString] = (
    "OPTIONAL MATCH (f:File {repository_id: $repository_id}) "
    "WITH collect(f.file_id) AS files "
    "OPTIONAL MATCH (s:Symbol {repository_id: $repository_id}) "
    "WITH files, collect(s.symbol_id) AS symbols "
    "OPTIONAL MATCH (a:Assertion {repository_id: $repository_id}) "
    "RETURN files, symbols, collect(a.assertion_id) AS assertions"
)
_SUBJECTS: Final[LiteralString] = (
    "MATCH (a:Assertion) WHERE a.assertion_id IN $ids RETURN collect(DISTINCT a.subject_id) AS subjects"
)


_BOUNDED: Final[LiteralString] = (
    "MATCH (a:Assertion {repository_id: $repository_id}) WHERE a.asserted_valid_to IS NOT NULL "
    "RETURN collect(a.assertion_id) AS assertions"
)


async def _expand(tx: Neo4jTransaction, repository_id: str, scope: _Scope) -> None:
    # `current` of an assertion with a declared `valid_to` depends on the newest run, which any
    # event can introduce: re-derive them whenever anything is projected.
    bounded = (await tx.run(_BOUNDED, parameters={"repository_id": repository_id})).records[0]
    scope.assertions |= set(bounded["assertions"])
    if scope.everything:
        row = (
            await tx.run(_REPOSITORY_SCOPE, parameters={"repository_id": repository_id})
        ).records[0]
        scope.files |= set(row["files"])
        scope.symbols |= set(row["symbols"])
        scope.assertions |= set(row["assertions"])
        return
    if scope.files:
        row = (await tx.run(_SCOPE_OF_FILES, parameters={"files": sorted(scope.files)})).records[0]
        scope.symbols |= set(row["symbols"])
        scope.assertions |= set(row["assertions"])
    if scope.symbols:
        row = (
            await tx.run(
                _ASSERTIONS_OF_SYMBOLS,
                parameters={"repository_id": repository_id, "symbols": sorted(scope.symbols)},
            )
        ).records[0]
        scope.assertions |= set(row["assertions"])


async def _derive(tx: Neo4jTransaction, repository_id: str, scope: _Scope) -> None:
    await _expand(tx, repository_id, scope)
    facts = await load_facts(
        tx,
        repository_id,
        files=scope.files,
        symbols=scope.symbols,
        assertions=scope.assertions,
    )
    for file_id in sorted(scope.files):
        await _write_file_state(tx, facts, file_id)
    for symbol_id in sorted(scope.symbols):
        await _write_symbol_state(tx, facts, symbol_id)
    for assertion_id in sorted(scope.assertions):
        await _write_assertion_state(tx, facts, assertion_id)
    subjects = {facts.assertions[a].subject_id for a in scope.assertions if a in facts.assertions}
    await _write_edges(tx, repository_id, subjects)


# --------------------------------------------------------------------------------------------
# as-of
# --------------------------------------------------------------------------------------------

type EntityKind = Literal["file", "symbol", "assertion"]


async def entity_as_of(
    tx: Neo4jTransaction,
    repository_id: str,
    kind: EntityKind,
    entity_id: str,
    *,
    valid_at: datetime,
    recorded_at: datetime | None = None,
) -> str | None:
    """The revision (file or symbol revision ID; for an assertion its own ID) current as of
    `valid_at` in valid time and `recorded_at` in transaction time, or `None`.

    Re-derives from the facts recorded by `recorded_at`, so it is independent of delivery order.
    """
    recorded = None if recorded_at is None else timestamp(recorded_at)
    facts = await load_facts(
        tx,
        repository_id,
        files=[entity_id] if kind == "file" else [],
        symbols=[entity_id] if kind == "symbol" else [],
        assertions=[entity_id] if kind == "assertion" else [],
        recorded_at=recorded,
    )
    if kind == "file":
        intervals = derive_file(facts, entity_id)
    elif kind == "symbol":
        intervals = derive_symbol(facts, entity_id)
    elif entity_id in facts.assertions:
        intervals = derive_assertion(facts, entity_id)
    else:
        return None
    found = interval_at(intervals, timestamp(valid_at))
    return None if found is None else found.key


# --------------------------------------------------------------------------------------------
# Projector
# --------------------------------------------------------------------------------------------


class CodeProjector:
    """Projects the `code.*` events into the temporal code graph."""

    name = "code"
    version = "1"

    def handles(self, event_type: str) -> bool:
        return event_type in CODE_EVENT_TYPES

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        await lock_event_nodes(tx, event)
        repository_id = _repository_of(event)
        scope = _Scope()
        if event.event_type in _RUN_TYPES:
            await _write_run_event(tx, event, repository_id, scope)
        else:
            await tx.run(_MERGE_REPOSITORY, parameters={"node_id": repository_id})
            await _write_assertion(tx, event, repository_id, scope)
        await _derive(tx, repository_id, scope)
