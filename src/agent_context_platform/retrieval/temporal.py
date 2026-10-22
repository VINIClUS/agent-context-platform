"""Bi-temporal reads of decisions and failures over the graph PLATFORM-038K projects.

Time. A decision is read at two instants. `valid_at` is when it must hold in the world
(`valid_from <= valid_at < end`); `recorded_at` is what the platform knew by then (only
recordings whose envelope `observed_at`, stored as `recorded_at`, is at or before it exist). Both
default to "now". They are not interchangeable: a decision recorded late is invisible at an earlier
`recorded_at` even though its `valid_from` is earlier still.

Decisions. The graph keeps one immutable `(:DecisionVersion)` per recording, reached with
`(:Decision)-[:HAS_VERSION]->`. This module loads the versions of a scope with bounded Cypher and
hands them to the PURE helpers of `projection.projectors.knowledge` (`decision_states`,
`supersession_edges`, `active_decisions`), so supersession, reopening by a rejected superseder and
`valid_to` expiry agree with the projector by construction. Nothing here re-derives them in
Cypher. `active_decisions` filters on a subject token; scoping is done in Cypher on the version's
`project_id`/`repository_id`, so every version is presented to it with one fixed subject. A
recording whose event carried no context has no scope and is invisible to every scoped read.
A decision recorded in two scopes is two sets of versions: each scope sees only its own.

`decisions(..., include_history=True)` also returns the decisions that held and have ended at
`valid_at`, each marked `historical` with its reason: `superseded` (by a named decision),
`expired` (its own `valid_to`) or `superseded_undated` (recorded as `superseded`, already in
force at `valid_at`, but no known superseder says when it stopped). Decisions that were only
`proposed` or `rejected`, and ones that have not begun at `valid_at`, are neither active nor
history and are not returned. `decision_history` follows the SUPERSEDES chain both ways, as known
now, from the same scoped versions, with every recording of each link. It is anchored on the
decision asked about: its own versions are read first (by its ID), then the chain is walked one
link at a time, reading only the versions of the decisions on it and of their superseders, so the
number of other decisions in the scope never hides the one asked about.

Failures. The graph keeps one immutable `(:FailureObservation)` per failure event, reached with
`(:Failure)-[:HAS_OBSERVATION]->`: the occurrence time (`valid_from`), the envelope `recorded_at`,
the fingerprint facts, scope, `source_event_ids` and the session, test run and CI run it names.
`failures` selects observations with `valid_from <= valid_at AND recorded_at <= recorded_at`, one
by one, and describes each failure from those observations only: its times, its events and where
it was seen. The design has no FIXED_BY edge, so a fix is derived from projected data only, for CI
runs: a failure observed in a CI run is `resolved` when a LATER CI run of the same provider,
workflow and job, in the same project and repository, has status `success` and a `commit_id` or
`snapshot_id` (the commit or snapshot it validated). "Later" is `(completed_at, ci_run_id)`
order; the graph holds no commit order, so `commit_changed` says whether the passing run's commit
differs from the failing one (`None` when either is unknown). Resolution follows the LATEST
visible failing run of each target: a failure that recurs after an earlier fix is `unresolved`
until a pass follows that latest run, and the path reported is for the latest occurrence. The
pass also vouches only for what happened before it: a failure observed again after the passing
run's completion (its occurrence is its visible CI run's completion, else its own valid time, so
a session, a test run or a CI run not completed at the cut counts) is `unresolved`. A rerun that passes on the SAME
commit is `resolved` with `commit_changed=False`; the composer must read the flag. The earliest
such run wins. The path is Failure -> failing CI run -> passing CI run -> validated commit or
snapshot. A run, failing or passing, counts only in scope, once its own `recorded_at` (the
earliest envelope `observed_at` that reported it) is at or before the cut, and once its
`completed_at` is at or before `valid_at`; a run still to complete is omitted from `observed_in`
(and so is no candidate for a fix path), never shown with an unknown status; an identity-only stub
has none and is never visible at a past cut.
Gaps, reported rather than guessed:
- `TestRunCompletedV1` carries no suite or target identifier (SDK gap FU-77), so the same test
  cannot be identified and a failure observed only in test runs is `unsupported`. `framework`
  is never used as a target.
- A failure observed in no CI or test run in scope is `unknown`.
- Run nodes keep the NEWEST observation of a run; a re-observation that changed its status is not
  versioned.

Rebuild required. The history nodes are complete only in a graph that the current knowledge and
quality projector versions projected from the START of the ledger: a version deployed over a
populated graph sees only the events delivered after it (delivered outbox rows are never claimed
again), and an entity that receives one new event then has some of its history but not all of it.
That cannot be told from the entities, so every read first checks, in its own transaction, the
projection history marker (`projection.schema`, recorded only by a verified rebuild or by a worker
that started on an empty graph with nothing delivered) and raises `TemporalProjectionOutdated`
unless it names both current versions; rebuild the graph (README, `agent-context projection
rebuild`). `projection status`/`verify` report the same marker.

Scope. Every call takes a `TemporalScope` with a `project_id` or a `repository_id` (both given:
both must match). Every node a query touches is matched on those properties, so a node without
scope, or of another scope, is never returned.

Bounds. Every query carries a `LIMIT`: 5,000 decision versions and 5,000 failure observations per
request (so no unbounded collection exists, history included). `truncated=True` says a cap was
hit. For `decisions` the cap is on versions: a decision the cut goes through is left out rather
than answered from part of its recordings, and the answer is then only as complete as the
versions read. Failure observations are read newest first, so the caps keep the newest
observations and failures (a failure the observation cut goes through is left out too). Results are limited to 500 decisions and
500 failures; a history lists at most 100 links per direction (depth at most 50) and the newest
100 recordings of each: per decision at most 101 versions are read (newest first, the extra one
only tells that more exist), at most 101 superseders per step, and 5,000 versions in all. One read
transaction per request carries the driver timeout, and the same
deadline bounds the client with `asyncio.timeout`; expiry raises `RetrievalDeadlineExceeded`. All
Cypher is static text and all input goes in as parameters.

Evidence for PLATFORM-043. Every item carries `source_event_ids` and its intervals as UTC
datetimes. A decision's are the recording events of its known versions; a failure's are the events of
its visible observations (which also covers a failure seen in no run); a run's is the event of its newest observation, read
from its `result_order`. The composer must resolve these through the ledger
(`event_content_refs`) to a content reference, or choose a metadata-only form, as for the graph
reads. A decision's `content_id` is on the `Decision` node and not on its versions, so it is not
returned.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, LiteralString, Self, TypeVar

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncManagedTransaction, Record, unit_of_work
from neo4j.exceptions import ConfigurationError, Neo4jError

from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors.code import timestamp
from agent_context_platform.projection.projectors.knowledge import (
    DecisionVersion,
    active_decisions,
    decision_states,
    supersession_edges,
)
from agent_context_platform.projection.schema import (
    REQUIRED_HISTORY,
    HistoryState,
    history_state,
    read_projection_history,
)
from agent_context_platform.retrieval.graph import (
    DEFAULT_DEADLINE_SECONDS,
    RetrievalDeadlineExceeded,
    RetrievalError,
)
from agent_context_platform.settings import Neo4jSettings

MAX_VERSIONS: Final = 5000
MAX_DECISIONS: Final = 500
MAX_FAILURES: Final = 500
MAX_CHAIN: Final = 100  # links per direction
MAX_DEPTH: Final = 50
MAX_ENTRY_VERSIONS: Final = 100  # recordings listed per history entry (the newest)
MAX_OBSERVATIONS: Final = 5000

# `active_decisions` matches a subject token; scope is enforced in Cypher, so every version is
# presented to it with this one subject.
_SUBJECT: Final = "scope"

ResultT = TypeVar("ResultT")


class TemporalRequestError(RetrievalError, ValueError):
    """The request is malformed (no scope, a naive time, a non-positive deadline)."""


class TemporalDecisionNotFound(RetrievalError, LookupError):
    """The decision does not exist in the scope."""


class TemporalProjectionOutdated(RetrievalError):
    """The graph lacks complete history for the current projector versions; rebuild it (README)."""


class DecisionStanding(StrEnum):
    ACTIVE = "active"
    HISTORICAL = "historical"


class HistoricalReason(StrEnum):
    SUPERSEDED = "superseded"
    EXPIRED = "expired"
    SUPERSEDED_UNDATED = "superseded_undated"


class ChainRelation(StrEnum):
    SELF = "self"
    EARLIER = "earlier"  # superseded, directly or not, by the decision asked about
    LATER = "later"  # supersedes the decision asked about, directly or not


class RunKind(StrEnum):
    SESSION = "session"
    TEST_RUN = "test_run"
    CI_RUN = "ci_run"


class FixStatus(StrEnum):
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class TemporalScope:
    """The project and/or repository every node of a request must belong to."""

    project_id: str | None = None
    repository_id: str | None = None

    def __post_init__(self) -> None:
        for value in (self.project_id, self.repository_id):
            if value is not None and not value.strip():
                raise TemporalRequestError("project_id and repository_id must not be blank")
        if self.project_id is None and self.repository_id is None:
            raise TemporalRequestError("a project_id or a repository_id is required")

    def parameters(self) -> dict[str, object]:
        return {"project_id": self.project_id, "repository_id": self.repository_id}


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    """A decision as known at `recorded_at`, active or historical at `valid_at`."""

    decision_id: str
    standing: DecisionStanding
    reason: HistoricalReason | None
    superseded_by: str | None
    status: str
    subjects: tuple[str, ...]
    valid_from: datetime
    valid_to: datetime | None
    effective_valid_to: datetime | None
    supersedes_id: str | None
    recorded_at: datetime
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DecisionsResult:
    scope: TemporalScope
    valid_at: datetime
    recorded_at: datetime
    decisions: tuple[DecisionRecord, ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class RecordedVersion:
    """One recording of a decision."""

    event_id: str
    status: str
    valid_from: datetime
    valid_to: datetime | None
    supersedes_id: str | None
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class HistoryEntry:
    decision_id: str
    relation: ChainRelation
    distance: int
    status: str
    valid_from: datetime
    valid_to: datetime | None
    effective_valid_to: datetime | None
    superseded_at: datetime | None
    supersedes_id: str | None
    recorded_at: datetime
    versions: tuple[RecordedVersion, ...]
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DecisionHistory:
    scope: TemporalScope
    decision_id: str
    entries: tuple[HistoryEntry, ...]  # earlier links, the decision, later links
    truncated: bool


@dataclass(frozen=True, slots=True)
class ObservedRun:
    kind: RunKind
    run_id: str
    status: str | None
    workflow: str | None
    job: str | None
    completed_at: datetime | None
    commit_id: str | None
    snapshot_id: str | None
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FixPath:
    """Failure -> failing CI run -> later passing CI run -> validated commit or snapshot."""

    failure_id: str
    failing_run_id: str
    passing_run_id: str
    workflow: str
    job: str
    failing_completed_at: datetime
    passing_completed_at: datetime
    validated_commit_id: str | None
    validated_snapshot_id: str | None
    commit_changed: bool | None
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FixResolution:
    status: FixStatus
    reason: str | None = None
    path: FixPath | None = None


@dataclass(frozen=True, slots=True)
class FailureRecord:
    failure_id: str
    component: str | None
    operation: str | None
    error_class: str | None
    fingerprint_sha256: str | None
    valid_from: datetime
    recorded_at: datetime
    observed_in: tuple[ObservedRun, ...]
    resolution: FixResolution
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FailuresResult:
    scope: TemporalScope
    valid_at: datetime
    recorded_at: datetime
    failures: tuple[FailureRecord, ...]
    truncated: bool


# --------------------------------------------------------------------------------------------
# Static Cypher
# --------------------------------------------------------------------------------------------


def _in_scope(alias: LiteralString) -> LiteralString:
    """Match `alias` on the scope parameters; a node with no such property never matches."""
    return (
        f"($project_id IS NULL OR {alias}.project_id = $project_id) "
        f"AND ($repository_id IS NULL OR {alias}.repository_id = $repository_id)"
    )


_VERSIONS: Final[LiteralString] = (
    "MATCH (v:DecisionVersion) WHERE " + _in_scope("v") + " AND v.recorded_at <= $recorded_at "
    "RETURN v.event_id AS event_id, v.decision_id AS decision_id, v.status AS status, "
    "v.subjects AS subjects, v.valid_from AS valid_from, v.valid_to AS valid_to, "
    "v.supersedes_id AS supersedes_id, v.recorded_at AS recorded_at, "
    "v.state_order AS state_order "
    "ORDER BY v.decision_id, v.state_order, v.event_id LIMIT $limit"
)

# `decision_history`: the newest `$per_decision` versions of each decision asked for, reached from
# its `Decision` node (so by its unique key, whatever else the scope holds), within `$limit` rows.
_DECISION_VERSIONS: Final[LiteralString] = (
    "UNWIND $decision_ids AS decision_id CALL (decision_id) { "
    "MATCH (:Decision {decision_id: decision_id})-[:HAS_VERSION]->(v:DecisionVersion) "
    "WHERE " + _in_scope("v") + " AND v.recorded_at <= $recorded_at "
    "RETURN v ORDER BY v.state_order DESC, v.event_id DESC LIMIT $per_decision } "
    "RETURN v.event_id AS event_id, v.decision_id AS decision_id, v.status AS status, "
    "v.subjects AS subjects, v.valid_from AS valid_from, v.valid_to AS valid_to, "
    "v.supersedes_id AS supersedes_id, v.recorded_at AS recorded_at, "
    "v.state_order AS state_order LIMIT $limit"
)
# The decisions with a recording in scope that supersedes one of `$decision_ids`: the only ones
# whose current version can close them or extend the chain past them.
_SUPERSEDERS: Final[LiteralString] = (
    "MATCH (v:DecisionVersion) WHERE v.supersedes_id IN $decision_ids "
    "AND v.status IN ['accepted', 'superseded'] AND v.recorded_at <= $recorded_at "
    "AND " + _in_scope("v") + " "
    "RETURN DISTINCT v.decision_id AS decision_id ORDER BY decision_id LIMIT $limit"
)

_OBSERVATIONS: Final[LiteralString] = (
    "MATCH (o:FailureObservation) WHERE " + _in_scope("o") + " "
    "AND o.valid_from <= $valid_at AND o.recorded_at <= $recorded_at "
    "RETURN o.event_id AS event_id, o.failure_id AS failure_id, o.component AS component, "
    "o.operation AS operation, o.error_class AS error_class, "
    "o.fingerprint_sha256 AS fingerprint_sha256, o.valid_from AS valid_from, "
    "o.recorded_at AS recorded_at, o.session_id AS session_id, o.test_run_id AS test_run_id, "
    "o.ci_run_id AS ci_run_id "
    "ORDER BY o.valid_from DESC, o.failure_id DESC, o.event_id DESC LIMIT $limit"
)
# A run is visible only in scope, once the platform had recorded it (a stub has no `recorded_at`
# and never is) and once it had completed: a run still to complete at `valid_at` is omitted from
# `observed_in` and is no candidate for a fix path.
_CI_RUNS: Final[LiteralString] = (
    "UNWIND $run_ids AS run_id MATCH (r:CIRun {ci_run_id: run_id}) WHERE " + _in_scope("r") + " "
    "AND r.recorded_at <= $recorded_at AND r.completed_at <= $valid_at "
    "RETURN r.ci_run_id AS run_id, r.status AS status, r.workflow AS workflow, r.job AS job, "
    "r.provider AS provider, r.project_id AS project_id, r.repository_id AS repository_id, "
    "r.completed_at AS completed_at, r.commit_id AS commit_id, r.snapshot_id AS snapshot_id, "
    "r.result_order AS result_order"
)
_TEST_RUNS: Final[LiteralString] = (
    "UNWIND $run_ids AS run_id MATCH (r:TestRun {test_run_id: run_id}) WHERE "
    + _in_scope("r")
    + " AND r.recorded_at <= $recorded_at AND r.completed_at <= $valid_at "
    "RETURN r.test_run_id AS run_id, r.status AS status, r.completed_at AS completed_at, "
    "r.commit_id AS commit_id, r.snapshot_id AS snapshot_id, r.result_order AS result_order"
)

# For each failing CI run, the earliest later passing run of the same target: one row per failing
# run (the subquery keeps only the first), never a collection of every later pass. The candidates
# are still found by a CIRun label scan, as no index covers a CI target.
_NEXT_PASS: Final[LiteralString] = (
    "UNWIND $run_ids AS run_id "
    "MATCH (r:CIRun {ci_run_id: run_id}) WHERE " + _in_scope("r") + " "
    "AND r.recorded_at <= $recorded_at AND r.completed_at <= $valid_at "
    "AND r.workflow IS NOT NULL AND r.job IS NOT NULL "
    "CALL (r) { OPTIONAL MATCH (p:CIRun) WHERE " + _in_scope("p") + " "
    "AND p.status = 'success' AND p.provider = r.provider AND p.workflow = r.workflow "
    "AND p.job = r.job "
    "AND coalesce(p.project_id, '') = coalesce(r.project_id, '') "
    "AND coalesce(p.repository_id, '') = coalesce(r.repository_id, '') "
    "AND (p.commit_id IS NOT NULL OR p.snapshot_id IS NOT NULL) "
    "AND (p.completed_at > r.completed_at "
    "OR (p.completed_at = r.completed_at AND p.ci_run_id > r.ci_run_id)) "
    "AND p.completed_at <= $valid_at AND p.recorded_at <= $recorded_at "
    "RETURN p ORDER BY p.completed_at, p.ci_run_id LIMIT 1 } "
    "RETURN r.ci_run_id AS failing_id, r.workflow AS workflow, r.job AS job, "
    "r.completed_at AS failing_completed_at, r.commit_id AS failing_commit_id, "
    "r.result_order AS failing_order, p.ci_run_id AS passing_id, "
    "p.completed_at AS passing_completed_at, p.commit_id AS commit_id, "
    "p.snapshot_id AS snapshot_id, p.result_order AS passing_order"
)


# --------------------------------------------------------------------------------------------
# Service
# --------------------------------------------------------------------------------------------


class TemporalService:
    """Typed read-only bi-temporal queries over projected decisions and failures."""

    def __init__(
        self,
        driver: AsyncDriver,
        *,
        database: str,
        default_deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
        owns_driver: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if default_deadline_seconds <= 0:
            raise TemporalRequestError("deadline must be positive")
        self._driver = driver
        self._database = database
        self._default_deadline = default_deadline_seconds
        self._owns_driver = owns_driver
        self._clock = clock

    @classmethod
    def from_settings(
        cls,
        settings: Neo4jSettings,
        *,
        default_deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> Self:
        """Open a driver the service owns (closed by `close`), like `GraphTraversalService`."""
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
            notifications_min_severity="OFF",
        )
        return cls(
            driver,
            database=settings.database,
            default_deadline_seconds=default_deadline_seconds,
            owns_driver=True,
            clock=clock,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_driver:
            await self._driver.close()

    # --- operations ---

    async def decisions(
        self,
        scope: TemporalScope,
        valid_at: datetime | None = None,
        recorded_at: datetime | None = None,
        include_history: bool = False,
        *,
        deadline_seconds: float | None = None,
    ) -> DecisionsResult:
        """Decisions active at `(valid_at, recorded_at)`; with history, the ended ones too."""
        now = self._clock()
        valid = _aware(valid_at or now, "valid_at")
        recorded = _aware(recorded_at or now, "recorded_at")
        rows, truncated = await self._read(
            deadline_seconds, lambda tx: _load_versions(tx, scope, recorded)
        )
        result, cut = _classify(rows, valid, recorded, include_history)
        return DecisionsResult(scope, valid, recorded, result, truncated or cut)

    async def decision_history(
        self,
        decision_id: str,
        scope: TemporalScope,
        *,
        deadline_seconds: float | None = None,
    ) -> DecisionHistory:
        """The SUPERSEDES chain around a decision, both ways, with intervals and recordings.

        Built from the scope's recorded versions with the same pure helpers as `decisions`, as
        known now, so a decision recorded in two scopes shows only the recordings of the scope
        asked about. The decision is read first, by its ID, then the chain from it (module
        docstring, "Bounds"), so it is found however many other versions the scope holds.
        """
        if not decision_id.strip():
            raise TemporalRequestError("decision_id is required")
        now = _aware(self._clock(), "now")
        entries, truncated = await self._read(
            deadline_seconds, lambda tx: _chain_history(tx, scope, decision_id, now)
        )
        return DecisionHistory(scope, decision_id, entries, truncated)

    async def failures(
        self,
        scope: TemporalScope,
        valid_at: datetime | None = None,
        recorded_at: datetime | None = None,
        include_resolved: bool = False,
        *,
        deadline_seconds: float | None = None,
    ) -> FailuresResult:
        """Failures observed by `valid_at` and recorded by `recorded_at`, with their fix paths.

        Resolved failures are left out unless `include_resolved`; `unresolved`, `unsupported`
        and `unknown` ones are always returned.
        """
        now = self._clock()
        valid = _aware(valid_at or now, "valid_at")
        recorded = _aware(recorded_at or now, "recorded_at")
        records, truncated = await self._read(
            deadline_seconds, lambda tx: _failures(tx, scope, valid, recorded)
        )
        if not include_resolved:
            records = tuple(r for r in records if r.resolution.status is not FixStatus.RESOLVED)
        return FailuresResult(scope, valid, recorded, records, truncated)

    # --- transaction boundary ---

    async def _read(
        self,
        deadline_seconds: float | None,
        work: Callable[[Neo4jTransaction], Awaitable[ResultT]],
    ) -> ResultT:
        """Run `work` after checking, in the same transaction, that the history is complete.

        Checked on every read, not once per instance: an in-place rebuild deletes the marker
        before it wipes anything and records it again only after verifying its replay, and
        nothing is served in between. Neo4j reads are read-committed, not a snapshot, so the
        marker is checked again AFTER the data was read: a wipe that started meanwhile deleted
        it first, and the answer is refused rather than served from a partly wiped graph.
        """

        async def guarded(tx: Neo4jTransaction) -> ResultT:
            await _require_history(tx)
            result = await work(tx)
            await _require_history(tx)
            return result

        return await self._run(deadline_seconds, guarded)

    async def _run(
        self,
        deadline_seconds: float | None,
        work: Callable[[Neo4jTransaction], Awaitable[ResultT]],
    ) -> ResultT:
        deadline = self._default_deadline if deadline_seconds is None else deadline_seconds
        if deadline <= 0:
            raise TemporalRequestError("deadline must be positive")

        async def execute(transaction: AsyncManagedTransaction) -> ResultT:
            return await work(Neo4jTransaction(transaction))

        async def attempt() -> ResultT:
            async with self._driver.session(database=self._database) as session:
                return await session.execute_read(unit_of_work(timeout=deadline)(execute))

        try:
            async with asyncio.timeout(deadline):
                return await attempt()
        except TimeoutError as error:
            raise RetrievalDeadlineExceeded("temporal request exceeded its deadline") from error
        except Neo4jError as error:
            if "TransactionTimedOut" in (error.code or ""):
                raise RetrievalDeadlineExceeded("temporal request exceeded its deadline") from error
            raise


async def _require_history(tx: Neo4jTransaction) -> None:
    state, missing = history_state(await read_projection_history(tx), REQUIRED_HISTORY)
    if state is not HistoryState.COMPLETE:
        raise TemporalProjectionOutdated(
            f"the graph has no complete history for {', '.join(missing)} ({state.value}): "
            "run `agent-context projection rebuild` (README, rebuild required on upgrade) "
            "before reading it"
        )


def _aware(moment: datetime, name: str) -> datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise TemporalRequestError(f"{name} must be timezone-aware")
    return moment.astimezone(UTC)


def _parse(value: object) -> datetime:
    return datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def _parse_optional(value: object) -> datetime | None:
    return None if value is None else _parse(value)


def _event_of(order: object) -> tuple[str, ...]:
    """The event of a run's newest observation: `result_order` is `<occurred_at>|<event_id>`."""
    return () if order is None else (str(order).split("|", 1)[-1],)


# --------------------------------------------------------------------------------------------
# Decisions
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _VersionRow:
    version: DecisionVersion
    event_id: str
    subjects: tuple[str, ...]


async def _load_versions(
    tx: Neo4jTransaction, scope: TemporalScope, recorded_at: datetime
) -> tuple[list[_VersionRow], bool]:
    records = (
        await tx.run(
            _VERSIONS,
            parameters={
                **scope.parameters(),
                "recorded_at": timestamp(recorded_at),
                "limit": MAX_VERSIONS + 1,
            },
        )
    ).records
    if len(records) <= MAX_VERSIONS:
        return _version_rows(records), False
    # The cut may go through one decision's recordings (the one the row after it belongs to):
    # answering from part of them could show an older status, so that decision is left out.
    partial = records[MAX_VERSIONS]["decision_id"]
    kept = [record for record in records[:MAX_VERSIONS] if record["decision_id"] != partial]
    return _version_rows(kept), True


def _classify(
    rows: Sequence[_VersionRow], valid_at: datetime, recorded_at: datetime, include_history: bool
) -> tuple[tuple[DecisionRecord, ...], bool]:
    versions = [row.version for row in rows]
    events: dict[str, list[str]] = {}
    subjects: dict[tuple[str, str], tuple[str, ...]] = {}
    for row in rows:
        events.setdefault(row.version.decision_id, []).append(row.event_id)
        subjects[(row.version.decision_id, row.version.order)] = row.subjects
    active = set(
        active_decisions(versions, scope=_SUBJECT, valid_at=valid_at, recorded_at=recorded_at)
    )
    records: list[DecisionRecord] = []
    for decision_id, state in decision_states(versions, recorded_at=recorded_at).items():
        version = state.version
        reason: HistoricalReason | None = None
        if decision_id in active:
            standing = DecisionStanding.ACTIVE
        elif not include_history:
            continue
        elif (
            version.status == "superseded" and state.end is None and version.valid_from <= valid_at
        ):
            standing, reason = DecisionStanding.HISTORICAL, HistoricalReason.SUPERSEDED_UNDATED
        elif state.ended_by(valid_at):
            standing = DecisionStanding.HISTORICAL
            by_successor = state.closed_at is not None and state.closed_at == state.end
            reason = HistoricalReason.SUPERSEDED if by_successor else HistoricalReason.EXPIRED
        else:
            continue
        records.append(
            DecisionRecord(
                decision_id=decision_id,
                standing=standing,
                reason=reason,
                superseded_by=state.superseded_by
                if reason is HistoricalReason.SUPERSEDED
                else None,
                status=version.status,
                subjects=subjects[(decision_id, version.order)],
                valid_from=version.valid_from,
                valid_to=version.valid_to,
                effective_valid_to=state.end,
                supersedes_id=version.supersedes_id,
                recorded_at=version.recorded_at,
                source_event_ids=tuple(sorted(events[decision_id])),
            )
        )
    records.sort(key=lambda item: (item.standing is not DecisionStanding.ACTIVE, item.decision_id))
    return tuple(records[:MAX_DECISIONS]), len(records) > MAX_DECISIONS


def _recorded(row: _VersionRow) -> RecordedVersion:
    version = row.version
    return RecordedVersion(
        event_id=row.event_id,
        status=version.status,
        valid_from=version.valid_from,
        valid_to=version.valid_to,
        supersedes_id=version.supersedes_id,
        recorded_at=version.recorded_at,
    )


def _version_rows(records: Sequence[Record]) -> list[_VersionRow]:
    return [
        _VersionRow(
            DecisionVersion(
                decision_id=str(record["decision_id"]),
                subjects=(_SUBJECT,),
                status=str(record["status"]),
                valid_from=_parse(record["valid_from"]),
                valid_to=_parse_optional(record["valid_to"]),
                recorded_at=_parse(record["recorded_at"]),
                supersedes_id=None
                if record["supersedes_id"] is None
                else str(record["supersedes_id"]),
                order=str(record["state_order"]),
            ),
            str(record["event_id"]),
            tuple(str(item) for item in record["subjects"] or ()),
        )
        for record in records
    ]


class _Chain:
    """The versions `decision_history` reads, one bounded query at a time, within one budget.

    `rows` holds, per decision read, its newest versions (at most `MAX_ENTRY_VERSIONS + 1`, so the
    current one is always among them). `truncated` is set whenever a cap may have hidden a
    version, a superseder or a link.
    """

    def __init__(self, tx: Neo4jTransaction, scope: TemporalScope, recorded_at: datetime) -> None:
        self._tx = tx
        self._scope = scope
        self._recorded_at = recorded_at
        self._read = 0
        self._asked: set[str] = set()  # decisions whose superseders were read
        self.rows: dict[str, list[_VersionRow]] = {}
        self.truncated = False

    async def load(self, decision_ids: Sequence[str]) -> None:
        wanted = sorted({item for item in decision_ids if item not in self.rows})
        budget = MAX_VERSIONS - self._read
        if not wanted:
            return
        if budget <= 0:
            self.truncated = True
            return
        records = (
            await self._tx.run(
                _DECISION_VERSIONS,
                parameters={
                    **self._scope.parameters(),
                    "recorded_at": timestamp(self._recorded_at),
                    "decision_ids": wanted,
                    "per_decision": MAX_ENTRY_VERSIONS + 1,
                    "limit": budget + 1,
                },
            )
        ).records
        if len(records) > budget:
            records, self.truncated = records[:budget], True
        self._read += len(records)
        for row in _version_rows(records):
            self.rows.setdefault(row.version.decision_id, []).append(row)

    async def superseders(self, decision_ids: Sequence[str]) -> list[str]:
        """Read the decisions that have a superseding recording of any of `decision_ids`."""
        wanted = sorted({item for item in decision_ids if item not in self._asked})
        if not wanted:
            return []
        self._asked.update(wanted)
        records = (
            await self._tx.run(
                _SUPERSEDERS,
                parameters={
                    **self._scope.parameters(),
                    "recorded_at": timestamp(self._recorded_at),
                    "decision_ids": wanted,
                    "limit": MAX_CHAIN + 1,
                },
            )
        ).records
        if len(records) > MAX_CHAIN:
            records, self.truncated = records[:MAX_CHAIN], True
        found = [str(record["decision_id"]) for record in records]
        await self.load(found)
        return [item for item in found if item in self.rows]

    def target(self, decision_id: str) -> str | None:
        """The decision `decision_id`'s CURRENT version supersedes, when it is read and asserts it."""
        states = decision_states(
            [row.version for row in self.rows.get(decision_id, ())], recorded_at=self._recorded_at
        )
        return next((target for _, target in supersession_edges(states)), None)


async def _chain_history(
    tx: Neo4jTransaction, scope: TemporalScope, decision_id: str, recorded_at: datetime
) -> tuple[tuple[HistoryEntry, ...], bool]:
    """Anchor on `decision_id`, then walk its SUPERSEDES chain both ways, one link at a time.

    Earlier links follow the supersession each current version asserts (one per decision); later
    links are the decisions whose current version supersedes the previous level. The superseders
    of every entry are read too, because they close it. Entries are cut at `MAX_DEPTH` and at
    `MAX_CHAIN` per direction, nearest first.
    """
    chain = _Chain(tx, scope, recorded_at)
    await chain.load([decision_id])
    if decision_id not in chain.rows:
        raise TemporalDecisionNotFound(decision_id)
    seen = {decision_id}
    earlier: dict[str, int] = {}
    current = decision_id
    while (target := chain.target(current)) is not None and target not in seen:
        await chain.load([target])
        if target not in chain.rows:  # a decision no recording of this scope has reached yet
            break
        if len(earlier) + 1 > min(MAX_DEPTH, MAX_CHAIN):
            chain.truncated = True
            break
        seen.add(target)
        earlier[target] = len(earlier) + 1
        current = target
    later: dict[str, int] = {}
    frontier, distance = [decision_id], 0
    while frontier:
        level = sorted(
            item
            for item in await chain.superseders(frontier)
            if item not in seen and chain.target(item) in frontier
        )
        if level and (distance + 1 > MAX_DEPTH or len(later) + len(level) > MAX_CHAIN):
            chain.truncated = True
            level = [] if distance + 1 > MAX_DEPTH else level[: MAX_CHAIN - len(later)]
        distance += 1
        seen.update(level)
        later.update((item, distance) for item in level)
        frontier = level
    # Whatever closes an entry is one of its superseders: read them for the earlier entries (and
    # any later entry whose own superseders the walk stopped before).
    await chain.superseders([*earlier, *later])
    states = decision_states(
        [row.version for rows in chain.rows.values() for row in rows], recorded_at=recorded_at
    )
    truncated = chain.truncated

    def entry(item: str, relation: ChainRelation, distance: int) -> HistoryEntry:
        nonlocal truncated
        state = states[item]
        mine = sorted(chain.rows[item], key=lambda r: (r.version.order, r.event_id))
        if len(mine) > MAX_ENTRY_VERSIONS:
            mine, truncated = mine[-MAX_ENTRY_VERSIONS:], True
        listed = tuple(_recorded(row) for row in mine)
        return HistoryEntry(
            decision_id=item,
            relation=relation,
            distance=distance,
            status=state.version.status,
            valid_from=state.version.valid_from,
            valid_to=state.version.valid_to,
            effective_valid_to=state.end,
            superseded_at=state.closed_at,
            supersedes_id=state.version.supersedes_id,
            recorded_at=state.version.recorded_at,
            versions=listed,
            source_event_ids=tuple(sorted(version.event_id for version in listed)),
        )

    before = [entry(item, ChainRelation.EARLIER, d) for item, d in reversed(earlier.items())]
    after = [
        entry(item, ChainRelation.LATER, d)
        for item, d in sorted(later.items(), key=lambda pair: (pair[1], pair[0]))
    ]
    return (*before, entry(decision_id, ChainRelation.SELF, 0), *after), truncated


# --------------------------------------------------------------------------------------------
# Failures
# --------------------------------------------------------------------------------------------


@dataclass(slots=True)
class _Seen:
    """One failure as the observations visible at the cut describe it."""

    rows: list[Record]


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _fix_path(failure_id: str, record: Record) -> FixPath:
    failing_commit = record["failing_commit_id"]
    passing_commit = record["commit_id"]
    return FixPath(
        failure_id=failure_id,
        failing_run_id=str(record["failing_id"]),
        passing_run_id=str(record["passing_id"]),
        workflow=str(record["workflow"]),
        job=str(record["job"]),
        failing_completed_at=_parse(record["failing_completed_at"]),
        passing_completed_at=_parse(record["passing_completed_at"]),
        validated_commit_id=_text(passing_commit),
        validated_snapshot_id=_text(record["snapshot_id"]),
        commit_changed=(
            None
            if failing_commit is None or passing_commit is None
            else failing_commit != passing_commit
        ),
        source_event_ids=tuple(
            sorted({*_event_of(record["failing_order"]), *_event_of(record["passing_order"])})
        ),
    )


def _occurrence(row: Record, ci: dict[str, Record]) -> datetime:
    """When an observation happened: its visible CI run's completion, else its own valid time."""
    run = ci.get(str(row["ci_run_id"])) if row["ci_run_id"] else None
    if run is not None and run["completed_at"] is not None:
        return _parse(run["completed_at"])
    return _parse(row["valid_from"])


def _occurred(run: ObservedRun) -> tuple[datetime, str]:
    return (run.completed_at or datetime.min.replace(tzinfo=UTC), run.run_id)


def _run(kind: RunKind, run_id: str, record: Record | None) -> ObservedRun:
    if record is None:  # a session: only the observation names it
        return ObservedRun(kind, run_id, None, None, None, None, None, None, ())
    return ObservedRun(
        kind=kind,
        run_id=run_id,
        status=_text(record["status"]),
        workflow=_text(record.get("workflow")),
        job=_text(record.get("job")),
        completed_at=_parse_optional(record["completed_at"]),
        commit_id=_text(record["commit_id"]),
        snapshot_id=_text(record["snapshot_id"]),
        source_event_ids=_event_of(record["result_order"]),
    )


async def _failures(
    tx: Neo4jTransaction, scope: TemporalScope, valid_at: datetime, recorded_at: datetime
) -> tuple[tuple[FailureRecord, ...], bool]:
    parameters: dict[str, object] = {
        **scope.parameters(),
        "valid_at": timestamp(valid_at),
        "recorded_at": timestamp(recorded_at),
    }
    rows = (
        await tx.run(_OBSERVATIONS, parameters={**parameters, "limit": MAX_OBSERVATIONS + 1})
    ).records
    # Newest first, so the caps keep the newest observations and failures: one seen late is
    # never hidden behind old ones. A failure the observation cut goes through is left out.
    truncated = len(rows) > MAX_OBSERVATIONS
    partial = str(rows[MAX_OBSERVATIONS]["failure_id"]) if truncated else None
    found: dict[str, _Seen] = {}
    for row in rows[:MAX_OBSERVATIONS]:
        if str(row["failure_id"]) != partial:
            found.setdefault(str(row["failure_id"]), _Seen([])).rows.append(row)
    truncated = truncated or len(found) > MAX_FAILURES
    kept = list(found.items())[:MAX_FAILURES]
    for _, seen in kept:
        seen.rows.reverse()  # oldest first, as each record describes them
    wanted = {
        key: sorted({str(row[key]) for _, seen in kept for row in seen.rows if row[key]})
        for key in ("ci_run_id", "test_run_id")
    }
    ci: dict[str, Record] = {}
    tests: dict[str, Record] = {}
    if wanted["ci_run_id"]:
        ci_rows = (
            await tx.run(_CI_RUNS, parameters={**parameters, "run_ids": wanted["ci_run_id"]})
        ).records
        ci = {str(record["run_id"]): record for record in ci_rows}
    if wanted["test_run_id"]:
        test_rows = (
            await tx.run(_TEST_RUNS, parameters={**parameters, "run_ids": wanted["test_run_id"]})
        ).records
        tests = {str(record["run_id"]): record for record in test_rows}
    passes: dict[str, Record] = {}
    if ci:
        records = (
            await tx.run(_NEXT_PASS, parameters={**parameters, "run_ids": sorted(ci)})
        ).records
        passes = {str(record["failing_id"]): record for record in records}
    result = [
        _failure_record(failure_id, seen.rows, ci, tests, passes) for failure_id, seen in kept
    ]
    result.sort(key=lambda record: (record.valid_from, record.failure_id))
    return tuple(result), truncated


def _failure_record(
    failure_id: str,
    observations: list[Record],
    ci: dict[str, Record],
    tests: dict[str, Record],
    passes: dict[str, Record],
) -> FailureRecord:
    runs: dict[tuple[RunKind, str], ObservedRun] = {}
    for row in observations:
        if row["session_id"]:
            session = str(row["session_id"])
            runs.setdefault((RunKind.SESSION, session), _run(RunKind.SESSION, session, None))
        if row["test_run_id"] and str(row["test_run_id"]) in tests:
            test = str(row["test_run_id"])
            runs.setdefault((RunKind.TEST_RUN, test), _run(RunKind.TEST_RUN, test, tests[test]))
        if row["ci_run_id"] and str(row["ci_run_id"]) in ci:
            run_id = str(row["ci_run_id"])
            runs.setdefault((RunKind.CI_RUN, run_id), _run(RunKind.CI_RUN, run_id, ci[run_id]))
    ordered = [runs[key] for key in sorted(runs, key=lambda k: (k[0].value, k[1]))]
    ci_runs = [run for run in ordered if run.kind is RunKind.CI_RUN]
    test_runs = [run for run in ordered if run.kind is RunKind.TEST_RUN]
    # Resolution follows the LATEST visible failing run of each target (provider, workflow, job,
    # project, repository): a recurrence after an earlier fix leaves the failure unresolved.
    latest: dict[tuple[object, ...], ObservedRun] = {}
    for run in ci_runs:
        record = ci[run.run_id]
        key = tuple(
            record.get(name)
            for name in ("provider", "workflow", "job", "project_id", "repository_id")
        )
        best = latest.get(key)
        if best is None or _occurred(run) > _occurred(best):
            latest[key] = run
    fixed = [
        (run, passes[run.run_id])
        for run in latest.values()
        if run.run_id in passes and passes[run.run_id]["passing_id"] is not None
    ]
    late: list[str] = []
    path: FixPath | None = None
    if latest and len(fixed) == len(latest):
        _, record = max(fixed, key=lambda item: _occurred(item[0]))
        path = _fix_path(failure_id, record)
        # The pass fixes only what happened before it: an observation that occurred later (in a
        # session, a test run or a CI run not completed at the cut) is a recurrence it cannot
        # vouch for.
        late = sorted(
            str(row["event_id"])
            for row in observations
            if _occurrence(row, ci) > path.passing_completed_at
        )
    if path is not None and not late:
        resolution = FixResolution(FixStatus.RESOLVED, path=path)
    elif path is not None:
        resolution = FixResolution(
            FixStatus.UNRESOLVED,
            f"observed again after the passing run {path.passing_run_id}: {', '.join(late)}",
        )
    elif ci_runs:
        open_runs = sorted(
            run.run_id for run in latest.values() if run not in [item[0] for item in fixed]
        )
        resolution = FixResolution(
            FixStatus.UNRESOLVED,
            f"no later passing run of the same CI workflow and job after {', '.join(open_runs)}",
        )
    elif test_runs:
        resolution = FixResolution(
            FixStatus.UNSUPPORTED,
            "observed only in test runs, which carry no test or suite identifier (FU-77)",
        )
    else:
        resolution = FixResolution(FixStatus.UNKNOWN, "observed in no CI or test run in scope")
    first = observations[0]
    return FailureRecord(
        failure_id=failure_id,
        component=_text(first["component"]),
        operation=_text(first["operation"]),
        error_class=_text(first["error_class"]),
        fingerprint_sha256=_text(first["fingerprint_sha256"]),
        valid_from=min(_parse(row["valid_from"]) for row in observations),
        recorded_at=min(_parse(row["recorded_at"]) for row in observations),
        observed_in=tuple(ordered),
        resolution=resolution,
        source_event_ids=tuple(sorted(str(row["event_id"]) for row in observations)),
    )
