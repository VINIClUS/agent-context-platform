"""Bi-temporal reads of decisions and failures over the graph PLATFORM-038K projects.

Time. A decision is read at two instants. `valid_at` is when it must hold in the world
(`valid_from <= valid_at < end`); `recorded_at` is what the platform knew by then (only
recordings whose envelope `observed_at`, stored as `recorded_at`, is at or before it exist). Both
default to "now". They are not interchangeable: a decision recorded late is invisible at an earlier
`recorded_at` even though its `valid_from` is earlier still.

Decisions. The graph keeps one immutable `(:DecisionVersion)` per recording, reached with
`(:Decision)-[:HAS_VERSION]->`. This module loads the versions of a scope with bounded Cypher and
hands them to the PURE helpers of `projection.projectors.knowledge` (`decision_states`,
`active_decisions`), so supersession, reopening by a rejected superseder and `valid_to` expiry agree
with the projector by construction. Nothing here re-derives them in Cypher. `active_decisions`
filters on a subject token; scoping is done in Cypher on `project_id`/`repository_id`, so every
version is presented to it with one fixed subject.

`decisions(..., include_history=True)` also returns the decisions that held and have ended at
`valid_at`, each marked `historical` with its reason: `superseded` (by a named decision),
`expired` (its own `valid_to`) or `superseded_undated` (recorded as `superseded` but no known
superseder says when it stopped). Decisions that were only `proposed` or `rejected`, and ones that
have not begun at `valid_at`, are neither active nor history and are not returned.
`decision_history` follows the SUPERSEDES chain both ways in the CURRENT graph, with every
recording of each link.

Failures. `failures` returns the `Failure` nodes of a scope that were observed (`valid_from`, the
event `occurred_at`) by `valid_at` and recorded by `recorded_at`, with the runs they were observed
in. The design has no FIXED_BY edge, so a fix is derived from projected data only, for CI runs:
a failure observed in a CI run is `resolved` when a LATER CI run of the same provider, workflow
and job, in the same project and repository, has status `success` and a `commit_id` or
`snapshot_id` (the commit or snapshot it validated). "Later" is `(completed_at, ci_run_id)`
order; the graph holds no commit order, so `commit_changed` says whether the passing run's commit
differs from the failing one (`None` when either is unknown). The earliest such run wins. The path
is Failure -> failing CI run -> passing CI run -> validated commit or snapshot.
Gaps, reported rather than guessed:
- `TestRunCompletedV1` carries no suite or target identifier (SDK gap FU-77), so the same test
  cannot be identified and a failure observed only in test runs is `unsupported`. `framework`
  is never used as a target.
- A failure observed in no CI or test run in scope is `unknown`.
- Run nodes keep the NEWEST observation of a run. A CI run also keeps `recorded_at`, the first time
  it was reported, so a run first reported after `recorded_at` is not used; a re-observation that
  changed its status is not versioned.

Scope. Every call takes a `TemporalScope` with a `project_id` or a `repository_id` (both given:
both must match). Every node a query touches is matched on those properties, so a node without
scope, or of another scope, is never returned.

Bounds. Every query carries a `LIMIT`: 5,000 decision versions and 500 failures per request, 100
chain links per direction (path depth at most 50). `truncated=True` says a cap was hit. For
`decisions` the cap is on versions, so a cut can drop recordings of a decision and the answer is
then only as complete as the versions read. The result is further limited to 500 decisions. One read
transaction per request carries the driver timeout, and the same deadline bounds the client with
`asyncio.timeout`; expiry raises `RetrievalDeadlineExceeded`. All Cypher is static text and all
input goes in as parameters.

Evidence for PLATFORM-043. Every item carries `source_event_ids` and its intervals as UTC
datetimes. A decision's are the recording events of its known versions; a failure's are the events
that observed it (`OBSERVED_IN` provenance); a run's is the event of its newest observation, read
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
MAX_CHAIN: Final = 100

# `active_decisions` matches a subject token; scope is enforced in Cypher, so every version is
# presented to it with this one subject.
_SUBJECT: Final = "scope"

ResultT = TypeVar("ResultT")


class TemporalRequestError(RetrievalError, ValueError):
    """The request is malformed (no scope, a naive time, a non-positive deadline)."""


class TemporalDecisionNotFound(RetrievalError, LookupError):
    """The decision does not exist in the scope."""


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

_CHAIN_FIELDS: Final[LiteralString] = (
    "x.decision_id AS decision_id, x.status AS status, x.valid_from AS valid_from, "
    "x.valid_to AS valid_to, x.effective_valid_to AS effective_valid_to, "
    "x.superseded_at AS superseded_at, x.supersedes_id AS supersedes_id, "
    "x.recorded_at AS recorded_at, distance, versions"
)
_CHAIN_VERSIONS: Final[LiteralString] = (
    "OPTIONAL MATCH (x)-[:HAS_VERSION]->(v:DecisionVersion) WHERE " + _in_scope("v") + " "
    "WITH x, distance, v ORDER BY v.state_order, v.event_id "
    "WITH x, distance, collect(CASE WHEN v IS NULL THEN NULL ELSE {event_id: v.event_id, "
    "status: v.status, valid_from: v.valid_from, valid_to: v.valid_to, "
    "supersedes_id: v.supersedes_id, recorded_at: v.recorded_at} END) AS versions "
    "ORDER BY distance, x.decision_id LIMIT $limit "
    "RETURN " + _CHAIN_FIELDS
)
# Earlier links (the decisions this one supersedes) and the decision itself: distance 0.
_EARLIER: Final[LiteralString] = (
    "MATCH (a:Decision {decision_id: $decision_id}) WHERE " + _in_scope("a") + " "
    "MATCH p = (a)-[:SUPERSEDES*0..50]->(x:Decision) "
    "WHERE all(n IN nodes(p) WHERE " + _in_scope("n") + ") "
    "WITH x, min(length(p)) AS distance "
) + _CHAIN_VERSIONS
_LATER: Final[LiteralString] = (
    "MATCH (a:Decision {decision_id: $decision_id}) WHERE " + _in_scope("a") + " "
    "MATCH p = (x:Decision)-[:SUPERSEDES*1..50]->(a) "
    "WHERE all(n IN nodes(p) WHERE " + _in_scope("n") + ") "
    "WITH x, min(length(p)) AS distance "
) + _CHAIN_VERSIONS

_FAILURES: Final[LiteralString] = (
    "MATCH (f:Failure) WHERE " + _in_scope("f") + " "
    "AND f.valid_from <= $valid_at AND f.recorded_at <= $recorded_at "
    "WITH f ORDER BY f.valid_from, f.failure_id LIMIT $limit "
    "OPTIONAL MATCH (f)-[o:OBSERVED_IN]->(r) "
    "WHERE r:Session OR ((r:TestRun OR r:CIRun) AND " + _in_scope("r") + ") "
    "RETURN f.failure_id AS failure_id, f.component AS component, f.operation AS operation, "
    "f.error_class AS error_class, f.fingerprint_sha256 AS fingerprint_sha256, "
    "f.valid_from AS valid_from, f.recorded_at AS recorded_at, "
    "head([l IN labels(r) WHERE l IN ['Session', 'TestRun', 'CIRun']]) AS kind, "
    "coalesce(r.session_id, r.test_run_id, r.ci_run_id) AS run_id, r.status AS status, "
    "r.workflow AS workflow, r.job AS job, r.completed_at AS completed_at, "
    "r.commit_id AS commit_id, r.snapshot_id AS snapshot_id, r.result_order AS result_order, "
    "o.source_event_ids AS edge_events "
    "ORDER BY f.valid_from, f.failure_id, kind, run_id"
)

# For each failing CI run, the earliest later passing run of the same target.
_NEXT_PASS: Final[LiteralString] = (
    "UNWIND $run_ids AS run_id "
    "MATCH (r:CIRun {ci_run_id: run_id}) WHERE " + _in_scope("r") + " "
    "AND r.completed_at IS NOT NULL AND r.workflow IS NOT NULL AND r.job IS NOT NULL "
    "OPTIONAL MATCH (p:CIRun) WHERE " + _in_scope("p") + " "
    "AND p.status = 'success' AND p.provider = r.provider AND p.workflow = r.workflow "
    "AND p.job = r.job "
    "AND coalesce(p.project_id, '') = coalesce(r.project_id, '') "
    "AND coalesce(p.repository_id, '') = coalesce(r.repository_id, '') "
    "AND (p.commit_id IS NOT NULL OR p.snapshot_id IS NOT NULL) "
    "AND (p.completed_at > r.completed_at "
    "OR (p.completed_at = r.completed_at AND p.ci_run_id > r.ci_run_id)) "
    "AND p.completed_at <= $valid_at AND p.recorded_at <= $recorded_at "
    "WITH r, p ORDER BY p.completed_at, p.ci_run_id "
    "WITH r, head(collect(p)) AS p "
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
        rows, truncated = await self._run(
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
        """The SUPERSEDES chain around a decision, both ways, with intervals and recordings."""
        if not decision_id.strip():
            raise TemporalRequestError("decision_id is required")
        entries, truncated = await self._run(
            deadline_seconds, lambda tx: _chain(tx, scope, decision_id)
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
        records, truncated = await self._run(
            deadline_seconds, lambda tx: _failures(tx, scope, valid, recorded)
        )
        if not include_resolved:
            records = tuple(r for r in records if r.resolution.status is not FixStatus.RESOLVED)
        return FailuresResult(scope, valid, recorded, records, truncated)

    # --- transaction boundary ---

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
    rows = [
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
        for record in records[:MAX_VERSIONS]
    ]
    return rows, len(records) > MAX_VERSIONS


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
        elif version.status == "superseded" and state.end is None:
            standing, reason = DecisionStanding.HISTORICAL, HistoricalReason.SUPERSEDED_UNDATED
        elif state.asserted and state.end is not None and state.end <= valid_at:
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


def _entry(record: Record, relation: ChainRelation) -> HistoryEntry:
    versions = tuple(
        RecordedVersion(
            event_id=str(item["event_id"]),
            status=str(item["status"]),
            valid_from=_parse(item["valid_from"]),
            valid_to=_parse_optional(item["valid_to"]),
            supersedes_id=None if item["supersedes_id"] is None else str(item["supersedes_id"]),
            recorded_at=_parse(item["recorded_at"]),
        )
        for item in record["versions"]
        if item is not None
    )
    return HistoryEntry(
        decision_id=str(record["decision_id"]),
        relation=relation,
        distance=int(record["distance"]),
        status=str(record["status"]),
        valid_from=_parse(record["valid_from"]),
        valid_to=_parse_optional(record["valid_to"]),
        effective_valid_to=_parse_optional(record["effective_valid_to"]),
        superseded_at=_parse_optional(record["superseded_at"]),
        supersedes_id=None if record["supersedes_id"] is None else str(record["supersedes_id"]),
        recorded_at=_parse(record["recorded_at"]),
        versions=versions,
        source_event_ids=tuple(sorted(item.event_id for item in versions)),
    )


async def _chain(
    tx: Neo4jTransaction, scope: TemporalScope, decision_id: str
) -> tuple[tuple[HistoryEntry, ...], bool]:
    parameters = {**scope.parameters(), "decision_id": decision_id, "limit": MAX_CHAIN + 1}
    earlier = (await tx.run(_EARLIER, parameters=parameters)).records
    if not earlier:
        raise TemporalDecisionNotFound(decision_id)
    later = (await tx.run(_LATER, parameters=parameters)).records
    truncated = len(earlier) > MAX_CHAIN or len(later) > MAX_CHAIN
    truncated = truncated or any(int(r["distance"]) >= 50 for r in (*earlier, *later))
    entries = [
        *reversed([_entry(r, ChainRelation.EARLIER) for r in earlier[:MAX_CHAIN] if r["distance"]]),
        *[_entry(r, ChainRelation.SELF) for r in earlier if not r["distance"]],
        *[_entry(r, ChainRelation.LATER) for r in later[:MAX_CHAIN]],
    ]
    return tuple(entries), truncated


# --------------------------------------------------------------------------------------------
# Failures
# --------------------------------------------------------------------------------------------


@dataclass(slots=True)
class _Failure:
    row: Record
    runs: list[ObservedRun]
    events: set[str]


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
        validated_commit_id=None if passing_commit is None else str(passing_commit),
        validated_snapshot_id=None if record["snapshot_id"] is None else str(record["snapshot_id"]),
        commit_changed=(
            None
            if failing_commit is None or passing_commit is None
            else failing_commit != passing_commit
        ),
        source_event_ids=tuple(
            sorted({*_event_of(record["failing_order"]), *_event_of(record["passing_order"])})
        ),
    )


async def _failures(
    tx: Neo4jTransaction, scope: TemporalScope, valid_at: datetime, recorded_at: datetime
) -> tuple[tuple[FailureRecord, ...], bool]:
    parameters: dict[str, object] = {
        **scope.parameters(),
        "valid_at": timestamp(valid_at),
        "recorded_at": timestamp(recorded_at),
    }
    rows = (await tx.run(_FAILURES, parameters={**parameters, "limit": MAX_FAILURES + 1})).records
    found: dict[str, _Failure] = {}
    for row in rows:
        entry = found.setdefault(str(row["failure_id"]), _Failure(row, [], set()))
        if row["kind"] is None:
            continue
        entry.events.update(str(item) for item in row["edge_events"] or ())
        kind = RunKind(
            {"Session": "session", "TestRun": "test_run", "CIRun": "ci_run"}[str(row["kind"])]
        )
        entry.runs.append(
            ObservedRun(
                kind=kind,
                run_id=str(row["run_id"]),
                status=None if row["status"] is None else str(row["status"]),
                workflow=None if row["workflow"] is None else str(row["workflow"]),
                job=None if row["job"] is None else str(row["job"]),
                completed_at=_parse_optional(row["completed_at"]),
                commit_id=None if row["commit_id"] is None else str(row["commit_id"]),
                snapshot_id=None if row["snapshot_id"] is None else str(row["snapshot_id"]),
                source_event_ids=_event_of(row["result_order"]),
            )
        )
    truncated = len(found) > MAX_FAILURES
    kept = list(found.items())[:MAX_FAILURES]
    ci_ids = sorted({run.run_id for _, f in kept for run in f.runs if run.kind is RunKind.CI_RUN})
    passes: dict[str, Record] = {}
    if ci_ids:
        records = (await tx.run(_NEXT_PASS, parameters={**parameters, "run_ids": ci_ids})).records
        passes = {str(record["failing_id"]): record for record in records}
    result = [_failure_record(failure_id, item, passes) for failure_id, item in kept]
    return tuple(result), truncated


def _failure_record(failure_id: str, item: _Failure, passes: dict[str, Record]) -> FailureRecord:
    ci_runs = [run for run in item.runs if run.kind is RunKind.CI_RUN]
    test_runs = [run for run in item.runs if run.kind is RunKind.TEST_RUN]
    paths = [
        _fix_path(failure_id, passes[run.run_id])
        for run in ci_runs
        if run.run_id in passes and passes[run.run_id]["passing_id"] is not None
    ]
    if paths:
        best = min(paths, key=lambda p: (p.passing_completed_at, p.passing_run_id))
        resolution = FixResolution(FixStatus.RESOLVED, path=best)
    elif ci_runs:
        resolution = FixResolution(
            FixStatus.UNRESOLVED, "no later passing run of the same CI workflow and job"
        )
    elif test_runs:
        resolution = FixResolution(
            FixStatus.UNSUPPORTED,
            "observed only in test runs, which carry no test or suite identifier (FU-77)",
        )
    else:
        resolution = FixResolution(FixStatus.UNKNOWN, "observed in no CI or test run in scope")
    row = item.row
    return FailureRecord(
        failure_id=failure_id,
        component=None if row["component"] is None else str(row["component"]),
        operation=None if row["operation"] is None else str(row["operation"]),
        error_class=None if row["error_class"] is None else str(row["error_class"]),
        fingerprint_sha256=(
            None if row["fingerprint_sha256"] is None else str(row["fingerprint_sha256"])
        ),
        valid_from=_parse(row["valid_from"]),
        recorded_at=_parse(row["recorded_at"]),
        observed_in=tuple(item.runs),
        resolution=resolution,
        source_event_ids=tuple(sorted(item.events)),
    )
