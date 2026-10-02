"""Project durable knowledge (decision, constraint, failure, summary) into the graph.

Knowledge is bi-temporal. `valid_from`/`valid_to` say when a fact holds in the world and come from
the payload (a failure carries none, so its `valid_from` is the event `occurred_at`).
`recorded_at` says when the platform learned it: the envelope `observed_at`. The payload's own
`recorded_at` claim is kept apart as `payload_recorded_at`.

A decision, constraint or summary is a current record: its attributes move forward-only on the
total order `(occurred_at, event_id)`, so a later re-recording (for example `proposed` then
`accepted`) wins in any delivery order. A failure is an immutable fingerprint: first write wins.

Supersession never deletes a decision. `(:Decision)-[:SUPERSEDES]->(:Decision)` is derived from
the superseder's CURRENT record (newest on `(occurred_at, event_id)`) and exists only while that
record's status is `accepted` or `superseded`; the edge's single source event is the event that
set that record. When a newer recording changes the superseder (re-recorded as `rejected` or
`proposed`, or pointed at another decision), the old edge is DELETED, the decision it closed
reopens, and a superseded decision that exists only as an identity-only stub is deleted with it.
A superseded decision's `effective_valid_to` is the earliest of its own `valid_to` and the
`valid_from` of every current superseder. It is recomputed from stored inputs, under the node locks,
whenever either side changes. The final graph is a function of the newest record of each decision,
so any delivery order, including a superseder delivered before the decision it closes, converges.

Design relations that cannot be derived from the published payloads are not invented:
`AFFECTS` (a subject is an untyped token, so its node label is unknown), and failure occurrences in
a turn or tool call (§9.2 relates a failure only to a session, test run or CI run). Constraint
supersession is kept as the `supersedes_id` property: §9.2 defines `SUPERSEDES` for decisions only.
Only content IDs enter the graph, never text.

History. The current record above forgets what an earlier recording said, which a read "as known
at record time T" needs. So every decision recording is also kept as one immutable
`(:Decision)-[:HAS_VERSION]->(:DecisionVersion)` node keyed by the recording event's `event_id`
(design §9.2 names no such label; it is a projection-internal bi-temporal history node). It holds
the payload's status, subjects, `valid_from`, `valid_to`, `supersedes_id` and `payload_recorded_at`,
the envelope `recorded_at` (`observed_at`), the `state_order` of the event and its
`source_event_ids`. It is written once and never changed, so it commutes in any delivery order.

Scope. Decision, DecisionVersion and Failure nodes (and the TestRun and CIRun nodes in
`quality.py`) carry the envelope `context.project_id` and `context.repository_id` as
`project_id`/`repository_id`, when the context has them. They are written with the smallest
non-null value winning, so a later event that disagrees cannot move them and any delivery order
converges. A node without scope belongs to no scope.

`active_decisions` is the pure bi-temporal read: which decisions were active for a scope at a
valid time, as known at a recorded time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Final, LiteralString

from agent_context_sdk import (
    ConstraintRecordedV1,
    DecisionRecordedV1,
    FailureObservedV1,
    StoredEventV1,
    SummaryRecordedV1,
)

from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors import (
    assert_link,
    event_order,
    fill_once,
    lock_event_nodes,
    lock_nodes,
    min_non_null,
    newest_wins,
    node_statement,
    relationship_statement,
    scope_parameters,
)
from agent_context_platform.projection.projectors.agent import session_node_id
from agent_context_platform.projection.projectors.code import timestamp

# Decision statuses whose supersession is asserted, and that count as active when still open.
_SUPERSEDING: Final = frozenset({"accepted", "superseded"})

_DECISION: Final = (
    node_statement("Decision", "decision_id")
    + newest_wins(
        "state",
        "subjects",
        "content_id",
        "status",
        "supersedes_id",
        "valid_from",
        "valid_to",
        "recorded_at",
        "payload_recorded_at",
    )
    + (" WITH n " + min_non_null("project_id", "repository_id"))
)
# One immutable node per decision recording, keyed by the event that recorded it.
_DECISION_VERSION: Final = node_statement("DecisionVersion", "event_id") + fill_once(
    "decision_id",
    "status",
    "subjects",
    "supersedes_id",
    "valid_from",
    "valid_to",
    "recorded_at",
    "payload_recorded_at",
    "state_order",
    "source_event_ids",
    "project_id",
    "repository_id",
)
_VERSION_FIELDS: Final = (
    "status",
    "subjects",
    "supersedes_id",
    "valid_from",
    "valid_to",
    "recorded_at",
    "payload_recorded_at",
    "project_id",
    "repository_id",
)
_HAS_VERSION: Final = relationship_statement(
    "Decision", "decision_id", "HAS_VERSION", "DecisionVersion", "event_id"
)
_CONSTRAINT: Final = node_statement("Constraint", "constraint_id") + newest_wins(
    "state",
    "subjects",
    "content_id",
    "supersedes_id",
    "valid_from",
    "valid_to",
    "recorded_at",
    "payload_recorded_at",
)
_SUMMARY: Final = node_statement("Summary", "summary_id") + newest_wins(
    "state",
    "subjects",
    "content_id",
    "grounded_event_ids",
    "valid_from",
    "valid_to",
    "recorded_at",
    "payload_recorded_at",
)
_FAILURE: Final = node_statement("Failure", "failure_id") + min_non_null(
    "component",
    "operation",
    "error_class",
    "fingerprint_version",
    "fingerprint_sha256",
    "details_content_id",
    "valid_from",
    "recorded_at",
    "project_id",
    "repository_id",
)

_SUPERSEDES: Final = relationship_statement(
    "Decision", "decision_id", "SUPERSEDES", "Decision", "decision_id"
)
_STORED_ORDER: Final[LiteralString] = (
    "MATCH (n:Decision {decision_id: $node_id}) RETURN n.state_order AS stored"
)
_OLD_TARGETS: Final[LiteralString] = (
    "MATCH (:Decision {decision_id: $node_id})-[:SUPERSEDES]->(t:Decision) "
    "RETURN t.decision_id AS target"
)
_DROP_EDGES: Final[LiteralString] = (
    "MATCH (:Decision {decision_id: $node_id})-[r:SUPERSEDES]->() DELETE r"
)
# A decision that was only ever the target of an edge (no record of its own) leaves with its edge.
_DROP_STUB: Final[LiteralString] = (
    "MATCH (n:Decision {decision_id: $node_id}) "
    "WHERE n.state_order IS NULL AND NOT (n)--() DELETE n"
)
# Recompute a decision's closure from its stored inputs; a pure function of the graph.
_CLOSE: Final[LiteralString] = (
    "MATCH (n:Decision {decision_id: $node_id}) "
    "OPTIONAL MATCH (s:Decision)-[:SUPERSEDES]->(n) "
    "WITH n, min(s.valid_from) AS closing "
    "SET n.superseded_at = closing, n.effective_valid_to = CASE "
    "WHEN closing IS NULL THEN n.valid_to "
    "WHEN n.valid_to IS NULL OR closing < n.valid_to THEN closing ELSE n.valid_to END"
)

_OBSERVED_IN_SESSION: Final = relationship_statement(
    "Failure", "failure_id", "OBSERVED_IN", "Session", "session_id"
)
_OBSERVED_IN_TEST_RUN: Final = relationship_statement(
    "Failure", "failure_id", "OBSERVED_IN", "TestRun", "test_run_id"
)
_OBSERVED_IN_CI_RUN: Final = relationship_statement(
    "Failure", "failure_id", "OBSERVED_IN", "CIRun", "ci_run_id"
)


def _knowledge_parameters(
    event: StoredEventV1,
    payload: DecisionRecordedV1 | ConstraintRecordedV1 | SummaryRecordedV1,
) -> dict[str, object]:
    return {
        "order": event_order(event),
        "subjects": list(payload.subjects),
        "content_id": payload.content_id,
        "valid_from": timestamp(payload.valid_from),
        "valid_to": timestamp(payload.valid_to) if payload.valid_to is not None else None,
        "recorded_at": timestamp(event.observed_at),
        "payload_recorded_at": timestamp(payload.recorded_at),
    }


async def _decision_recorded(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = DecisionRecordedV1.model_validate(dict(event.payload))
    order = event_order(event)
    stored = (await tx.run(_STORED_ORDER, parameters={"node_id": payload.decision_id})).records
    newer = not stored or stored[0]["stored"] is None or order > stored[0]["stored"]
    parameters = _knowledge_parameters(event, payload)
    parameters |= {
        "node_id": payload.decision_id,
        "status": payload.status,
        "supersedes_id": payload.supersedes_id,
        **scope_parameters(event),
    }
    await tx.run(_DECISION, parameters=parameters)
    # The recording itself is history: written once, whatever its order against the others.
    version = str(event.event_id)
    await tx.run(
        _DECISION_VERSION,
        parameters={
            **{key: parameters[key] for key in _VERSION_FIELDS},
            "node_id": version,
            "decision_id": payload.decision_id,
            "state_order": order,
            "source_event_ids": [version],
        },
    )
    await assert_link(tx, _HAS_VERSION, event, payload.decision_id, version)
    if newer:
        # This record is now the superseder's current one: replace what the old one asserted.
        old = (await tx.run(_OLD_TARGETS, parameters={"node_id": payload.decision_id})).records
        target = payload.supersedes_id if payload.status in _SUPERSEDING else None
        targets = {str(record["target"]) for record in old} | ({target} if target else set())
        # The old targets are only known from the graph; lock them before changing them.
        await lock_nodes(tx, [("Decision", item) for item in targets])
        await tx.run(_DROP_EDGES, parameters={"node_id": payload.decision_id})
        if target is not None:
            await assert_link(tx, _SUPERSEDES, event, payload.decision_id, target)
        for item in sorted(targets):
            await tx.run(_DROP_STUB, parameters={"node_id": item})
            await tx.run(_CLOSE, parameters={"node_id": item})
    elif payload.supersedes_id is not None and payload.status in _SUPERSEDING:
        # Locking merged the target as an identity-only node; an older record changes no edge.
        await tx.run(_DROP_STUB, parameters={"node_id": payload.supersedes_id})
    await tx.run(_CLOSE, parameters={"node_id": payload.decision_id})


async def _constraint_recorded(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = ConstraintRecordedV1.model_validate(dict(event.payload))
    parameters = _knowledge_parameters(event, payload)
    parameters |= {"node_id": payload.constraint_id, "supersedes_id": payload.supersedes_id}
    await tx.run(_CONSTRAINT, parameters=parameters)


async def _summary_recorded(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = SummaryRecordedV1.model_validate(dict(event.payload))
    parameters = _knowledge_parameters(event, payload)
    parameters |= {
        "node_id": payload.summary_id,
        "grounded_event_ids": sorted({str(item) for item in payload.source_event_ids}),
    }
    await tx.run(_SUMMARY, parameters=parameters)


async def _failure_observed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = FailureObservedV1.model_validate(dict(event.payload))
    await tx.run(
        _FAILURE,
        parameters={
            "node_id": payload.failure_id,
            "component": payload.component,
            "operation": payload.operation,
            "error_class": payload.error_class,
            "fingerprint_version": payload.fingerprint_version,
            "fingerprint_sha256": payload.fingerprint_sha256,
            "details_content_id": payload.details_content_id,
            "valid_from": timestamp(event.occurred_at),
            "recorded_at": timestamp(event.observed_at),
            **scope_parameters(event),
        },
    )
    failure = payload.failure_id
    if payload.session_id is not None:
        await assert_link(
            tx, _OBSERVED_IN_SESSION, event, failure, session_node_id(payload.session_id)
        )
    if payload.test_run_id is not None:
        await assert_link(tx, _OBSERVED_IN_TEST_RUN, event, failure, payload.test_run_id)
    if payload.ci_run_id is not None:
        await assert_link(tx, _OBSERVED_IN_CI_RUN, event, failure, payload.ci_run_id)


_HANDLERS: Final[dict[str, Callable[[Neo4jTransaction, StoredEventV1], Awaitable[None]]]] = {
    "knowledge.decision.recorded": _decision_recorded,
    "knowledge.constraint.recorded": _constraint_recorded,
    "knowledge.failure.observed": _failure_observed,
    "knowledge.summary.recorded": _summary_recorded,
}

KNOWLEDGE_EVENT_TYPES: Final = frozenset(_HANDLERS)


def lock_keys(event: StoredEventV1) -> list[tuple[str, str]]:
    """Nodes `KnowledgeProjector` writes for `event`, from the IDs it writes with."""
    kind = event.event_type
    if kind == "knowledge.decision.recorded":
        decision = DecisionRecordedV1.model_validate(dict(event.payload))
        keys = [
            ("Decision", decision.decision_id),
            ("DecisionVersion", str(event.event_id)),
        ]
        if decision.supersedes_id is not None and decision.status in _SUPERSEDING:
            keys.append(("Decision", decision.supersedes_id))
        return keys
    if kind == "knowledge.constraint.recorded":
        constraint = ConstraintRecordedV1.model_validate(dict(event.payload))
        return [("Constraint", constraint.constraint_id)]
    if kind == "knowledge.summary.recorded":
        summary = SummaryRecordedV1.model_validate(dict(event.payload))
        return [("Summary", summary.summary_id)]
    if kind == "knowledge.failure.observed":
        failure = FailureObservedV1.model_validate(dict(event.payload))
        keys = [("Failure", failure.failure_id)]
        if failure.session_id is not None:
            keys.append(("Session", session_node_id(failure.session_id)))
        if failure.test_run_id is not None:
            keys.append(("TestRun", failure.test_run_id))
        if failure.ci_run_id is not None:
            keys.append(("CIRun", failure.ci_run_id))
        return keys
    return []


class KnowledgeProjector:
    """Projects decision, constraint, failure and summary events."""

    name = "knowledge"
    version = "1"

    def handles(self, event_type: str) -> bool:
        return event_type in _HANDLERS

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        await lock_event_nodes(tx, event)
        await _HANDLERS[event.event_type](tx, event)


# --------------------------------------------------------------------------------------------
# As-of reads: a pure function of recorded decision versions
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DecisionVersion:
    """One recording of a decision, as the ledger knows it."""

    decision_id: str
    subjects: tuple[str, ...]
    status: str
    valid_from: datetime
    valid_to: datetime | None
    recorded_at: datetime
    supersedes_id: str | None = None
    order: str = ""


def decision_version(event: StoredEventV1) -> DecisionVersion:
    """The `DecisionVersion` a `knowledge.decision.recorded` event contributes."""
    payload = DecisionRecordedV1.model_validate(dict(event.payload))
    return DecisionVersion(
        decision_id=payload.decision_id,
        subjects=payload.subjects,
        status=payload.status,
        valid_from=payload.valid_from,
        valid_to=payload.valid_to,
        recorded_at=event.observed_at,
        supersedes_id=payload.supersedes_id,
        order=event_order(event),
    )


@dataclass(frozen=True, slots=True)
class DecisionState:
    """A decision as known at one record time: its newest version and what closes it."""

    version: DecisionVersion
    superseded_by: str | None  # the current superseder with the earliest `valid_from`
    closed_at: datetime | None  # that superseder's `valid_from`
    end: datetime | None  # the earlier of `version.valid_to` and `closed_at`

    @property
    def asserted(self) -> bool:
        """Whether the decision was ever in force: `accepted`, or `superseded` with a known end."""
        status = self.version.status
        return status in _SUPERSEDING and not (status == "superseded" and self.end is None)


def decision_states(
    versions: Iterable[DecisionVersion], *, recorded_at: datetime
) -> dict[str, DecisionState]:
    """Every decision known at `recorded_at`, with its closure; the one place that rule lives.

    Only versions recorded at or before `recorded_at` exist. Each decision is its newest known
    version (by `order`, then recording time, like the graph's current pointer). It is closed at
    its own `valid_to` or at the earliest `valid_from` of a decision whose NEWEST known version is
    `accepted` or `superseded` and names it in `supersedes_id`, whichever comes first.
    """
    current: dict[str, DecisionVersion] = {}
    for version in versions:
        if version.recorded_at > recorded_at:
            continue
        best = current.get(version.decision_id)
        if best is None or (version.order, version.recorded_at) > (best.order, best.recorded_at):
            current[version.decision_id] = version
    closing: dict[str, tuple[datetime, str]] = {}
    for version in current.values():
        if version.supersedes_id is not None and version.status in _SUPERSEDING:
            candidate = (version.valid_from, version.decision_id)
            earlier = closing.get(version.supersedes_id)
            if earlier is None or candidate < earlier:
                closing[version.supersedes_id] = candidate
    states: dict[str, DecisionState] = {}
    for decision_id, version in current.items():
        closed_at, superseded_by = closing.get(decision_id, (None, None))
        ends = [moment for moment in (version.valid_to, closed_at) if moment]
        states[decision_id] = DecisionState(
            version, superseded_by, closed_at, min(ends) if ends else None
        )
    return states


def active_decisions(
    versions: Iterable[DecisionVersion],
    *,
    scope: str,
    valid_at: datetime,
    recorded_at: datetime,
) -> list[str]:
    """IDs of the decisions active for `scope` at valid time `valid_at`, as known at `recorded_at`.

    See `decision_states` for what is known and how a decision closes. A superseder later
    re-recorded as `rejected` or `proposed` closes nothing, exactly like the graph edge it would
    delete. A `superseded` decision with no known closing time is not active: nothing says when it
    stopped holding. `proposed` and `rejected` decisions never are.
    """
    active: list[str] = []
    for decision_id, state in decision_states(versions, recorded_at=recorded_at).items():
        version = state.version
        if (
            state.asserted
            and scope in version.subjects
            and version.valid_from <= valid_at
            and (state.end is None or valid_at < state.end)
        ):
            active.append(decision_id)
    return sorted(active)
