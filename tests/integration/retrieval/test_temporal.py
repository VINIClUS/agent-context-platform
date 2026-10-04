"""Bi-temporal decision and failure reads (PLATFORM-041) over a graph the real projectors built.

Every fixture is an SDK event delivered through `KnowledgeProjector`/`QualityProjector` (and the
portfolio, agent and git projectors that own the stubs they point at); no node is written by hand.
Days are offsets from `T0`: `observed` is the envelope `observed_at` (record time), `valid_from`
the payload's valid time.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, LiteralString

import pytest
from agent_context_sdk import (
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content import ContentDisposition

from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.projectors.knowledge import (
    KnowledgeProjector,
    active_decisions,
    decision_version,
)
from agent_context_platform.projection.projectors.quality import QualityProjector
from agent_context_platform.retrieval import temporal
from agent_context_platform.retrieval.graph import RetrievalDeadlineExceeded
from agent_context_platform.retrieval.temporal import (
    ChainRelation,
    DecisionStanding,
    FixStatus,
    HistoricalReason,
    TemporalDecisionNotFound,
    TemporalRequestError,
    TemporalScope,
    TemporalService,
)

from ..projection.conftest import neo4j_integration_settings
from ..projection.projectors.conftest import (
    PROJECTORS,
    event_uuid,
    project_event,
)
from .conftest import wipe

pytestmark = pytest.mark.integration

ALL = (*PROJECTORS, KnowledgeProjector(), QualityProjector())
T0 = datetime(2026, 8, 13, 13, 0, 0, tzinfo=UTC)
SHA = "e" * 64
OID_A = "a" * 40
OID_B = "b" * 40
P1 = TemporalScope(project_id="prj_1")
P2 = TemporalScope(project_id="prj_2")
R1 = TemporalScope(repository_id="repo_1")
CTX1 = {"project_id": "prj_1", "repository_id": "repo_1"}
CTX2 = {"project_id": "prj_2", "repository_id": "repo_2"}
FAR = 400  # a record time after everything
_GRAPH_CLOSURE: LiteralString = (
    "MATCH (n:Decision) WHERE n.project_id = $project_id AND n.status IN "
    "['accepted', 'superseded'] AND n.valid_from <= $at "
    "AND (n.effective_valid_to IS NULL OR $at < n.effective_valid_to) "
    "AND NOT (n.status = 'superseded' AND n.effective_valid_to IS NULL) "
    "RETURN n.decision_id AS id ORDER BY id"
)


def day(days: float) -> datetime:
    return T0 + timedelta(days=days)


def iso(days: float) -> str:
    return day(days).strftime("%Y-%m-%dT%H:%M:%SZ")


def make(
    number: int,
    event_type: str,
    payload: dict[str, object],
    *,
    observed: float,
    context: dict[str, str] | None = None,
) -> StoredEventV1:
    draft = EventDraftV1(
        event_id=event_uuid(number),
        event_type=event_type,
        stream_id=f"stream-{number}",
        occurred_at=T0 + timedelta(seconds=number),
        observed_at=day(observed),
        producer=ProducerV1(producer_id="temporal-test", name="temporal-test", version="1.0.0"),
        context=EventContextV1(**(CTX1 if context is None else context)),
        payload=payload,
        redaction=EventRedactionSummaryV1(
            policy_version="test-policy-v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"key-{number}",
    )
    return seal_event(draft, [], 1, None)


def decision(
    number: int,
    decision_id: str,
    *,
    supersedes: str | None = None,
    status: str = "accepted",
    valid_from: float = 0,
    valid_to: float | None = None,
    observed: float | None = None,
    context: dict[str, str] | None = None,
) -> StoredEventV1:
    seen = number if observed is None else observed
    return make(
        number,
        "knowledge.decision.recorded",
        {
            "decision_id": decision_id,
            "status": status,
            "supersedes_id": supersedes,
            "subjects": ["mod_a"],
            "content_id": f"content_{decision_id}",
            "valid_from": iso(valid_from),
            "valid_to": None if valid_to is None else iso(valid_to),
            "recorded_at": iso(seen),
        },
        observed=seen,
        context=context,
    )


def ci_run(
    number: int,
    ci_run_id: str,
    status: str,
    *,
    job: str = "test",
    commit: str = OID_A,
    observed: float | None = None,
    context: dict[str, str] | None = None,
) -> StoredEventV1:
    return make(
        number,
        "quality.ci_run.completed",
        {
            "ci_run_id": ci_run_id,
            "provider": "github",
            "workflow": "ci",
            "job": job,
            "external_id": ci_run_id,
            "status": status,
            "duration_ms": 10,
            "commit_id": commit,
        },
        observed=number if observed is None else observed,
        context=context,
    )


def run_of_tests(number: int, test_run_id: str, status: str = "failed") -> StoredEventV1:
    return make(
        number,
        "quality.test_run.completed",
        {
            "test_run_id": test_run_id,
            "framework": "pytest",
            "status": status,
            "total_count": 1,
            "passed_count": 0,
            "failed_count": 1,
            "skipped_count": 0,
            "error_count": 0,
            "duration_ms": 1,
            "commit_id": OID_A,
        },
        observed=number,
    )


def failure(
    number: int,
    failure_id: str,
    *,
    ci_run_id: str | None = None,
    test_run_id: str | None = None,
    session_id: str | None = None,
    observed: float | None = None,
    context: dict[str, str] | None = None,
) -> StoredEventV1:
    return make(
        number,
        "knowledge.failure.observed",
        {
            "failure_id": failure_id,
            "component": "api",
            "operation": "ingest",
            "error_class": "Timeout",
            "fingerprint_version": "1",
            "fingerprint_sha256": SHA,
            "ci_run_id": ci_run_id,
            "test_run_id": test_run_id,
            "session_id": session_id,
        },
        observed=number if observed is None else observed,
        context=context,
    )


def with_service(
    body: Callable[[Neo4jStore, TemporalService], Awaitable[None]],
) -> None:
    async def run() -> None:
        settings = neo4j_integration_settings()
        async with Neo4jStore(settings) as store:
            await wipe(store)
            service = TemporalService.from_settings(settings)
            try:
                await body(store, service)
            finally:
                await service.close()
                await wipe(store)

    asyncio.run(run())


async def graph_active(store: Neo4jStore, moment: float) -> list[str]:
    """The decisions the graph's own stored closure (`effective_valid_to`) says are open."""

    async def read(tx: Neo4jTransaction) -> list[str]:
        rows = (
            await tx.run(
                _GRAPH_CLOSURE,
                parameters={
                    "project_id": "prj_1",
                    "at": day(moment).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            )
        ).records
        return [str(row["id"]) for row in rows]

    return await store.execute_read(read)


async def deliver(store: Neo4jStore, events: list[StoredEventV1]) -> None:
    await wipe(store)
    for event in events:
        await project_event(store, event, ALL)


async def active(
    service: TemporalService,
    scope: TemporalScope,
    valid: float,
    recorded: float = FAR,
) -> list[str]:
    result = await service.decisions(scope, day(valid), day(recorded))
    assert all(item.standing is DecisionStanding.ACTIVE for item in result.decisions)
    return [item.decision_id for item in result.decisions]


def chain() -> list[StoredEventV1]:
    return [
        decision(1, "dec_a", valid_from=0),
        decision(2, "dec_b", supersedes="dec_a", valid_from=10),
        decision(3, "dec_c", supersedes="dec_b", valid_from=20),
    ]


# --- bi-temporal correctness ---


def test_a_decision_recorded_late_is_invisible_before_it_was_recorded() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        # Valid since day 0, but the platform only learned of it on day 10.
        await deliver(store, [decision(1, "dec_a", valid_from=0, observed=10)])
        assert await active(service, P1, valid=5, recorded=5) == []
        assert await active(service, P1, valid=5, recorded=9.99) == []
        assert await active(service, P1, valid=5, recorded=10) == ["dec_a"]
        assert await active(service, P1, valid=5, recorded=50) == ["dec_a"]
        # valid time still applies once it is known
        assert await active(service, P1, valid=-1, recorded=50) == []

    with_service(body)


def test_a_superseded_decision_is_active_before_and_historical_after_its_superseder() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(store, chain()[:2])
        assert await active(service, P1, valid=5) == ["dec_a"]
        assert await active(service, P1, valid=10) == ["dec_b"]
        assert await active(service, P1, valid=15) == ["dec_b"]
        before = await service.decisions(P1, day(5), day(FAR), include_history=True)
        assert [(d.decision_id, d.standing) for d in before.decisions] == [
            ("dec_a", DecisionStanding.ACTIVE)
        ]
        after = await service.decisions(P1, day(15), day(FAR), include_history=True)
        assert [(d.decision_id, d.standing, d.reason) for d in after.decisions] == [
            ("dec_b", DecisionStanding.ACTIVE, None),
            ("dec_a", DecisionStanding.HISTORICAL, HistoricalReason.SUPERSEDED),
        ]
        old = after.decisions[1]
        assert old.superseded_by == "dec_b"
        assert (old.valid_from, old.effective_valid_to) == (day(0), day(10))
        assert old.source_event_ids == (str(event_uuid(1)),)
        assert not after.truncated
        # Known only up to day 1.5, the superseder (recorded on day 2) does not exist yet.
        assert await active(service, P1, valid=15, recorded=1.5) == ["dec_a"]

    with_service(body)


def test_a_superseder_re_recorded_as_rejected_reopens_the_decision() -> None:
    events = [
        decision(1, "dec_a", valid_from=0),
        decision(2, "dec_b", supersedes="dec_a", valid_from=10),
        decision(3, "dec_b", supersedes="dec_a", valid_from=10, status="rejected"),
    ]

    async def body(store: Neo4jStore, service: TemporalService) -> None:
        for order in itertools.permutations(events):
            await deliver(store, list(order))
            assert await active(service, P1, valid=15, recorded=2.5) == ["dec_b"]
            assert await active(service, P1, valid=15, recorded=3) == ["dec_a"]
            assert await active(service, P1, valid=15) == ["dec_a"]
            history = await service.decisions(P1, day(15), day(2.5), include_history=True)
            assert [d.decision_id for d in history.decisions] == ["dec_b", "dec_a"]
            assert history.decisions[1].reason is HistoricalReason.SUPERSEDED

    with_service(body)


def test_valid_to_expiry_is_historical_with_its_reason() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(store, [decision(1, "dec_d", valid_from=2, valid_to=12)])
        assert await active(service, P1, valid=1) == []
        assert await active(service, P1, valid=2) == ["dec_d"]
        assert await active(service, P1, valid=11.9) == ["dec_d"]
        assert await active(service, P1, valid=12) == []
        gone = await service.decisions(P1, day(30), day(FAR), include_history=True)
        assert [(d.decision_id, d.standing, d.reason) for d in gone.decisions] == [
            ("dec_d", DecisionStanding.HISTORICAL, HistoricalReason.EXPIRED)
        ]
        assert gone.decisions[0].superseded_by is None
        assert gone.decisions[0].effective_valid_to == day(12)
        # not yet begun: neither active nor history
        early = await service.decisions(P1, day(1), day(FAR), include_history=True)
        assert early.decisions == ()

    with_service(body)


def test_proposed_and_rejected_decisions_are_never_returned() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(
            store,
            [
                decision(1, "dec_p", status="proposed"),
                decision(2, "dec_r", status="rejected"),
                decision(3, "dec_u", status="superseded"),
            ],
        )
        shown = await service.decisions(P1, day(30), day(FAR), include_history=True)
        assert [(d.decision_id, d.reason) for d in shown.decisions] == [
            ("dec_u", HistoricalReason.SUPERSEDED_UNDATED)
        ]

    with_service(body)


def test_the_supersession_chain_gives_the_same_answers_in_every_delivery_order() -> None:
    probes = [(v, r) for v in (-1, 0, 5, 10, 15, 20, 25) for r in (0.5, 1.5, 2.5, FAR)]

    async def answers(service: TemporalService) -> list[Any]:
        found: list[Any] = []
        for valid, recorded in probes:
            result = await service.decisions(P1, day(valid), day(recorded), include_history=True)
            found.append(
                [(d.decision_id, d.standing, d.reason, d.superseded_by) for d in result.decisions]
            )
        return found

    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(store, chain())
        expected = await answers(service)
        assert await active(service, P1, valid=5) == ["dec_a"]
        assert await active(service, P1, valid=15) == ["dec_b"]
        assert await active(service, P1, valid=25) == ["dec_c"]
        for order in itertools.permutations(chain()):
            await deliver(store, list(order))
            assert await answers(service) == expected, [str(e.event_id)[-1] for e in order]

    with_service(body)


def test_decisions_agree_with_active_decisions_and_the_graph_closure_on_one_ledger() -> None:
    ledger = [
        decision(1, "dec_a", valid_from=0),
        decision(2, "dec_b", supersedes="dec_a", valid_from=10),
        decision(3, "dec_c", supersedes="dec_b", valid_from=20),
        decision(4, "dec_d", valid_from=2, valid_to=12),
        decision(5, "dec_e", supersedes="dec_d", valid_from=8),
        decision(6, "dec_c", supersedes="dec_b", valid_from=20, status="rejected"),
        decision(7, "dec_f", supersedes="dec_a", valid_from=4, status="superseded"),
        decision(8, "dec_g", valid_from=3, observed=30),
    ]

    async def body(store: Neo4jStore, service: TemporalService) -> None:
        versions = [decision_version(event) for event in ledger]
        for delivery in (ledger, list(reversed(ledger))):
            await deliver(store, delivery)
            for valid in (-1, 0, 3, 5, 9, 10, 11, 12, 15, 20, 25):
                for recorded in (1, 4.5, 7.5, 20, FAR):
                    expected = active_decisions(
                        versions, scope="mod_a", valid_at=day(valid), recorded_at=day(recorded)
                    )
                    assert await active(service, P1, valid, recorded) == expected, (valid, recorded)

            for valid in (-1, 3, 9, 15, 25):
                stored = await graph_active(store, valid)
                assert await active(service, P1, valid) == stored, valid

    with_service(body)


def test_now_is_the_default_for_valid_time_and_record_time() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(store, [decision(1, "dec_a", valid_from=0)])
        result = await service.decisions(P1)
        assert [d.decision_id for d in result.decisions] == ["dec_a"]
        assert result.valid_at.tzinfo is not None and result.recorded_at == result.valid_at
        with pytest.raises(TemporalRequestError):
            await service.decisions(P1, datetime(2026, 9, 1))  # naive

    with_service(body)


# --- scope ---


def test_scope_isolation_between_projects_repositories_and_unscoped_nodes() -> None:
    both = {"project_id": "prj_1", "repository_id": "repo_2"}
    events = [
        decision(1, "dec_1", context=CTX1),
        decision(2, "dec_2", context=CTX2),
        decision(3, "dec_3", context=both),
        decision(4, "dec_4", context={}),
        failure(5, "fail_1", ci_run_id="ci_1", context=CTX1),
        failure(6, "fail_2", ci_run_id="ci_2", context=CTX2),
        failure(7, "fail_3", ci_run_id="ci_3", context={}),
        ci_run(9, "ci_2", "failure", context=CTX2),
    ]

    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(store, events)
        assert await active(service, P1, 5) == ["dec_1", "dec_3"]
        assert await active(service, P2, 5) == ["dec_2"]
        assert await active(service, R1, 5) == ["dec_1"]
        assert await active(service, TemporalScope("prj_1", "repo_2"), 5) == ["dec_3"]
        assert await active(service, TemporalScope("prj_1", "repo_1"), 5) == ["dec_1"]
        assert await active(service, TemporalScope("prj_9"), 5) == []
        found = await service.failures(P1, day(FAR), day(FAR))
        assert [f.failure_id for f in found.failures] == ["fail_1"]
        assert [
            f.failure_id for f in (await service.failures(R1, day(FAR), day(FAR))).failures
        ] == ["fail_1"]
        # a run of another scope is never shown as where a failure was seen
        mixed = failure(8, "fail_4", ci_run_id="ci_2", context=CTX1)
        await project_event(store, mixed, ALL)
        shown = (await service.failures(P1, day(FAR), day(FAR))).failures
        assert [(f.failure_id, f.observed_in) for f in shown if f.failure_id == "fail_4"] == [
            ("fail_4", ())
        ]
        assert [f.resolution.status for f in shown if f.failure_id == "fail_4"] == [
            FixStatus.UNKNOWN
        ]
        with pytest.raises(TemporalDecisionNotFound):
            await service.decision_history("dec_2", P1)
        assert (await service.decision_history("dec_2", P2)).entries[0].decision_id == "dec_2"

    with_service(body)


def test_a_scope_needs_a_project_or_a_repository() -> None:
    with pytest.raises(TemporalRequestError):
        TemporalScope()
    with pytest.raises(TemporalRequestError):
        TemporalScope(project_id="  ")


# --- decision history ---


def test_decision_history_follows_the_chain_both_ways_with_intervals_and_recordings() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        events = [*chain(), decision(4, "dec_b", supersedes="dec_a", valid_from=11)]
        await deliver(store, events)
        middle = await service.decision_history("dec_b", P1)
        assert [(e.decision_id, e.relation, e.distance) for e in middle.entries] == [
            ("dec_a", ChainRelation.EARLIER, 1),
            ("dec_b", ChainRelation.SELF, 0),
            ("dec_c", ChainRelation.LATER, 1),
        ]
        a, b, c = middle.entries
        assert (a.valid_from, a.effective_valid_to, a.superseded_at) == (day(0), day(11), day(11))
        assert (b.valid_from, b.effective_valid_to) == (day(11), day(20))
        assert (c.valid_from, c.effective_valid_to, c.supersedes_id) == (day(20), None, "dec_b")
        assert [v.event_id for v in b.versions] == [str(event_uuid(2)), str(event_uuid(4))]
        assert [v.valid_from for v in b.versions] == [day(10), day(11)]
        assert b.source_event_ids == (str(event_uuid(2)), str(event_uuid(4)))
        assert not middle.truncated
        last = await service.decision_history("dec_c", P1)
        assert [e.decision_id for e in last.entries] == ["dec_a", "dec_b", "dec_c"]
        assert [e.relation for e in last.entries] == [
            ChainRelation.EARLIER,
            ChainRelation.EARLIER,
            ChainRelation.SELF,
        ]
        assert [e.distance for e in last.entries] == [2, 1, 0]
        with pytest.raises(TemporalDecisionNotFound):
            await service.decision_history("dec_missing", P1)
        with pytest.raises(TemporalDecisionNotFound):
            await service.decision_history("dec_b", P2)

    with_service(body)


def test_a_rejected_superseder_leaves_the_chain() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(
            store,
            [
                decision(1, "dec_a"),
                decision(2, "dec_b", supersedes="dec_a", valid_from=10),
                decision(3, "dec_b", supersedes="dec_a", valid_from=10, status="rejected"),
            ],
        )
        history = await service.decision_history("dec_a", P1)
        assert [e.decision_id for e in history.entries] == ["dec_a"]
        rejected = await service.decision_history("dec_b", P1)
        assert [(e.decision_id, e.status) for e in rejected.entries] == [("dec_b", "rejected")]
        assert [v.status for v in rejected.entries[0].versions] == ["accepted", "rejected"]

    with_service(body)


# --- failures and fix paths ---


def test_a_failure_followed_by_a_passing_run_has_a_fix_path() -> None:
    events = [
        ci_run(1, "ci_1", "failure", commit=OID_A),
        failure(2, "fail_1", ci_run_id="ci_1"),
        ci_run(3, "ci_2", "failure", commit=OID_A),
        ci_run(4, "ci_3", "success", commit=OID_B),
        ci_run(5, "ci_4", "success", commit=OID_B),
    ]

    async def body(store: Neo4jStore, service: TemporalService) -> None:
        for order in (events, list(reversed(events))):
            await deliver(store, order)
            now = await service.failures(P1, day(FAR), day(FAR))
            assert now.failures == ()  # resolved ones are hidden by default
            shown = await service.failures(P1, day(FAR), day(FAR), include_resolved=True)
            (found,) = shown.failures
            assert found.failure_id == "fail_1"
            assert found.resolution.status is FixStatus.RESOLVED
            path = found.resolution.path
            assert path is not None
            assert (path.failing_run_id, path.passing_run_id) == ("ci_1", "ci_3")  # the earliest
            assert (path.validated_commit_id, path.commit_changed) == (OID_B, True)
            assert (path.workflow, path.job) == ("ci", "test")
            assert path.failing_completed_at < path.passing_completed_at
            assert path.source_event_ids == (str(event_uuid(1)), str(event_uuid(4)))
            assert [(r.run_id, r.status) for r in found.observed_in] == [("ci_1", "failure")]
            assert found.source_event_ids == (str(event_uuid(2)),)
            assert found.recorded_at == day(2) and found.valid_from == T0 + timedelta(seconds=2)

    with_service(body)


def test_a_fix_path_respects_valid_time_and_record_time() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        # The pass completes at 3 s, but is only reported (recorded) on day 20.
        await deliver(
            store,
            [
                ci_run(1, "ci_1", "failure"),
                failure(2, "fail_1", ci_run_id="ci_1"),
                ci_run(3, "ci_2", "success", commit=OID_B, observed=20),
            ],
        )

        async def status(valid: float | datetime, recorded: float) -> FixStatus:
            moment = valid if isinstance(valid, datetime) else day(valid)
            shown = await service.failures(P1, moment, day(recorded), include_resolved=True)
            return shown.failures[0].resolution.status

        assert await status(FAR, 19) is FixStatus.UNRESOLVED
        assert await status(FAR, 20) is FixStatus.RESOLVED
        assert await status(T0 + timedelta(seconds=2.5), 30) is FixStatus.UNRESOLVED
        assert await status(T0 + timedelta(seconds=3), 30) is FixStatus.RESOLVED
        # a failure recorded after the record time, or observed after the valid time, is absent
        assert (
            await service.failures(P1, day(FAR), day(1.5), include_resolved=True)
        ).failures == ()
        assert (
            await service.failures(P1, T0 + timedelta(seconds=1), day(30), include_resolved=True)
        ).failures == ()

    with_service(body)


def test_a_failure_with_no_later_pass_is_unresolved() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        # ci_0 completes (1 s) BEFORE ci_1 (2 s): an earlier pass is not a fix, whenever observed
        await deliver(
            store,
            [
                ci_run(2, "ci_1", "failure"),
                failure(3, "fail_1", ci_run_id="ci_1"),
                ci_run(1, "ci_0", "success", commit=OID_B),
                ci_run(4, "ci_2", "success", job="lint", commit=OID_B),
                ci_run(5, "ci_3", "failure", commit=OID_B),
                ci_run(6, "ci_4", "success", commit=OID_B, context=CTX2),
            ],
        )
        (found,) = (await service.failures(P1, day(FAR), day(FAR))).failures
        assert found.resolution.status is FixStatus.UNRESOLVED
        assert found.resolution.path is None
        assert found.resolution.reason is not None

    with_service(body)


def test_failures_seen_only_in_test_runs_or_sessions_are_unsupported_or_unknown() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(
            store,
            [
                run_of_tests(1, "tr_1"),
                failure(2, "fail_t", test_run_id="tr_1"),
                failure(4, "fail_s", session_id="sess_1"),
            ],
        )
        shown = (await service.failures(P1, day(FAR), day(FAR))).failures
        assert {f.failure_id: f.resolution.status for f in shown} == {
            "fail_t": FixStatus.UNSUPPORTED,
            "fail_s": FixStatus.UNKNOWN,
        }
        by_id = {f.failure_id: f for f in shown}
        assert by_id["fail_t"].resolution.path is None
        assert by_id["fail_t"].resolution.reason is not None
        assert [(r.kind.value, r.run_id) for r in by_id["fail_t"].observed_in] == [
            ("test_run", "tr_1")
        ]
        assert by_id["fail_t"].observed_in[0].source_event_ids == (str(event_uuid(1)),)
        assert [(r.kind.value, r.run_id) for r in by_id["fail_s"].observed_in] == [
            ("session", "sess_1")
        ]

    with_service(body)


# --- hostile input, bounds and deadline ---


def test_injection_shaped_ids_are_data() -> None:
    hostile = "dec_a' }) DETACH DELETE n //"
    scope_hostile = "prj_1' OR true //"

    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(store, [decision(1, "dec_a"), failure(2, "fail_1", ci_run_id="ci_1")])
        with pytest.raises(TemporalDecisionNotFound):
            await service.decision_history(hostile, P1)
        assert (await service.decisions(TemporalScope(project_id=scope_hostile))).decisions == ()
        assert (await service.decisions(TemporalScope(repository_id=hostile))).decisions == ()
        assert (await service.failures(TemporalScope(project_id=scope_hostile))).failures == ()
        # an odd but legal ID is just an ID
        await project_event(store, decision(3, "dec:odd/../x+y@z"), ALL)
        assert await active(service, P1, 5) == ["dec:odd/../x+y@z", "dec_a"]
        assert (await service.decision_history("dec:odd/../x+y@z", P1)).entries[0].status == (
            "accepted"
        )
        assert await active(service, P1, 5) == ["dec:odd/../x+y@z", "dec_a"]  # nothing deleted

    with_service(body)


def test_result_caps_are_enforced_and_flagged(monkeypatch: pytest.MonkeyPatch) -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(
            store,
            [
                *[decision(n, f"dec_{n}") for n in range(1, 6)],
                *[failure(10 + n, f"fail_{n}", ci_run_id=f"ci_{n}") for n in range(1, 5)],
                decision(20, "dec_1", supersedes="dec_9", valid_from=1),
                decision(21, "dec_9", supersedes="dec_8", valid_from=1),
                decision(22, "dec_8", supersedes="dec_7", valid_from=1),
            ],
        )
        monkeypatch.setattr(temporal, "MAX_VERSIONS", 100)
        monkeypatch.setattr(temporal, "MAX_DECISIONS", 100)
        monkeypatch.setattr(temporal, "MAX_FAILURES", 100)
        monkeypatch.setattr(temporal, "MAX_CHAIN", 100)
        whole = await service.decisions(P1, day(FAR), day(FAR))
        assert not whole.truncated and len(whole.decisions) == 5
        assert not (await service.failures(P1, day(FAR), day(FAR))).truncated
        monkeypatch.setattr(temporal, "MAX_VERSIONS", 4)
        cut = await service.decisions(P1, day(FAR), day(FAR))
        assert cut.truncated  # 8 versions in scope
        monkeypatch.setattr(temporal, "MAX_VERSIONS", 8)
        assert not (await service.decisions(P1, day(FAR), day(FAR))).truncated  # exactly at it
        monkeypatch.setattr(temporal, "MAX_DECISIONS", 3)
        capped = await service.decisions(P1, day(FAR), day(FAR))
        assert capped.truncated and len(capped.decisions) == 3
        monkeypatch.setattr(temporal, "MAX_FAILURES", 2)
        failures = await service.failures(P1, day(FAR), day(FAR))
        assert failures.truncated and [f.failure_id for f in failures.failures] == [
            "fail_1",
            "fail_2",
        ]
        monkeypatch.setattr(temporal, "MAX_FAILURES", 4)
        assert not (await service.failures(P1, day(FAR), day(FAR))).truncated
        monkeypatch.setattr(temporal, "MAX_CHAIN", 1)
        history = await service.decision_history("dec_8", P1)
        assert history.truncated

    with_service(body)


class _StalledDriver:
    """A driver whose session never answers, to prove the client clock enforces the deadline."""

    def session(self, **_kwargs: Any) -> _StalledDriver:
        return self

    async def __aenter__(self) -> _StalledDriver:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute_read(self, *_args: Any, **_kwargs: Any) -> Any:
        await asyncio.sleep(3600)

    async def close(self) -> None:
        return None


def test_the_deadline_is_enforced_by_the_client_clock() -> None:
    async def run() -> None:
        service = TemporalService(_StalledDriver(), database="neo4j")  # type: ignore[arg-type]
        with pytest.raises(RetrievalDeadlineExceeded):
            await service.decisions(P1, deadline_seconds=0.05)
        with pytest.raises(RetrievalDeadlineExceeded):
            await service.decision_history("dec_a", P1, deadline_seconds=0.05)
        with pytest.raises(RetrievalDeadlineExceeded):
            await service.failures(P1, deadline_seconds=0.05)
        with pytest.raises(TemporalRequestError):
            await service.decisions(P1, deadline_seconds=0)

    asyncio.run(run())


def test_a_tiny_deadline_against_the_database_raises_and_the_service_recovers() -> None:
    async def body(store: Neo4jStore, service: TemporalService) -> None:
        await deliver(store, [decision(1, "dec_a")])
        cold = TemporalService.from_settings(neo4j_integration_settings())
        try:
            with pytest.raises(RetrievalDeadlineExceeded):
                await cold.decisions(P1, deadline_seconds=0.0001)
            assert [
                d.decision_id for d in (await cold.decisions(P1, deadline_seconds=30)).decisions
            ] == ["dec_a"]
        finally:
            await cold.close()

    with_service(body)
