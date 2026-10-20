"""Golden Cypher-state, replay, ordering and supersession tests for the knowledge projector."""

from __future__ import annotations

import asyncio
import itertools
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

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

from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.projectors.knowledge import (
    KnowledgeProjector,
    active_decisions,
    decision_version,
)
from agent_context_platform.projection.projectors.quality import QualityProjector

from ..conftest import neo4j_integration_settings
from .conftest import (
    NODE_KEYS,
    PROJECTORS,
    digest,
    event_uuid,
    graph_state,
    project_event,
    wipe_projected_graph,
)

pytestmark = pytest.mark.integration

# The shared state helpers only see the labels listed here; these suites add the knowledge ones.
NODE_KEYS.update(
    {
        "Decision": "decision_id",
        "Constraint": "constraint_id",
        "Failure": "failure_id",
        "Summary": "summary_id",
        "TestRun": "test_run_id",
        "CIRun": "ci_run_id",
        "Finding": "finding_id",
    }
)
ALL = (*PROJECTORS, KnowledgeProjector(), QualityProjector())
T0 = datetime(2026, 8, 13, 13, 0, 0, tzinfo=UTC)
SHA = "e" * 64


def iso(days: int) -> str:
    return (T0 + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def ts(days: int) -> str:
    return (T0 + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000000Z")


def make(
    number: int,
    event_type: str,
    payload: dict[str, object],
    *,
    observed: int | None = None,
    context: dict[str, str] | None = None,
) -> StoredEventV1:
    """A sealed event whose `observed_at` (days) can differ from its `occurred_at` (seconds)."""
    draft = EventDraftV1(
        event_id=event_uuid(number),
        event_type=event_type,
        stream_id=f"stream-{number}",
        occurred_at=T0 + timedelta(seconds=number),
        observed_at=T0 + timedelta(days=number if observed is None else observed),
        producer=ProducerV1(producer_id="projector-test", name="projector-test", version="1.0.0"),
        context=EventContextV1(**(context or {})),
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
    valid_from: int = 0,
    valid_to: int | None = None,
) -> StoredEventV1:
    return make(
        number,
        "knowledge.decision.recorded",
        {
            "decision_id": decision_id,
            "status": status,
            "supersedes_id": supersedes,
            "subjects": ["mod_a", "mod_b"],
            "content_id": f"content_{decision_id}",
            "valid_from": iso(valid_from),
            "valid_to": None if valid_to is None else iso(valid_to),
            "recorded_at": iso(number),
        },
    )


def chain() -> list[StoredEventV1]:
    """A is superseded by B, which is superseded by C."""
    return [
        decision(1, "dec_a", valid_from=0),
        decision(2, "dec_b", supersedes="dec_a", valid_from=10),
        decision(3, "dec_c", supersedes="dec_b", valid_from=20),
    ]


def knowledge_events() -> list[StoredEventV1]:
    return [
        *chain(),
        make(
            4,
            "knowledge.constraint.recorded",
            {
                "constraint_id": "con_1",
                "supersedes_id": "con_0",
                "subjects": ["mod_a"],
                "content_id": "content_con_1",
                "valid_from": iso(1),
                "recorded_at": iso(4),
            },
        ),
        make(
            5,
            "knowledge.summary.recorded",
            {
                "summary_id": "sum_1",
                "source_event_ids": [str(event_uuid(1)), str(event_uuid(2))],
                "subjects": ["mod_a"],
                "content_id": "content_sum_1",
                "valid_from": iso(2),
                "valid_to": iso(30),
                "recorded_at": iso(5),
            },
        ),
        make(
            6,
            "knowledge.failure.observed",
            {
                "failure_id": "fail_1",
                "component": "api",
                "operation": "ingest",
                "error_class": "Timeout",
                "fingerprint_version": "1",
                "fingerprint_sha256": SHA,
                "session_id": "sess_1",
                "test_run_id": "tr_1",
                "ci_run_id": "ci_1",
                "turn_id": "turn_1",
                "details_content_id": "content_fail_1",
            },
        ),
    ]


def with_graph(body: Callable[[Neo4jStore], Awaitable[None]]) -> None:
    async def run() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe_projected_graph(store)
            try:
                await body(store)
            finally:
                await wipe_projected_graph(store)

    asyncio.run(run())


async def deliver(store: Neo4jStore, events: list[StoredEventV1]) -> dict[str, list[dict]]:
    await wipe_projected_graph(store)
    for event in events:
        await project_event(store, event, ALL)
    return await graph_state(store)


def node(state: dict[str, list[dict]], node_id: str) -> dict[str, Any]:
    return next(item["props"] for item in state["nodes"] if item["id"] == node_id)


def rels(state: dict[str, list[dict]], rel_type: str) -> list[tuple[str, str, dict[str, Any]]]:
    return [
        (item["from"], item["to"], item["props"])
        for item in state["relationships"]
        if item["type"] == rel_type
    ]


def test_golden_state_of_each_knowledge_event_type() -> None:
    async def body(store: Neo4jStore) -> None:
        state = await deliver(store, knowledge_events())
        assert node(state, "Decision:dec_a") == {
            "decision_id": "dec_a",
            "status": "accepted",
            "subjects": ["mod_a", "mod_b"],
            "content_id": "content_dec_a",
            "valid_from": ts(0),
            "recorded_at": ts(1),
            "payload_recorded_at": ts(1),
            "state_order": f"2026-08-13T13:00:01.000000|{event_uuid(1)}",
            "superseded_at": ts(10),
            "effective_valid_to": ts(10),
        }
        assert node(state, "Decision:dec_c") == {
            "decision_id": "dec_c",
            "status": "accepted",
            "subjects": ["mod_a", "mod_b"],
            "content_id": "content_dec_c",
            "supersedes_id": "dec_b",
            "valid_from": ts(20),
            "recorded_at": ts(3),
            "payload_recorded_at": ts(3),
            "state_order": f"2026-08-13T13:00:03.000000|{event_uuid(3)}",
        }
        assert node(state, "Constraint:con_1") == {
            "constraint_id": "con_1",
            "subjects": ["mod_a"],
            "content_id": "content_con_1",
            "supersedes_id": "con_0",
            "valid_from": ts(1),
            "recorded_at": ts(4),
            "payload_recorded_at": ts(4),
            "state_order": f"2026-08-13T13:00:04.000000|{event_uuid(4)}",
        }
        assert node(state, "Summary:sum_1") == {
            "summary_id": "sum_1",
            "subjects": ["mod_a"],
            "content_id": "content_sum_1",
            "grounded_event_ids": [str(event_uuid(1)), str(event_uuid(2))],
            "valid_from": ts(2),
            "valid_to": ts(30),
            "recorded_at": ts(5),
            "payload_recorded_at": ts(5),
            "state_order": f"2026-08-13T13:00:05.000000|{event_uuid(5)}",
        }
        assert node(state, "Failure:fail_1") == {
            "failure_id": "fail_1",
            "component": "api",
            "operation": "ingest",
            "error_class": "Timeout",
            "fingerprint_version": "1",
            "fingerprint_sha256": SHA,
            "details_content_id": "content_fail_1",
            "valid_from": "2026-08-13T13:00:06.000000Z",
            "recorded_at": ts(6),
        }

    with_graph(body)


def test_decisions_are_linked_by_supersedes_and_nothing_is_deleted() -> None:
    async def body(store: Neo4jStore) -> None:
        state = await deliver(store, knowledge_events())
        supersedes = rels(state, "SUPERSEDES")
        assert [(a, b) for a, b, _ in supersedes] == [
            ("Decision:dec_b", "Decision:dec_a"),
            ("Decision:dec_c", "Decision:dec_b"),
        ]
        assert [props["source_event_ids"] for _, _, props in supersedes] == [
            [str(event_uuid(2))],
            [str(event_uuid(3))],
        ]
        assert node(state, "Decision:dec_b")["effective_valid_to"] == ts(20)
        assert "effective_valid_to" not in node(state, "Decision:dec_c")
        assert not rels(state, "AFFECTS")

    with_graph(body)


def test_a_failure_links_to_the_session_test_run_and_ci_run_it_was_seen_in() -> None:
    async def body(store: Neo4jStore) -> None:
        state = await deliver(store, knowledge_events())
        observed = rels(state, "OBSERVED_IN")
        assert [(a, b) for a, b, _ in observed] == [
            ("Failure:fail_1", "CIRun:ci_1"),
            ("Failure:fail_1", "Session:sess_1"),
            ("Failure:fail_1", "TestRun:tr_1"),
        ]
        assert all(props["source_event_ids"] == [str(event_uuid(6))] for _, _, props in observed)

    with_graph(body)


def test_every_supersession_chain_order_converges_and_closes_each_decision() -> None:
    async def body(store: Neo4jStore) -> None:
        expected = digest(await deliver(store, chain()))
        for permutation in itertools.permutations(chain()):
            state = await deliver(store, list(permutation))
            assert digest(state) == expected, [str(e.event_id)[-1] for e in permutation]
            assert node(state, "Decision:dec_a")["effective_valid_to"] == ts(10)
            assert node(state, "Decision:dec_b")["effective_valid_to"] == ts(20)
            assert len([n for n in state["nodes"] if n["id"].startswith("Decision:")]) == 3

    with_graph(body)


def test_a_superseding_decision_delivered_first_still_closes_a_late_decision() -> None:
    async def body(store: Neo4jStore) -> None:
        await deliver(store, [])
        await project_event(store, decision(2, "dec_b", supersedes="dec_a", valid_from=10), ALL)
        early = await graph_state(store)
        assert node(early, "Decision:dec_a") == {
            "decision_id": "dec_a",
            "superseded_at": ts(10),
            "effective_valid_to": ts(10),
        }
        await project_event(store, decision(1, "dec_a", valid_from=0), ALL)
        late = await graph_state(store)
        assert node(late, "Decision:dec_a")["content_id"] == "content_dec_a"
        assert node(late, "Decision:dec_a")["effective_valid_to"] == ts(10)

    with_graph(body)


def test_a_decision_keeps_its_own_earlier_valid_to_and_a_proposed_one_closes_nothing() -> None:
    async def body(store: Neo4jStore) -> None:
        state = await deliver(
            store,
            [
                decision(1, "dec_a", valid_from=0, valid_to=5),
                decision(2, "dec_b", supersedes="dec_a", valid_from=10),
                decision(3, "dec_x", status="proposed", supersedes="dec_y", valid_from=3),
            ],
        )
        assert node(state, "Decision:dec_a")["effective_valid_to"] == ts(5)
        assert node(state, "Decision:dec_a")["superseded_at"] == ts(10)
        assert not [r for r in rels(state, "SUPERSEDES") if r[0] == "Decision:dec_x"]

    with_graph(body)


def test_a_later_recording_of_a_decision_wins_in_any_delivery_order() -> None:
    async def body(store: Neo4jStore) -> None:
        proposed = decision(1, "dec_a", status="proposed")
        accepted = decision(2, "dec_a", status="accepted")
        for order in ([proposed, accepted], [accepted, proposed]):
            state = await deliver(store, order)
            assert node(state, "Decision:dec_a")["status"] == "accepted"
            assert node(state, "Decision:dec_a")["recorded_at"] == ts(2)

    with_graph(body)


def test_replaying_twice_keeps_one_node_per_logical_id_and_sorted_provenance() -> None:
    async def body(store: Neo4jStore) -> None:
        events = knowledge_events()
        once = digest(await deliver(store, events))
        for event in events:
            await project_event(store, event, ALL)
        twice = await graph_state(store)
        assert digest(twice) == once
        ids = [item["id"] for item in twice["nodes"]]
        assert len(ids) == len(set(ids))
        for rel in twice["relationships"]:
            assert rel["props"]["source_event_ids"] == sorted(set(rel["props"]["source_event_ids"]))

    with_graph(body)


def test_occurred_observed_reversed_shuffled_and_duplicated_delivery_agree() -> None:
    async def body(store: Neo4jStore) -> None:
        events = knowledge_events()
        expected = digest(await deliver(store, events))
        shuffled = events * 2
        random.Random(38).shuffle(shuffled)
        deliveries = {
            "observed": sorted(events, key=lambda e: e.observed_at),
            "reversed": list(reversed(events)),
            "shuffled with duplicates": shuffled,
        }
        for name, delivery in deliveries.items():
            assert digest(await deliver(store, delivery)) == expected, name

    with_graph(body)


def reopening_events() -> tuple[StoredEventV1, StoredEventV1, StoredEventV1]:
    """A, its accepted superseder B, and B re-recorded as rejected."""
    return (
        decision(1, "dec_a", valid_from=0),
        decision(2, "dec_b", supersedes="dec_a", valid_from=10),
        decision(3, "dec_b", supersedes="dec_a", valid_from=10, status="rejected"),
    )


def test_a_superseder_re_recorded_as_rejected_reopens_the_decision_in_any_order() -> None:
    async def body(store: Neo4jStore) -> None:
        a, accepted, rejected = reopening_events()
        expected = digest(await deliver(store, [a, accepted, rejected]))
        for order in itertools.permutations([a, accepted, rejected]):
            state = await deliver(store, list(order))
            assert digest(state) == expected, [str(e.event_id)[-1] for e in order]
        assert not rels(state, "SUPERSEDES")
        assert "effective_valid_to" not in node(state, "Decision:dec_a")
        assert "superseded_at" not in node(state, "Decision:dec_a")
        assert node(state, "Decision:dec_b")["status"] == "rejected"
        # Accepted again later: the edge and the closure come back, whatever the order.
        again = decision(4, "dec_b", supersedes="dec_a", valid_from=12)
        for order in itertools.permutations([a, accepted, rejected, again]):
            state = await deliver(store, list(order))
            assert node(state, "Decision:dec_a")["effective_valid_to"] == ts(12)
            assert [(x, y) for x, y, _ in rels(state, "SUPERSEDES")] == [
                ("Decision:dec_b", "Decision:dec_a")
            ]

    with_graph(body)


def test_a_dropped_supersession_leaves_no_stub_of_a_decision_that_never_arrived() -> None:
    async def body(store: Neo4jStore) -> None:
        _, accepted, rejected = reopening_events()
        both = digest(await deliver(store, [accepted, rejected]))
        assert digest(await deliver(store, [rejected, accepted])) == both
        state = await deliver(store, [accepted, rejected])
        assert [item["id"] for item in state["nodes"]] == ["Decision:dec_b"]
        # While the edge stands, the stub is there and carries the closure.
        assert node(await deliver(store, [accepted]), "Decision:dec_a")["superseded_at"] == ts(10)

    with_graph(body)


def test_a_superseder_re_pointed_or_re_proposed_reopens_the_old_target() -> None:
    async def body(store: Neo4jStore) -> None:
        events = [
            decision(1, "dec_a", valid_from=0),
            decision(2, "dec_x", valid_from=0),
            decision(3, "dec_b", supersedes="dec_a", valid_from=10),
            decision(4, "dec_b", supersedes="dec_x", valid_from=11),
            decision(5, "dec_c", supersedes="dec_x", valid_from=15, status="proposed"),
        ]
        expected = digest(await deliver(store, events))
        shuffled = events * 2
        random.Random(40).shuffle(shuffled)
        for delivery in (list(reversed(events)), shuffled):
            assert digest(await deliver(store, delivery)) == expected
        state = await deliver(store, events)
        assert "effective_valid_to" not in node(state, "Decision:dec_a")
        assert node(state, "Decision:dec_x")["effective_valid_to"] == ts(11)
        assert [(x, y) for x, y, _ in rels(state, "SUPERSEDES")] == [
            ("Decision:dec_b", "Decision:dec_x")
        ]

    with_graph(body)


def graph_active(state: dict[str, list[dict]], scope: str, valid_day: int) -> list[str]:
    """The decisions the graph's own `effective_valid_to` says are active (the as-of rule)."""
    at = ts(valid_day)
    found = []
    for item in state["nodes"]:
        props = item["props"]
        if not item["id"].startswith("Decision:") or "state_order" not in props:
            continue
        end = props.get("effective_valid_to")
        if props["status"] not in ("accepted", "superseded"):
            continue
        if props["status"] == "superseded" and end is None:
            continue
        if scope in props["subjects"] and props["valid_from"] <= at and (end is None or at < end):
            found.append(props["decision_id"])
    return sorted(found)


def test_active_decisions_agrees_with_the_graph_closure_at_several_as_of_points() -> None:
    ledger = [
        decision(1, "dec_a", valid_from=0),
        decision(2, "dec_b", supersedes="dec_a", valid_from=10),
        decision(3, "dec_c", supersedes="dec_b", valid_from=20),
        decision(4, "dec_d", valid_from=2, valid_to=12),
        decision(5, "dec_e", supersedes="dec_d", valid_from=8),
        decision(6, "dec_c", supersedes="dec_b", valid_from=20, status="rejected"),
        decision(7, "dec_f", supersedes="dec_a", valid_from=4, status="superseded"),
    ]

    async def body(store: Neo4jStore) -> None:
        versions = [decision_version(event) for event in ledger]
        for delivery in (ledger, list(reversed(ledger))):
            state = await deliver(store, delivery)
            for valid_day in (-1, 0, 3, 5, 9, 10, 11, 12, 15, 20, 25):
                expected = active_decisions(
                    versions,
                    scope="mod_a",
                    valid_at=T0 + timedelta(days=valid_day),
                    recorded_at=T0 + timedelta(days=365),
                )
                assert graph_active(state, "mod_a", valid_day) == expected, valid_day

    with_graph(body)
