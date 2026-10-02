"""Golden Cypher-state, replay and ordering tests for the quality projector."""

from __future__ import annotations

import itertools
import random

import pytest
from agent_context_sdk import StoredEventV1

from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.projectors.git import commit_node_id

from .conftest import OID_A, OID_B, SHA, digest, event_uuid, project_event
from .test_knowledge import ALL, deliver, make, node, rels, with_graph

pytestmark = pytest.mark.integration

REPO = {"repository_id": "repo_1"}
COUNTS = {
    "total_count": 3,
    "passed_count": 2,
    "failed_count": 1,
    "skipped_count": 0,
    "error_count": 0,
}


def quality_events() -> list[StoredEventV1]:
    return [
        make(
            1,
            "quality.test_run.completed",
            {
                "test_run_id": "tr_1",
                "framework": "pytest",
                "status": "failed",
                "duration_ms": 120,
                "output_content_id": "out_tr_1",
                "commit_id": OID_B,
                **COUNTS,
            },
            context=REPO,
        ),
        make(
            2,
            "quality.test_run.completed",
            {
                "test_run_id": "tr_2",
                "framework": "pytest",
                "status": "skipped",
                "duration_ms": 1,
                "snapshot_id": "snap_1",
                "total_count": 2,
                "passed_count": 0,
                "failed_count": 0,
                "skipped_count": 2,
                "error_count": 0,
            },
        ),
        make(
            3,
            "quality.ci_run.completed",
            {
                "ci_run_id": "ci_1",
                "provider": "github",
                "workflow": "ci",
                "job": "test",
                "external_id": "9001",
                "status": "failure",
                "duration_ms": 5000,
                "error_class": "TestFailure",
                "commit_id": OID_A,
            },
            context=REPO,
        ),
        make(
            4,
            "quality.finding.observed",
            {
                "finding_id": "find_1",
                "scanner": "bandit",
                "rule_id": "B101",
                "severity": "low",
                "status": "open",
                "fingerprint_sha256": SHA,
                "path": "pkg/calc.py",
                "details_content_id": "content_find_1",
                "commit_id": OID_B,
            },
            context=REPO,
        ),
        make(
            5,
            "knowledge.failure.observed",
            {
                "failure_id": "fail_1",
                "component": "pytest",
                "operation": "run",
                "error_class": "AssertionError",
                "fingerprint_version": "1",
                "fingerprint_sha256": SHA,
                "test_run_id": "tr_1",
                "ci_run_id": "ci_1",
            },
        ),
    ]


def test_golden_state_of_each_quality_event_type() -> None:
    async def body(store: Neo4jStore) -> None:
        state = await deliver(store, quality_events())
        assert node(state, "TestRun:tr_1") == {
            "test_run_id": "tr_1",
            "framework": "pytest",
            "status": "failed",
            "duration_ms": 120,
            "output_content_id": "out_tr_1",
            "commit_id": OID_B,
            "completed_at": "2026-08-13T13:00:01.000000Z",
            "repository_id": "repo_1",
            "result_order": f"2026-08-13T13:00:01.000000|{event_uuid(1)}",
            **COUNTS,
        }
        assert node(state, "TestRun:tr_2")["snapshot_id"] == "snap_1"
        assert "commit_id" not in node(state, "TestRun:tr_2")
        assert node(state, "CIRun:ci_1") == {
            "ci_run_id": "ci_1",
            "provider": "github",
            "workflow": "ci",
            "job": "test",
            "external_id": "9001",
            "status": "failure",
            "duration_ms": 5000,
            "error_class": "TestFailure",
            "commit_id": OID_A,
            "completed_at": "2026-08-13T13:00:03.000000Z",
            "recorded_at": "2026-08-16T13:00:00.000000Z",
            "repository_id": "repo_1",
            "result_order": f"2026-08-13T13:00:03.000000|{event_uuid(3)}",
        }
        assert node(state, "Finding:find_1") == {
            "finding_id": "find_1",
            "scanner": "bandit",
            "rule_id": "B101",
            "severity": "low",
            "status": "open",
            "fingerprint_sha256": SHA,
            "path": "pkg/calc.py",
            "details_content_id": "content_find_1",
            "commit_id": OID_B,
            "observed_at": "2026-08-13T13:00:04.000000Z",
            "result_order": f"2026-08-13T13:00:04.000000|{event_uuid(4)}",
        }

    with_graph(body)


def test_a_test_run_validates_its_commit_or_snapshot() -> None:
    async def body(store: Neo4jStore) -> None:
        state = await deliver(store, quality_events())
        validates = rels(state, "VALIDATES")
        assert [(a, b) for a, b, _ in validates] == [
            ("TestRun:tr_1", f"Commit:{commit_node_id('repo_1', OID_B)}"),
            ("TestRun:tr_2", "WorkspaceSnapshot:snap_1"),
        ]
        assert all(props["source_event_ids"] for _, _, props in validates)
        commit = node(state, f"Commit:{commit_node_id('repo_1', OID_B)}")
        assert commit == {
            "commit_id": commit_node_id("repo_1", OID_B),
            "repository_id": "repo_1",
            "oid": OID_B,
        }

    with_graph(body)


def test_a_failure_is_linked_to_the_test_run_and_ci_run_whichever_arrives_first() -> None:
    async def body(store: Neo4jStore) -> None:
        events = quality_events()
        failure_first = [events[4], *events[:4]]
        state = await deliver(store, failure_first)
        assert [(a, b) for a, b, _ in rels(state, "OBSERVED_IN")] == [
            ("Failure:fail_1", "CIRun:ci_1"),
            ("Failure:fail_1", "TestRun:tr_1"),
        ]
        assert node(state, "TestRun:tr_1")["framework"] == "pytest"
        assert node(state, "CIRun:ci_1")["provider"] == "github"
        assert digest(state) == digest(await deliver(store, events))

    with_graph(body)


def test_the_newest_result_of_a_run_wins_in_any_delivery_order() -> None:
    def run(number: int, status: str, counts: dict[str, int]) -> StoredEventV1:
        return make(
            number,
            "quality.test_run.completed",
            {
                "test_run_id": "tr_1",
                "framework": "pytest",
                "status": status,
                "duration_ms": number,
                "commit_id": OID_A,
                **counts,
            },
            context=REPO,
        )

    async def body(store: Neo4jStore) -> None:
        failed = run(1, "failed", {**COUNTS})
        passed = run(
            2,
            "passed",
            {
                "total_count": 3,
                "passed_count": 3,
                "failed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
            },
        )
        for order in ([failed, passed], [passed, failed]):
            state = await deliver(store, order)
            assert node(state, "TestRun:tr_1")["status"] == "passed"
            assert node(state, "TestRun:tr_1")["duration_ms"] == 2

    with_graph(body)


def test_replay_and_any_delivery_order_give_one_node_per_id_and_one_digest() -> None:
    async def body(store: Neo4jStore) -> None:
        events = quality_events()
        expected = digest(await deliver(store, events))
        for event in events:
            await project_event(store, event, ALL)
        again = await deliver(store, events + events)
        ids = [item["id"] for item in again["nodes"]]
        assert len(ids) == len(set(ids))
        shuffled = events * 2
        random.Random(39).shuffle(shuffled)
        deliveries = {
            "observed": sorted(events, key=lambda e: e.observed_at),
            "reversed": list(reversed(events)),
            "shuffled with duplicates": shuffled,
        }
        assert digest(again) == expected
        for name, delivery in deliveries.items():
            assert digest(await deliver(store, delivery)) == expected, name

    with_graph(body)


def observed(number: int, **target: str) -> StoredEventV1:
    return make(
        number,
        "quality.test_run.completed",
        {
            "test_run_id": "tr_1",
            "framework": "pytest",
            "status": "passed",
            "duration_ms": number,
            "total_count": 1,
            "passed_count": 1,
            "failed_count": 0,
            "skipped_count": 0,
            "error_count": 0,
            **target,
        },
        context=REPO,
    )


def test_a_newer_observation_replaces_the_stale_validates_edges_in_any_order() -> None:
    async def body(store: Neo4jStore) -> None:
        first = observed(1, commit_id=OID_A)
        second = observed(2, commit_id=OID_B)
        third = observed(3, snapshot_id="snap_1")
        moves = [first, second, third]
        expected = digest(await deliver(store, moves))
        for order in itertools.permutations(moves):
            state = await deliver(store, list(order))
            assert digest(state) == expected, [str(e.event_id)[-1] for e in order]
        assert [(a, b) for a, b, _ in rels(state, "VALIDATES")] == [
            ("TestRun:tr_1", "WorkspaceSnapshot:snap_1")
        ]
        assert node(state, "TestRun:tr_1")["snapshot_id"] == "snap_1"
        assert "commit_id" not in node(state, "TestRun:tr_1")
        # The commits and the repository only the dropped edges kept alive are gone too.
        assert [item["id"] for item in state["nodes"]] == [
            "TestRun:tr_1",
            "WorkspaceSnapshot:snap_1",
        ]
        commits = await deliver(store, [first, second])
        assert [(a, b) for a, b, _ in rels(commits, "VALIDATES")] == [
            ("TestRun:tr_1", f"Commit:{commit_node_id('repo_1', OID_B)}")
        ]
        assert [b for _, b, _ in rels(commits, "IN_REPOSITORY")] == ["Repository:repo_1"]
        assert commit_node_id("repo_1", OID_A) not in str(commits["nodes"])

    with_graph(body)


def test_re_observing_the_same_target_keeps_one_edge_with_the_newest_provenance() -> None:
    async def body(store: Neo4jStore) -> None:
        first = observed(1, commit_id=OID_A)
        second = observed(2, commit_id=OID_A)
        for order in ([first, second], [second, first], [first, second, first]):
            state = await deliver(store, order)
            ((_, _, props),) = rels(state, "VALIDATES")
            assert props["source_event_ids"] == [str(event_uuid(2))]
            ((_, _, repo_props),) = rels(state, "IN_REPOSITORY")
            assert repo_props["source_event_ids"] == [str(event_uuid(2))]

    with_graph(body)
