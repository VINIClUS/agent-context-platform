"""Golden Cypher-state, replay, ordering and concurrency tests for the domain projectors."""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import LiteralString

import pytest
from agent_context_sdk import StoredEventV1

from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.projectors import lock_nodes
from agent_context_platform.projection.projectors.agent import AgentProjector
from agent_context_platform.projection.projectors.git import GitProjector
from agent_context_platform.projection.projectors.portfolio import PortfolioProjector
from agent_context_platform.projection.runtime import Projector

from ..conftest import neo4j_integration_settings
from .conftest import (
    SHA,
    TRANSACTION_ATTEMPTS,
    build_event,
    digest,
    graph_state,
    project_all,
    project_event,
    wipe_projected_graph,
)
from .fixtures import CALL, CALL_CONTEXT, CONTEXT, IDS, TURN_CONTEXT, session_events

pytestmark = pytest.mark.integration

GOLDEN = Path(__file__).parent / "golden" / "session.json"


def with_graph(body: Callable[[Neo4jStore], Awaitable[None]]) -> None:
    """Run `body` against a clean, schema-ready graph and wipe it afterwards."""

    async def run() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe_projected_graph(store)
            try:
                await body(store)
            finally:
                await wipe_projected_graph(store)

    asyncio.run(run())


async def state_after(store: Neo4jStore, events: list[StoredEventV1]) -> dict[str, list[dict]]:
    await wipe_projected_graph(store)
    await project_all(store, events)
    return await graph_state(store)


def test_forward_delivery_matches_the_golden_graph() -> None:
    async def body(store: Neo4jStore) -> None:
        await project_all(store, session_events())
        state = await graph_state(store)
        assert state == json.loads(GOLDEN.read_text())

    with_graph(body)


def test_replaying_the_fixture_twice_keeps_one_node_per_logical_id() -> None:
    async def body(store: Neo4jStore) -> None:
        events = session_events()
        await project_all(store, events)
        once = await graph_state(store)
        await project_all(store, events)
        twice = await graph_state(store)
        assert digest(twice) == digest(once)
        ids = [node["id"] for node in twice["nodes"]]
        assert len(ids) == len(set(ids)) == 11

    with_graph(body)


def test_every_relationship_carries_sorted_source_event_provenance() -> None:
    async def body(store: Neo4jStore) -> None:
        await project_all(store, session_events())
        for rel in (await graph_state(store))["relationships"]:
            ids = rel["props"]["source_event_ids"]
            assert ids, rel
            assert ids == sorted(set(ids)), rel

    with_graph(body)


def test_any_delivery_order_and_duplicates_give_the_same_digest() -> None:
    async def body(store: Neo4jStore) -> None:
        events = session_events()
        expected = digest(await state_after(store, events))
        shuffled = list(events) * 2
        random.Random(25).shuffle(shuffled)
        deliveries = {
            "reversed": list(reversed(events)),
            "shuffled with duplicates": shuffled,
            "reversed twice": list(reversed(events)) * 2,
        }
        for name, delivery in deliveries.items():
            assert digest(await state_after(store, delivery)) == expected, name

    with_graph(body)


def test_conflicting_snapshot_events_resolve_the_same_way_in_any_order() -> None:
    def snapshot(number: int, files: list[str], paths: list[str]) -> StoredEventV1:
        return build_event(
            number,
            "git.workspace_snapshot.captured",
            {
                "snapshot_id": "snap_1",
                "repository_id": "repo_1",
                "checkout_id": "co_1",
                "base_commit": None,
                "dirty_patch_sha256": SHA,
                "modified_content_sha256": files,
                "untracked_paths": paths,
            },
            context=CONTEXT,
        )

    async def body(store: Neo4jStore) -> None:
        events = [snapshot(20, ["a" * 64, "b" * 64], ["a.md", "z.md"]), snapshot(21, [], ["m.md"])]
        expected = digest(await state_after(store, events))
        for delivery in (list(reversed(events)), events * 2, list(reversed(events)) * 2):
            assert digest(await state_after(store, delivery)) == expected
        node = next(
            n for n in (await graph_state(store))["nodes"] if n["id"] == "WorkspaceSnapshot:snap_1"
        )
        assert node["props"]["untracked_paths"] == ["m.md"]
        assert node["props"]["modified_content_sha256"] == []

    with_graph(body)


REGISTRATION_ORDERS = {
    "portfolio-agent-git": (PortfolioProjector(), AgentProjector(), GitProjector()),
    "git-portfolio-agent": (GitProjector(), PortfolioProjector(), AgentProjector()),
    "agent-git-portfolio": (AgentProjector(), GitProjector(), PortfolioProjector()),
}


@pytest.mark.parametrize("order", REGISTRATION_ORDERS.values(), ids=REGISTRATION_ORDERS.keys())
def test_concurrent_workers_converge_without_deadlocks_in_any_registration_order(
    order: tuple[Projector, ...],
) -> None:
    async def body(store: Neo4jStore) -> None:
        events = session_events()
        expected = digest(await state_after(store, events))
        await wipe_projected_graph(store)
        TRANSACTION_ATTEMPTS[0] = 0
        await asyncio.gather(*(project_event(store, event, order) for event in events * 3))
        assert TRANSACTION_ATTEMPTS[0] == len(events) * 3, "a transaction was retried (deadlock)"
        assert digest(await graph_state(store)) == expected

    with_graph(body)


def test_hook_and_otel_observations_coalesce_without_dropping_either_event() -> None:
    async def body(store: Neo4jStore) -> None:
        events = session_events()
        await project_all(store, events)
        state = await graph_state(store)
        calls = [n for n in state["nodes"] if n["id"].startswith("ToolCall:")]
        assert len(calls) == 1
        invoked = next(r for r in state["relationships"] if r["type"] == "INVOKED")
        started, observed, completed = (events[4], events[5], events[6])
        assert invoked["props"]["source_event_ids"] == sorted(
            str(e.event_id) for e in (started, observed, completed)
        )

    with_graph(body)


def test_a_later_hook_event_never_erases_or_sets_an_outcome() -> None:
    async def body(store: Neo4jStore) -> None:
        completed = build_event(
            1,
            "agent.tool_call.completed",
            {
                "source": "codex_otel",
                **CALL,
                "success": False,
                "error_class": "Timeout",
                "output_content_id": "out_1",
            },
            context=CALL_CONTEXT,
        )
        observed = build_event(
            2,
            "agent.tool_call.output_observed",
            {"source": "codex_hook", **CALL, "output_content_id": "out_1"},
            context=CALL_CONTEXT,
        )
        turn_completed = build_event(
            3,
            "agent.turn.completed",
            {"source": "codex_otel", **IDS, "success": False, "error_class": "Timeout"},
            context=TURN_CONTEXT,
        )
        turn_stopped = build_event(
            4,
            "agent.turn.stopped",
            {"source": "codex_hook", **IDS, "stop_hook_active": False},
            context=TURN_CONTEXT,
        )
        for delivery in ([completed, observed], [observed, completed]):
            await wipe_projected_graph(store)
            await project_all(store, [*delivery, turn_completed, turn_stopped])
            nodes = {n["id"]: n["props"] for n in (await graph_state(store))["nodes"]}
            call = next(p for i, p in nodes.items() if i.startswith("ToolCall:"))
            turn = next(p for i, p in nodes.items() if i.startswith("Turn:"))
            assert call["success"] is False and call["error_class"] == "Timeout"
            assert call["status"] == "completed"
            assert turn["success"] is False and turn["error_class"] == "Timeout"
            assert turn["status"] == "completed"
            assert "stopped_at" in turn

        # A hook-only tool call has no outcome at all.
        await wipe_projected_graph(store)
        await project_event(store, observed)
        nodes = [
            n["props"]
            for n in (await graph_state(store))["nodes"]
            if n["id"].startswith("ToolCall:")
        ]
        assert "success" not in nodes[0] and "error_class" not in nodes[0]
        assert nodes[0]["status"] == "output_observed"

    with_graph(body)


def test_the_same_oid_in_two_repositories_is_two_commits() -> None:
    async def body(store: Neo4jStore) -> None:
        events = []
        for number, repo in ((1, "repo_1"), (2, "repo_2")):
            events.append(
                build_event(
                    number,
                    "git.commit.observed",
                    {
                        "repository_id": repo,
                        "commit_id": "a" * 40,
                        "tree_id": "c" * 40,
                        "authored_at": "2026-08-13T13:00:10Z",
                        "committed_at": "2026-08-13T13:00:11Z",
                    },
                    context={"repository_id": repo},
                )
            )
        await project_all(store, events)
        ids = [
            n["id"] for n in (await graph_state(store))["nodes"] if n["id"].startswith("Commit:")
        ]
        assert ids == [f"Commit:commit:6:repo_1:{'a' * 40}", f"Commit:commit:6:repo_2:{'a' * 40}"]

    with_graph(body)


def test_the_graph_holds_content_ids_and_never_content_text() -> None:
    async def body(store: Neo4jStore) -> None:
        await project_all(store, session_events())
        state = await graph_state(store)
        text = json.dumps(state)
        for content_id in ("msg_user_1", "in_1", "out_1", "msg_asst_1", "msg_commit_1"):
            assert content_id in text
        entities = [*state["nodes"], *state["relationships"]]
        for props in (entity["props"] for entity in entities):
            for key in props:
                if "content" in key or "message" in key:
                    assert key.endswith(("_content_id", "_content_sha256")), key

    with_graph(body)


_SLOW_POINTER_UPDATE: LiteralString = (
    "MERGE (n:Checkout {checkout_id: $node_id}) "
    "WITH n, collect(n.head_order) AS seen "
    "UNWIND range(1, $stall) AS step "
    "WITH n, seen, count(step) AS steps "
    "WITH n, (size(seen) = 0 OR $order > seen[0]) AS newer "
    "SET n.head_commit = CASE WHEN newer THEN $head_commit ELSE n.head_commit END, "
    "n.head_order = CASE WHEN newer THEN $order ELSE n.head_order END"
)


async def _race_two_pointer_updates(store: Neo4jStore, *, lock: bool) -> str:
    """The older worker decides "I am newer" and stalls before writing; the newer one runs.

    The statement has the same shape as `newest_wins` (read the pointer, compare,
    write) but is stretched so the interleaving is deterministic instead of a
    microsecond window.
    """

    async def create(tx: Neo4jTransaction) -> None:
        # MERGE that creates the node locks it via its uniqueness constraint,
        # which would serialize the workers by itself. Race on an existing node.
        await tx.run("MERGE (n:Checkout {checkout_id: 'co_1'})", parameters={})

    await store.execute_write(create)
    started = asyncio.Event()

    def worker(order: str, head: str, stall: int) -> Awaitable[None]:
        async def run(tx: Neo4jTransaction) -> None:
            if lock:
                await lock_nodes(tx, [("Checkout", "co_1")])
            started.set()
            await tx.run(
                _SLOW_POINTER_UPDATE,
                parameters={
                    "node_id": "co_1",
                    "order": order,
                    "head_commit": head,
                    "stall": stall,
                },
            )

        return store.execute_write(run)

    older = asyncio.create_task(worker("1", "older", 10_000_000))
    await started.wait()
    await asyncio.sleep(0.5)
    await worker("2", "newer", 1)
    await older
    nodes = {n["id"]: n["props"] for n in (await graph_state(store))["nodes"]}
    return str(nodes["Checkout:co_1"]["head_commit"])


def test_the_node_lock_stops_an_older_worker_overwriting_a_newer_pointer() -> None:
    async def body(store: Neo4jStore) -> None:
        assert await _race_two_pointer_updates(store, lock=True) == "newer"
        await wipe_projected_graph(store)
        # Control: the same interleaving without the lock loses the update, so
        # the assertion above is only satisfied because `lock_nodes` serializes.
        assert await _race_two_pointer_updates(store, lock=False) == "older"

    with_graph(body)
