"""Initial domain projectors and the write rules they share.

Delivery is at-least-once and unordered, so every write here is idempotent and
commutative: the final graph never depends on arrival order.

- Immutable facts (`started_at`, `model`, content IDs) use "smallest non-null
  wins" (`MIN_NON_NULL`), which commutes.
- Mutable current pointers (checkout head, session end, turn outcome) sit next
  to an `<group>_order` key and only move to an event that is newer by the total
  order `(occurred_at, event_id)` (`event_order`).
- Lifecycle `status` advances by rank and never regresses (`STATUS_RANK`).
- Every relationship keeps a sorted, de-duplicated `source_event_ids` list
  (`ASSERT_RELATIONSHIP`), so a hook and an OTel event asserting the same edge
  coalesce without either source event being dropped.
- Only content IDs enter the graph, never content bytes or text.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC
from typing import LiteralString

from agent_context_sdk import StoredEventV1

from agent_context_platform.projection.neo4j import Neo4jTransaction

_LOCK_STATEMENTS: dict[str, LiteralString] = {
    "Branch": "MERGE (n:Branch {branch_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "CIRun": "MERGE (n:CIRun {ci_run_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Checkout": "MERGE (n:Checkout {checkout_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Commit": "MERGE (n:Commit {commit_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Constraint": (
        "MERGE (n:Constraint {constraint_id: $node_id}) SET n._lock = true REMOVE n._lock"
    ),
    "Decision": "MERGE (n:Decision {decision_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "DecisionVersion": (
        "MERGE (n:DecisionVersion {event_id: $node_id}) SET n._lock = true REMOVE n._lock"
    ),
    "Failure": "MERGE (n:Failure {failure_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "FailureObservation": (
        "MERGE (n:FailureObservation {event_id: $node_id}) SET n._lock = true REMOVE n._lock"
    ),
    "Finding": "MERGE (n:Finding {finding_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Project": "MERGE (n:Project {project_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Repository": "MERGE (n:Repository {repository_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Session": "MERGE (n:Session {session_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Summary": "MERGE (n:Summary {summary_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "TestRun": "MERGE (n:TestRun {test_run_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "ToolCall": "MERGE (n:ToolCall {tool_call_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Turn": "MERGE (n:Turn {turn_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "Workspace": "MERGE (n:Workspace {workspace_id: $node_id}) SET n._lock = true REMOVE n._lock",
    "WorkspaceSnapshot": (
        "MERGE (n:WorkspaceSnapshot {snapshot_id: $node_id}) SET n._lock = true REMOVE n._lock"
    ),
}


async def lock_nodes(tx: Neo4jTransaction, nodes: Iterable[tuple[str, str]]) -> None:
    """Take write locks on every `(label, id)` node an event touches, before any write.

    Locks are taken in one global order (label, then id), and each projector calls
    this first with everything it will touch. Two workers therefore queue on the
    first node they share instead of each holding one the other wants (deadlock),
    and the read-compare-write of a current pointer runs with its node locked, so
    it cannot lose an update to a competing worker. The `_lock` property is removed
    in the same statement and is never committed.
    """
    for label, node_id in sorted(set(nodes)):
        await tx.run(_LOCK_STATEMENTS[label], parameters={"node_id": node_id})


def event_lock_keys(event: StoredEventV1) -> list[tuple[str, str]]:
    """Sorted `(label, id)` union of the nodes ANY projector touches for `event`.

    `ProjectionRunner` runs the matching projectors in the caller's registration
    order inside one transaction. Each projector locks this full union first, so
    the global lock order holds whichever runs first (later locks are no-ops).
    Each projector derives its part from the same ID helpers it writes with.
    """
    # Imported here: the projector modules import this package.
    from agent_context_platform.projection.projectors import (
        agent,
        code,
        git,
        knowledge,
        portfolio,
        quality,
    )

    keys = {
        *agent.lock_keys(event),
        *code.lock_keys(event),
        *git.lock_keys(event),
        *knowledge.lock_keys(event),
        *portfolio.lock_keys(event),
        *quality.lock_keys(event),
    }
    return sorted(keys)


async def lock_event_nodes(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    """Lock the union node set of `event`; every projector calls this before writing."""
    await lock_nodes(tx, event_lock_keys(event))


# Appended after `MERGE (...)-[r:TYPE]->(...)`; requires `$event_id`. Both endpoints
# are already locked by `lock_nodes`, which serializes writers of `r`.
_ASSERT_RELATIONSHIP: LiteralString = (
    "WITH r "
    "UNWIND coalesce(r.source_event_ids, []) + [$event_id] AS source_event_id "
    "WITH DISTINCT r, source_event_id ORDER BY source_event_id "
    "WITH r, collect(source_event_id) AS source_event_ids "
    "SET r.source_event_ids = source_event_ids"
)

# Lifecycle rank; a higher rank is never overwritten by a lower one.
_CURRENT_STATUS_RANK: LiteralString = (
    "CASE n.status "
    "WHEN 'started' THEN 0 WHEN 'stopped' THEN 1 WHEN 'output_observed' THEN 1 "
    "WHEN 'completed' THEN 2 WHEN 'ended' THEN 2 ELSE -1 END"
)
STATUS_RANKS: dict[str, int] = {
    "started": 0,
    "stopped": 1,
    "output_observed": 1,
    "completed": 2,
    "ended": 2,
}


def node_statement(label: LiteralString, key: LiteralString) -> LiteralString:
    """`MERGE` a node by its constrained property, exposed as `n` (locked by `lock_nodes`)."""
    return f"MERGE (n:{label} {{{key}: $node_id}}) WITH n "


def min_non_null(*props: LiteralString) -> LiteralString:
    """`SET` immutable facts; the smallest non-null value wins, so order is irrelevant."""
    return "SET " + ", ".join(
        f"n.{prop} = CASE WHEN ${prop} IS NULL THEN n.{prop} "
        f"WHEN n.{prop} IS NULL OR ${prop} < n.{prop} THEN ${prop} ELSE n.{prop} END"
        for prop in props
    )


def fill_once(*props: LiteralString) -> LiteralString:
    """`SET` immutable content whose identity already fixes its value (first write wins)."""
    return "SET " + ", ".join(f"n.{prop} = coalesce(n.{prop}, ${prop})" for prop in props)


def newest_wins(group: LiteralString, *props: LiteralString) -> LiteralString:
    """Move a current pointer only when `$order` is newer than the stored `<group>_order`."""
    assignments = ", ".join(
        f"n.{prop} = CASE WHEN newer_{group} THEN ${prop} ELSE n.{prop} END" for prop in props
    )
    return (
        f"WITH n, (n.{group}_order IS NULL OR $order > n.{group}_order) AS newer_{group} SET "
        f"{assignments}, n.{group}_order = CASE WHEN newer_{group} THEN $order ELSE n.{group}_order END"
    )


def advance_status() -> LiteralString:
    """Raise `n.status` to `$status` (rank `$rank`); never regress it."""
    return (
        "WITH n SET n.status = CASE WHEN $rank > " + _CURRENT_STATUS_RANK + " "
        "THEN $status ELSE n.status END"
    )


def relationship_statement(
    source_label: LiteralString,
    source_key: LiteralString,
    rel_type: LiteralString,
    target_label: LiteralString,
    target_key: LiteralString,
) -> LiteralString:
    """`MERGE` one relationship and add `$event_id` to its sorted provenance list.

    Both endpoints are merged (as identity-only stubs when not yet seen); the caller
    must have locked them with `lock_nodes`, so concurrent workers cannot create
    duplicate relationships. Requires `$source_id`, `$target_id` and `$event_id`.
    """
    return (
        f"MERGE (a:{source_label} {{{source_key}: $source_id}}) MERGE (b:{target_label} {{{target_key}: $target_id}}) "
        f"MERGE (a)-[r:{rel_type}]->(b) "
    ) + (_ASSERT_RELATIONSHIP)


def scope_parameters(event: StoredEventV1) -> dict[str, object]:
    """The envelope `context` scope (`project_id`, `repository_id`) a scoped node is written with.

    Written with `min_non_null`: the smallest non-null value wins, so a later event that disagrees
    cannot move it and any delivery order converges on the same value.
    """
    return {
        "project_id": event.context.project_id,
        "repository_id": event.context.repository_id,
    }


def event_order(event: StoredEventV1) -> str:
    """Total order key `(occurred_at, event_id)`, fixed-width so strings compare correctly."""
    occurred_at = event.occurred_at.astimezone(UTC)
    return f"{occurred_at:%Y-%m-%dT%H:%M:%S.%f}|{event.event_id}"


async def assert_link(
    tx: Neo4jTransaction,
    statement: LiteralString,
    event: StoredEventV1,
    source_id: str,
    target_id: str,
) -> None:
    """Run a `relationship_statement`, recording `event` as one of its sources."""
    await tx.run(
        statement,
        parameters={
            "event_id": str(event.event_id),
            "source_id": source_id,
            "target_id": target_id,
        },
    )
