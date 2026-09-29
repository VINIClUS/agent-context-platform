"""Fixtures for the domain projector suite: SDK-sealed events and a graph-state digest.

The P024 `graph_digest` only sees nodes labelled `ProjectionRuntimeTestNode`, so it
cannot see relationships or the real projection labels. `graph_state` keeps its
approach (hash every property, sorted, JSON-canonical) but reads the whole projected
graph: nodes and relationships with all properties. The projectors write nothing
volatile (no wall clock, no run ID), so nothing is excluded.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, LiteralString
from uuid import UUID

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
from agent_context_platform.projection.projectors.agent import AgentProjector
from agent_context_platform.projection.projectors.git import GitProjector
from agent_context_platform.projection.projectors.portfolio import PortfolioProjector
from agent_context_platform.projection.runtime import Projector
from agent_context_platform.projection.schema import SCHEMA_STATEMENTS, ensure_schema

from ..conftest import neo4j_integration_settings

PROJECTORS: tuple[Projector, ...] = (PortfolioProjector(), AgentProjector(), GitProjector())

BASE_TIME = datetime(2026, 8, 13, 13, 0, 0, tzinfo=UTC)
NODE_KEYS = {
    "Workspace": "workspace_id",
    "Project": "project_id",
    "Repository": "repository_id",
    "Branch": "branch_id",
    "Commit": "commit_id",
    "Checkout": "checkout_id",
    "WorkspaceSnapshot": "snapshot_id",
    "Session": "session_id",
    "Turn": "turn_id",
    "ToolCall": "tool_call_id",
}

_WIPE: LiteralString = (
    "MATCH (n) WHERE any(label IN labels(n) WHERE label IN $labels) DETACH DELETE n"
)
_NODES: LiteralString = "MATCH (n) RETURN labels(n) AS labels, properties(n) AS props"
_RELATIONSHIPS: LiteralString = (
    "MATCH (a)-[r]->(b) RETURN labels(a) AS a_labels, properties(a) AS a_props, "
    "type(r) AS type, properties(r) AS props, labels(b) AS b_labels, properties(b) AS b_props"
)

OID_A = "a" * 40
OID_B = "b" * 40
OID_TREE = "c" * 40
SHA = "d" * 64


def event_uuid(number: int) -> UUID:
    """A fixed UUIDv7 whose lexicographic order follows `number`."""
    return UUID(f"0198a4b1-98c0-7c28-ae3f-{number:012x}")


def build_event(
    number: int,
    event_type: str,
    payload: dict[str, object],
    *,
    seconds: int | None = None,
    context: dict[str, str] | None = None,
) -> StoredEventV1:
    """A sealed event; `number` fixes the event ID and (by default) `occurred_at`."""
    moment = BASE_TIME + timedelta(seconds=number if seconds is None else seconds)
    draft = EventDraftV1(
        event_id=event_uuid(number),
        event_type=event_type,
        stream_id=f"stream-{number}",
        occurred_at=moment,
        observed_at=moment,
        producer=ProducerV1(producer_id="projector-test", name="projector-test", version="1.0.0"),
        context=EventContextV1(**(context or {})),
        payload=payload,
        redaction=EventRedactionSummaryV1(
            policy_version="test-policy-v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"key-{number}",
    )
    return seal_event(draft, [], 1, None)


TRANSACTION_ATTEMPTS = [0]


async def project_event(store: Neo4jStore, event: StoredEventV1) -> None:
    """Apply one event in its own write transaction, as the runtime does.

    `TRANSACTION_ATTEMPTS` counts callback runs; the driver re-runs a callback
    only after a transient failure such as a deadlock, so attempts beyond one
    per event are retries.
    """

    async def run(tx: Neo4jTransaction) -> None:
        TRANSACTION_ATTEMPTS[0] += 1
        for projector in PROJECTORS:
            if projector.handles(event.event_type):
                await projector.project(tx, event)

    await store.execute_write(run)


async def project_all(store: Neo4jStore, events: Iterable[StoredEventV1]) -> None:
    for event in events:
        await project_event(store, event)


async def wipe_projected_graph(store: Neo4jStore) -> None:
    async def run(tx: Neo4jTransaction) -> None:
        await tx.run(_WIPE, parameters={"labels": list(NODE_KEYS)})

    await store.execute_write(run)


def _identity(labels: Sequence[str], props: dict[str, Any]) -> str:
    label = labels[0]
    return f"{label}:{props[NODE_KEYS[label]]}"


async def graph_state(store: Neo4jStore) -> dict[str, list[dict[str, Any]]]:
    """Every projected node and relationship with all properties, in canonical order."""

    async def read(tx: Neo4jTransaction) -> dict[str, list[dict[str, Any]]]:
        nodes = (await tx.run(_NODES, parameters={})).records
        relationships = (await tx.run(_RELATIONSHIPS, parameters={})).records
        return {
            "nodes": sorted(
                (
                    {"id": _identity(r["labels"], r["props"]), "props": _plain(r["props"])}
                    for r in nodes
                    if r["labels"][0] in NODE_KEYS
                ),
                key=lambda node: node["id"],
            ),
            "relationships": sorted(
                (
                    {
                        "from": _identity(r["a_labels"], r["a_props"]),
                        "type": r["type"],
                        "to": _identity(r["b_labels"], r["b_props"]),
                        "props": _plain(r["props"]),
                    }
                    for r in relationships
                    if r["a_labels"][0] in NODE_KEYS
                ),
                key=lambda rel: (rel["from"], rel["type"], rel["to"]),
            ),
        }

    return await store.execute_read(read)


def _plain(props: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(props, sort_keys=True, default=str))


def digest(state: dict[str, list[dict[str, Any]]]) -> str:
    canonical = json.dumps(state, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def _set_schema(*, present: bool) -> None:
    async with Neo4jStore(neo4j_integration_settings()) as store:
        if present:
            await ensure_schema(store)
            return

        async def drop(tx: Neo4jTransaction) -> None:
            for statement in SCHEMA_STATEMENTS:
                kind = "CONSTRAINT" if "CONSTRAINT" in statement.query else "INDEX"
                await tx.run(f"DROP {kind} {statement.name} IF EXISTS", parameters={})  # type: ignore[arg-type]

        await store.execute_write(drop)


@pytest.fixture(scope="module", autouse=True)
def projection_graph_schema() -> Iterator[None]:
    """Create the projection schema once, and drop it again.

    `test_neo4j_schema` requires a database with no projection schema, so this
    suite must leave the shared test database exactly as it found it.
    """
    asyncio.run(_set_schema(present=True))
    try:
        yield
    finally:
        asyncio.run(_set_schema(present=False))
