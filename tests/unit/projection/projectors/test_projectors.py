"""Unit tests for deterministic IDs, ordering keys, query builders and event routing."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from agent_context_sdk import (
    EVENT_PAYLOAD_MODELS,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content import ContentDisposition

from agent_context_platform.projection.projectors import (
    STATUS_RANKS,
    advance_status,
    event_order,
    fill_once,
    min_non_null,
    newest_wins,
    relationship_statement,
)
from agent_context_platform.projection.projectors.agent import (
    AgentProjector,
    session_node_id,
    tool_call_node_id,
    turn_node_id,
)
from agent_context_platform.projection.projectors.git import (
    GitProjector,
    branch_node_id,
    commit_node_id,
)
from agent_context_platform.projection.projectors.portfolio import PortfolioProjector

PROJECTORS = (PortfolioProjector(), AgentProjector(), GitProjector())
MOMENT = datetime(2026, 8, 13, 13, 0, 0, 123456, tzinfo=UTC)


def _event(
    event_type: str,
    payload: dict[str, object],
    *,
    number: int = 1,
    occurred_at: datetime = MOMENT,
    context: dict[str, str] | None = None,
) -> StoredEventV1:
    draft = EventDraftV1(
        event_id=UUID(f"0198a4b1-98c0-7c28-ae3f-{number:012x}"),
        event_type=event_type,
        stream_id="stream",
        occurred_at=occurred_at,
        observed_at=occurred_at,
        producer=ProducerV1(producer_id="p", name="p", version="1"),
        context=EventContextV1(**(context or {})),
        payload=payload,
        redaction=EventRedactionSummaryV1(
            policy_version="v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"key-{number}",
    )
    return seal_event(draft, [], 1, None)


class RecordingTransaction:
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, *, parameters: dict[str, Any]) -> None:
        self.statements.append((query, parameters))


def test_node_ids_derive_from_native_identifiers_and_stay_unambiguous() -> None:
    assert session_node_id("thr_1") == "thr_1"
    assert turn_node_id("a", "b:c") != turn_node_id("a:b", "c")
    assert tool_call_node_id("a", "b:c") != tool_call_node_id("a:b", "c")
    assert turn_node_id("s", "t") != tool_call_node_id("s", "t")
    assert branch_node_id("a", "b:c") != branch_node_id("a:b", "c")
    assert branch_node_id("repo", "feature/x") == "branch:4:repo:feature/x"


def test_commit_id_is_scoped_by_repository() -> None:
    oid = "a" * 40
    assert commit_node_id("repo_1", oid) == f"commit:repo_1:{oid}"
    assert commit_node_id("repo_1", oid) != commit_node_id("repo_2", oid)


def test_event_order_is_a_total_order_on_time_then_event_id() -> None:
    early = _event("x", {}, number=2, occurred_at=MOMENT)
    late = _event("x", {}, number=1, occurred_at=MOMENT + timedelta(microseconds=1))
    tie_low = _event("x", {}, number=1)
    tie_high = _event("x", {}, number=2)
    assert event_order(early) < event_order(late)
    assert event_order(tie_low) < event_order(tie_high)
    assert event_order(tie_low) == event_order(_event("x", {}, number=1))


def test_status_ranks_never_regress_a_completed_activity() -> None:
    assert STATUS_RANKS["started"] < STATUS_RANKS["stopped"] < STATUS_RANKS["completed"]
    assert STATUS_RANKS["stopped"] == STATUS_RANKS["output_observed"]
    assert "WHEN 'completed' THEN 2" in advance_status()


def test_query_builders_compose_static_cypher() -> None:
    assert min_non_null("model") == (
        "SET n.model = CASE WHEN $model IS NULL THEN n.model "
        "WHEN n.model IS NULL OR $model < n.model THEN $model ELSE n.model END"
    )
    assert fill_once("a", "b") == "SET n.a = coalesce(n.a, $a), n.b = coalesce(n.b, $b)"
    pointer = newest_wins("head", "branch")
    assert "$order > n.head_order" in pointer
    assert "n.branch = CASE WHEN newer_head THEN $branch ELSE n.branch END" in pointer
    link = relationship_statement("Session", "session_id", "TARGETED", "Checkout", "checkout_id")
    assert "MERGE (a)-[r:TARGETED]->(b)" in link
    assert "ORDER BY source_event_id" in link


@pytest.mark.parametrize(
    "event_type",
    [
        "agent.source.unmapped",
        "agent.capture.discarded",
        "agent.subagent.started",
        "agent.subagent.stopped",
        "agent.permission.decision",
        "agent.compaction.observed",
        "code.file.indexed",
    ],
)
def test_metadata_only_and_unprojected_events_are_skipped(event_type: str) -> None:
    assert not any(projector.handles(event_type) for projector in PROJECTORS)


def test_every_projected_event_type_is_a_registered_sdk_contract() -> None:
    registered = {event_type for event_type, _ in EVENT_PAYLOAD_MODELS}
    handled = {
        event_type
        for event_type in registered
        if any(projector.handles(event_type) for projector in PROJECTORS)
    }
    assert len(handled) == 12
    assert handled <= registered
    assert PortfolioProjector().handles("git.repository.observed")
    assert not GitProjector().handles("git.repository.observed")
    assert not AgentProjector().handles("git.commit.observed")


def _project(event: StoredEventV1) -> list[tuple[str, dict[str, Any]]]:
    tx = RecordingTransaction()
    for projector in PROJECTORS:
        if projector.handles(event.event_type):
            asyncio.run(projector.project(tx, event))  # type: ignore[arg-type]
    return tx.statements


def test_turn_stopped_never_writes_an_outcome() -> None:
    event = _event(
        "agent.turn.stopped",
        {"source": "codex_hook", "session_id": "s", "turn_id": "t", "stop_hook_active": True},
    )
    for query, parameters in _project(event):
        assert "success" not in query and "error_class" not in query
        assert "success" not in parameters and "error_class" not in parameters


def test_output_observed_never_writes_an_outcome() -> None:
    event = _event(
        "agent.tool_call.output_observed",
        {
            "source": "codex_hook",
            "session_id": "s",
            "turn_id": "t",
            "tool_call_id": "c",
            "tool_name": "Bash",
            "output_content_id": "out",
        },
    )
    statements = _project(event)
    assert statements
    for query, parameters in statements:
        assert "success" not in query and "error_class" not in query
        assert "success" not in parameters and "error_class" not in parameters


def test_the_graph_only_receives_content_ids() -> None:
    event = _event(
        "agent.turn.completed",
        {
            "source": "codex_otel",
            "session_id": "s",
            "turn_id": "t",
            "success": True,
            "assistant_message_content_id": "msg_1",
        },
    )
    values = {v for _, parameters in _project(event) for v in parameters.values()}
    assert "msg_1" in values
