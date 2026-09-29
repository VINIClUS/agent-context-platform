"""Project agent activity (session, turn, tool call) into the graph.

Hook, OTel and JSONL observations describe the same native session, turn and
tool call, so node IDs derive only from those native identifiers (never from
the capture surface) and the observations coalesce onto one node. Every write
is order-independent; see the package docstring for the rules.

`agent.turn.stopped` and `agent.tool_call.output_observed` come from hooks,
which cannot see the outcome: they never touch `success` or `error_class`.
Only the `*.completed` events set them, and a later hook event cannot erase
them. Subagent, permission and compaction events are not projected yet: the
graph schema has no node for them.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Final, LiteralString

from agent_context_sdk import (
    SessionEndedV1,
    SessionStartedV1,
    StoredEventV1,
    ToolCallCompletedV1,
    ToolCallOutputObservedV1,
    ToolCallStartedV1,
    TurnCompletedV1,
    TurnStartedV1,
    TurnStoppedV1,
)

from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors import (
    STATUS_RANKS,
    advance_status,
    assert_link,
    event_order,
    lock_nodes,
    min_non_null,
    newest_wins,
    node_statement,
    relationship_statement,
)


def session_node_id(session_id: str) -> str:
    """Graph identity of a session: its native session ID."""
    return session_id


def turn_node_id(session_id: str, turn_id: str) -> str:
    """Graph identity of a turn; the length prefix keeps `(session, turn)` unambiguous."""
    return f"turn:{len(session_id)}:{session_id}:{turn_id}"


def tool_call_node_id(session_id: str, tool_call_id: str) -> str:
    """Graph identity of a tool call; the length prefix keeps `(session, call)` unambiguous."""
    return f"tool_call:{len(session_id)}:{session_id}:{tool_call_id}"


_SESSION: Final = node_statement("Session", "session_id")
_TURN: Final = node_statement("Turn", "turn_id")
_TOOL_CALL: Final = node_statement("ToolCall", "tool_call_id")

_SESSION_STARTED: Final[LiteralString] = (
    _SESSION
    + min_non_null("started_at", "start_reason", "model", "permission_mode", "sandbox_mode")
    + " "
    + advance_status()
)
_SESSION_ENDED: Final[LiteralString] = (
    _SESSION
    + newest_wins("ended", "ended_at", "end_reason", "duration_ms")
    + " "
    + advance_status()
)
_TURN_STARTED: Final[LiteralString] = (
    _TURN
    + min_non_null(
        "session_id",
        "started_at",
        "user_message_content_id",
        "model",
        "permission_mode",
        "sandbox_mode",
    )
    + " "
    + advance_status()
)
_TURN_STOPPED: Final[LiteralString] = (
    _TURN
    + min_non_null("session_id")
    + " "
    + newest_wins("stopped", "stopped_at", "stop_hook_active", "stopped_message_content_id")
    + " "
    + advance_status()
)
_TURN_COMPLETED: Final[LiteralString] = (
    _TURN
    + min_non_null("session_id")
    + " "
    + newest_wins(
        "completed",
        "completed_at",
        "success",
        "error_class",
        "duration_ms",
        "assistant_message_content_id",
    )
    + " "
    + advance_status()
)
_TOOL_CALL_STARTED: Final[LiteralString] = (
    _TOOL_CALL
    + min_non_null("session_id", "turn_id", "tool_name", "input_content_id", "started_at")
    + " "
    + advance_status()
)
_TOOL_CALL_OUTPUT_OBSERVED: Final[LiteralString] = (
    _TOOL_CALL
    + min_non_null(
        "session_id",
        "turn_id",
        "tool_name",
        "input_content_id",
        "output_content_id",
        "output_observed_at",
    )
    + " "
    + advance_status()
)
_TOOL_CALL_COMPLETED: Final[LiteralString] = (
    _TOOL_CALL
    + min_non_null("session_id", "turn_id", "tool_name", "input_content_id", "output_content_id")
    + " "
    + newest_wins("completed", "completed_at", "success", "error_class", "duration_ms")
    + " "
    + advance_status()
)

_TARGETED: Final = relationship_statement(
    "Session", "session_id", "TARGETED", "Checkout", "checkout_id"
)
_HAS_TURN: Final = relationship_statement("Session", "session_id", "HAS_TURN", "Turn", "turn_id")
_INVOKED: Final = relationship_statement("Turn", "turn_id", "INVOKED", "ToolCall", "tool_call_id")


def _base(event: StoredEventV1, status: str) -> dict[str, object]:
    return {
        "event_id": str(event.event_id),
        "order": event_order(event),
        "status": status,
        "rank": STATUS_RANKS[status],
    }


async def _session_started(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = SessionStartedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Session", session_node_id(payload.session_id)),
            *(
                [("Checkout", event.context.checkout_id)]
                if event.context.checkout_id is not None
                else []
            ),
        ],
    )
    await tx.run(
        _SESSION_STARTED,
        parameters={
            **_base(event, "started"),
            "node_id": session_node_id(payload.session_id),
            "started_at": event.occurred_at,
            "start_reason": payload.start_reason,
            "model": payload.model,
            "permission_mode": payload.permission_mode,
            "sandbox_mode": payload.sandbox_mode,
        },
    )
    await _target_checkout(tx, event, payload.session_id)


async def _session_ended(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = SessionEndedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Session", session_node_id(payload.session_id)),
            *(
                [("Checkout", event.context.checkout_id)]
                if event.context.checkout_id is not None
                else []
            ),
        ],
    )
    await tx.run(
        _SESSION_ENDED,
        parameters={
            **_base(event, "ended"),
            "node_id": session_node_id(payload.session_id),
            "ended_at": event.occurred_at,
            "end_reason": payload.end_reason,
            "duration_ms": payload.duration_ms,
        },
    )
    await _target_checkout(tx, event, payload.session_id)


async def _target_checkout(tx: Neo4jTransaction, event: StoredEventV1, session_id: str) -> None:
    if event.context.checkout_id is None:
        return
    await assert_link(tx, _TARGETED, event, session_node_id(session_id), event.context.checkout_id)


async def _has_turn(
    tx: Neo4jTransaction, event: StoredEventV1, session_id: str, turn_id: str
) -> None:
    await assert_link(
        tx, _HAS_TURN, event, session_node_id(session_id), turn_node_id(session_id, turn_id)
    )


async def _turn_started(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = TurnStartedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Session", session_node_id(payload.session_id)),
            ("Turn", turn_node_id(payload.session_id, payload.turn_id)),
        ],
    )
    await tx.run(
        _TURN_STARTED,
        parameters={
            **_base(event, "started"),
            "node_id": turn_node_id(payload.session_id, payload.turn_id),
            "session_id": payload.session_id,
            "started_at": event.occurred_at,
            "user_message_content_id": payload.user_message_content_id,
            "model": payload.model,
            "permission_mode": payload.permission_mode,
            "sandbox_mode": payload.sandbox_mode,
        },
    )
    await _has_turn(tx, event, payload.session_id, payload.turn_id)


async def _turn_stopped(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = TurnStoppedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Session", session_node_id(payload.session_id)),
            ("Turn", turn_node_id(payload.session_id, payload.turn_id)),
        ],
    )
    await tx.run(
        _TURN_STOPPED,
        parameters={
            **_base(event, "stopped"),
            "node_id": turn_node_id(payload.session_id, payload.turn_id),
            "session_id": payload.session_id,
            "stopped_at": event.occurred_at,
            "stop_hook_active": payload.stop_hook_active,
            "stopped_message_content_id": payload.assistant_message_content_id,
        },
    )
    await _has_turn(tx, event, payload.session_id, payload.turn_id)


async def _turn_completed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = TurnCompletedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Session", session_node_id(payload.session_id)),
            ("Turn", turn_node_id(payload.session_id, payload.turn_id)),
        ],
    )
    await tx.run(
        _TURN_COMPLETED,
        parameters={
            **_base(event, "completed"),
            "node_id": turn_node_id(payload.session_id, payload.turn_id),
            "session_id": payload.session_id,
            "completed_at": event.occurred_at,
            "success": payload.success,
            "error_class": payload.error_class,
            "duration_ms": payload.duration_ms,
            "assistant_message_content_id": payload.assistant_message_content_id,
        },
    )
    await _has_turn(tx, event, payload.session_id, payload.turn_id)


async def _invoked(
    tx: Neo4jTransaction, event: StoredEventV1, session_id: str, turn_id: str, tool_call_id: str
) -> None:
    await assert_link(
        tx,
        _INVOKED,
        event,
        turn_node_id(session_id, turn_id),
        tool_call_node_id(session_id, tool_call_id),
    )


async def _tool_call_started(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = ToolCallStartedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Turn", turn_node_id(payload.session_id, payload.turn_id)),
            ("ToolCall", tool_call_node_id(payload.session_id, payload.tool_call_id)),
        ],
    )
    await tx.run(
        _TOOL_CALL_STARTED,
        parameters={
            **_base(event, "started"),
            "node_id": tool_call_node_id(payload.session_id, payload.tool_call_id),
            "session_id": payload.session_id,
            "turn_id": turn_node_id(payload.session_id, payload.turn_id),
            "tool_name": payload.tool_name,
            "input_content_id": payload.input_content_id,
            "started_at": event.occurred_at,
        },
    )
    await _invoked(tx, event, payload.session_id, payload.turn_id, payload.tool_call_id)


async def _tool_call_output_observed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = ToolCallOutputObservedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Turn", turn_node_id(payload.session_id, payload.turn_id)),
            ("ToolCall", tool_call_node_id(payload.session_id, payload.tool_call_id)),
        ],
    )
    await tx.run(
        _TOOL_CALL_OUTPUT_OBSERVED,
        parameters={
            **_base(event, "output_observed"),
            "node_id": tool_call_node_id(payload.session_id, payload.tool_call_id),
            "session_id": payload.session_id,
            "turn_id": turn_node_id(payload.session_id, payload.turn_id),
            "tool_name": payload.tool_name,
            "input_content_id": payload.input_content_id,
            "output_content_id": payload.output_content_id,
            "output_observed_at": event.occurred_at,
        },
    )
    await _invoked(tx, event, payload.session_id, payload.turn_id, payload.tool_call_id)


async def _tool_call_completed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = ToolCallCompletedV1.model_validate(dict(event.payload))
    await lock_nodes(
        tx,
        [
            ("Turn", turn_node_id(payload.session_id, payload.turn_id)),
            ("ToolCall", tool_call_node_id(payload.session_id, payload.tool_call_id)),
        ],
    )
    await tx.run(
        _TOOL_CALL_COMPLETED,
        parameters={
            **_base(event, "completed"),
            "node_id": tool_call_node_id(payload.session_id, payload.tool_call_id),
            "session_id": payload.session_id,
            "turn_id": turn_node_id(payload.session_id, payload.turn_id),
            "tool_name": payload.tool_name,
            "input_content_id": payload.input_content_id,
            "output_content_id": payload.output_content_id,
            "completed_at": event.occurred_at,
            "success": payload.success,
            "error_class": payload.error_class,
            "duration_ms": payload.duration_ms,
        },
    )
    await _invoked(tx, event, payload.session_id, payload.turn_id, payload.tool_call_id)


_HANDLERS: Final[dict[str, Callable[[Neo4jTransaction, StoredEventV1], Awaitable[None]]]] = {
    "agent.session.started": _session_started,
    "agent.session.ended": _session_ended,
    "agent.turn.started": _turn_started,
    "agent.turn.stopped": _turn_stopped,
    "agent.turn.completed": _turn_completed,
    "agent.tool_call.started": _tool_call_started,
    "agent.tool_call.output_observed": _tool_call_output_observed,
    "agent.tool_call.completed": _tool_call_completed,
}

AGENT_EVENT_TYPES: Final = frozenset(_HANDLERS)


class AgentProjector:
    """Projects session, turn and tool-call events by native identifiers."""

    name = "agent"
    version = "1"

    def handles(self, event_type: str) -> bool:
        return event_type in _HANDLERS

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        await _HANDLERS[event.event_type](tx, event)
