"""Hook- and OTel-origin session observations coalesce onto one graph node."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from agent_context_sdk import (
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content import ContentDisposition

from agent_context_platform.ledger.codex_otel import (
    CodexOtelMapperConfig,
    CodexOtelRecord,
    OtelSignal,
    map_record,
)
from agent_context_platform.projection.projectors import event_lock_keys
from agent_context_platform.projection.projectors.agent import AgentProjector, session_node_id

FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "codex_otel"
    / "conversation_starts.trace.json"
)
SESSION_ID = "0198a4b1-98c0-7c28-ae3f-000000000001"


class RecordingTransaction:
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, *, parameters: dict[str, Any]) -> None:
        self.statements.append((query, parameters))


def _otel_event() -> StoredEventV1:
    raw = json.loads(FIXTURE.read_text())
    config = CodexOtelMapperConfig(
        hmac_key=b"k" * 32,
        producer=ProducerV1(producer_id="codex-otel", name="codex-otel-mapper", version="1"),
        stream_id="otel-stream",
    )
    draft = map_record(
        config,
        CodexOtelRecord(OtelSignal.TRACE, raw["resource"], raw["attributes"]),
        observed_at=datetime(2026, 8, 13, 13, 0, 1, tzinfo=UTC),
    )
    assert draft is not None
    draft = draft.model_copy(update={"event_id": UUID("0198a4b1-98c0-7c28-ae3f-0000000000a2")})
    return seal_event(draft, [], 1, None)


def _hook_event() -> StoredEventV1:
    moment = datetime(2026, 8, 13, 12, 59, 59, tzinfo=UTC)
    draft = EventDraftV1(
        event_id=UUID("0198a4b1-98c0-7c28-ae3f-0000000000a1"),
        event_type="agent.session.started",
        stream_id="hook-stream",
        occurred_at=moment,
        observed_at=moment,
        producer=ProducerV1(producer_id="hook", name="codex-hook", version="1"),
        context=EventContextV1(session_id=SESSION_ID),
        payload={"source": "codex_hook", "session_id": SESSION_ID},
        redaction=EventRedactionSummaryV1(
            policy_version="v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key="hook-1",
    )
    return seal_event(draft, [], 1, None)


def _project(event: StoredEventV1) -> list[tuple[str, dict[str, Any]]]:
    tx = RecordingTransaction()
    asyncio.run(AgentProjector().project(tx, event))  # type: ignore[arg-type]
    return tx.statements


def test_otel_and_hook_session_started_write_the_same_node() -> None:
    hook, otel = _hook_event(), _otel_event()
    assert (
        event_lock_keys(hook) == event_lock_keys(otel) == [("Session", session_node_id(SESSION_ID))]
    )

    hook_statements, otel_statements = _project(hook), _project(otel)
    assert [q for q, _ in hook_statements] == [q for q, _ in otel_statements]
    hook_write, otel_write = hook_statements[-1][1], otel_statements[-1][1]
    assert hook_write["node_id"] == otel_write["node_id"] == SESSION_ID
    assert hook_write["event_id"] != otel_write["event_id"]
    # The OTel observation adds detail without displacing the hook's: fields are min-non-null.
    assert otel_write["model"] == "gpt-5.5"
    assert otel_write["permission_mode"] == "on-request"
    assert otel_write["sandbox_mode"] == "workspace-write"
    assert hook_write["model"] is None
