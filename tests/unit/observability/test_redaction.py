"""The span allowlist: unknown keys, unchecked values, messages and stacks never leave a span."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.trace import Status, StatusCode

from agent_context_platform.observability import core, traced
from agent_context_platform.observability.redaction import (
    SanitizingSpanProcessor,
    sanitize_attributes,
)

from ._capture import Canaries, Capture

pytestmark = pytest.mark.unit


def test_only_allowlisted_keys_with_valid_values_survive(
    telemetry: Capture, canaries: Canaries
) -> None:
    with telemetry.handle.tracer.start_as_current_span("agent_context.test") as span:
        span.set_attributes(
            {
                "agent_context.event_id": "0b7c4d36-0e4f-4c11-9a8d-1f3a5e6c7d8e",
                "agent_context.batch_id": canaries.content,  # not a UUID
                "agent_context.event_count": 3,
                "agent_context.claimed": -1,  # not a count
                "agent_context.delivered": True,  # a bool is not a count
                "agent_context.outcome": "accepted",
                "agent_context.reason": canaries.prompt,
                "http.route": canaries.path,
                "http.response.status_code": 999,
                "error.type": f"{canaries.tool_argument} with spaces",
                "db.statement": f"SELECT '{canaries.dsn_password}'",
                "http.request.header.authorization": f"Bearer {canaries.token_secret}",
                "gen_ai.prompt": canaries.prompt,
                "gen_ai.tool.name": "symbol_context",
                "mcp.method.name": "tools/call",
            }
        )
    exported = telemetry.span("agent_context.test")

    assert dict(exported.attributes or {}) == {
        "agent_context.event_id": "0b7c4d36-0e4f-4c11-9a8d-1f3a5e6c7d8e",
        "agent_context.event_count": 3,
        "agent_context.outcome": "accepted",
        "gen_ai.tool.name": "symbol_context",
        "mcp.method.name": "tools/call",
    }
    canaries.assert_absent(telemetry.dump())


def test_routes_must_look_like_templates() -> None:
    kept = sanitize_attributes(
        {"http.route": "/v1/ingestion/batches", "http.response.status_code": 200}
    )
    assert kept == {"http.route": "/v1/ingestion/batches", "http.response.status_code": 200}
    assert sanitize_attributes({"http.route": "relative?query=1"}) == {}
    assert sanitize_attributes({"http.route": "/home/user/repo"}) == {}
    assert sanitize_attributes({"http.route": "/v1/../etc/passwd"}) == {}
    assert sanitize_attributes({"http.route": "/mcp"}) == {"http.route": "/mcp"}
    assert sanitize_attributes({"http.route": "other"}) == {"http.route": "other"}
    assert sanitize_attributes(None) == {}
    assert sanitize_attributes({"agent_context.event_count": [1, 2]}) == {}


def test_exceptions_keep_only_their_class(telemetry: Capture, canaries: Canaries) -> None:
    with telemetry.handle.tracer.start_as_current_span("agent_context.sdk_recorded") as span:
        # The SDK's own recording carries the message and the stack: the allowlist strips both.
        try:
            raise ValueError(f"leaked {canaries.content} {canaries.dsn_password}")
        except ValueError as error:
            span.record_exception(error, attributes={"extra": canaries.prompt})
            span.set_status(Status(StatusCode.ERROR, f"description {canaries.token_secret}"))
        span.add_event("log", {"message": canaries.prompt})
    with pytest.raises(KeyError), traced("agent_context.traced"):
        raise KeyError(canaries.tool_argument)

    recorded = telemetry.span("agent_context.sdk_recorded")
    assert [event.name for event in recorded.events] == ["exception"]
    assert dict(recorded.events[0].attributes or {}) == {"exception.type": "ValueError"}
    assert recorded.status.status_code is StatusCode.ERROR
    assert recorded.status.description is None
    failed = telemetry.span("agent_context.traced")
    assert dict(failed.attributes or {}) == {"error.type": "KeyError"}
    assert dict(failed.events[0].attributes or {}) == {"exception.type": "KeyError"}
    assert failed.status.status_code is StatusCode.ERROR
    canaries.assert_absent(telemetry.dump())


def test_a_span_name_that_is_not_a_plain_label_is_replaced(
    telemetry: Capture, canaries: Canaries
) -> None:
    with telemetry.handle.tracer.start_as_current_span(f"GET /x?token={canaries.token_secret}"):
        pass
    assert [span.name for span in telemetry.spans()] == ["agent_context.span"]


def test_link_attributes_are_dropped(telemetry: Capture, canaries: Canaries) -> None:
    from opentelemetry.trace import Link

    with telemetry.handle.tracer.start_as_current_span("agent_context.parent") as parent:
        context = parent.get_span_context()
    with telemetry.handle.tracer.start_as_current_span(
        "agent_context.linked", links=[Link(context, {"secret": canaries.content})]
    ):
        pass
    (link,) = telemetry.span("agent_context.linked").links
    assert link.context == context
    assert not link.attributes
    canaries.assert_absent(telemetry.dump())


def test_a_span_that_cannot_be_sanitized_is_dropped_not_raised() -> None:
    delegate = Mock()
    processor = SanitizingSpanProcessor(delegate)
    broken = Mock(spec=ReadableSpan)
    broken.name = None  # fails the name check with a TypeError

    processor.on_end(broken)

    delegate.on_end.assert_not_called()
    processor.on_start(Mock())
    processor.force_flush(5)
    processor.shutdown()
    delegate.on_start.assert_called_once()
    delegate.force_flush.assert_called_once_with(5)
    delegate.shutdown.assert_called_once()


def test_helpers_never_raise_when_the_sdk_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    broken = core.Telemetry.disabled()
    broken.content_objects = Mock(add=Mock(side_effect=RuntimeError("sdk down")))
    previous = core.install(broken)
    try:
        core.record_content_object("inline", "stored")  # swallowed
        span = Mock(set_status=Mock(side_effect=RuntimeError("sdk down")))
        core.mark_error(span, ValueError("x"))  # swallowed
    finally:
        core.install(previous)
