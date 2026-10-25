"""The span attribute allowlist: the last step before a finished span reaches an exporter.

Instrumentation only sets allowlisted keys, but this layer does not trust that. Every finished span
is copied with: allowlisted attributes whose values pass their check (closed sets, UUIDs,
non-negative integers, route templates, class names), exception events reduced to
``exception.type``, no other events, no link attributes and no status description. Anything else
is dropped, never truncated or escaped, so content, prompts, tool arguments, tokens, DSNs, paths and
exception messages cannot leave the process through a span. A span that cannot be sanitized is
dropped.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from typing import Final

from opentelemetry.context import Context
from opentelemetry.sdk.trace import Event, ReadableSpan, Span, SpanProcessor
from opentelemetry.trace import Link, SpanContext, Status, TraceState
from opentelemetry.util.types import Attributes

from agent_context_platform.observability import contract

_UUID: Final = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.ASCII)
_CLASS_NAME: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_.]{0,127}", re.ASCII)
_SPAN_NAME: Final = re.compile(r"[A-Za-z0-9_./{} -]{1,160}", re.ASCII)
_FALLBACK_SPAN_NAME: Final = "agent_context.span"
_EXCEPTION_EVENT: Final = "exception"
_EXCEPTION_TYPE: Final = "exception.type"

_OUTCOMES: Final = (
    contract.INGESTION_OUTCOMES
    | contract.RETRIEVAL_OUTCOMES
    | contract.MCP_OUTCOMES
    | {contract.OTHER}
)
_RESULTS: Final = (
    contract.PROJECTION_RESULTS
    | contract.INDEX_RESULTS
    | contract.EVENT_RESULTS
    | contract.CONTENT_RESULTS
    | {"lost_lease", contract.OTHER}
)


def _in(allowed: frozenset[str]) -> Callable[[object], bool]:
    return lambda value: isinstance(value, str) and value in allowed


def _count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _uuid(value: object) -> bool:
    return isinstance(value, str) and _UUID.fullmatch(value) is not None


def _class_name(value: object) -> bool:
    return isinstance(value, str) and _CLASS_NAME.fullmatch(value) is not None


def _route(value: object) -> bool:
    return isinstance(value, str) and contract.route_label(value) == value


def _status_code(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599


def _projector(value: object) -> bool:
    return isinstance(value, str) and contract.projector_label(value) == value


ALLOWED_ATTRIBUTES: Final[Mapping[str, Callable[[object], bool]]] = {
    "agent_context.batch_id": _uuid,
    "agent_context.event_id": _uuid,
    "agent_context.outbox_id": _count,
    "agent_context.event_count": _count,
    "agent_context.item_count": _count,
    "agent_context.events.created": _count,
    "agent_context.events.submitted": _count,
    "agent_context.claimed": _count,
    "agent_context.delivered": _count,
    "agent_context.retried": _count,
    "agent_context.dead_lettered": _count,
    "agent_context.outcome": _in(_OUTCOMES),
    "agent_context.reason": _in(contract.INGESTION_REASONS | {contract.OTHER}),
    "agent_context.result": _in(_RESULTS),
    "agent_context.operation": _in(contract.RETRIEVAL_OPERATIONS | {contract.OTHER}),
    "agent_context.projector": _projector,
    "mcp.method.name": _in(contract.MCP_METHODS | {contract.OTHER}),
    "gen_ai.tool.name": _in(contract.MCP_TOOLS),
    "http.request.method": _in(contract.HTTP_METHODS | {contract.HTTP_OTHER_METHOD}),
    "http.route": _route,
    "http.response.status_code": _status_code,
    "error.type": _class_name,
}


def sanitize_attributes(attributes: Attributes) -> dict[str, contract.ScalarAttribute]:
    """Keep only allowlisted keys whose (scalar) value passes the key's check."""
    if not attributes:
        return {}
    kept: dict[str, contract.ScalarAttribute] = {}
    for key, value in attributes.items():
        check = ALLOWED_ATTRIBUTES.get(key)
        if check is not None and isinstance(value, str | bool | int | float) and check(value):
            kept[key] = value
    return kept


def _sanitize_events(events: Sequence[Event]) -> tuple[Event, ...]:
    kept: list[Event] = []
    for event in events:
        if event.name != _EXCEPTION_EVENT:
            continue
        exception_type: object = (
            None if event.attributes is None else event.attributes.get(_EXCEPTION_TYPE)
        )
        attributes: dict[str, contract.ScalarAttribute] = {}
        if isinstance(exception_type, str) and _class_name(exception_type):
            attributes[_EXCEPTION_TYPE] = exception_type
        kept.append(Event(_EXCEPTION_EVENT, attributes, timestamp=event.timestamp))
    return tuple(kept)


def _without_trace_state(context: SpanContext | None) -> SpanContext | None:
    """``context`` without its W3C ``tracestate``: caller-chosen text, never exported."""
    if context is None:
        return None
    return SpanContext(
        context.trace_id, context.span_id, context.is_remote, context.trace_flags, TraceState()
    )


def sanitize_span(span: ReadableSpan) -> ReadableSpan:
    """A copy of ``span`` holding only what the allowlist permits."""
    name = span.name if _SPAN_NAME.fullmatch(span.name) else _FALLBACK_SPAN_NAME
    return ReadableSpan(
        name=name,
        context=_without_trace_state(span.context),
        parent=_without_trace_state(span.parent),
        resource=span.resource,
        attributes=sanitize_attributes(span.attributes),
        events=_sanitize_events(span.events),
        links=tuple(
            Link(context)
            for context in map(_without_trace_state, (link.context for link in span.links))
            if context is not None
        ),
        kind=span.kind,
        status=Status(span.status.status_code),
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


class SanitizingSpanProcessor(SpanProcessor):
    """Hands the delegate (a batch processor) a sanitized copy of every finished span."""

    def __init__(self, delegate: SpanProcessor) -> None:
        self._delegate = delegate

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        self._delegate.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        try:
            sanitized = sanitize_span(span)
        except Exception:
            # Fail closed for privacy (drop the span) and open for the request (never raise).
            return
        self._delegate.on_end(sanitized)

    def shutdown(self) -> None:
        self._delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return self._delegate.force_flush(timeout_millis)
