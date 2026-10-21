"""MCP request telemetry: one span plus the `mcp.requests` / `mcp.duration` samples per request.

`McpTelemetry` wraps the protocol guard, so authentication, rate-limit and transport rejections are
counted too. The method label is the guard's validated body method, else the ``Mcp-Method``
header (the SDK rejects a request whose header and body disagree), bounded to the served methods;
the tool label is the ``Mcp-Name`` header of a ``tools/call`` (also enforced equal to the body),
bounded to the five tools. Nothing else is read from the request: never the body, arguments,
results or credentials. The outcome comes from the HTTP status only.

The guard re-attaches the caller's trace context around the SDK app, so `McpSpanScope` (wrapped
directly around the SDK app) makes the request span current again: spans recorded by tools nest
under it.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any, Final

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from agent_context_platform.mcp.protocol_guard import (
    METHOD_SCOPE_KEY,
    AccessGate,
    AdmittedRequest,
)
from agent_context_platform.observability import contract
from agent_context_platform.observability.core import (
    current,
    mark_error,
    mcp_outcome,
    record_mcp_request,
)

SPAN_SCOPE_KEY: Final = "agent_context.mcp_span"
GATE_SCOPE_KEY: Final = "agent_context.mcp_access_gate"
_METHOD_HEADER: Final = b"mcp-method"
_NAME_HEADER: Final = b"mcp-name"
_PENDING_SPAN_NAME: Final = "mcp.request"
_TRACE_PROPAGATOR: Final = TraceContextTextMapPropagator()


def _header(scope: Scope, name: bytes) -> str | None:
    for header_name, value in scope["headers"]:
        if header_name.lower() == name:
            return str(value.decode("latin-1"))
    return None


def _labels(scope: Scope) -> tuple[str, str]:
    method = scope.get(METHOD_SCOPE_KEY) or _header(scope, _METHOD_HEADER)
    method_label = contract.closed(method, contract.MCP_METHODS)
    tool_label = contract.mcp_tool_label(method_label, _header(scope, _NAME_HEADER))
    return method_label, tool_label


class McpTelemetry:
    """Pure ASGI middleware around the guarded MCP app; it never reads or buffers the body."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        status: int | None = None

        async def send_with_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        started = time.perf_counter()
        failed = False
        with current().tracer.start_as_current_span(
            _PENDING_SPAN_NAME,
            context=_caller_context(scope),
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            scope[SPAN_SCOPE_KEY] = span
            try:
                await self._app(scope, receive, send_with_status)
            except Exception as error:
                status, failed = 500, True
                mark_error(span, error)
                raise
            finally:
                # A request the client abandoned before any answer counts as a client error.
                outcome = (
                    "client_error"
                    if status is None
                    else mcp_outcome(status, reached_access_gate=bool(scope.get(GATE_SCOPE_KEY)))
                )
                method, tool = _labels(scope)
                _finish_span(span, method, tool, outcome, already_failed=failed)
                record_mcp_request(method, tool, outcome, time.perf_counter() - started)


def _caller_context(scope: Scope) -> Context | None:
    """The caller's W3C trace context when no span is current (a mount without the app's
    request-context middleware); ``None`` keeps the current one. Baggage is never imported."""
    if trace.get_current_span().get_span_context().is_valid:
        return None
    headers = {
        name.decode("latin-1").lower(): value.decode("latin-1")
        for name, value in scope.get("headers", ())
    }
    return _TRACE_PROPAGATOR.extract(headers)


def _finish_span(
    span: trace.Span, method: str, tool: str, outcome: str, *, already_failed: bool
) -> None:
    try:
        span.update_name(method)
        attributes: dict[str, str] = {"mcp.method.name": method, "agent_context.outcome": outcome}
        if tool in contract.MCP_TOOLS:
            attributes["gen_ai.tool.name"] = tool
        span.set_attributes(attributes)
        if outcome == "server_error" and not already_failed:
            mark_error(span, outcome)
    except Exception:
        return


class ObservedAccessGate:
    """Marks the requests that reached the access gate, so 401/403 are classified by cause."""

    def __init__(self, gate: AccessGate) -> None:
        self._gate = gate

    async def admit(self, scope: Scope, send: Send) -> AdmittedRequest | None:
        scope[GATE_SCOPE_KEY] = True
        return await self._gate.admit(scope, send)


class MetaTraceContext:
    """Context-tier middleware replacing the SDK's OpenTelemetry middleware.

    The 2026-07-28 profile lets a request carry W3C trace context in ``params._meta``; that
    ``traceparent`` parents the spans recorded while serving the request. Only ``traceparent`` is
    imported: ``tracestate`` and baggage are dropped, and the platform's sampler ignores the
    caller's sampled flag. Without a valid one the platform request span stays the parent.
    """

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        parent = _meta_parent(ctx.meta)
        if parent is None:
            return await call_next(ctx)
        token = otel_context.attach(parent)
        try:
            return await call_next(ctx)
        finally:
            otel_context.detach(token)


def _meta_parent(meta: Mapping[str, Any] | None) -> Context | None:
    value = None if not isinstance(meta, Mapping) else meta.get("traceparent")
    if not isinstance(value, str):
        return None
    parent = _TRACE_PROPAGATOR.extract({"traceparent": value})
    return parent if trace.get_current_span(parent).get_span_context().is_valid else None


class McpSpanScope:
    """Make the request span current around the SDK app (the guard re-attached the caller's)."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        span = scope.get(SPAN_SCOPE_KEY)
        if not isinstance(span, trace.Span):
            await self._app(scope, receive, send)
            return
        with trace.use_span(
            span, end_on_exit=False, record_exception=False, set_status_on_exception=False
        ):
            await self._app(scope, receive, send)
