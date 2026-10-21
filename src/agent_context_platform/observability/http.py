"""HTTP server span and `http.server.request.duration` with route templates only.

The span is named ``{METHOD} {route}`` and the route is the matched template (``/v1/...``), or
``other`` when nothing matched: the raw path, query string, headers and body are never read.
"""

from __future__ import annotations

import time
from typing import Final

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from starlette.routing import Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from agent_context_platform.observability import contract
from agent_context_platform.observability.core import (
    current,
    http_method_label,
    mark_error,
    record_http_request,
)

_PENDING_SPAN_NAME: Final = "HTTP"


def route_template(scope: Scope) -> str:
    """The template of the route that served ``scope``, or ``other``."""
    route = scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str):
        return path
    app = scope.get("app")
    for candidate in getattr(getattr(app, "router", None), "routes", ()):
        template = getattr(candidate, "path", None)
        if isinstance(template, str) and candidate.matches(scope)[0] is Match.FULL:
            return template
    return contract.OTHER


class HttpTelemetryMiddleware:
    """Pure ASGI middleware: one server span and one duration sample per HTTP request."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        method = http_method_label(str(scope.get("method", "")))
        status = 500

        async def send_with_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        started = time.perf_counter()
        with current().tracer.start_as_current_span(
            _PENDING_SPAN_NAME,
            kind=SpanKind.SERVER,
            attributes={"http.request.method": method},
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                await self._app(scope, receive, send_with_status)
            except Exception as error:
                status = 500
                mark_error(span, error)
                raise
            finally:
                route = contract.route_label(_safe_route(scope))
                _finish_span(span, method, route, status)
                record_http_request(method, route, status, time.perf_counter() - started)


def _safe_route(scope: Scope) -> str:
    try:
        return route_template(scope)
    except Exception:
        return contract.OTHER


def _finish_span(span: trace.Span, method: str, route: str, status: int) -> None:
    try:
        span.update_name(f"{method} {route}")
        span.set_attributes({"http.route": route, "http.response.status_code": status})
        if status >= 500:
            span.set_status(Status(StatusCode.ERROR))
    except Exception:
        return
