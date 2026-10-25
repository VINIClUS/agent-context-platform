"""Callers cannot steer the platform's telemetry: trace state, sampling, export headers, labels.

Regression tests for the PLATFORM-050 review: client trace state never reaches an export, a
client's sampling decision never drops platform spans, the MCP SDK's body-driven trace context is
switched off, malformed export headers never reach a log, an Origin rejection is not an auth
error, and a failing telemetry setup never stops the process.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

import httpx2
import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from agent_context_platform.app import create_app
from agent_context_platform.mcp.server import create_mcp_server
from agent_context_platform.observability import contract, core, current
from agent_context_platform.observability import setup as telemetry_setup
from agent_context_platform.observability.core import retrieval_span
from agent_context_platform.observability.setup import build_telemetry
from agent_context_platform.settings import ObservabilitySettings, Settings

from ._capture import Canaries, Capture, log_text
from .test_mcp_telemetry import _call, _client, _headers, _rpc

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
UNSAMPLED = f"00-{TRACE_ID}-00f067aa0ba902b7-00"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class ListExporter(SpanExporter):
    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None


async def _post_with_meta(
    client: httpx2.AsyncClient, token: str, trace_meta: dict[str, str]
) -> httpx2.Response:
    """A `symbol_context` call whose `params._meta` also carries ``trace_meta``."""
    body = _rpc("tools/call", {"name": "symbol_context", "arguments": {"query": "q"}})
    body["params"]["_meta"].update(trace_meta)
    return await client.post(
        "/mcp",
        json=body,
        headers={
            **_headers("tools/call", **{"mcp-name": "symbol_context"}),
            "authorization": f"Bearer {token}",
        },
    )


def _settings(**observability: Any) -> Settings:
    return Settings(environment="test", observability=ObservabilitySettings(**observability))


async def test_client_trace_state_and_sampling_never_reach_the_exported_spans(
    canaries: Canaries,
) -> None:
    # The production pipeline (sampler included), with a recording exporter in place of OTLP.
    exporter = ListExporter()
    settings = _settings(otlp_endpoint="http://127.0.0.1:9", schedule_delay_seconds=0.01)
    telemetry = build_telemetry(settings, environ={}, span_exporter=exporter)
    assert telemetry is not None
    previous = core.install(telemetry)
    try:
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=create_app(settings)),
            base_url="http://127.0.0.1:8000",
        ) as client:
            response = await client.get(
                "/health/live",
                headers={"traceparent": UNSAMPLED, "tracestate": f"vendor={canaries.content}"},
            )
        assert response.status_code == 200
    finally:
        core.install(previous)
        telemetry.shutdown(2.0)

    (server,) = [span for span in exporter.spans if span.name == "GET /health/live"]
    # Linked to the caller's trace, but sampled whatever the caller asked for.
    assert format(server.context.trace_id, "032x") == TRACE_ID
    assert server.context.trace_flags.sampled
    assert len(server.context.trace_state) == 0
    assert server.parent is not None and len(server.parent.trace_state) == 0
    for span in exporter.spans:
        canaries.assert_absent(span.to_json())


async def test_only_the_body_traceparent_is_imported_never_its_state_or_sampling(
    canaries: Canaries,
) -> None:
    from mcp.server._otel import OpenTelemetryMiddleware

    from agent_context_platform.mcp.telemetry import MetaTraceContext

    middleware = create_mcp_server().middleware
    assert not any(isinstance(item, OpenTelemetryMiddleware) for item in middleware)
    assert any(isinstance(item, MetaTraceContext) for item in middleware)

    # The production sampler: a body flag of 00 must not drop the tool's spans.
    exporter = ListExporter()
    settings = _settings(otlp_endpoint="http://127.0.0.1:9", schedule_delay_seconds=0.01)
    telemetry = build_telemetry(settings, environ={}, span_exporter=exporter)
    assert telemetry is not None
    previous = core.install(telemetry)
    body_trace = f"00-{'a' * 32}-{'b' * 16}-00"
    try:
        async with _client(canaries) as (client, token):
            response = await _post_with_meta(
                client,
                token,
                {
                    "traceparent": body_trace,
                    "tracestate": f"vendor={canaries.prompt}",
                    "baggage": f"user={canaries.content}",
                },
            )
        assert response.status_code == 200
    finally:
        core.install(previous)
        telemetry.shutdown(2.0)

    (retrieval,) = [
        span for span in exporter.spans if span.name == "agent_context.retrieval.symbol_context"
    ]
    # The profile's per-request trace context parents the request's work (sampled regardless).
    assert format(retrieval.context.trace_id, "032x") == "a" * 32
    assert retrieval.parent is not None and format(retrieval.parent.span_id, "016x") == "b" * 16
    assert retrieval.context.trace_flags.sampled
    assert len(retrieval.context.trace_state) == 0
    for span in exporter.spans:
        canaries.assert_absent(span.to_json())


async def test_an_invalid_body_traceparent_keeps_the_platform_request_span_as_parent(
    telemetry: Capture, canaries: Canaries
) -> None:
    async with _client(canaries) as (client, token):
        response = await _post_with_meta(client, token, {"traceparent": f"00-{canaries.content}"})
    assert response.status_code == 200
    call = telemetry.span("tools/call")
    retrieval = telemetry.span("agent_context.retrieval.symbol_context")
    assert retrieval.parent is not None and retrieval.parent.span_id == call.context.span_id
    canaries.assert_absent(telemetry.dump())


@pytest.mark.parametrize(
    ("variable", "metrics_export"),
    [("OTEL_EXPORTER_OTLP_HEADERS", False), ("OTEL_EXPORTER_OTLP_TRACES_HEADERS", True)],
)
def test_malformed_export_headers_disable_export_without_logging_them(
    caplog: pytest.LogCaptureFixture, canaries: Canaries, variable: str, metrics_export: bool
) -> None:
    caplog.set_level(logging.DEBUG)
    # Not `key=value`: the SDK's liberal parser would log the whole entry, secret included.
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
        variable: f"CF-Access-Client-Secret: {canaries.token_secret}",
    }

    telemetry = build_telemetry(_settings(), environ=environ)

    if metrics_export:
        # Only the traces headers are malformed: metrics still export, traces never do.
        assert telemetry is not None
        assert type(telemetry.tracer).__name__ == "NoOpTracer"
        assert telemetry.exports_metrics
        telemetry.shutdown(1.0)
    else:
        assert telemetry is None
    assert "telemetry export disabled: malformed OTLP headers" in caplog.text
    canaries.assert_absent(log_text(caplog))


def test_well_formed_export_headers_are_accepted(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
        "OTEL_EXPORTER_OTLP_HEADERS": "cf-access-client-id=abc.access, x-other = v%20alue",
    }
    telemetry = build_telemetry(_settings(), environ=environ)
    assert telemetry is not None
    telemetry.shutdown(1.0)
    assert "malformed" not in caplog.text


async def test_an_origin_rejection_is_a_client_error_not_an_auth_error(
    telemetry: Capture, canaries: Canaries
) -> None:
    async with _client(canaries) as (client, token):
        origin = await _call(client, token, "tools/list", origin="https://evil.example")
        host = await _call(client, token, "tools/list", host="evil.example")
        scope = await _call(client, None, "tools/list")
    assert (origin.status_code, host.status_code, scope.status_code) == (403, 421, 401)

    assert telemetry.points(contract.MCP_REQUESTS) == {
        (("method", "tools/list"), ("outcome", "client_error"), ("tool", "none")): 2,
        (("method", "tools/list"), ("outcome", "auth_error"), ("tool", "none")): 1,
    }


def test_a_failing_telemetry_setup_never_stops_the_process(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def explode(*_args: Any, **_kwargs: Any) -> Any:
        raise ValueError("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT=lots")

    monkeypatch.setattr(telemetry_setup, "build_telemetry", explode)
    before = current()

    assert telemetry_setup.start(_settings(otlp_endpoint="http://collector:4318")) is None
    assert current() is before
    assert "telemetry setup failed; continuing without export" in caplog.text
    assert "lots" not in caplog.text


def test_a_retrieval_outcome_is_closed_even_when_set_directly(telemetry: Capture) -> None:
    with retrieval_span("graph") as observation:
        observation.outcome = "made-up"

    assert telemetry.points(contract.RETRIEVAL_DURATION) == {
        (("operation", "graph"), ("outcome", "other")): 1
    }
