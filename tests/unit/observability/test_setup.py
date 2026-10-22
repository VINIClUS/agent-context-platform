"""Provider setup: disabled without an endpoint, bounded queues, never blocking on the exporter."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Sequence
from typing import Any

import httpx2
import pytest
from opentelemetry import trace as trace_api
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    MetricExportResult,
    MetricsData,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from pydantic import ValidationError

from agent_context_platform.app import create_app
from agent_context_platform.observability import core, current
from agent_context_platform.observability import setup as telemetry_setup
from agent_context_platform.observability.setup import build_resource, build_telemetry
from agent_context_platform.settings import ObservabilitySettings, Settings

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

_CLOSED_PORT_ENDPOINT = "http://127.0.0.1:9"


class SlowSpanExporter(SpanExporter):
    """A collector that hangs on every export (until released) and then fails."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.calls = 0

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.calls += 1
        self.release.wait(timeout=5)
        return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        self.release.set()


class FailingMetricExporter(MetricExporter):
    def __init__(self) -> None:
        super().__init__(preferred_temporality={}, preferred_aggregation={})
        self.calls = 0

    def export(
        self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: Any
    ) -> MetricExportResult:
        self.calls += 1
        raise ConnectionError("collector down")

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return False

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        return None


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _settings(**observability: Any) -> Settings:
    return Settings(environment="test", observability=ObservabilitySettings(**observability))


def test_disabled_without_an_endpoint() -> None:
    assert build_telemetry(_settings(), environ={}) is None


@pytest.mark.parametrize(
    "environ",
    [
        {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318", "OTEL_SDK_DISABLED": "true"},
        {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318", "OTEL_SDK_DISABLED": " TRUE "},
        {"OTEL_EXPORTER_OTLP_ENDPOINT": "  "},
    ],
)
def test_the_kill_switch_and_blank_endpoints_disable(environ: dict[str, str]) -> None:
    assert build_telemetry(_settings(), environ=environ) is None


def test_settings_can_disable_even_with_an_endpoint() -> None:
    settings = _settings(enabled=False, otlp_endpoint="http://collector:4318")
    assert build_telemetry(settings, environ={}) is None


def test_the_settings_endpoint_gets_the_signal_paths() -> None:
    telemetry = build_telemetry(_settings(otlp_endpoint="http://collector:4318/"), environ={})
    assert telemetry is not None
    try:
        assert not isinstance(telemetry.tracer, trace_api.NoOpTracer)
    finally:
        telemetry.shutdown(1.0)


def test_a_signal_without_an_endpoint_stays_a_no_op() -> None:
    environ = {"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://collector:4318/v1/traces"}
    telemetry = build_telemetry(_settings(), environ=environ)
    assert telemetry is not None
    try:
        assert not isinstance(telemetry.tracer, trace_api.NoOpTracer)
        # No metrics endpoint: the meter is a no-op, so nothing is ever sent to a default host.
        assert type(telemetry.mcp_requests).__name__.startswith("NoOp")
    finally:
        telemetry.shutdown(1.0)

    environ = {"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://collector:4318/v1/metrics"}
    telemetry = build_telemetry(_settings(), environ=environ)
    assert telemetry is not None
    try:
        assert isinstance(telemetry.tracer, trace_api.NoOpTracer)
        assert not type(telemetry.mcp_requests).__name__.startswith("NoOp")
    finally:
        telemetry.shutdown(1.0)


def test_the_resource_carries_the_contract_and_the_loop_marker() -> None:
    attributes = dict(build_resource(_settings(service_instance_id="replica-1")).attributes)
    assert attributes["service.name"] == "agent-context-platform"
    assert attributes["service.namespace"] == "agent-context"
    assert attributes["service.instance.id"] == "replica-1"
    assert attributes["deployment.environment"] == "test"
    assert attributes["agent_context.origin"] == "platform"
    assert isinstance(attributes["service.version"], str) and attributes["service.version"]
    # A fresh process id per handle when none is configured.
    first = build_resource(_settings()).attributes["service.instance.id"]
    assert first != build_resource(_settings()).attributes["service.instance.id"]


def test_the_resource_ignores_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_SERVICE_NAME", "codex_cli_rs")
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "agent_context.origin=codex")
    attributes = dict(build_resource(_settings()).attributes)
    assert attributes["service.name"] == "agent-context-platform"
    assert attributes["agent_context.origin"] == "platform"


def test_the_version_falls_back_when_the_package_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from importlib import metadata

    def missing(_name: str) -> str:
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(telemetry_setup.metadata, "version", missing)
    assert build_resource(_settings()).attributes["service.version"] == "0+unknown"


@pytest.mark.parametrize(
    "overrides",
    [
        {"otlp_endpoint": "http://user:secret@collector:4318"},
        {"max_queue_size": 10, "max_export_batch_size": 20},
        {"export_timeout_seconds": 0},
        {"service_instance_id": ""},
    ],
)
def test_invalid_observability_settings_are_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError) as info:
        ObservabilitySettings(**overrides)
    assert "secret" not in str(info.value)


def test_settings_read_the_observability_section_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_OBSERVABILITY__OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("AGENT_CONTEXT_OBSERVABILITY__METRIC_EXPORT_INTERVAL_SECONDS", "10")
    observability = Settings().observability
    assert str(observability.otlp_endpoint) == "http://collector:4318/"
    assert observability.metric_export_interval_seconds == 10


async def test_start_installs_and_stop_restores_the_no_op_handle() -> None:
    settings = _settings(otlp_endpoint=_CLOSED_PORT_ENDPOINT, shutdown_timeout_seconds=1)
    telemetry = telemetry_setup.start(settings)
    try:
        assert telemetry is not None
        assert current() is telemetry
    finally:
        await telemetry_setup.stop(telemetry, settings)
    assert current() is not telemetry
    assert isinstance(current().tracer, trace_api.NoOpTracer)
    await telemetry_setup.stop(None, settings)  # nothing to stop


async def test_a_hanging_collector_never_slows_or_fails_a_request() -> None:
    settings = _settings(
        otlp_endpoint=_CLOSED_PORT_ENDPOINT,
        schedule_delay_seconds=0.01,
        max_queue_size=8,
        max_export_batch_size=4,
        metric_export_interval_seconds=1,
        export_timeout_seconds=0.5,
        shutdown_timeout_seconds=0.5,
    )
    spans = SlowSpanExporter()
    metrics = FailingMetricExporter()
    telemetry = build_telemetry(settings, environ={}, span_exporter=spans, metric_exporter=metrics)
    assert telemetry is not None
    previous = core.install(telemetry)
    app = create_app(settings)
    try:
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
        ) as client:
            started = time.perf_counter()
            for _ in range(60):  # far more spans than the queue holds: the excess is dropped
                response = await client.get("/health/live")
                assert response.status_code == 200
            elapsed = time.perf_counter() - started
        # 60 requests in well under one blocked export (5 s); no request waited on the exporter.
        assert elapsed < 3.0
        assert spans.calls >= 1
    finally:
        core.install(previous)
        started = time.perf_counter()
        await telemetry_setup.stop(telemetry, settings)
        assert time.perf_counter() - started < 3.0


async def test_an_unreachable_otlp_endpoint_never_fails_a_request() -> None:
    settings = _settings(
        otlp_endpoint=_CLOSED_PORT_ENDPOINT,
        schedule_delay_seconds=0.01,
        export_timeout_seconds=0.5,
        shutdown_timeout_seconds=0.5,
    )
    telemetry = build_telemetry(settings, environ={})
    assert telemetry is not None
    previous = core.install(telemetry)
    try:
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=create_app(settings)),
            base_url="http://127.0.0.1:8000",
        ) as client:
            for _ in range(5):
                assert (await client.get("/health/live")).status_code == 200
            await asyncio.sleep(0.1)  # let the real OTLP exporter try (and fail) in the background
    finally:
        core.install(previous)
        await telemetry_setup.stop(telemetry, settings)


async def test_shutdown_is_bounded_even_if_the_providers_hang() -> None:
    gate = threading.Event()
    telemetry = core.Telemetry(
        trace_api.NoOpTracerProvider(),
        core.metrics_api.NoOpMeterProvider(),
        shutdown=lambda _timeout: gate.wait(5),
    )
    settings = _settings(shutdown_timeout_seconds=0.2)
    started = time.perf_counter()
    await telemetry_setup.stop(telemetry, settings)
    gate.set()
    assert time.perf_counter() - started < 2.0
    # A second shutdown is a no-op.
    telemetry.shutdown(0.1)
