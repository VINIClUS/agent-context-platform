"""Build and install the OTLP/HTTP providers from settings and the standard ``OTEL_*`` variables.

Each signal is exported only when an endpoint is configured for it (the settings endpoint, the
generic ``OTEL_EXPORTER_OTLP_ENDPOINT`` or the signal-specific one); ``OTEL_SDK_DISABLED=true`` or
``observability.enabled=false`` disables both. Without an endpoint nothing is built and the no-op
handle stays current. Spans go through the attribute allowlist, then a batch processor with a
bounded queue; metrics through a periodic reader. Both export on their own threads, so a collector
that is down or slow never blocks a request; shutdown flushes within a bounded time.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from importlib import metadata
from typing import Final

from opentelemetry import metrics as metrics_api
from opentelemetry import trace as trace_api
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter

from agent_context_platform.observability import contract
from agent_context_platform.observability.core import Telemetry, current, install
from agent_context_platform.observability.redaction import SanitizingSpanProcessor
from agent_context_platform.settings import Settings

_SDK_DISABLED: Final = "OTEL_SDK_DISABLED"
_GENERIC_ENDPOINT: Final = "OTEL_EXPORTER_OTLP_ENDPOINT"
_TRACES_ENDPOINT: Final = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
_METRICS_ENDPOINT: Final = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
_PACKAGE: Final = "agent-context-platform"
_UNKNOWN_VERSION: Final = "0+unknown"


def _version() -> str:
    try:
        return metadata.version(_PACKAGE)
    except metadata.PackageNotFoundError:
        return _UNKNOWN_VERSION


def build_resource(settings: Settings) -> Resource:
    """The fixed resource of every platform signal, including the loop-prevention marker.

    Built explicitly (no environment detectors), so ``OTEL_SERVICE_NAME`` or
    ``OTEL_RESOURCE_ATTRIBUTES`` can never rename the service or drop the origin marker.
    """
    observability = settings.observability
    return Resource(
        {
            "service.name": contract.SERVICE_NAME,
            "service.namespace": contract.SERVICE_NAMESPACE,
            "service.version": _version(),
            "service.instance.id": observability.service_instance_id or str(uuid.uuid4()),
            "deployment.environment": settings.environment,
            contract.ORIGIN_ATTRIBUTE: contract.ORIGIN_PLATFORM,
        }
    )


def _signal_endpoint(
    settings: Settings, environ: Mapping[str, str], specific: str, path: str
) -> tuple[bool, str | None]:
    """Whether a signal is exported, and the explicit endpoint (``None``: the exporter reads env)."""
    observability = settings.observability
    if not observability.enabled or environ.get(_SDK_DISABLED, "").strip().lower() == "true":
        return False, None
    if observability.otlp_endpoint is not None:
        return True, str(observability.otlp_endpoint).rstrip("/") + path
    configured = environ.get(specific, "").strip() or environ.get(_GENERIC_ENDPOINT, "").strip()
    return bool(configured), None


def build_telemetry(
    settings: Settings,
    *,
    environ: Mapping[str, str] | None = None,
    span_exporter: SpanExporter | None = None,
    metric_exporter: MetricExporter | None = None,
) -> Telemetry | None:
    """The configured telemetry handle, or ``None`` when no signal has an endpoint.

    ``span_exporter`` / ``metric_exporter`` replace the OTLP exporters (tests); they still go
    through the allowlist, the batch processor and the periodic reader.
    """
    env = os.environ if environ is None else environ
    observability = settings.observability
    traces, traces_endpoint = _signal_endpoint(settings, env, _TRACES_ENDPOINT, "/v1/traces")
    metrics, metrics_endpoint = _signal_endpoint(settings, env, _METRICS_ENDPOINT, "/v1/metrics")
    if not traces and not metrics:
        return None
    resource = build_resource(settings)
    timeout = observability.export_timeout_seconds

    tracer_provider: trace_api.TracerProvider = trace_api.NoOpTracerProvider()
    sdk_tracer: TracerProvider | None = None
    spans: SpanExporter | None = None
    if traces:
        spans = span_exporter or OTLPSpanExporter(endpoint=traces_endpoint, timeout=timeout)
        sdk_tracer = TracerProvider(resource=resource)
        sdk_tracer.add_span_processor(
            SanitizingSpanProcessor(
                BatchSpanProcessor(
                    spans,
                    max_queue_size=observability.max_queue_size,
                    schedule_delay_millis=observability.schedule_delay_seconds * 1000,
                    max_export_batch_size=observability.max_export_batch_size,
                    export_timeout_millis=timeout * 1000,
                )
            )
        )
        tracer_provider = sdk_tracer

    meter_provider: metrics_api.MeterProvider = metrics_api.NoOpMeterProvider()
    sdk_meter: MeterProvider | None = None
    if metrics:
        reader = PeriodicExportingMetricReader(
            metric_exporter or OTLPMetricExporter(endpoint=metrics_endpoint, timeout=timeout),
            export_interval_millis=observability.metric_export_interval_seconds * 1000,
            export_timeout_millis=timeout * 1000,
        )
        sdk_meter = MeterProvider(resource=resource, metric_readers=[reader])
        meter_provider = sdk_meter

    def shutdown(timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        if sdk_tracer is not None and spans is not None:
            # The SDK's force_flush ignores its timeout (it exports every queued batch), so it
            # runs on a daemon thread; each step gets a third of the timeout, metrics the rest.
            _bounded(sdk_tracer.force_flush, timeout_seconds / 3)
            # Stop the exporter before the processor drains its queue: what the bounded flush
            # could not send is dropped at once instead of waiting on the collector per batch.
            with contextlib.suppress(Exception):
                spans.shutdown()
            _bounded(sdk_tracer.shutdown, timeout_seconds / 3)
        if sdk_meter is not None:
            meter = sdk_meter
            remaining = max(deadline - time.monotonic(), 0.001)
            _bounded(lambda: meter.shutdown(timeout_millis=remaining * 1000), remaining)

    return Telemetry(tracer_provider, meter_provider, shutdown=shutdown)


def _bounded(call: Callable[[], object], seconds: float) -> None:
    """Run ``call`` on a daemon thread and wait for it at most ``seconds`` (errors swallowed)."""

    def run() -> None:
        with contextlib.suppress(Exception):
            call()

    worker = threading.Thread(target=run, name="agent-context-telemetry-shutdown", daemon=True)
    worker.start()
    worker.join(seconds)


def start(settings: Settings) -> Telemetry | None:
    """Install the configured handle; ``None`` (the no-op handle stays) when disabled."""
    telemetry = build_telemetry(settings)
    if telemetry is not None:
        install(telemetry)
    return telemetry


async def stop(telemetry: Telemetry | None, settings: Settings) -> None:
    """Flush and shut down ``telemetry`` off the event loop, bounded by the shutdown timeout.

    The disabled handle is reinstalled first, so nothing records into providers being shut down.
    """
    if telemetry is None:
        return
    if current() is telemetry:
        install(Telemetry.disabled())
    timeout = settings.observability.shutdown_timeout_seconds
    with contextlib.suppress(Exception):
        await asyncio.wait_for(asyncio.to_thread(telemetry.shutdown, timeout), timeout + 1.0)
