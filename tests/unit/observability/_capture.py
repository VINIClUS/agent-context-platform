"""In-memory telemetry capture for unit tests (fixtures re-exported by `tests/unit/conftest.py`).

The real allowlist, in-memory span and metric readers.

`telemetry` installs a handle whose spans pass the same `SanitizingSpanProcessor` production
uses (into an in-memory exporter) and whose metrics are read by an in-memory reader; the previous
handle is restored afterwards. `Capture.dump` serializes every exported span (resource included)
and every metric data point, for canary scans.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    Gauge,
    Histogram,
    HistogramDataPoint,
    InMemoryMetricReader,
    Sum,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_context_platform.observability import Telemetry, install
from agent_context_platform.observability.redaction import SanitizingSpanProcessor
from agent_context_platform.observability.setup import build_resource
from agent_context_platform.settings import Settings


@dataclass
class Capture:
    spans_exporter: InMemorySpanExporter
    reader: InMemoryMetricReader
    handle: Telemetry

    def spans(self, name: str | None = None) -> list[ReadableSpan]:
        found = list(self.spans_exporter.get_finished_spans())
        return found if name is None else [span for span in found if span.name == name]

    def span(self, name: str) -> ReadableSpan:
        found = self.spans(name)
        assert len(found) == 1, [span.name for span in self.spans()]
        return found[0]

    def metrics(self) -> dict[str, Any]:
        """Instrument name -> metric, across every resource and scope."""
        data = self.reader.get_metrics_data()
        found: dict[str, Any] = {}
        if data is None:
            return found
        for resource_metrics in data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    found[metric.name] = metric
        return found

    def points(self, name: str) -> dict[tuple[tuple[str, Any], ...], float]:
        """Attribute set -> value (sum, gauge value or histogram count) for one instrument."""
        metric = self.metrics().get(name)
        if metric is None:
            return {}
        values: dict[tuple[tuple[str, Any], ...], float] = {}
        for point in metric.data.data_points:
            key = tuple(sorted((point.attributes or {}).items()))
            values[key] = point.count if isinstance(point, HistogramDataPoint) else point.value
        return values

    def value(self, name: str, **attributes: Any) -> float:
        return self.points(name).get(tuple(sorted(attributes.items())), 0)

    def kind(self, name: str) -> str:
        data = self.metrics()[name].data
        if isinstance(data, Sum):
            return "counter"
        if isinstance(data, Histogram):
            return "histogram"
        assert isinstance(data, Gauge)
        return "gauge"

    def dump(self) -> str:
        """Every exported span and metric data point as text (for canary scans)."""
        spans = "\n".join(span.to_json() for span in self.spans())
        data = self.reader.get_metrics_data()
        metrics = "" if data is None else data.to_json()
        return spans + "\n" + metrics


@contextmanager
def capture_telemetry() -> Iterator[Capture]:
    """Install an in-memory handle for the block; the previous handle is restored after."""
    resource = build_resource(Settings(environment="test"))
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(SanitizingSpanProcessor(SimpleSpanProcessor(exporter)))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    handle = Telemetry(tracer_provider, meter_provider)
    previous = install(handle)
    try:
        yield Capture(exporter, reader, handle)
    finally:
        install(previous)
        tracer_provider.shutdown()
        meter_provider.shutdown()


@pytest.fixture
def telemetry() -> Iterator[Capture]:
    with capture_telemetry() as capture:
        yield capture


@dataclass(frozen=True)
class Canaries:
    """Fresh per-test secret-shaped values; none may appear in any telemetry or log record."""

    content: str
    prompt: str
    tool_argument: str
    token_secret: str
    dsn_password: str
    path: str

    def all(self) -> tuple[str, ...]:
        return (
            self.content,
            self.prompt,
            self.tool_argument,
            self.token_secret,
            self.dsn_password,
            self.path,
        )

    def assert_absent(self, *texts: str) -> None:
        for text in texts:
            for canary in self.all():
                assert canary not in text


@pytest.fixture
def canaries() -> Canaries:
    def fresh(label: str) -> str:
        return f"CANARY{label}{secrets.token_hex(8)}"

    return Canaries(
        content=fresh("content"),
        prompt=fresh("prompt"),
        tool_argument=fresh("toolarg"),
        token_secret=fresh("token") + "x" * 20,
        dsn_password=fresh("dsn"),
        path=f"/home/{fresh('path')}/repo",
    )


def log_text(caplog: pytest.LogCaptureFixture, *prefixes: str) -> str:
    """Captured log records (message and extra fields), optionally only the named loggers."""
    return "\n".join(
        f"{record.getMessage()} {json.dumps(record.__dict__, default=str)}"
        for record in caplog.records
        if not prefixes or record.name.startswith(prefixes)
    )
