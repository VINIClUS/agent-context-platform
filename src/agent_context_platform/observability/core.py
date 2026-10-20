"""The process telemetry handle and the fail-open helpers instrumentation calls.

`Telemetry` owns the tracer and every contract instrument. The process holds one current handle:
a disabled one (OpenTelemetry no-op providers) until `install` replaces it, which `setup` does when
an endpoint is configured and tests do with in-memory readers. Instrumentation never touches
providers directly: it calls the `record_*` helpers, `traced` and `retrieval_span`, which bound
every attribute value (`contract.closed`) and never raise (a telemetry failure must not fail or
slow a request).
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Final

from opentelemetry import metrics as metrics_api
from opentelemetry import trace as trace_api
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.trace import SpanKind, Status, StatusCode

from agent_context_platform.observability import contract

_LOG = logging.getLogger(__name__)


# Fixed ingestion error codes (ledger/api.py and ledger/service.py) -> contract `reason`.
_INGESTION_REASONS: Final[Mapping[str, str]] = {
    "missing_credential": "auth",
    "invalid_credential": "auth",
    "insufficient_scope": "auth",
    "idempotency_key_required": "schema",
    "idempotency_key_mismatch": "schema",
    "invalid_request_schema": "schema",
    "duplicate_idempotency_key": "schema",
    "duplicate_content_id": "schema",
    "unsupported_media_type": "schema",
    "invalid_content_encoding": "schema",
    "content_resolution_mismatch": "schema",
    "content_requires_redaction": "redaction_required",
    "request_too_large": "too_large",
    "producer_mismatch": "producer_mismatch",
    "idempotency_conflict": "conflict",
    "stream_quarantined": "conflict",
}


def _fail_open[**P](function: Callable[P, None]) -> Callable[P, None]:
    """Run a recording helper; a telemetry failure is swallowed, never raised to the caller."""

    @functools.wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        try:
            function(*args, **kwargs)
        except Exception:
            # No exception text: an SDK or exporter message is not ours to vouch for.
            _LOG.debug("telemetry recording failed", exc_info=False)

    return wrapper


@dataclass(frozen=True, slots=True)
class ProjectionSnapshot:
    """What the projection gauges report: refreshed by the worker, read by metric collection."""

    outbox_pending: int
    lag_seconds: Mapping[str, float] = field(default_factory=dict)
    dead_letters: Mapping[str, int] = field(default_factory=dict)


class Telemetry:
    """A tracer plus the contract instruments, bound to one tracer and one meter provider."""

    def __init__(
        self,
        tracer_provider: trace_api.TracerProvider,
        meter_provider: metrics_api.MeterProvider,
        *,
        shutdown: Callable[[float], None] | None = None,
    ) -> None:
        self.tracer = tracer_provider.get_tracer(contract.INSTRUMENTATION_SCOPE)
        meter = meter_provider.get_meter(contract.INSTRUMENTATION_SCOPE)
        self._shutdown = shutdown
        self._projection: ProjectionSnapshot | None = None

        def histogram(name: str, description: str) -> metrics_api.Histogram:
            return meter.create_histogram(
                name,
                unit="s",
                description=description,
                explicit_bucket_boundaries_advisory=list(contract.DURATION_BUCKETS),
            )

        self.ingestion_batches = meter.create_counter(
            contract.INGESTION_BATCHES, unit="{batch}", description="Ingestion requests."
        )
        self.ingestion_events = meter.create_counter(
            contract.INGESTION_EVENTS, unit="{event}", description="Events per ingestion result."
        )
        self.ingestion_duration = histogram(
            contract.INGESTION_DURATION, "Duration of ingestion requests."
        )
        self.content_objects = meter.create_counter(
            contract.CONTENT_OBJECTS, unit="{object}", description="Prepared content items."
        )
        self.projection_events = meter.create_counter(
            contract.PROJECTION_EVENTS, unit="{event}", description="Projected outbox events."
        )
        self.projection_duration = histogram(
            contract.PROJECTION_DURATION, "Duration of one projector applying one event."
        )
        self.integrity_failures = meter.create_counter(
            contract.LEDGER_INTEGRITY_FAILURES,
            unit="{failure}",
            description="Detected ledger integrity failures.",
        )
        self.index_runs = meter.create_counter(
            contract.INDEX_RUNS, unit="{run}", description="Repository index runs."
        )
        self.index_duration = histogram(contract.INDEX_DURATION, "Duration of index runs.")
        self.retrieval_duration = histogram(
            contract.RETRIEVAL_DURATION, "Duration of retrieval operations."
        )
        self.mcp_requests = meter.create_counter(
            contract.MCP_REQUESTS, unit="{request}", description="MCP requests."
        )
        self.mcp_duration = histogram(contract.MCP_DURATION, "Duration of MCP requests.")
        self.http_duration = histogram(
            contract.HTTP_SERVER_DURATION, "Duration of HTTP server requests."
        )
        meter.create_observable_gauge(
            contract.PROJECTION_LAG,
            callbacks=[self._observe_lag],
            unit="s",
            description="Age of the oldest outstanding outbox row.",
        )
        meter.create_observable_gauge(
            contract.PROJECTION_OUTBOX_PENDING,
            callbacks=[self._observe_outbox_pending],
            unit="{event}",
            description="Outstanding (pending or leased) outbox rows.",
        )
        meter.create_observable_gauge(
            contract.PROJECTION_DEAD_LETTERS,
            callbacks=[self._observe_dead_letters],
            unit="{event}",
            description="Open dead letters.",
        )

    @classmethod
    def disabled(cls) -> Telemetry:
        """A handle on the no-op providers: every call is accepted and nothing is recorded."""
        return cls(trace_api.NoOpTracerProvider(), metrics_api.NoOpMeterProvider())

    def set_projection_snapshot(self, snapshot: ProjectionSnapshot | None) -> None:
        """Replace the gauge values; ``None`` stops reporting them until the next snapshot."""
        self._projection = snapshot

    def shutdown(self, timeout_seconds: float) -> None:
        """Flush and stop the providers this handle owns (bounded by ``timeout_seconds``)."""
        if self._shutdown is not None:
            shutdown, self._shutdown = self._shutdown, None
            shutdown(timeout_seconds)

    # Observable gauge callbacks: read the cached snapshot only, never a datastore.
    def _observe_lag(self, _options: CallbackOptions) -> Iterable[Observation]:
        snapshot = self._projection
        if snapshot is None:
            return ()
        return [
            Observation(seconds, {"projector": contract.projector_label(name)})
            for name, seconds in sorted(snapshot.lag_seconds.items())
        ]

    def _observe_outbox_pending(self, _options: CallbackOptions) -> Iterable[Observation]:
        snapshot = self._projection
        return () if snapshot is None else [Observation(snapshot.outbox_pending)]

    def _observe_dead_letters(self, _options: CallbackOptions) -> Iterable[Observation]:
        snapshot = self._projection
        if snapshot is None:
            return ()
        totals: dict[str, int] = {}
        for name, count in snapshot.dead_letters.items():
            label = contract.projector_label(name)
            totals[label] = totals.get(label, 0) + count
        return [Observation(count, {"projector": label}) for label, count in sorted(totals.items())]


_current: Telemetry = Telemetry.disabled()


def current() -> Telemetry:
    """The process telemetry handle (disabled until one is installed)."""
    return _current


def install(telemetry: Telemetry) -> Telemetry:
    """Make ``telemetry`` current and return the previous handle (tests restore it)."""
    global _current
    previous, _current = _current, telemetry
    return previous


# --- spans --------------------------------------------------------------------------------------


def error_type(error: BaseException) -> str:
    """The exception class name: the only part of an exception telemetry ever carries."""
    return type(error).__name__


def mark_error(span: trace_api.Span, error: BaseException | str) -> None:
    """Set ``ERROR`` status and ``error.type``; an exception adds an event with its class only."""
    try:
        name = error if isinstance(error, str) else error_type(error)
        span.set_status(Status(StatusCode.ERROR))
        span.set_attribute("error.type", name)
        if not isinstance(error, str):
            span.add_event("exception", {"exception.type": name})
    except Exception:
        _LOG.debug("telemetry span update failed", exc_info=False)


@_fail_open
def set_span_attributes(attributes: Mapping[str, contract.ScalarAttribute]) -> None:
    """Set attributes on the current span (the allowlist decides what is exported)."""
    span = trace_api.get_current_span()
    if span.is_recording():
        span.set_attributes(attributes)


@contextmanager
def traced(
    name: str,
    attributes: Mapping[str, contract.ScalarAttribute] | None = None,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
) -> Iterator[trace_api.Span]:
    """A current span around the block; an exception marks it failed (class only) and propagates.

    Exceptions are never recorded by the SDK itself (its event carries the message and stack).
    """
    with current().tracer.start_as_current_span(
        name,
        kind=kind,
        attributes=attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            yield span
        except Exception as error:
            mark_error(span, error)
            raise


@dataclass(slots=True)
class RetrievalObservation:
    """Handle yielded by `retrieval_span`; ``set_outcome`` overrides the inferred outcome."""

    outcome: str | None = None

    def set_outcome(self, outcome: str) -> None:
        self.outcome = contract.closed(outcome, contract.RETRIEVAL_OUTCOMES)


@contextmanager
def retrieval_span(
    operation: str,
    *,
    client_errors: tuple[type[BaseException], ...] = (),
    deadline_errors: tuple[type[BaseException], ...] = (TimeoutError,),
) -> Iterator[RetrievalObservation]:
    """Span `agent_context.retrieval.<operation>` plus the `retrieval.duration` sample.

    The outcome is ``ok`` when the block returns, ``deadline`` / ``client_error`` when it raises
    one of ``deadline_errors`` / ``client_errors`` and ``server_error`` for anything else;
    `RetrievalObservation.set_outcome` overrides it for a failure that is returned, not raised.
    Only ``server_error`` and ``deadline`` mark the span failed.
    """
    label = contract.closed(operation, contract.RETRIEVAL_OPERATIONS)
    observation = RetrievalObservation()
    started = time.perf_counter()
    with current().tracer.start_as_current_span(
        contract.SPAN_RETRIEVAL_PREFIX + label,
        attributes={"agent_context.operation": label},
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            yield observation
        except BaseException as error:
            if isinstance(error, deadline_errors):
                observation.outcome = "deadline"
            elif isinstance(error, client_errors):
                observation.outcome = "client_error"
            else:
                observation.outcome = "server_error"
            _finish_retrieval(span, label, observation.outcome, started, error)
            raise
        _finish_retrieval(span, label, observation.outcome or "ok", started, None)


def traced_retrieval[**P, R](
    operation: str,
    *,
    client_errors: tuple[type[BaseException], ...] = (),
    deadline_errors: tuple[type[BaseException], ...] = (TimeoutError,),
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Decorate an async retrieval operation with `retrieval_span` (same classification)."""

    def decorate(function: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(function)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with retrieval_span(
                operation, client_errors=client_errors, deadline_errors=deadline_errors
            ):
                return await function(*args, **kwargs)

        return wrapper

    return decorate


@_fail_open
def _finish_retrieval(
    span: trace_api.Span,
    operation: str,
    outcome: str,
    started: float,
    error: BaseException | None,
) -> None:
    span.set_attribute("agent_context.outcome", outcome)
    if outcome in {"server_error", "deadline"}:
        mark_error(span, error if error is not None else outcome)
    elif error is not None:
        span.set_attribute("error.type", error_type(error))
    current().retrieval_duration.record(
        time.perf_counter() - started, {"operation": operation, "outcome": outcome}
    )


# --- ingestion ----------------------------------------------------------------------------------


def classify_ingestion(status: int, codes: Iterable[str]) -> tuple[str, str]:
    """``(outcome, reason)`` for an ingestion response from its status and fixed error codes."""
    if status == 200:
        return "accepted", "none"
    if status >= 500:
        return "error", "internal"
    for code in sorted(codes):
        reason = _INGESTION_REASONS.get(code)
        if reason is not None:
            return "rejected", reason
    return "rejected", contract.OTHER


@dataclass(slots=True)
class IngestionObservation:
    """Filled in by the route: the response status and the fixed error codes it carries."""

    status: int = 500
    codes: tuple[str, ...] = ()


@contextmanager
def observe_ingestion() -> Iterator[IngestionObservation]:
    """Span `agent_context.ingest_batch` and the batch counter and duration for one request."""
    observation = IngestionObservation()
    started = time.perf_counter()
    with traced(contract.SPAN_INGEST_BATCH) as span:
        try:
            yield observation
        finally:
            _finish_ingestion(span, observation, time.perf_counter() - started)


@_fail_open
def _finish_ingestion(
    span: trace_api.Span, observation: IngestionObservation, seconds: float
) -> None:
    outcome, reason = classify_ingestion(observation.status, observation.codes)
    span.set_attributes({"agent_context.outcome": outcome, "agent_context.reason": reason})
    if outcome == "error":
        mark_error(span, "internal")
    telemetry = current()
    telemetry.ingestion_batches.add(1, {"outcome": outcome, "reason": reason})
    telemetry.ingestion_duration.record(seconds, {"outcome": outcome})


@_fail_open
def record_ingestion_events(*, created: int, existing: int) -> None:
    telemetry = current()
    if created:
        telemetry.ingestion_events.add(created, {"result": "created"})
    if existing:
        telemetry.ingestion_events.add(existing, {"result": "existing"})


# --- content, ledger, projection, index ---------------------------------------------------------


@_fail_open
def record_content_object(storage: str, result: str) -> None:
    current().content_objects.add(
        1,
        {
            "storage": contract.closed(storage, contract.CONTENT_STORAGES),
            "result": contract.closed(result, contract.CONTENT_RESULTS),
        },
    )


@_fail_open
def record_integrity_failure(check: str) -> None:
    current().integrity_failures.add(
        1, {"check": contract.closed(check, contract.INTEGRITY_CHECKS)}
    )


@_fail_open
def record_projection_event(projector: str | None, result: str) -> None:
    current().projection_events.add(
        1,
        {
            "projector": contract.projector_label(projector),
            "result": contract.closed(result, contract.PROJECTION_RESULTS),
        },
    )


@_fail_open
def record_projection_duration(projector: str, seconds: float) -> None:
    current().projection_duration.record(
        seconds, {"projector": contract.projector_label(projector)}
    )


@_fail_open
def record_index_run(result: str, seconds: float) -> None:
    label = contract.closed(result, contract.INDEX_RESULTS)
    telemetry = current()
    telemetry.index_runs.add(1, {"result": label})
    telemetry.index_duration.record(seconds, {"result": label})


@_fail_open
def set_projection_snapshot(snapshot: ProjectionSnapshot | None) -> None:
    current().set_projection_snapshot(snapshot)


# --- MCP and HTTP -------------------------------------------------------------------------------


def mcp_outcome(status: int) -> str:
    """The MCP ``outcome`` for the HTTP status the request was answered with."""
    if 200 <= status < 400:
        return "ok"
    if status in {401, 403}:
        return "auth_error"
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return "server_error"
    return "client_error"


@_fail_open
def record_mcp_request(method: str, tool: str, outcome: str, seconds: float) -> None:
    method_label = contract.closed(method, contract.MCP_METHODS)
    tool_label = tool if tool == contract.MCP_NO_TOOL else contract.closed(tool, contract.MCP_TOOLS)
    telemetry = current()
    telemetry.mcp_requests.add(
        1,
        {
            "method": method_label,
            "tool": tool_label,
            "outcome": contract.closed(outcome, contract.MCP_OUTCOMES),
        },
    )
    telemetry.mcp_duration.record(seconds, {"method": method_label, "tool": tool_label})


def http_method_label(method: str) -> str:
    return method if method in contract.HTTP_METHODS else contract.HTTP_OTHER_METHOD


@_fail_open
def record_http_request(method: str, route: str, status: int, seconds: float) -> None:
    current().http_duration.record(
        seconds,
        {
            "http.request.method": http_method_label(method),
            "http.route": contract.route_label(route),
            "http.response.status_code": status,
        },
    )
