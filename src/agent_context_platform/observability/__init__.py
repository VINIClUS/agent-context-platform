"""Platform self-telemetry (PLATFORM-050): contract v1 in `docs/operations/telemetry.md`.

Instrumentation imports from here. The submodules: `contract` (names and closed attribute sets),
`core` (the current handle and fail-open helpers), `redaction` (the span attribute allowlist),
`setup` (OTLP providers from settings), `http` (the HTTP server middleware) and `projection`
(the worker's gauge sampler).
"""

from agent_context_platform.observability.contract import (
    SPAN_CONTENT_PREPARE,
    SPAN_INDEX_RUN,
    SPAN_LEDGER_APPEND,
    SPAN_PROJECTION_BATCH,
    SPAN_PROJECTION_EVENT,
)
from agent_context_platform.observability.core import (
    ProjectionSnapshot,
    RetrievalObservation,
    Telemetry,
    current,
    install,
    mark_error,
    observe_ingestion,
    record_content_object,
    record_index_run,
    record_ingestion_events,
    record_integrity_failure,
    record_projection_duration,
    record_projection_event,
    retrieval_span,
    set_projection_snapshot,
    set_span_attributes,
    traced,
    traced_retrieval,
)

__all__ = [
    "SPAN_CONTENT_PREPARE",
    "SPAN_INDEX_RUN",
    "SPAN_LEDGER_APPEND",
    "SPAN_PROJECTION_BATCH",
    "SPAN_PROJECTION_EVENT",
    "ProjectionSnapshot",
    "RetrievalObservation",
    "Telemetry",
    "current",
    "install",
    "mark_error",
    "observe_ingestion",
    "record_content_object",
    "record_index_run",
    "record_ingestion_events",
    "record_integrity_failure",
    "record_projection_duration",
    "record_projection_event",
    "retrieval_span",
    "set_projection_snapshot",
    "set_span_attributes",
    "traced",
    "traced_retrieval",
]
