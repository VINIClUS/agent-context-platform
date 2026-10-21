"""Telemetry contract v1: instrument names, units and the closed attribute value sets.

`docs/operations/telemetry.md` is the human-readable copy the infrastructure dashboards and alerts
are built from; the two must change together. Every attribute value recorded on a metric goes
through `closed`, so a value outside its set becomes `other` and cardinality stays bounded.
"""

from __future__ import annotations

import re
from collections.abc import Collection
from functools import cache
from typing import Final

SERVICE_NAME: Final = "agent-context-platform"
SERVICE_NAMESPACE: Final = "agent-context"
ORIGIN_ATTRIBUTE: Final = "agent_context.origin"
ORIGIN_PLATFORM: Final = "platform"
INSTRUMENTATION_SCOPE: Final = "agent_context_platform"

OTHER: Final = "other"

# Every attribute the platform records is a scalar (no sequences, maps or bytes).
type ScalarAttribute = str | bool | int | float

# --- metric instruments -------------------------------------------------------------------------
INGESTION_BATCHES: Final = "agent_context.ingestion.batches"
INGESTION_EVENTS: Final = "agent_context.ingestion.events"
INGESTION_DURATION: Final = "agent_context.ingestion.duration"
CONTENT_OBJECTS: Final = "agent_context.content.objects"
PROJECTION_EVENTS: Final = "agent_context.projection.events"
PROJECTION_DURATION: Final = "agent_context.projection.duration"
PROJECTION_LAG: Final = "agent_context.projection.lag"
PROJECTION_OUTBOX_PENDING: Final = "agent_context.projection.outbox_pending"
PROJECTION_DEAD_LETTERS: Final = "agent_context.projection.dead_letters"
LEDGER_INTEGRITY_FAILURES: Final = "agent_context.ledger.integrity_failures"
INDEX_RUNS: Final = "agent_context.index.runs"
INDEX_DURATION: Final = "agent_context.index.duration"
RETRIEVAL_DURATION: Final = "agent_context.retrieval.duration"
MCP_REQUESTS: Final = "agent_context.mcp.requests"
MCP_DURATION: Final = "agent_context.mcp.duration"
HTTP_SERVER_DURATION: Final = "http.server.request.duration"
OTEL_REQUESTS: Final = "agent_context.otel.requests"
OTEL_SPANS: Final = "agent_context.otel.spans"

DURATION_BUCKETS: Final = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

# --- span names ---------------------------------------------------------------------------------
SPAN_INGEST_BATCH: Final = "agent_context.ingest_batch"
SPAN_CONTENT_PREPARE: Final = "agent_context.content.prepare"
SPAN_LEDGER_APPEND: Final = "agent_context.ledger.append"
SPAN_PROJECTION_BATCH: Final = "agent_context.projection.batch"
SPAN_PROJECTION_EVENT: Final = "agent_context.projection.event"
SPAN_INDEX_RUN: Final = "agent_context.index.run"
SPAN_RETRIEVAL_PREFIX: Final = "agent_context.retrieval."

# --- closed attribute value sets ----------------------------------------------------------------
INGESTION_OUTCOMES: Final = frozenset({"accepted", "rejected", "error"})
INGESTION_REASONS: Final = frozenset(
    {
        "none",
        "auth",
        "schema",
        "redaction_required",
        "too_large",
        "producer_mismatch",
        "conflict",
        "internal",
    }
)
EVENT_RESULTS: Final = frozenset({"created", "existing"})
CONTENT_STORAGES: Final = frozenset({"inline", "object"})
CONTENT_RESULTS: Final = frozenset({"stored", "reused", "rejected"})
PROJECTION_RESULTS: Final = frozenset({"delivered", "retried", "dead_lettered"})
INTEGRITY_CHECKS: Final = frozenset(
    {"hash_chain", "missing_object", "digest_mismatch", "checkpoint"}
)
INDEX_RESULTS: Final = frozenset({"success", "failed", "refused"})
RETRIEVAL_OPERATIONS: Final = frozenset(
    {
        "context_for_task",
        "repository_overview",
        "symbol_context",
        "decision_history",
        "failure_history",
        "graph",
        "temporal",
        "search",
    }
)
# Codex OTLP trace receiver (PLATFORM-053).
OTEL_OUTCOMES: Final = frozenset({"accepted", "rejected", "error"})
OTEL_REASONS: Final = frozenset(
    {
        "none",
        "auth",
        "rate_limited",
        "unsupported_media_type",
        "too_large",
        "malformed",
        "internal",
    }
)
OTEL_SPAN_RESULTS: Final = frozenset({"mapped", "unmapped", "rejected"})
RETRIEVAL_OUTCOMES: Final = frozenset({"ok", "client_error", "server_error", "deadline"})
# The methods `mcp/server.py` serves (`_ALWAYS_SERVED | _TOOL_METHODS`); a test keeps them equal.
MCP_METHODS: Final = frozenset({"server/discover", "tools/list", "tools/call"})
MCP_TOOLS: Final = frozenset(
    {
        "context_for_task",
        "repository_overview",
        "symbol_context",
        "decision_history",
        "failure_history",
    }
)
MCP_NO_TOOL: Final = "none"
MCP_OUTCOMES: Final = frozenset(
    {"ok", "client_error", "auth_error", "rate_limited", "server_error"}
)
# Projector labels besides the registered projector names.
PROJECTOR_NONE: Final = "none"
PROJECTOR_UNATTRIBUTED: Final = "unattributed"
HTTP_METHODS: Final = frozenset(
    {"CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE"}
)
HTTP_OTHER_METHOD: Final = "_OTHER"
# Route templates are bounded to the platform's own route namespaces, so a value shaped like a
# filesystem path can never become an `http.route`.
_ROUTE_TEMPLATE: Final = re.compile(r"/[A-Za-z0-9_./{}-]{0,127}", re.ASCII)
ROUTE_PREFIXES: Final = ("/v1/", "/health/", "/mcp", "/openapi.json", "/docs", "/redoc")


def closed(value: object, allowed: Collection[str]) -> str:
    """``value`` when it is one of ``allowed``, else ``other``: never an unbounded label."""
    return value if isinstance(value, str) and value in allowed else OTHER


@cache
def route_label(route: object) -> str:
    """``route`` when it is a template under a platform route namespace, else ``other``."""
    if (
        isinstance(route, str)
        and _ROUTE_TEMPLATE.fullmatch(route) is not None
        and ".." not in route
        and route.startswith(ROUTE_PREFIXES)
    ):
        return route
    return OTHER


@cache
def registered_projector_names() -> frozenset[str]:
    """The registered projector names, read once (lazily: the registry imports the runtime)."""
    from agent_context_platform.projection.registry import registered_projectors

    return frozenset(projector.name for projector in registered_projectors())


def projector_label(name: str | None) -> str:
    """The bounded ``projector`` attribute: a registered name, ``unattributed`` or ``other``."""
    if name is None:
        return PROJECTOR_UNATTRIBUTED
    if name in {PROJECTOR_NONE, PROJECTOR_UNATTRIBUTED}:
        return name
    return closed(name, registered_projector_names())


def mcp_tool_label(method: str, tool: object) -> str:
    """``tool`` for a ``tools/call`` naming one of the five tools, ``none`` without a tool."""
    if method != "tools/call" or tool is None:
        return MCP_NO_TOOL
    return closed(tool, MCP_TOOLS)
