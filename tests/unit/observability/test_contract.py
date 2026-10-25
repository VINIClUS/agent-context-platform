"""Telemetry contract v1: instrument names, kinds, units, closed attribute sets and the doc."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from agent_context_platform.mcp import server as mcp_server
from agent_context_platform.observability import contract, core
from agent_context_platform.observability.core import ProjectionSnapshot

from ._capture import Capture

pytestmark = pytest.mark.unit

DOC = Path(__file__).parents[3] / "docs" / "operations" / "telemetry.md"

# (instrument, kind, unit, Prometheus series name) exactly as the contract table states them.
CONTRACT = [
    (contract.INGESTION_BATCHES, "counter", "{batch}", "agent_context_ingestion_batches_total"),
    (contract.INGESTION_EVENTS, "counter", "{event}", "agent_context_ingestion_events_total"),
    (contract.INGESTION_DURATION, "histogram", "s", "agent_context_ingestion_duration_seconds_*"),
    (contract.CONTENT_OBJECTS, "counter", "{object}", "agent_context_content_objects_total"),
    (contract.PROJECTION_EVENTS, "counter", "{event}", "agent_context_projection_events_total"),
    (
        contract.PROJECTION_DURATION,
        "histogram",
        "s",
        "agent_context_projection_duration_seconds_*",
    ),
    (contract.PROJECTION_LAG, "gauge", "s", "agent_context_projection_lag_seconds"),
    (
        contract.PROJECTION_OUTBOX_PENDING,
        "gauge",
        "{event}",
        "agent_context_projection_outbox_pending",
    ),
    (contract.PROJECTION_DEAD_LETTERS, "gauge", "{event}", "agent_context_projection_dead_letters"),
    (
        contract.LEDGER_INTEGRITY_FAILURES,
        "counter",
        "{failure}",
        "agent_context_ledger_integrity_failures_total",
    ),
    (contract.INDEX_RUNS, "counter", "{run}", "agent_context_index_runs_total"),
    (contract.INDEX_DURATION, "histogram", "s", "agent_context_index_duration_seconds_*"),
    (contract.RETRIEVAL_DURATION, "histogram", "s", "agent_context_retrieval_duration_seconds_*"),
    (contract.MCP_REQUESTS, "counter", "{request}", "agent_context_mcp_requests_total"),
    (contract.MCP_DURATION, "histogram", "s", "agent_context_mcp_duration_seconds_*"),
    (contract.HTTP_SERVER_DURATION, "histogram", "s", "http_server_request_duration_seconds_*"),
    (contract.OTEL_REQUESTS, "counter", "{request}", "agent_context_otel_requests_total"),
    (contract.OTEL_SPANS, "counter", "{span}", "agent_context_otel_spans_total"),
]


def _record_one_of_each(capture: Capture) -> None:
    with core.observe_ingestion() as observation:
        observation.status = 200
    core.record_ingestion_events(created=1, existing=0)
    core.record_content_object("inline", "stored")
    core.record_projection_event("none", "delivered")
    core.record_projection_duration("none", 0.01)
    core.record_integrity_failure("hash_chain")
    core.record_index_run("success", 1.0)
    with core.retrieval_span("graph"):
        pass
    core.record_mcp_request("tools/list", "none", "ok", 0.01)
    core.record_http_request("GET", "/health/live", 200, 0.001)
    core.record_otel_request("accepted", "none")
    core.record_otel_spans("mapped", 1)
    core.record_otel_spans("unmapped", 0)  # nothing recorded for an empty tally
    capture.handle.set_projection_snapshot(
        ProjectionSnapshot(outbox_pending=3, lag_seconds={"none": 1.5}, dead_letters={"none": 1})
    )


def test_every_contract_instrument_has_its_kind_and_unit(telemetry: Capture) -> None:
    _record_one_of_each(telemetry)
    metrics = telemetry.metrics()

    assert set(metrics) == {name for name, *_ in CONTRACT}
    for name, kind, unit, _prometheus in CONTRACT:
        assert telemetry.kind(name) == kind, name
        assert metrics[name].unit == unit, name


def test_histograms_use_the_documented_buckets(telemetry: Capture) -> None:
    _record_one_of_each(telemetry)
    for name, kind, *_ in CONTRACT:
        if kind == "histogram":
            (point,) = telemetry.metrics()[name].data.data_points
            assert tuple(point.explicit_bounds) == contract.DURATION_BUCKETS


def test_the_operations_doc_states_every_instrument_and_series() -> None:
    text = DOC.read_text()
    for name, kind, unit, prometheus in CONTRACT:
        row = next((line for line in text.splitlines() if line.startswith(f"| `{name}` |")), "")
        assert f"| {kind} |" in row, name
        assert f"| `{unit}` |" in row, name
        assert f"`{prometheus}`" in row, name
    for bucket in contract.DURATION_BUCKETS:
        assert f"{bucket:g}" in text


# Attribute keys each instrument records, and the closed value set of each documented key.
ATTRIBUTE_KEYS = {
    contract.INGESTION_BATCHES: {"outcome", "reason"},
    contract.INGESTION_EVENTS: {"result"},
    contract.INGESTION_DURATION: {"outcome"},
    contract.CONTENT_OBJECTS: {"storage", "result"},
    contract.PROJECTION_EVENTS: {"projector", "result"},
    contract.PROJECTION_DURATION: {"projector"},
    contract.PROJECTION_LAG: {"projector"},
    contract.PROJECTION_OUTBOX_PENDING: set(),
    contract.PROJECTION_DEAD_LETTERS: {"projector"},
    contract.LEDGER_INTEGRITY_FAILURES: {"check"},
    contract.INDEX_RUNS: {"result"},
    contract.INDEX_DURATION: {"result"},
    contract.RETRIEVAL_DURATION: {"operation", "outcome"},
    contract.MCP_REQUESTS: {"method", "tool", "outcome"},
    contract.MCP_DURATION: {"method", "tool"},
    contract.HTTP_SERVER_DURATION: {
        "http.request.method",
        "http.route",
        "http.response.status_code",
    },
    contract.OTEL_REQUESTS: {"outcome", "reason"},
    contract.OTEL_SPANS: {"result"},
}
CLOSED_SETS = {
    (contract.INGESTION_BATCHES, "outcome"): contract.INGESTION_OUTCOMES,
    (contract.INGESTION_BATCHES, "reason"): contract.INGESTION_REASONS,
    (contract.INGESTION_EVENTS, "result"): contract.EVENT_RESULTS,
    (contract.CONTENT_OBJECTS, "storage"): contract.CONTENT_STORAGES,
    (contract.CONTENT_OBJECTS, "result"): contract.CONTENT_RESULTS,
    (contract.PROJECTION_EVENTS, "result"): contract.PROJECTION_RESULTS,
    (contract.LEDGER_INTEGRITY_FAILURES, "check"): contract.INTEGRITY_CHECKS,
    (contract.INDEX_RUNS, "result"): contract.INDEX_RESULTS,
    (contract.RETRIEVAL_DURATION, "operation"): contract.RETRIEVAL_OPERATIONS,
    (contract.RETRIEVAL_DURATION, "outcome"): contract.RETRIEVAL_OUTCOMES,
    (contract.MCP_REQUESTS, "method"): contract.MCP_METHODS | {contract.OTHER},
    (contract.MCP_REQUESTS, "tool"): contract.MCP_TOOLS | {contract.MCP_NO_TOOL},
    (contract.MCP_REQUESTS, "outcome"): contract.MCP_OUTCOMES,
    (contract.OTEL_REQUESTS, "outcome"): contract.OTEL_OUTCOMES,
    (contract.OTEL_REQUESTS, "reason"): contract.OTEL_REASONS,
    (contract.OTEL_SPANS, "result"): contract.OTEL_SPAN_RESULTS,
}


def _documented_attributes() -> dict[str, dict[str, frozenset[str] | None]]:
    """Instrument -> {attribute key: documented closed set, or None when none is listed}."""
    found: dict[str, dict[str, frozenset[str] | None]] = {}
    for line in DOC.read_text().splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if (
            len(cells) != 5
            or not cells[0].startswith("`")
            or cells[1]
            not in {
                "counter",
                "histogram",
                "gauge",
            }
        ):
            continue
        name = cells[0].strip("`")
        attributes: dict[str, frozenset[str] | None] = {}
        for part in re.split(r"[;,]", re.sub(r"\([^)]*\)", "", cells[3])):
            key_match = re.match(r"\s*`([^`]+)`", part)
            if key_match is None:
                continue
            values = part.split("=", 1)[1] if "=" in part else None
            attributes[key_match.group(1)] = (
                None
                if values is None
                else frozenset(value.strip() for value in values.split(" / "))
            )
        found[name] = attributes
    return found


def test_the_doc_states_each_instruments_attribute_keys_and_closed_sets() -> None:
    documented = _documented_attributes()
    assert set(documented) == set(ATTRIBUTE_KEYS)
    for name, keys in ATTRIBUTE_KEYS.items():
        assert set(documented[name]) == keys, name
    for (name, key), values in CLOSED_SETS.items():
        # A key restated by name only (`outcome` on a histogram) shares its counter's set.
        assert documented[name][key] == values, (name, key)


def test_every_instrument_records_exactly_the_documented_keys(telemetry: Capture) -> None:
    _record_one_of_each(telemetry)
    for name, keys in ATTRIBUTE_KEYS.items():
        for attributes in telemetry.points(name):
            assert {key for key, _value in attributes} == keys, name


def test_http_method_labels_are_the_standard_methods_or_other() -> None:
    for method in contract.HTTP_METHODS:
        assert core.http_method_label(method) == method
    assert core.http_method_label("BREW") == contract.HTTP_OTHER_METHOD
    assert core.http_method_label("get") == contract.HTTP_OTHER_METHOD


def test_mcp_methods_are_exactly_the_served_methods() -> None:
    assert contract.MCP_METHODS == mcp_server._ALWAYS_SERVED | mcp_server._TOOL_METHODS


@pytest.mark.parametrize(
    "hostile",
    ["/etc/passwd", "repo-name", "8f0e7c1a-0000-4000-8000-000000000000", "", None, 7],
)
def test_unknown_attribute_values_map_to_other(telemetry: Capture, hostile: object) -> None:
    core.record_content_object(hostile, hostile)  # type: ignore[arg-type]
    core.record_integrity_failure(hostile)  # type: ignore[arg-type]
    core.record_index_run(hostile, 0.1)  # type: ignore[arg-type]
    core.record_projection_event(str(hostile), hostile)  # type: ignore[arg-type]
    core.record_mcp_request(str(hostile), str(hostile), str(hostile), 0.1)
    with pytest.raises(RuntimeError), core.retrieval_span(str(hostile)):
        raise RuntimeError

    assert telemetry.value(contract.CONTENT_OBJECTS, storage="other", result="other") == 1
    assert telemetry.value(contract.LEDGER_INTEGRITY_FAILURES, check="other") == 1
    assert telemetry.value(contract.INDEX_RUNS, result="other") == 1
    assert telemetry.value(contract.PROJECTION_EVENTS, projector="other", result="other") == 1
    assert (
        telemetry.value(contract.MCP_REQUESTS, method="other", tool="other", outcome="other") == 1
    )
    assert (
        telemetry.value(contract.RETRIEVAL_DURATION, operation="other", outcome="server_error") == 1
    )
    assert telemetry.span("agent_context.retrieval.other")


def test_every_recorded_attribute_value_is_in_a_closed_set(telemetry: Capture) -> None:
    _record_one_of_each(telemetry)
    for name in telemetry.metrics():
        for attributes in telemetry.points(name):
            for key, value in attributes:
                if key == "http.response.status_code":
                    assert isinstance(value, int)
                    continue
                if key == "http.route":
                    assert value.startswith("/") or value == "other"
                    continue
                assert isinstance(value, str) and len(value) <= 32, (name, key, value)


def test_projector_labels_are_registered_names_or_fixed_tokens() -> None:
    registered = contract.registered_projector_names()
    assert {"agent", "code", "git"} <= registered
    assert contract.projector_label("agent") == "agent"
    assert contract.projector_label(None) == "unattributed"
    assert contract.projector_label("unattributed") == "unattributed"
    assert contract.projector_label("none") == "none"
    assert contract.projector_label("my-test-projector") == "other"


def test_mcp_tool_labels() -> None:
    assert contract.mcp_tool_label("tools/call", "symbol_context") == "symbol_context"
    assert contract.mcp_tool_label("tools/call", "rm -rf") == "other"
    assert contract.mcp_tool_label("tools/call", None) == "none"
    assert contract.mcp_tool_label("tools/list", "symbol_context") == "none"


def test_projection_gauges_report_only_a_snapshot(telemetry: Capture) -> None:
    assert contract.PROJECTION_LAG not in telemetry.metrics()

    core.set_projection_snapshot(
        ProjectionSnapshot(
            outbox_pending=4,
            lag_seconds={"agent": 2.5, "ghost": 9.0},
            dead_letters={"agent": 2, "unattributed": 1, "ghost": 1, "phantom": 3},
        )
    )
    assert telemetry.value(contract.PROJECTION_OUTBOX_PENDING) == 4
    assert telemetry.value(contract.PROJECTION_LAG, projector="agent") == 2.5
    assert telemetry.value(contract.PROJECTION_LAG, projector="other") == 9.0
    assert telemetry.value(contract.PROJECTION_DEAD_LETTERS, projector="agent") == 2
    assert telemetry.value(contract.PROJECTION_DEAD_LETTERS, projector="unattributed") == 1
    # Unknown names are summed under one bounded label.
    assert telemetry.value(contract.PROJECTION_DEAD_LETTERS, projector="other") == 4

    core.set_projection_snapshot(None)
    assert telemetry.points(contract.PROJECTION_OUTBOX_PENDING) == {}


@pytest.mark.parametrize(
    ("status", "codes", "expected"),
    [
        (200, (), ("accepted", "none")),
        (401, ("invalid_credential",), ("rejected", "auth")),
        (403, ("insufficient_scope",), ("rejected", "auth")),
        (403, ("producer_mismatch",), ("rejected", "producer_mismatch")),
        (400, ("idempotency_key_required",), ("rejected", "schema")),
        (413, ("request_too_large",), ("rejected", "too_large")),
        (422, ("content_requires_redaction",), ("rejected", "redaction_required")),
        (422, ("unsupported_media_type",), ("rejected", "schema")),
        (409, ("batch_rejected", "idempotency_conflict"), ("rejected", "conflict")),
        (409, ("stream_quarantined",), ("rejected", "conflict")),
        (422, ("brand_new_code",), ("rejected", "other")),
        (503, ("service_unavailable",), ("error", "internal")),
        (500, ("internal_error",), ("error", "internal")),
    ],
)
def test_ingestion_classification(
    status: int, codes: tuple[str, ...], expected: tuple[str, str]
) -> None:
    assert core.classify_ingestion(status, codes) == expected


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (200, "ok"),
        (202, "ok"),
        (400, "client_error"),
        (404, "client_error"),
        (401, "auth_error"),
        (403, "auth_error"),
        (429, "rate_limited"),
        (500, "server_error"),
        (503, "server_error"),
    ],
)
def test_mcp_outcome_from_status(status: int, outcome: str) -> None:
    assert core.mcp_outcome(status) == outcome
