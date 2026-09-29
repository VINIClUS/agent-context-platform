"""Contract tests for Codex OTel normalization (fixtures are source-derived, not captured)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from agent_context_sdk import EVENT_PAYLOAD_MODELS, ProducerV1

from agent_context_platform.ledger.codex_otel import (
    ALLOWED_SERVICE_NAMES,
    GAP_EVENT_NAMES,
    MAX_ATTRIBUTES,
    MAX_DECLARED_VALUE_CHARS,
    MAX_KEY_CHARS,
    CodexOtelError,
    CodexOtelMapperConfig,
    CodexOtelRecord,
    OtelSignal,
    RejectionReason,
    map_record,
)

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "codex_otel"
MAPPING_DOC = ROOT / "docs" / "data" / "codex-otel-mapping.md"
CONVERSATION_ID = "0198a4b1-98c0-7c28-ae3f-000000000001"
OBSERVED = datetime(2026, 8, 13, 13, 0, 1, tzinfo=UTC)
CANARIES = (
    "CANARY-PROMPT-TEXT-do-not-store",
    "CANARY-ARGUMENTS",
    "CANARY-OUTPUT",
    "canary-user@example.invalid",
    "acct-canary-7f3a",
    "req_canary_1",
)
CONFIG = CodexOtelMapperConfig(
    hmac_key=b"k" * 32,
    producer=ProducerV1(producer_id="codex-otel", name="codex-otel-mapper", version="1"),
    stream_id="codex-otel-stream",
)
LOG_CONFIG = CodexOtelMapperConfig(
    hmac_key=b"k" * 32,
    producer=CONFIG.producer,
    stream_id="codex-otel-stream",
    accepted_signal=OtelSignal.LOG,
)
FREE_TEXT_CANARY = "CANARY-FREE-TEXT-do-not-store"


def _load(name: str) -> CodexOtelRecord:
    raw: dict[str, Any] = json.loads((FIXTURES / name).read_text())
    return CodexOtelRecord(
        signal=OtelSignal(raw["signal"]), resource=raw["resource"], attributes=raw["attributes"]
    )


def _record(
    attributes: dict[str, object] | None = None, resource: dict[str, object] | None = None
) -> CodexOtelRecord:
    base = _load("conversation_starts.trace.json")
    return CodexOtelRecord(
        signal=OtelSignal.TRACE,
        resource=base.resource if resource is None else resource,
        attributes=base.attributes if attributes is None else attributes,
    )


def _attrs(**changes: object) -> dict[str, object]:
    attributes = dict(_load("conversation_starts.trace.json").attributes)
    attributes.update(changes)
    return {k: v for k, v in attributes.items() if v is not None}


def _reason(record: CodexOtelRecord, config: CodexOtelMapperConfig = CONFIG) -> RejectionReason:
    with pytest.raises(CodexOtelError) as info:
        map_record(config, record, observed_at=OBSERVED)
    assert str(info.value) == info.value.reason.value
    return info.value.reason


def test_conversation_starts_maps_to_a_registered_session_started_event() -> None:
    draft = map_record(CONFIG, _load("conversation_starts.trace.json"), observed_at=OBSERVED)
    assert draft is not None
    assert ("agent.session.started", draft.schema_version) in EVENT_PAYLOAD_MODELS
    assert draft.event_type == "agent.session.started"
    assert dict(draft.payload) == {
        "source": "codex_otel",
        "session_id": CONVERSATION_ID,
        "start_reason": None,
        "model": "gpt-5.5",
        "permission_mode": "on-request",
        "sandbox_mode": "workspace-write",
    }
    EVENT_PAYLOAD_MODELS[("agent.session.started", "1.0.0")].model_validate(dict(draft.payload))
    assert draft.context.session_id == CONVERSATION_ID
    assert draft.occurred_at == datetime(2026, 8, 13, 13, 0, 0, 124000, tzinfo=UTC)
    assert draft.observed_at == OBSERVED
    assert draft.redaction.disposition == "metadata_only"
    assert draft.content_claims == ()


def test_idempotency_key_is_deterministic_keyed_and_reveals_no_values() -> None:
    record = _load("conversation_starts.trace.json")
    first = map_record(CONFIG, record, observed_at=OBSERVED)
    again = map_record(CONFIG, record, observed_at=datetime(2030, 1, 1, tzinfo=UTC))
    assert first is not None and again is not None
    assert first.idempotency_key == again.idempotency_key
    assert first.event_id != again.event_id
    assert CONVERSATION_ID in first.idempotency_key
    assert "2026-08-13T13:00:00.124Z" in first.idempotency_key
    for value in ("gpt-5.5", "workspace-write", "on-request"):
        assert value not in first.idempotency_key

    other_key = CodexOtelMapperConfig(
        hmac_key=b"z" * 32, producer=CONFIG.producer, stream_id=CONFIG.stream_id
    )
    keyed = map_record(other_key, record, observed_at=OBSERVED)
    assert keyed is not None and keyed.idempotency_key != first.idempotency_key

    later = map_record(
        CONFIG,
        _record(_attrs(**{"event.timestamp": "2026-08-13T13:00:00.125Z"})),
        observed_at=OBSERVED,
    )
    changed = map_record(CONFIG, _record(_attrs(model="other")), observed_at=OBSERVED)
    assert later is not None and changed is not None
    assert later.idempotency_key != first.idempotency_key
    assert changed.idempotency_key != first.idempotency_key


def test_the_key_needs_at_least_32_bytes() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        CodexOtelMapperConfig(hmac_key=b"k" * 31, producer=CONFIG.producer, stream_id="s")


def test_the_default_config_accepts_only_the_trace_signal() -> None:
    assert CONFIG.accepted_signal is OtelSignal.TRACE
    assert _reason(_load("conversation_starts.log.json")) is RejectionReason.UNSUPPORTED_SIGNAL


def test_the_log_signal_is_accepted_only_when_configured_and_never_both() -> None:
    log = _load("conversation_starts.log.json")
    draft = map_record(LOG_CONFIG, log, observed_at=OBSERVED)
    assert draft is not None
    assert draft.occurred_at == datetime(2026, 8, 13, 13, 0, 0, 123000, tzinfo=UTC)
    assert "2026-08-13T13:00:00.123Z" in draft.idempotency_key
    assert _reason(_load("conversation_starts.trace.json"), LOG_CONFIG) is (
        RejectionReason.UNSUPPORTED_SIGNAL
    )
    dumped = draft.model_dump_json()
    for canary in ("canary-user@example.invalid", "acct-canary-7f3a"):
        assert canary not in dumped and canary not in draft.idempotency_key


def test_the_accepted_signal_must_be_a_known_signal() -> None:
    with pytest.raises(ValueError, match="accepted_signal"):
        CodexOtelMapperConfig(
            hmac_key=b"k" * 32,
            producer=CONFIG.producer,
            stream_id="s",
            accepted_signal="log",  # type: ignore[arg-type]
        )


def test_free_text_attributes_are_never_read_or_stored() -> None:
    attributes = _attrs(
        **{
            "error.message": FREE_TEXT_CANARY,
            "provider_name": FREE_TEXT_CANARY,
            "auth.env_provider_key_name": FREE_TEXT_CANARY,
            "slug": FREE_TEXT_CANARY,
            "terminal.type": FREE_TEXT_CANARY,
            "app.version": FREE_TEXT_CANARY,
            "reasoning_effort": FREE_TEXT_CANARY,
            "prompt": FREE_TEXT_CANARY,
        }
    )
    draft = map_record(CONFIG, _record(attributes), observed_at=OBSERVED)
    assert draft is not None
    assert FREE_TEXT_CANARY not in draft.model_dump_json()
    assert FREE_TEXT_CANARY not in draft.idempotency_key
    baseline = map_record(CONFIG, _record(), observed_at=OBSERVED)
    assert baseline is not None and baseline.idempotency_key == draft.idempotency_key

    # Even a malformed or oversized value in a never-read attribute cannot leak into an error.
    bad = _attrs(**{"error.message": FREE_TEXT_CANARY * 1000, "extra": FREE_TEXT_CANARY})
    assert map_record(CONFIG, _record(bad), observed_at=OBSERVED) is not None
    failing = _attrs(**{"error.message": FREE_TEXT_CANARY, "conversation.id": None})
    with pytest.raises(CodexOtelError) as info:
        map_record(CONFIG, _record(failing), observed_at=OBSERVED)
    assert FREE_TEXT_CANARY not in str(info.value) + repr(info.value.args)


@pytest.mark.parametrize(
    "name",
    [
        "api_request.log.json",
        "sse_event_completed.log.json",
        "user_prompt.log.json",
        "user_prompt.canary.log.json",
        "tool_decision.log.json",
        "tool_result.log.json",
        "turn_cost.log.json",
    ],
)
def test_events_without_an_sdk_contract_map_to_nothing(name: str) -> None:
    record = _load(name)
    assert record.attributes["event.name"] in GAP_EVENT_NAMES
    assert map_record(LOG_CONFIG, record, observed_at=OBSERVED) is None


def test_unknown_codex_events_map_to_nothing() -> None:
    record = _record(_attrs(**{"event.name": "codex.something_new"}))
    assert map_record(CONFIG, record, observed_at=OBSERVED) is None


def test_content_and_personal_attributes_never_reach_the_draft() -> None:
    attributes = _attrs(
        prompt=CANARIES[0],
        arguments=CANARIES[1],
        output=CANARIES[2],
        **{"user.email": CANARIES[3], "user.account_id": CANARIES[4]},
    )
    draft = map_record(CONFIG, _record(attributes), observed_at=OBSERVED)
    assert draft is not None
    dumped = draft.model_dump_json()
    for canary in CANARIES:
        assert canary not in dumped
        assert canary not in draft.idempotency_key


def test_writable_roots_style_values_are_dropped_not_copied() -> None:
    draft = map_record(
        CONFIG,
        _record(_attrs(sandbox_policy="workspace-write /home/u/secret", approval_policy="x")),
        observed_at=OBSERVED,
    )
    assert draft is not None
    assert draft.payload["sandbox_mode"] is None
    assert draft.payload["permission_mode"] is None


@pytest.mark.parametrize("service", ["codex_cli_rs", "codex_exec"])
def test_codex_service_names_are_accepted(service: str) -> None:
    assert service in ALLOWED_SERVICE_NAMES
    record = _record(resource={"service.name": service})
    assert map_record(CONFIG, record, observed_at=OBSERVED) is not None


@pytest.mark.parametrize(
    "service",
    [
        "agent-context-platform",
        "agent-context-ingest",
        "Agent-Context-Platform",
        "agent_context_mcp",
        "agent-context",
    ],
)
def test_platform_origin_telemetry_is_rejected_to_prevent_loops(service: str) -> None:
    record = _record(resource={"service.name": service})
    assert _reason(record) is RejectionReason.PLATFORM_ORIGIN
    gap = _load("api_request.log.json")
    gap_record = CodexOtelRecord(gap.signal, {"service.name": service}, gap.attributes)
    assert _reason(gap_record, LOG_CONFIG) is RejectionReason.PLATFORM_ORIGIN


@pytest.mark.parametrize("service", ["otelcol", "codex-cli", "codex_cli_rs2", "", "x" * 500])
def test_unknown_service_names_are_rejected(service: str) -> None:
    reason = _reason(_record(resource={"service.name": service}))
    assert reason in {RejectionReason.UNKNOWN_SERVICE, RejectionReason.MISSING_SERVICE}


@pytest.mark.parametrize("resource", [{}, {"service.name": 7}, {"service.name": None}])
def test_a_missing_service_name_is_rejected(resource: dict[str, object]) -> None:
    assert _reason(_record(resource=resource)) is RejectionReason.MISSING_SERVICE


def test_rejections_never_echo_the_offending_value() -> None:
    secret = "agent-context-SECRETSUFFIX"
    with pytest.raises(CodexOtelError) as info:
        map_record(CONFIG, _record(resource={"service.name": secret}), observed_at=OBSERVED)
    assert "SECRET" not in str(info.value) and "SECRET" not in repr(info.value.args)


def test_input_bounds_fail_closed() -> None:
    many = {f"k{i}": 1 for i in range(MAX_ATTRIBUTES + 1)}
    assert _reason(_record(many)) is RejectionReason.TOO_MANY_ATTRIBUTES
    long_key = _attrs(**{"x" * (MAX_KEY_CHARS + 1): 1})
    assert _reason(_record(long_key)) is RejectionReason.ATTRIBUTE_KEY_TOO_LONG
    long_value = _attrs(model="m" * (MAX_DECLARED_VALUE_CHARS + 1))
    assert _reason(_record(long_value)) is RejectionReason.ATTRIBUTE_VALUE_TOO_LONG
    huge_resource = {"service.name": "codex_cli_rs", **{f"r{i}": 1 for i in range(65)}}
    assert _reason(_record(resource=huge_resource)) is RejectionReason.TOO_MANY_ATTRIBUTES


@pytest.mark.parametrize("value", [["a"], {"a": 1}, b"bytes", ("t",)])
def test_non_scalar_attribute_values_are_rejected(value: object) -> None:
    assert _reason(_record(_attrs(extra=value))) is RejectionReason.NON_SCALAR_ATTRIBUTE
    assert _reason(_record(resource={"service.name": "codex_cli_rs", "r": value})) is (
        RejectionReason.NON_SCALAR_ATTRIBUTE
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"conversation.id": None}, RejectionReason.MISSING_ATTRIBUTE),
        ({"conversation.id": "  "}, RejectionReason.MISSING_ATTRIBUTE),
        ({"conversation.id": 5}, RejectionReason.INVALID_ATTRIBUTE),
        ({"conversation.id": "has space"}, RejectionReason.INVALID_ATTRIBUTE),
        ({"event.timestamp": None}, RejectionReason.MISSING_ATTRIBUTE),
        ({"event.timestamp": "yesterday"}, RejectionReason.INVALID_ATTRIBUTE),
        ({"event.timestamp": "2026-08-13T13:00:00"}, RejectionReason.INVALID_ATTRIBUTE),
        ({"event.name": None}, RejectionReason.MISSING_ATTRIBUTE),
        ({"model": 3}, RejectionReason.INVALID_ATTRIBUTE),
    ],
)
def test_malformed_declared_attributes_are_rejected(
    changes: dict[str, object], reason: RejectionReason
) -> None:
    assert _reason(_record(_attrs(**changes))) is reason


def test_blank_optional_fields_map_to_none() -> None:
    draft = map_record(CONFIG, _record(_attrs(model=" ")), observed_at=OBSERVED)
    assert draft is not None and draft.payload["model"] is None


def test_trace_context_is_carried_only_when_valid() -> None:
    base = _record()
    good = CodexOtelRecord(base.signal, base.resource, base.attributes, "a" * 32, "b" * 16)
    draft = map_record(CONFIG, good, observed_at=OBSERVED)
    assert draft is not None and draft.trace is not None
    assert (draft.trace.trace_id, draft.trace.span_id) == ("a" * 32, "b" * 16)
    for trace_id, span_id in [
        ("a" * 32, None),
        (None, "b" * 16),
        ("z" * 32, "b" * 16),
        ("0" * 32, "b" * 16),
    ]:
        bad = CodexOtelRecord(base.signal, base.resource, base.attributes, trace_id, span_id)
        assert _reason(bad) is RejectionReason.INVALID_TRACE_CONTEXT


def test_naive_observed_at_is_an_invalid_event() -> None:
    with pytest.raises(CodexOtelError) as info:
        map_record(CONFIG, _record(), observed_at=datetime(2026, 8, 13, 13, 0, 1))
    assert info.value.reason is RejectionReason.INVALID_EVENT


def test_mapping_doc_lists_every_gap_event_and_the_tags() -> None:
    doc = MAPPING_DOC.read_text()
    for name in (*GAP_EVENT_NAMES, "codex.conversation_starts", "SDK-019", "CODEX-050"):
        assert name in doc, name
    assert "rust-v0.147.0" in doc and "rust-v0.157.1" in doc
