"""Normalize Codex OTel records into canonical SDK event drafts (pure mapping, no I/O).

Only telemetry that fits an *existing* SDK v0.3.2 contract is mapped: today that is
`agent.session.started` from `codex.conversation_starts`. Every other Codex event is
intentionally left unmapped (`map_record` returns `None`); the field-level gap table in
`docs/data/codex-otel-mapping.md` is the input to SDK-019.

The input is untrusted. Bounds and the service-name allowlist are enforced before any
attribute is read for mapping, failures raise `CodexOtelError` carrying only a fixed reason
code (never an attribute value), and content-bearing or personal attributes (`prompt`,
`arguments`, `output`, `user.email`, `user.account_id`, ...) are never read.

Only the OTLP *log* signal is accepted. Codex emits each event twice (log and trace-safe
variants) with independently computed `event.timestamp` values, so consuming both would
defeat idempotency. Source: `codex-rs/otel/src/events/shared.rs` (`log_event!`,
`trace_event!`) at `rust-v0.147.0` and `rust-v0.157.1`.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final

from agent_context_sdk import (
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    SessionStartedV1,
    TraceContextV1,
)
from agent_context_sdk.content import ContentDisposition
from pydantic import ValidationError

SOURCE_LABEL: Final = "codex_otel"
REDACTION_POLICY_VERSION: Final = "codex-otel-metadata-v1"
MIN_KEY_BYTES: Final = 32

# `service.name` is the Codex `originator` (or an explicit override):
# `codex-rs/core/src/otel_init.rs` and `codex-rs/login/src/auth/default_client.rs`
# (`DEFAULT_ORIGINATOR = "codex_cli_rs"`), `codex-rs/exec/src/lib.rs`
# (`set_default_originator("codex_exec")`), both at rust-v0.147.0 and rust-v0.157.1.
ALLOWED_SERVICE_NAMES: Final = frozenset({"codex_cli_rs", "codex_exec"})
_PLATFORM_SERVICE_PREFIXES: Final = ("agent-context-", "agent_context_", "agent-context.")
_PLATFORM_SERVICE_NAMES: Final = frozenset({"agent-context", "agent_context"})

MAX_ATTRIBUTES: Final = 128
MAX_RESOURCE_ATTRIBUTES: Final = 64
MAX_KEY_CHARS: Final = 128
MAX_DECLARED_VALUE_CHARS: Final = 256
MAX_OPAQUE_VALUE_CHARS: Final = 1_048_576

EVENT_CONVERSATION_STARTS: Final = "codex.conversation_starts"

# Codex events that have no SDK v0.3.2 contract (or lack a required field). They are
# recognised so callers can tell "known gap" from "unknown", and stay unmapped.
GAP_EVENT_NAMES: Final = frozenset(
    {
        "codex.api_request",
        "codex.sse_event",
        "codex.websocket_connect",
        "codex.websocket_request",
        "codex.auth_recovery",
        "codex.user_prompt",
        "codex.tool_decision",
        "codex.tool_result",
        "codex.sandbox_outcome",
        "codex.startup_phase",
        "codex.turn_ttft",
        "codex.turn_cost",
    }
)

# `AskForApproval` and `SandboxPolicy` derive strum `Display` in kebab-case, which prints the
# variant name only (never `writable_roots` or other payload data):
# `codex-rs/protocol/src/protocol.rs` at both tags. Values outside these sets are dropped.
_APPROVAL_POLICIES: Final = frozenset({"untrusted", "on-request", "granular", "never"})
_SANDBOX_POLICIES: Final = frozenset(
    {"danger-full-access", "read-only", "external-sandbox", "workspace-write"}
)
_NATIVE_ID: Final = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_HEX_TRACE_ID: Final = re.compile(r"^[0-9a-f]{32}$")
_HEX_SPAN_ID: Final = re.compile(r"^[0-9a-f]{16}$")

type Scalar = str | int | float | bool | None


class RejectionReason(StrEnum):
    """Content-free cause for refusing an OTel record."""

    UNSUPPORTED_SIGNAL = "unsupported_signal"
    PLATFORM_ORIGIN = "platform_origin"
    UNKNOWN_SERVICE = "unknown_service"
    MISSING_SERVICE = "missing_service"
    TOO_MANY_ATTRIBUTES = "too_many_attributes"
    ATTRIBUTE_KEY_TOO_LONG = "attribute_key_too_long"
    ATTRIBUTE_VALUE_TOO_LONG = "attribute_value_too_long"
    NON_SCALAR_ATTRIBUTE = "non_scalar_attribute"
    MISSING_ATTRIBUTE = "missing_attribute"
    INVALID_ATTRIBUTE = "invalid_attribute"
    INVALID_TRACE_CONTEXT = "invalid_trace_context"
    INVALID_EVENT = "invalid_event"


class CodexOtelError(Exception):
    """Typed, content-free rejection: the message is the fixed reason code, nothing else."""

    def __init__(self, reason: RejectionReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class OtelSignal(StrEnum):
    """OTLP signal a record arrived on; only `LOG` is mapped."""

    LOG = "log"
    TRACE = "trace"


@dataclass(frozen=True, slots=True)
class CodexOtelRecord:
    """One decoded OTLP record: resource attributes plus the event's own attributes."""

    signal: OtelSignal
    resource: Mapping[str, object]
    attributes: Mapping[str, object]
    trace_id: str | None = None
    span_id: str | None = None


@dataclass(frozen=True, slots=True)
class CodexOtelMapperConfig:
    """Injected mapper settings. `hmac_key` keys the idempotency digest (>= 32 bytes)."""

    hmac_key: bytes = field(repr=False)
    producer: ProducerV1
    stream_id: str

    def __post_init__(self) -> None:
        if len(self.hmac_key) < MIN_KEY_BYTES:
            raise ValueError("hmac_key must be at least 32 bytes")


def _is_platform_origin(service_name: str) -> bool:
    lowered = service_name.lower()
    return lowered in _PLATFORM_SERVICE_NAMES or lowered.startswith(_PLATFORM_SERVICE_PREFIXES)


def _check_service(resource: Mapping[str, object]) -> None:
    """Reject records not produced by an allowlisted Codex service (loop prevention)."""
    if len(resource) > MAX_RESOURCE_ATTRIBUTES:
        raise CodexOtelError(RejectionReason.TOO_MANY_ATTRIBUTES)
    _check_scalars(resource)
    service = resource.get("service.name")
    if not isinstance(service, str) or not service:
        raise CodexOtelError(RejectionReason.MISSING_SERVICE)
    if len(service) > MAX_KEY_CHARS:
        raise CodexOtelError(RejectionReason.UNKNOWN_SERVICE)
    if _is_platform_origin(service):
        raise CodexOtelError(RejectionReason.PLATFORM_ORIGIN)
    if service not in ALLOWED_SERVICE_NAMES:
        raise CodexOtelError(RejectionReason.UNKNOWN_SERVICE)


def _check_scalars(attributes: Mapping[str, object]) -> None:
    for key, value in attributes.items():
        if not isinstance(key, str):
            raise CodexOtelError(RejectionReason.INVALID_ATTRIBUTE)
        if len(key) > MAX_KEY_CHARS:
            raise CodexOtelError(RejectionReason.ATTRIBUTE_KEY_TOO_LONG)
        if value is not None and not isinstance(value, str | int | float | bool):
            raise CodexOtelError(RejectionReason.NON_SCALAR_ATTRIBUTE)
        if isinstance(value, str) and len(value) > MAX_OPAQUE_VALUE_CHARS:
            raise CodexOtelError(RejectionReason.ATTRIBUTE_VALUE_TOO_LONG)


def _declared(attributes: Mapping[str, object], key: str, *, required: bool) -> str | None:
    value = attributes.get(key)
    if value is None:
        if required:
            raise CodexOtelError(RejectionReason.MISSING_ATTRIBUTE)
        return None
    if not isinstance(value, str):
        raise CodexOtelError(RejectionReason.INVALID_ATTRIBUTE)
    if len(value) > MAX_DECLARED_VALUE_CHARS:
        raise CodexOtelError(RejectionReason.ATTRIBUTE_VALUE_TOO_LONG)
    if not value.strip():
        if required:
            raise CodexOtelError(RejectionReason.MISSING_ATTRIBUTE)
        return None
    return value


def _timestamp(raw: str) -> datetime:
    """Parse Codex's RFC 3339 `event.timestamp` (`shared.rs::timestamp`, millisecond `Z`)."""
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise CodexOtelError(RejectionReason.INVALID_ATTRIBUTE) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CodexOtelError(RejectionReason.INVALID_ATTRIBUTE)
    return parsed.astimezone(UTC)


def _trace(record: CodexOtelRecord) -> TraceContextV1 | None:
    if record.trace_id is None and record.span_id is None:
        return None
    if (
        record.trace_id is None
        or record.span_id is None
        or not _HEX_TRACE_ID.fullmatch(record.trace_id)
        or not _HEX_SPAN_ID.fullmatch(record.span_id)
    ):
        raise CodexOtelError(RejectionReason.INVALID_TRACE_CONTEXT)
    try:
        return TraceContextV1(trace_id=record.trace_id, span_id=record.span_id)
    except ValidationError:
        raise CodexOtelError(RejectionReason.INVALID_TRACE_CONTEXT) from None


def idempotency_key(
    key: bytes,
    event_name: str,
    native_ids: tuple[str, ...],
    timestamp: str,
    declared: Mapping[str, Scalar],
) -> str:
    """Deterministic key: event name, native ids, timestamp and an HMAC over declared attributes.

    The digest is keyed so attribute values are never recoverable from (or guessable via)
    the stored key; re-exporting the same record yields the same key.
    """
    canonical = json.dumps(dict(declared), sort_keys=True, separators=(",", ":"))
    digest = hmac.new(key, canonical.encode(), hashlib.sha256).hexdigest()
    return ":".join((SOURCE_LABEL, event_name, *native_ids, timestamp, digest))


def _session_started(
    config: CodexOtelMapperConfig, record: CodexOtelRecord, observed_at: datetime
) -> EventDraftV1:
    attributes = record.attributes
    conversation_id = _declared(attributes, "conversation.id", required=True)
    raw_timestamp = _declared(attributes, "event.timestamp", required=True)
    if conversation_id is None or raw_timestamp is None:  # narrowed for the type checker
        raise CodexOtelError(RejectionReason.MISSING_ATTRIBUTE)
    if not _NATIVE_ID.fullmatch(conversation_id):
        raise CodexOtelError(RejectionReason.INVALID_ATTRIBUTE)
    occurred_at = _timestamp(raw_timestamp)
    model = _declared(attributes, "model", required=False)
    approval = _declared(attributes, "approval_policy", required=False)
    sandbox = _declared(attributes, "sandbox_policy", required=False)
    permission_mode = approval if approval in _APPROVAL_POLICIES else None
    sandbox_mode = sandbox if sandbox in _SANDBOX_POLICIES else None
    payload = SessionStartedV1(
        source=SOURCE_LABEL,
        session_id=conversation_id,
        model=model,
        permission_mode=permission_mode,
        sandbox_mode=sandbox_mode,
    )
    declared: dict[str, Scalar] = {
        "conversation.id": conversation_id,
        "model": model,
        "approval_policy": permission_mode,
        "sandbox_policy": sandbox_mode,
    }
    return EventDraftV1(
        event_type="agent.session.started",
        stream_id=config.stream_id,
        occurred_at=occurred_at,
        observed_at=observed_at,
        producer=config.producer,
        context=EventContextV1(session_id=conversation_id),
        trace=_trace(record),
        payload=payload.model_dump(mode="json"),
        redaction=EventRedactionSummaryV1(
            policy_version=REDACTION_POLICY_VERSION, disposition=ContentDisposition.METADATA_ONLY
        ),
        idempotency_key=idempotency_key(
            config.hmac_key,
            EVENT_CONVERSATION_STARTS,
            (conversation_id,),
            raw_timestamp,
            declared,
        ),
    )


def map_record(
    config: CodexOtelMapperConfig, record: CodexOtelRecord, *, observed_at: datetime
) -> EventDraftV1 | None:
    """Map one Codex OTel record to a draft, or `None` when it has no SDK v0.3.2 contract.

    Raises `CodexOtelError` for platform-origin, unknown-service, oversized or malformed
    input. `None` means "intentionally unmapped" (documented gap or unknown Codex event).
    """
    if record.signal is not OtelSignal.LOG:
        raise CodexOtelError(RejectionReason.UNSUPPORTED_SIGNAL)
    _check_service(record.resource)
    if len(record.attributes) > MAX_ATTRIBUTES:
        raise CodexOtelError(RejectionReason.TOO_MANY_ATTRIBUTES)
    _check_scalars(record.attributes)
    event_name = _declared(record.attributes, "event.name", required=True)
    if event_name != EVENT_CONVERSATION_STARTS:
        return None
    try:
        return _session_started(config, record, observed_at)
    except ValidationError:
        raise CodexOtelError(RejectionReason.INVALID_EVENT) from None
