"""Codex OTLP trace receiver: ``POST /v1/otel/traces`` (PLATFORM-053).

The VPS collector forwards Codex trace-safe telemetry here as an OTLP/HTTP
``ExportTraceServiceRequest`` (protobuf). Each Codex event record (a span, or a span event,
carrying ``event.name``) goes through the pure mapper ``ledger.codex_otel``; the mapped drafts
are appended through the ingestion service as ONE atomic batch per request, so idempotency comes
from the mapper's keys and a collector retry of the same export is answered from the ledger.

Order, mirroring ``POST /v1/ingestion/batches``: authenticate (the same producer credentials,
``events:ingest`` scope, same bounded Argon2 path), rate limit per producer, media type and
encoding, streamed size limit, bounded gzip inflation, protobuf parsing, span limit, mapping,
ingestion. Records the mapper does not model are counted as unmapped; records it refuses are
rejected spans, reported through ``partial_success`` with a fixed message naming only fixed
reason codes. No attribute value is ever logged, echoed or stored outside the mapped event.

Status mapping (OTLP/HTTP): 200 (``partial_success`` when spans were rejected), 400 malformed,
401/403 credentials, 413 too large or too many spans, 415 media type or encoding, 429 rate
limited, 503 not configured or a retryable outage (``Retry-After``). Error bodies are a
``google.rpc.Status`` with a fixed message. Not part of the frozen ``/v1/ingestion/*`` OpenAPI.
"""

from __future__ import annotations

import logging
import zlib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final

from agent_context_sdk import (  # type: ignore[import-untyped, unused-ignore]
    EventDraftV1,
    IngestBatchRequestV1,
    ProducerV1,
)
from agent_context_sdk.ids import new_uuid7  # type: ignore[import-untyped, unused-ignore]
from fastapi import APIRouter, Request
from fastapi.responses import Response
from google.protobuf.message import DecodeError
from google.rpc.code_pb2 import Code
from google.rpc.status_pb2 import Status
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTracePartialSuccess,
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from agent_context_platform.ledger import codex_otel
from agent_context_platform.ledger.auth import AuthError, ProducerAuthenticator
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.mcp.auth import RateLimitedError, TokenBucketLimiter
from agent_context_platform.observability import record_otel_request, record_otel_spans

logger = logging.getLogger("agent_context_platform.ledger.otlp_api")

TRACES_PATH: Final = "/v1/otel/traces"
PROTOBUF_MEDIA_TYPE: Final = "application/x-protobuf"
PRODUCER_NAME: Final = "codex-otel-receiver"
# Part of every mapped draft: a change makes re-exported records conflict, so it stays fixed.
PRODUCER_VERSION: Final = "1.0.0"
STREAM_PREFIX: Final = "codex-otel:"
_RETRY_AFTER_SECONDS: Final = 5
_READ_CHUNK_BYTES: Final = 65_536
_UNMAPPED: Final = "unmapped"

# Fixed, content-free status messages.
_MESSAGES: Final[Mapping[str, tuple[int, str]]] = {
    "receiver_unavailable": (Code.UNAVAILABLE, "The OTLP receiver is not available."),
    "service_unavailable": (Code.UNAVAILABLE, "The service is temporarily unavailable."),
    "auth_overloaded": (Code.UNAVAILABLE, "Authentication is busy; retry later."),
    "missing_credential": (Code.UNAUTHENTICATED, "A bearer credential is required."),
    "invalid_credential": (Code.UNAUTHENTICATED, "The credential is not valid."),
    "insufficient_scope": (Code.PERMISSION_DENIED, "The credential may not ingest events."),
    "rate_limited": (Code.RESOURCE_EXHAUSTED, "Too many requests; retry later."),
    "unsupported_media_type": (
        Code.INVALID_ARGUMENT,
        "Only application/x-protobuf, identity or gzip, is accepted.",
    ),
    "request_too_large": (Code.RESOURCE_EXHAUSTED, "The request exceeds a size limit."),
    "too_many_spans": (Code.RESOURCE_EXHAUSTED, "The request holds too many spans."),
    "malformed_request": (Code.INVALID_ARGUMENT, "The request is not a valid OTLP export."),
    "internal_error": (Code.INTERNAL, "The request could not be processed."),
}
# Error code -> telemetry (outcome, reason).
_TELEMETRY: Final[Mapping[str, tuple[str, str]]] = {
    "receiver_unavailable": ("error", "internal"),
    "service_unavailable": ("error", "internal"),
    "auth_overloaded": ("error", "internal"),
    "missing_credential": ("rejected", "auth"),
    "invalid_credential": ("rejected", "auth"),
    "insufficient_scope": ("rejected", "auth"),
    "rate_limited": ("rejected", "rate_limited"),
    "unsupported_media_type": ("rejected", "unsupported_media_type"),
    "request_too_large": ("rejected", "too_large"),
    "too_many_spans": ("rejected", "too_large"),
    "malformed_request": ("rejected", "malformed"),
    "internal_error": ("error", "internal"),
}


@dataclass(frozen=True, slots=True)
class OtlpReceiverRuntime:
    """Everything the route needs; built once per application and injectable in tests."""

    authenticator: ProducerAuthenticator
    service: IngestionService
    limiter: TokenBucketLimiter
    hmac_key: bytes = field(repr=False)
    max_request_body_bytes: int
    max_decompressed_bytes: int
    max_spans: int


class _RequestError(Exception):
    def __init__(self, status: int, code: str) -> None:
        super().__init__(code)
        self.status = status
        self.code = code


@dataclass(slots=True)
class _Mapping:
    """The mapping of one export: drafts with the span each came from, and span tallies."""

    drafts: list[tuple[int, EventDraftV1]] = field(default_factory=list)
    rejected_spans: set[int] = field(default_factory=set)
    reasons: set[str] = field(default_factory=set)
    unmapped_spans: int = 0
    span_count: int = 0


router = APIRouter(prefix="/v1/otel", tags=["otel"])


@router.post("/traces", include_in_schema=False)
async def export_traces(request: Request) -> Response:
    runtime: OtlpReceiverRuntime | None = getattr(request.app.state, "otlp_receiver", None)
    if runtime is None:
        return _failure(request, 503, "receiver_unavailable")
    try:
        return await _handle(request, runtime)
    except _RequestError as error:
        return _failure(request, error.status, error.code)
    except (OperationalError, InterfaceError, PoolTimeoutError):
        return _failure(request, 503, "service_unavailable")
    except Exception as error:
        # Never log the exception itself: driver errors can embed statement parameters.
        _log(request, "otlp_export_failed", error_class=type(error).__name__)
        return _failure(request, 500, "internal_error")


async def _handle(request: Request, runtime: OtlpReceiverRuntime) -> Response:
    try:
        principal = await runtime.authenticator.authenticate(request.headers.get("authorization"))
    except AuthError as error:
        raise _RequestError(error.status_code, error.code) from None
    try:
        runtime.limiter.acquire(principal.producer_id)
    except RateLimitedError:
        raise _RequestError(429, "rate_limited") from None

    _check_media_type(request)
    body = await _read_body(request, runtime)
    export = _parse(body)
    span_count = sum(
        len(scope_spans.spans)
        for resource_spans in export.resource_spans
        for scope_spans in resource_spans.scope_spans
    )
    if span_count > runtime.max_spans:
        raise _RequestError(413, "too_many_spans")

    config = codex_otel.CodexOtelMapperConfig(
        hmac_key=runtime.hmac_key,
        producer=ProducerV1(
            producer_id=principal.producer_id, name=PRODUCER_NAME, version=PRODUCER_VERSION
        ),
        stream_id=f"{STREAM_PREFIX}{principal.producer_id}",
    )
    mapping = map_export(config, export)
    if len(mapping.drafts) > runtime.max_spans:
        raise _RequestError(413, "too_many_spans")

    if mapping.drafts:
        outcome = await runtime.service.ingest(
            IngestBatchRequestV1(
                batch_id=new_uuid7(), events=tuple(draft for _, draft in mapping.drafts)
            )
        )
        if outcome.http_status >= 500:
            raise _RequestError(503, "service_unavailable")
        if outcome.http_status != 200:
            # The whole batch was refused (conflict, quarantined stream): its spans are rejected.
            mapping.rejected_spans.update(index for index, _ in mapping.drafts)
            mapping.reasons.update(result.error_code for result in outcome.response.rejected)

    rejected = len(mapping.rejected_spans)
    mapped = len({index for index, _ in mapping.drafts} - mapping.rejected_spans)
    record_otel_spans("rejected", rejected)
    record_otel_spans("mapped", mapped)
    record_otel_spans(_UNMAPPED, mapping.unmapped_spans)
    record_otel_request("accepted", "none")
    _log(
        request,
        "otlp_export_completed",
        producer_id=principal.producer_id,
        spans=mapping.span_count,
        mapped=mapped,
        unmapped=mapping.unmapped_spans,
        rejected=rejected,
        reasons=",".join(sorted(mapping.reasons)) or "-",
    )
    response = ExportTraceServiceResponse()
    if rejected:
        response.partial_success.CopyFrom(
            ExportTracePartialSuccess(
                rejected_spans=rejected,
                error_message="spans rejected: " + ",".join(sorted(mapping.reasons)),
            )
        )
    return Response(response.SerializeToString(), media_type=PROTOBUF_MEDIA_TYPE)


def _check_media_type(request: Request) -> None:
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    encoding = request.headers.get("content-encoding", "identity").strip().lower()
    if media_type != PROTOBUF_MEDIA_TYPE or encoding not in {"identity", "gzip"}:
        raise _RequestError(415, "unsupported_media_type")


async def _read_body(request: Request, runtime: OtlpReceiverRuntime) -> bytes:
    """The request body, size-limited while streaming and inflated within a fixed bound."""
    limit = runtime.max_request_body_bytes
    declared = request.headers.get("content-length")
    if declared is not None and declared.isascii() and declared.isdigit() and int(declared) > limit:
        raise _RequestError(413, "request_too_large")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise _RequestError(413, "request_too_large")
        chunks.append(chunk)
    body = b"".join(chunks)
    if request.headers.get("content-encoding", "identity").strip().lower() == "gzip":
        return _gunzip(body, runtime.max_decompressed_bytes)
    return body


def _gunzip(data: bytes, limit: int) -> bytes:
    """Inflate one gzip member, never producing more than ``limit`` bytes (bomb protection)."""
    inflater = zlib.decompressobj(wbits=31)
    output: list[bytes] = []
    produced = 0
    pending = data
    try:
        while not inflater.eof:
            piece = inflater.decompress(pending, _READ_CHUNK_BYTES)
            produced += len(piece)
            if produced > limit:
                raise _RequestError(413, "request_too_large")
            output.append(piece)
            pending = inflater.unconsumed_tail
            if not piece and not pending:
                break  # truncated: no input left and no progress
    except zlib.error:
        raise _RequestError(400, "malformed_request") from None
    # Exactly one complete member: truncated or trailing data is malformed.
    if not inflater.eof or inflater.unused_data:
        raise _RequestError(400, "malformed_request")
    return b"".join(output)


def _parse(body: bytes) -> ExportTraceServiceRequest:
    export = ExportTraceServiceRequest()
    try:
        export.ParseFromString(body)
    except (DecodeError, RecursionError, ValueError):
        raise _RequestError(400, "malformed_request") from None
    return export


def _scalar(value: AnyValue) -> object:
    """A scalar attribute value, or a marker the mapper rejects as non-scalar."""
    kind = value.WhichOneof("value")
    if kind == "string_value":
        return value.string_value
    if kind == "bool_value":
        return value.bool_value
    if kind == "int_value":
        return value.int_value
    if kind == "double_value":
        return value.double_value
    if kind is None:
        return None
    return (kind,)  # array, kvlist or bytes: never mapped


def _attributes(pairs: Iterable[KeyValue]) -> dict[str, object]:
    return {pair.key: _scalar(pair.value) for pair in pairs}


def _observed_at(time_unix_nano: int) -> datetime | None:
    """The record's own OTLP time: stable across retries, unlike the arrival time."""
    if time_unix_nano <= 0:
        return None
    try:
        return datetime.fromtimestamp(time_unix_nano / 1e9, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _records(
    span: Span, resource: Mapping[str, object]
) -> Iterator[tuple[codex_otel.CodexOtelRecord, int]]:
    """The Codex event records one span carries: itself and its events, when named events."""
    trace_id = span.trace_id.hex() or None
    span_id = span.span_id.hex() or None
    attributes = _attributes(span.attributes)
    if "event.name" in attributes:
        yield (
            codex_otel.CodexOtelRecord(
                codex_otel.OtelSignal.TRACE, resource, attributes, trace_id, span_id
            ),
            span.end_time_unix_nano or span.start_time_unix_nano,
        )
    for event in span.events:
        event_attributes = _attributes(event.attributes)
        if "event.name" in event_attributes:
            yield (
                codex_otel.CodexOtelRecord(
                    codex_otel.OtelSignal.TRACE, resource, event_attributes, trace_id, span_id
                ),
                event.time_unix_nano,
            )


def map_export(
    config: codex_otel.CodexOtelMapperConfig, export: ExportTraceServiceRequest
) -> _Mapping:
    """Map every Codex event record of ``export``; duplicates (same idempotency key) collapse.

    A span is rejected when its resource, or any of its records, is refused by the mapper; a
    span with no mapped and no refused record is unmapped.
    """
    mapping = _Mapping()
    seen: set[str] = set()
    index = -1
    for resource_spans in export.resource_spans:
        resource = _attributes(resource_spans.resource.attributes)
        try:
            codex_otel.check_resource(resource)
            resource_reason: str | None = None
        except codex_otel.CodexOtelError as error:
            resource_reason = error.reason.value
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                index += 1
                mapping.span_count += 1
                if resource_reason is not None:
                    mapping.rejected_spans.add(index)
                    mapping.reasons.add(resource_reason)
                    continue
                produced = False
                for record, time_unix_nano in _records(span, resource):
                    observed_at = _observed_at(time_unix_nano)
                    try:
                        if observed_at is None:
                            raise codex_otel.CodexOtelError(
                                codex_otel.RejectionReason.INVALID_EVENT
                            )
                        draft = codex_otel.map_record(config, record, observed_at=observed_at)
                    except codex_otel.CodexOtelError as error:
                        mapping.rejected_spans.add(index)
                        mapping.reasons.add(error.reason.value)
                        continue
                    if draft is None:
                        continue
                    produced = True
                    if draft.idempotency_key not in seen:
                        seen.add(draft.idempotency_key)
                        mapping.drafts.append((index, draft))
                if not produced and index not in mapping.rejected_spans:
                    mapping.unmapped_spans += 1
    return mapping


def _failure(request: Request, status: int, code: str) -> Response:
    outcome, reason = _TELEMETRY[code]
    record_otel_request(outcome, reason)
    _log(request, "otlp_export_rejected", status=status, error_code=code)
    rpc_code, message = _MESSAGES[code]
    body = Status(code=rpc_code, message=message).SerializeToString()
    headers: dict[str, str] = {}
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    if status in {429, 503}:
        headers["Retry-After"] = str(_RETRY_AFTER_SECONDS)
    return Response(body, status_code=status, media_type=PROTOBUF_MEDIA_TYPE, headers=headers)


def _log(request: Request, event: str, **fields: Any) -> None:
    """Log ids, counts and fixed codes only; ``fields`` must never hold attribute values."""
    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "%s request_id=%s",
        event,
        request_id or "-",
        extra={"request_id": request_id, **fields},
    )
