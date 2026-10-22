"""Codex OTLP trace receiver: ``POST /v1/otel/traces`` (PLATFORM-053).

The VPS collector forwards Codex trace-safe telemetry here as an OTLP/HTTP
``ExportTraceServiceRequest`` (protobuf). Each Codex event record (a span, or a span event,
carrying ``event.name``) goes through the pure mapper ``ledger.codex_otel``; the mapped drafts
are appended through the ingestion service as one atomic batch per request (retried once without
the events a conflict is attributable to), so idempotency comes from the mapper's keys and a
collector retry of the same export is answered from the ledger.

Order: authenticate (the ingestion producers' credentials, ``events:ingest`` scope, same bounded
Argon2 path), rate limit per producer, media type and encoding, streamed size limit; then, behind
a bounded admission gate (two decodes at once, four waiting, 503 beyond) and on a worker thread:
bounded gzip inflation, a structural scan of the wire bytes that stops at the first exceeded
request-level limit (``ledger.otlp_wire``: containers, spans, records, links, attributes, before
any message is parsed), protobuf parsing and mapping (per-span limits reject that span only).
The ingestion append that follows is async database work on the event loop, one statement round
per event. Records the mapper does not model are counted as unmapped; records it refuses are
rejected spans, reported through ``partial_success`` with a fixed message naming only fixed
reason codes. No attribute value is ever logged, echoed or stored outside the mapped event.

Status mapping (OTLP/HTTP): 200 (``partial_success`` when spans were rejected), 400 malformed,
401/403 credentials, 413 with a fixed code per exceeded limit, 415 media type or encoding, 429
rate limited, 503 not configured or a retryable outage (``Retry-After``). Error bodies are a
``google.rpc.Status`` with a fixed message. Not part of the frozen ``/v1/ingestion/*`` OpenAPI.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import logging
import zlib
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, TypeVar
from uuid import UUID

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

from agent_context_platform.ledger import codex_otel, otlp_wire
from agent_context_platform.ledger.auth import AuthError, ProducerAuthenticator
from agent_context_platform.ledger.service import BATCH_REJECTED, IngestionService, IngestOutcome
from agent_context_platform.mcp.auth import RateLimitedError, TokenBucketLimiter
from agent_context_platform.observability import record_otel_request, record_otel_spans

logger = logging.getLogger("agent_context_platform.ledger.otlp_api")
_T = TypeVar("_T")

TRACES_PATH: Final = "/v1/otel/traces"
PROTOBUF_MEDIA_TYPE: Final = "application/x-protobuf"
PRODUCER_NAME: Final = "codex-otel-receiver"
# Part of every mapped draft: a change makes re-exported records conflict, so it stays fixed.
PRODUCER_VERSION: Final = "1.0.0"
# One stream per producer and Codex session: ``codex-otel:`` plus 40 hex characters of a SHA-256
# over the length-prefixed producer id and session id. Fixed length (fits the ledger's 512) and
# collision-safe; the readable ids stay in the event (producer, payload and context).
STREAM_PREFIX: Final = "codex-otel:"


def stream_id(producer_id: str, session_id: str | None) -> str:
    """The stream of one producer and Codex session (``session_id`` ``None``: no session)."""
    digest = hashlib.sha256()
    for part in (producer_id, session_id):
        encoded = b"" if part is None else part.encode()
        # Length-prefixed (and None distinct from ""): no two pairs share an encoding.
        digest.update(b"-" if part is None else len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return STREAM_PREFIX + digest.hexdigest()[:40]


# One ingestion batch holds at most 500 events.
MAX_DRAFTS: Final = 500
# Any string attribute value longer than this rejects its record before mapping.
MAX_ATTRIBUTE_VALUE_CHARS: Final = 16_384
FUTURE_TIMESTAMP: Final = "future_timestamp"
TOO_MANY_EVENTS: Final = "too_many_events"
TOO_MANY_ATTRIBUTES: Final = "too_many_attributes"
MISSING_TIMESTAMP: Final = "missing_timestamp"
_RETRY_AFTER_SECONDS: Final = 5
_READ_CHUNK_BYTES: Final = 65_536
# Decoding and mapping run off the event loop, at most this many at once per application, with
# at most this many more waiting; beyond that a request is refused at once (503).
DECODE_WORKERS: Final = 2
DECODE_QUEUE_DEPTH: Final = 4
# A refused batch is resubmitted without its offenders at most this many times in all.
MAX_INGEST_ATTEMPTS: Final = 8

# Fixed, content-free status messages; each 413 names its limit so collector logs show it.
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
    "request_too_large": (Code.RESOURCE_EXHAUSTED, "request_too_large: body over the limit."),
    "inflated_too_large": (
        Code.RESOURCE_EXHAUSTED,
        "inflated_too_large: decompressed body over the limit.",
    ),
    "too_many_spans": (Code.RESOURCE_EXHAUSTED, "too_many_spans: span limit exceeded."),
    "too_many_records": (
        Code.RESOURCE_EXHAUSTED,
        "too_many_records: span plus span event limit exceeded.",
    ),
    "too_many_attributes": (
        Code.RESOURCE_EXHAUSTED,
        "too_many_attributes: attribute limit exceeded.",
    ),
    "too_many_resources": (
        Code.RESOURCE_EXHAUSTED,
        "too_many_resources: resource spans limit exceeded.",
    ),
    "too_many_scopes": (Code.RESOURCE_EXHAUSTED, "too_many_scopes: scope spans limit exceeded."),
    "too_many_links": (Code.RESOURCE_EXHAUSTED, "too_many_links: span link limit exceeded."),
    "decode_busy": (Code.UNAVAILABLE, "The receiver is busy; retry later."),
    "client_disconnected": (Code.CANCELLED, "The client went away."),
    "too_many_drafts": (
        Code.RESOURCE_EXHAUSTED,
        "too_many_drafts: more than 500 distinct events in one export.",
    ),
    "malformed_request": (Code.INVALID_ARGUMENT, "The request is not a valid OTLP export."),
    "repeated_singular_field": (
        Code.INVALID_ARGUMENT,
        "repeated_singular_field: a singular message field occurs more than once.",
    ),
    "too_many_fields": (Code.RESOURCE_EXHAUSTED, "too_many_fields: field limit exceeded."),
    "too_many_objects": (
        Code.RESOURCE_EXHAUSTED,
        "too_many_objects: repeated element limit exceeded.",
    ),
    "internal_error": (Code.INTERNAL, "The request could not be processed."),
}
_TOO_LARGE: Final = frozenset(
    {
        "request_too_large",
        "inflated_too_large",
        "too_many_spans",
        "too_many_records",
        "too_many_attributes",
        "too_many_drafts",
        "too_many_resources",
        "too_many_scopes",
        "too_many_links",
        "too_many_fields",
        "too_many_objects",
    }
)
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
    "malformed_request": ("rejected", "malformed"),
    "repeated_singular_field": ("rejected", "unsupported"),
    "internal_error": ("error", "internal"),
    "decode_busy": ("error", "internal"),
    "client_disconnected": ("rejected", "abandoned"),
    **dict.fromkeys(_TOO_LARGE, ("rejected", "too_large")),
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
    max_records: int = 1_000
    max_events_per_span: int = 64
    max_attributes: int = 128
    max_clock_skew: timedelta = timedelta(minutes=5)
    max_resources: int = 16
    max_scopes: int = 64
    max_links: int = 1_000
    max_total_attributes: int = 65_536
    max_fields: int = 300_000
    max_objects: int = 200_000
    decode_gate: DecodeGate = field(
        default_factory=lambda: DecodeGate(DECODE_WORKERS, DECODE_QUEUE_DEPTH),
        repr=False,
        compare=False,
    )

    def wire_limits(self) -> otlp_wire.WireLimits:
        return otlp_wire.WireLimits(
            max_resources=self.max_resources,
            max_scopes=self.max_scopes,
            max_spans=self.max_spans,
            max_records=self.max_records,
            max_links=self.max_links,
            max_total_attributes=self.max_total_attributes,
            max_container_attributes=self.max_attributes,
            max_fields=self.max_fields,
            max_objects=self.max_objects,
        )


class DecodeGate:
    """Admission for blocking decodes: bounded concurrency, queue and per-producer share.

    At most ``workers`` decodes run at once and ``queue_depth`` more wait; one producer holds at
    most ``per_producer`` of those places and runs at most ONE decode at a time (its other places
    wait for its own turn before they compete for a worker), so a producer within its own rate
    limit never holds more than one worker and cannot starve the others. Anything beyond answers
    503 at once. A place is given back when the worker thread finishes, not when the request is
    cancelled: a cancelled request's decode keeps its slot until the thread is done, so no more
    than ``workers`` threads ever decode.
    """

    def __init__(self, workers: int, queue_depth: int, per_producer: int = 2) -> None:
        self._slots = asyncio.Semaphore(workers)
        self._capacity = workers + queue_depth
        self._per_producer = per_producer
        self._admitted = 0
        self._by_producer: dict[str, int] = {}
        self._turns: dict[str, asyncio.Lock] = {}

    async def run(self, producer: str, request: Request, work: Callable[[], _T]) -> _T:
        """Run ``work`` on a worker thread once admitted; skip it if the client has left."""
        if (
            self._admitted >= self._capacity
            or self._by_producer.get(producer, 0) >= self._per_producer
        ):
            raise _RequestError(503, "decode_busy")
        turn = self._admit(producer)
        holds_slot = False
        handed_over = False
        try:
            await turn.acquire()  # released with the slot by `_release`
            try:
                await self._slots.acquire()
            except BaseException:
                turn.release()
                raise
            holds_slot = True
            if await request.is_disconnected():
                raise _RequestError(499, "client_disconnected")
            future = asyncio.get_running_loop().run_in_executor(None, work)
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                # The thread cannot be stopped: its place is released when it finishes.
                handed_over = True
                future.add_done_callback(lambda _: self._release(producer, holds_slot=True))
                raise
        finally:
            if not handed_over:
                self._release(producer, holds_slot=holds_slot)

    def _admit(self, producer: str) -> asyncio.Lock:
        self._admitted += 1
        self._by_producer[producer] = self._by_producer.get(producer, 0) + 1
        return self._turns.setdefault(producer, asyncio.Lock())

    def _release(self, producer: str, *, holds_slot: bool) -> None:
        if holds_slot:
            self._slots.release()
            self._turns[producer].release()
        self._admitted -= 1
        remaining = self._by_producer[producer] - 1
        if remaining:
            self._by_producer[producer] = remaining
        else:
            del self._by_producer[producer]
            del self._turns[producer]  # no other place of this producer can hold or await it

    @property
    def admitted(self) -> int:
        return self._admitted


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

    def reject(self, index: int, reason: str) -> None:
        self.rejected_spans.add(index)
        self.reasons.add(reason)


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

    gzipped = _check_media_type(request)
    body = await _read_body(request, runtime.max_request_body_bytes)
    config = codex_otel.CodexOtelMapperConfig(
        hmac_key=runtime.hmac_key,
        producer=ProducerV1(
            producer_id=principal.producer_id, name=PRODUCER_NAME, version=PRODUCER_VERSION
        ),
        stream_id=stream_id(principal.producer_id, None),
    )
    now = datetime.now(UTC)
    mapping = await runtime.decode_gate.run(
        principal.producer_id,
        request,
        functools.partial(decode_and_map, body, gzipped, runtime, config, now),
    )

    if mapping.drafts:
        await _ingest(runtime, mapping)

    rejected = len(mapping.rejected_spans)
    mapped = len({index for index, _ in mapping.drafts} - mapping.rejected_spans)
    record_otel_spans("rejected", rejected)
    record_otel_spans("mapped", mapped)
    record_otel_spans("unmapped", mapping.unmapped_spans)
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


async def _ingest(runtime: OtlpReceiverRuntime, mapping: _Mapping) -> None:
    """Append the drafts as one batch; a refused batch is resubmitted without its offenders.

    A conflict (or quarantined stream) is attributable to some events: those are rejected and
    the others resubmitted, until a batch is accepted, no offender is named or
    ``MAX_INGEST_ATTEMPTS`` batches were tried (the ledger names one conflict per batch). Events
    already stored are ``existing``, never rejected.
    """
    pending = list(mapping.drafts)
    for attempt in range(1, MAX_INGEST_ATTEMPTS + 1):
        outcome = await runtime.service.ingest(
            IngestBatchRequestV1(batch_id=new_uuid7(), events=tuple(draft for _, draft in pending))
        )
        if outcome.http_status >= 500:
            raise _RequestError(503, "service_unavailable")
        if outcome.http_status == 200:
            return
        span_of = {draft.event_id: index for index, draft in pending}
        offenders = _offenders(outcome)
        last = attempt == MAX_INGEST_ATTEMPTS or not offenders
        for result in outcome.response.rejected:
            if result.event_id in offenders or last:
                mapping.reject(span_of[result.event_id], result.error_code)
        retry = {result.event_id for result in outcome.response.rejected} - offenders
        pending = [(index, draft) for index, draft in pending if draft.event_id in retry]
        if last or not pending:
            return


def _offenders(outcome: IngestOutcome) -> set[UUID]:
    return {
        result.event_id
        for result in outcome.response.rejected
        if result.error_code != BATCH_REJECTED
    }


def _check_media_type(request: Request) -> bool:
    """Whether the body is gzip; anything but protobuf, identity or gzip is a 415."""
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    encoding = request.headers.get("content-encoding", "identity").strip().lower()
    if media_type != PROTOBUF_MEDIA_TYPE or encoding not in {"identity", "gzip"}:
        raise _RequestError(415, "unsupported_media_type")
    return encoding == "gzip"


async def _read_body(request: Request, limit: int) -> bytes:
    """The request body, size-limited while streaming (``Content-Length`` is only a hint)."""
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
    return b"".join(chunks)


def decode_and_map(
    body: bytes,
    gzipped: bool,
    runtime: OtlpReceiverRuntime,
    config: codex_otel.CodexOtelMapperConfig,
    now: datetime,
) -> _Mapping:
    """Inflate, parse, pre-scan and map one export (blocking: runs on a worker thread)."""
    data = _gunzip(body, runtime.max_decompressed_bytes) if gzipped else body
    try:
        # Before parsing: parsing allocates per message, the scan is bounded by the limits.
        otlp_wire.scan(data, runtime.wire_limits())
    except otlp_wire.WireLimitExceeded as error:
        raise _RequestError(413, error.code) from None
    except otlp_wire.WireRefused as error:
        raise _RequestError(400, error.code) from None
    export = _parse(data)
    mapping = map_export(
        config,
        export,
        now=now,
        max_clock_skew=runtime.max_clock_skew,
        max_events_per_span=runtime.max_events_per_span,
        max_attributes=runtime.max_attributes,
    )
    if len(mapping.drafts) > MAX_DRAFTS:
        raise _RequestError(413, "too_many_drafts")
    return mapping


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
                raise _RequestError(413, "inflated_too_large")
            output.append(piece)
            pending = inflater.unconsumed_tail
            if not piece and not pending:
                break  # truncated: no input left and no progress
    except zlib.error:
        raise _RequestError(400, "malformed_request") from None
    # Exactly one complete member: truncated, trailing or multi-member data is malformed.
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


class _ValueTooLong:
    """Marker for an over-long string value: the mapper refuses it as non-scalar."""


_TOO_LONG: Final = _ValueTooLong()


def _scalar(value: AnyValue) -> object:
    """A scalar attribute value, or a marker the mapper rejects."""
    kind = value.WhichOneof("value")
    if kind == "string_value":
        text = value.string_value
        return _TOO_LONG if len(text) > MAX_ATTRIBUTE_VALUE_CHARS else text
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


def _carries_marker(pairs: Iterable[KeyValue]) -> bool:
    return any(pair.key == codex_otel.PLATFORM_ORIGIN_ATTRIBUTE for pair in pairs)


def _otlp_time(time_unix_nano: int) -> datetime | None:
    if time_unix_nano <= 0:
        return None
    try:
        return datetime.fromtimestamp(time_unix_nano / 1e9, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _records(
    span: Span, resource: Mapping[str, object]
) -> Iterator[tuple[codex_otel.CodexOtelRecord | None, int]]:
    """The Codex event records of one span (itself and its events, when named events).

    A ``None`` record is one carrying the platform-origin marker: the span is rejected.
    """
    trace_id = span.trace_id.hex() or None
    span_id = span.span_id.hex() or None
    if _carries_marker(span.attributes):
        yield None, 0
        return
    if any(pair.key == "event.name" for pair in span.attributes):
        yield (
            codex_otel.CodexOtelRecord(
                codex_otel.OtelSignal.TRACE,
                resource,
                _attributes(span.attributes),
                trace_id,
                span_id,
            ),
            span.end_time_unix_nano or span.start_time_unix_nano,
        )
    for event in span.events:
        if _carries_marker(event.attributes):
            yield None, 0
            continue
        if any(pair.key == "event.name" for pair in event.attributes):
            yield (
                codex_otel.CodexOtelRecord(
                    codex_otel.OtelSignal.TRACE,
                    resource,
                    _attributes(event.attributes),
                    trace_id,
                    span_id,
                ),
                event.time_unix_nano,
            )


def _map_record(
    config: codex_otel.CodexOtelMapperConfig,
    record: codex_otel.CodexOtelRecord,
    time_unix_nano: int,
    *,
    latest: datetime,
) -> EventDraftV1 | None:
    """Map one record with bounded, deterministic times; raises ``CodexOtelError`` or a reason.

    ``observed_at`` is Codex's own OTLP time (stable across retries), never earlier than
    ``occurred_at``; neither may be later than ``latest`` (now plus the allowed clock skew).
    """
    if any(value is _TOO_LONG for value in record.attributes.values()):
        raise _RecordRejected(codex_otel.RejectionReason.ATTRIBUTE_VALUE_TOO_LONG.value)
    otlp_time = _otlp_time(time_unix_nano)
    if otlp_time is None:
        raise _RecordRejected(MISSING_TIMESTAMP)
    if otlp_time > latest:
        raise _RecordRejected(FUTURE_TIMESTAMP)
    draft = codex_otel.map_record(config, record, observed_at=otlp_time)
    if draft is None:
        return None
    if draft.occurred_at > latest:
        raise _RecordRejected(FUTURE_TIMESTAMP)
    # Per-session stream; observed_at no earlier than occurred_at. Deterministic, so a retry
    # yields the identical draft.
    session = draft.context.session_id if draft.context is not None else None
    stream = stream_id(config.producer.producer_id, session or None)
    return draft.model_copy(
        update={"stream_id": stream, "observed_at": max(draft.occurred_at, otlp_time)}
    )


class _RecordRejected(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def map_export(
    config: codex_otel.CodexOtelMapperConfig,
    export: ExportTraceServiceRequest,
    *,
    now: datetime,
    max_clock_skew: timedelta = timedelta(minutes=5),
    max_events_per_span: int = 64,
    max_attributes: int = 128,
) -> _Mapping:
    """Map every Codex event record of ``export``; duplicates (same idempotency key) collapse.

    A span is rejected when its resource or scope is refused, when it or one of its events
    carries the platform-origin marker, when it has more than ``max_events_per_span`` events or
    it or one of its events more than ``max_attributes`` attributes, or when the mapper refuses
    one of its records; a span with no mapped and no refused record is unmapped. The first
    record with a given idempotency key wins. A resource without spans is never converted.
    """
    mapping = _Mapping()
    seen: set[str] = set()
    latest = now + max_clock_skew
    index = -1
    for resource_spans in export.resource_spans:
        if not any(scope_spans.spans for scope_spans in resource_spans.scope_spans):
            continue
        resource = _attributes(resource_spans.resource.attributes)
        resource_reason = _resource_reason(resource)
        for scope_spans in resource_spans.scope_spans:
            scope_reason = resource_reason
            if scope_reason is None and _carries_marker(scope_spans.scope.attributes):
                scope_reason = codex_otel.RejectionReason.PLATFORM_ORIGIN.value
            for span in scope_spans.spans:
                index += 1
                mapping.span_count += 1
                span_reason = scope_reason or _span_reason(
                    span, max_events_per_span, max_attributes
                )
                if span_reason is not None:
                    mapping.reject(index, span_reason)
                    continue
                produced = False
                for record, time_unix_nano in _records(span, resource):
                    try:
                        if record is None:
                            raise _RecordRejected(codex_otel.RejectionReason.PLATFORM_ORIGIN.value)
                        draft = _map_record(config, record, time_unix_nano, latest=latest)
                    except codex_otel.CodexOtelError as error:
                        mapping.reject(index, error.reason.value)
                        continue
                    except _RecordRejected as error:
                        mapping.reject(index, error.reason)
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


def _resource_reason(resource: Mapping[str, object]) -> str | None:
    """Why every span of this resource is rejected, or ``None``."""
    if any(value is _TOO_LONG for value in resource.values()):
        return codex_otel.RejectionReason.ATTRIBUTE_VALUE_TOO_LONG.value
    try:
        codex_otel.check_resource(resource)
    except codex_otel.CodexOtelError as error:
        return error.reason.value
    return None


def _span_reason(span: Span, max_events: int, max_attributes: int) -> str | None:
    """Per-span limits (O(1) length checks): over them the span alone is rejected."""
    if len(span.events) > max_events:
        return TOO_MANY_EVENTS
    if len(span.attributes) > max_attributes or any(
        len(event.attributes) > max_attributes for event in span.events
    ):
        return TOO_MANY_ATTRIBUTES
    return None


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
