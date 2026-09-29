"""HTTP surface for batch ingestion plus request-ID and trace-context middleware.

``POST /v1/ingestion/batches`` takes an ``IngestBatchRequestV1`` and answers
with an ``IngestBatchResponseV1``. The body is deliberately not declared as a
FastAPI parameter: framework validation would run before authentication and its
default 422 body echoes the offending input. The handler instead runs, in
order: authenticate, ``Idempotency-Key`` presence, streamed size limit, SDK
schema validation, ``Idempotency-Key == batch_id``, producer binding, then the
ingestion service. Every failure body is fixed and content-free.

Status mapping (frozen by PLATFORM-023):

* 400 ``idempotency_key_required``: header absent (nothing to compare).
* 401 ``missing_credential`` / ``invalid_credential``; 403 ``insufficient_scope``
  / ``producer_mismatch``. Authentication always precedes body handling.
* 413 ``request_too_large``: bytes are counted while streaming;
  ``Content-Length`` is only an early hint, never trusted.
* 422 ``invalid_request_schema`` (SDK model or event payload rejected) and
  ``idempotency_key_mismatch`` (well-formed but not equal to ``batch_id``). A
  mismatch is a semantic error in an otherwise well-formed request, hence 422.
* 409 ``idempotency_conflict`` / ``stream_quarantined``; 422 content codes; the
  body is an ``IngestBatchResponseV1`` with the whole batch rejected.
* 503 ``ingestion_unavailable`` (not configured) or a retryable outage.
"""

from __future__ import annotations

import logging
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from agent_context_sdk import (  # type: ignore[import-untyped, unused-ignore]
    IngestBatchRequestV1,
    IngestBatchResponseV1,
    resolve_event_model,
)
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from agent_context_platform.ledger.auth import (
    AuthError,
    ProducerAuthenticator,
    ProducerMismatchError,
    ProducerPrincipal,
)
from agent_context_platform.ledger.service import IngestionService

logger = logging.getLogger("agent_context_platform.ledger.api")

INGESTION_PATH: Final = "/v1/ingestion/batches"
REQUEST_ID_HEADER: Final = "x-request-id"

_REQUEST_ID_PATTERN: Final = re.compile(r"[A-Za-z0-9._-]{1,64}", re.ASCII)
_TRACE_PROPAGATOR: Final = TraceContextTextMapPropagator()
_RETRY_AFTER_SECONDS: Final = 5

_MESSAGES: Final[Mapping[str, str]] = {
    "idempotency_key_required": "The Idempotency-Key header is required.",
    "idempotency_key_mismatch": "The Idempotency-Key header must equal batch_id.",
    "request_too_large": "The request body exceeds the size limit.",
    "invalid_request_schema": "The request body does not match the ingestion schema.",
    "missing_credential": "A bearer credential is required.",
    "invalid_credential": "The credential is not valid.",
    "insufficient_scope": "The credential may not ingest events.",
    "producer_mismatch": "Every event must name the credential's producer.",
    "ingestion_unavailable": "Ingestion is not available.",
    "auth_overloaded": "Authentication is busy; retry later.",
    "service_unavailable": "The service is temporarily unavailable.",
    "internal_error": "The request could not be processed.",
}


@dataclass(frozen=True, slots=True)
class IngestionRuntime:
    """Everything the route needs; built once per application and injectable in tests."""

    authenticator: ProducerAuthenticator
    service: IngestionService
    max_request_body_bytes: int


class RequestContextMiddleware:
    """Pure ASGI middleware: request ID and W3C trace context, never touching bodies.

    A client ``X-Request-ID`` is honoured only when it is a short token of safe
    characters; anything else is replaced. Only ``traceparent``/``tracestate``
    are imported (baggage is dropped). ``receive`` passes through untouched, so
    no request body is buffered or read here.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        supplied = next(
            (
                value.decode("latin-1")
                for name, value in scope["headers"]
                if name.lower() == REQUEST_ID_HEADER.encode()
            ),
            None,
        )
        request_id = (
            supplied
            if supplied is not None and _REQUEST_ID_PATTERN.fullmatch(supplied)
            else uuid.uuid4().hex
        )
        headers = {
            name.decode("latin-1").lower(): value.decode("latin-1")
            for name, value in scope["headers"]
        }
        token = otel_context.attach(_TRACE_PROPAGATOR.extract(headers))
        span_context = trace.get_current_span().get_span_context()
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        state["trace_id"] = format(span_context.trace_id, "032x") if span_context.is_valid else None

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                response_headers = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() != REQUEST_ID_HEADER.encode()
                ]
                response_headers.append((REQUEST_ID_HEADER.encode(), request_id.encode("ascii")))
                message = {**message, "headers": response_headers}
            await send(message)

        try:
            await self._app(scope, receive, send_with_request_id)
        finally:
            otel_context.detach(token)


class _BodyTooLargeError(Exception):
    pass


async def _read_limited_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared is not None and declared.isascii() and declared.isdigit() and int(declared) > limit:
        raise _BodyTooLargeError
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise _BodyTooLargeError
        chunks.append(chunk)
    return b"".join(chunks)


def _validate_event_payloads(batch: IngestBatchRequestV1) -> bool:
    """Check each payload against its registered SDK model; ``False`` when any is invalid."""
    for event in batch.events:
        try:
            resolve_event_model(event.event_type, event.schema_version).model_validate(
                event.model_dump(mode="json")["payload"]
            )
        except (KeyError, ValidationError, ValueError):
            return False
    return True


def _error(
    request: Request, status: int, code: str, headers: Mapping[str, str] | None = None
) -> JSONResponse:
    body = {
        "error": {"code": code, "message": _MESSAGES[code]},
        "request_id": request.state.request_id,
    }
    return JSONResponse(body, status_code=status, headers=dict(headers or {}))


def _log(request: Request, event: str, **fields: Any) -> None:
    """Log IDs, counts and error codes only; ``fields`` must never hold request content."""
    request_id = request.state.request_id
    trace_id = request.state.trace_id
    logger.info(
        "%s request_id=%s trace_id=%s",
        event,
        request_id,
        trace_id or "-",
        extra={"request_id": request_id, "trace_id": trace_id, **fields},
    )


class ErrorDetailV1(BaseModel):
    """Fixed, content-free error description; ``code`` is stable, ``message`` is not."""

    code: str
    message: str


class ErrorResponseV1(BaseModel):
    """Body of every failure the route answers before or instead of a batch outcome."""

    error: ErrorDetailV1
    request_id: str


_REQUEST_ID_HEADER_DOC: Final[dict[str, Any]] = {
    "description": "Request ID: echoed when a short safe token, otherwise generated.",
    "schema": {"type": "string"},
}
_RETRY_AFTER_HEADER_DOC: Final[dict[str, Any]] = {
    "description": "Seconds to wait before retrying.",
    "schema": {"type": "integer"},
}
_BATCH_OR_ERROR: Final = IngestBatchResponseV1 | ErrorResponseV1

# OpenAPI metadata only: mirrors the status mapping in the module docstring. The
# ingestion snapshot (``openapi/agent-context-v1.json``) freezes this table.
_RESPONSES: Final[dict[int | str, dict[str, Any]]] = {
    400: {
        "model": ErrorResponseV1,
        "description": "Idempotency-Key header absent: `idempotency_key_required`.",
        "headers": {"x-request-id": _REQUEST_ID_HEADER_DOC},
    },
    401: {
        "model": ErrorResponseV1,
        "description": "Missing or invalid producer credential: "
        "`missing_credential`, `invalid_credential`.",
        "headers": {
            "x-request-id": _REQUEST_ID_HEADER_DOC,
            "WWW-Authenticate": {"description": "Always `Bearer`.", "schema": {"type": "string"}},
        },
    },
    403: {
        "model": ErrorResponseV1,
        "description": "Credential lacks the scope or names another producer: "
        "`insufficient_scope`, `producer_mismatch`.",
        "headers": {"x-request-id": _REQUEST_ID_HEADER_DOC},
    },
    409: {
        "model": IngestBatchResponseV1,
        "description": "Whole batch rejected, nothing stored: `idempotency_conflict`, "
        "`stream_quarantined`; other events are `batch_rejected`.",
        "headers": {"x-request-id": _REQUEST_ID_HEADER_DOC},
    },
    413: {
        "model": ErrorResponseV1,
        "description": "Request body exceeds the size limit: `request_too_large`.",
        "headers": {"x-request-id": _REQUEST_ID_HEADER_DOC},
    },
    422: {
        "model": _BATCH_OR_ERROR,
        "description": "Error envelope: `invalid_request_schema`, `idempotency_key_mismatch`. "
        "Batch response, nothing stored: `duplicate_idempotency_key`, `duplicate_content_id`, "
        "`unsupported_media_type`, `invalid_content_encoding`, `content_requires_redaction`, "
        "`content_resolution_mismatch`.",
        "headers": {"x-request-id": _REQUEST_ID_HEADER_DOC},
    },
    500: {
        "model": ErrorResponseV1,
        "description": "Unexpected failure: `internal_error`.",
        "headers": {"x-request-id": _REQUEST_ID_HEADER_DOC},
    },
    503: {
        "model": _BATCH_OR_ERROR,
        "description": "Error envelope: `ingestion_unavailable`, `auth_overloaded`, "
        "`service_unavailable`. Batch response (retryable outage): rejected events carry "
        "`service_unavailable` with `retryable` true.",
        "headers": {
            "x-request-id": _REQUEST_ID_HEADER_DOC,
            "Retry-After": _RETRY_AFTER_HEADER_DOC,
        },
    },
}


router = APIRouter(prefix="/v1/ingestion", tags=["ingestion"])


@router.post(
    "/batches",
    response_model=IngestBatchResponseV1,
    responses=_RESPONSES,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "title": "IngestBatchRequestV1",
                        "description": "agent-context-sdk IngestBatchRequestV1.",
                    }
                }
            },
        },
        "parameters": [
            {
                "name": "Idempotency-Key",
                "in": "header",
                "required": True,
                "schema": {"type": "string", "format": "uuid"},
                "description": "Must equal the batch_id in the body.",
            }
        ],
    },
)
async def ingest_batch(request: Request) -> Response:
    runtime: IngestionRuntime | None = getattr(request.app.state, "ingestion", None)
    if runtime is None:
        return _error(
            request, 503, "ingestion_unavailable", {"Retry-After": str(_RETRY_AFTER_SECONDS)}
        )
    try:
        return await _handle(request, runtime)
    except (OperationalError, InterfaceError, PoolTimeoutError):
        _log(request, "ingestion_unavailable")
        return _error(
            request, 503, "service_unavailable", {"Retry-After": str(_RETRY_AFTER_SECONDS)}
        )
    except Exception as error:
        # Never log the exception itself: driver errors can embed statement parameters.
        _log(request, "ingestion_failed", error_class=type(error).__name__)
        return _error(request, 500, "internal_error")


async def _handle(request: Request, runtime: IngestionRuntime) -> Response:
    try:
        principal = await runtime.authenticator.authenticate(request.headers.get("authorization"))
    except AuthError as error:
        _log(request, "ingestion_rejected", status=error.status_code, error_code=error.code)
        headers = {"WWW-Authenticate": "Bearer"} if error.status_code == 401 else None
        if error.status_code == 503:
            headers = {"Retry-After": str(_RETRY_AFTER_SECONDS)}
        return _error(request, error.status_code, error.code, headers)

    idempotency_key = request.headers.get("idempotency-key")
    if idempotency_key is None:
        return _reject(request, principal, 400, "idempotency_key_required")

    try:
        body = await _read_limited_body(request, runtime.max_request_body_bytes)
    except _BodyTooLargeError:
        return _reject(request, principal, 413, "request_too_large")

    try:
        batch = IngestBatchRequestV1.model_validate_json(body)
    except (ValidationError, ValueError):
        return _reject(request, principal, 422, "invalid_request_schema")
    if not _validate_event_payloads(batch):
        return _reject(request, principal, 422, "invalid_request_schema")

    if not _is_same_uuid(idempotency_key, batch.batch_id):
        return _reject(request, principal, 422, "idempotency_key_mismatch")

    if any(event.producer.producer_id != principal.producer_id for event in batch.events):
        code = ProducerMismatchError.code
        return _reject(request, principal, ProducerMismatchError.status_code, code)

    outcome = await runtime.service.ingest(batch)
    response = outcome.response
    _log(
        request,
        "ingestion_completed",
        producer_id=principal.producer_id,
        batch_id=batch.batch_id,
        events=len(batch.events),
        accepted=sum(1 for result in response.accepted if result.status == "accepted"),
        existing=sum(1 for result in response.accepted if result.status == "existing"),
        rejected=len(response.rejected),
        error_codes=",".join(sorted({result.error_code for result in response.rejected})) or "-",
        status=outcome.http_status,
    )
    headers = (
        None
        if outcome.retry_after_seconds is None
        else {"Retry-After": str(outcome.retry_after_seconds)}
    )
    return JSONResponse(
        response.model_dump(mode="json"), status_code=outcome.http_status, headers=headers
    )


def _reject(request: Request, principal: ProducerPrincipal, status: int, code: str) -> Response:
    _log(
        request,
        "ingestion_rejected",
        producer_id=principal.producer_id,
        status=status,
        error_code=code,
    )
    return _error(request, status, code)


def _is_same_uuid(header_value: str, batch_id: uuid.UUID) -> bool:
    try:
        return uuid.UUID(header_value) == batch_id
    except ValueError:
        return False
