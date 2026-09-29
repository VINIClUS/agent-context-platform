from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any, Final, Protocol

from mcp.shared.inbound import (
    ERROR_CODE_HTTP_STATUS,
    MCP_PROTOCOL_VERSION_HEADER,
    unsupported_protocol_version_rejection,
)
from mcp_types import INVALID_REQUEST
from opentelemetry import context as otel_context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_TRACE_PROPAGATOR: Final = TraceContextTextMapPropagator()
_BODY_LIMIT_STATUS: Final = 413
_HTTP_BAD_REQUEST: Final = 400


class _BodyTooLargeError(Exception):
    """Raised internally when a request body exceeds the configured limit."""


class _ClientDisconnectedError(Exception):
    """Raised internally when the client goes away before the body is complete."""


_PORT: Final = re.compile(r"[0-9]{1,5}", re.ASCII)


def _matches(value: str, allowed: Iterable[str]) -> bool:
    """Exact match, or ``prefix:*`` for a 1-5 digit port after the prefix."""
    for pattern in allowed:
        if value == pattern:
            return True
        if (
            pattern.endswith(":*")
            and value.startswith(pattern[:-1])
            and _PORT.fullmatch(value[len(pattern) - 1 :])
        ):
            return True
    return False


async def _respond(
    send: Send,
    status: int,
    body: Mapping[str, Any] | None = None,
    headers: Iterable[tuple[bytes, bytes]] = (),
) -> None:
    payload = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
    response_headers = [(b"content-length", str(len(payload)).encode()), *headers]
    if body is not None:
        response_headers.append((b"content-type", b"application/json"))
    await send({"type": "http.response.start", "status": status, "headers": response_headers})
    await send({"type": "http.response.body", "body": payload})


def _decode(body: bytes) -> Any:
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        return None


def _request_method(decoded: Any) -> str:
    """The JSON-RPC ``method`` from the body, or a fixed token; never caller-chosen text."""
    method = decoded.get("method") if isinstance(decoded, dict) else None
    if isinstance(method, str) and _METHOD_PATTERN.fullmatch(method):
        return method
    return INVALID_METHOD


def _request_id(decoded: Any) -> str | int | None:
    """Best-effort JSON-RPC id so a rejection can be correlated by the client."""
    request_id = decoded.get("id") if isinstance(decoded, dict) else None
    if isinstance(request_id, bool) or not isinstance(request_id, str | int):
        return None
    return request_id


def _error_body(
    code: int, message: str, data: Any = None, request_id: str | int | None = None
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


METHOD_SCOPE_KEY: Final = "agent_context.mcp_method"
INVALID_METHOD: Final = "invalid"
_METHOD_PATTERN: Final = re.compile(r"[A-Za-z0-9_./-]{1,64}", re.ASCII)


class AdmittedRequest(Protocol):
    """A request the access gate let through; ``finish`` records its audit exactly once."""

    send: Send

    def finish(self, outcome: str | None) -> None:
        """Called once the request ends; ``outcome`` is ``None`` after a normal completion."""
        ...


class AccessGate(Protocol):
    """Authentication and authorisation step run after the Host and Origin checks."""

    async def admit(self, scope: Scope, send: Send) -> AdmittedRequest | None:
        """Return the admitted request, or ``None`` once the gate has answered a rejection."""
        ...


class ProtocolGuard:
    """Pure ASGI guard that runs before the MCP application.

    Enforces, in order: Host allowlist, Origin check, the access gate (401/403/429), POST-only, the
    ``2026-07-28`` protocol version header, and a streamed request-size limit.
    Rejections carry fixed content-free bodies and never log request data.
    The version check also keeps the SDK's handshake-era transport unreachable,
    because that transport serves requests whose version header is absent or old.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        allowed_hosts: Iterable[str],
        allowed_origins: Iterable[str],
        max_body_bytes: int,
        access_gate: AccessGate,
    ) -> None:
        self._app = app
        self._access_gate = access_gate
        self._allowed_hosts = tuple(allowed_hosts)
        self._allowed_origins = tuple(allowed_origins)
        self._max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        headers = self._single_valued_headers(scope)
        host = headers.get("host")
        if (
            self._is_duplicated(scope, b"host")
            or host is None
            or not _matches(host, self._allowed_hosts)
        ):
            await _respond(send, 421, _error_body(INVALID_REQUEST, "Invalid Host"))
            return
        # A missing Origin is a non-browser client; a present one must be listed.
        origin = headers.get("origin")
        if self._is_duplicated(scope, b"origin") or (
            origin is not None and not _matches(origin, self._allowed_origins)
        ):
            await _respond(send, 403, _error_body(INVALID_REQUEST, "Invalid Origin"))
            return
        # Authenticate before reading any body, so an unauthenticated client costs nothing.
        admitted = await self._access_gate.admit(scope, send)
        if admitted is None:
            return
        outcome: str | None = "error"
        try:
            outcome = await self._serve(scope, receive, admitted.send, headers)
        finally:
            # Exactly one audit per admitted request, even on disconnect or an exception.
            admitted.finish(outcome)

    async def _serve(
        self, scope: Scope, receive: Receive, send: Send, headers: Mapping[str, str]
    ) -> str | None:
        """Run the checks after admission; returns a fixed outcome label, ``None`` if normal."""
        if scope["method"] != "POST":
            await _respond(send, 405, None, [(b"allow", b"POST")])
            return None

        declared = headers.get("content-length")
        if declared is not None and (
            not declared.isascii() or not declared.isdigit() or int(declared) > self._max_body_bytes
        ):
            await _respond(send, _BODY_LIMIT_STATUS, _error_body(INVALID_REQUEST, "Too large"))
            return None

        try:
            body = await self._read_limited_body(receive)
        except _BodyTooLargeError:
            await _respond(send, _BODY_LIMIT_STATUS, _error_body(INVALID_REQUEST, "Too large"))
            return None
        except _ClientDisconnectedError:
            return "disconnect"

        # Parsed once here; the audit reads this validated value, never the caller's header.
        decoded = _decode(body)
        scope[METHOD_SCOPE_KEY] = _request_method(decoded)

        rejection = unsupported_protocol_version_rejection(
            headers.get(MCP_PROTOCOL_VERSION_HEADER, "")
        )
        if rejection is not None:
            status = ERROR_CODE_HTTP_STATUS.get(rejection.code, _HTTP_BAD_REQUEST)
            await _respond(
                send,
                status,
                _error_body(
                    rejection.code, rejection.message, rejection.data, _request_id(decoded)
                ),
            )
            return None

        # Only W3C trace context is imported; client-supplied baggage is dropped entirely
        # (empty allowlist) so attacker-controlled values never become ambient.
        token = otel_context.attach(_TRACE_PROPAGATOR.extract(headers))
        try:
            await self._app(scope, self._replay(body, receive), send)
        finally:
            otel_context.detach(token)
        return None

    @staticmethod
    def _is_duplicated(scope: Scope, name: bytes) -> bool:
        """Ambiguous repeated Host/Origin headers are rejected, never resolved."""
        return sum(1 for header_name, _ in scope["headers"] if header_name.lower() == name) > 1

    @staticmethod
    def _single_valued_headers(scope: Scope) -> dict[str, str]:
        return {
            name.decode("latin-1").lower(): value.decode("latin-1")
            for name, value in scope["headers"]
        }

    async def _read_limited_body(self, receive: Receive) -> bytes:
        # Count streamed bytes: Content-Length may be absent or understated.
        chunks: list[bytes] = []
        total = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                raise _ClientDisconnectedError
            chunk: bytes = message.get("body", b"")
            total += len(chunk)
            if total > self._max_body_bytes:
                raise _BodyTooLargeError
            chunks.append(chunk)
            if not message.get("more_body", False):
                return b"".join(chunks)

    @staticmethod
    def _replay(body: bytes, receive: Receive) -> Callable[[], Awaitable[Message]]:
        sent = False

        async def replay() -> Message:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        return replay
