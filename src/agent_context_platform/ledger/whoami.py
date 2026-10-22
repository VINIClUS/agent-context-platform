"""A side-effect-free probe of a bearer token: ``GET /v1/auth/whoami``.

Clients (the Codex doctor) use it to tell a working credential from a broken one without sending
data. It accepts a producer token (``events:ingest``) or an MCP token (``memory:read``) and
answers ``204`` with ``X-Agent-Context-Token-Kind: producer|mcp`` and no body; anything else is a
fixed, content-free error body (the ingestion envelope: ``error.code``, ``error.message``,
``request_id``).

It is deliberately outside the frozen ingestion OpenAPI (``include_in_schema=False``; the
snapshot covers ``/v1/ingestion/*`` only) and it writes nothing: both authenticators only read
their registration row (neither keeps a last-used time or an audit row in the database), so a
probe changes no row. It is no cheaper an oracle than the real planes: a producer token goes
through the ingestion authenticator and its Argon2 admission gate, an MCP token through the MCP
authenticator, its own gate and principal cache, and the per-principal MCP rate limit.
"""

from __future__ import annotations

import logging
from typing import Final

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy.exc import SQLAlchemyError

from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.mcp.auth import (
    MCP_TOKEN_PREFIX,
    MEMORY_READ_SCOPE,
    McpAuthHolder,
    RateLimitedError,
)
from agent_context_platform.security.bearer import (
    AuthError,
    AuthOverloadedError,
    InsufficientScopeError,
    InvalidCredentialError,
    MissingCredentialError,
    parse_bearer,
)

logger = logging.getLogger("agent_context_platform.ledger.whoami")

WHOAMI_PATH: Final = "/v1/auth/whoami"
TOKEN_KIND_HEADER: Final = "X-Agent-Context-Token-Kind"
_RETRY_AFTER_SECONDS: Final = 5
_OVERLOAD_RETRY_AFTER_SECONDS: Final = 1

_MESSAGES: Final = {
    "missing_credential": "A bearer credential is required.",
    "invalid_credential": "The credential is not valid.",
    "insufficient_scope": "The credential has no usable scope.",
    "auth_overloaded": "Authentication is busy; retry later.",
    "auth_unavailable": "Authentication is not available.",
    "rate_limited": "Too many requests; retry later.",
    "internal_error": "The request could not be processed.",
}

router = APIRouter()


@router.get(WHOAMI_PATH, include_in_schema=False)
async def whoami(request: Request) -> Response:
    """204 and the token kind for a valid producer or MCP token; a fixed error otherwise."""
    try:
        kind = await _authenticate(request)
    except AuthOverloadedError as error:
        return _error(request, error.status_code, error.code, _OVERLOAD_RETRY_AFTER_SECONDS)
    except AuthError as error:
        return _error(request, error.status_code, error.code)
    except RateLimitedError as error:
        return _error(request, 429, "rate_limited", error.retry_after_seconds)
    except _Unavailable:
        return _error(request, 503, "auth_unavailable", _RETRY_AFTER_SECONDS)
    except SQLAlchemyError:
        # Never log the exception: driver errors can embed statement parameters.
        return _error(request, 503, "auth_unavailable", _RETRY_AFTER_SECONDS)
    except Exception as error:
        _log(request, 500, error_class=type(error).__name__)
        return _error(request, 500, "internal_error", log=False)
    _log(request, 204, kind=kind)
    return Response(status_code=204, headers={TOKEN_KIND_HEADER: kind, "Cache-Control": "no-store"})


class _Unavailable(Exception):
    """The plane the token belongs to is not configured in this process."""


async def _authenticate(request: Request) -> str:
    authorizations = request.headers.getlist("authorization")
    if not authorizations:
        raise MissingCredentialError
    # An ambiguous repeated header is rejected, never resolved.
    if len(authorizations) > 1:
        raise InvalidCredentialError
    authorization = authorizations[0]
    bearer = parse_bearer(authorization)
    if bearer is None:
        raise InvalidCredentialError
    if bearer.prefix.startswith(MCP_TOKEN_PREFIX):
        holder: McpAuthHolder | None = getattr(request.app.state, "mcp_auth", None)
        runtime = None if holder is None else holder.runtime
        if runtime is None:
            raise _Unavailable
        principal = await runtime.authenticator.authenticate(authorization)
        if MEMORY_READ_SCOPE not in principal.scopes:
            raise InsufficientScopeError
        runtime.limiter.acquire(principal.principal)
        return "mcp"
    ingestion: IngestionRuntime | None = getattr(request.app.state, "ingestion", None)
    if ingestion is None:
        raise _Unavailable
    await ingestion.authenticator.authenticate(authorization)
    return "producer"


def _error(
    request: Request, status: int, code: str, retry_after: int | None = None, *, log: bool = True
) -> JSONResponse:
    if log:
        _log(request, status, error_code=code)
    headers = {"Cache-Control": "no-store"}
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    request.state.telemetry_codes = (code,)  # telemetry (PLATFORM-050): a fixed code only
    body = {
        "error": {"code": code, "message": _MESSAGES[code]},
        "request_id": request.state.request_id,
    }
    return JSONResponse(body, status_code=status, headers=headers)


def _log(request: Request, status: int, **fields: str) -> None:
    """One line per probe: status, token kind or error code; never a token or a principal."""
    logger.info(
        "whoami status=%s request_id=%s",
        status,
        request.state.request_id,
        extra={"request_id": request.state.request_id, "status": status, **fields},
    )
