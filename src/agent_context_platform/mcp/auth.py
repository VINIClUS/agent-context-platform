"""Read-only MCP authentication: a pre-provisioned bearer, not OAuth MCP.

A client presents ``Authorization: Bearer mcp_<id>.<secret>``. The non-secret prefix selects
one row of ``operations.mcp_tokens``; the Argon2id verifier proves possession in a bounded
worker (:mod:`agent_context_platform.security.bearer`, shared with ingestion but with its own
verifier instance and admission gate). Only a successful verification is cached, and only its
principal metadata: the cache key is ``HMAC-SHA256(server key, token)``, so neither the raw token
nor a bare digest of it is retained, and a revocation is honoured within the cache TTL.

:func:`require_scope` builds the access gate that :class:`ProtocolGuard` runs right after the
Host/Origin check. In order it authenticates (401), checks the scope (403), spends a per-principal
token-bucket permit (429, per replica) and emits one structured audit line carrying only the
principal, token id, method, status and latency, never a token, body or argument.

Operators mint tokens with ``python -m agent_context_platform.mcp.auth mint``; the secret is
printed once and only its verifier is stored.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import secrets
import sys
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from math import ceil
from typing import Any, Final
from uuid import UUID, uuid4

from argon2 import PasswordHasher
from sqlalchemy import create_engine, insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.types import Message, Scope, Send

from agent_context_platform.mcp.protocol_guard import METHOD_SCOPE_KEY, AdmittedRequest
from agent_context_platform.operations.models import McpTokenRow
from agent_context_platform.security.bearer import (
    Argon2Verifier,
    AuthError,
    AuthOverloadedError,
    BearerToken,
    InsufficientScopeError,
    InvalidCredentialError,
    MissingCredentialError,
    parse_bearer,
)

logger = logging.getLogger("agent_context_platform.mcp.auth")

MEMORY_READ_SCOPE: Final = "memory:read"
# Ingestion tokens have no such prefix, so they are refused before any database work.
MCP_TOKEN_PREFIX: Final = "mcp_"
PRINCIPAL_SCOPE_KEY: Final = "agent_context.mcp_principal"
DSN_ENVIRONMENT_VARIABLE: Final = "AGENT_CONTEXT_POSTGRESQL__DSN"

UNREAD_METHOD: Final = "unread"
_OVERLOAD_RETRY_AFTER_SECONDS: Final = 1
_UNAVAILABLE_RETRY_AFTER_SECONDS: Final = 5
_DEFAULT_HASHER: Final = PasswordHasher()


class RateLimitedError(Exception):
    """The principal's bucket is empty; ``retry_after_seconds`` is a whole number >= 1."""

    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__("rate_limited")
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True, slots=True)
class McpTokenRecord:
    """The ``operations.mcp_tokens`` columns authentication needs; carries the verifier."""

    token_id: UUID
    token_verifier: str = field(repr=False)
    principal: str
    scopes: tuple[str, ...]
    expires_at: datetime
    revoked_at: datetime | None


@dataclass(frozen=True, slots=True)
class McpPrincipal:
    """Verified caller metadata; the only thing the cache keeps."""

    principal: str
    token_id: UUID
    scopes: frozenset[str]
    expires_at: datetime


McpTokenLookup = Callable[[str], Awaitable[McpTokenRecord | None]]


class SqlMcpTokenLookup:
    """Select one token record by its unique non-secret prefix (read-only session)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __call__(self, token_prefix: str) -> McpTokenRecord | None:
        # Closed before the slow Argon2 verify runs.
        async with self._session_factory() as session:
            row = await session.scalar(
                select(McpTokenRow).where(McpTokenRow.token_prefix == token_prefix)
            )
        if row is None:
            return None
        return McpTokenRecord(
            token_id=row.token_id,
            token_verifier=row.token_verifier,
            principal=row.principal,
            scopes=tuple(row.scopes),
            expires_at=row.expires_at,
            revoked_at=row.revoked_at,
        )


class PrincipalCache:
    """Short-lived, bounded cache of verified principals keyed by ``HMAC(server key, token)``.

    The TTL runs on a monotonic deadline, so a wall-clock step backwards cannot stretch the
    revocation bound. The token's absolute ``expires_at`` is still compared with wall time.
    """

    def __init__(
        self,
        key: bytes,
        *,
        ttl: timedelta,
        max_entries: int,
        clock: Callable[[], datetime],
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._key = key
        self._ttl_seconds = ttl.total_seconds()
        self._max_entries = max_entries
        self._clock = clock
        self._monotonic = monotonic
        self._entries: dict[bytes, tuple[McpPrincipal, float]] = {}

    def key_for(self, token: str) -> bytes:
        return hmac.new(self._key, token.encode("ascii"), hashlib.sha256).digest()

    def get(self, cache_key: bytes) -> McpPrincipal | None:
        entry = self._entries.get(cache_key)
        if entry is None:
            return None
        principal, deadline = entry
        if self._monotonic() >= deadline or self._clock() >= principal.expires_at:
            del self._entries[cache_key]
            return None
        return principal

    def put(self, cache_key: bytes, principal: McpPrincipal) -> None:
        """Cache for ``ttl`` (or until the token expires); only successes are ever passed here."""
        if self._clock() >= principal.expires_at:
            return
        now = self._monotonic()
        if len(self._entries) >= self._max_entries and cache_key not in self._entries:
            # Drop expired entries first, then the oldest insertion.
            wall = self._clock()
            for stale in [
                k
                for k, (cached, deadline) in self._entries.items()
                if deadline <= now or cached.expires_at <= wall
            ]:
                del self._entries[stale]
            if len(self._entries) >= self._max_entries:
                del self._entries[next(iter(self._entries))]
        self._entries[cache_key] = (principal, now + self._ttl_seconds)


class TokenBucketLimiter:
    """Per-principal in-memory token bucket; state is per replica, not shared.

    Buckets that have refilled to full are indistinguishable from a new one, so they are
    evicted: at most once per refill horizon, and always when ``max_buckets`` is reached.
    If every bucket is still in use at the cap, the oldest is dropped (its owner regains a
    full burst, a bounded leniency in exchange for bounded memory).
    """

    def __init__(
        self,
        *,
        rate_per_second: float,
        burst: int,
        max_buckets: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._rate = rate_per_second
        self._burst = float(burst)
        self._horizon = self._burst / rate_per_second
        self._max_buckets = max_buckets
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}
        self._last_sweep = clock()

    def acquire(self, principal: str) -> None:
        now = self._clock()
        if now - self._last_sweep >= self._horizon or (
            len(self._buckets) >= self._max_buckets and principal not in self._buckets
        ):
            self._sweep(now)
        tokens, updated = self._buckets.pop(principal, (self._burst, now))
        tokens = min(self._burst, tokens + (now - updated) * self._rate)
        if tokens < 1.0:
            self._buckets[principal] = (tokens, now)
            raise RateLimitedError(max(1, ceil((1.0 - tokens) / self._rate)))
        self._buckets[principal] = (tokens - 1.0, now)

    def _sweep(self, now: float) -> None:
        self._last_sweep = now
        for name, (tokens, updated) in list(self._buckets.items()):
            if tokens + (now - updated) * self._rate >= self._burst:
                del self._buckets[name]
        while len(self._buckets) >= self._max_buckets:
            # Insertion order is least recently used: acquire re-inserts on every access.
            del self._buckets[next(iter(self._buckets))]


class McpAuthenticator:
    """Authenticate a ``mcp_`` bearer into a :class:`McpPrincipal`."""

    def __init__(
        self,
        lookup: McpTokenLookup,
        verifier: Argon2Verifier,
        cache: PrincipalCache,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._lookup = lookup
        self._verifier = verifier
        self._cache = cache
        self._clock = clock

    async def authenticate(self, authorization: str | None) -> McpPrincipal:
        if authorization is None:
            raise MissingCredentialError
        bearer = parse_bearer(authorization)
        if bearer is None or not bearer.prefix.startswith(MCP_TOKEN_PREFIX):
            raise InvalidCredentialError
        return await self._authenticate(bearer)

    async def _authenticate(self, bearer: BearerToken) -> McpPrincipal:
        cache_key = self._cache.key_for(bearer.token)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        # One gate covers lookup and verify, so a flood cannot exhaust the DB pool.
        async with self._verifier.admission():
            record = await self._lookup(bearer.prefix)
            # Always run one Argon2 verification, so an unknown prefix is not faster.
            verified = await self._verifier.verify(
                None if record is None else record.token_verifier, bearer.token
            )
        if record is None or not verified:
            raise InvalidCredentialError

        now = self._clock()
        if record.revoked_at is not None and record.revoked_at <= now:
            raise InvalidCredentialError
        if record.expires_at <= now:
            raise InvalidCredentialError
        principal = McpPrincipal(
            principal=record.principal,
            token_id=record.token_id,
            scopes=frozenset(record.scopes),
            expires_at=record.expires_at,
        )
        self._cache.put(cache_key, principal)
        return principal


@dataclass(frozen=True, slots=True)
class McpAuthRuntime:
    """The authenticator and limiter one replica uses."""

    authenticator: McpAuthenticator
    limiter: TokenBucketLimiter


class McpAuthHolder:
    """Late-bound runtime: the app builds the mount before its database exists."""

    def __init__(self, runtime: McpAuthRuntime | None = None) -> None:
        self.runtime = runtime


class McpAccessGate:
    """Authenticate, authorise, rate-limit and audit one request before the MCP app sees it."""

    def __init__(self, scope: str, holder: McpAuthHolder) -> None:
        self._scope = scope
        self._holder = holder

    async def admit(self, scope: Scope, send: Send) -> AdmittedRequest | None:
        """Return the admitted request, or ``None`` after answering a rejection."""
        started = time.perf_counter()
        headers = _header_values(scope)
        audit = _Audit(scope, started)

        authorizations = headers.get("authorization", [])
        try:
            if not authorizations:
                raise MissingCredentialError
            # An ambiguous repeated header is rejected, never resolved.
            if len(authorizations) > 1:
                raise InvalidCredentialError
            runtime = self._holder.runtime
            if runtime is None:
                await _reject(send, 503, "auth_unavailable", retry_after=5)
                audit.record(503, "auth_unavailable")
                return None
            audit.principal = await runtime.authenticator.authenticate(authorizations[0])
            if self._scope not in audit.principal.scopes:
                raise InsufficientScopeError
            runtime.limiter.acquire(audit.principal.principal)
        except AuthOverloadedError as error:
            await _reject(send, error.status_code, error.code, retry_after=1)
            audit.record(error.status_code, error.code)
            return None
        except AuthError as error:
            challenge = error.status_code == 401
            await _reject(send, error.status_code, error.code, challenge=challenge)
            audit.record(error.status_code, error.code)
            return None
        except RateLimitedError as error:
            await _respond(
                send, 429, None, [(b"retry-after", str(error.retry_after_seconds).encode())]
            )
            audit.record(429, "rate_limited")
            return None
        except SQLAlchemyError:
            # Never log the exception: driver errors can embed statement parameters.
            await _reject(send, 503, "auth_unavailable", retry_after=5)
            audit.record(503, "auth_unavailable")
            return None

        scope[PRINCIPAL_SCOPE_KEY] = audit.principal

        async def audited_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                audit.record(int(message["status"]))
            await send(message)

        return _Admitted(audited_send, audit)


class _Audit:
    """One audit line per request: the first ``record`` wins, later ones are no-ops."""

    def __init__(self, scope: Scope, started: float) -> None:
        self.principal: McpPrincipal | None = None
        self._scope = scope
        self._started = started
        self._done = False

    def record(self, status: int | str, error_code: str | None = None) -> None:
        if self._done:
            return
        self._done = True
        # Set by the protocol guard from the validated body; rejections that happen before the
        # body is read have no method to report.
        method = self._scope.get(METHOD_SCOPE_KEY, UNREAD_METHOD)
        _audit(self.principal, method, status, self._started, error_code)


class _Admitted:
    def __init__(self, send: Send, audit: _Audit) -> None:
        self.send = send
        self._audit = audit

    def finish(self, outcome: str | None) -> None:
        # A response that started was already audited with its status; anything else
        # (disconnect, exception before a response) gets a fixed label instead.
        if outcome is not None:
            self._audit.record(outcome, outcome)
        else:
            self._audit.record("error", "no_response")


def require_scope(scope: str, holder: McpAuthHolder) -> McpAccessGate:
    """Build the gate that demands ``scope`` (e.g. ``memory:read``) for every MCP request."""
    return McpAccessGate(scope, holder)


def _header_values(scope: Scope) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for name, value in scope["headers"]:
        values.setdefault(name.decode("latin-1").lower(), []).append(value.decode("latin-1"))
    return values


def _audit(
    principal: McpPrincipal | None,
    method: str,
    status: int | str,
    started: float,
    error_code: str | None,
) -> None:
    latency_ms = round((time.perf_counter() - started) * 1000, 3)
    principal_name = "-" if principal is None else principal.principal
    token_id = "-" if principal is None else str(principal.token_id)
    logger.info(
        "mcp_request principal=%s token_id=%s method=%s status=%s latency_ms=%s",
        principal_name,
        token_id,
        method,
        status,
        latency_ms,
        extra={
            "principal": principal_name,
            "token_id": token_id,
            "method": method,
            "status": status,
            "latency_ms": latency_ms,
            "error_code": error_code,
        },
    )


_MESSAGES: Final[Mapping[str, str]] = {
    "missing_credential": "A bearer credential is required.",
    "invalid_credential": "The credential is not valid.",
    "insufficient_scope": "The credential may not read memory.",
    "auth_overloaded": "Authentication is busy; retry later.",
    "auth_unavailable": "Authentication is not available.",
}


async def _reject(
    send: Send,
    status: int,
    code: str,
    *,
    challenge: bool = False,
    retry_after: int | None = None,
) -> None:
    headers: list[tuple[bytes, bytes]] = []
    if challenge:
        # A bare scheme: no realm, error description or hint about why it failed.
        headers.append((b"www-authenticate", b"Bearer"))
    if retry_after is not None:
        headers.append((b"retry-after", str(retry_after).encode()))
    await _respond(send, status, {"error": code, "message": _MESSAGES[code]}, headers)


async def _respond(
    send: Send,
    status: int,
    body: Mapping[str, Any] | None,
    headers: Iterable[tuple[bytes, bytes]] = (),
) -> None:
    payload = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
    response_headers = [(b"content-length", str(len(payload)).encode()), *headers]
    if body is not None:
        response_headers.append((b"content-type", b"application/json"))
    await send({"type": "http.response.start", "status": status, "headers": response_headers})
    await send({"type": "http.response.body", "body": payload})


# ---------------------------------------------------------------------------
# Operator provisioning
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MintedToken:
    """A freshly minted credential: ``token`` is shown once, ``row`` holds only the verifier."""

    token: str = field(repr=False)
    token_id: UUID = field(default_factory=uuid4)
    row: dict[str, Any] = field(default_factory=dict, repr=False)


def mint_token(
    principal: str,
    expires_at: datetime,
    *,
    now: datetime | None = None,
    hasher: PasswordHasher = _DEFAULT_HASHER,
) -> MintedToken:
    """Generate a ``memory:read`` token and the row (verifier only) to store for it."""
    created_at = datetime.now(UTC) if now is None else now
    if not principal.strip() or len(principal) > 255:
        raise ValueError("principal must be 1-255 non-blank characters")
    if expires_at.tzinfo is None or expires_at <= created_at:
        raise ValueError("expires must be a timezone-aware time in the future")
    prefix = MCP_TOKEN_PREFIX + secrets.token_urlsafe(9)
    token = f"{prefix}.{secrets.token_urlsafe(32)}"
    token_id = uuid4()
    row = {
        "token_id": token_id,
        "token_prefix": prefix,
        "token_verifier": hasher.hash(token),
        "principal": principal,
        "scopes": [MEMORY_READ_SCOPE],
        "created_at": created_at,
        "expires_at": expires_at,
    }
    return MintedToken(token=token, token_id=token_id, row=row)


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m agent_context_platform.mcp.auth",
        description="Provision read-only MCP bearer tokens.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    mint = commands.add_parser("mint", help="mint a memory:read token; the secret prints once")
    mint.add_argument("--principal", required=True)
    mint.add_argument(
        "--expires", required=True, help="ISO 8601 with offset, e.g. 2027-01-01T00:00:00+00:00"
    )
    mint.add_argument(
        "--dsn-env",
        default=DSN_ENVIRONMENT_VARIABLE,
        help="environment variable holding an operator PostgreSQL DSN (postgresql+psycopg://)",
    )
    args = parser.parse_args(argv)

    dsn = os.environ.get(args.dsn_env)
    if not dsn or not dsn.startswith("postgresql+psycopg://"):
        print(f"{args.dsn_env} must hold a postgresql+psycopg:// operator DSN", file=sys.stderr)
        return 2
    try:
        minted = mint_token(args.principal, datetime.fromisoformat(args.expires))
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    engine = create_engine(dsn)
    try:
        with engine.begin() as connection:
            connection.execute(insert(McpTokenRow), [minted.row])
    finally:
        engine.dispose()
    print(f"token_id={minted.token_id}", file=sys.stderr)
    print(minted.token)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
