"""Auth-isolated loopback profiles for the MCP conformance suite (PLATFORM-047). TEST-ONLY.

The pinned suite (`conformance.lock`) connects as an anonymous MCP client: it has no way to send a
bearer token. Both profiles therefore run behind `LoopbackPrincipal`, a shim that adds ONE
runtime-generated token to requests that carry no credential, and only on a socket bound to
127.0.0.1. Everything behind the shim is shipping code: the token goes through the real bearer
authenticator, principal cache, rate limiter and `memory:read` scope gate, a request that brings
its own credential is left untouched (so a wrong token is still refused), and the protocol guard
still checks Host, Origin, the protocol version and the body before any of that.

- `shipping`: the shipping `create_app` (its `/mcp` mount, protocol guard, served surface and the
  five memory tools), bound to `SeededComposer`, a deterministic double of the composer.
- `fixture`: the SAME transport stack (`build_mcp_mount`, protocol guard, stateless handler,
  served surface and error mapping) serving the suite's fixture tools (`fixture_server`), so the
  scenarios that need them exercise our transport for real.

Nothing here is importable from `src/` (a test asserts it), and the shim exists only in this file.

Run one profile: ``python tests/mcp/conformance/loopback.py --profile shipping --port-file F``.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import secrets
import socket
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final
from uuid import UUID

from agent_context_sdk import (
    CommitRevisionV1,
    ContentDisposition,
    ContentRefV1,
    ContentStorage,
    ContextItemV1,
    ContextPackageV1,
    EvidenceRefV1,
    QueryBudgetV1,
    RepositoryScopeV1,
)
from argon2 import PasswordHasher
from pydantic import BaseModel, SecretStr
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from agent_context_platform.app import create_app
from agent_context_platform.mcp.auth import (
    MEMORY_READ_SCOPE,
    McpAuthenticator,
    McpAuthHolder,
    McpAuthRuntime,
    McpTokenRecord,
    PrincipalCache,
    TokenBucketLimiter,
    require_scope,
)
from agent_context_platform.mcp.server import MCP_PATH, build_mcp_mount
from agent_context_platform.retrieval.composer import RequestContext
from agent_context_platform.retrieval.cursor import CursorCodec
from agent_context_platform.security.bearer import Argon2Verifier
from agent_context_platform.settings import MCPSettings, Settings

LOOPBACK: Final = "127.0.0.1"
PRINCIPAL: Final = "conformance-reader"
TOKEN_PREFIX: Final = "mcp_conform01"
# Cursors are signed with the real codec; a fixed clock keeps every answer byte-identical.
SEEDED_NOW: Final = datetime(2026, 9, 1, tzinfo=UTC)
PAGES: Final = 3
# The runner passes its own token so it can scan every artifact for it; otherwise one is minted.
TOKEN_ENV: Final = "AGENT_CONTEXT_CONFORMANCE_TOKEN"
# Replicas that must accept each other's cursors share this key (the shipping setting's name).
CURSOR_KEY_ENV: Final = "AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY"

_HASHER: Final = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)


class LoopbackPrincipal:
    """Adds the profile's own bearer token to credential-less requests (TEST-ONLY shim)."""

    def __init__(self, app: ASGIApp, token: str) -> None:
        self._app = app
        self._header = (b"authorization", f"Bearer {token}".encode())

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and not any(
            name.lower() == b"authorization" for name, _ in scope["headers"]
        ):
            scope = {**scope, "headers": [*scope["headers"], self._header]}
        await self._app(scope, receive, send)


def loopback_auth(token: str, *, principal: str = PRINCIPAL) -> McpAuthRuntime:
    """The real authenticator over a one-token in-memory table (no database)."""
    prefix = token.partition(".")[0]
    record = McpTokenRecord(
        token_id=UUID(int=1),
        token_verifier=_HASHER.hash(token),
        principal=principal,
        scopes=(MEMORY_READ_SCOPE,),
        expires_at=datetime.now(UTC) + timedelta(days=1),
        revoked_at=None,
    )

    async def lookup(candidate: str) -> McpTokenRecord | None:
        return record if candidate == prefix else None

    verifier = Argon2Verifier(
        time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=4, max_queue_depth=64
    )
    cache = PrincipalCache(
        b"c" * 32, ttl=timedelta(seconds=300), max_entries=8, clock=lambda: datetime.now(UTC)
    )
    return McpAuthRuntime(
        authenticator=McpAuthenticator(lookup, verifier, cache),
        # The suite sends a few hundred requests in bursts: it must never be rate limited.
        limiter=TokenBucketLimiter(rate_per_second=10_000.0, burst=10_000),
    )


def new_token() -> str:
    return f"{TOKEN_PREFIX}.{secrets.token_urlsafe(32)}"


def mcp_settings(**overrides: Any) -> MCPSettings:
    return MCPSettings(
        allowed_hosts=(f"{LOOPBACK}:*", "localhost:*"),
        allowed_origins=(f"http://{LOOPBACK}:*", "http://localhost:*"),
        **overrides,
    )


# --- the seeded composer ------------------------------------------------------------------------


def _digest(tool: str, request: BaseModel) -> str:
    canonical = request.model_dump_json(exclude={"cursor"})
    return hashlib.sha256(f"{tool}\n{canonical}".encode()).hexdigest()


def _uuid7(seed: str) -> UUID:
    """A valid, deterministic UUIDv7 derived from ``seed``."""
    raw = bytearray(hashlib.sha256(seed.encode()).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x70
    raw[8] = (raw[8] & 0x3F) | 0x80
    return UUID(bytes=bytes(raw))


class SeededComposer:
    """A deterministic `MemoryComposer` double: the same input gives the same bytes anywhere.

    Every tool answers a fixed package derived from its request. The answer is split into
    `PAGES` pages; each truncated page carries a cursor signed by the real `CursorCodec` with the
    configured key, and a continuation is checked by that codec (so a cursor from another key,
    tool or query is refused exactly as the shipping composer refuses it).
    """

    def __init__(self, codec: CursorCodec) -> None:
        self._codec = codec

    async def _answer(self, tool: str, request: Any, context: RequestContext) -> ContextPackageV1:
        digest = _digest(tool, request)
        as_of = str(_uuid7(f"as-of:{digest}"))
        position = 0
        if request.cursor is not None:
            position = self._codec.decode(
                request.cursor, tool=tool, digest=digest, as_of_event_id=as_of, now=SEEDED_NOW
            ).position
        last = position + 1 >= PAGES
        next_cursor = None
        if not last:
            next_cursor = self._codec.encode(
                tool=tool,
                digest=digest,
                position=position + 1,
                as_of_event_id=as_of,
                now=SEEDED_NOW,
            )
        return _package(tool, request, digest, as_of, position, next_cursor)

    async def context_for_task(self, request: Any, context: RequestContext) -> ContextPackageV1:
        return await self._answer("context_for_task", request, context)

    async def repository_overview(self, request: Any, context: RequestContext) -> ContextPackageV1:
        return await self._answer("repository_overview", request, context)

    async def symbol_context(self, request: Any, context: RequestContext) -> ContextPackageV1:
        return await self._answer("symbol_context", request, context)

    async def decision_history(self, request: Any, context: RequestContext) -> ContextPackageV1:
        return await self._answer("decision_history", request, context)

    async def failure_history(self, request: Any, context: RequestContext) -> ContextPackageV1:
        return await self._answer("failure_history", request, context)


def _package(
    tool: str, request: Any, digest: str, as_of: str, position: int, next_cursor: str | None
) -> ContextPackageV1:
    content = f"seeded answer {position + 1} of {PAGES} for {tool}"
    data = content.encode()
    content_sha = hashlib.sha256(data).hexdigest()
    evidence = EvidenceRefV1(
        source_event_id=_uuid7(f"evidence:{digest}:{position}"),
        evidence_kind="scip",
        confidence=1.0,
        valid_from=SEEDED_NOW,
        content_ref=ContentRefV1(
            content_id=f"c-{position}",
            content_sha256=content_sha,
            media_type="text/plain",
            uncompressed_bytes=len(data),
            disposition=ContentDisposition.SANITIZED,
            storage=ContentStorage.INLINE,
            inline_id=content_sha,
        ),
        resource_uri=f"agent-context://events/{digest[:16]}-{position}",
    )
    # decision_history carries its own (repository or project) scope; the others a repository.
    scope: Any = getattr(request, "scope", None) or RepositoryScopeV1(
        kind="repository", repository_id=request.repository_id
    )
    commit = getattr(request, "commit_id", None) or "a" * 40
    return ContextPackageV1(
        query_id=f"query_{_uuid7(f'query:{digest}').hex}",
        generated_at=SEEDED_NOW,
        scope=scope,
        revision=CommitRevisionV1(kind="commit", commit_id=commit),
        budget=QueryBudgetV1.model_validate(request.budget.model_dump()),
        as_of_event_id=UUID(as_of),
        projection_checkpoint="code:1@7",
        items=(
            ContextItemV1(
                item_id=f"item-{position}",
                kind="symbol",
                title=f"{tool} page {position + 1}",
                content=content,
                evidence=(evidence,),
            ),
        ),
        truncated=next_cursor is not None,
        next_cursor=next_cursor,
        warnings=(),
        staleness_ms=0,
    )


# --- the two profiles ---------------------------------------------------------------------------


def shipping_app(token: str, *, cursor_key: SecretStr | None = None) -> ASGIApp:
    """The shipping application with its MCP auth runtime and composer injected."""
    key = cursor_key if cursor_key is not None else SecretStr(secrets.token_urlsafe(48))
    application = create_app(
        Settings(environment="test", mcp=mcp_settings(cursor_hmac_key=key)),
        mcp_auth=loopback_auth(token),
        composer=SeededComposer(CursorCodec(key)),
    )
    return LoopbackPrincipal(application, token)


def fixture_app(token: str) -> ASGIApp:
    """The shipping transport stack serving the suite's fixture tools."""
    from fixture_server import create_fixture_server

    mount = build_mcp_mount(
        create_fixture_server(),
        mcp_settings(),
        access_gate=require_scope(MEMORY_READ_SCOPE, McpAuthHolder(loopback_auth(token))),
    )

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        async with mount.lifespan():
            yield

    application = Starlette(routes=[Route(MCP_PATH, mount.asgi_app)], lifespan=lifespan)
    return LoopbackPrincipal(application, token)


PROFILES: Final[Mapping[str, Any]] = {"shipping": shipping_app, "fixture": fixture_app}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(PROFILES), required=True)
    parser.add_argument("--port-file", type=Path, required=True)
    arguments = parser.parse_args(argv)

    import uvicorn

    # Bound here, on loopback only, so the shim can never face another interface.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((LOOPBACK, 0))
    listener.listen(128)  # connections queue until the server accepts them
    token = os.environ.pop(TOKEN_ENV, None) or new_token()
    if arguments.profile == "shipping":
        key = os.environ.pop(CURSOR_KEY_ENV, None)
        application = shipping_app(token, cursor_key=None if key is None else SecretStr(key))
    else:
        application = fixture_app(token)
    server = uvicorn.Server(
        uvicorn.Config(application, log_level="warning", access_log=False, lifespan="on")
    )
    # Written whole, then renamed: a reader never sees a partial port number.
    partial = arguments.port_file.with_suffix(".partial")
    partial.write_text(str(listener.getsockname()[1]), encoding="utf-8")
    partial.replace(arguments.port_file)
    server.run(sockets=[listener])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
