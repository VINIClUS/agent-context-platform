from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx2
from argon2 import PasswordHasher
from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette
from starlette.routing import Route

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
from agent_context_platform.mcp.server import MCP_PATH, build_mcp_mount, create_mcp_server
from agent_context_platform.security.bearer import Argon2Verifier
from agent_context_platform.settings import MCPSettings

PROTOCOL_VERSION = "2026-07-28"
VERSION_META_KEY = "io.modelcontextprotocol/protocolVersion"
CAPABILITIES_META_KEY = "io.modelcontextprotocol/clientCapabilities"
SERVER_INFO_META_KEY = "io.modelcontextprotocol/serverInfo"


def rpc_body(
    method: str, params: dict[str, Any] | None = None, request_id: int = 1
) -> dict[str, Any]:
    meta = {VERSION_META_KEY: PROTOCOL_VERSION, CAPABILITIES_META_KEY: {}}
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": {**(params or {}), "_meta": meta},
    }


def mcp_headers(method: str, **extra: str) -> dict[str, str]:
    return {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
        "mcp-protocol-version": PROTOCOL_VERSION,
        "mcp-method": method,
        **extra,
    }


NOW = datetime(2026, 9, 1, tzinfo=UTC)
PREFIX = "mcp_test0001"
SECRET = "s" * 43
TOKEN = f"{PREFIX}.{SECRET}"
HMAC_KEY = b"k" * 32
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)


class OpenGate:
    """An access gate that admits everything, for tests of the guard's other checks."""

    async def admit(self, scope: Any, send: Any) -> Any:
        return send


class Clock:
    """A controllable wall clock shared by the authenticator, cache and limiter."""

    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)

    def monotonic(self) -> float:
        return self.now.timestamp()


def token_record(token: str = TOKEN, **overrides: Any) -> McpTokenRecord:
    values: dict[str, Any] = {
        "token_id": uuid4(),
        "token_verifier": _HASHER.hash(token),
        "principal": "codex-reader",
        "scopes": (MEMORY_READ_SCOPE,),
        "expires_at": NOW + timedelta(days=1),
        "revoked_at": None,
    }
    values.update(overrides)
    return McpTokenRecord(**values)


@dataclass
class AuthHarness:
    """An in-memory token table plus the runtime that authenticates against it."""

    clock: Clock = field(default_factory=Clock)
    records: dict[str, McpTokenRecord] = field(default_factory=dict)
    lookups: list[str] = field(default_factory=list)
    lookup_error: Exception | None = None

    def add(self, token: str = TOKEN, **overrides: Any) -> McpTokenRecord:
        record = token_record(token, **overrides)
        self.records[token.partition(".")[0]] = record
        return record

    async def lookup(self, prefix: str) -> McpTokenRecord | None:
        self.lookups.append(prefix)
        if self.lookup_error is not None:
            raise self.lookup_error
        return self.records.get(prefix)

    def runtime(
        self,
        *,
        ttl_seconds: float = 30.0,
        rate_per_second: float = 1000.0,
        burst: int = 1000,
        max_concurrency: int = 2,
        max_queue_depth: int = 8,
    ) -> McpAuthRuntime:
        self.verifier = Argon2Verifier(
            time_cost=1,
            memory_cost_kib=8,
            parallelism=1,
            max_concurrency=max_concurrency,
            max_queue_depth=max_queue_depth,
        )
        self.cache = PrincipalCache(
            HMAC_KEY, ttl=timedelta(seconds=ttl_seconds), max_entries=8, clock=self.clock
        )
        return McpAuthRuntime(
            authenticator=McpAuthenticator(
                self.lookup, self.verifier, self.cache, clock=self.clock
            ),
            limiter=TokenBucketLimiter(
                rate_per_second=rate_per_second, burst=burst, clock=self.clock.monotonic
            ),
        )


@asynccontextmanager
async def mcp_client(
    server: MCPServer[Any] | None = None,
    *,
    runtime: McpAuthRuntime | None = None,
    token: str | None = TOKEN,
    **settings: Any,
) -> AsyncIterator[httpx2.AsyncClient]:
    """Serve one MCP server in-process; no sockets, no Docker.

    By default the caller is a valid ``memory:read`` principal; pass ``token=None``
    to send no credential, or a ``runtime`` to control the token table.
    """
    if runtime is None:
        harness = AuthHarness()
        harness.add()
        runtime = harness.runtime()
    mount = build_mcp_mount(
        server or create_mcp_server(),
        MCPSettings(**settings),
        access_gate=require_scope(MEMORY_READ_SCOPE, McpAuthHolder(runtime)),
    )
    application = Starlette(routes=[Route(MCP_PATH, mount.asgi_app)])
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    async with mount.lifespan():
        transport = httpx2.ASGITransport(app=application)
        async with httpx2.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:8000", headers=headers
        ) as client:
            yield client
