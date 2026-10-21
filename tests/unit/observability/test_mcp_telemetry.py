"""MCP request telemetry through the real guarded transport, authentication included.

Tool arguments, tool results, bearer tokens and caller-chosen method or tool names are canaries:
none may reach a span, a metric or a log record. Labels come from closed sets only.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx2
import pytest
from argon2 import PasswordHasher
from mcp.server.mcpserver import MCPServer
from opentelemetry.trace import StatusCode
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
from agent_context_platform.mcp.telemetry import McpSpanScope, McpTelemetry
from agent_context_platform.observability import contract, retrieval_span
from agent_context_platform.security.bearer import Argon2Verifier
from agent_context_platform.settings import MCPSettings

from ._capture import Canaries, Capture, log_text

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

PROTOCOL_VERSION = "2026-07-28"
NOW = datetime(2026, 9, 1, tzinfo=UTC)
PREFIX = "mcp_tele0001"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _rpc(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    meta = {
        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
    }
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": {**(params or {}), "_meta": meta},
    }


def _headers(method: str, **extra: str) -> dict[str, str]:
    return {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
        "mcp-protocol-version": PROTOCOL_VERSION,
        "mcp-method": method,
        **extra,
    }


def _server(canaries: Canaries) -> MCPServer[Any]:
    server = create_mcp_server(advertise_tools=True)

    @server.tool()
    async def symbol_context(query: str) -> str:
        # A tool's own retrieval span nests under the MCP request span.
        with retrieval_span("symbol_context"):
            return f"result {canaries.content} for {query}"

    @server.tool()
    async def failure_history(query: str) -> str:
        raise RuntimeError(f"tool failed on {query} {canaries.content}")

    return server


@asynccontextmanager
async def _client(
    canaries: Canaries, *, burst: int = 1000
) -> AsyncIterator[tuple[httpx2.AsyncClient, str]]:
    token = f"{PREFIX}.{canaries.token_secret}"
    record = McpTokenRecord(
        token_id=uuid4(),
        token_verifier=_HASHER.hash(token),
        principal="codex-reader",
        scopes=(MEMORY_READ_SCOPE,),
        expires_at=NOW + timedelta(days=1),
        revoked_at=None,
    )

    async def lookup(prefix: str) -> McpTokenRecord | None:
        return record if prefix == PREFIX else None

    clock = lambda: NOW  # noqa: E731
    runtime = McpAuthRuntime(
        authenticator=McpAuthenticator(
            lookup,
            Argon2Verifier(
                time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2, max_queue_depth=8
            ),
            PrincipalCache(b"k" * 32, ttl=timedelta(seconds=30), max_entries=8, clock=clock),
            clock=clock,
        ),
        limiter=TokenBucketLimiter(rate_per_second=0.001, burst=burst),
    )
    mount = build_mcp_mount(
        _server(canaries),
        MCPSettings(),
        access_gate=require_scope(MEMORY_READ_SCOPE, McpAuthHolder(runtime)),
    )
    application = Starlette(routes=[Route(MCP_PATH, mount.asgi_app)])
    async with (
        mount.lifespan(),
        httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=application), base_url="http://127.0.0.1:8000"
        ) as client,
    ):
        yield client, token


async def _call(
    client: httpx2.AsyncClient,
    token: str | None,
    method: str,
    params: dict[str, Any] | None = None,
    **headers: str,
) -> httpx2.Response:
    auth = {} if token is None else {"authorization": f"Bearer {token}"}
    return await client.post(
        MCP_PATH, json=_rpc(method, params), headers={**_headers(method, **headers), **auth}
    )


async def test_requests_are_counted_by_method_tool_and_outcome(
    telemetry: Capture, canaries: Canaries, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    async with _client(canaries) as (client, token):
        assert (await _call(client, token, "server/discover")).status_code == 200
        assert (await _call(client, token, "tools/list")).status_code == 200
        call = await _call(
            client,
            token,
            "tools/call",
            {"name": "symbol_context", "arguments": {"query": canaries.tool_argument}},
            **{"mcp-name": "symbol_context", "traceparent": TRACEPARENT},
        )
        assert call.status_code == 200
        assert canaries.content in call.text  # the client gets the result; telemetry does not
        failed = await _call(
            client,
            token,
            "tools/call",
            {"name": "failure_history", "arguments": {"query": canaries.prompt}},
            **{"mcp-name": "failure_history"},
        )
        assert failed.status_code == 200
        unknown = await _call(
            client,
            token,
            "tools/call",
            {"name": canaries.tool_argument, "arguments": {}},
            **{"mcp-name": canaries.tool_argument},
        )
        assert unknown.status_code == 200
        assert (await _call(client, token, "resources/list")).status_code == 404
        assert (await _call(client, None, "tools/list")).status_code == 401
        assert (await _call(client, "mcp_tele0001.wrong", canaries.prompt)).status_code == 401

    points = telemetry.points(contract.MCP_REQUESTS)
    assert points == {
        (("method", "server/discover"), ("outcome", "ok"), ("tool", "none")): 1,
        (("method", "tools/list"), ("outcome", "ok"), ("tool", "none")): 1,
        (("method", "tools/call"), ("outcome", "ok"), ("tool", "symbol_context")): 1,
        (("method", "tools/call"), ("outcome", "ok"), ("tool", "failure_history")): 1,
        (("method", "tools/call"), ("outcome", "ok"), ("tool", "other")): 1,
        (("method", "other"), ("outcome", "client_error"), ("tool", "none")): 1,
        (("method", "tools/list"), ("outcome", "auth_error"), ("tool", "none")): 1,
        (("method", "other"), ("outcome", "auth_error"), ("tool", "none")): 1,
    }
    assert telemetry.value(contract.MCP_DURATION, method="tools/call", tool="symbol_context") == 1

    calls = telemetry.spans("tools/call")
    symbol = next(
        span
        for span in calls
        if (span.attributes or {}).get("gen_ai.tool.name") == "symbol_context"
    )
    assert dict(symbol.attributes or {}) == {
        "mcp.method.name": "tools/call",
        "gen_ai.tool.name": "symbol_context",
        "agent_context.outcome": "ok",
    }
    assert format(symbol.context.trace_id, "032x") == TRACEPARENT.split("-")[1]
    retrieval = telemetry.span("agent_context.retrieval.symbol_context")
    assert retrieval.parent is not None and retrieval.parent.span_id == symbol.context.span_id
    assert (
        telemetry.value(contract.RETRIEVAL_DURATION, operation="symbol_context", outcome="ok") == 1
    )
    # The MCP SDK's own logger reports tool exceptions with their text (PLATFORM-046's concern);
    # the platform's and the OpenTelemetry SDK's records must stay content-free.
    canaries.assert_absent(
        telemetry.dump(), log_text(caplog, "agent_context_platform", "opentelemetry")
    )


async def test_rate_limited_requests_have_their_own_outcome(
    telemetry: Capture, canaries: Canaries
) -> None:
    async with _client(canaries, burst=1) as (client, token):
        assert (await _call(client, token, "tools/list")).status_code == 200
        assert (await _call(client, token, "tools/list")).status_code == 429

    assert (
        telemetry.value(
            contract.MCP_REQUESTS, method="tools/list", tool="none", outcome="rate_limited"
        )
        == 1
    )


async def test_a_crash_inside_the_transport_is_a_server_error(
    telemetry: Capture, canaries: Canaries
) -> None:
    async def explode(scope: Any, receive: Any, send: Any) -> None:
        raise RuntimeError(canaries.content)

    with pytest.raises(RuntimeError):
        await McpTelemetry(explode)(
            {"type": "http", "headers": [(b"mcp-method", b"tools/list")]},
            None,
            None,  # type: ignore[arg-type]
        )

    assert (
        telemetry.value(
            contract.MCP_REQUESTS, method="tools/list", tool="none", outcome="server_error"
        )
        == 1
    )
    span = telemetry.span("tools/list")
    assert span.status.status_code is StatusCode.ERROR
    assert (span.attributes or {})["error.type"] == "RuntimeError"
    canaries.assert_absent(telemetry.dump())


async def test_an_abandoned_request_is_a_client_error(telemetry: Capture) -> None:
    async def silent(scope: Any, receive: Any, send: Any) -> None:
        return None

    await McpTelemetry(silent)({"type": "http", "headers": []}, None, None)  # type: ignore[arg-type]

    assert (
        telemetry.value(contract.MCP_REQUESTS, method="other", tool="none", outcome="client_error")
        == 1
    )


async def test_non_http_scopes_pass_through_untouched(telemetry: Capture) -> None:
    seen: list[str] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append(scope["type"])

    await McpTelemetry(app)({"type": "lifespan"}, None, None)  # type: ignore[arg-type]
    await McpSpanScope(app)({"type": "http"}, None, None)  # type: ignore[arg-type]

    assert seen == ["lifespan", "http"]
    assert telemetry.points(contract.MCP_REQUESTS) == {}
