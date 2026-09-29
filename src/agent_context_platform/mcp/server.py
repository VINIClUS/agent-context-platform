from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Final

from mcp.server.caching import CacheHint
from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.shared.exceptions import MCPError
from mcp_types import METHOD_NOT_FOUND
from starlette.applications import Starlette
from starlette.types import ASGIApp

from agent_context_platform.mcp.protocol_guard import ProtocolGuard
from agent_context_platform.settings import MCPSettings

MCP_PATH: Final = "/mcp"
SERVER_NAME: Final = "agent-context-platform"
SERVER_VERSION: Final = "0.1.0"
# Lists change only on deploy; private until auth scopes (PLATFORM-045) decide visibility.
LIST_CACHE_HINT: Final = CacheHint(ttl_ms=60_000, scope="private")

_ALWAYS_SERVED: Final = frozenset({"server/discover"})
_TOOL_METHODS: Final = frozenset({"tools/list", "tools/call"})


class _ServedSurface:
    """Middleware that exposes only the methods this server actually backs.

    The SDK registers resources, prompts and a per-process change-notification
    stream by default. None survives independent replicas, so anything
    outside the allowlist answers "method not found" and discovery advertises
    only what is served.
    """

    def __init__(self, *, advertise_tools: bool) -> None:
        self._advertise_tools = advertise_tools
        self._served = _ALWAYS_SERVED | (_TOOL_METHODS if advertise_tools else frozenset())

    async def __call__(
        self, ctx: ServerRequestContext[Any, Any], call_next: CallNext
    ) -> HandlerResult:
        if ctx.method not in self._served:
            raise MCPError(code=METHOD_NOT_FOUND, message="Method not found")
        result = await call_next(ctx)
        if ctx.method == "server/discover" and isinstance(result, dict):
            result = {
                **result,
                "capabilities": {"tools": {"listChanged": False}} if self._advertise_tools else {},
            }
        return result


class _SortedToolsServer(MCPServer[Any]):
    """MCPServer whose tool listing order does not depend on registration order."""

    async def list_tools(self) -> list[Any]:
        return sorted(await super().list_tools(), key=lambda tool: tool.name)


def create_mcp_server(*, advertise_tools: bool = False) -> MCPServer[Any]:
    """Build the read-only MCP server; tools are registered by PLATFORM-046."""
    return _SortedToolsServer(
        SERVER_NAME,
        version=SERVER_VERSION,
        cache_hints={"tools/list": LIST_CACHE_HINT},
        middleware=[_ServedSurface(advertise_tools=advertise_tools)],
    )


class MCPMount:
    """A guarded stateless MCP ASGI app plus the lifespan it needs to run."""

    def __init__(self, asgi_app: ASGIApp, sdk_app: Starlette) -> None:
        self.asgi_app = asgi_app
        self._sdk_app = sdk_app

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        return self._run()

    @asynccontextmanager
    async def _run(self) -> AsyncIterator[None]:
        async with self._sdk_app.router.lifespan_context(self._sdk_app):
            yield


def build_mcp_mount(server: MCPServer[Any], settings: MCPSettings) -> MCPMount:
    """Wrap ``server`` as a stateless Streamable HTTP app behind the protocol guard."""
    sdk_app = server.streamable_http_app(
        streamable_http_path=MCP_PATH,
        json_response=True,
        stateless_http=settings.stateless_http,
        max_request_body_size=settings.max_request_body_bytes,
        # Host and Origin are enforced by ProtocolGuard; avoid a second, divergent allowlist.
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    guarded = ProtocolGuard(
        sdk_app,
        allowed_hosts=settings.allowed_hosts,
        allowed_origins=settings.allowed_origins,
        max_body_bytes=settings.max_request_body_bytes,
    )
    return MCPMount(guarded, sdk_app)
