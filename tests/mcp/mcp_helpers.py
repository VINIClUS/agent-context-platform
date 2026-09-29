from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx2
from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette
from starlette.routing import Route

from agent_context_platform.mcp.server import MCP_PATH, build_mcp_mount, create_mcp_server
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


@asynccontextmanager
async def mcp_client(
    server: MCPServer[Any] | None = None, **settings: Any
) -> AsyncIterator[httpx2.AsyncClient]:
    """Serve one MCP server in-process; no sockets, no Docker."""
    mount = build_mcp_mount(server or create_mcp_server(), MCPSettings(**settings))
    application = Starlette(routes=[Route(MCP_PATH, mount.asgi_app)])
    async with mount.lifespan():
        transport = httpx2.ASGITransport(app=application)
        async with httpx2.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:8000"
        ) as client:
            yield client
