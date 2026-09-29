from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI
from starlette.routing import Route

from agent_context_platform.mcp.server import MCP_PATH, build_mcp_mount, create_mcp_server
from agent_context_platform.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create an isolated platform application with no external resources."""
    resolved_settings = Settings() if settings is None else settings
    # One server per application: the SDK app can only be started once.
    mcp_mount = build_mcp_mount(create_mcp_server(), resolved_settings.mcp)

    @asynccontextmanager
    async def lifespan(_application: FastAPI) -> AsyncIterator[None]:
        async with mcp_mount.lifespan():
            yield

    application = FastAPI(title="Agent Context Platform", lifespan=lifespan)
    application.state.settings = resolved_settings
    # Exact path, no Mount: a mount would redirect POST /mcp to /mcp/.
    application.router.routes.append(Route(MCP_PATH, mcp_mount.asgi_app))

    @application.get("/health/live", include_in_schema=True)
    def health_live() -> dict[str, Literal["ok"]]:
        return {"status": "ok"}

    return application
