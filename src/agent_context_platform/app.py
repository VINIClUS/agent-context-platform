from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal

from agent_context_sdk import RedactionPolicyV1  # type: ignore[import-untyped, unused-ignore]
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.routing import Route

from agent_context_platform.content.blob_store import S3BlobStore
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import create_engine, session_factory
from agent_context_platform.ledger.api import IngestionRuntime, RequestContextMiddleware, router
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    SqlProducerLookup,
)
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.mcp.server import MCP_PATH, build_mcp_mount, create_mcp_server
from agent_context_platform.settings import Settings


def _build_ingestion(settings: Settings) -> tuple[IngestionRuntime, AsyncEngine] | None:
    """Wire ingestion from settings; ``None`` (route answers 503) when unconfigured.

    Engine and S3 client creation are lazy, so this opens no connection.
    """
    if settings.postgresql.dsn is None or settings.s3.endpoint_url is None:
        return None
    engine = create_engine(settings)
    sessions = session_factory(engine)
    ingestion = settings.ingestion
    runtime = IngestionRuntime(
        authenticator=ProducerAuthenticator(
            SqlProducerLookup(sessions),
            Argon2Verifier(
                time_cost=ingestion.argon2_time_cost,
                memory_cost_kib=ingestion.argon2_memory_cost_kib,
                parallelism=ingestion.argon2_parallelism,
                max_concurrency=ingestion.argon2_max_concurrency,
                max_queue_depth=ingestion.argon2_max_queue_depth,
            ),
        ),
        service=IngestionService(
            ContentService(S3BlobStore.from_settings(settings.s3), RedactionPolicyV1()), sessions
        ),
        max_request_body_bytes=ingestion.max_request_body_bytes,
    )
    return runtime, engine


def create_app(
    settings: Settings | None = None, *, ingestion: IngestionRuntime | None = None
) -> FastAPI:
    """Create an isolated platform application with no external resources.

    ``ingestion`` injects a ready runtime (tests); otherwise it is built from
    settings when the application starts.
    """
    resolved_settings = Settings() if settings is None else settings
    # One server per application: the SDK app can only be started once.
    mcp_mount = build_mcp_mount(create_mcp_server(), resolved_settings.mcp)

    @asynccontextmanager
    async def lifespan(_application: FastAPI) -> AsyncIterator[None]:
        built = None if ingestion is not None else _build_ingestion(resolved_settings)
        application.state.ingestion = ingestion if built is None else built[0]
        try:
            async with mcp_mount.lifespan():
                yield
        finally:
            if built is not None:
                await built[1].dispose()

    application = FastAPI(title="Agent Context Platform", lifespan=lifespan)
    application.state.settings = resolved_settings
    application.state.ingestion = ingestion
    application.include_router(router)
    application.add_middleware(RequestContextMiddleware)
    # Exact path, no Mount: a mount would redirect POST /mcp to /mcp/.
    application.router.routes.append(Route(MCP_PATH, mcp_mount.asgi_app))

    @application.get("/health/live", include_in_schema=True)
    def health_live() -> dict[str, Literal["ok"]]:
        return {"status": "ok"}

    return application
