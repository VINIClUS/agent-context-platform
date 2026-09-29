import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal

from agent_context_sdk import RedactionPolicyV1  # type: ignore[import-untyped, unused-ignore]
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
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
from agent_context_platform.mcp.auth import (
    MEMORY_READ_SCOPE,
    McpAuthenticator,
    McpAuthHolder,
    McpAuthRuntime,
    PrincipalCache,
    SqlMcpTokenLookup,
    TokenBucketLimiter,
    require_scope,
)
from agent_context_platform.mcp.server import MCP_PATH, build_mcp_mount, create_mcp_server
from agent_context_platform.settings import Settings


def _build_ingestion(
    settings: Settings, sessions: async_sessionmaker[AsyncSession]
) -> IngestionRuntime | None:
    """Wire ingestion from settings; ``None`` (route answers 503) when unconfigured.

    Engine and S3 client creation are lazy, so this opens no connection.
    """
    if settings.s3.endpoint_url is None:
        return None
    ingestion = settings.ingestion
    return IngestionRuntime(
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


def _build_mcp_auth(
    settings: Settings, sessions: async_sessionmaker[AsyncSession]
) -> McpAuthRuntime:
    """Wire MCP authentication; it needs only PostgreSQL, and no connection opens here."""
    mcp = settings.mcp
    ingestion = settings.ingestion
    configured_key = None if mcp.token_hmac_key is None else mcp.token_hmac_key.get_secret_value()
    # Outside production a random per-process key is enough: the cache is in-memory.
    key = configured_key.strip().encode() if configured_key else secrets.token_bytes(32)
    clock = _utc_now
    return McpAuthRuntime(
        authenticator=McpAuthenticator(
            SqlMcpTokenLookup(sessions),
            # Its own verifier and admission gate: an MCP flood must not shed ingestion load.
            Argon2Verifier(
                time_cost=ingestion.argon2_time_cost,
                memory_cost_kib=ingestion.argon2_memory_cost_kib,
                parallelism=ingestion.argon2_parallelism,
                max_concurrency=ingestion.argon2_max_concurrency,
                max_queue_depth=ingestion.argon2_max_queue_depth,
            ),
            PrincipalCache(
                key,
                ttl=timedelta(seconds=mcp.principal_cache_ttl_seconds),
                max_entries=mcp.principal_cache_max_entries,
                clock=clock,
            ),
            clock=clock,
        ),
        limiter=TokenBucketLimiter(
            rate_per_second=mcp.rate_limit_per_second, burst=mcp.rate_limit_burst
        ),
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def create_app(
    settings: Settings | None = None,
    *,
    ingestion: IngestionRuntime | None = None,
    mcp_auth: McpAuthRuntime | None = None,
) -> FastAPI:
    """Create an isolated platform application with no external resources.

    ``ingestion`` and ``mcp_auth`` inject ready runtimes (tests); otherwise they are built
    from settings when the application starts. Until MCP authentication is bound, ``/mcp``
    answers 401 without a token and 503 with one: it fails closed.
    """
    resolved_settings = Settings() if settings is None else settings
    mcp_holder = McpAuthHolder(mcp_auth)
    # One server per application: the SDK app can only be started once.
    mcp_mount = build_mcp_mount(
        create_mcp_server(),
        resolved_settings.mcp,
        access_gate=require_scope(MEMORY_READ_SCOPE, mcp_holder),
    )

    @asynccontextmanager
    async def lifespan(_application: FastAPI) -> AsyncIterator[None]:
        dsn = resolved_settings.postgresql.dsn
        engine = None if dsn is None else create_engine(resolved_settings)
        sessions = None if engine is None else session_factory(engine)
        if ingestion is None and sessions is not None:
            application.state.ingestion = _build_ingestion(resolved_settings, sessions)
        if mcp_auth is None and sessions is not None:
            mcp_holder.runtime = _build_mcp_auth(resolved_settings, sessions)
        try:
            async with mcp_mount.lifespan():
                yield
        finally:
            if engine is not None:
                await engine.dispose()

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
