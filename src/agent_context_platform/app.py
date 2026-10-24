import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from agent_context_sdk import RedactionPolicyV1  # type: ignore[import-untyped, unused-ignore]
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from starlette.routing import Route

from agent_context_platform.content.blob_store import S3BlobStore
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import create_engine, session_factory
from agent_context_platform.ledger.api import (
    SECURITY_SCHEME,
    SECURITY_SCHEME_NAME,
    IngestionRuntime,
    RequestContextMiddleware,
    request_schema_components,
    router,
)
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    SqlProducerLookup,
)
from agent_context_platform.ledger.otlp_api import OtlpReceiverRuntime
from agent_context_platform.ledger.otlp_api import router as otlp_router
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.ledger.whoami import router as whoami_router
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
from agent_context_platform.mcp.tools import ComposerHolder, MemoryComposer
from agent_context_platform.observability import setup as telemetry_setup
from agent_context_platform.observability.http import HttpTelemetryMiddleware
from agent_context_platform.operations import faults, migrate
from agent_context_platform.operations.readiness import (
    DEFAULT_TIMEOUT_SECONDS,
    ReadinessProbe,
    ReadinessReport,
    build_probe,
    readiness_engine,
)
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.retrieval.composer import ContextComposer
from agent_context_platform.retrieval.cursor import CursorCodec
from agent_context_platform.retrieval.embeddings import EmbeddingProvider, LocalMiniLMProvider
from agent_context_platform.retrieval.evidence import EvidenceLedger
from agent_context_platform.retrieval.freshness import FreshnessReader
from agent_context_platform.retrieval.graph import GraphTraversalService
from agent_context_platform.retrieval.search import SearchService
from agent_context_platform.retrieval.temporal import TemporalService
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


# --- Codex OTLP trace receiver (PLATFORM-053) ----------------------------------------------------
def _build_otlp_receiver(
    settings: Settings, ingestion: IngestionRuntime | None
) -> OtlpReceiverRuntime | None:
    """The receiver shares ingestion's authenticator and service; ``None`` (503) until both
    ingestion and the stable idempotency key are configured."""
    receiver = settings.otel_receiver
    if ingestion is None or receiver.idempotency_hmac_key is None:
        return None
    return OtlpReceiverRuntime(
        authenticator=ingestion.authenticator,
        service=ingestion.service,
        limiter=TokenBucketLimiter(
            rate_per_second=receiver.rate_limit_per_second, burst=receiver.rate_limit_burst
        ),
        hmac_key=receiver.idempotency_hmac_key.get_secret_value().strip().encode(),
        max_request_body_bytes=receiver.max_request_body_bytes,
        max_decompressed_bytes=receiver.max_decompressed_bytes,
        max_spans=receiver.max_spans,
        max_records=receiver.max_records,
        max_events_per_span=receiver.max_events_per_span,
        max_attributes=receiver.max_attributes,
        max_clock_skew=timedelta(seconds=receiver.max_clock_skew_seconds),
        max_resources=receiver.max_resources,
        max_scopes=receiver.max_scopes,
        max_links=receiver.max_links,
        max_total_attributes=receiver.max_total_attributes,
        max_fields=receiver.max_fields,
        max_objects=receiver.max_objects,
    )


# --- end Codex OTLP trace receiver ---------------------------------------------------------------


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


def _composer_configured(settings: Settings) -> bool:
    """The memory tools need PostgreSQL, a complete Neo4j login and S3 (object content)."""
    neo4j = settings.neo4j
    return (
        settings.postgresql.effective_api_dsn is not None
        and neo4j.uri is not None
        and bool(neo4j.username and neo4j.username.strip())
        and neo4j.password is not None
        and bool(neo4j.password.get_secret_value().strip())
        and settings.s3.endpoint_url is not None
    )


async def _build_composer(
    settings: Settings, sessions: async_sessionmaker[AsyncSession], store: Neo4jStore
) -> tuple[ContextComposer, list[Callable[[], Awaitable[None]]]]:
    """Wire the five memory tools' composer (PLATFORM-043) to the API role, never the projector.

    ``sessions`` must be the API-role factory (``postgresql.api_dsn``, falling back to ``dsn``):
    PLATFORM-055 grants that role every read the readers need. Without a model directory the
    search has no embedder (lexical-only): the composer reports ``vector_search_unavailable``.
    Loading a configured model verifies its digests and fails start-up closed on a mismatch.
    """
    embeddings: EmbeddingProvider | None = None
    model_dir = settings.search.model_dir
    if model_dir is not None:
        embeddings = await asyncio.to_thread(
            LocalMiniLMProvider.load,
            model_dir,
            batch_size=settings.search.embedding_batch_size,
            threads=settings.search.embedding_threads,
        )
    graph = GraphTraversalService.from_settings(settings.neo4j)
    temporal = TemporalService.from_settings(settings.neo4j)
    reader = store.read_only()
    content = ContentService(S3BlobStore.from_settings(settings.s3), RedactionPolicyV1())
    composer = ContextComposer(
        graph=graph,
        graph_reader=reader,
        temporal=temporal,
        search=SearchService(sessions, reader, embeddings),
        ledger=EvidenceLedger(sessions, content),
        freshness=FreshnessReader(sessions),
        # Settings require the key in production; elsewhere cursors die with the process.
        cursor=CursorCodec.from_key(settings.mcp.cursor_hmac_key),
    )
    return composer, [graph.close, temporal.close]


def _api_engine(settings: Settings, engine: AsyncEngine | None) -> AsyncEngine | None:
    """The API-role engine: a separate one only when ``api_dsn`` differs from ``dsn``."""
    postgresql = settings.postgresql
    if postgresql.api_dsn is None:
        return engine
    return create_async_engine(
        postgresql.api_dsn.get_secret_value().unicode_string(), pool_pre_ping=True
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _shipped_head() -> str | None:
    """The Alembic head next to this code (``None``: readiness reports it unconfigured)."""
    try:
        return migrate.image_head(migrate.alembic_config())
    except migrate.MigrationError:
        return None


def create_app(
    settings: Settings | None = None,
    *,
    ingestion: IngestionRuntime | None = None,
    mcp_auth: McpAuthRuntime | None = None,
    readiness: ReadinessProbe | None = None,
    composer: MemoryComposer | None = None,
    otlp_receiver: OtlpReceiverRuntime | None = None,
) -> FastAPI:
    """Create an isolated platform application with no external resources.

    ``ingestion`` and ``mcp_auth`` inject ready runtimes (tests); otherwise they are built
    from settings when the application starts. Until MCP authentication is bound, ``/mcp``
    answers 401 without a token and 503 with one: it fails closed. ``readiness`` injects the
    ``/health/ready`` probe; until one is bound the route answers 503. ``composer`` injects the
    memory tools' composer; otherwise it is built at start-up when PostgreSQL, Neo4j and S3 are
    configured, and until then every tool call answers ``context_unavailable``. ``otlp_receiver``
    injects the Codex OTLP receiver; otherwise it is built with ingestion (503 until configured).
    """
    resolved_settings = Settings() if settings is None else settings
    faults.configure(resolved_settings.fault_injection)
    mcp_holder = McpAuthHolder(mcp_auth)
    composer_holder = ComposerHolder(composer)
    # One server per application: the SDK app can only be started once.
    mcp_mount = build_mcp_mount(
        create_mcp_server(composer=composer_holder),
        resolved_settings.mcp,
        access_gate=require_scope(MEMORY_READ_SCOPE, mcp_holder),
    )

    @asynccontextmanager
    async def lifespan(_application: FastAPI) -> AsyncIterator[None]:
        # Telemetry (PLATFORM-050): exported only when an endpoint is configured.
        telemetry = telemetry_setup.start(resolved_settings)
        try:
            async with _datastores():
                yield
        finally:
            await telemetry_setup.stop(telemetry, resolved_settings)

    @asynccontextmanager
    async def _datastores() -> AsyncIterator[None]:
        dsn = resolved_settings.postgresql.dsn
        engine = None if dsn is None else create_engine(resolved_settings)
        sessions = None if engine is None else session_factory(engine)
        if ingestion is None and sessions is not None:
            application.state.ingestion = _build_ingestion(resolved_settings, sessions)
        if otlp_receiver is None:
            application.state.otlp_receiver = _build_otlp_receiver(
                resolved_settings, application.state.ingestion
            )
        if mcp_auth is None and sessions is not None:
            mcp_holder.runtime = _build_mcp_auth(resolved_settings, sessions)
        neo4j = resolved_settings.neo4j
        store = None if neo4j.uri is None else Neo4jStore(neo4j)
        api_engine = _api_engine(resolved_settings, engine)
        closers: list[Callable[[], Awaitable[None]]] = []
        readiness_db = None
        if readiness is None:
            readiness_db = None if dsn is None else readiness_engine(dsn, DEFAULT_TIMEOUT_SECONDS)
            application.state.readiness = build_probe(
                resolved_settings,
                readiness_db,
                store,
                None if readiness_db is None else _shipped_head(),
            )
        try:
            if (
                composer is None
                and store is not None
                and api_engine is not None
                and _composer_configured(resolved_settings)
            ):
                composer_holder.composer, closers = await _build_composer(
                    resolved_settings, session_factory(api_engine), store
                )
            async with mcp_mount.lifespan():
                yield
        finally:
            application.state.readiness = readiness
            composer_holder.composer = composer
            for close in closers:
                await close()
            if api_engine is not None and api_engine is not engine:
                await api_engine.dispose()
            if store is not None:
                await store.close()
            if readiness_db is not None:
                await readiness_db.dispose()
            if engine is not None:
                await engine.dispose()

    application = FastAPI(title="Agent Context Platform", lifespan=lifespan)
    application.state.settings = resolved_settings
    framework_openapi = application.openapi

    def openapi() -> dict[str, Any]:
        """Framework schema plus the SDK request model the raw-body route documents."""
        schema = framework_openapi()
        schemas = schema.setdefault("components", {}).setdefault("schemas", {})
        for name, component in request_schema_components().items():
            if schemas.setdefault(name, component) != component:
                raise RuntimeError(f"OpenAPI component {name} conflicts with the request schema")
        schema["components"].setdefault("securitySchemes", {})[SECURITY_SCHEME_NAME] = (
            SECURITY_SCHEME
        )
        return schema

    application.openapi = openapi  # type: ignore[method-assign]
    application.state.ingestion = ingestion
    application.state.readiness = readiness
    application.state.otlp_receiver = otlp_receiver
    application.state.mcp_auth = mcp_holder  # read by the token probe (`ledger.whoami`)
    application.include_router(router)
    application.include_router(otlp_router)  # PLATFORM-053; outside the frozen OpenAPI
    application.include_router(whoami_router)  # outside the frozen ingestion OpenAPI
    # Inside the request-context middleware: the server span is a child of the caller's trace.
    application.add_middleware(HttpTelemetryMiddleware)
    application.add_middleware(RequestContextMiddleware)
    # Exact path, no Mount: a mount would redirect POST /mcp to /mcp/.
    application.router.routes.append(Route(MCP_PATH, mcp_mount.asgi_app))

    @application.get("/health/live", include_in_schema=True)
    def health_live() -> dict[str, Literal["ok"]]:
        return {"status": "ok"}

    @application.get(
        "/health/ready",
        include_in_schema=True,
        response_class=JSONResponse,
        responses={503: {"description": "Not ready: component names and coarse states only."}},
    )
    async def health_ready() -> JSONResponse:
        """PostgreSQL at the image's Alembic head, Neo4j and S3 reachable (never cached)."""
        probe: ReadinessProbe | None = application.state.readiness
        report = ReadinessReport({}) if probe is None else await probe.probe()
        return JSONResponse(
            report.to_dict(),
            status_code=200 if report.ready else 503,
            headers={"Cache-Control": "no-store"},
        )

    return application
