"""Shared fixtures for the content persistence integration suite.

Builds a real ``ContentService`` / ``OrphanSweeper`` stack against the live
Postgres and S3-compatible (Garage) test services started by
``scripts/test-services.sh``: the schema is migrated to head with the same
Alembic entry point the platform ships, and blob storage goes through a real
``boto3`` client, not a fake.

Postgres: reuses ``tests/integration/conftest.py``'s module-scoped
``postgres_dsn`` fixture (a fresh, disposable database per test module) and
layers a schema migration on top of it. The ``_run_alembic`` helper below is
intentionally a local copy of ``tests/integration/test_migrations.py``'s
helper of the same name, not a cross-import: test modules should not depend
on each other's internals, and this one is only a few lines. The migration
is torn down (downgrade to base) in a ``finally`` block because
``agent_context_api`` / ``agent_context_projector`` are cluster-global
roles -- ``test_migrations.py``'s own prerequisite check asserts neither
role exists yet, and would fail (turning CI red on an unrelated file) if a
module here left one behind. ``content_engine`` uses ``NullPool``: each test
function drives its own ``asyncio.run()`` (its own event loop), and a
pooled ``AsyncEngine`` shared across loops is exactly the "attached to a
different loop" failure SQLAlchemy's docs warn about.

S3: settings are read from the same ``AGENT_CONTEXT_TEST_S3_*`` variables as
``tests/integration/content/test_s3_contract.py``, with the same skip/fail
semantics (copied, not imported, for the same reason as above). A raw
``boto3`` client is built the same way ``S3BlobStore.from_settings`` builds
its own internal client -- duplicated here (rather than reaching into
``S3BlobStore``'s private ``_client`` attribute) because ``OrphanSweeper``
also needs a raw client, and sharing one real client between the store and
the sweeper avoids opening a second connection pool to Garage for no
reason.

Ledger rows: ``ledger/repository.py`` is owned by PLATFORM-020 and is off
limits to this task, so ``insert_minimal_event`` / ``insert_content_ref``
hand-build the minimal constraint-valid ``ledger.event_streams`` /
``ledger.events`` / ``ledger.event_content_refs`` rows this suite's
foreign-key and crash-matrix tests need, over raw SQL. They are exposed as
fixtures (not plain importable functions) so ``test_service.py`` and
``test_crash_matrix.py`` share them without importing each other.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import boto3
import pytest
from agent_context_sdk import ContentRefV1, RedactionPolicyV1
from alembic.config import Config
from botocore.config import Config as BotoConfig
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from agent_context_platform.content.blob_store import S3BlobStore
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.settings import S3Settings
from alembic import command

PROJECT_ROOT = Path(__file__).parents[3]

_REQUIRED_S3_VARS = {
    "endpoint_url": "AGENT_CONTEXT_TEST_S3_ENDPOINT_URL",
    "region_name": "AGENT_CONTEXT_TEST_S3_REGION_NAME",
    "bucket_name": "AGENT_CONTEXT_TEST_S3_BUCKET_NAME",
    "access_key_id": "AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID",
    "secret_access_key": "AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY",
}


# ---------------------------------------------------------------------------
# PostgreSQL: migrate to head, hand out a session factory, migrate back down
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def content_engine(postgres_dsn: str) -> Iterator[AsyncEngine]:
    """A module-scoped engine bound to a database migrated to head.

    Downgrades to base in a ``finally`` before this fixture's disposable
    database is itself dropped by ``postgres_dsn`` -- see the module
    docstring for why that ordering is load-bearing for later test modules.
    """
    asyncio.run(_migrate(postgres_dsn, command.upgrade, "head"))
    engine = create_async_engine(postgres_dsn, poolclass=NullPool)
    try:
        yield engine
    finally:
        asyncio.run(_teardown(engine, postgres_dsn))


async def _teardown(engine: AsyncEngine, dsn: str) -> None:
    await engine.dispose()
    await _migrate(dsn, command.downgrade, "base")


async def _migrate(dsn: str, operation: Callable[[Config, str], None], revision: str) -> None:
    engine = create_async_engine(dsn, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await _run_alembic(connection, operation, revision)
            await connection.commit()
    finally:
        await engine.dispose()


async def _run_alembic(
    connection: AsyncConnection,
    operation: Callable[[Config, str], None],
    revision: str,
) -> None:
    def run(sync_connection: Connection) -> None:
        config = Config(PROJECT_ROOT / "alembic.ini")
        config.attributes["connection"] = sync_connection
        operation(config, revision)

    await connection.run_sync(run)


@pytest.fixture(scope="module")
def content_session_factory(
    content_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """The same ``async_sessionmaker`` shape ``db.session_factory`` builds."""
    return session_factory(content_engine)


# ---------------------------------------------------------------------------
# S3 (Garage): settings, a raw boto3 client, and an S3BlobStore over it
# ---------------------------------------------------------------------------


def _s3_settings() -> S3Settings:
    configured = {name: os.getenv(variable) for name, variable in _REQUIRED_S3_VARS.items()}
    present = [name for name, value in configured.items() if value is not None]
    if not present:
        pytest.skip("content integration tests require AGENT_CONTEXT_TEST_S3_* configuration")
    missing = [variable for name, variable in _REQUIRED_S3_VARS.items() if not configured[name]]
    if missing:
        pytest.fail("partial S3 contract configuration; missing: " + ", ".join(missing))
    return S3Settings(**configured)  # type: ignore[arg-type]


def _build_s3_client(settings: S3Settings) -> Any:
    """Build a raw boto3 client the same way ``S3BlobStore.from_settings`` does."""
    access_key_id = (
        None if settings.access_key_id is None else settings.access_key_id.get_secret_value()
    )
    secret_access_key = (
        None
        if settings.secret_access_key is None
        else settings.secret_access_key.get_secret_value()
    )
    session = boto3.session.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=settings.region_name,
    )
    return session.client(
        "s3",
        endpoint_url=None if settings.endpoint_url is None else str(settings.endpoint_url),
        region_name=settings.region_name,
        config=BotoConfig(
            connect_timeout=settings.connect_timeout_seconds,
            read_timeout=settings.read_timeout_seconds,
            retries={"total_max_attempts": settings.max_attempts, "mode": "standard"},
            s3={"addressing_style": settings.addressing_style},
        ),
    )


@pytest.fixture(scope="module")
def s3_settings() -> S3Settings:
    return _s3_settings()


@pytest.fixture(scope="module")
def s3_client(s3_settings: S3Settings) -> Any:
    return _build_s3_client(s3_settings)


@pytest.fixture(scope="module")
def blob_store(s3_settings: S3Settings, s3_client: Any) -> S3BlobStore:
    """An ``S3BlobStore`` sharing ``s3_client`` with ``OrphanSweeper`` fixtures."""
    return S3BlobStore(s3_settings, client=s3_client)


# ---------------------------------------------------------------------------
# ContentService
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def content_service(blob_store: S3BlobStore) -> ContentService:
    return ContentService(blob_store, RedactionPolicyV1())


# ---------------------------------------------------------------------------
# Minimal ledger rows (hand-built: ledger/repository.py is off limits)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MinimalEvent:
    """Identifiers for one committed, constraint-valid event."""

    stream_id: str
    event_id: UUID


@pytest.fixture
def insert_minimal_event() -> Callable[[AsyncSession], Awaitable[MinimalEvent]]:
    """Insert and commit one minimal ``event_streams`` + ``events`` row pair.

    Call this on a session with no transaction already open -- it manages
    and commits its own transaction, so the returned ``event_id`` is durably
    visible to whatever transaction the caller opens next (e.g. the
    ``ContentService.attach`` + ``event_content_refs`` insert under test).
    Every call generates fresh identifiers so concurrent tests sharing one
    module-scoped database never collide.
    """

    async def _insert(session: AsyncSession) -> MinimalEvent:
        stream_id = f"stream-{uuid.uuid4().hex}"
        event_id = uuid.uuid4()
        now = datetime.now(UTC)
        digest = hashlib.sha256(event_id.bytes).hexdigest()
        async with session.begin():
            await session.execute(
                text(
                    "INSERT INTO ledger.event_streams "
                    "(stream_id, last_sequence, status, created_at, updated_at) "
                    "VALUES (:stream_id, 0, 'active', :now, :now)"
                ),
                {"stream_id": stream_id, "now": now},
            )
            await session.execute(
                text(
                    "INSERT INTO ledger.events "
                    "(event_id, event_type, schema_version, stream_id, stream_sequence, "
                    " producer_id, idempotency_key, occurred_at, observed_at, recorded_at, "
                    " producer, context, payload, redaction, payload_sha256, "
                    " previous_event_sha256, event_sha256) "
                    "VALUES (:event_id, 'test.minimal.v1', '1.0.0', :stream_id, 1, "
                    " 'test-producer', :idempotency_key, :now, :now, :now, "
                    " '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, :digest, "
                    " NULL, :digest)"
                ),
                {
                    "event_id": event_id,
                    "stream_id": stream_id,
                    "idempotency_key": f"idem-{uuid.uuid4().hex}",
                    "now": now,
                    "digest": digest,
                },
            )
        return MinimalEvent(stream_id=stream_id, event_id=event_id)

    return _insert


@pytest.fixture
def insert_content_ref() -> Callable[[AsyncSession, UUID, ContentRefV1], Awaitable[None]]:
    """Insert one ``ledger.event_content_refs`` row for a resolved ``ContentRefV1``.

    Mirrors the column mapping ``ledger/repository.py`` (PLATFORM-020, off
    limits to this task) would perform when it appends an event's content
    references; duplicated here only so this module's FK-ordering and
    crash-matrix tests can exercise the real ``event_content_refs`` foreign
    keys without depending on that module. Does not manage a transaction:
    call this inside the caller's own open transaction.
    """

    async def _insert(session: AsyncSession, event_id: UUID, ref: ContentRefV1) -> None:
        await session.execute(
            text(
                "INSERT INTO ledger.event_content_refs "
                "(event_id, content_id, content_sha256, media_type, uncompressed_bytes, "
                " disposition, storage, inline_id, object_key, encoding) "
                "VALUES (:event_id, :content_id, :content_sha256, :media_type, "
                " :uncompressed_bytes, :disposition, :storage, :inline_id, :object_key, "
                " :encoding)"
            ),
            {
                "event_id": event_id,
                "content_id": ref.content_id,
                "content_sha256": ref.content_sha256,
                "media_type": ref.media_type,
                "uncompressed_bytes": ref.uncompressed_bytes,
                "disposition": ref.disposition.value,
                "storage": ref.storage.value,
                "inline_id": ref.inline_id,
                "object_key": ref.object_key,
                "encoding": ref.encoding,
            },
        )

    return _insert
