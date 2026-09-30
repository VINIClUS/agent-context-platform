"""Fixtures for the indexing integration tests: migrated schema, S3 and the ingestion service."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.settings import S3Settings
from alembic import command

PROJECT_ROOT = Path(__file__).parents[3]
_S3_VARIABLES = {
    "endpoint_url": "AGENT_CONTEXT_TEST_S3_ENDPOINT_URL",
    "region_name": "AGENT_CONTEXT_TEST_S3_REGION_NAME",
    "bucket_name": "AGENT_CONTEXT_TEST_S3_BUCKET_NAME",
    "access_key_id": "AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID",
    "secret_access_key": "AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY",
}


async def _alembic(
    connection: AsyncConnection, operation: Callable[[Config, str], None], revision: str
) -> None:
    def run(sync_connection: Connection) -> None:
        config = Config(PROJECT_ROOT / "alembic.ini")
        config.attributes["connection"] = sync_connection
        operation(config, revision)

    await connection.run_sync(run)


@pytest.fixture(scope="module")
def ledger_engine(postgres_dsn: str) -> Iterator[AsyncEngine]:
    """An owner engine on a disposable database at the migration head (downgraded at teardown)."""
    engine = create_async_engine(postgres_dsn, poolclass=NullPool)

    async def upgrade() -> None:
        async with engine.connect() as connection:
            await _alembic(connection, command.upgrade, "head")
            await connection.commit()

    async def downgrade() -> None:
        async with engine.connect() as connection:
            await _alembic(connection, command.downgrade, "base")
            await connection.commit()
        await engine.dispose()

    asyncio.run(upgrade())
    yield engine
    asyncio.run(downgrade())


@pytest.fixture(scope="module")
def s3_settings() -> S3Settings:
    values = {name: os.getenv(variable) for name, variable in _S3_VARIABLES.items()}
    if not any(values.values()):
        pytest.skip("indexing integration tests require AGENT_CONTEXT_TEST_S3_* configuration")
    missing = [_S3_VARIABLES[name] for name, value in values.items() if not value]
    if missing:
        pytest.fail("partial S3 configuration; missing: " + ", ".join(missing))
    return S3Settings(**values)  # type: ignore[arg-type]
