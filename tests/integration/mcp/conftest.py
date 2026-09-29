"""Fixtures for MCP authentication against the real migrated schema.

Builds the schema with the real Alembic migration (the roles and grants are part of what is
under test) in a disposable database, and downgrades to base at teardown because the two
managed roles are cluster-global.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from alembic import command

PROJECT_ROOT = Path(__file__).parents[3]


@pytest.fixture(scope="module")
def owner_engine(postgres_dsn: str) -> Iterator[AsyncEngine]:
    engine = create_async_engine(postgres_dsn, poolclass=NullPool)

    async def migrate(operation: Callable[[Config, str], None], revision: str) -> None:
        async with engine.connect() as connection:
            await _run_alembic(connection, operation, revision)
            await connection.commit()

    asyncio.run(migrate(command.upgrade, "head"))
    yield engine
    asyncio.run(migrate(command.downgrade, "base"))
    asyncio.run(engine.dispose())


@pytest.fixture
def api_engine(postgres_dsn: str, owner_engine: AsyncEngine) -> Iterator[AsyncEngine]:
    """The least-privileged runtime role, as the API process uses it."""
    engine = create_async_engine(
        postgres_dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    yield engine
    asyncio.run(engine.dispose())


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
