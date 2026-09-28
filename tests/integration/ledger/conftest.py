"""Shared fixtures for the ledger integration test suite.

Builds the full authoritative schema -- every managed schema, table, role,
and grant -- by running the real Alembic migration against a disposable
database, the same way ``tests/integration/test_migrations.py`` does. Using
the real migration (not ad hoc ``metadata.create_all``) is what makes the
least-privilege tests in ``test_integrity.py`` meaningful:
``agent_context_api``/``agent_context_projector`` are cluster-global roles
created by the migration itself, not by SQLAlchemy metadata.
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
def ledger_engine(postgres_dsn: str) -> Iterator[AsyncEngine]:
    """Yield an engine bound to a disposable database at the migration head.

    Downgrades back to ``base`` at teardown, before the database itself is
    dropped by ``postgres_dsn``: the two managed roles are cluster-global,
    and leaking them would break other test modules run against the same
    live services.
    """
    engine = create_async_engine(postgres_dsn, poolclass=NullPool)

    async def upgrade() -> None:
        async with engine.connect() as connection:
            await _run_alembic(connection, command.upgrade, "head")
            await connection.commit()

    async def downgrade() -> None:
        async with engine.connect() as connection:
            await _run_alembic(connection, command.downgrade, "base")
            await connection.commit()
        await engine.dispose()

    asyncio.run(upgrade())
    yield engine
    asyncio.run(downgrade())


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
