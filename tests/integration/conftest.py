"""Shared fixtures for the integration test suite.

Every PostgreSQL-backed integration test module gets its own disposable
database, created from ``AGENT_CONTEXT_TEST_POSTGRES_DSN`` and dropped at
teardown. This keeps test modules from seeing each other's schemas and lets
the whole suite pass repeatedly against one already-running service set,
regardless of run order.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

POSTGRES_DSN_VARIABLE = "AGENT_CONTEXT_TEST_POSTGRES_DSN"


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    """Yield a DSN pointing at a fresh, disposable PostgreSQL database.

    The database is created under the cluster referenced by
    ``AGENT_CONTEXT_TEST_POSTGRES_DSN`` before the first test in a module
    runs, and dropped after the last one. Skip semantics match every other
    PostgreSQL-backed integration test: skip with the same message when the
    base DSN is not configured.
    """
    base_dsn = os.environ.get(POSTGRES_DSN_VARIABLE)
    if base_dsn is None:
        pytest.skip(f"{POSTGRES_DSN_VARIABLE} is required for PostgreSQL tests")

    base_url = make_url(base_dsn)
    database_name = f"agent_context_test_{uuid.uuid4().hex}"

    asyncio.run(_create_database(base_url, database_name))
    try:
        yield base_url.set(database=database_name).render_as_string(hide_password=False)
    finally:
        asyncio.run(_drop_database(base_url, database_name))


async def _create_database(base_url: URL, database_name: str) -> None:
    engine = create_async_engine(base_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    finally:
        await engine.dispose()


async def _drop_database(base_url: URL, database_name: str) -> None:
    engine = create_async_engine(base_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            )
    finally:
        await engine.dispose()
