"""Fixtures for the end-to-end suites.

The e2e tests reuse the integration services and fixtures (`scripts/test-services.sh`): the
disposable PostgreSQL database per module and the migrated ledger engine. `tests/` is not a
package root, so the shared fixtures are imported by name (`pytest_plugins` is not allowed in a
non-root conftest). Without the `AGENT_CONTEXT_TEST_*` variables every test skips, as the
integration suite does.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest
from integration.conftest import postgres_dsn  # noqa: F401  (fixture)
from integration.indexing.conftest import ledger_engine  # noqa: F401  (fixture)
from integration.projection.conftest import neo4j_integration_settings

from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.schema import SCHEMA_STATEMENTS, ensure_schema


async def _set_schema(*, present: bool) -> None:
    async with Neo4jStore(neo4j_integration_settings()) as store:

        async def wipe(tx: Neo4jTransaction) -> None:
            await tx.run("MATCH (n) DETACH DELETE n", parameters={})

        await store.execute_write(wipe)
        if present:
            await ensure_schema(store)
            return

        async def drop(tx: Neo4jTransaction) -> None:
            for statement in SCHEMA_STATEMENTS:
                kind = "CONSTRAINT" if "CONSTRAINT" in statement.query else "INDEX"
                await tx.run(f"DROP {kind} {statement.name} IF EXISTS", parameters={})  # type: ignore[arg-type]

        await store.execute_write(drop)


@pytest.fixture(scope="module", autouse=True)
def e2e_graph_schema() -> Iterator[None]:
    """An empty projection schema for the module, and a schema-free database afterwards.

    `tests/integration/projection/test_neo4j_schema.py` needs a database with no projection
    schema, and the e2e suite runs before it.
    """
    asyncio.run(_set_schema(present=True))
    try:
        yield
    finally:
        asyncio.run(_set_schema(present=False))
