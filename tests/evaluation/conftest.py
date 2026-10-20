"""Fixtures of the context-quality evaluation: a disposable database and the graph schema.

The evaluation is its own top-level test package, so it imports the integration suite's fixtures
(a fresh PostgreSQL database per module, migrated by `projection_engine`) instead of redefining them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest
from integration.conftest import postgres_dsn  # noqa: F401  (fixture)
from integration.projection.conftest import projection_engine  # noqa: F401  (fixture)
from integration.retrieval.conftest import _set_schema


@pytest.fixture(scope="module", autouse=True)
def evaluation_graph_schema() -> Iterator[None]:
    """The projection schema (unique keys, vector index), created once and dropped again."""
    asyncio.run(_set_schema(present=True))
    try:
        yield
    finally:
        asyncio.run(_set_schema(present=False))
