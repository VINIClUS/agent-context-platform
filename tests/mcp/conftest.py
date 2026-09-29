from __future__ import annotations

from collections.abc import AsyncIterator

import httpx2
import pytest
from mcp_helpers import mcp_client


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def client() -> AsyncIterator[httpx2.AsyncClient]:
    async with mcp_client() as http_client:
        yield http_client
