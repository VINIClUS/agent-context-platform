"""Integration tests: the consumer fixtures in ``tests/fixtures/ingestion`` match the service.

Each fixture request goes through the ASGI app over ``httpx2.ASGITransport`` (no
sockets) against the real ``IngestionService``, PostgreSQL and S3. Fixtures are
ordered and stateful (see the fixture README), so the stack is module-scoped and
the fixtures run in file order. Only the volatile ``request_id`` is normalized.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx2
import pytest
from agent_context_sdk import RedactionPolicyV1
from argon2 import PasswordHasher
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.app import create_app
from agent_context_platform.content.blob_store import S3BlobStore
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    SqlProducerLookup,
)
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.settings import S3Settings, Settings

pytestmark = pytest.mark.integration

FIXTURES = sorted((Path(__file__).parents[2] / "fixtures" / "ingestion").glob("[0-9]*.json"))
PRODUCER_ID = "fixture-producer"
PREFIX = "fixture1"
TOKEN = f"{PREFIX}.{'f' * 43}"
MAX_BODY_BYTES = 4096
OUTAGE_FIXTURE = "transient-outage"
REQUEST_ID_PLACEHOLDER = "<request-id>"
_S3_VARIABLES = {
    "endpoint_url": "AGENT_CONTEXT_TEST_S3_ENDPOINT_URL",
    "region_name": "AGENT_CONTEXT_TEST_S3_REGION_NAME",
    "bucket_name": "AGENT_CONTEXT_TEST_S3_BUCKET_NAME",
    "access_key_id": "AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID",
    "secret_access_key": "AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY",
}


class Apps:
    """The healthy application and one whose ingestion database is unreachable."""

    def __init__(self, healthy: Any, unavailable: Any) -> None:
        self.healthy = healthy
        self.unavailable = unavailable


@pytest.fixture(scope="module")
def s3_settings() -> S3Settings:
    values = {name: os.getenv(variable) for name, variable in _S3_VARIABLES.items()}
    if not any(values.values()):
        pytest.skip("ingestion fixture tests require AGENT_CONTEXT_TEST_S3_* configuration")
    missing = [_S3_VARIABLES[name] for name, value in values.items() if not value]
    if missing:
        pytest.fail("partial S3 configuration; missing: " + ", ".join(missing))
    return S3Settings(**values)  # type: ignore[arg-type]


def _runtime(
    lookup_engine: AsyncEngine, service_engine: AsyncEngine, blob_store: S3BlobStore
) -> IngestionRuntime:
    return IngestionRuntime(
        authenticator=ProducerAuthenticator(
            SqlProducerLookup(session_factory(lookup_engine)),
            Argon2Verifier(
                time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2, max_queue_depth=8
            ),
        ),
        service=IngestionService(
            ContentService(blob_store, RedactionPolicyV1()), session_factory(service_engine)
        ),
        max_request_body_bytes=MAX_BODY_BYTES,
    )


@pytest.fixture(scope="module")
def apps(ledger_engine: AsyncEngine, postgres_dsn: str, s3_settings: S3Settings) -> Iterator[Apps]:
    """Register the fixture producer and serve the app as ``agent_context_api``."""
    now = datetime.now(UTC)

    async def register() -> None:
        async with ledger_engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO operations.registered_producers (producer_id, token_prefix, "
                    "token_verifier, scope, expires_at, created_at, updated_at) VALUES "
                    "(:id, :prefix, :verifier, 'events:ingest', :expires, :now, :now)"
                ),
                {
                    "id": PRODUCER_ID,
                    "prefix": PREFIX,
                    "verifier": PasswordHasher(time_cost=1, memory_cost=8, parallelism=1).hash(
                        TOKEN
                    ),
                    "expires": now + timedelta(days=1),
                    "now": now,
                },
            )

    asyncio.run(register())
    api_engine = create_async_engine(
        postgres_dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    # Nothing listens on port 1, so every ledger query fails fast as a connection error.
    down_engine = create_async_engine(
        "postgresql+psycopg://nobody@127.0.0.1:1/none",
        poolclass=NullPool,
        connect_args={"connect_timeout": 2},
    )
    blob_store = S3BlobStore.from_settings(s3_settings)
    settings = Settings(environment="test")
    yield Apps(
        create_app(settings, ingestion=_runtime(api_engine, api_engine, blob_store)),
        create_app(settings, ingestion=_runtime(api_engine, down_engine, blob_store)),
    )
    asyncio.run(api_engine.dispose())
    asyncio.run(down_engine.dispose())


def _normalized(body: Any) -> Any:
    if isinstance(body, dict) and "request_id" in body:
        return {**body, "request_id": REQUEST_ID_PLACEHOLDER}
    return body


@pytest.mark.parametrize("path", FIXTURES, ids=lambda path: path.stem)
def test_service_answers_each_fixture_exactly(apps: Apps, path: Path) -> None:
    fixture = json.loads(path.read_text(encoding="utf-8"))
    request = fixture["request"]
    headers = {
        name: f"Bearer {TOKEN}" if value == "Bearer <producer-token>" else value
        for name, value in request["headers"].items()
    }
    app = apps.unavailable if fixture["name"] == OUTAGE_FIXTURE else apps.healthy

    async def send() -> httpx2.Response:
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
        ) as client:
            return await client.request(
                request["method"],
                request["path"],
                content=json.dumps(request["body"], separators=(",", ":")).encode("utf-8"),
                headers=headers,
            )

    response = asyncio.run(send())

    expected = fixture["response"]
    assert response.status_code == expected["status"], response.text
    assert _normalized(response.json()) == expected["body"]
    contract_headers = {
        name: value
        for name, value in response.headers.items()
        if name in {"retry-after", "www-authenticate"}
    }
    assert contract_headers == expected["headers"]
