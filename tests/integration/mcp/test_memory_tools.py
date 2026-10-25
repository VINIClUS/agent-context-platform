"""The memory tools wired from settings to the real stores, as the API role (PLATFORM-046).

The app builds the composer itself (`app._build_composer`) from a DSN that logs in as a member of
`agent_context_api` only, so every read the tools make is checked against the grants that role
really holds (PLATFORM-055). Composition quality is PLATFORM-043's to test; here an unknown scope
must come back as its typed, content-free tool error, which proves the freshness, ledger and graph
readers all ran through the API role without a permission or wiring failure. One delivered event
gives the projection a snapshot; without one every tool answers `projection_not_ready`.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from argon2 import PasswordHasher
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.app import create_app
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.settings import Settings

from ..projection.conftest import neo4j_integration_settings
from ..retrieval.search_harness import SearchWorld

pytestmark = pytest.mark.integration

_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)
PREFIX = "mcp_tools0001"
TOKEN = f"{PREFIX}.{'t' * 43}"
COMMIT = "b" * 40
BUDGET = {"max_items": 5, "max_bytes": 20_000, "max_tokens": 5_000}
_SERVICE_ENV = {
    "neo4j": ("URI", "USERNAME", "PASSWORD", "DATABASE"),
    "s3": ("ENDPOINT_URL", "REGION_NAME", "BUCKET_NAME", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"),
}
CALLS: dict[str, dict[str, Any]] = {
    "context_for_task": {
        "repository_id": "repo-missing",
        "revision": {"kind": "commit", "commit_id": COMMIT},
        "task": "anything",
        "budget": BUDGET,
    },
    "repository_overview": {"repository_id": "repo-missing", "commit_id": COMMIT, "budget": BUDGET},
    "symbol_context": {
        "repository_id": "repo-missing",
        "commit_id": COMMIT,
        "symbol_name": "parse",
        "budget": BUDGET,
    },
    "decision_history": {
        "scope": {"kind": "project", "project_id": "project-missing"},
        "query": "anything",
        "valid_at": "2026-09-01T00:00:00Z",
        "budget": BUDGET,
    },
    "failure_history": {
        "repository_id": "repo-missing",
        "query": "anything",
        "valid_from": "2026-08-01T00:00:00Z",
        "valid_to": "2026-09-01T00:00:00Z",
        "budget": BUDGET,
    },
}


def _service_settings() -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    for section, names in _SERVICE_ENV.items():
        for name in names:
            value = os.environ.get(f"AGENT_CONTEXT_TEST_{section.upper()}_{name}")
            if value is None:
                pytest.skip(f"AGENT_CONTEXT_TEST_{section.upper()}_{name} is required")
            found.setdefault(section, {})[name.lower()] = value
    return found


@pytest.fixture(scope="module")
def api_login(postgres_dsn: str, owner_engine: AsyncEngine) -> Iterator[str]:
    """A LOGIN role whose only privileges come from `agent_context_api`; dropped before the
    module's downgrade (roles are cluster-global)."""
    name = f"mcp_api_{uuid.uuid4().hex[:8]}"
    password = uuid.uuid4().hex

    async def run(statement: str) -> None:
        engine = create_async_engine(postgres_dsn, poolclass=NullPool, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                await connection.execute(text(statement))
        finally:
            await engine.dispose()

    asyncio.run(run(f"CREATE ROLE {name} LOGIN PASSWORD '{password}' IN ROLE agent_context_api"))
    try:
        url = make_url(postgres_dsn).set(username=name, password=password)
        yield url.render_as_string(hide_password=False)
    finally:
        asyncio.run(run(f"DROP ROLE IF EXISTS {name}"))


@pytest.fixture(scope="module")
def token(owner_engine: AsyncEngine) -> str:
    async def insert() -> None:
        async with owner_engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO operations.mcp_tokens (token_id, token_prefix, token_verifier, "
                    "principal, scopes, expires_at, created_at) VALUES "
                    "(:id, :prefix, :verifier, 'codex-reader', ARRAY['memory:read'], :expires, "
                    ":now)"
                ),
                {
                    "id": uuid.uuid4(),
                    "prefix": PREFIX,
                    "verifier": _HASHER.hash(TOKEN),
                    "expires": datetime.now(UTC) + timedelta(days=1),
                    "now": datetime.now(UTC),
                },
            )

    asyncio.run(insert())
    return TOKEN


@pytest.fixture(scope="module")
def delivered(postgres_dsn: str, owner_engine: AsyncEngine) -> None:
    """One ingested event whose outbox row is delivered: the projection has a snapshot."""

    async def seed() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            world = SearchWorld(postgres_dsn, owner_engine, store)
            try:
                await world.ingest("some session text", "c-seed", project="project-seed")
            finally:
                await world.close()
        async with owner_engine.begin() as connection:
            await connection.execute(
                text("UPDATE projection.outbox SET status = 'delivered', delivered_at = now()")
            )

    asyncio.run(seed())


@pytest.fixture(scope="module")
def client(api_login: str, token: str, delivered: None) -> Iterator[TestClient]:
    services = _service_settings()
    settings = Settings(
        environment="test",
        postgresql={"dsn": api_login},
        neo4j=services["neo4j"],
        s3=services["s3"],
    )
    with TestClient(create_app(settings), base_url="http://127.0.0.1:8000") as http:
        yield http


def call(client: TestClient, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    response = client.post(
        "/mcp",
        headers={
            "accept": "application/json, text/event-stream",
            "content-type": "application/json",
            "mcp-protocol-version": "2026-07-28",
            "mcp-method": "tools/call",
            "mcp-name": name,
            "authorization": f"Bearer {TOKEN}",
        },
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments,
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": {},
                },
            },
        },
    )
    assert response.status_code == 200, response.text
    assert "mcp-session-id" not in response.headers
    result: dict[str, Any] = response.json()["result"]
    return result


@pytest.mark.parametrize("tool", sorted(CALLS))
def test_each_tool_reads_through_the_api_role_and_reports_a_typed_error(
    client: TestClient, tool: str
) -> None:
    result = call(client, tool, CALLS[tool])

    assert result["isError"] is True
    error = json.loads(result["content"][0]["text"])["error"]
    # Past the freshness read (one event is delivered) to the scope check, which needs the graph:
    # never a permission failure (`context_unavailable`) or a crash (`internal_error`).
    assert error == {
        "code": "scope_not_found",
        "message": "the scope is unknown",
        "retryable": False,
    }


def test_an_unreachable_graph_is_unavailable_quickly_not_a_deadline(
    api_login: str, token: str, delivered: None
) -> None:
    services = _service_settings()
    unreachable = {**services["neo4j"], "uri": "bolt://127.0.0.1:1"}
    settings = Settings(
        environment="test",
        postgresql={"dsn": api_login},
        neo4j=unreachable,
        s3=services["s3"],
    )
    with TestClient(create_app(settings), base_url="http://127.0.0.1:8000") as http:
        started = time.monotonic()
        result = call(http, "repository_overview", CALLS["repository_overview"])
        elapsed = time.monotonic() - started

    error = json.loads(result["content"][0]["text"])["error"]
    assert (error["code"], error["retryable"]) == ("context_unavailable", True)
    assert elapsed < 8.0  # well inside the 10 s tool deadline


def test_an_exhausted_composer_pool_is_unavailable_not_a_crash(
    api_login: str, token: str, delivered: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agent_context_platform import app as app_module

    monkeypatch.setattr(app_module, "COMPOSER_POOL_SIZE", 1)
    monkeypatch.setattr(app_module, "COMPOSER_POOL_OVERFLOW", 0)
    monkeypatch.setattr(app_module, "COMPOSER_POOL_TIMEOUT_SECONDS", 0.5)
    engines: list[AsyncEngine] = []
    build = app_module._composer_engine

    def capture(settings: Settings) -> AsyncEngine:
        engines.append(build(settings))
        return engines[-1]

    monkeypatch.setattr(app_module, "_composer_engine", capture)
    services = _service_settings()
    settings = Settings(
        environment="test",
        postgresql={"dsn": api_login},
        neo4j=services["neo4j"],
        s3=services["s3"],
    )
    with TestClient(create_app(settings), base_url="http://127.0.0.1:8000") as http:
        assert http.portal is not None
        (engine,) = engines
        held = http.portal.call(engine.connect().start)  # the pool's only connection, on its loop
        try:
            started = time.monotonic()
            result = call(http, "repository_overview", CALLS["repository_overview"])
            elapsed = time.monotonic() - started
        finally:
            http.portal.call(held.close)

    error = json.loads(result["content"][0]["text"])["error"]
    assert (error["code"], error["retryable"]) == ("context_unavailable", True)
    assert elapsed < 5.0
