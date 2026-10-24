"""Successful memory-tool answers from the real stores, composed as the API role (PLATFORM-046).

The context-quality world (`evaluation.world`) is seeded through the real ingestion service and
projection runner. The app then runs with `postgresql.dsn` set to a login that holds NO grant at
all and `postgresql.api_dsn` set to a LOGIN member of `agent_context_api`: a non-empty package can
only come from a composer built on the API-role engine, never on the main one.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from argon2 import PasswordHasher
from evaluation.world import REPOSITORY, EvalWorld
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.app import create_app
from agent_context_platform.mcp.auth import (
    MEMORY_READ_SCOPE,
    McpAuthenticator,
    McpAuthRuntime,
    McpTokenRecord,
    PrincipalCache,
    TokenBucketLimiter,
)
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.security.bearer import Argon2Verifier
from agent_context_platform.settings import Settings

from ..conftest import postgres_dsn  # noqa: F401  (fixture)
from ..projection.conftest import neo4j_integration_settings, projection_engine  # noqa: F401
from ..retrieval.conftest import _set_schema, api_login, wipe  # noqa: F401

pytestmark = pytest.mark.integration

BUDGET = {"max_items": 10, "max_bytes": 40_000, "max_tokens": 10_000}
TOKEN = f"mcp_seeded01.{'s' * 43}"
OTHER_TOKEN = f"mcp_seeded02.{'o' * 43}"
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)


def _auth_runtime() -> McpAuthRuntime:
    """In-memory tokens for two principals: the powerless main login never reads the table."""
    records = {
        token.partition(".")[0]: McpTokenRecord(
            token_id=uuid.uuid4(),
            token_verifier=_HASHER.hash(token),
            principal=principal,
            scopes=(MEMORY_READ_SCOPE,),
            expires_at=datetime.now(UTC) + timedelta(days=1),
            revoked_at=None,
        )
        for token, principal in ((TOKEN, "codex-reader"), (OTHER_TOKEN, "other-reader"))
    }

    async def lookup(prefix: str) -> McpTokenRecord | None:
        return records.get(prefix)

    verifier = Argon2Verifier(
        time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2, max_queue_depth=8
    )
    return McpAuthRuntime(
        authenticator=McpAuthenticator(
            lookup,
            verifier,
            PrincipalCache(
                b"k" * 32,
                ttl=timedelta(seconds=30),
                max_entries=8,
                clock=lambda: datetime.now(UTC),
            ),
        ),
        limiter=TokenBucketLimiter(rate_per_second=1000.0, burst=1000),
    )


def output_schema(tool: str) -> dict[str, Any]:
    path = f"schemas/v1/mcp.{tool}.output.v1.json"
    loaded: dict[str, Any] = json.loads(
        resources.files("agent_context_sdk").joinpath(path).read_text("utf-8")
    )
    return loaded


def raw_call(
    client: TestClient, name: str, arguments: dict[str, Any], token: str = TOKEN
) -> dict[str, Any]:
    meta = {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
    }
    response = client.post(
        "/mcp",
        headers={
            "accept": "application/json, text/event-stream",
            "content-type": "application/json",
            "mcp-protocol-version": "2026-07-28",
            "mcp-method": "tools/call",
            "mcp-name": name,
            "authorization": f"Bearer {token}",
        },
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments, "_meta": meta},
        },
    )
    assert response.status_code == 200, response.text
    result: dict[str, Any] = response.json()["result"]
    return result


def error_code(result: dict[str, Any]) -> str:
    assert result["isError"] is True
    code: str = json.loads(result["content"][0]["text"])["error"]["code"]
    return code


_S3_ENV = ("ENDPOINT_URL", "REGION_NAME", "BUCKET_NAME", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY")


@pytest.fixture(scope="module")
def graph_schema() -> Iterator[None]:
    asyncio.run(_set_schema(present=True))
    try:
        yield
    finally:
        asyncio.run(_set_schema(present=False))


@pytest.fixture(scope="module")
def commit(
    postgres_dsn: str,  # noqa: F811
    projection_engine: AsyncEngine,  # noqa: F811
    api_login: str,  # noqa: F811
    graph_schema: None,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[str]:
    """Seed the world once; yield its first indexed commit."""

    async def seed(tmp: Path) -> str:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)
            world = EvalWorld(postgres_dsn, projection_engine, store, api_login)
            try:
                await world.reset()
                await world.seed(tmp)
                report = await world.project()
                assert report.dead_lettered == 0, report
                return world.revisions.c1
            finally:
                await world.close()

    async def clean() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)

    yield asyncio.run(seed(tmp_path_factory.mktemp("mcp-world")))
    asyncio.run(clean())


@pytest.fixture(scope="module")
def powerless_login(
    postgres_dsn: str,  # noqa: F811
    projection_engine: AsyncEngine,  # noqa: F811
) -> Iterator[str]:
    """A LOGIN role with no grant and no role membership: it cannot read anything retrieval needs."""
    name = f"mcp_none_{uuid.uuid4().hex[:8]}"
    password = uuid.uuid4().hex

    async def run(statement: str) -> None:
        async with projection_engine.begin() as connection:
            await connection.execute(text(statement))

    asyncio.run(run(f"CREATE ROLE {name} LOGIN PASSWORD '{password}'"))
    try:
        yield (
            make_url(postgres_dsn)
            .set(username=name, password=password)
            .render_as_string(hide_password=False)
        )
    finally:
        asyncio.run(run(f"DROP ROLE {name}"))


@pytest.fixture(scope="module")
def client(commit: str, powerless_login: str, api_login: str) -> Iterator[TestClient]:  # noqa: F811
    s3 = {}
    for name in _S3_ENV:
        value = os.environ.get(f"AGENT_CONTEXT_TEST_S3_{name}")
        if value is None:
            pytest.skip(f"AGENT_CONTEXT_TEST_S3_{name} is required")
        s3[name.lower()] = value
    neo4j = neo4j_integration_settings()
    assert neo4j.password is not None
    settings = Settings(
        environment="test",
        postgresql={"dsn": powerless_login, "api_dsn": api_login},
        neo4j={
            "uri": str(neo4j.uri),
            "username": neo4j.username,
            "password": neo4j.password.get_secret_value(),
            "database": neo4j.database,
        },
        s3=s3,
    )
    with TestClient(
        create_app(settings, mcp_auth=_auth_runtime()), base_url="http://127.0.0.1:8000"
    ) as http:
        yield http


def call(client: TestClient, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    result = raw_call(client, name, arguments)
    assert result.get("isError", False) is False, result["content"][0]["text"]
    return result


def test_repository_overview_composes_a_real_package_through_the_api_role(
    client: TestClient, commit: str
) -> None:
    result = call(
        client,
        "repository_overview",
        {"repository_id": REPOSITORY, "commit_id": commit, "budget": BUDGET},
    )

    structured = result["structuredContent"]
    jsonschema.Draft202012Validator(output_schema("repository_overview")).validate(structured)
    assert structured["items"], structured
    assert structured["scope"] == {"kind": "repository", "repository_id": REPOSITORY}
    assert all(item["evidence"] for item in structured["items"])
    # Lexical-only (no model directory): the vector leg is skipped, not reported as a failure.
    assert "vector_search_unavailable" not in structured["warnings"]


def test_context_for_task_answers_lexically_without_a_vector_warning(
    client: TestClient, commit: str
) -> None:
    result = call(
        client,
        "context_for_task",
        {
            "repository_id": REPOSITORY,
            "revision": {"kind": "commit", "commit_id": commit},
            "task": "where is add implemented",
            "budget": BUDGET,
        },
    )

    structured = result["structuredContent"]
    jsonschema.Draft202012Validator(output_schema("context_for_task")).validate(structured)
    assert structured["items"]
    assert "vector_search_unavailable" not in structured["warnings"]
    assert json.dumps(structured).count('"content_trust": "untrusted_evidence"') == len(
        structured["items"]
    )


def test_a_tampered_or_foreign_cursor_is_refused_end_to_end(
    client: TestClient, commit: str
) -> None:
    arguments = {
        "repository_id": REPOSITORY,
        "commit_id": commit,
        "budget": {"max_items": 1, "max_bytes": 40_000, "max_tokens": 10_000},
    }
    first = call(client, "repository_overview", arguments)["structuredContent"]
    cursor = first["next_cursor"]
    assert first["truncated"] is True and cursor

    second = call(client, "repository_overview", {**arguments, "cursor": cursor})
    assert second["structuredContent"]["items"] != first["items"]  # the next page

    prefix, payload, signature = cursor.split(".")
    flipped = "A" if payload[-2] != "A" else "B"
    tampered = f"{prefix}.{payload[:-2]}{flipped}{payload[-1]}.{signature}"
    for bad in (tampered, cursor + "x"):
        result = raw_call(client, "repository_overview", {**arguments, "cursor": bad})
        assert error_code(result) == "cursor_invalid"
    # Another principal's request, or another query, cannot continue this cursor.
    foreign = raw_call(client, "repository_overview", {**arguments, "cursor": cursor}, OTHER_TOKEN)
    assert error_code(foreign) == "cursor_mismatch"
    other = {**arguments, "budget": {"max_items": 2, "max_bytes": 40_000, "max_tokens": 10_000}}
    result = raw_call(client, "repository_overview", {**other, "cursor": cursor})
    assert error_code(result) == "cursor_mismatch"
