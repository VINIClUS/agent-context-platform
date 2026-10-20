"""`GET /health/ready` against the real PostgreSQL, Neo4j and Garage services (PLATFORM-049)."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Connection, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.app import create_app
from agent_context_platform.operations import migrate
from agent_context_platform.settings import Settings
from alembic import command

pytestmark = pytest.mark.integration

ALEMBIC_INI = Path(__file__).parents[3] / "alembic.ini"
SERVICE_VARIABLES = (
    "AGENT_CONTEXT_TEST_NEO4J_URI",
    "AGENT_CONTEXT_TEST_NEO4J_USERNAME",
    "AGENT_CONTEXT_TEST_NEO4J_PASSWORD",
    "AGENT_CONTEXT_TEST_S3_ENDPOINT_URL",
    "AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID",
    "AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY",
    "AGENT_CONTEXT_TEST_S3_BUCKET_NAME",
)


@pytest.fixture(scope="module")
def services() -> dict[str, str]:
    missing = [name for name in SERVICE_VARIABLES if not os.environ.get(name)]
    if missing:
        pytest.skip(f"{', '.join(missing)} are required for readiness tests")
    return {name: os.environ[name] for name in SERVICE_VARIABLES}


@pytest.fixture(scope="module")
def migrated(postgres_dsn: str, services: dict[str, str]) -> Iterator[str]:
    async def upgrade() -> None:
        engine = create_async_engine(postgres_dsn, poolclass=NullPool)
        try:
            await migrate.upgrade(engine, migrate.alembic_config(ALEMBIC_INI))
        finally:
            await engine.dispose()

    async def downgrade() -> None:
        engine = create_async_engine(postgres_dsn, poolclass=NullPool)
        try:
            async with engine.connect() as connection:

                def run(sync_connection: Connection) -> None:
                    config = migrate.alembic_config(ALEMBIC_INI)
                    config.attributes["connection"] = sync_connection
                    command.downgrade(config, "base")

                await connection.run_sync(run)
                await connection.commit()
        finally:
            await engine.dispose()

    asyncio.run(upgrade())
    yield postgres_dsn
    asyncio.run(downgrade())


def _settings(dsn: str, services: dict[str, str], **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "environment": "test",
        "postgresql": {"dsn": dsn},
        "neo4j": {
            "uri": services["AGENT_CONTEXT_TEST_NEO4J_URI"],
            "username": services["AGENT_CONTEXT_TEST_NEO4J_USERNAME"],
            "password": services["AGENT_CONTEXT_TEST_NEO4J_PASSWORD"],
        },
        "s3": {
            "endpoint_url": services["AGENT_CONTEXT_TEST_S3_ENDPOINT_URL"],
            "access_key_id": services["AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID"],
            "secret_access_key": services["AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY"],
            "bucket_name": services["AGENT_CONTEXT_TEST_S3_BUCKET_NAME"],
        },
    }
    for section, fields in overrides.items():
        values[section] = {**values[section], **fields}
    return Settings(**values)


def _ready(settings: Settings) -> tuple[int, dict[str, Any], str]:
    with TestClient(create_app(settings)) as client:
        response = client.get("/health/ready")
    return response.status_code, response.json(), response.text


def _not_ready(**states: str) -> dict[str, Any]:
    components = {"neo4j": "ok", "postgresql": "ok", "s3": "ok", "schema": "ok", **states}
    return {"status": "not_ready", "components": dict(sorted(components.items()))}


def test_ready_when_every_service_is_reachable_and_the_schema_is_at_head(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))

    assert _ready(_settings(migrated, services))[:2] == (200, {"status": "ready"})


def test_the_probe_writes_nothing(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))

    async def counters() -> tuple[int, int]:
        engine = create_async_engine(migrated, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT pg_stat_force_next_flush()"))
                row = (
                    await connection.execute(
                        text(
                            "SELECT xact_commit, tup_inserted + tup_updated + tup_deleted "
                            "FROM pg_stat_database WHERE datname = current_database()"
                        )
                    )
                ).one()
                return int(row[0]), int(row[1])
        finally:
            await engine.dispose()

    before = asyncio.run(counters())
    for _ in range(3):
        assert _ready(_settings(migrated, services))[0] == 200
    after = asyncio.run(counters())

    assert after[1] == before[1]


def test_a_schema_behind_the_image_is_not_ready(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))
    head = migrate.image_head(migrate.alembic_config(ALEMBIC_INI))

    async def set_version(revision: str) -> None:
        engine = create_async_engine(migrated, poolclass=NullPool)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text("UPDATE alembic_version SET version_num = :revision"),
                    {"revision": revision},
                )
        finally:
            await engine.dispose()

    asyncio.run(set_version("20260823_0001"))
    try:
        assert _ready(_settings(migrated, services))[:2] == (503, _not_ready(schema="mismatch"))
    finally:
        asyncio.run(set_version(head))


def test_an_api_role_without_select_on_alembic_version_is_not_ready(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))
    url = make_url(migrated).update_query_dict({"options": "-c role=agent_context_api"})
    dsn = url.render_as_string(hide_password=False)

    assert _ready(_settings(dsn, services))[:2] == (503, _not_ready(schema="forbidden"))


def test_unreachable_dependencies_are_reported_without_hosts_or_credentials(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))
    unreachable_pg = make_url(migrated).set(host="127.0.0.1", port=1)
    settings = _settings(
        unreachable_pg.render_as_string(hide_password=False),
        services,
        neo4j={"uri": "bolt://127.0.0.1:1"},
        s3={"bucket_name": "agent-context-missing-bucket"},
    )

    status, body, raw = _ready(settings)

    assert status == 503
    assert body == _not_ready(
        postgresql="unavailable", schema="unknown", neo4j="unavailable", s3="unavailable"
    )
    password = make_url(migrated).password
    assert password and password not in raw
    assert "127.0.0.1" not in raw and "agent-context-missing-bucket" not in raw


def test_wrong_credentials_are_forbidden(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))
    settings = _settings(
        migrated,
        services,
        neo4j={"password": "not-the-password"},
        s3={"secret_access_key": "0" * 64},
    )

    status, body, _ = _ready(settings)

    assert status == 503
    assert body == _not_ready(neo4j="forbidden", s3="forbidden")
