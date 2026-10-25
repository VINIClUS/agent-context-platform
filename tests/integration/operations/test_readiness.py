"""`GET /health/ready` against the real PostgreSQL, Neo4j and Garage services (PLATFORM-049)."""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Connection, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.app import create_app
from agent_context_platform.operations import migrate, readiness
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.settings import Settings
from alembic import command

pytestmark = pytest.mark.integration

ALEMBIC_INI = Path(__file__).parents[3] / "alembic.ini"
# Only for the test that must see every check answer, however loaded the machine is.
LOADED_TIMEOUT_SECONDS = 30.0
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


# The platform's own tables: autovacuum and autoanalyze write system catalogs (pg_class,
# pg_statistic) at any moment, so database-wide counters are not the probe's writes.
PLATFORM_SCHEMAS = ("ledger", "projection", "catalog", "operations", "retrieval")
VERSION_TABLE = "public.alembic_version"


@dataclass(frozen=True)
class TableCounters:
    writes: dict[str, int]  # inserted + updated + deleted tuples per table (HOT is a subset)
    version_reads: int  # sequential and index scans of the version table


async def _quiet_table_counters(dsn: str) -> TableCounters:
    """Per-table counters of the platform's tables, once nothing else can still flush them.

    A backend (an autovacuum worker included) flushes its counters when it exits (or at most a
    second after a transaction), so wait until this connection is the only one on the database.
    """
    engine = create_async_engine(dsn, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            for _ in range(600):
                others = await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                    )
                )
                await connection.commit()
                if others == 0:
                    break
                await asyncio.sleep(0.05)
            assert others == 0
            await connection.execute(text("SELECT pg_stat_clear_snapshot()"))
            rows = (
                await connection.execute(
                    text(
                        "SELECT schemaname || '.' || relname, n_tup_ins + n_tup_upd + n_tup_del, "
                        "coalesce(seq_scan, 0) + coalesce(idx_scan, 0) "
                        "FROM pg_stat_user_tables "
                        "WHERE schemaname = ANY(:schemas) "
                        "OR schemaname || '.' || relname = :version_table"
                    ),
                    {"schemas": list(PLATFORM_SCHEMAS), "version_table": VERSION_TABLE},
                )
            ).all()
            await connection.commit()
    finally:
        await engine.dispose()
    reads = {str(row[0]): int(row[2]) for row in rows}
    return TableCounters({str(row[0]): int(row[1]) for row in rows}, reads.get(VERSION_TABLE, 0))


def test_the_probe_writes_nothing(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three complete probes (every check answered ok) change no row of the platform's tables.

    The 200 proves each check actually ran to the end; the probe gets a generous timeout of its
    own so a slow Neo4j or S3 answer under full-suite load cannot turn that into a timeout. What
    is asserted is unchanged: a full probe, and not a single written tuple.
    """
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))
    settings = _settings(migrated, services)
    head = migrate.image_head(migrate.alembic_config(ALEMBIC_INI))
    assert settings.postgresql.dsn is not None

    before = asyncio.run(_quiet_table_counters(migrated))
    # Not vacuous: every platform table is counted, the ones readiness touches included.
    assert set(readiness.API_READ_TABLES) <= before.writes.keys()
    assert len(before.writes) >= 20
    engine = readiness.readiness_engine(settings.postgresql.dsn, LOADED_TIMEOUT_SECONDS)
    store = Neo4jStore(settings.neo4j)
    probe = readiness.build_probe(
        settings, engine, store, head, timeout_seconds=LOADED_TIMEOUT_SECONDS
    )

    async def close() -> None:
        await store.close()
        await engine.dispose()

    with TestClient(create_app(settings, readiness=probe)) as client:
        try:
            for _ in range(3):
                response = client.get("/health/ready")
                assert (response.status_code, response.json()) == (200, {"status": "ready"})
        finally:
            # The driver and the engine belong to the client's event loop.
            client.portal.call(close)  # type: ignore[union-attr]
    after = asyncio.run(_quiet_table_counters(migrated))

    changed = {
        table: (before.writes.get(table), count)
        for table, count in after.writes.items()
        if before.writes.get(table) != count
    }
    assert changed == {}
    assert after.writes.keys() == before.writes.keys()
    # The probes really read the schema head: one scan of the version table each, at least.
    assert after.version_reads - before.version_reads >= 3


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


@pytest.fixture
def api_login(migrated: str) -> Iterator[str]:
    """A LOGIN member of ``agent_context_api`` with no grant of its own, as infra creates it."""
    name = f"agent_context_api_login_{uuid.uuid4().hex[:12]}"
    password = uuid.uuid4().hex

    async def run(*statements: str) -> None:
        engine = create_async_engine(migrated, poolclass=NullPool)
        try:
            async with engine.begin() as connection:
                for statement in statements:
                    await connection.execute(text(statement))
        finally:
            await engine.dispose()

    asyncio.run(
        run(
            f"CREATE ROLE {name} LOGIN INHERIT PASSWORD '{password}'",
            f"GRANT agent_context_api TO {name}",
        )
    )
    try:
        url = make_url(migrated).set(username=name, password=password)
        yield url.render_as_string(hide_password=False)
    finally:
        asyncio.run(run(f"DROP ROLE {name}"))


def test_the_least_privileged_api_login_is_ready_with_only_the_migrated_grants(
    api_login: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))

    assert _ready(_settings(api_login, services))[:2] == (200, {"status": "ready"})


@pytest.mark.parametrize("table", readiness.API_READ_TABLES)
def test_an_api_login_missing_one_read_grant_is_not_ready(
    api_login: str,
    migrated: str,
    services: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    table: str,
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))

    async def grant(statement: str) -> None:
        engine = create_async_engine(migrated, poolclass=NullPool)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement))
        finally:
            await engine.dispose()

    asyncio.run(grant(f"REVOKE SELECT ON {table} FROM agent_context_api"))
    try:
        assert _ready(_settings(api_login, services))[:2] == (
            503,
            _not_ready(schema="forbidden"),
        )
    finally:
        asyncio.run(grant(f"GRANT SELECT ON {table} TO agent_context_api"))


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


class BlackholeProxy:
    """A TCP proxy to PostgreSQL that can silently drop every byte, like a vanished peer."""

    def __init__(self, host: str, port: int) -> None:
        self._target = (host, port)
        self.blackhole = threading.Event()
        self._closed = threading.Event()
        self._sockets: list[socket.socket] = []
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(0.2)
        self.port = int(self._listener.getsockname()[1])
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while not self._closed.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            upstream = socket.create_connection(self._target)
            self._sockets += [client, upstream]
            for source, sink in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pump, args=(source, sink), daemon=True).start()

    def _pump(self, source: socket.socket, sink: socket.socket) -> None:
        with contextlib.suppress(OSError):
            while data := source.recv(65536):
                if not self.blackhole.is_set():
                    sink.sendall(data)

    def close(self) -> None:
        self._closed.set()
        self._listener.close()
        for item in self._sockets:
            with contextlib.suppress(OSError):
                item.close()


def test_a_blackholed_database_is_reported_within_the_bound_and_recovers(
    migrated: str, services: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))
    url = make_url(migrated)
    assert url.host is not None and url.port is not None
    proxy = BlackholeProxy(url.host, url.port)
    dsn = url.set(host="127.0.0.1", port=proxy.port).render_as_string(hide_password=False)
    try:
        with TestClient(create_app(_settings(dsn, services))) as client:
            assert client.get("/health/ready").status_code == 200

            proxy.blackhole.set()
            for _ in range(3):
                started = time.monotonic()
                response = client.get("/health/ready")
                elapsed = time.monotonic() - started
                assert response.status_code == 503
                assert response.json() == _not_ready(postgresql="timeout", schema="unknown")
                assert elapsed < 4.0

            proxy.blackhole.clear()
            deadline = time.monotonic() + 30
            status = 503
            while status != 200 and time.monotonic() < deadline:
                status = client.get("/health/ready").status_code
                if status != 200:
                    time.sleep(0.5)
            assert status == 200
    finally:
        proxy.blackhole.clear()
        proxy.close()
