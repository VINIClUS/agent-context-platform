"""`agent-context db upgrade|current` against a real PostgreSQL (PLATFORM-049).

Every test starts from an empty disposable database and downgrades to base afterwards: the two
managed roles the first revision creates are cluster-global.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import Connection, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.operations import migrate
from alembic import command

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).parents[3]
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
FIRST_REVISION = "20260823_0001"


def _config() -> Config:
    return migrate.alembic_config(ALEMBIC_INI)


def _head() -> str:
    return migrate.image_head(_config())


def _engine(dsn: str) -> AsyncEngine:
    return create_async_engine(dsn, poolclass=NullPool)


@pytest.fixture
def dsn(postgres_dsn: str) -> Iterator[str]:
    yield postgres_dsn

    async def downgrade() -> None:
        engine = _engine(postgres_dsn)
        try:
            async with engine.connect() as connection:

                def run(sync_connection: Connection) -> None:
                    config = _config()
                    config.attributes["connection"] = sync_connection
                    command.downgrade(config, "base")

                await connection.run_sync(run)
                await connection.execute(text("DROP TABLE IF EXISTS alembic_version"))
                await connection.commit()
        finally:
            await engine.dispose()

    asyncio.run(downgrade())


async def _upgrade(dsn: str, **options: object) -> migrate.MigrationReport:
    engine = _engine(dsn)
    try:
        return await migrate.upgrade(engine, _config(), **options)  # type: ignore[arg-type]
    finally:
        await engine.dispose()


async def _scalar(dsn: str, statement: str) -> object:
    engine = _engine(dsn)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(text(statement))
    finally:
        await engine.dispose()


def test_upgrade_from_an_empty_database_reaches_the_image_head(dsn: str) -> None:
    report = asyncio.run(_upgrade(dsn))

    assert report == migrate.MigrationReport(None, _head(), _head(), applied=True)
    assert asyncio.run(_scalar(dsn, "SELECT version_num FROM alembic_version")) == _head()
    assert asyncio.run(_scalar(dsn, "SELECT to_regclass('ledger.events') IS NOT NULL"))


def test_a_re_run_is_a_no_op(dsn: str) -> None:
    asyncio.run(_upgrade(dsn))

    report = asyncio.run(_upgrade(dsn, expect_head=_head()))

    assert report == migrate.MigrationReport(_head(), _head(), _head(), applied=False)


def test_an_older_schema_is_upgraded_forward(dsn: str) -> None:
    async def partial() -> None:
        engine = _engine(dsn)
        try:
            async with engine.connect() as connection:

                def run(sync_connection: Connection) -> None:
                    config = _config()
                    config.attributes["connection"] = sync_connection
                    command.upgrade(config, FIRST_REVISION)

                await connection.run_sync(run)
                await connection.commit()
        finally:
            await engine.dispose()

    asyncio.run(partial())

    report = asyncio.run(_upgrade(dsn))

    assert (report.before, report.after, report.applied) == (FIRST_REVISION, _head(), True)


def test_concurrent_runs_serialize_on_the_migration_lock(dsn: str) -> None:
    async def scenario() -> tuple[list[migrate.MigrationReport], bool]:
        holder = _engine(dsn)
        try:
            async with holder.connect() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_lock(:key)"), {"key": migrate.MIGRATION_LOCK_KEY}
                )
                await connection.commit()
                runs = [asyncio.create_task(_upgrade(dsn)) for _ in range(2)]
                waiting = False
                for _ in range(200):
                    count = await connection.scalar(
                        text(
                            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                            "AND NOT granted AND database = (SELECT oid FROM pg_database "
                            "WHERE datname = current_database())"
                        )
                    )
                    await connection.commit()
                    if count == 2:
                        waiting = True
                        break
                    await asyncio.sleep(0.05)
                assert not any(run.done() for run in runs)
                assert (
                    await connection.scalar(text("SELECT to_regclass('alembic_version')")) is None
                )
                await connection.execute(
                    text("SELECT pg_advisory_unlock(:key)"), {"key": migrate.MIGRATION_LOCK_KEY}
                )
                await connection.commit()
                return list(await asyncio.gather(*runs)), waiting
        finally:
            await holder.dispose()

    reports, waiting = asyncio.run(scenario())

    assert waiting
    assert sorted(report.applied for report in reports) == [False, True]
    assert {report.after for report in reports} == {_head()}


def test_a_held_lock_times_out_without_migrating(dsn: str) -> None:
    async def scenario() -> None:
        holder = _engine(dsn)
        try:
            async with holder.connect() as connection:
                await connection.execute(
                    text("SELECT pg_advisory_lock(:key)"), {"key": migrate.MIGRATION_LOCK_KEY}
                )
                await connection.commit()
                with pytest.raises(migrate.MigrationError) as raised:
                    await _upgrade(dsn, lock_timeout_seconds=0.2)
                assert raised.value.code == "lock_timeout"
        finally:
            await holder.dispose()

    asyncio.run(scenario())

    assert asyncio.run(_scalar(dsn, "SELECT to_regclass('alembic_version')")) is None


def test_an_expected_head_mismatch_fails_before_connecting(dsn: str) -> None:
    with pytest.raises(migrate.MigrationError) as raised:
        asyncio.run(_upgrade(dsn, expect_head=FIRST_REVISION))

    assert raised.value.code == "expected_head_mismatch"
    assert not raised.value.refusal
    assert asyncio.run(_scalar(dsn, "SELECT to_regclass('alembic_version')")) is None


def test_a_database_ahead_of_the_image_is_refused_not_downgraded(dsn: str) -> None:
    asyncio.run(_upgrade(dsn))

    async def advance(revision: str) -> None:
        engine = _engine(dsn)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text("UPDATE alembic_version SET version_num = :revision"),
                    {"revision": revision},
                )
        finally:
            await engine.dispose()

    asyncio.run(advance("29991231_0001"))
    try:
        with pytest.raises(migrate.MigrationError) as raised:
            asyncio.run(_upgrade(dsn))
        assert raised.value.code == "unknown_revision"
        assert raised.value.refusal
        assert asyncio.run(_scalar(dsn, "SELECT to_regclass('ledger.events') IS NOT NULL"))
        assert asyncio.run(
            _scalar(dsn, f"SELECT pg_try_advisory_lock({migrate.MIGRATION_LOCK_KEY})")
        )
    finally:
        asyncio.run(advance(_head()))


def test_current_reports_each_state(dsn: str) -> None:
    async def current() -> migrate.CurrentReport:
        engine = _engine(dsn)
        try:
            return await migrate.current(engine, _config())
        finally:
            await engine.dispose()

    assert asyncio.run(current()) == migrate.CurrentReport(None, _head(), "empty")
    asyncio.run(_upgrade(dsn))
    assert asyncio.run(current()) == migrate.CurrentReport(_head(), _head(), "at_head")


def _cli(dsn: str, *args: str) -> tuple[int, str, str]:
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        list(args),
        env={
            "AGENT_CONTEXT_POSTGRESQL__DSN": None,
            "AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN": dsn,
            migrate.ALEMBIC_CONFIG_ENV: str(ALEMBIC_INI),
        },
    )
    return result.exit_code, result.stdout, result.stderr


def test_the_cli_upgrades_reports_and_never_prints_the_dsn(dsn: str) -> None:
    password = make_url(dsn).password
    assert password

    code, out, err = _cli(dsn, "db", "current")
    assert code == cli.EXIT_FAILED
    assert out.strip() == f"revision none head {_head()}: empty"

    code, out, err = _cli(dsn, "db", "upgrade", "--expect-head", _head(), "--json")
    assert code == cli.EXIT_OK, err
    assert json.loads(out) == {"before": None, "after": _head(), "head": _head(), "applied": True}

    code, out, _ = _cli(dsn, "db", "upgrade")
    assert (code, out.strip()) == (cli.EXIT_OK, f"schema already at head {_head()}")

    code, out, _ = _cli(dsn, "db", "current", "--json")
    assert code == cli.EXIT_OK
    assert json.loads(out) == {"revision": _head(), "head": _head(), "state": "at_head"}

    code, out, err = _cli(dsn, "db", "upgrade", "--expect-head", FIRST_REVISION)
    assert code == cli.EXIT_FAILED
    assert err.startswith("error: expected_head_mismatch: ")

    for text_ in (out, err):
        assert password not in text_ and make_url(dsn).host not in text_


def test_the_cli_reports_an_unreachable_database_by_class_only(dsn: str) -> None:
    unreachable = make_url(dsn).set(host="127.0.0.1", port=1).render_as_string(hide_password=False)

    code, out, err = _cli(unreachable, "db", "upgrade")

    assert code == cli.EXIT_FAILED
    assert out == ""
    assert err.startswith("error: ") and make_url(dsn).password not in err  # type: ignore[operator]
