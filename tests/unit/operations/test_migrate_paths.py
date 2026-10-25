"""`upgrade` and `current` on a scripted connection: the lock protocol and every refusal.

The shipped Alembic scripts are real (the head and the known revisions come from them); the
connection, the revision Alembic reads from it and `command.upgrade` are doubles that record what
the migration does, in order. The SQL and Alembic itself are proven against PostgreSQL by
`tests/integration/operations/test_migrate.py`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from alembic.config import Config
from sqlalchemy.exc import DBAPIError

from agent_context_platform.operations import migrate

pytestmark = pytest.mark.unit

ALEMBIC_INI = Path(__file__).parents[3] / "alembic.ini"
FIRST_REVISION = "20260823_0001"
SECRET = "migrator-secret"  # what a driver message could carry; never in a MigrationError
LOCK = f"SELECT pg_advisory_lock(:key) {migrate.MIGRATION_LOCK_KEY}"
UNLOCK = f"SELECT pg_advisory_unlock(:key) {migrate.MIGRATION_LOCK_KEY}"


class Driver(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(f"server said no to {SECRET}")
        self.sqlstate = sqlstate


def driver_error(sqlstate: str) -> DBAPIError:
    return DBAPIError(f"SELECT '{SECRET}'", {}, Driver(sqlstate))


class Connection:
    """A sync connection double: the database's revision and a log of what was done to it."""

    def __init__(self, revision: tuple[str, ...] = ()) -> None:
        self.revision = revision
        self.log: list[str] = []
        self.fail: dict[str, DBAPIError] = {}  # statement prefix -> error raised on execute

    def execute(self, statement: Any, parameters: dict[str, Any] | None = None) -> None:
        sql = str(statement)
        self.log.append(" ".join([sql, *(str(value) for value in (parameters or {}).values())]))
        for prefix, error in self.fail.items():
            if sql.startswith(prefix):
                raise error

    def commit(self) -> None:
        self.log.append("commit")

    def rollback(self) -> None:
        self.log.append("rollback")

    def invalidate(self) -> None:
        self.log.append("invalidate")


class AsyncConnection:
    def __init__(self, sync: Connection) -> None:
        self.sync = sync

    async def run_sync(self, function: Callable[..., Any], *args: Any) -> Any:
        return function(self.sync, *args)


class Engine:
    """`AsyncEngine.connect()` handing out the one scripted connection."""

    def __init__(self, sync: Connection) -> None:
        self.sync = sync
        self.connections = 0

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        self.connections += 1
        self.sync.log.append("connect")
        try:
            yield AsyncConnection(self.sync)
        finally:
            self.sync.log.append("disconnect")


@pytest.fixture
def alembic(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Alembic's revision read and `command.upgrade`, against the scripted connection."""
    state: dict[str, Any] = {"upgrades": [], "lands_at": None, "upgrade_error": None}

    def configure(connection: Connection) -> Any:
        def heads() -> tuple[str, ...]:
            connection.log.append("read revision")
            return connection.revision

        return SimpleNamespace(get_current_heads=heads)

    def upgrade(config: Config, revision: str) -> None:
        connection: Connection = config.attributes["connection"]
        state["upgrades"].append(revision)
        connection.log.append(f"alembic upgrade {revision}")
        if state["upgrade_error"] is not None:
            raise state["upgrade_error"]
        connection.revision = (revision,) if state["lands_at"] is None else state["lands_at"]

    monkeypatch.setattr(migrate, "MigrationContext", SimpleNamespace(configure=configure))
    monkeypatch.setattr(migrate, "command", SimpleNamespace(upgrade=upgrade))
    return state


def config() -> Config:
    return migrate.alembic_config(ALEMBIC_INI)


HEAD = migrate.image_head(config())


def upgrade(connection: Connection, **options: Any) -> migrate.MigrationReport:
    return asyncio.run(migrate.upgrade(Engine(connection), config(), **options))  # type: ignore[arg-type]


def acquired(timeout: str) -> list[str]:
    return [
        f"SELECT set_config('lock_timeout', :timeout, false) {timeout}",
        LOCK,
        "RESET lock_timeout",
        "commit",  # the lock is session-level: it survives the commit
    ]


RELEASED = ["rollback", UNLOCK, "commit"]


def test_an_empty_database_is_upgraded_to_the_head_under_the_lock(
    alembic: dict[str, Any],
) -> None:
    connection = Connection()

    report = upgrade(connection)

    assert report == migrate.MigrationReport(None, HEAD, HEAD, applied=True)
    assert alembic["upgrades"] == [HEAD]
    # The lock wait is bounded and reset before Alembic runs; the lock is released afterwards.
    assert connection.log == [
        "connect",
        *acquired("600000ms"),
        "read revision",
        "commit",
        f"alembic upgrade {HEAD}",
        "commit",
        "read revision",
        "commit",
        *RELEASED,
        "disconnect",
    ]


def test_an_older_schema_moves_forward_and_a_re_run_changes_nothing(
    alembic: dict[str, Any],
) -> None:
    connection = Connection((FIRST_REVISION,))

    first = upgrade(connection, expect_head=HEAD, lock_timeout_seconds=0.2)
    connection.log.clear()
    again = upgrade(connection)

    assert first == migrate.MigrationReport(FIRST_REVISION, HEAD, HEAD, applied=True)
    assert again == migrate.MigrationReport(HEAD, HEAD, HEAD, applied=False)
    assert alembic["upgrades"] == [HEAD]  # the no-op re-run never invokes Alembic
    assert connection.log == [
        "connect",
        *acquired("600000ms"),
        "read revision",
        "commit",
        "read revision",
        "commit",
        *RELEASED,
        "disconnect",
    ]


@pytest.mark.parametrize(("seconds", "timeout"), [(0.2, "200ms"), (0.0001, "1ms"), (5, "5000ms")])
def test_the_lock_wait_is_whole_milliseconds_and_never_zero(
    alembic: dict[str, Any], seconds: float, timeout: str
) -> None:
    connection = Connection((HEAD,))

    upgrade(connection, lock_timeout_seconds=seconds)

    assert connection.log[1:5] == acquired(timeout)


@pytest.mark.parametrize(
    "revision", [("29990101_0001",), (FIRST_REVISION, HEAD)], ids=["newer", "two-heads"]
)
def test_a_revision_the_image_does_not_ship_is_refused_and_the_lock_released(
    alembic: dict[str, Any], revision: tuple[str, ...]
) -> None:
    connection = Connection(revision)

    with pytest.raises(migrate.MigrationError) as raised:
        upgrade(connection)

    assert raised.value.code == "unknown_revision" and raised.value.usage_error
    assert str(raised.value) == (
        f"the database is at revision {','.join(revision)}, which this image does not ship "
        f"(its head is {HEAD}); refusing to downgrade"
    )
    assert alembic["upgrades"] == []  # never a downgrade, never an upgrade
    assert connection.log[-4:] == [*RELEASED, "disconnect"]


@pytest.mark.parametrize(("lands_at", "named"), [(("x",), "x"), ((), "no revision")])
def test_an_upgrade_that_does_not_land_on_the_head_fails_and_releases_the_lock(
    alembic: dict[str, Any], lands_at: tuple[str, ...], named: str
) -> None:
    alembic["lands_at"] = lands_at
    connection = Connection()

    with pytest.raises(migrate.MigrationError) as raised:
        upgrade(connection)

    assert raised.value.code == "head_mismatch" and not raised.value.usage_error
    assert str(raised.value) == (
        f"the database is at {named} after the upgrade, not the image's head {HEAD}"
    )
    assert connection.log[-4:] == [*RELEASED, "disconnect"]


def test_a_failing_migration_still_releases_the_lock(alembic: dict[str, Any]) -> None:
    alembic["upgrade_error"] = RuntimeError("a migration step failed")
    connection = Connection()

    with pytest.raises(RuntimeError, match="a migration step failed"):
        upgrade(connection)

    assert connection.log[-5:] == [f"alembic upgrade {HEAD}", *RELEASED, "disconnect"]


def test_a_held_lock_times_out_without_migrating_or_driver_detail(
    alembic: dict[str, Any],
) -> None:
    connection = Connection()
    connection.fail["SELECT pg_advisory_lock"] = driver_error("55P03")

    with pytest.raises(migrate.MigrationError) as raised:
        upgrade(connection)

    assert raised.value.code == "lock_timeout" and not raised.value.usage_error
    assert str(raised.value) == "another migration held the migration lock for too long"
    assert raised.value.__cause__ is None and raised.value.__suppress_context__
    assert SECRET not in str(raised.value)
    assert alembic["upgrades"] == []
    # The failed transaction is rolled back; there is no lock to release.
    assert connection.log == ["connect", *acquired("600000ms")[:2], "rollback", "disconnect"]


def test_any_other_lock_failure_propagates_unchanged(alembic: dict[str, Any]) -> None:
    connection = Connection()
    error = driver_error("08006")  # the connection dropped while waiting
    connection.fail["SELECT pg_advisory_lock"] = error

    with pytest.raises(DBAPIError) as raised:
        upgrade(connection)

    assert raised.value is error
    assert alembic["upgrades"] == []
    assert connection.log[-2:] == ["rollback", "disconnect"]


def test_an_unlock_failure_invalidates_the_connection_and_keeps_the_report(
    alembic: dict[str, Any],
) -> None:
    connection = Connection((HEAD,))
    connection.fail["SELECT pg_advisory_unlock"] = driver_error("08006")

    report = upgrade(connection)

    assert report == migrate.MigrationReport(HEAD, HEAD, HEAD, applied=False)
    # Closing the server session releases the lock; a possibly locked one never goes back.
    assert connection.log[-4:] == ["rollback", UNLOCK, "invalidate", "disconnect"]


@pytest.mark.parametrize(
    ("revision", "expected"),
    [
        ((), migrate.CurrentReport(None, HEAD, "empty")),
        ((FIRST_REVISION,), migrate.CurrentReport(FIRST_REVISION, HEAD, "behind")),
        ((HEAD,), migrate.CurrentReport(HEAD, HEAD, "at_head")),
        (("29990101_0001",), migrate.CurrentReport("29990101_0001", HEAD, "unknown")),
    ],
)
def test_current_reads_the_revision_without_a_lock_or_a_write(
    alembic: dict[str, Any], revision: tuple[str, ...], expected: migrate.CurrentReport
) -> None:
    connection = Connection(revision)
    engine = Engine(connection)

    report = asyncio.run(migrate.current(engine, config()))  # type: ignore[arg-type]

    assert report == expected
    assert connection.log == ["connect", "read revision", "commit", "disconnect"]
    assert engine.connections == 1 and alembic["upgrades"] == []
