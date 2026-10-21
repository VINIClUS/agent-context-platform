"""Schema migrations for the platform-migrate role (PLATFORM-049).

`agent-context db upgrade` runs Alembic programmatically on one dedicated connection that holds
a session-level PostgreSQL advisory lock for the whole run, so two migrate jobs serialize instead
of interleaving: the second one waits, then finds the schema at head and changes nothing.

The image's head is the single head of the Alembic scripts it ships (`alembic.ini` and
`alembic/`, located through ``AGENT_CONTEXT_ALEMBIC_CONFIG``). Upgrading only ever moves forward
to that head: a database at a revision the image does not know (a newer image migrated it) is a
refusal, never a downgrade, and the resulting revision is asserted to be exactly the head.

Errors carry a stable code and never a DSN, a password or a driver message.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.util.exc import CommandError
from sqlalchemy import Connection, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from alembic import command

ALEMBIC_CONFIG_ENV: Final = "AGENT_CONTEXT_ALEMBIC_CONFIG"
DEFAULT_ALEMBIC_CONFIG: Final = "alembic.ini"
DEFAULT_LOCK_TIMEOUT_SECONDS: Final = 600
# PostgreSQL's lock_timeout is an int of milliseconds.
MAX_LOCK_TIMEOUT_SECONDS: Final = 2_147_483.0

# One key for every migration run, derived from a fixed name (as the rebuild lock).
MIGRATION_LOCK_NAME: Final = "agent-context.schema.migrate"
MIGRATION_LOCK_KEY: Final = int.from_bytes(
    hashlib.sha256(MIGRATION_LOCK_NAME.encode()).digest()[:8], "big", signed=True
)

_LOCK_NOT_AVAILABLE: Final = "55P03"

MigrationErrorCode = Literal[
    "alembic_config_missing",
    "ambiguous_head",
    "expected_head_mismatch",
    "unknown_revision",
    "lock_timeout",
    "head_mismatch",
    "interrupted",
]
# Exit code 2 for an unsafe refusal (the database is ahead of the image) or a missing Alembic
# configuration (a usage error), 1 otherwise.
_USAGE_ERRORS: Final = frozenset({"unknown_revision", "alembic_config_missing"})


class MigrationError(Exception):
    """A migration that did not reach the image's head; the message holds no secret."""

    def __init__(self, code: MigrationErrorCode, message: str) -> None:
        super().__init__(message)
        self.code: MigrationErrorCode = code

    @property
    def usage_error(self) -> bool:
        return self.code in _USAGE_ERRORS


@dataclass(frozen=True, slots=True)
class MigrationReport:
    """The revision before and after one upgrade; ``applied`` is False for a no-op re-run."""

    before: str | None
    after: str
    head: str
    applied: bool


@dataclass(frozen=True, slots=True)
class CurrentReport:
    """The database's revision against the image's head."""

    revision: str | None
    head: str
    state: Literal["at_head", "behind", "empty", "unknown"]


def alembic_config(path: str | os.PathLike[str] | None = None) -> Config:
    """The shipped Alembic configuration: ``path``, else ``$AGENT_CONTEXT_ALEMBIC_CONFIG``.

    The fallback ``alembic.ini`` is relative to the working directory (the repository root in
    development; the image sets the variable).
    """
    resolved = Path(path or os.environ.get(ALEMBIC_CONFIG_ENV) or DEFAULT_ALEMBIC_CONFIG)
    if not resolved.is_file():
        raise MigrationError("alembic_config_missing", "the Alembic configuration was not found")
    return Config(resolved)


def image_head(config: Config) -> str:
    """The single head revision of the shipped scripts."""
    try:
        heads = ScriptDirectory.from_config(config).get_heads()
    except CommandError:
        raise MigrationError(
            "alembic_config_missing", "the Alembic scripts could not be loaded"
        ) from None
    if len(heads) != 1:
        raise MigrationError("ambiguous_head", f"the image ships {len(heads)} Alembic heads")
    return heads[0]


def _known_revisions(config: Config, head: str) -> frozenset[str]:
    """Every revision on the path from base to ``head``: the only ones an upgrade starts from."""
    script = ScriptDirectory.from_config(config)
    return frozenset(item.revision for item in script.walk_revisions(base="base", head=head))


def _current(connection: Connection) -> tuple[str, ...]:
    heads = MigrationContext.configure(connection).get_current_heads()
    connection.commit()
    return tuple(heads)


def _state(current: tuple[str, ...], head: str, known: frozenset[str]) -> CurrentReport:
    if not current:
        return CurrentReport(None, head, "empty")
    if len(current) != 1 or current[0] not in known:
        return CurrentReport(",".join(current), head, "unknown")
    return CurrentReport(current[0], head, "at_head" if current[0] == head else "behind")


def _acquire(connection: Connection, lock_timeout_seconds: float) -> None:
    """Wait for the migration lock for at most ``lock_timeout_seconds``.

    ``lock_timeout`` bounds the advisory-lock wait; it is reset before Alembic runs so the
    migrations themselves keep the server default.
    """
    milliseconds = max(1, round(lock_timeout_seconds * 1000))
    connection.execute(
        text("SELECT set_config('lock_timeout', :timeout, false)"),
        {"timeout": f"{milliseconds}ms"},
    )
    try:
        connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
    except DBAPIError as error:
        connection.rollback()
        if getattr(error.orig, "sqlstate", None) == _LOCK_NOT_AVAILABLE:
            raise MigrationError(
                "lock_timeout", "another migration held the migration lock for too long"
            ) from None
        raise
    connection.execute(text("RESET lock_timeout"))
    connection.commit()  # the lock is session-level: it survives the commit


def _release(connection: Connection) -> None:
    try:
        connection.rollback()
        connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": MIGRATION_LOCK_KEY})
        connection.commit()
    except DBAPIError:
        # Closing the server session releases the lock; never hand a locked one back to a pool.
        connection.invalidate()


def _upgrade(
    connection: Connection, config: Config, head: str, lock_timeout_seconds: float
) -> MigrationReport:
    known = _known_revisions(config, head)
    _acquire(connection, lock_timeout_seconds)
    try:
        before = _state(_current(connection), head, known)
        if before.state == "unknown":
            raise MigrationError(
                "unknown_revision",
                f"the database is at revision {before.revision}, which this image does not "
                f"ship (its head is {head}); refusing to downgrade",
            )
        applied = before.state != "at_head"
        if applied:
            config.attributes["connection"] = connection
            command.upgrade(config, head)
            connection.commit()
        after = _current(connection)
        if after != (head,):
            raise MigrationError(
                "head_mismatch",
                f"the database is at {','.join(after) or 'no revision'} after the upgrade, "
                f"not the image's head {head}",
            )
        return MigrationReport(before.revision, head, head, applied)
    finally:
        _release(connection)


async def upgrade(
    engine: AsyncEngine,
    config: Config,
    *,
    expect_head: str | None = None,
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> MigrationReport:
    """Upgrade to the image's head under the migration lock and assert the result."""
    head = image_head(config)
    if expect_head is not None and expect_head != head:
        raise MigrationError(
            "expected_head_mismatch",
            f"--expect-head {expect_head} is not the image's head {head}",
        )
    async with engine.connect() as connection:
        return await connection.run_sync(_upgrade, config, head, lock_timeout_seconds)


async def current(engine: AsyncEngine, config: Config) -> CurrentReport:
    """Read the database's revision; takes no lock and writes nothing."""
    head = image_head(config)
    known = _known_revisions(config, head)
    async with engine.connect() as connection:
        revisions = await connection.run_sync(_current)
    return _state(revisions, head, known)
