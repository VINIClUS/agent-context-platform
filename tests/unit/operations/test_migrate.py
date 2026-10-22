"""Migration helpers without a database: config discovery, the image head and state rules."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest

from agent_context_platform.operations import migrate
from agent_context_platform.projection.verify import REBUILD_LOCK_KEY

pytestmark = pytest.mark.unit

PROJECT_ROOT = Path(__file__).parents[3]
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
SHIPPED = frozenset({"20260823_0001", "20260928_0001", "20260929_0001", "20261002_0001"})


def test_the_config_comes_from_the_argument_then_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(ALEMBIC_INI))
    assert migrate.alembic_config().config_file_name == str(ALEMBIC_INI)

    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(tmp_path / "missing.ini"))
    with pytest.raises(migrate.MigrationError) as raised:
        migrate.alembic_config()
    assert raised.value.code == "alembic_config_missing"
    assert migrate.alembic_config(ALEMBIC_INI).config_file_name == str(ALEMBIC_INI)


def test_the_config_defaults_to_the_working_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv(migrate.ALEMBIC_CONFIG_ENV, raising=False)
    monkeypatch.chdir(PROJECT_ROOT)
    assert migrate.alembic_config().config_file_name == "alembic.ini"

    monkeypatch.chdir(tmp_path)
    with pytest.raises(migrate.MigrationError):
        migrate.alembic_config()


def test_the_image_head_is_the_single_shipped_head() -> None:
    head = migrate.image_head(migrate.alembic_config(ALEMBIC_INI))

    assert head in SHIPPED
    assert head == max(SHIPPED)


def _scripts(tmp_path: Path, revisions: dict[str, str | None]) -> Path:
    versions = tmp_path / "alembic" / "versions"
    versions.mkdir(parents=True)
    (tmp_path / "alembic" / "script.py.mako").write_text("")
    (tmp_path / "alembic" / "env.py").write_text("")
    for revision, down in revisions.items():
        (versions / f"{revision}.py").write_text(
            f"revision = {revision!r}\ndown_revision = {down!r}\n"
            "def upgrade():\n    pass\ndef downgrade():\n    pass\n"
        )
    ini = tmp_path / "alembic.ini"
    ini.write_text("[alembic]\nscript_location = %(here)s/alembic\n")
    return ini


def test_branched_scripts_have_no_single_head(tmp_path: Path) -> None:
    ini = _scripts(tmp_path, {"a": None, "b": "a", "c": "a"})

    with pytest.raises(migrate.MigrationError) as raised:
        migrate.image_head(migrate.alembic_config(ini))

    assert raised.value.code == "ambiguous_head"


def test_unloadable_scripts_are_reported_as_missing(tmp_path: Path) -> None:
    ini = tmp_path / "alembic.ini"
    ini.write_text("[alembic]\n")

    with pytest.raises(migrate.MigrationError) as raised:
        migrate.image_head(migrate.alembic_config(ini))

    assert raised.value.code == "alembic_config_missing"


@pytest.mark.parametrize(
    ("current", "expected"),
    [
        ((), migrate.CurrentReport(None, "c", "empty")),
        (("c",), migrate.CurrentReport("c", "c", "at_head")),
        (("a",), migrate.CurrentReport("a", "c", "behind")),
        (("z",), migrate.CurrentReport("z", "c", "unknown")),
        (("a", "c"), migrate.CurrentReport("a,c", "c", "unknown")),
    ],
)
def test_the_database_state_against_the_head(
    current: tuple[str, ...], expected: migrate.CurrentReport
) -> None:
    assert migrate._state(current, "c", frozenset({"a", "b", "c"})) == expected


class NoEngine:
    def connect(self) -> None:
        raise AssertionError("must not connect")


def test_an_expected_head_mismatch_never_connects() -> None:
    with pytest.raises(migrate.MigrationError) as raised:
        asyncio.run(
            migrate.upgrade(
                NoEngine(),  # type: ignore[arg-type]
                migrate.alembic_config(ALEMBIC_INI),
                expect_head="20260823_0001",
            )
        )

    assert raised.value.code == "expected_head_mismatch"
    assert raised.value.usage_error is False


def test_only_unsafe_refusals_and_a_missing_config_are_usage_errors() -> None:
    assert migrate.MigrationError("unknown_revision", "x").usage_error
    assert migrate.MigrationError("alembic_config_missing", "x").usage_error
    assert not migrate.MigrationError("head_mismatch", "x").usage_error
    assert not migrate.MigrationError("lock_timeout", "x").usage_error
    assert not migrate.MigrationError("interrupted", "x").usage_error


def test_the_largest_lock_timeout_fits_postgresql() -> None:
    # lock_timeout is an int of milliseconds.
    assert migrate.MAX_LOCK_TIMEOUT_SECONDS * 1000 <= 2**31 - 1


def test_the_lock_key_is_stable_and_distinct_from_the_rebuild_lock() -> None:
    assert (
        int.from_bytes(
            hashlib.sha256(b"agent-context.schema.migrate").digest()[:8],
            "big",
            signed=True,
        )
        == migrate.MIGRATION_LOCK_KEY
    )
    assert migrate.MIGRATION_LOCK_KEY != REBUILD_LOCK_KEY
