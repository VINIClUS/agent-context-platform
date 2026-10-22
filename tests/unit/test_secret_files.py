"""File-based secrets (`AGENT_CONTEXT_SECRETS_DIR`): precedence, refusals, nested names."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from agent_context_platform import settings as settings_module
from agent_context_platform.settings import (
    SECRET_FILE_NAMES,
    SECRETS_DIR_ENV,
    SecretFileError,
    Settings,
)

pytestmark = pytest.mark.unit

API_DSN = "postgresql+psycopg://api:file-secret@postgres/agent_context"
ENV_DSN = "postgresql+psycopg://api:env-secret@postgres/agent_context"


@pytest.fixture
def secrets_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in SECRET_FILE_NAMES:
        monkeypatch.delenv(name, raising=False)
    directory = tmp_path / "secrets"
    directory.mkdir(mode=0o700)
    monkeypatch.setenv(SECRETS_DIR_ENV, str(directory))
    return directory


def _write(directory: Path, name: str, value: str, mode: int = 0o400) -> Path:
    path = directory / name
    path.write_text(value)
    path.chmod(mode)
    return path


def test_every_secret_field_has_a_file_name_and_nothing_else_does() -> None:
    assert set(SECRET_FILE_NAMES) == {
        "AGENT_CONTEXT_POSTGRESQL__DSN",
        "AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN",
        "AGENT_CONTEXT_POSTGRESQL__API_DSN",
        "AGENT_CONTEXT_POSTGRESQL__ADMIN_DSN",
        "AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN",
        "AGENT_CONTEXT_NEO4J__PASSWORD",
        "AGENT_CONTEXT_S3__ACCESS_KEY_ID",
        "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY",
        "AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY",
    }


def test_nested_secrets_are_read_from_files_named_after_their_variables(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_POSTGRESQL__API_DSN", API_DSN + "\n")
    _write(secrets_dir, "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY", "s3-file-secret")
    _write(secrets_dir, "unrelated.txt", "ignored", mode=0o644)  # not ours: left alone
    monkeypatch.setenv("AGENT_CONTEXT_S3__ACCESS_KEY_ID", "GKfromenv")

    settings = Settings()

    assert settings.postgresql.api_dsn is not None
    assert settings.postgresql.api_dsn.get_secret_value().unicode_string() == API_DSN
    assert settings.s3.secret_access_key is not None
    assert settings.s3.secret_access_key.get_secret_value() == "s3-file-secret"
    assert settings.s3.access_key_id is not None  # the environment still works beside files
    assert settings.s3.access_key_id.get_secret_value() == "GKfromenv"
    assert "file-secret" not in repr(settings)


@pytest.mark.parametrize("env_value", [ENV_DSN, ""])  # an empty value is set, too
@pytest.mark.parametrize(
    "env_name", ["AGENT_CONTEXT_POSTGRESQL__API_DSN", "agent_context_postgresql__api_dsn"]
)
def test_a_variable_set_both_in_the_environment_and_as_a_file_is_refused(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch, env_value: str, env_name: str
) -> None:
    # A stale variable left after a rotation would silently keep the old key (and keep it in
    # /proc/<pid>/environ): refuse to start instead, naming the variable but never a value.
    _write(secrets_dir, "AGENT_CONTEXT_POSTGRESQL__API_DSN", API_DSN)
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_POSTGRESQL__API_DSN") as error:
        Settings()

    assert "both" in str(error.value)
    assert "file-secret" not in str(error.value) and "env-secret" not in str(error.value)


@pytest.mark.parametrize("mode", [0o440, 0o404, 0o600 | 0o020, 0o402])
def test_a_secret_file_readable_or_writable_by_others_is_refused(
    secrets_dir: Path, mode: int
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_NEO4J__PASSWORD", "neo4j-file-secret", mode=mode)

    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_NEO4J__PASSWORD") as error:
        Settings()

    assert "neo4j-file-secret" not in str(error.value)


@pytest.mark.parametrize("mode", [0o400, 0o600])
def test_owner_only_files_are_accepted(secrets_dir: Path, mode: int) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_NEO4J__PASSWORD", "neo4j-file-secret", mode=mode)
    password = Settings().neo4j.password
    assert password is not None and password.get_secret_value() == "neo4j-file-secret"


@pytest.mark.skipif(os.geteuid() != 0, reason="changing a file's owner needs root")
def test_a_secret_file_owned_by_another_user_is_refused(secrets_dir: Path) -> None:
    path = _write(secrets_dir, "AGENT_CONTEXT_NEO4J__PASSWORD", "neo4j-file-secret")
    os.chown(path, 4242, 4242)

    with pytest.raises(SecretFileError, match="owner"):
        Settings()


def test_a_symlinked_secret_file_is_refused(secrets_dir: Path, tmp_path: Path) -> None:
    target = _write(tmp_path, "real-secret", "neo4j-file-secret")
    (secrets_dir / "AGENT_CONTEXT_NEO4J__PASSWORD").symlink_to(target)

    with pytest.raises(SecretFileError, match="regular file"):
        Settings()


@pytest.mark.parametrize("content", ["", "\n", "two\nlines", "x" * 70_000, "bad\x00byte"])
def test_an_empty_oversized_or_malformed_secret_file_is_refused(
    secrets_dir: Path, content: str
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY", content)

    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY"):
        Settings()


def test_an_agent_context_file_that_is_not_a_secret_is_refused(secrets_dir: Path) -> None:
    # A file the operator expects to apply but which never would: fail loudly, never silently.
    _write(secrets_dir, "AGENT_CONTEXT_ENVIRONMENT", "production")
    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_ENVIRONMENT"):
        Settings()


@pytest.mark.parametrize("value", ["relative/secrets", "/does/not/exist"])
def test_the_directory_must_be_an_existing_absolute_path(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv(SECRETS_DIR_ENV, value)
    with pytest.raises(SecretFileError, match=SECRETS_DIR_ENV):
        Settings()


def test_a_directory_others_can_write_or_a_symlinked_directory_is_refused(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    secrets_dir.chmod(0o777)
    with pytest.raises(SecretFileError, match="writable"):
        Settings()
    secrets_dir.chmod(0o700)
    link = tmp_path / "linked"
    link.symlink_to(secrets_dir)
    monkeypatch.setenv(SECRETS_DIR_ENV, str(link))
    with pytest.raises(SecretFileError, match="symlink"):
        Settings()


def test_without_the_variable_no_directory_is_read(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_NEO4J__PASSWORD", "neo4j-file-secret", mode=0o644)
    monkeypatch.delenv(SECRETS_DIR_ENV)
    assert Settings().neo4j.password is None


def test_the_cli_reports_an_unsafe_secret_file_as_a_usage_error(secrets_dir: Path) -> None:
    from typer.testing import CliRunner

    from agent_context_platform import cli

    _write(secrets_dir, "AGENT_CONTEXT_NEO4J__PASSWORD", "neo4j-file-secret", mode=0o644)

    result = CliRunner().invoke(cli.app, ["projection", "status"])

    assert result.exit_code == 2 and "AGENT_CONTEXT_NEO4J__PASSWORD" in result.stderr
    assert "neo4j-file-secret" not in result.output + result.stderr


def test_a_non_regular_or_non_utf8_secret_file_is_refused(secrets_dir: Path) -> None:
    fifo = secrets_dir / "AGENT_CONTEXT_NEO4J__PASSWORD"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(SecretFileError, match="regular file"):
        Settings()
    fifo.unlink()
    path = secrets_dir / "AGENT_CONTEXT_NEO4J__PASSWORD"
    path.write_bytes(b"\xff\xfe")
    path.chmod(0o400)
    with pytest.raises(SecretFileError, match="UTF-8"):
        Settings()


def test_owners_other_than_the_process_user_or_root_are_refused_as_any_user(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Runs as any uid: the accepted owners are made to exclude whoever owns the test files.
    _write(secrets_dir, "AGENT_CONTEXT_NEO4J__PASSWORD", "neo4j-file-secret")
    owner = secrets_dir.stat().st_uid
    monkeypatch.setattr(settings_module, "_accepted_owners", lambda: frozenset({owner + 1}))
    with pytest.raises(SecretFileError, match="directory owner"):
        Settings()

    calls = iter([frozenset({owner}), frozenset({owner + 1})])  # directory ok, file not
    monkeypatch.setattr(settings_module, "_accepted_owners", lambda: next(calls))
    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_NEO4J__PASSWORD: the file owner"):
        Settings()


def test_the_accepted_owners_are_the_process_user_and_root() -> None:
    assert settings_module._accepted_owners() == frozenset({os.geteuid(), 0})


@pytest.mark.parametrize(
    "content",
    [
        "\ufeffvalue",  # a byte-order mark
        " value",
        "value ",
        "val\u2028ue",
        "val\vue",
        "val\fue",
        "val\x1bue",
        "value\n\n",
    ],
)
def test_a_value_with_a_bom_control_characters_or_surrounding_space_is_refused(
    secrets_dir: Path, content: str
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY", content)
    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY"):
        Settings()


@pytest.mark.parametrize("ending", ["\n", "\r\n", ""])
def test_only_one_final_line_ending_is_dropped(secrets_dir: Path, ending: str) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_NEO4J__PASSWORD", "inner value" + ending)
    password = Settings().neo4j.password
    assert password is not None and password.get_secret_value() == "inner value"


def test_a_directory_reached_through_a_symlinked_parent_is_refused(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path)
    monkeypatch.setenv(SECRETS_DIR_ENV, str(alias / "secrets"))
    with pytest.raises(SecretFileError, match="symlink"):
        Settings()
    monkeypatch.setenv(SECRETS_DIR_ENV, str(secrets_dir / ".." / "secrets"))
    with pytest.raises(SecretFileError, match="canonical"):
        Settings()


@pytest.mark.parametrize("section_name", ["AGENT_CONTEXT_S3", "agent_context_s3"])
@pytest.mark.parametrize(
    "value",
    [
        '{"secret_access_key": "stale-env"}',
        '{"SECRET_ACCESS_KEY": "stale-env"}',
        '{"secret_access_key": ""}',
    ],
)
def test_a_section_variable_holding_a_mounted_secret_is_refused(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch, section_name: str, value: str
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY", "s3-file-secret")
    monkeypatch.setenv(section_name, value)

    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_S3") as error:
        Settings()

    assert "stale-env" not in str(error.value) and "s3-file-secret" not in str(error.value)


def test_a_section_variable_without_the_mounted_secret_merges_with_the_file(
    secrets_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY", "s3-file-secret")
    monkeypatch.setenv("AGENT_CONTEXT_S3", '{"bucket_name": "from-env"}')

    settings = Settings()

    assert settings.s3.bucket_name == "from-env"
    assert settings.s3.secret_access_key is not None
    assert settings.s3.secret_access_key.get_secret_value() == "s3-file-secret"


def test_an_unreadable_section_variable_counts_as_setting_the_secret() -> None:
    # Settings itself refuses such a value earlier; the helper fails closed on its own too.
    exported = {"AGENT_CONTEXT_S3": "not json at all"}
    assert settings_module._section_sets(exported, "s3", "secret_access_key")
    assert settings_module._section_sets({"AGENT_CONTEXT_S3": "[1]"}, "s3", "secret_access_key")
    assert not settings_module._section_sets(
        {"AGENT_CONTEXT_S3": '{"bucket_name": "b"}'}, "s3", "secret_access_key"
    )
    assert not settings_module._section_sets({}, "s3", "secret_access_key")


@pytest.mark.parametrize("content", ["val\N{NO-BREAK SPACE}ue", "value\r", "value\r\r\n"])
def test_a_no_break_space_or_a_bare_carriage_return_is_refused(
    secrets_dir: Path, content: str
) -> None:
    _write(secrets_dir, "AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY", content)
    with pytest.raises(SecretFileError, match="AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY"):
        Settings()
