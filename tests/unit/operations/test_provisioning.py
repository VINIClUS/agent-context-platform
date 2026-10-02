"""Token generation and verifier hashing for operator provisioning (PLATFORM-039B)."""

from __future__ import annotations

import asyncio
import os
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.mcp.auth import MCP_TOKEN_PREFIX
from agent_context_platform.operations import provisioning
from agent_context_platform.security.bearer import Argon2Verifier, hash_verifier, parse_bearer

pytestmark = pytest.mark.unit

COST = {"time_cost": 1, "memory_cost_kib": 8, "parallelism": 1}


@pytest.mark.parametrize(
    ("namespace", "prefix_check"),
    [(provisioning.PRODUCER_TOKEN_PREFIX, False), (MCP_TOKEN_PREFIX, True)],
)
def test_generated_tokens_parse_and_stay_in_their_namespace(
    namespace: str, prefix_check: bool
) -> None:
    prefix, token = provisioning._new_token(namespace)
    bearer = parse_bearer(f"Bearer {token}")
    assert bearer is not None and bearer.prefix == prefix and bearer.token == token
    assert prefix.startswith(MCP_TOKEN_PREFIX) is prefix_check
    assert provisioning._new_token(namespace)[1] != token


def test_hash_verifier_uses_the_configured_cost_and_verifies() -> None:
    token = "prd_abcdefghijkl." + "s" * 43
    verifier = hash_verifier(token, **COST)
    assert verifier.startswith("$argon2id$v=19$m=8,t=1,p=1$")
    assert token not in verifier
    checker = Argon2Verifier(**COST, max_concurrency=1, max_queue_depth=1)
    assert asyncio.run(checker.verify(verifier, token))
    assert not asyncio.run(checker.verify(verifier, token + "x"))


@pytest.mark.parametrize("value", ["", "   ", "x" * 256, "bad\nname"])
def test_names_are_validated(value: str) -> None:
    with pytest.raises(provisioning.ProvisioningError):
        provisioning._name(value, "principal")


@pytest.mark.parametrize("days", [0, -1, 3651])
def test_expiry_is_bounded(days: int) -> None:
    with pytest.raises(provisioning.ProvisioningError):
        provisioning._expiry(datetime.now(UTC), days)


def test_only_the_memory_read_scope_exists() -> None:
    assert provisioning.MCP_SCOPES == ("memory:read",)


def test_secret_file_is_new_private_and_outside_git(tmp_path: Path) -> None:
    created = cli.SecretFile.create(tmp_path / "token")
    assert stat.S_IMODE(created.path.stat().st_mode) == 0o600
    created.abandon()
    assert not created.path.exists()
    kept = cli.SecretFile.create(tmp_path / "token")
    with pytest.raises(cli.CliUsageError):
        cli.SecretFile.create(tmp_path / "token")
    (tmp_path / "work" / ".git").mkdir(parents=True)
    with pytest.raises(cli.CliUsageError):
        cli.SecretFile.create(tmp_path / "work" / "token")
    with pytest.raises(cli.CliUsageError):
        cli.SecretFile.create(tmp_path / "missing" / "token")
    link = tmp_path / "link"
    os.symlink(tmp_path / "elsewhere", link)
    with pytest.raises(cli.CliUsageError):
        cli.SecretFile.create(link)
    kept.abandon()


def test_missing_admin_dsn_and_bad_options_are_usage_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("DSN", "ADMIN_DSN"):
        monkeypatch.delenv(f"AGENT_CONTEXT_POSTGRESQL__{name}", raising=False)
    runner = CliRunner()
    missing = runner.invoke(
        cli.app, ["producer", "register", "--producer-id", "p", "--expires-in", "30"]
    )
    assert missing.exit_code == 2
    for args in (
        ["producer", "register", "--producer-id", "p", "--expires-in", "0"],
        ["producer", "register", "--producer-id", "p"],
        [
            "mcp-token",
            "create",
            "--principal",
            "x",
            "--expires-in",
            "1",
            "--scope",
            "events:ingest",
        ],
    ):
        assert runner.invoke(cli.app, args).exit_code == 2


def test_a_replaced_output_path_never_receives_the_token_and_is_not_unlinked(
    tmp_path: Path,
) -> None:
    held = cli.SecretFile.create(tmp_path / "token")
    # An account that can write the directory swaps the name during the slow provisioning step.
    held.path.rename(tmp_path / "moved")
    attacker = tmp_path / "token"
    attacker.write_text("attacker")
    held.write("prd_secret")
    assert attacker.read_text() == "attacker"
    assert (tmp_path / "moved").read_text() == "prd_secret\n"

    other = cli.SecretFile.create(tmp_path / "second")
    other.path.unlink()
    other.path.write_text("attacker")
    other.abandon()  # not ours any more: left in place
    assert other.path.read_text() == "attacker"
