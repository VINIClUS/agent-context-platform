"""The artifact secret scan fails closed and never echoes what it found (PLATFORM-047)."""

from __future__ import annotations

import secrets
from pathlib import Path

import pytest
import secret_scan

pytestmark = pytest.mark.unit


def _canaries() -> dict[str, str]:
    """Credential-shaped values minted at run time, one per pattern."""
    word = secrets.token_hex(8)
    return {
        "mcp_token": f"mcp_{word[:8]}.{secrets.token_urlsafe(32)}",
        "authorization_bearer": f"Authorization: Bearer prod_{word}.{secrets.token_urlsafe(12)}",
        "url_password": f"postgresql://agent:{word}@db.invalid/ledger",
        "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----",
        "aws_access_key": "AKIA" + secrets.token_hex(8).upper(),
        "github_token": "ghp_" + secrets.token_hex(18),
        "test_canary": f"AGENT_CONTEXT_CANARY_{word}",
    }


@pytest.mark.parametrize("name", sorted(_canaries()))
def test_each_credential_shape_is_found_by_name_only(
    tmp_path: Path, name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    value = _canaries()[name]
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "checks.json").write_text(f'{{"detail": "x {value} y"}}\n', "utf-8")

    assert secret_scan.main([str(tmp_path)]) == 1

    output = capsys.readouterr()
    assert f"checks.json:1: {name}" in output.err
    assert value not in output.out + output.err


def test_the_runtime_token_is_found_wherever_it_hides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    token = secrets.token_urlsafe(24)  # no credential shape: only the exact value finds it
    (tmp_path / "app.log").write_text(f"line one\nrequest saw {token}\n", "utf-8")
    monkeypatch.setenv(secret_scan.TOKEN_ENV, token)

    assert secret_scan.main([str(tmp_path / "app.log")]) == 1
    assert "app.log:2: runtime_token" in capsys.readouterr().err


def test_the_runtime_tokens_secret_is_found_without_its_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    secret = secrets.token_urlsafe(32)
    monkeypatch.setenv(secret_scan.TOKEN_ENV, f"mcp_conform01.{secret}")
    (tmp_path / "suite.txt").write_text(
        f"ok\nsecret={secret}\nmcp_conform01 is a prefix\n", "utf-8"
    )

    assert secret_scan.main([str(tmp_path)]) == 1
    assert capsys.readouterr().err.count(": runtime_token") == 1  # the bare prefix is not secret
    assert secret_scan.token_needles(None) == ()


def test_clean_suite_output_passes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # What the suite and the servers really write: ids, statuses, URLs without credentials,
    # cursor-free tool listings and the platform's log lines.
    (tmp_path / "checks.json").write_text(
        '[{"id": "tools-list", "status": "SUCCESS", '
        '"specReferences": [{"url": "https://modelcontextprotocol.io/specification/x"}]}]\n'
        "INFO mcp_request outcome=ok method=tools/list principal=hash:4f2a\n"
        "http://127.0.0.1:41234/mcp\n",
        "utf-8",
    )
    assert secret_scan.main([str(tmp_path)]) == 0
    assert "0 finding(s)" in capsys.readouterr().out


def test_a_missing_or_oversized_input_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert secret_scan.main([str(tmp_path / "absent")]) == 1
    (tmp_path / "big.log").write_text("x" * 64, "utf-8")
    monkeypatch.setattr(secret_scan, "MAX_FILE_BYTES", 16)
    assert secret_scan.main([str(tmp_path)]) == 1
