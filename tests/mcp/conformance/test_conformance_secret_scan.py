"""The artifact secret scan fails closed and never echoes what it found (PLATFORM-047)."""

from __future__ import annotations

import json
import secrets
from pathlib import Path

import pytest
import secret_scan
import yaml

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


def test_a_leaked_token_segment_blocks_the_upload_though_the_ci_rescan_is_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The review's case: only the secret part leaks, which only the token-aware scan sees."""
    secret = secrets.token_urlsafe(32)
    out = tmp_path / "out"
    (out / "shipping").mkdir(parents=True)
    (out / "shipping" / "checks.json").write_text(f'{{"echo": "{secret}"}}\n', "utf-8")
    verdict = out / "secret-scan.json"

    # The runner's scan, with the token.
    monkeypatch.setenv(secret_scan.TOKEN_ENV, f"mcp_conform01.{secret}")
    assert secret_scan.main(["--verdict", str(verdict), str(out)]) == 1
    recorded = verdict.read_text("utf-8")
    assert secret not in recorded
    assert json.loads(recorded) == {"passed": False, "findings": 1, "token_aware": True}

    # The workflow's rescan has no token and finds nothing; the verdict check still refuses.
    monkeypatch.delenv(secret_scan.TOKEN_ENV)
    assert secret_scan.main([str(out)]) == 0
    assert secret_scan.main(["--check-verdict", str(verdict)]) == 1


@pytest.mark.parametrize(
    ("content", "passes"),
    [
        ('{"passed": true, "findings": 0, "token_aware": true}', True),
        ('{"passed": true, "findings": 0, "token_aware": false}', False),  # scanned without it
        ('{"passed": true, "findings": 2, "token_aware": true}', False),
        ('{"passed": false, "findings": 0, "token_aware": true}', False),
        ("[]", False),
        ("not json", False),
        (None, False),  # the runner died before scanning
    ],
)
def test_only_a_passing_token_aware_verdict_passes(
    tmp_path: Path, content: str | None, passes: bool
) -> None:
    verdict = tmp_path / "secret-scan.json"
    if content is not None:
        verdict.write_text(content, "utf-8")
    assert secret_scan.main(["--check-verdict", str(verdict)]) == (0 if passes else 1)


def test_a_clean_token_aware_scan_records_a_passing_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "summary.json").write_text('{"errors": 0}\n', "utf-8")
    monkeypatch.setenv(secret_scan.TOKEN_ENV, f"mcp_conform01.{secrets.token_urlsafe(32)}")
    verdict = tmp_path / "verdict.json"
    assert secret_scan.main(["--verdict", str(verdict), str(tmp_path / "out")]) == 0
    assert secret_scan.verdict_passed(verdict)
    with pytest.raises(SystemExit):
        secret_scan.main([])  # paths are required to scan


def test_the_workflow_uploads_only_after_both_scans_pass() -> None:
    root = Path(__file__).resolve().parents[3]
    workflow = yaml.safe_load((root / ".github/workflows/mcp-conformance.yml").read_text("utf-8"))
    steps = {
        step.get("id", step["name"]): step for step in workflow["jobs"]["conformance"]["steps"]
    }

    assert steps["conformance"]["continue-on-error"] is True
    assert steps["scan"]["if"] == "always()"
    assert "secret_scan.py" in steps["scan"]["run"]
    assert steps["verdict"]["if"] == "always()"
    assert '--check-verdict "$OUTPUT_DIR/secret-scan.json"' in steps["verdict"]["run"]
    upload = next(step for step in steps.values() if "upload-artifact" in step.get("uses", ""))
    assert upload["if"] == (
        "always() && steps.scan.outcome == 'success' && steps.verdict.outcome == 'success'"
    )
    order = list(steps)
    assert order.index("scan") < order.index("verdict") < order.index(upload["name"])
    assert workflow["permissions"] == {"contents": "read"}
    script = (root / "scripts/run-mcp-conformance.sh").read_text("utf-8")
    assert '--verdict "$out/secret-scan.json"' in script
