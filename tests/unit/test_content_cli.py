"""`agent-context content sweep-orphans`: safe defaults, refusals and counts-only output."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.content.orphans import SweepReport

pytestmark = pytest.mark.unit

PASSWORD = "s3cret-password"
DSN = f"postgresql+psycopg://operator:{PASSWORD}@db.example.test/agent"
S3_SECRET = "s3-secret-key-value"


@pytest.fixture
def sweep(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    for name, value in {
        "AGENT_CONTEXT_POSTGRESQL__API_DSN": DSN,
        "AGENT_CONTEXT_S3__ENDPOINT_URL": "http://garage.example.test:3900",
        "AGENT_CONTEXT_S3__BUCKET_NAME": "content-bucket",
        "AGENT_CONTEXT_S3__ACCESS_KEY_ID": "GKexample",
        "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY": S3_SECRET,
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AGENT_CONTEXT_ENVIRONMENT", raising=False)
    state: dict[str, Any] = {"report": SweepReport(5, 2, 1, 2), "error": None}

    class Sweeper:
        def __init__(self, client: Any, bucket: str, sessions: Any, *, min_age: timedelta) -> None:
            state.update(client=client, bucket=bucket, min_age=min_age)

        async def run(self, cutoff: datetime, *, dry_run: bool = False) -> SweepReport:
            state.update(cutoff=cutoff, dry_run=dry_run)
            if state["error"] is not None:
                raise state["error"]
            report: SweepReport = state["report"]
            return report

    monkeypatch.setattr(cli, "OrphanSweeper", Sweeper)
    monkeypatch.setattr(cli, "build_s3_client", lambda settings: ("client", settings.bucket_name))
    return state


def run(*args: str) -> Any:
    return CliRunner().invoke(cli.app, ["content", "sweep-orphans", *args])


def test_the_default_is_the_24_hour_floor_and_the_output_is_counts_only(
    sweep: dict[str, Any],
) -> None:
    before = datetime.now(UTC)
    result = run("--json")

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "examined": 5,
        "deleted": 2,
        "retained": 1,
        "skipped_recent": 2,
        "would_delete": 0,
        "dry_run": False,
        "min_age_seconds": 86_400,
    }
    assert sweep["min_age"] == timedelta(hours=24) and sweep["bucket"] == "content-bucket"
    assert sweep["cutoff"] >= before and sweep["dry_run"] is False
    human = run()
    assert human.stdout.strip() == (
        "orphan sweep: examined 5, deleted 2, retained 1, skipped recent 2 (min age 86400 s)"
    )
    for secret in (PASSWORD, S3_SECRET, "db.example.test"):
        assert secret not in result.output + human.output


def test_a_dry_run_deletes_nothing_and_says_so(sweep: dict[str, Any]) -> None:
    sweep["report"] = SweepReport(4, 0, 1, 1, would_delete=2, dry_run=True)

    result = run("--dry-run", "--min-age", "172800")

    assert result.exit_code == 0 and sweep["dry_run"] is True
    assert sweep["min_age"] == timedelta(days=2)
    assert result.stdout.strip() == (
        "orphan sweep (dry run): examined 4, would delete 2, retained 1, skipped recent 1 "
        "(min age 172800 s)"
    )


@pytest.mark.parametrize("min_age", ["0", "3600", "86399"])
def test_a_min_age_below_the_floor_needs_the_explicit_test_flag(
    sweep: dict[str, Any], min_age: str
) -> None:
    refused = run("--min-age", min_age)
    assert refused.exit_code == 2 and "--allow-short-min-age" in refused.stderr
    assert "min_age" not in sweep

    allowed = run("--min-age", min_age, "--allow-short-min-age")
    assert allowed.exit_code == 0 and sweep["min_age"] == timedelta(seconds=int(min_age))


def test_production_refuses_the_short_min_age_flag(
    sweep: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_ENVIRONMENT", "production")
    monkeypatch.setattr(cli, "_load_settings", lambda: _production_like())

    refused = run("--min-age", "0", "--allow-short-min-age")

    assert refused.exit_code == 2 and "production" in refused.stderr
    assert "min_age" not in sweep
    assert run("--min-age", "90000").exit_code == 0  # a safe min-age is fine in production


def _production_like() -> Any:
    real = cli.Settings(environment="development")
    return real.model_copy(update={"environment": "production"})


@pytest.mark.parametrize("min_age", ["-1", "nan", "1e12"])
def test_a_malformed_min_age_is_a_usage_error(sweep: dict[str, Any], min_age: str) -> None:
    assert run("--min-age", min_age, "--allow-short-min-age").exit_code == 2
    assert "min_age" not in sweep


def test_missing_storage_settings_are_a_usage_error(
    sweep: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AGENT_CONTEXT_S3__ENDPOINT_URL")
    result = run()
    assert result.exit_code == 2 and "AGENT_CONTEXT_S3__ENDPOINT_URL" in result.stderr


def test_a_failing_sweep_exits_1_with_the_error_class_only(sweep: dict[str, Any]) -> None:
    sweep["error"] = ConnectionError(f"could not reach {DSN}")

    result = run()

    assert result.exit_code == 1 and "ConnectionError" in result.stderr
    assert PASSWORD not in result.output + result.stderr
