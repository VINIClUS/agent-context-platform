"""The `agent-context` CLI: option rules, exit codes and secret hygiene (PLATFORM-039)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from agent_context_sdk.events.system import ProjectionOutcome
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.projection.verify import (
    GraphDigest,
    ProjectorStatus,
    ProjectorVerification,
    RebuildReport,
    RecordResult,
    RunnerActiveError,
    StreamHeadCheck,
    TargetNotEmptyError,
    VerificationReport,
)
from agent_context_platform.settings import Neo4jSettings

pytestmark = pytest.mark.unit

PASSWORD = "s3cret-password"
DSN = f"postgresql+psycopg://operator:{PASSWORD}@db.example.test/agent"
LIVE = "bolt://live.example.test:7687"


@pytest.fixture(autouse=True)
def environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "AGENT_CONTEXT_POSTGRESQL__DSN": DSN,
        "AGENT_CONTEXT_NEO4J__URI": LIVE,
        "AGENT_CONTEXT_NEO4J__USERNAME": "neo4j",
        "AGENT_CONTEXT_NEO4J__PASSWORD": PASSWORD,
        cli.TARGET_PASSWORD_ENV: PASSWORD,
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv(cli.TARGET_USERNAME_ENV, raising=False)


class Calls:
    def __init__(self) -> None:
        self.verify: list[dict[str, Any]] = []
        self.rebuild: list[dict[str, Any]] = []
        self.runtime_store: Any = None
        self.closed: list[Any] = []


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> Calls:
    seen = Calls()

    @asynccontextmanager
    async def runtime(_settings: Any) -> AsyncIterator[cli.Runtime]:
        seen.runtime_store = object()
        yield cli.Runtime(sessions=object(), store=seen.runtime_store)  # type: ignore[arg-type]

    monkeypatch.setattr(cli, "_runtime", runtime)
    return seen


def projector(*issues: str) -> ProjectorVerification:
    return ProjectorVerification(
        name="git",
        version="1",
        checkpoint_outbox_id=4,
        processed_count=2,
        handled_events=2,
        covered_events=2,
        lag=0,
        in_flight=0,
        dead_lettered=0,
        unqueued=0,
        from_event_id=None,
        through_event_id=None,
        issues=issues,
    )


def verification(*issues: str, replay: str | None = None) -> VerificationReport:
    return VerificationReport(
        projectors=(projector(*issues),),
        streams=StreamHeadCheck(2, 0, (), 0),
        graph=GraphDigest("d" * 64, 3, 2, {"Commit": 3}, {"HAS": 2}),
        orphan_count=0,
        orphan_sample=(),
        source_ids_checked=7,
        ledger_head_outbox_id=4,
        replay_digest=replay,
        recorded=RecordResult(1, 0),
    )


def patch_verify(
    monkeypatch: pytest.MonkeyPatch, calls: Calls, report: VerificationReport | Exception
) -> None:
    async def fake(*args: Any, **kwargs: Any) -> VerificationReport:
        calls.verify.append({"args": args, **kwargs})
        if isinstance(report, Exception):
            raise report
        return report

    monkeypatch.setattr(cli, "verify_projections", fake)


def patch_rebuild(
    monkeypatch: pytest.MonkeyPatch, calls: Calls, outcome: Exception | ProjectionOutcome
) -> None:
    async def fake(*args: Any, **kwargs: Any) -> RebuildReport:
        calls.rebuild.append({"args": args, **kwargs})
        if isinstance(outcome, Exception):
            raise outcome
        return RebuildReport(
            mode="in_place" if kwargs["in_place"] else "standby",
            target=kwargs["target_description"],
            projected_events=5,
            verification=verification(),
            wiped_nodes=1,
            outcome=outcome,
            recorded=RecordResult(1, 0),
        )

    monkeypatch.setattr(cli, "rebuild_projections", fake)


def run(*args: str) -> Any:
    return CliRunner().invoke(cli.app, list(args))


TARGET = ["--target-uri", "bolt://standby.example.test:7687", "--target-database", "neo4j"]
USER = ["--target-username", "operator"]


def test_the_cli_has_help_for_every_projection_command() -> None:
    assert "projection" in run().output
    for command in ("rebuild", "verify", "status"):
        assert run("projection", command, "--help").exit_code == 0
    assert "projection" in run("--help").output


def test_verify_reports_a_match_as_json_and_exit_zero(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_verify(monkeypatch, calls, verification())

    result = run("projection", "verify", "--json")

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] and payload["graph"]["digest"] == "d" * 64
    options = calls.verify[0]
    assert options["require_caught_up"] is False and options["replay_target"] is None
    assert options["recorder"] is not None and options["batch_size"] == 1000


def test_verify_human_output_and_no_record(monkeypatch: pytest.MonkeyPatch, calls: Calls) -> None:
    patch_verify(monkeypatch, calls, verification(replay="d" * 64))

    result = run("projection", "verify", "--no-record", "--require-caught-up", "--batch-size", "5")

    assert result.exit_code == 0
    assert "graph digest " + "d" * 64 in result.output and "verified" in result.output
    assert "replay digest" in result.output and "recorded 1 new" in result.output
    assert calls.verify[0]["recorder"] is None and calls.verify[0]["batch_size"] == 5


def test_verify_exits_one_on_a_mismatch(monkeypatch: pytest.MonkeyPatch, calls: Calls) -> None:
    patch_verify(monkeypatch, calls, verification("checkpoint_regressed"))

    result = run("projection", "verify")

    assert result.exit_code == 1
    assert "MISMATCH" in result.output and "git: checkpoint_regressed" in result.output


def test_verify_replay_check_builds_the_scratch_target_from_explicit_options(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_verify(monkeypatch, calls, verification())

    result = run(
        "projection",
        "verify",
        "--replay-check",
        *TARGET,
        *USER,
        "--wipe-target",
        "--confirm",
        "neo4j",
    )

    assert result.exit_code == 0, result.output
    assert calls.verify[0]["replay_target"] is not None and calls.verify[0]["wipe_replay_target"]


@pytest.mark.parametrize(
    "args",
    [
        ["--replay-check"],
        ["--replay-check", *TARGET],  # no username
        [
            "--replay-check",
            "--target-uri",
            LIVE,
            "--target-database",
            "neo4j",
            *USER,
        ],  # the live graph
        ["--replay-check", *TARGET, *USER, "--wipe-target"],  # wipe without confirm
        ["--replay-check", "--target-uri", "http://x", "--target-database", "neo4j", *USER],
        [*TARGET],  # target options without a replay check
        ["--wipe-target"],
        ["--confirm", "neo4j"],
    ],
)
def test_verify_usage_errors_exit_two(args: list[str], calls: Calls) -> None:
    result = run("projection", "verify", *args)

    assert result.exit_code == 2
    assert PASSWORD not in result.output + result.stderr


def test_rebuild_in_place_needs_the_live_database_name_as_confirmation(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)

    assert run("projection", "rebuild", "--in-place").exit_code == 2
    assert run("projection", "rebuild", "--in-place", "--confirm", "other").exit_code == 2
    assert run("projection", "rebuild", "--in-place", "--confirm", "neo4j", *TARGET).exit_code == 2
    assert calls.rebuild == []

    result = run("projection", "rebuild", "--in-place", "--confirm", "neo4j", "--json")

    assert result.exit_code == 0
    request = calls.rebuild[0]
    assert request["in_place"] is True and request["args"][1] is calls.runtime_store
    assert json.loads(result.output)["mode"] == "in_place"


def test_a_standby_rebuild_takes_an_explicit_target_and_prints_the_verified_target(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)

    result = run("projection", "rebuild", *TARGET, *USER)

    assert result.exit_code == 0, result.output
    request = calls.rebuild[0]
    assert request["in_place"] is False and request["wipe_target"] is False
    assert request["target_description"] == "standby.example.test:7687/neo4j"
    assert "verified target standby.example.test:7687/neo4j digest " + "d" * 64 in result.output
    assert "operator step" in result.output
    assert PASSWORD not in result.output


def test_a_standby_rebuild_can_wipe_with_the_database_name_confirmed(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)

    assert run("projection", "rebuild", *TARGET, *USER, "--wipe-target").exit_code == 2
    assert (
        run("projection", "rebuild", *TARGET, *USER, "--wipe-target", "--confirm", "x").exit_code
        == 2
    )
    ok = run(
        "projection",
        "rebuild",
        *TARGET,
        *USER,
        "--wipe-target",
        "--confirm",
        "neo4j",
        "--no-record",
    )

    assert ok.exit_code == 0 and calls.rebuild[0]["wipe_target"] is True
    assert calls.rebuild[0]["recorder"] is None


def test_the_target_username_may_come_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)
    monkeypatch.setenv(cli.TARGET_USERNAME_ENV, "operator")

    assert run("projection", "rebuild", *TARGET).exit_code == 0


@pytest.mark.parametrize(
    "args",
    [
        [],  # no target at all
        ["--target-uri", "bolt://standby.example.test:7687"],
        ["--target-database", "neo4j"],
        [*TARGET],  # no username
        ["--target-uri", LIVE, "--target-database", "neo4j", *USER],  # the live projection
        ["--target-uri", "bolt://localhost:7687", "--target-database", "neo4j", *USER],
        [
            "--target-uri",
            "bolt://user:pw@standby.example.test",
            "--target-database",
            "neo4j",
            *USER,
        ],
    ],
)
def test_a_standby_rebuild_refuses_missing_or_unsafe_targets(
    args: list[str], monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)
    if args[:1] == ["--target-uri"] and "localhost" in args[1]:
        monkeypatch.setenv("AGENT_CONTEXT_NEO4J__URI", "bolt://127.0.0.1:7687")

    result = run("projection", "rebuild", *args)

    assert result.exit_code == 2 and calls.rebuild == []
    assert PASSWORD not in result.output + result.stderr


def test_a_missing_target_password_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)
    monkeypatch.delenv(cli.TARGET_PASSWORD_ENV)

    result = run("projection", "rebuild", *TARGET, *USER)

    assert result.exit_code == 2 and cli.TARGET_PASSWORD_ENV in result.stderr


def test_rebuild_failure_modes_map_to_exit_codes_without_leaking_messages(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, TargetNotEmptyError("holds data"))
    refused = run("projection", "rebuild", *TARGET, *USER)
    assert refused.exit_code == 2 and "--wipe-target" in refused.stderr

    patch_rebuild(monkeypatch, calls, RunnerActiveError("leases"))
    active = run("projection", "rebuild", "--in-place", "--confirm", "neo4j")
    assert active.exit_code == 1 and "stop it first" in active.stderr

    patch_rebuild(monkeypatch, calls, RuntimeError(f"connect failed with {PASSWORD}"))
    failed = run("projection", "rebuild", *TARGET, *USER)
    assert failed.exit_code == 1 and failed.stderr.strip() == "error: RuntimeError"
    assert PASSWORD not in failed.output + failed.stderr


def test_a_rebuild_that_does_not_verify_exits_one(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.FAILED)

    result = run("projection", "rebuild", *TARGET, *USER, "--json")

    assert result.exit_code == 1
    assert json.loads(result.output)["verified_target"] is None
    assert "verified target" not in run("projection", "rebuild", *TARGET, *USER).output


def test_status_lists_each_projector_read_only(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    statuses = [
        ProjectorStatus(
            "git", "1", "active", 4, 2, 6, 2, "RuntimeError", datetime(2026, 1, 1, tzinfo=UTC)
        ),
        ProjectorStatus("code", "1", None, None, 0, 6, 6, None, None),
    ]

    async def fake(_sessions: Any, _projectors: Any) -> list[ProjectorStatus]:
        return statuses

    monkeypatch.setattr(cli, "projection_status", fake)

    human = run("projection", "status")
    assert human.exit_code == 0
    assert "git 1: state active, checkpoint 4/6, lag 2, last error RuntimeError" in human.output
    assert "code 1: state none" in human.output and "last run never" in human.output

    machine = json.loads(run("projection", "status", "--json").output)
    assert [item["name"] for item in machine["projectors"]] == ["git", "code"]


def test_a_missing_or_invalid_configuration_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AGENT_CONTEXT_POSTGRESQL__DSN")
    missing = run("projection", "status")
    assert missing.exit_code == 2 and "POSTGRESQL__DSN" in missing.stderr

    monkeypatch.setenv("AGENT_CONTEXT_NEO4J__URI", "ftp://wrong")
    invalid = run("projection", "status")
    assert invalid.exit_code == 2 and "invalid" in invalid.stderr


def test_the_real_runtime_opens_lazily_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake(sessions: Any, _projectors: Any) -> list[ProjectorStatus]:
        assert sessions is not None
        return []

    monkeypatch.setattr(cli, "projection_status", fake)

    result = run("projection", "status", "--json")

    assert result.exit_code == 0 and json.loads(result.output) == {"projectors": []}


def test_server_keys_normalize_local_hosts_and_default_ports() -> None:
    def settings(uri: str | None, database: str = "neo4j") -> Neo4jSettings:
        return Neo4jSettings.model_validate({"uri": uri, "database": database})

    assert cli._server_key(settings("bolt://LOCALHOST")) == ("127.0.0.1", 7687, "neo4j")
    assert cli._server_key(settings("neo4j://[::1]:7688", "x")) == ("127.0.0.1", 7688, "x")
    assert cli._server_key(settings(None)) == ("", 7687, "neo4j")
    assert cli._describe(settings("bolt://Host.Example:7000")) == "host.example:7000/neo4j"
