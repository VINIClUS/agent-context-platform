"""The `agent-context` CLI: option rules, exit codes and secret hygiene (PLATFORM-039)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from agent_context_sdk.events.system import ProjectionOutcome
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.operations import migrate
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.registry import registered_projectors
from agent_context_platform.projection.schema import ProjectionHistory
from agent_context_platform.projection.verify import (
    GraphDigest,
    MissingGrantError,
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
        "AGENT_CONTEXT_SEARCH__MODEL_DIR": "/models/minilm",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv(cli.TARGET_USERNAME_ENV, raising=False)


class Calls:
    def __init__(self) -> None:
        self.verify: list[dict[str, Any]] = []
        self.rebuild: list[dict[str, Any]] = []
        self.runtime_store: Any = None
        self.closed: list[Any] = []
        self.recording: list[bool] = []
        self.preflight: list[tuple[bool, bool]] = []
        self.preflight_error: Exception | None = None
        self.extra_grants: list[tuple[Any, ...]] = []
        self.history: ProjectionHistory | None = None


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> Calls:
    seen = Calls()

    @asynccontextmanager
    async def runtime(_settings: Any, *, record: bool = False) -> AsyncIterator[cli.Runtime]:
        seen.runtime_store = object()
        seen.recording.append(record)
        yield cli.Runtime(
            sessions=object(),  # type: ignore[arg-type]
            recording_sessions=object() if record else None,  # type: ignore[arg-type]
            store=seen.runtime_store,
        )

    async def fake_preflight(
        _projector: Any, recorder: Any, *, write_checkpoints: bool, extra_grants: Any = ()
    ) -> None:
        seen.preflight.append((recorder is not None, write_checkpoints))
        seen.extra_grants.append(tuple(extra_grants))
        if seen.preflight_error is not None:
            raise seen.preflight_error

    async def no_search_projectors(*_args: Any, **_kwargs: Any) -> tuple[Any, ...]:
        return ()  # the model is never loaded here; the search wiring has its own tests

    async def fake_history(store: Any) -> ProjectionHistory | None:
        assert store is seen.runtime_store  # the live graph, never a scratch or standby target
        return seen.history

    monkeypatch.setattr(cli, "_runtime", runtime)
    monkeypatch.setattr(cli, "preflight", fake_preflight)
    monkeypatch.setattr(cli, "_search_projectors", no_search_projectors)
    monkeypatch.setattr(cli, "projection_history", fake_history)
    return seen


REGISTERED = sorted(f"{item.name}:{item.version}" for item in registered_projectors())
# The marker never names the search projector (`records_history = False`).
RECORDED = [item for item in REGISTERED if item != "search:1"]
REQUIRED = ["knowledge:2", "quality:2"]
OTHERS = [item for item in RECORDED if item not in REQUIRED]
MISSING = {"state": "missing", "missing": REQUIRED, "others_missing": OTHERS}


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


def verification(
    *issues: str, replay: str | None = None, record_error: str | None = None
) -> VerificationReport:
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
        record_error=record_error,
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
    monkeypatch: pytest.MonkeyPatch,
    calls: Calls,
    outcome: Exception | ProjectionOutcome,
    record_error: str | None = None,
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
            record_error=record_error,
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


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["verify"], None),  # plain verify never builds (loads) a search provider
        (["verify", "--replay-check", *TARGET, *USER], False),
        (["rebuild", *TARGET, *USER], False),  # standby: live PostgreSQL stays untouched
        (["rebuild", "--in-place", "--confirm", "neo4j"], True),
    ],
)
def test_only_projecting_commands_bind_search_and_only_in_place_writes_documents(
    monkeypatch: pytest.MonkeyPatch, calls: Calls, args: list[str], expected: bool | None
) -> None:
    patch_verify(monkeypatch, calls, verification(replay="d" * 64))
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)
    built: list[bool] = []

    async def fake_search_projectors(
        _settings: Any, _runtime: Any, *, write_documents: bool, without_search: bool
    ) -> tuple[Any, ...]:
        built.append(write_documents)
        return ()

    monkeypatch.setattr(cli, "_search_projectors", fake_search_projectors)

    run("projection", *args)

    assert built == ([] if expected is None else [expected])


def test_verify_reports_a_match_as_json_and_exit_zero(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_verify(monkeypatch, calls, verification())

    result = run("projection", "verify", "--json")

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] and payload["graph"]["digest"] == "d" * 64
    assert payload["history"] == MISSING
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
    lines = result.output.splitlines()
    assert lines[-2].startswith("history: missing, rebuild required before temporal reads")
    assert "knowledge:2" in lines[-2] and lines[-1] == "verified"
    calls.history = ProjectionHistory(frozenset(RECORDED))
    complete = run("projection", "verify", "--no-record")
    assert "history: complete for temporal reads\n" in complete.output
    # the same set the temporal reads check: other projectors are only listed
    calls.history = ProjectionHistory(frozenset(REQUIRED))
    required_only = run("projection", "verify", "--no-record")
    assert "history: complete for temporal reads; other projectors not replayed from the" in (
        required_only.output
    )
    # the marker is reported, never a verification failure of its own
    calls.history = ProjectionHistory(frozenset({"knowledge:1"}))
    outdated = run("projection", "verify", "--no-record")
    assert outdated.exit_code == 0 and "history: outdated, rebuild required" in outdated.output


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

    patch_rebuild(monkeypatch, calls, RunnerActiveError("the runner may be running, stop it first"))
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
    monkeypatch.setattr(cli, "_search_status", _search_counts(documents=5, embeddings=3))

    human = run("projection", "status")
    assert human.exit_code == 0
    assert "git 1: state active, checkpoint 4/6, lag 2, last error RuntimeError" in human.output
    assert "code 1: state none" in human.output and "last run never" in human.output
    assert "search: 5 documents, 3 embeddings (about 2 without embeddings: run" in human.output

    assert "history: missing, rebuild required before temporal reads" in human.output

    machine = json.loads(run("projection", "status", "--json").output)
    assert [item["name"] for item in machine["projectors"]] == ["git", "code"]
    assert machine["history"] == MISSING
    calls.history = ProjectionHistory(frozenset(RECORDED))
    machine = json.loads(run("projection", "status", "--json").output)
    assert machine["history"] == {"state": "complete", "missing": [], "others_missing": []}
    assert machine["search"] == {
        "documents": 5,
        "embeddings": 3,
        "documents_without_embeddings": 2,
    }
    monkeypatch.setattr(cli, "_search_status", _search_counts(documents=3, embeddings=3))
    assert "search: 3 documents, 3 embeddings\n" in run("projection", "status").output


def _search_counts(*, documents: int, embeddings: int) -> Any:
    async def counts(_runtime: Any) -> dict[str, int]:
        return {
            "documents": documents,
            "embeddings": embeddings,
            "documents_without_embeddings": max(documents - embeddings, 0),
        }

    return counts


def test_a_missing_or_invalid_configuration_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AGENT_CONTEXT_POSTGRESQL__DSN")
    missing = run("projection", "status")
    assert missing.exit_code == 2 and "PROJECTOR_DSN" in missing.stderr

    monkeypatch.setenv("AGENT_CONTEXT_NEO4J__URI", "ftp://wrong")
    invalid = run("projection", "status")
    assert invalid.exit_code == 2 and "invalid" in invalid.stderr


def test_preflight_runs_before_any_work_and_a_missing_grant_exits_one(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_verify(monkeypatch, calls, verification())
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED)

    run("projection", "verify")
    run("projection", "verify", "--no-record")
    run("projection", "rebuild", "--in-place", "--confirm", "neo4j")
    run("projection", "rebuild", *TARGET, *USER, "--no-record")
    assert calls.preflight == [(True, False), (False, False), (True, True), (False, False)]
    assert calls.recording == [True, False, True, False]

    calls.preflight_error = MissingGrantError("api", ["INSERT on ledger.events"])
    calls.rebuild.clear()
    calls.verify.clear()
    for args in (
        ("rebuild", *TARGET, *USER, "--wipe-target", "--confirm", "neo4j"),
        ("rebuild", "--in-place", "--confirm", "neo4j"),
        ("verify",),
    ):
        result = run("projection", *args)
        assert result.exit_code == 1 and "INSERT on ledger.events" in result.stderr
    assert calls.rebuild == [] and calls.verify == []


def test_a_recording_failure_never_masks_the_verification_result(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_verify(monkeypatch, calls, verification(record_error="record_failed"))

    machine = run("projection", "verify", "--json")
    human = run("projection", "verify")

    assert machine.exit_code == 1 and human.exit_code == 1
    assert json.loads(machine.stdout)["record_error"] == "record_failed"
    assert json.loads(machine.stdout)["ok"] is True
    assert "recording failed: record_failed" in human.output and "verified" in human.output
    assert "error: record_failed" in human.stderr


def test_a_recording_failure_still_reports_the_verified_rebuild_target(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    patch_rebuild(monkeypatch, calls, ProjectionOutcome.COMPLETED, record_error="record_failed")

    machine = run("projection", "rebuild", *TARGET, *USER, "--json")
    human = run("projection", "rebuild", *TARGET, *USER)

    assert machine.exit_code == 1 and human.exit_code == 1
    payload = json.loads(machine.stdout)
    assert payload["verified_target"] == "standby.example.test:7687/neo4j"
    assert payload["graph_digest"] == "d" * 64 and payload["record_error"] == "record_failed"
    assert "verified target standby.example.test:7687/neo4j" in human.output
    assert "error: record_failed" in human.stderr


async def _open_real(settings: Any, *, record: bool) -> tuple[Any, Any]:
    async with cli._runtime(settings, record=record) as runtime:
        return runtime.sessions, runtime.recording_sessions


def test_the_real_runtime_builds_one_engine_per_role_and_only_records_with_the_api_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    monkeypatch.setenv(
        "AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN", "postgresql+psycopg://projector:pw@db/agent"
    )
    settings = cli._load_settings()

    projector_only = asyncio.run(_open_real(settings, record=False))
    both = asyncio.run(_open_real(settings, record=True))

    assert projector_only[1] is None and both[0] is not None and both[1] is not None


def test_a_missing_dsn_for_a_needed_role_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    monkeypatch.delenv("AGENT_CONTEXT_POSTGRESQL__DSN")
    monkeypatch.setenv(
        "AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN", "postgresql+psycopg://projector:pw@db/agent"
    )
    settings = cli._load_settings()

    assert asyncio.run(_open_real(settings, record=False))[1] is None
    with pytest.raises(cli.CliUsageError, match="API_DSN"):
        asyncio.run(_open_real(settings, record=True))


def test_the_real_runtime_opens_lazily_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake(sessions: Any, _projectors: Any) -> list[ProjectorStatus]:
        assert sessions is not None
        return []

    async def history(store: Any) -> None:
        assert isinstance(store, Neo4jStore)

    monkeypatch.setattr(cli, "projection_status", fake)
    monkeypatch.setattr(cli, "projection_history", history)
    monkeypatch.setattr(cli, "_search_status", _search_counts(documents=0, embeddings=0))

    result = run("projection", "status", "--json")

    assert result.exit_code == 0 and json.loads(result.output) == {
        "projectors": [],
        "history": MISSING,
        "search": {"documents": 0, "embeddings": 0, "documents_without_embeddings": 0},
    }


def test_server_keys_normalize_local_hosts_and_default_ports() -> None:
    def settings(uri: str | None, database: str = "neo4j") -> Neo4jSettings:
        return Neo4jSettings.model_validate({"uri": uri, "database": database})

    assert cli._server_key(settings("bolt://LOCALHOST")) == ("127.0.0.1", 7687, "neo4j")
    assert cli._server_key(settings("neo4j://[::1]:7688", "x")) == ("127.0.0.1", 7688, "x")
    assert cli._server_key(settings(None)) == ("", 7687, "neo4j")
    assert cli._describe(settings("bolt://Host.Example:7000")) == "host.example:7000/neo4j"


def test_the_runner_quiet_period_is_an_option_and_dead_letters_are_reported(
    monkeypatch: pytest.MonkeyPatch, calls: Calls
) -> None:
    poison = uuid4()

    async def fake(*args: Any, **kwargs: Any) -> RebuildReport:
        calls.rebuild.append({"args": args, **kwargs})
        return replace(
            RebuildReport(
                mode="in_place",
                target="t",
                projected_events=1,
                verification=verification(),
                wiped_nodes=0,
                outcome=ProjectionOutcome.COMPLETED,
                recorded=None,
            ),
            skipped_dead_lettered=(poison,) * 11,
        )

    monkeypatch.setattr(cli, "rebuild_projections", fake)

    default = run("projection", "rebuild", "--in-place", "--confirm", "neo4j", "--json")
    custom = run(
        "projection", "rebuild", "--in-place", "--confirm", "neo4j", "--runner-quiet-seconds", "0"
    )

    assert calls.rebuild[0]["runner_quiet_seconds"] == 30
    assert calls.rebuild[1]["runner_quiet_seconds"] == 0
    skipped = json.loads(default.stdout)["skipped_dead_lettered"]
    assert skipped["count"] == 11 and skipped["event_ids"][0] == str(poison)
    assert "skipped 11 dead-lettered events" in custom.output and "..." in custom.output
    assert "MUST be stopped" in run("projection", "rebuild", "--help").output.replace("\n", " ")


# --- `agent-context db` (PLATFORM-049) ------------------------------------------------------------


class MigrateCalls:
    def __init__(self) -> None:
        self.upgrade: list[dict[str, Any]] = []
        self.current = 0
        self.disposed = 0


@pytest.fixture
def migrations(monkeypatch: pytest.MonkeyPatch) -> MigrateCalls:
    calls = MigrateCalls()
    monkeypatch.delenv("AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN", raising=False)
    monkeypatch.delenv(migrate.ALEMBIC_CONFIG_ENV, raising=False)

    class Engine:
        def __init__(self, url: str) -> None:
            self.url = url

        async def dispose(self) -> None:
            calls.disposed += 1

    def fake_engine(url: str, **_kwargs: Any) -> Engine:
        calls.upgrade.append({"url": url})
        return Engine(url)

    monkeypatch.setattr(cli, "create_async_engine", fake_engine)
    return calls


def _migration_report(*, applied: bool) -> migrate.MigrationReport:
    return migrate.MigrationReport(
        "20260823_0001" if applied else "20260929_0001", "20260929_0001", "20260929_0001", applied
    )


def test_db_upgrade_uses_the_migration_dsn_and_reports_without_secrets(
    monkeypatch: pytest.MonkeyPatch, migrations: MigrateCalls
) -> None:
    migration_password = "migration-only-secret"
    monkeypatch.setenv(
        "AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN",
        f"postgresql+psycopg://migrator:{migration_password}@db.example.test/agent",
    )
    seen: dict[str, Any] = {}

    async def fake_upgrade(engine: Any, config: Any, **options: Any) -> migrate.MigrationReport:
        seen.update(options, url=engine.url, config=config.config_file_name)
        return _migration_report(applied=True)

    monkeypatch.setattr(migrate, "upgrade", fake_upgrade)

    result = CliRunner().invoke(
        cli.app, ["db", "upgrade", "--expect-head", "20260929_0001", "--lock-timeout", "5"]
    )

    assert result.exit_code == cli.EXIT_OK, result.stderr
    assert result.stdout.strip() == "schema upgraded from 20260823_0001 to 20260929_0001"
    assert seen["url"].startswith("postgresql+psycopg://migrator:")
    assert seen["expect_head"] == "20260929_0001"
    assert seen["lock_timeout_seconds"] == 5
    assert seen["config"] == "alembic.ini"
    assert migrations.disposed == 1
    assert migration_password not in result.output and PASSWORD not in result.output


def test_db_upgrade_falls_back_to_the_shared_dsn_and_reports_a_no_op_as_json(
    monkeypatch: pytest.MonkeyPatch, migrations: MigrateCalls
) -> None:
    async def fake_upgrade(engine: Any, config: Any, **options: Any) -> migrate.MigrationReport:
        assert engine.url == DSN
        return _migration_report(applied=False)

    monkeypatch.setattr(migrate, "upgrade", fake_upgrade)

    result = CliRunner().invoke(cli.app, ["db", "upgrade", "--json"])

    assert result.exit_code == cli.EXIT_OK
    assert json.loads(result.stdout) == {
        "before": "20260929_0001",
        "after": "20260929_0001",
        "head": "20260929_0001",
        "applied": False,
    }
    result = CliRunner().invoke(cli.app, ["db", "upgrade"])
    assert result.stdout.strip() == "schema already at head 20260929_0001"


@pytest.mark.parametrize(
    ("error", "exit_code"),
    [
        (migrate.MigrationError("unknown_revision", "refusing to downgrade"), cli.EXIT_USAGE),
        (migrate.MigrationError("alembic_config_missing", "not found"), cli.EXIT_USAGE),
        (migrate.MigrationError("interrupted", "stopped by a signal"), cli.EXIT_FAILED),
        (migrate.MigrationError("head_mismatch", "not at head"), cli.EXIT_FAILED),
        (migrate.MigrationError("lock_timeout", "held too long"), cli.EXIT_FAILED),
        (RuntimeError(f"connection to {DSN} failed"), cli.EXIT_FAILED),
    ],
)
def test_db_upgrade_failures_map_to_exit_codes_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
    migrations: MigrateCalls,
    error: Exception,
    exit_code: int,
) -> None:
    async def fake_upgrade(*_args: Any, **_options: Any) -> migrate.MigrationReport:
        raise error

    monkeypatch.setattr(migrate, "upgrade", fake_upgrade)

    result = CliRunner().invoke(cli.app, ["db", "upgrade"])

    assert result.exit_code == exit_code
    assert result.stdout == ""
    assert PASSWORD not in result.output and "db.example.test" not in result.output
    assert migrations.disposed == 1
    if isinstance(error, migrate.MigrationError):
        assert result.stderr.strip() == f"error: {error.code}: {error}"


def test_db_commands_need_a_dsn_and_the_alembic_scripts(
    monkeypatch: pytest.MonkeyPatch, migrations: MigrateCalls, tmp_path: Any
) -> None:
    monkeypatch.delenv("AGENT_CONTEXT_POSTGRESQL__DSN")
    result = CliRunner().invoke(cli.app, ["db", "upgrade"])
    assert result.exit_code == cli.EXIT_USAGE
    assert "AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN (or __DSN) is required" in result.stderr

    missing = str(tmp_path / "missing.ini")
    result = CliRunner().invoke(cli.app, ["db", "current", "--alembic-config", missing])
    assert result.exit_code == cli.EXIT_USAGE
    assert result.stderr.startswith("error: alembic_config_missing: ")
    assert migrations.upgrade == []


@pytest.mark.parametrize(
    ("state", "exit_code"),
    [("at_head", cli.EXIT_OK), ("behind", cli.EXIT_FAILED), ("empty", cli.EXIT_FAILED)],
)
def test_db_current_reports_the_state(
    monkeypatch: pytest.MonkeyPatch, migrations: MigrateCalls, state: str, exit_code: int
) -> None:
    revision = None if state == "empty" else "20260929_0001"
    report = migrate.CurrentReport(revision, "20260929_0001", state)  # type: ignore[arg-type]

    async def fake_current(*_args: Any) -> migrate.CurrentReport:
        return report

    monkeypatch.setattr(migrate, "current", fake_current)

    result = CliRunner().invoke(cli.app, ["db", "current"])
    as_json = CliRunner().invoke(cli.app, ["db", "current", "--json"])

    assert result.exit_code == exit_code
    assert result.stdout.strip() == f"revision {revision or 'none'} head 20260929_0001: {state}"
    assert json.loads(as_json.stdout) == {
        "revision": revision,
        "head": "20260929_0001",
        "state": state,
    }
    assert migrations.disposed == 2


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1", "1e12", "2147484"])
def test_db_upgrade_rejects_a_lock_timeout_postgresql_cannot_hold(
    monkeypatch: pytest.MonkeyPatch, migrations: MigrateCalls, value: str
) -> None:
    async def fake_upgrade(*_args: Any, **_options: Any) -> migrate.MigrationReport:
        raise AssertionError("must not upgrade")

    monkeypatch.setattr(migrate, "upgrade", fake_upgrade)

    result = CliRunner().invoke(cli.app, ["db", "upgrade", "--lock-timeout", value])

    assert result.exit_code == cli.EXIT_USAGE
    assert migrations.upgrade == []


def test_db_upgrade_accepts_the_largest_lock_timeout(
    monkeypatch: pytest.MonkeyPatch, migrations: MigrateCalls
) -> None:
    seen: dict[str, Any] = {}

    async def fake_upgrade(engine: Any, config: Any, **options: Any) -> migrate.MigrationReport:
        seen.update(options)
        return _migration_report(applied=False)

    monkeypatch.setattr(migrate, "upgrade", fake_upgrade)

    result = CliRunner().invoke(
        cli.app, ["db", "upgrade", "--lock-timeout", str(migrate.MAX_LOCK_TIMEOUT_SECONDS)]
    )

    assert result.exit_code == cli.EXIT_OK, result.stderr
    assert seen["lock_timeout_seconds"] == migrate.MAX_LOCK_TIMEOUT_SECONDS
