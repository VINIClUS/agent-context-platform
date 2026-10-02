"""`agent-context projection run`: the worker loop, exit codes and shutdown (PLATFORM-039D)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.projection.runtime import OutboxBacklog, ProjectionRunReport

pytestmark = pytest.mark.unit

PASSWORD = "s3cret-password"
DSN = f"postgresql+psycopg://operator:{PASSWORD}@db.example.test/agent"


class FakeRunner:
    """Scripted reports; `outstanding` rows remain until the script is exhausted."""

    def __init__(self, reports: list[ProjectionRunReport], *, outstanding: int = 0) -> None:
        self.reports = list(reports)
        self.outstanding = outstanding
        self.batches: list[int] = []
        self.released = 0

    async def run_once(self, limit: int) -> ProjectionRunReport:
        self.batches.append(limit)
        return self.reports.pop(0) if self.reports else ProjectionRunReport()

    async def backlog(self) -> OutboxBacklog:
        return OutboxBacklog(pending=0 if not self.reports else self.outstanding)

    async def release_leases(self) -> int:
        self.released += 1
        return 0


def drive(
    runner: FakeRunner,
    *,
    held: Callable[[int], bool] = lambda _call: False,
    stop: asyncio.Event | None = None,
    once: bool = True,
    max_seconds: float | None = None,
    log_seconds: float = 3600.0,
    now: Callable[[], float] | None = None,
    log: list[str] | None = None,
    touches: list[int] | None = None,
) -> cli.WorkerTotals:
    calls = 0

    async def probe() -> bool:
        nonlocal calls
        calls += 1
        return held(calls)

    async def main() -> cli.WorkerTotals:
        return await cli._drive_worker(
            runner,
            probe,
            stop or asyncio.Event(),
            once=once,
            batch_size=7,
            poll_seconds=0.01,
            max_seconds=max_seconds,
            log_seconds=log_seconds,
            touch=lambda: (touches if touches is not None else []).append(1),
            log=(log if log is not None else []).append,
            **({} if now is None else {"now": now}),
        )

    return asyncio.run(main())


def test_once_drains_until_nothing_is_outstanding() -> None:
    runner = FakeRunner(
        [
            ProjectionRunReport(claimed=2, delivered=1, retried=1),
            ProjectionRunReport(claimed=1, dead_lettered=1),
        ],
        outstanding=1,
    )
    touches: list[int] = []
    totals = drive(runner, touches=touches)
    assert totals.outcome == "drained"
    assert (totals.claimed, totals.delivered, totals.retried, totals.dead_lettered) == (3, 1, 1, 1)
    assert runner.batches == [7, 7, 7]
    assert len(touches) == 3
    assert (
        cli._worker_exit_code(totals, once=True) == cli.EXIT_FAILED
    )  # something was dead-lettered


def test_once_waits_out_a_retry_delay_instead_of_exiting() -> None:
    # Nothing claimable, but one row is still pending (backoff): keep polling until it is done.
    runner = FakeRunner(
        [ProjectionRunReport(), ProjectionRunReport(), ProjectionRunReport(claimed=1, delivered=1)],
        outstanding=1,
    )
    totals = drive(runner)
    assert totals.outcome == "drained" and totals.delivered == 1
    assert cli._worker_exit_code(totals, once=True) == cli.EXIT_OK


def test_max_seconds_times_out_with_exit_1() -> None:
    clock = iter(range(1000))
    runner = FakeRunner([ProjectionRunReport()] * 50, outstanding=1)
    totals = drive(runner, max_seconds=5, now=lambda: float(next(clock)))
    assert totals.outcome == "timeout"
    assert cli._worker_exit_code(totals, once=True) == cli.EXIT_FAILED


def test_a_held_rebuild_lock_pauses_claiming_and_releasing_it_resumes() -> None:
    runner = FakeRunner([ProjectionRunReport(claimed=1, delivered=1)])
    log: list[str] = []
    totals = drive(runner, held=lambda call: call <= 3, log=log)
    assert totals.paused_for_rebuild == 3
    assert log == ["paused_for_rebuild", "resumed_after_rebuild"]  # logged once per pause
    assert totals.delivered == 1 and totals.outcome == "drained"
    assert runner.batches[0:1] == [7] and len(runner.batches) == 2  # zero claims while held


def test_stop_request_ends_the_loop_without_claiming_again() -> None:
    stop = asyncio.Event()

    class StoppingRunner(FakeRunner):
        async def run_once(self, limit: int) -> ProjectionRunReport:
            stop.set()
            return ProjectionRunReport(claimed=1, delivered=1)

    runner = StoppingRunner([])
    totals = drive(runner, stop=stop, once=False)
    assert totals.outcome == "stopped" and totals.delivered == 1
    assert cli._worker_exit_code(totals, once=False) == cli.EXIT_OK


def test_a_stopped_long_running_worker_exits_0_even_after_dead_letters() -> None:
    stopped = cli.WorkerTotals(dead_lettered=2, outcome="stopped")
    assert cli._worker_exit_code(stopped, once=False) == cli.EXIT_OK
    assert cli._worker_exit_code(stopped, once=True) == cli.EXIT_FAILED
    timed_out = cli.WorkerTotals(outcome="timeout")
    assert cli._worker_exit_code(timed_out, once=False) == cli.EXIT_FAILED


def test_idle_backoff_is_capped_and_never_overflows() -> None:
    assert cli._idle_delay(1.0, 1) == 2.0
    assert cli._idle_delay(1.0, 10_000) == cli.WORKER_MAX_IDLE_SECONDS
    assert cli._idle_delay(10.0, 10_000) == 10.0  # a poll interval above the cap is kept


def test_progress_lines_are_content_free_counts() -> None:
    runner = FakeRunner([ProjectionRunReport(claimed=1, delivered=1)], outstanding=2)
    log: list[str] = []
    drive(runner, log_seconds=0.0, log=log)
    progress = [line for line in log if line.startswith("progress")]
    assert progress and all(" lag=" in line and "dead_lettered=" in line for line in progress)


def test_a_stop_during_a_slow_probe_claims_nothing_more() -> None:
    stop = asyncio.Event()
    runner = FakeRunner([ProjectionRunReport(claimed=1, delivered=1)])

    async def slow_probe() -> bool:
        await asyncio.sleep(0.01)  # the signal lands while the probe is in flight
        stop.set()
        return False

    async def main() -> cli.WorkerTotals:
        return await cli._drive_worker(
            runner,
            slow_probe,
            stop,
            once=False,
            batch_size=7,
            poll_seconds=0.01,
            max_seconds=None,
            log_seconds=3600.0,
            touch=lambda: None,
            log=lambda _message: None,
        )

    totals = asyncio.run(main())
    assert runner.batches == [] and totals.claimed == 0 and totals.outcome == "stopped"


def test_a_deadline_reached_during_a_slow_probe_claims_nothing_more() -> None:
    clock = [0.0]
    runner = FakeRunner([ProjectionRunReport(claimed=1, delivered=1)])

    async def slow_probe() -> bool:
        clock[0] = 10.0  # the deadline passes while the probe is in flight
        return False

    async def main() -> cli.WorkerTotals:
        return await cli._drive_worker(
            runner,
            slow_probe,
            asyncio.Event(),
            once=True,
            batch_size=7,
            poll_seconds=0.01,
            max_seconds=5,
            log_seconds=3600.0,
            touch=lambda: None,
            log=lambda _message: None,
            now=lambda: clock[0],
        )

    totals = asyncio.run(main())
    assert runner.batches == [] and totals.outcome == "timeout"


class FakeStore:
    closed = False


@pytest.fixture
def worker(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {"released": 0, "drive_error": None, "outcome": "drained"}
    for name, value in {
        "AGENT_CONTEXT_POSTGRESQL__DSN": DSN,
        "AGENT_CONTEXT_NEO4J__URI": "bolt://live.example.test:7687",
        "AGENT_CONTEXT_NEO4J__USERNAME": "neo4j",
        "AGENT_CONTEXT_NEO4J__PASSWORD": PASSWORD,
    }.items():
        monkeypatch.setenv(name, value)

    @asynccontextmanager
    async def runtime(_settings: Any, *, record: bool = False) -> AsyncIterator[cli.Runtime]:
        state["runtime_opened"] = True
        yield cli.Runtime(None, None, FakeStore())  # type: ignore[arg-type]
        state["runtime_closed"] = True

    class Runner:
        def __init__(self, *_args: Any, worker_id: str, **_kwargs: Any) -> None:
            state["worker_id"] = worker_id

        async def release_leases(self) -> int:
            state["released"] += 1
            return 0

    async def drive_worker(*_args: Any, **kwargs: Any) -> cli.WorkerTotals:
        kwargs["touch"]()
        if state["drive_error"] is not None:
            raise state["drive_error"]
        return cli.WorkerTotals(
            claimed=1, delivered=1, outcome=state["outcome"], dead_lettered=state.get("dead", 0)
        )

    async def status(*_args: Any) -> list[Any]:
        return []

    monkeypatch.setattr(cli, "_runtime", runtime)
    monkeypatch.setattr(cli, "ProjectionRunner", Runner)
    monkeypatch.setattr(cli, "_drive_worker", drive_worker)
    monkeypatch.setattr(cli, "projection_status", status)
    return state


def test_run_prints_the_summary_and_removes_the_health_file(
    worker: dict[str, Any], tmp_path: Path
) -> None:
    health = tmp_path / "health"
    result = CliRunner().invoke(
        cli.app, ["projection", "run", "--once", "--json", "--health-file", str(health)]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["outcome"] == "drained" and payload["delivered"] == 1
    assert not health.exists()
    assert worker["released"] == 1 and worker["runtime_closed"]
    assert PASSWORD not in result.output and "db.example.test" not in result.output


def test_run_exit_codes_for_dead_letters_and_timeout(worker: dict[str, Any]) -> None:
    worker["dead"] = 1
    assert CliRunner().invoke(cli.app, ["projection", "run", "--once"]).exit_code == 1
    worker["dead"] = 0
    worker["outcome"] = "timeout"
    timed_out = CliRunner().invoke(cli.app, ["projection", "run", "--once", "--max-seconds", "1"])
    assert timed_out.exit_code == 1 and "timeout" in timed_out.stdout


def test_run_releases_leases_and_reports_the_class_when_the_loop_fails(
    worker: dict[str, Any], tmp_path: Path
) -> None:
    worker["drive_error"] = ConnectionError(f"could not connect to {DSN}")
    health = tmp_path / "health"
    result = CliRunner().invoke(cli.app, ["projection", "run", "--health-file", str(health)])
    assert result.exit_code == 1
    assert worker["released"] == 1 and not health.exists()
    assert "ConnectionError" in result.stderr and PASSWORD not in result.stderr


def test_run_usage_errors_exit_2(worker: dict[str, Any], tmp_path: Path) -> None:
    missing = tmp_path / "nope" / "health"
    assert (
        CliRunner().invoke(cli.app, ["projection", "run", "--health-file", str(missing)]).exit_code
        == 2
    )
    assert CliRunner().invoke(cli.app, ["projection", "run", "--max-seconds", "0"]).exit_code == 2
    assert "runtime_opened" not in worker


def test_the_worker_builds_its_projectors_through_one_factory(
    worker: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    from agent_context_platform.projection.registry import registered_projectors
    from agent_context_platform.settings import Settings

    assert cli.build_projectors(Settings()) == registered_projectors()
    built: list[object] = []
    monkeypatch.setattr(cli, "build_projectors", lambda settings: built.append(settings) or ())
    assert CliRunner().invoke(cli.app, ["projection", "run", "--once"]).exit_code == 0
    assert len(built) == 1


def test_an_interrupted_once_exits_1_and_is_not_a_drain() -> None:
    stop = asyncio.Event()

    class StoppingRunner(FakeRunner):
        async def run_once(self, limit: int) -> ProjectionRunReport:
            stop.set()
            return ProjectionRunReport(claimed=1, delivered=1)

        async def backlog(self) -> OutboxBacklog:
            return OutboxBacklog(pending=5)

    totals = drive(StoppingRunner([]), stop=stop, once=True)
    assert totals.outcome == "interrupted"
    assert cli._worker_exit_code(totals, once=True) == cli.EXIT_FAILED
