"""`agent-context projection run` against real services (PLATFORM-039D).

The worker drains the G3 fixture ledger that `test_rebuild.py` builds and must reproduce its
golden digest; a poison event dead-letters (exit 1); a held rebuild lock pauses claiming; and the
installed entry point, run as a subprocess, shuts down cleanly on SIGTERM without a lease behind.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.projection import verify
from e2e.test_rebuild import (  # noqa: F401  (login_roles is a fixture)
    GOLDEN_DIGEST,
    World,
    build_ledger,
    graph_facts,
    ingest_drafts,
    invoke,
    login_roles,
    new_session_drafts,
    sql,
)

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def ledger(
    postgres_dsn: str,
    ledger_engine: AsyncEngine,
    login_roles: dict[str, str],  # noqa: F811
    tmp_path_factory: Any,
) -> Iterator[World]:
    """The fixture ledger, ingested but NOT projected: the worker does that."""
    root = tmp_path_factory.mktemp("worker-repo")
    count = asyncio.run(build_ledger(postgres_dsn, root))
    owner = create_async_engine(postgres_dsn, poolclass=NullPool)
    yield World(postgres_dsn, owner, None, count, login_roles)  # type: ignore[arg-type]
    asyncio.run(owner.dispose())


def report(result: Any) -> dict[str, Any]:
    """The JSON summary on stdout (progress logs go to stderr)."""
    assert result.stdout.strip(), result.stderr
    return json.loads(result.stdout)  # type: ignore[no-any-return]


def count_status(world: World, status: str) -> int:
    rows = asyncio.run(
        sql(world.owner, "SELECT count(*) FROM projection.outbox WHERE status = :s", s=status)
    )
    return int(rows[0][0])


def test_run_once_projects_the_fixture_ledger_to_the_golden_digest(ledger: World) -> None:
    result = invoke(["projection", "run", "--once", "--json"], ledger.env(api=None))

    assert result.exit_code == 0, result.output
    summary = report(result)
    assert summary["outcome"] == "drained"
    assert summary["delivered"] == ledger.event_count == summary["claimed"]
    assert summary["dead_lettered"] == 0 and summary["lost_leases"] == 0
    assert all(item["lag"] == 0 and item["checkpoint_outbox_id"] for item in summary["projectors"])
    assert asyncio.run(graph_facts()).digest == GOLDEN_DIGEST
    assert count_status(ledger, "delivered") == ledger.event_count

    # Nothing left: a second drain claims nothing and still exits 0.
    again = report(invoke(["projection", "run", "--once", "--json"], ledger.env(api=None)))
    assert again["claimed"] == 0


def test_a_poison_event_is_dead_lettered_and_exits_1(ledger: World) -> None:
    [poison] = new_session_drafts(1, "worker-poison")
    asyncio.run(ingest_drafts(ledger.dsn, [poison]))
    asyncio.run(
        sql(
            ledger.owner,
            # A payload that no longer matches its sealed digest: EventIntegrityError on load.
            "UPDATE ledger.events SET payload = payload || '{\"tampered\": true}'::jsonb "
            "WHERE event_id = :event",
            event=poison.event_id,
        )
    )
    # One attempt left, so the first failure dead-letters instead of waiting out the backoff.
    asyncio.run(
        sql(
            ledger.owner,
            "UPDATE projection.outbox SET retry_count = 4 WHERE event_id = :event",
            event=poison.event_id,
        )
    )

    result = invoke(["projection", "run", "--once", "--json"], ledger.env(api=None))

    assert result.exit_code == 1, result.output
    summary = report(result)
    assert summary["dead_lettered"] == 1 and summary["outcome"] == "drained"
    assert count_status(ledger, "dead_lettered") == 1
    assert "tampered" not in result.output and ledger.roles["projector"] not in result.output
    # A later run has no NEW dead letters, so it is clean again.
    assert invoke(["projection", "run", "--once"], ledger.env(api=None)).exit_code == 0


def test_a_held_rebuild_lock_pauses_claiming_and_releasing_it_resumes(ledger: World) -> None:
    asyncio.run(ingest_drafts(ledger.dsn, new_session_drafts(3, "worker-lock")))
    sync_dsn = ledger.roles["readonly"].replace("postgresql+psycopg", "postgresql")
    args = ["projection", "run", "--once", "--max-seconds", "2", "--poll-seconds", "0.05", "--json"]

    with psycopg.connect(sync_dsn, autocommit=True) as rebuild:
        rebuild.execute("SELECT pg_advisory_lock(%s)", (verify.REBUILD_LOCK_KEY,))
        paused = invoke(args, ledger.env(api=None))
        rebuild.execute("SELECT pg_advisory_unlock(%s)", (verify.REBUILD_LOCK_KEY,))

    # `--max-seconds` expiring is exit 1 and `timeout`; the lock meant zero claims.
    assert paused.exit_code == 1, paused.output
    summary = report(paused)
    assert summary["outcome"] == "timeout"
    assert summary["claimed"] == 0 and summary["paused_for_rebuild"] > 0
    assert "paused_for_rebuild" in paused.stderr
    assert count_status(ledger, "pending") == 3 and count_status(ledger, "leased") == 0

    resumed = invoke(["projection", "run", "--once", "--json"], ledger.env(api=None))
    assert resumed.exit_code == 0, resumed.output
    assert report(resumed)["delivered"] == 3
    assert count_status(ledger, "pending") == 0
    # The probe left no advisory lock behind, so a rebuild can take it.
    held = asyncio.run(sql(ledger.owner, "SELECT count(*) FROM pg_locks WHERE locktype='advisory'"))
    assert held[0][0] == 0


def test_sigterm_releases_leases_removes_the_health_file_and_exits_0(
    ledger: World, tmp_path: Path
) -> None:
    executable = Path(sys.executable).parent / "agent-context"
    assert executable.exists(), "the project scripts are not installed"
    asyncio.run(ingest_drafts(ledger.dsn, new_session_drafts(40, "worker-term")))
    health = tmp_path / "projector.health"

    worker = subprocess.Popen(
        [str(executable), "projection", "run", "--batch-size", "5", "--health-file", str(health)],
        env={"PATH": os.environ.get("PATH", ""), **ledger.env(api=None)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 120
        while not health.exists():  # touched once the worker is up and has made a pass
            assert worker.poll() is None, worker.communicate()
            assert time.monotonic() < deadline, "the worker never touched its health file"
            time.sleep(0.1)
        first_touch = health.stat().st_mtime_ns
        while health.stat().st_mtime_ns == first_touch and time.monotonic() < deadline:
            time.sleep(0.05)  # the file is touched again after each loop iteration
        assert health.stat().st_mtime_ns != first_touch
        worker.send_signal(signal.SIGTERM)
        stdout, stderr = worker.communicate(timeout=120)
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.communicate()

    assert worker.returncode == 0, stderr
    assert not health.exists()
    assert count_status(ledger, "leased") == 0
    assert "stopped" in stderr and ledger.roles["projector"] not in stderr + stdout
    # Whatever it left pending is simply claimable: a drain finishes the job.
    assert invoke(["projection", "run", "--once"], ledger.env(api=None)).exit_code == 0
    assert count_status(ledger, "pending") == 0
