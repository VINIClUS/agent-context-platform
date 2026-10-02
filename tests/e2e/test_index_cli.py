"""``agent-context index`` end to end (PLATFORM-039C).

A temp git repository with Python, TypeScript and Go files is indexed through the real CLI as the
least-privileged ``agent_context_api`` role (adapters confined by Landlock when the kernel has it),
projected by the real projection runner and checked with ``agent-context projection verify``.
Lineage is never seeded: it comes from the repository's first-parent history.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from integration.projection.conftest import neo4j_integration_settings, role_scoped_engine
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine
from typer.testing import CliRunner

from agent_context_platform.cli import app
from agent_context_platform.db import session_factory
from agent_context_platform.indexing import identity
from agent_context_platform.indexing.emitter import INDEXER_PRODUCER_ID
from agent_context_platform.indexing.tree_sitter import landlock
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.registry import registered_projectors
from agent_context_platform.projection.runtime import ProjectionRunner

pytestmark = pytest.mark.e2e

PY_CALC = "def add(left: int, right: int) -> int:\n    return left + right\n"
PY_CALC_V2 = PY_CALC + "\n\ndef sub(left: int, right: int) -> int:\n    return left - right\n"
PY_UTIL = "from .calc import add\n\n\ndef twice(value: int) -> int:\n    return add(value, value)\n"
TS_APP = (
    "export class Hello {\n"
    "  greet(name: string): string { return `hello ${name}`; }\n"
    "}\n"
    "export function main(): string { return new Hello().greet('world'); }\n"
)
GO_MAIN = (
    "package svc\n\n"
    "type Server struct{ Name string }\n\n"
    'func Start() Server { return Server{Name: "main"} }\n'
)
_GIT_ENV = {
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_AUTHOR_NAME": "Index CLI Test",
    "GIT_AUTHOR_EMAIL": "index@example.invalid",
    "GIT_COMMITTER_NAME": "Index CLI Test",
    "GIT_COMMITTER_EMAIL": "index@example.invalid",
    "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
    "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
}


def git(root: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main", *args],
        cwd=root,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(root.parent), **_GIT_ENV},
        capture_output=True,
        check=True,
        text=True,
    )
    return done.stdout.strip()


def write(root: Path, path: str, content: str) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)


def commit(root: Path) -> str:
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "change")
    return git(root, "rev-parse", "HEAD")


def as_role(dsn: str, role: str) -> str:
    """The same database, every connection running as ``role`` (a member of a migration role)."""
    url = make_url(dsn).update_query_dict({"options": f"-c role={role}"})
    return url.render_as_string(hide_password=False)


@pytest.fixture
def repository_id() -> str:
    return f"repo-039c-e2e-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "checkout"
    root.mkdir()
    git(root, "init", "-q")
    write(root, "pkg/calc.py", PY_CALC)
    write(root, "pkg/util.py", PY_UTIL)
    write(root, "web/app.ts", TS_APP)
    write(root, "svc/main.go", GO_MAIN)
    commit(root)
    return root


@pytest.fixture
def env(postgres_dsn: str, ledger_engine: AsyncEngine) -> dict[str, str]:
    """Least-privilege roles only: no owner DSN is available to the CLI."""
    neo4j = neo4j_integration_settings()
    environment = {
        "AGENT_CONTEXT_POSTGRESQL__API_DSN": as_role(postgres_dsn, "agent_context_api"),
        "AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN": as_role(postgres_dsn, "agent_context_projector"),
        "AGENT_CONTEXT_NEO4J__URI": str(neo4j.uri),
        "AGENT_CONTEXT_NEO4J__USERNAME": neo4j.username or "",
        "AGENT_CONTEXT_NEO4J__PASSWORD": neo4j.password.get_secret_value()
        if neo4j.password
        else "",
        "AGENT_CONTEXT_NEO4J__DATABASE": neo4j.database,
    }
    if landlock.abi_version() < 1:
        environment["AGENT_CONTEXT_INDEXER_ALLOW_UNCONFINED_ADAPTERS"] = "true"
    return environment


def index(env: dict[str, str], repository_id: str, root: Path, *extra: str) -> Any:
    args = ["index", "--repository-id", repository_id, "--checkout", str(root), "--json", *extra]
    return CliRunner().invoke(app, args, env=env)


def report(result: Any) -> dict[str, Any]:
    assert result.output.strip(), result.stderr
    return json.loads(result.output)  # type: ignore[no-any-return]


def file_ids(owner: AsyncEngine, repository_id: str) -> dict[str, set[str]]:
    async def read() -> dict[str, set[str]]:
        async with owner.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT payload->>'path', payload->>'file_id' FROM ledger.events "
                    "WHERE producer_id = :p AND stream_id = :s AND event_type = 'code.file.indexed'"
                ),
                {"p": INDEXER_PRODUCER_ID, "s": f"code-index:{repository_id}"},
            )
            found: dict[str, set[str]] = {}
            for path, file_id in rows:
                found.setdefault(path, set()).add(file_id)
            return found

    return asyncio.run(read())


def test_index_reindex_edit_rename_and_dirty(
    env: dict[str, str], ledger_engine: AsyncEngine, repo: Path, repository_id: str
) -> None:
    seed = git(repo, "rev-parse", "HEAD")
    first = index(env, repository_id, repo)
    assert first.exit_code == 0, first.output
    done = report(first)
    assert done["success"] is True and done["error_class"] is None
    assert done["target"] == {"kind": "commit", "id": seed}
    assert done["submitted"] > 0 and done["skipped"] == 0 and done["files"] == 4
    before = file_ids(ledger_engine, repository_id)

    again = report(index(env, repository_id, repo))
    assert again["submitted"] == 0 and again["skipped"] == done["submitted"]
    assert again["index_id"] == done["index_id"]

    # An edit plus a rename in one commit: the File keeps its logical ID.
    write(repo, "pkg/calc.py", PY_CALC_V2)
    git(repo, "mv", "pkg/util.py", "pkg/helpers.py")
    second = commit(repo)
    moved = report(index(env, repository_id, repo))
    assert moved["success"] and moved["target"]["id"] == second and moved["submitted"] > 0
    after = file_ids(ledger_engine, repository_id)
    util = str(identity.file_logical_id(repository_id, "pkg/util.py", seed))
    assert before["pkg/util.py"] == {util} and after["pkg/helpers.py"] == {util}
    assert after["pkg/calc.py"] == before["pkg/calc.py"]

    # Dirty: the snapshot is the target, an untracked file is provisional.
    write(repo, "pkg/fresh.py", "def fresh() -> int:\n    return 1\n")
    dirty = report(index(env, repository_id, repo))
    assert dirty["success"] and dirty["target"]["kind"] == "snapshot"
    assert dirty["target"]["id"] != second
    provisional = identity.uncommitted_file_logical_id(repository_id, "pkg/fresh.py")
    assert file_ids(ledger_engine, repository_id)["pkg/fresh.py"] == {str(provisional)}


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads files regardless of mode")
def test_an_unreadable_code_file_fails_with_snapshot_incomplete_and_still_prints_counts(
    env: dict[str, str], repo: Path, repository_id: str
) -> None:
    locked = repo / "pkg" / "locked.py"
    write(repo, "pkg/locked.py", "def locked() -> int:\n    return 1\n")
    locked.chmod(0)
    try:
        result = index(env, repository_id, repo)
    finally:
        locked.chmod(0o644)
    assert result.exit_code == 1, result.output
    failed = report(result)
    assert failed["success"] is False and failed["error_class"] == "snapshot_incomplete"
    assert failed["submitted"] > 0 and failed["index_id"]


def test_usage_errors_exit_2_and_a_shallow_checkout_exits_1(
    env: dict[str, str], repo: Path, repository_id: str, tmp_path: Path
) -> None:
    runner = CliRunner()
    assert runner.invoke(app, ["index", "--checkout", str(repo)], env=env).exit_code == 2
    assert runner.invoke(app, ["index", "--repository-id", repository_id], env=env).exit_code == 2
    missing = index(env, repository_id, tmp_path / "missing")
    assert missing.exit_code == 2
    no_scip = index(env, repository_id, repo, "--scip", str(tmp_path / "missing.scip"))
    assert no_scip.exit_code == 2
    assert index(env, "bad\nid", repo).exit_code == 2

    write(repo, "pkg/calc.py", PY_CALC_V2)
    commit(repo)
    shallow = tmp_path / "shallow"
    git(
        repo, "-c", "protocol.file.allow=always", "clone", "-q", "--depth", "1",
        f"file://{repo}", str(shallow),
    )  # fmt: skip
    refused = index(env, repository_id, shallow)
    assert refused.exit_code == 1
    body = report(refused)
    assert body["error_class"] == "shallow_history" and body["submitted"] == 0


def test_the_output_carries_no_dsn_secret_or_content(
    env: dict[str, str], repo: Path, repository_id: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("DEBUG")
    result = index(env, repository_id, repo)
    assert result.exit_code == 0, result.output
    password = make_url(env["AGENT_CONTEXT_POSTGRESQL__API_DSN"]).password or ""
    text_out = result.output + result.stderr + caplog.text
    for needle in (password, env["AGENT_CONTEXT_NEO4J__PASSWORD"], "def add", "greet", "calc.py"):
        assert needle and needle not in text_out
    plain = CliRunner().invoke(
        app,
        ["index", "--repository-id", repository_id, "--checkout", str(repo)],
        env=env,
    )
    assert plain.exit_code == 0 and "already present" in plain.output


async def _project(owner_dsn: str) -> None:
    engine = role_scoped_engine(owner_dsn, "agent_context_projector")
    try:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            runner = ProjectionRunner(
                session_factory(engine), store, registered_projectors(), worker_id="e2e-index"
            )
            for _ in range(200):
                if (await runner.run_once(200)).claimed == 0:
                    return
    finally:
        await engine.dispose()
    raise AssertionError("the projection runner did not drain the outbox")


def test_the_projection_of_an_index_verifies(
    env: dict[str, str], postgres_dsn: str, repo: Path, repository_id: str
) -> None:
    assert index(env, repository_id, repo).exit_code == 0
    write(repo, "pkg/calc.py", PY_CALC_V2)
    git(repo, "mv", "pkg/util.py", "pkg/helpers.py")
    commit(repo)
    assert index(env, repository_id, repo).exit_code == 0
    write(repo, "pkg/fresh.py", "def fresh() -> int:\n    return 1\n")
    assert index(env, repository_id, repo).exit_code == 0  # a snapshot target too

    asyncio.run(_project(postgres_dsn))
    verified = CliRunner().invoke(
        app, ["projection", "verify", "--no-record", "--require-caught-up", "--json"], env=env
    )
    assert verified.exit_code == 0, verified.output
    assert report(verified)["ok"] is True


def test_the_installed_entry_point_indexes_a_checkout(
    env: dict[str, str], repo: Path, repository_id: str
) -> None:
    entry = Path(sys.executable).parent / "agent-context"
    assert entry.is_file()
    command: Sequence[str] = [
        str(entry), "index", "--repository-id", repository_id, "--checkout", str(repo), "--json",
    ]  # fmt: skip
    process_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(repo.parent), **env}
    done = subprocess.run(command, env=process_env, capture_output=True, text=True, check=False)
    assert done.returncode == 0, done.stderr[-500:]
    body = json.loads(done.stdout)
    assert body["success"] is True and body["submitted"] > 0
    assert make_url(env["AGENT_CONTEXT_POSTGRESQL__API_DSN"]).password not in (
        done.stdout + done.stderr
    )
