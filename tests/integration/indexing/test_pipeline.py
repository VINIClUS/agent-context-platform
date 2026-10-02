"""``index_checkout`` end to end: real git repo, sandboxed adapters, history lineage, PostgreSQL.

The service connects as the least-privileged ``agent_context_api`` role, as ``agent-context index``
does. No lineage is seeded from HEAD: the pipeline derives it from the first-parent history.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from agent_context_sdk import RedactionPolicyV1
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.indexing import identity
from agent_context_platform.indexing.emitter import (
    INDEXER_PRODUCER_ID,
    IndexingConfig,
    IndexingService,
)
from agent_context_platform.indexing.pipeline import IndexReport, IndexRequest, index_checkout
from agent_context_platform.indexing.tree_sitter import landlock
from agent_context_platform.indexing.tree_sitter.base import StructuralError, StructuralErrorCode
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.projection.verify import NoContentBlobStore
from agent_context_platform.settings import Settings

from .conftest import ledger_engine  # noqa: F401  (fixture)

pytestmark = pytest.mark.integration

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
    "GIT_AUTHOR_NAME": "Pipeline Test",
    "GIT_AUTHOR_EMAIL": "pipeline@example.invalid",
    "GIT_COMMITTER_NAME": "Pipeline Test",
    "GIT_COMMITTER_EMAIL": "pipeline@example.invalid",
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


def commit(root: Path, message: str = "change") -> str:
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", message)
    return git(root, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    write(root, "pkg/calc.py", PY_CALC)
    write(root, "pkg/util.py", PY_UTIL)
    write(root, "web/app.ts", TS_APP)
    write(root, "svc/main.go", GO_MAIN)
    commit(root, "seed")
    return root


@dataclass
class Stack:
    owner: AsyncEngine
    api: AsyncEngine
    repository_id: str

    def settings(self) -> Settings:
        return Settings(indexer_allow_unconfined_adapters=landlock.abi_version() < 1)

    def run(self, request: IndexRequest) -> IndexReport:
        sessions = session_factory(self.api)
        ingestion = IngestionService(
            ContentService(NoContentBlobStore(), RedactionPolicyV1()), sessions
        )
        service = IndexingService(IndexingConfig(self.repository_id), ingestion, sessions)
        return asyncio.run(index_checkout(request, self.settings(), service))

    def index(self, root: Path, **options: object) -> IndexReport:
        return self.run(IndexRequest(self.repository_id, root, **options))  # type: ignore[arg-type]

    def file_ids(self) -> dict[str, set[str]]:
        """Every ``file_id`` the ledger holds per path, over all indexes of this repository."""

        async def read() -> dict[str, set[str]]:
            async with self.owner.connect() as connection:
                rows = await connection.execute(
                    text(
                        "SELECT payload->>'path', payload->>'file_id' FROM ledger.events "
                        "WHERE producer_id = :p AND stream_id = :s "
                        "AND event_type = 'code.file.indexed'"
                    ),
                    {"p": INDEXER_PRODUCER_ID, "s": f"code-index:{self.repository_id}"},
                )
                found: dict[str, set[str]] = {}
                for path, file_id in rows:
                    found.setdefault(path, set()).add(file_id)
                return found

        return asyncio.run(read())


@pytest.fixture
def stack(ledger_engine: AsyncEngine, postgres_dsn: str) -> Iterator[Stack]:  # noqa: F811
    api = create_async_engine(
        postgres_dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    yield Stack(ledger_engine, api, f"repo-039c-{uuid.uuid4().hex[:8]}")
    asyncio.run(api.dispose())


def test_index_then_reindex_unchanged_submits_nothing(stack: Stack, repo: Path) -> None:
    first = stack.index(repo)
    assert first.success and first.error_class is None, first
    assert first.target_kind == "commit" and first.target == git(repo, "rev-parse", "HEAD")
    assert first.submitted > 0 and first.skipped == 0 and first.files == 4

    again = stack.index(repo)
    assert again.success
    assert again.submitted == 0 and again.skipped == first.submitted
    assert again.index_id == first.index_id and again.target == first.target


def test_a_commit_with_an_edit_and_a_rename_keeps_the_file_logical_ids(
    stack: Stack, repo: Path
) -> None:
    first_commit = git(repo, "rev-parse", "HEAD")
    assert stack.index(repo).success
    before = stack.file_ids()
    write(repo, "pkg/calc.py", PY_CALC_V2)
    git(repo, "mv", "pkg/util.py", "pkg/helpers.py")
    second_commit = commit(repo)
    report = stack.index(repo)
    assert report.success and report.target == second_commit != first_commit
    after = stack.file_ids()

    util = identity.file_logical_id(stack.repository_id, "pkg/util.py", first_commit)
    assert before["pkg/util.py"] == {str(util)}
    assert after["pkg/helpers.py"] == {str(util)}  # renamed: same File, never re-identified
    assert after["pkg/calc.py"] == before["pkg/calc.py"]  # edited: same File, new revision
    assert len(after["pkg/calc.py"]) == 1


def test_a_dirty_checkout_targets_a_snapshot_with_provisional_ids(stack: Stack, repo: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    write(repo, "pkg/calc.py", PY_CALC_V2)
    write(repo, "pkg/fresh.py", "def fresh() -> int:\n    return 1\n")
    report = stack.index(repo)
    assert report.success, report
    assert report.target_kind == "snapshot" and report.target != head
    ids = stack.file_ids()
    fresh = identity.uncommitted_file_logical_id(stack.repository_id, "pkg/fresh.py")
    assert ids["pkg/fresh.py"] == {str(fresh)}
    committed = identity.file_logical_id(stack.repository_id, "pkg/calc.py", head)
    assert ids["pkg/calc.py"] == {str(committed)}  # tracked: its history lineage, not provisional


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads files regardless of mode")
def test_an_unreadable_dirty_code_file_makes_the_snapshot_incomplete(
    stack: Stack, repo: Path
) -> None:
    secret = repo / "pkg" / "locked.py"
    write(repo, "pkg/locked.py", "def locked() -> int:\n    return 1\n")
    secret.chmod(0)
    try:
        report = stack.index(repo)
    finally:
        secret.chmod(0o644)
    assert not report.success and report.error_class == "snapshot_incomplete"
    assert report.submitted > 0 and report.index_id is not None


def test_a_shallow_checkout_is_a_failed_report_with_no_events(
    stack: Stack, repo: Path, tmp_path: Path
) -> None:
    write(repo, "pkg/calc.py", PY_CALC_V2)
    commit(repo)
    shallow = tmp_path / "shallow"
    git(
        repo, "-c", "protocol.file.allow=always", "clone", "-q", "--depth", "1",
        f"file://{repo}", str(shallow),
    )  # fmt: skip
    report = stack.index(shallow)
    assert not report.success and report.error_class == "shallow_history"
    assert report.submitted == 0 and report.index_id is None
    assert stack.file_ids() == {}


def test_a_report_never_carries_content_or_paths(stack: Stack, repo: Path) -> None:
    report = stack.index(repo)
    rendered = repr(report.to_dict())
    for needle in ("calc.py", "def add", str(repo), "hello"):
        assert needle not in rendered


def test_a_scip_index_is_imported_with_the_history_lineage(stack: Stack, tmp_path: Path) -> None:
    fixtures = Path(__file__).resolve().parents[2] / "fixtures" / "scip"
    root = tmp_path / "scip-repo"
    root.mkdir()
    git(root, "init", "-q")
    for source in sorted((fixtures / "scip-python-basic-src" / "pkg").iterdir()):
        content = source.read_bytes()
        if source.name == "shapes.py":
            # The committed index predates a second blank line in the fixture source.
            content = content.replace(b"import math\n\n\nclass", b"import math\n\nclass")
        target = root / "pkg" / source.name
        target.parent.mkdir(exist_ok=True)
        target.write_bytes(content)
    seed = commit(root, "seed")
    write(root, "pkg/extra.py", "def extra() -> int:\n    return 1\n")
    commit(root, "later")
    # A SCIP index of the seed commit is not the checked-out HEAD: its files still at the seed
    # bytes bind, and the run is not a failure of the pipeline.
    scip = fixtures / "scip-python-basic.scip"
    report = stack.index(root, scip=scip)
    assert report.error_class in (None, "files_degraded"), report
    assert report.submitted > 0
    ids = stack.file_ids()
    assert ids["pkg/shapes.py"] == {
        str(identity.file_logical_id(stack.repository_id, "pkg/shapes.py", seed))
    }


def test_an_unusable_scip_index_is_a_failed_report(
    stack: Stack, repo: Path, tmp_path: Path
) -> None:
    junk = tmp_path / "junk.scip"
    junk.write_bytes(b"\xff\xff not a protobuf")
    report = stack.index(repo, scip=junk)
    assert not report.success and (report.error_class or "").startswith("scip_")
    assert report.submitted == 0


def test_a_service_for_another_repository_is_refused_before_anything_is_built(
    stack: Stack, repo: Path
) -> None:
    other = IndexRequest(f"{stack.repository_id}-other", repo)
    report = stack.run(other)
    assert not report.success and report.error_class == "repository_mismatch"
    assert report.submitted == 0 and report.index_id is None
    assert stack.file_ids() == {}


def test_a_structural_refusal_is_a_failed_report_not_an_exception(
    stack: Stack, repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*_args: object, **_kwargs: object) -> object:
        raise StructuralError(StructuralErrorCode.SANDBOX_UNAVAILABLE)

    monkeypatch.setattr("agent_context_platform.indexing.pipeline.parse_structural", refuse)
    report = stack.index(repo)
    assert not report.success and report.error_class == "structural_sandbox_unavailable"
    assert report.submitted == 0 and report.diagnostics == {"structural_sandbox_unavailable": 1}
    assert stack.file_ids() == {}
