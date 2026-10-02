"""The indexer end to end: real git repo, real Python parser, SCIP fixture, PostgreSQL ledger.

The service connects as the least-privileged ``agent_context_api`` role and ingests through the
in-process ``IngestionService`` (no HTTP, no producer registration).
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import subprocess
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from agent_context_sdk import RedactionPolicyV1
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from agent_context_platform.content.blob_store import S3BlobStore
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.indexing import identity
from agent_context_platform.indexing.emitter import (
    INDEXER_PRODUCER_ID,
    IndexingConfig,
    IndexingError,
    IndexingService,
    blob_oid,
    claim_key,
    event_id_for,
    parse_structural,
    read_sources,
)
from agent_context_platform.indexing.lineage import derive_lineage
from agent_context_platform.indexing.scanner import RepositoryScan, scan_repository
from agent_context_platform.indexing.scip import import_scip
from agent_context_platform.indexing.tree_sitter import python as python_adapter
from agent_context_platform.indexing.tree_sitter.base import (
    ParsedDiagnostic,
    ParsedModule,
    ParseRequest,
)
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.settings import S3Settings

pytestmark = pytest.mark.integration

REPO_ID = "repo-037-it"
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "scip"
SOURCES = FIXTURES / "scip-python-basic-src" / "pkg"
CIRCLE = "scip-python python fixturepkg 0.0.1 `pkg.shapes`/Circle#"
HELPER = "from .shapes import describe\n\n\ndef helper() -> None:\n    describe(None)\n"
NOW = datetime(2026, 3, 1, tzinfo=UTC)
_GIT_ENV = {
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_AUTHOR_NAME": "Indexer Test",
    "GIT_AUTHOR_EMAIL": "indexer@example.invalid",
    "GIT_COMMITTER_NAME": "Indexer Test",
    "GIT_COMMITTER_EMAIL": "indexer@example.invalid",
    "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
    "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
}


class InProcessPython:
    language = "python"

    def parse(self, request: ParseRequest) -> ParsedModule:
        return python_adapter.parse_python(request)


def git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main", *args],
        cwd=root,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(root), **_GIT_ENV},
        capture_output=True,
        check=True,
        text=True,
    )
    return completed.stdout.strip()


@dataclass
class Stack:
    owner: AsyncEngine
    service: IndexingService
    ingestion: IngestionService
    sessions: async_sessionmaker[AsyncSession]

    def service_at(self, when: datetime) -> IndexingService:
        """The same indexer with a later clock: only observation times may differ."""
        config = dataclasses.replace(
            self.service.config, clock=lambda: when, monotonic=lambda: when.timestamp()
        )
        return IndexingService(config, self.ingestion, self.sessions)

    @property
    def repository_id(self) -> str:
        return self.service.config.repository_id

    async def counts(self) -> dict[str, int]:
        async with self.owner.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT event_type, count(*) FROM ledger.events "
                    "WHERE producer_id = :p AND stream_id = :s GROUP BY event_type"
                ),
                {"p": INDEXER_PRODUCER_ID, "s": f"code-index:{self.repository_id}"},
            )
            return {name: int(count) for name, count in rows}


@pytest.fixture
def stack(
    ledger_engine: AsyncEngine, postgres_dsn: str, s3_settings: S3Settings
) -> Iterator[Stack]:
    api = create_async_engine(
        postgres_dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    sessions = session_factory(api)
    ingestion = IngestionService(
        ContentService(S3BlobStore.from_settings(s3_settings), RedactionPolicyV1()), sessions
    )
    config = IndexingConfig(
        repository_id=f"{REPO_ID}-{uuid.uuid4().hex[:8]}",
        checkout_id="checkout-1",
        clock=lambda: NOW,
    )
    yield Stack(ledger_engine, IndexingService(config, ingestion, sessions), ingestion, sessions)
    asyncio.run(api.dispose())


def _write_repo(root: Path) -> None:
    root.mkdir()
    git(root, "init", "-q")
    (root / "pkg").mkdir()
    for source in sorted(SOURCES.iterdir()):
        content = source.read_bytes()
        if source.name == "shapes.py":
            # The committed index predates a second blank line in the fixture source.
            content = content.replace(b"import math\n\n\nclass", b"import math\n\nclass")
        (root / "pkg" / source.name).write_bytes(content)
    (root / "pkg" / "old_name.py").write_text(HELPER)


def _commit(root: Path, message: str) -> str:
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", message)
    return git(root, "rev-parse", "HEAD")


def _index(
    stack: Stack,
    scan: RepositoryScan,
    ids: dict[str, uuid.UUID],
    supersessions: tuple = (),
    service: IndexingService | None = None,
    mutate: Callable[[list], list] | None = None,
) -> list:
    head = scan.workspace.head_commit
    assert head is not None
    semantic = import_scip(
        (FIXTURES / "scip-python-basic.scip").read_bytes(),
        stack.service.config.repository_id,
        head,
        file_logical_ids=ids,
    )
    parsed = parse_structural({"python": InProcessPython()}, read_sources(scan)).files  # type: ignore[dict-item]
    evidence = list(parsed) if mutate is None else mutate(list(parsed))
    return (service or stack.service).index(
        scan, semantic, evidence, file_logical_ids=ids, supersessions=supersessions
    )


def _seed(stack: Stack, tmp_path: Path) -> tuple[Path, str, RepositoryScan, dict[str, uuid.UUID]]:
    repo_id = stack.service.config.repository_id
    root = tmp_path / "repo"
    _write_repo(root)
    commit = _commit(root, "seed")
    scan = scan_repository(root)
    ids = dict(derive_lineage(scan, repo_id).file_logical_ids)  # from history, never from HEAD
    return root, commit, scan, ids


def test_reindexing_the_same_head_adds_no_row_of_any_kind(stack: Stack, tmp_path: Path) -> None:
    _, _, scan, ids = _seed(stack, tmp_path)
    first = _index(stack, scan, ids)
    outcome = asyncio.run(stack.service.ingest(first))
    after_first = asyncio.run(stack.counts())

    assert outcome.submitted == len(first) and outcome.existing == 0
    assert after_first["code.file.indexed"] == 4
    assert after_first["code.index.started"] == after_first["code.index.completed"] == 1
    assert after_first["code.symbol.indexed"] > 0
    assert after_first["code.relation.asserted"] > 0
    assert after_first["code.assertion.observed"] >= after_first["code.relation.asserted"]
    assert after_first["code.file.coverage_reported"] == after_first["code.file.indexed"] == 4

    # A later run: same target and evidence, but different observation times and duration.
    later = stack.service_at(NOW + timedelta(days=3))
    again = _index(stack, scan, ids, service=later)
    assert [d.idempotency_key for d in again] == [d.idempotency_key for d in first]
    assert [d.occurred_at for d in again] != [d.occurred_at for d in first]
    repeat = asyncio.run(later.ingest(again))

    assert repeat.submitted == 0 and repeat.existing == len(first)
    assert asyncio.run(stack.counts()) == after_first


def test_a_new_commit_records_membership_and_only_the_changed_revisions(
    stack: Stack, tmp_path: Path
) -> None:
    root, _, scan, ids = _seed(stack, tmp_path)
    asyncio.run(stack.service.ingest(_index(stack, scan, ids)))
    before = asyncio.run(stack.counts())
    revisions_before = asyncio.run(_distinct(stack, "file.indexed", "file_revision_id"))
    symbol_files_before = asyncio.run(_symbol_rows(stack))
    observed_before = asyncio.run(_distinct(stack, "assertion.observed", "file_revision_id"))

    (root / "pkg" / "old_name.py").write_text(HELPER + "\n\ndef extra() -> int:\n    return 1\n")
    _commit(root, "edit one file")
    edited = scan_repository(root)
    later = stack.service_at(NOW + timedelta(days=1))
    asyncio.run(later.ingest(_index(stack, edited, ids, service=later)))
    after = asyncio.run(stack.counts())

    # Membership: every file of the new target is listed again (four, not one)...
    assert after["code.file.indexed"] == before["code.file.indexed"] + 4
    assert after["code.index.started"] == 2 and after["code.index.completed"] == 2
    # ...with exactly one new file revision, the edited file's.
    revisions_after = asyncio.run(_distinct(stack, "file.indexed", "file_revision_id"))
    assert len(revisions_after) == len(revisions_before) + 1
    # New symbol rows only for the edited file.
    new_symbols = asyncio.run(_symbol_rows(stack)) - symbol_files_before
    assert new_symbols
    assert {file_id for file_id, _ in new_symbols} == {str(ids["pkg/old_name.py"])}
    # Assertion membership follows file revisions: only the edited revision is observed anew,
    # while coverage is per target and lists every file of the new commit again.
    assert after["code.file.coverage_reported"] == before["code.file.coverage_reported"] + 4
    new_observed = after["code.assertion.observed"] - before["code.assertion.observed"]
    assert 0 < new_observed < before["code.assertion.observed"]
    observed_after = asyncio.run(_distinct(stack, "assertion.observed", "file_revision_id"))
    assert observed_after - observed_before == revisions_after - revisions_before


def test_a_conflicting_payload_under_an_existing_key_is_surfaced_not_dropped(
    stack: Stack, tmp_path: Path
) -> None:
    _, _, scan, ids = _seed(stack, tmp_path)
    drafts = _index(stack, scan, ids)
    asyncio.run(stack.service.ingest(drafts))
    before = asyncio.run(stack.counts())
    victim = next(d for d in drafts if d.event_type == "code.symbol.indexed")
    changed = victim.model_copy(
        update={"payload": {**victim.payload, "qualified_name": "pkg.not_the_same_symbol"}}
    )

    with pytest.raises(IndexingError) as error:
        asyncio.run(stack.service.ingest([changed]))

    assert error.value.code == "idempotency_conflict"
    assert asyncio.run(stack.counts()) == before  # nothing was written


def test_a_degraded_first_run_does_not_make_the_target_unindexable(
    stack: Stack, tmp_path: Path
) -> None:
    _, _, scan, ids = _seed(stack, tmp_path)

    def degrade(parsed: list) -> list:
        return [
            p.model_copy(
                update={
                    "symbols": (),
                    "relations": (),
                    "references": (),
                    "diagnostics": (ParsedDiagnostic(code="file_degraded", count=1),),
                }
            )
            if p.path == "pkg/old_name.py"
            else p
            for p in parsed
        ]

    first = _index(stack, scan, ids, mutate=degrade)
    asyncio.run(stack.service.ingest(first))
    clean = _index(stack, scan, ids)
    outcome = asyncio.run(stack.service.ingest(clean))  # no idempotency_conflict

    assert outcome.submitted > 0 and outcome.existing > 0
    counts = asyncio.run(stack.counts())
    assert counts["code.index.started"] == 1  # the target's start is one claim
    assert counts["code.index.completed"] == 2  # one per outcome
    assert asyncio.run(_distinct(stack, "index.completed", "success")) == {"false", "true"}
    assert asyncio.run(_distinct(stack, "index.completed", "error_class")) == {
        "files_degraded",
        None,
    }


def test_the_same_commit_from_a_second_checkout_is_a_replay(stack: Stack, tmp_path: Path) -> None:
    _, _, scan, ids = _seed(stack, tmp_path)
    first = _index(stack, scan, ids)
    asyncio.run(stack.service.ingest(first))
    before = asyncio.run(stack.counts())
    other = IndexingService(
        dataclasses.replace(
            stack.service.config, checkout_id="checkout-2", workspace_id="workspace-2"
        ),
        stack.ingestion,
        stack.sessions,
    )

    again = _index(stack, scan, ids, service=other)
    outcome = asyncio.run(other.ingest(again))

    assert outcome.submitted == 0 and outcome.existing == len(first)
    assert asyncio.run(stack.counts()) == before


def test_a_symbol_named_under_another_file_is_a_new_event_not_a_conflict(
    stack: Stack, tmp_path: Path
) -> None:
    _, _, scan, ids = _seed(stack, tmp_path)
    drafts = _index(stack, scan, ids)
    asyncio.run(stack.service.ingest(drafts))
    before = asyncio.run(stack.counts())
    victim = next(d for d in drafts if d.event_type == "code.symbol.indexed")
    moved = victim.model_copy(
        update={
            "payload": {
                **victim.payload,
                "file_id": str(uuid.uuid4()),
                "file_revision_id": str(uuid.uuid4()),
            }
        }
    )
    moved = moved.model_copy(
        update={
            "idempotency_key": claim_key(moved),
            "event_id": event_id_for(claim_key(moved)),
        }
    )

    outcome = asyncio.run(stack.service.ingest([moved]))

    assert outcome.submitted == 1
    after = asyncio.run(stack.counts())
    assert after["code.symbol.indexed"] == before["code.symbol.indexed"] + 1


async def _distinct(stack: Stack, kind: str, field: str) -> set[str]:
    async with stack.owner.connect() as connection:
        rows = await connection.execute(
            text(
                f"SELECT DISTINCT payload->>'{field}' FROM ledger.events "
                "WHERE producer_id = :p AND stream_id = :s AND event_type = :t"
            ),
            {
                "p": INDEXER_PRODUCER_ID,
                "s": f"code-index:{stack.repository_id}",
                "t": f"code.{kind}",
            },
        )
        return {row[0] for row in rows}


async def _symbol_rows(stack: Stack) -> set[tuple[str, str]]:
    async with stack.owner.connect() as connection:
        rows = await connection.execute(
            text(
                "SELECT payload->>'file_id', payload->>'symbol_revision_id' FROM ledger.events "
                "WHERE producer_id = :p AND stream_id = :s AND event_type = 'code.symbol.indexed'"
            ),
            {"p": INDEXER_PRODUCER_ID, "s": f"code-index:{stack.repository_id}"},
        )
        return {(row[0], row[1]) for row in rows}


def test_a_rename_keeps_identity_and_the_file_revision(stack: Stack, tmp_path: Path) -> None:
    repo_id = stack.service.config.repository_id
    root, _, scan, ids = _seed(stack, tmp_path)
    first = _index(stack, scan, ids)
    asyncio.run(stack.service.ingest(first))
    after_first = asyncio.run(stack.counts())

    git(root, "mv", "pkg/old_name.py", "pkg/util.py")
    second_commit = _commit(root, "rename")
    renamed = scan_repository(root)
    old = next(f for f in scan.files if f.path == "pkg/old_name.py")
    new = next(f for f in renamed.files if f.path == "pkg/util.py")
    assert old.oid == new.oid == blob_oid(HELPER.encode(), renamed.object_format)
    resolution = identity.resolve_rename(
        repo_id,
        second_commit,
        [identity.PriorFile(old.path, ids[old.path], old.oid or "")],
        [identity.NewFile(new.path, new.oid or "")],
    )
    ids2 = {
        item.path: ids.get(item.path) or resolution.logical_ids[item.path] for item in renamed.files
    }
    assert ids2["pkg/util.py"] == ids["pkg/old_name.py"]

    moved = _index(stack, renamed, ids2, tuple(resolution.supersessions))
    asyncio.run(stack.service.ingest(moved))
    after_rename = asyncio.run(stack.counts())

    def revision(drafts: list, path: str) -> str:
        return next(d.payload["file_revision_id"] for d in drafts if d.payload.get("path") == path)

    def relation_keys(drafts: list) -> set[str]:
        return {d.idempotency_key for d in drafts if d.event_type == "code.relation.asserted"}

    assert revision(moved, "pkg/util.py") == revision(first, "pkg/old_name.py")
    # Membership of the new target (all four files); relations are content-keyed, so only new
    # edges land; the two symbols named after the module path are new.
    assert after_rename["code.file.indexed"] == after_first["code.file.indexed"] + 4
    fresh_relations = relation_keys(moved) - relation_keys(first)
    assert after_rename["code.relation.asserted"] == (
        after_first["code.relation.asserted"] + len(fresh_relations)
    )
    assert after_rename["code.symbol.indexed"] == after_first["code.symbol.indexed"] + 2
    assert after_rename["code.index.started"] == 2 and after_rename["code.index.completed"] == 2


def test_scip_symbol_is_the_identity_in_the_ledger(stack: Stack, tmp_path: Path) -> None:
    repo_id = stack.service.config.repository_id
    root = tmp_path / "repo"
    _write_repo(root)
    commit = _commit(root, "seed")
    scan = scan_repository(root)
    ids = {item.path: identity.file_logical_id(repo_id, item.path, commit) for item in scan.files}
    asyncio.run(stack.service.ingest(_index(stack, scan, ids)))

    async def circle() -> tuple[str, str]:
        async with stack.owner.connect() as connection:
            row = await connection.execute(
                text(
                    "SELECT payload->>'symbol_id', payload->>'semantic_fingerprint_sha256' "
                    "FROM ledger.events WHERE event_type = 'code.symbol.indexed' "
                    "AND producer_id = :p AND payload->>'scip_symbol' = :s "
                    "AND payload->>'file_id' = :f"
                ),
                {"p": INDEXER_PRODUCER_ID, "s": CIRCLE, "f": str(ids["pkg/shapes.py"])},
            )
            return tuple(row.one())  # type: ignore[return-value]

    symbol_id, fingerprint = asyncio.run(circle())
    parsed = parse_structural({"python": InProcessPython()}, read_sources(scan)).files  # type: ignore[dict-item]
    circle_symbol = next(
        s
        for f in parsed
        if f.path == "pkg/shapes.py"
        for s in f.symbols
        if s.qualified_name.endswith("Circle")
    )
    assert fingerprint == circle_symbol.semantic_fingerprint
    assert uuid.UUID(symbol_id) == identity.symbol_logical_id(
        repo_id,
        scip_symbol=CIRCLE,
        language="python",
        file_logical_id=ids["pkg/shapes.py"],
        qualified_name="",
        kind="",
    )


def test_ingest_without_a_service_is_refused() -> None:
    bare = IndexingService(IndexingConfig(repository_id="r"))

    with pytest.raises(IndexingError) as error:
        asyncio.run(bare.ingest([]))
    assert error.value.code == "ingestion_not_configured"


def test_reindexing_after_a_non_code_dirty_file_changes_content_adds_no_row(
    stack: Stack, tmp_path: Path
) -> None:
    root, _, _, ids = _seed(stack, tmp_path)
    (root / ".env").write_text("TOKEN=one\n")
    first_scan = scan_repository(root)
    first = _index(stack, first_scan, ids)
    asyncio.run(stack.service.ingest(first))
    before = asyncio.run(stack.counts())

    (root / ".env").write_text("TOKEN=two, and longer\n")  # content only: same dirty state
    later = stack.service_at(NOW + timedelta(days=1))
    again = _index(stack, scan_repository(root), ids, service=later)
    repeat = asyncio.run(later.ingest(again))

    assert [d.idempotency_key for d in again] == [d.idempotency_key for d in first]
    assert repeat.submitted == 0 and repeat.existing == len(first)
    assert asyncio.run(stack.counts()) == before
