"""The deterministic fixture world of the context-quality evaluation (PLATFORM-043).

Everything enters through the real write path: `IndexingService` drafts of a real git repository
and hand-written SDK drafts go through `IngestionService` as `agent_context_api` (so content is
stored by the real content service and redaction re-check), then the real `ProjectionRunner`
projects them as `agent_context_projector` with every registered projector, search included. The
only fake is the embedder (deterministic hash vectors).

Timeline (hours after `T0`; occurred = observed):

- 1:00  index of commit C1: `pkg/calc.py` (add), `pkg/report.py` (total -> add, imports json),
        `pkg/legacy_parser.py` (parse_legacy).
- 1:00  decision `dec_rounding_v1` (float arithmetic in add), accepted, valid from 1:00.
- 1:30  session `sess_eval_1`: a user message, a pytest tool call whose output shows the
        rounding drift, an assistant reply.
- 1:40  CI run `ci_run_1` (ci/test) fails on C1; failure `fail_rounding` observed in it, in the
        session and in the tool call, with sanitized details.
- 1:50  session content later purged (`content.purged` at 2:10).
- 2:00  decision `dec_rounding_v2` supersedes v1 (Decimal arithmetic), valid from 2:00.
- 2:30  decision `dec_parser` (rename the legacy parser), valid from 2:30.
- 3:00  index of commit C2: `pkg/legacy_parser.py` renamed to `pkg/parser.py` (same logical
        file), `pkg/calc.py` gains `subtract`.
- 3:10  CI run `ci_run_2` (ci/test) passes on C2: the rounding failure's fix path (commit changed).
- 3:20  CI run `ci_run_3` (ci/lint) fails on C2; failure `fail_flaky` observed in it.
- 3:30  CI run `ci_run_4` (ci/lint) passes on the SAME commit C2 (a flaky re-run).
- 5:00  index of a dirty workspace snapshot S on C2: `pkg/report.py` gains `average`, untracked
        `pkg/draft.py`.
- 5:10  CI run `ci_run_5` (ci/test) fails on S; failure `fail_dirty` observed in it.
- 5:30  session `sess_eval_2`: a user message asking for the average helper.
- 6:00  decision `dec_average` (empty lists), valid from 6:00.

Revision windows: C1 [1:00, 3:00), C2 [3:00, 5:00), S [5:00, open).
"""

from __future__ import annotations

import base64
import hashlib
import os
import subprocess
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    ProducerV1,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
)
from agent_context_sdk.ids import new_uuid7
from integration.projection.conftest import neo4j_integration_settings
from integration.retrieval.fake_embeddings import FakeEmbeddingProvider
from integration.retrieval.search_harness import InlineOnlyBlobStore, role_engine
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.indexing import identity
from agent_context_platform.indexing.emitter import (
    IndexingConfig,
    IndexingService,
    parse_structural,
    read_sources,
)
from agent_context_platform.indexing.scanner import scan_repository
from agent_context_platform.indexing.tree_sitter import python as python_adapter
from agent_context_platform.indexing.tree_sitter.base import ParsedModule, ParseRequest
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.projectors.search import SearchBackend
from agent_context_platform.projection.registry import projectors_with_search
from agent_context_platform.projection.runtime import ProjectionRunner, ProjectionRunReport
from agent_context_platform.projection.verify import record_history_if_fresh
from agent_context_platform.retrieval.composer import ContextComposer
from agent_context_platform.retrieval.cursor import CursorCodec
from agent_context_platform.retrieval.evidence import EvidenceLedger
from agent_context_platform.retrieval.freshness import FreshnessReader
from agent_context_platform.retrieval.graph import GraphTraversalService
from agent_context_platform.retrieval.search import SearchService
from agent_context_platform.retrieval.temporal import TemporalService

T0 = datetime(2026, 9, 1, 8, 0, 0, tzinfo=UTC)
NOW = T0 + timedelta(hours=8)  # the composer's fixed clock: every package is reproducible
PROJECT = "prj_eval"
REPOSITORY = "repo_eval"
OTHER_PROJECT = "prj_other"
OTHER_REPOSITORY = "repo_other"
OTHER_WORKSPACE = "ws_other"
WORKSPACE = "ws_eval"
CURSOR_KEY = SecretStr("evaluation-cursor-key-0123456789abcdef")
FINGERPRINT_ROUNDING = hashlib.sha256(b"fingerprint:rounding").hexdigest()
FINGERPRINT_FLAKY = hashlib.sha256(b"fingerprint:flaky-lint").hexdigest()
FINGERPRINT_DIRTY = hashlib.sha256(b"fingerprint:dirty").hexdigest()

CALC_V1 = "def add(left, right):\n    return left + right\n"
CALC_V2 = CALC_V1 + "\n\ndef subtract(left, right):\n    return left - right\n"
REPORT_V1 = (
    "import json\n\nfrom .calc import add\n\n\ndef total(values):\n    result = 0\n"
    "    for value in values:\n        result = add(result, value)\n    return result\n\n\n"
    "def render(values):\n    return json.dumps({'total': total(values)})\n"
)
REPORT_DIRTY = REPORT_V1 + "\n\ndef average(values):\n    return total(values) / len(values)\n"
PARSER = "def parse_legacy(text):\n    return text.split(',')\n"
DRAFT = "def draft_helper():\n    return None\n"
OTHER_CALC = "def add(left, right):\n    return round(left + right, 2)\n"

_PRODUCER = ProducerV1(producer_id="evaluation-producer", name="Codex", version="1.0.0")


def at(hours: float) -> datetime:
    return T0 + timedelta(hours=hours)


def event_uuid(number: int) -> UUID:
    return UUID(f"0198a4b1-98c0-7c28-ae3f-{0xE000 + number:012x}")


class _InProcessPython:
    language = "python"

    def parse(self, request: ParseRequest) -> ParsedModule:
        return python_adapter.parse_python(request)


class _Repo:
    """A real git repository whose commits have fixed dates, so their OIDs are stable."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True)
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(root.parent),
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Evaluation",
            "GIT_AUTHOR_EMAIL": "evaluation@example.invalid",
            "GIT_COMMITTER_NAME": "Evaluation",
            "GIT_COMMITTER_EMAIL": "evaluation@example.invalid",
        }
        self.git("init", "-q")

    def git(self, *args: str, when: datetime | None = None) -> str:
        env = dict(self.env)
        if when is not None:
            env["GIT_AUTHOR_DATE"] = env["GIT_COMMITTER_DATE"] = when.isoformat()
        done = subprocess.run(
            ["git", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false", *args],
            cwd=self.root,
            env=env,
            capture_output=True,
            check=True,
            text=True,
        )
        return done.stdout.strip()

    def write(self, path: str, body: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body)

    def commit(self, when: datetime, message: str) -> str:
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message, when=when)
        return self.git("rev-parse", "HEAD")


@dataclass
class Revisions:
    c1: str = ""
    c2: str = ""
    snapshot: str = ""
    other_c1: str = ""  # the second repository's commit and dirty snapshot
    other_snapshot: str = ""


@dataclass
class EvalWorld:
    """Ingests and projects the world; `keys` names its events for the question file."""

    dsn: str
    owner: AsyncEngine
    store: Neo4jStore
    api_login: str  # a LOGIN member of agent_context_api: freshness reads with its grants only
    keys: dict[str, str] = field(default_factory=dict)
    symbols: dict[str, str] = field(default_factory=dict)  # qualified name -> symbol ID
    other_symbols: dict[str, str] = field(default_factory=dict)  # the same, second repository
    revisions: Revisions = field(default_factory=Revisions)
    embeddings: FakeEmbeddingProvider = field(default_factory=FakeEmbeddingProvider)

    def __post_init__(self) -> None:
        self._engines = [
            role_engine(self.dsn, "agent_context_api"),
            role_engine(self.dsn, "agent_context_projector"),
            create_async_engine(self.api_login, poolclass=NullPool),
        ]
        self.api: async_sessionmaker[AsyncSession] = session_factory(self._engines[0])
        self.projector: async_sessionmaker[AsyncSession] = session_factory(self._engines[1])
        self.api_reads: async_sessionmaker[AsyncSession] = session_factory(self._engines[2])
        self.content = ContentService(InlineOnlyBlobStore(), RedactionPolicyV1())
        self.ingestion = IngestionService(self.content, self.api)
        self._services: list[Any] = []
        self._claims: dict[str, str] = {}
        self._other = False  # seeding the second repository and project

    async def close(self) -> None:
        for service in self._services:
            await service.close()
        for engine in self._engines:
            await engine.dispose()

    # --- composing ---

    def composer(
        self,
        *,
        clock: Callable[[], datetime] = lambda: NOW,
        content: ContentService | None = None,
        deadline_seconds: float = 60.0,
        lexical_only: bool = False,
    ) -> ContextComposer:
        settings = neo4j_integration_settings()
        graph = GraphTraversalService.from_settings(settings)
        temporal = TemporalService.from_settings(settings, clock=clock)
        self._services += [graph, temporal]
        return ContextComposer(
            graph=graph,
            graph_reader=self.store.read_only(),
            temporal=temporal,
            search=SearchService(
                self.api_reads, self.store.read_only(), None if lexical_only else self.embeddings
            ),
            ledger=EvidenceLedger(self.api_reads, content or self.content),
            freshness=FreshnessReader(self.api_reads),
            cursor=CursorCodec(CURSOR_KEY),
            clock=clock,
            default_deadline_seconds=deadline_seconds,
            vector_search=not lexical_only,
        )

    def backend(self) -> SearchBackend:
        return SearchBackend(self.projector, self.content, self.embeddings)

    # --- seeding ---

    async def seed(self, tmp: Path) -> None:
        repo = _Repo(tmp / "repo")
        repo.write("pkg/__init__.py", "")
        repo.write("pkg/calc.py", CALC_V1)
        repo.write("pkg/report.py", REPORT_V1)
        repo.write("pkg/legacy_parser.py", PARSER)
        self.revisions.c1 = repo.commit(at(0.5), "first")
        lineage = self._lineage(repo, {})
        await self._index(repo, lineage, at(1), "c1")

        await self._knowledge_before_c2()

        repo.git("mv", "pkg/legacy_parser.py", "pkg/parser.py")
        repo.write("pkg/calc.py", CALC_V2)
        self.revisions.c2 = repo.commit(at(2.75), "rename parser, add subtract")
        renamed = dict(lineage)
        renamed["pkg/parser.py"] = renamed.pop("pkg/legacy_parser.py")
        lineage = self._lineage(repo, renamed)
        await self._index(repo, lineage, at(3), "c2")

        await self._quality_on_c2()

        repo.write("pkg/report.py", REPORT_DIRTY)
        repo.write("pkg/draft.py", DRAFT)
        lineage = self._lineage(repo, lineage, extra=("pkg/draft.py",))
        await self._index(repo, lineage, at(5), "snapshot")
        self._repo, self._repo_lineage = repo, lineage

        await self._after_snapshot()
        await self._other_repository(tmp)

    async def commit_worktree(self, hours: float, name: str) -> str:
        """Commit the dirty working tree indexed as the snapshot, unchanged, and index it: the
        usual flow (index a dirty workspace, then commit). Returns the new commit."""
        oid = self._repo.commit(at(hours), "commit the snapshot")
        await self._index(self._repo, self._repo_lineage, at(hours), name)
        return oid

    def _scope(self) -> tuple[str, str, str]:
        if self._other:
            return OTHER_REPOSITORY, OTHER_PROJECT, OTHER_WORKSPACE
        return REPOSITORY, PROJECT, WORKSPACE

    async def _other_repository(self, tmp: Path) -> None:
        """A second repository AND project whose names overlap the first one's: the same paths
        and qualified names, the same decision and failure IDs and failure fingerprint, rounding
        words in its content, and its own dirty snapshot. Content IDs stay distinct (a content ID
        is one search document). Its text says "other-repository", which no answer may quote."""
        self._other = True
        try:
            repo = _Repo(tmp / "other")
            repo.write("pkg/__init__.py", "")
            repo.write("pkg/calc.py", OTHER_CALC)
            repo.write("pkg/other_only.py", "def other_only():\n    return 0\n")
            self.revisions.other_c1 = repo.commit(at(0.6), "other first")
            lineage = self._lineage(repo, {})
            await self._index(repo, lineage, at(1.2), "other:c1")
            await self._session(
                80,
                "sess_other_1",
                "turn_other_1",
                1.6,
                user="other-repository: why does add in calc drift with rounding in report totals?",
                tool_output=None,
                reply=None,
            )
            await self._ci_run(
                90, "ci_other_1", 1.7, "test", "failure", commit=repo.git("rev-parse", "HEAD")
            )
            await self._failure(
                91,
                "fail_rounding",
                1.72,
                component="pytest",
                operation="test_add_rounding",
                error_class="AssertionError",
                fingerprint=FINGERPRINT_ROUNDING,
                ci_run="ci_other_1",
                details="other-repository failure: rounding drift in add for report totals",
                content_id="details_other_fail_rounding",
            )
            await self._decision(
                5,
                "dec_rounding_v2",
                2.2,
                "other-repository decision: use banker's rounding in add for report totals.",
                content_id="content_other_dec_rounding_v2",
            )
            repo.write("pkg/calc.py", OTHER_CALC + "\n\ndef average(values):\n    return 0\n")
            await self._index(repo, self._lineage(repo, lineage), at(4.75), "other:snapshot")
        finally:
            self._other = False

    def _lineage(
        self, repo: _Repo, inherited: dict[str, UUID], extra: Sequence[str] = ()
    ) -> dict[str, UUID]:
        head = repo.git("rev-parse", "HEAD")
        paths = [p for p in repo.git("ls-files").splitlines() if p.endswith(".py")] + list(extra)
        return {
            path: inherited.get(path) or identity.file_logical_id(self._scope()[0], path, head)
            for path in paths
        }

    async def _index(
        self, repo: _Repo, lineage: dict[str, UUID], when: datetime, name: str
    ) -> None:
        scan = scan_repository(repo.root)
        structural = parse_structural({"python": _InProcessPython()}, read_sources(scan))  # type: ignore[dict-item]
        repository_id, project_id, workspace_id = self._scope()
        service = IndexingService(
            IndexingConfig(
                repository_id=repository_id,
                project_id=project_id,
                workspace_id=workspace_id,
                checkout_id="checkout-eval",
                clock=lambda: when,
                monotonic=lambda: 0.0,
            )
        )
        drafts = service.index(scan, None, list(structural.files), file_logical_ids=lineage)
        # A claim an earlier run already made is a replay: the ledger keeps the first (as
        # `IndexingService.ingest` does), so its event ID is the one the evidence cites.
        fresh = [d for d in drafts if d.idempotency_key not in self._claims]
        self._claims.update({d.idempotency_key: str(d.event_id) for d in fresh})
        for start in range(0, len(fresh), 500):
            await self._submit(fresh[start : start + 500], ())
        names: dict[str, str] = {}
        for draft in drafts:
            payload = dict(draft.payload)
            if draft.event_type == "code.symbol.indexed":
                names[str(payload["symbol_id"])] = str(payload["qualified_name"])
                symbols = self.other_symbols if self._other else self.symbols
                symbols[str(payload["qualified_name"])] = str(payload["symbol_id"])
            elif draft.event_type == "code.file.indexed":
                names[str(payload["file_id"])] = str(payload["path"])
        for draft in drafts:
            draft = draft.model_copy(update={"event_id": UUID(self._claims[draft.idempotency_key])})
            payload = dict(draft.payload)
            if draft.event_type == "code.symbol.indexed":
                self.keys[f"{name}:symbol:{payload['qualified_name']}"] = str(draft.event_id)
            elif draft.event_type == "code.file.indexed":
                self.keys[f"{name}:file:{payload['path']}"] = str(draft.event_id)
            elif draft.event_type in ("code.index.started", "code.index.completed"):
                self.keys[f"{name}:{draft.event_type}"] = str(draft.event_id)
                if payload.get("snapshot_id") and self._other:
                    self.revisions.other_snapshot = str(payload["snapshot_id"])
                elif payload.get("snapshot_id"):
                    self.revisions.snapshot = str(payload["snapshot_id"])
            elif draft.event_type == "code.relation.asserted":
                subject = names.get(str(payload["subject_id"]), str(payload["subject_id"]))
                target = names.get(str(payload["object_id"]), str(payload["object_id"]))
                self.keys[f"{name}:relation:{subject} {payload['predicate']} {target}"] = str(
                    draft.event_id
                )
            elif draft.event_type == "code.dependency.asserted":
                self.keys[f"{name}:dependency"] = str(draft.event_id)

    async def _knowledge_before_c2(self) -> None:
        await self._decision(1, "dec_rounding_v1", 1, "Use float arithmetic in add for speed.")
        await self._session(
            10,
            "sess_eval_1",
            "turn_eval_1",
            1.5,
            user="Why does add in calc drift when report totals are rounded?",
            tool_output=(
                "FAILED tests/test_calc.py::test_add_rounding - AssertionError: rounding drift "
                "0.30000000000000004 != 0.3"
            ),
            reply="The add function uses float arithmetic; switching to Decimal fixes the drift.",
        )
        await self._ci_run(20, "ci_run_1", 1 + 40 / 60, "test", "failure", commit=self.revisions.c1)
        await self._failure(
            21,
            "fail_rounding",
            1 + 41 / 60,
            component="pytest",
            operation="test_add_rounding",
            error_class="AssertionError",
            fingerprint=FINGERPRINT_ROUNDING,
            ci_run="ci_run_1",
            session="sess_eval_1",
            details="AssertionError: rounding drift in add for report totals",
        )
        await self._content_event(
            30,
            "purged-transcript",
            1 + 50 / 60,
            "sess_eval_1",
            "turn_eval_purged",
            "scratch transcript about rounding drift that the operator purged",
        )
        await self._purge(31, 2 + 10 / 60, "purged-transcript")
        await self._decision(
            2,
            "dec_rounding_v2",
            2,
            "Use Decimal arithmetic in add so report totals are exact to the cent.",
            supersedes="dec_rounding_v1",
        )
        await self._decision(
            3,
            "dec_parser",
            2.5,
            "Rename the legacy parser module to parser; keep parse_legacy as its entry point.",
            subjects=("pkg/parser.py", "pkg.parser.parse_legacy"),
        )

    async def _quality_on_c2(self) -> None:
        await self._ci_run(40, "ci_run_2", 3 + 10 / 60, "test", "success", commit=self.revisions.c2)
        await self._ci_run(41, "ci_run_3", 3 + 20 / 60, "lint", "failure", commit=self.revisions.c2)
        await self._failure(
            42,
            "fail_flaky",
            3 + 21 / 60,
            component="ruff",
            operation="lint",
            error_class="TimeoutError",
            fingerprint=FINGERPRINT_FLAKY,
            ci_run="ci_run_3",
            details="lint job timed out while downloading the flaky cache",
        )
        await self._ci_run(43, "ci_run_4", 3.5, "lint", "success", commit=self.revisions.c2)

    async def _after_snapshot(self) -> None:
        # CI output of the DIRTY workspace, inside C2's window: never evidence for a commit.
        await self._ci_run(
            55,
            "ci_run_dirty_output",
            4.5,
            "dirty",
            "failure",
            snapshot=self.revisions.snapshot,
            output="dirty workspace test output: the average helper in report failed",
        )
        # Occurred inside C2's window but observed (recorded) only after it: later knowledge.
        await self._content_event(
            70,
            "late-observed-note",
            4.0,
            "sess_eval_3",
            "turn_eval_3",
            "late observed note: the average helper in report should reuse total",
            observed=at(7.9),
        )
        await self._ci_run(
            50, "ci_run_5", 5 + 10 / 60, "test", "failure", snapshot=self.revisions.snapshot
        )
        await self._failure(
            51,
            "fail_dirty",
            5 + 11 / 60,
            component="pytest",
            operation="test_average",
            error_class="ZeroDivisionError",
            fingerprint=FINGERPRINT_DIRTY,
            ci_run="ci_run_5",
            details="ZeroDivisionError in the dirty average helper on an empty snapshot list",
        )
        await self._session(
            60,
            "sess_eval_2",
            "turn_eval_2",
            5.5,
            user="Add an average helper to report that reuses total.",
            tool_output=None,
            reply=None,
        )
        await self._decision(
            4,
            "dec_average",
            6,
            "The average helper must return zero for an empty list.",
            subjects=("pkg.report.average",),
        )

    # --- event builders ---

    async def _decision(
        self,
        number: int,
        decision_id: str,
        hours: float,
        body: str,
        *,
        supersedes: str | None = None,
        subjects: Sequence[str] = ("pkg.calc.add", "pkg/calc.py"),
        content_id: str | None = None,
    ) -> None:
        content_id = content_id or f"content_{decision_id}"
        item, claim = _content(content_id, body)
        moment = at(hours)
        event_id = await self._event(
            number,
            "knowledge.decision.recorded",
            moment,
            {
                "decision_id": decision_id,
                "status": "accepted",
                "supersedes_id": supersedes,
                "subjects": sorted(subjects),
                "content_id": content_id,
                "valid_from": moment.isoformat(),
                "recorded_at": moment.isoformat(),
            },
            claims=(claim,),
            items=(item,),
        )
        self.keys[f"{'other:' if self._other else ''}decision:{decision_id}"] = event_id

    async def _session(
        self,
        number: int,
        session_id: str,
        turn_id: str,
        hours: float,
        *,
        user: str,
        tool_output: str | None,
        reply: str | None,
    ) -> None:
        start = at(hours)
        context = {"session_id": session_id}
        self.keys[f"session:{session_id}:started"] = await self._event(
            number,
            "agent.session.started",
            start,
            {"source": "codex", "session_id": session_id, "start_reason": "startup"},
            context=context,
        )
        item, claim = _content(f"{turn_id}:user", user)
        self.keys[f"session:{session_id}:user"] = await self._event(
            number + 1,
            "agent.turn.started",
            start + timedelta(minutes=1),
            {
                "source": "codex",
                "session_id": session_id,
                "turn_id": turn_id,
                "user_message_content_id": claim.content_id,
            },
            context=context | {"turn_id": turn_id},
            claims=(claim,),
            items=(item,),
        )
        if tool_output is not None:
            tool_call = f"{turn_id}:pytest"
            input_item, input_claim = _content(f"{tool_call}:input", '{"command":"pytest -q"}')
            output_item, output_claim = _content(f"{tool_call}:output", tool_output)
            self.keys[f"session:{session_id}:tool_output"] = await self._event(
                number + 2,
                "agent.tool_call.completed",
                start + timedelta(minutes=2),
                {
                    "source": "codex",
                    "session_id": session_id,
                    "turn_id": turn_id,
                    "tool_call_id": tool_call,
                    "tool_name": "shell",
                    "success": False,
                    "error_class": "CommandFailed",
                    "input_content_id": input_claim.content_id,
                    "output_content_id": output_claim.content_id,
                    "duration_ms": 1200,
                },
                context=context | {"turn_id": turn_id, "tool_call_id": tool_call},
                claims=(input_claim, output_claim),
                items=(input_item, output_item),
            )
        if reply is not None:
            reply_item, reply_claim = _content(f"{turn_id}:assistant", reply)
            self.keys[f"session:{session_id}:reply"] = await self._event(
                number + 3,
                "agent.turn.completed",
                start + timedelta(minutes=3),
                {
                    "source": "codex",
                    "session_id": session_id,
                    "turn_id": turn_id,
                    "success": True,
                    "assistant_message_content_id": reply_claim.content_id,
                    "duration_ms": 3000,
                },
                context=context | {"turn_id": turn_id},
                claims=(reply_claim,),
                items=(reply_item,),
            )

    async def _ci_run(
        self,
        number: int,
        run_id: str,
        hours: float,
        job: str,
        status: str,
        *,
        commit: str | None = None,
        snapshot: str | None = None,
        output: str | None = None,
    ) -> None:
        claims: tuple[ContentClaimV1, ...] = ()
        items: tuple[SanitizedContentItemV1, ...] = ()
        if output is not None:
            item, claim = _content(f"output_{run_id}", output)
            claims, items = (claim,), (item,)
        payload: dict[str, Any] = {
            "ci_run_id": run_id,
            "provider": "github",
            "workflow": "ci",
            "job": job,
            "external_id": f"run-{number}",
            "status": status,
            "duration_ms": 60_000,
            "commit_id": commit,
            "snapshot_id": snapshot,
            "output_content_id": None if output is None else f"output_{run_id}",
        }
        if status != "success":
            payload["error_class"] = "JobFailed"
        self.keys[f"ci:{run_id}"] = await self._event(
            number, "quality.ci_run.completed", at(hours), payload, claims=claims, items=items
        )

    async def _failure(
        self,
        number: int,
        failure_id: str,
        hours: float,
        *,
        component: str,
        operation: str,
        error_class: str,
        fingerprint: str,
        ci_run: str,
        details: str,
        session: str | None = None,
        content_id: str | None = None,
    ) -> None:
        item, claim = _content(content_id or f"details_{failure_id}", details)
        self.keys[f"{'other:' if self._other else ''}failure:{failure_id}"] = await self._event(
            number,
            "knowledge.failure.observed",
            at(hours),
            {
                "failure_id": failure_id,
                "component": component,
                "operation": operation,
                "error_class": error_class,
                "fingerprint_version": "1",
                "fingerprint_sha256": fingerprint,
                "session_id": session,
                "ci_run_id": ci_run,
                "details_content_id": claim.content_id,
            },
            context={"session_id": session} if session else None,
            claims=(claim,),
            items=(item,),
        )

    async def _content_event(
        self,
        number: int,
        content_id: str,
        hours: float,
        session_id: str,
        turn_id: str,
        body: str,
        *,
        observed: datetime | None = None,
    ) -> None:
        item, claim = _content(content_id, body)
        self.keys[f"content:{content_id}"] = await self._event(
            number,
            "agent.turn.started",
            at(hours),
            {
                "source": "codex",
                "session_id": session_id,
                "turn_id": turn_id,
                "user_message_content_id": content_id,
            },
            context={"session_id": session_id, "turn_id": turn_id},
            claims=(claim,),
            items=(item,),
            observed=observed,
        )

    async def _purge(self, number: int, hours: float, content_id: str) -> None:
        body = b"scratch transcript about rounding drift that the operator purged"
        self.keys[f"purge:{content_id}"] = await self._event(
            number,
            "content.purged",
            at(hours),
            {
                "purge_request_id": f"purge-{content_id}",
                "purged_contents": [
                    {"content_id": content_id, "content_sha256": hashlib.sha256(body).hexdigest()}
                ],
                "reason": "operator_request",
                "purged_at": at(hours).isoformat(),
                "affected_event_count": 1,
                "storage_kinds": ["inline"],
            },
            scoped=False,
        )

    async def _event(
        self,
        number: int,
        event_type: str,
        moment: datetime,
        payload: dict[str, Any],
        *,
        context: dict[str, str] | None = None,
        claims: Sequence[ContentClaimV1] = (),
        items: Sequence[SanitizedContentItemV1] = (),
        scoped: bool = True,
        observed: datetime | None = None,
    ) -> str:
        repository_id, project_id, workspace_id = self._scope()
        scope = (
            {"workspace_id": workspace_id, "project_id": project_id, "repository_id": repository_id}
            if scoped
            else {}
        )
        draft = EventDraftV1(
            event_id=event_uuid(number),
            event_type=event_type,
            stream_id=f"evaluation-{number}",
            occurred_at=moment,
            observed_at=observed or moment,
            producer=_PRODUCER,
            payload={k: v for k, v in payload.items() if v is not None},
            context=EventContextV1(**scope, **(context or {})),
            redaction=EventRedactionSummaryV1(
                policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
            ),
            idempotency_key=f"evaluation-{number}",
            content_claims=tuple(claims),
        )
        await self._submit([draft], items)
        return str(draft.event_id)

    async def _submit(
        self, drafts: Sequence[EventDraftV1], items: Sequence[SanitizedContentItemV1]
    ) -> None:
        outcome = await self.ingestion.ingest(
            IngestBatchRequestV1(
                batch_id=new_uuid7(), events=tuple(drafts), content_items=tuple(items)
            )
        )
        assert outcome.http_status < 300, outcome.response

    # --- projection ---

    async def project(self) -> ProjectionRunReport:
        projectors = projectors_with_search(self.backend())
        # As the projection worker does at start-up: an empty graph with nothing delivered yet is
        # projected from the ledger's start, so the history marker the temporal reads need holds.
        await record_history_if_fresh(self.projector, self.store, projectors)
        runner = ProjectionRunner(
            self.projector,
            self.store,
            projectors,
            worker_id="evaluation-world",
            max_attempts=1,
        )
        total = ProjectionRunReport()
        while True:
            report = await runner.run_once(100)
            if not report.claimed:
                return total
            total = ProjectionRunReport(
                claimed=total.claimed + report.claimed,
                delivered=total.delivered + report.delivered,
                retried=total.retried + report.retried,
                dead_lettered=total.dead_lettered + report.dead_lettered,
                lost_leases=total.lost_leases + report.lost_leases,
            )

    async def reset(self) -> None:
        async with self.owner.begin() as connection:
            await connection.execute(
                text(
                    "TRUNCATE ledger.event_streams, ledger.events, retrieval.search_documents, "
                    "retrieval.content_tombstones, projection.projection_checkpoints, "
                    "projection.dead_letters CASCADE"
                )
            )


def _content(content_id: str, body: str) -> tuple[SanitizedContentItemV1, ContentClaimV1]:
    data = body.encode()
    claim = ContentClaimV1(
        content_id=content_id,
        content_sha256=hashlib.sha256(data).hexdigest(),
        media_type="text/plain",
        uncompressed_bytes=len(data),
    )
    item = SanitizedContentItemV1(
        claim=claim,
        sanitized_bytes_base64=base64.b64encode(data).decode(),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )
    return item, claim


__all__ = ["NOW", "PROJECT", "REPOSITORY", "T0", "EvalWorld", "Revisions", "at", "uuid"]
