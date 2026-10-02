"""Graph rebuild and verification end to end (PLATFORM-039, the G3 gate's golden graph).

A realistic ledger: the P025 session/agent fixtures plus code events produced by the real
`IndexingService` from a temp git repository with Python, TypeScript and Go files, including a
content revision and a rename. The real `ProjectionRunner` projects it, then the CLI rebuilds it
in place and into a standby target and must reproduce the same digest, counts and node IDs;
`verify` must match, and must fail (exit 1) on tampering.

Neo4j Community has one user database per instance and the test stack has one instance. The
standby rebuild therefore targets that same instance, with the "live" connection of the CLI
pointed at an unreachable placeholder (a standby rebuild never connects to the live store), after
wiping it. That exercises the same code path as a second instance would.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg
import pytest
from agent_context_sdk import (
    EventDraftV1,
    IngestBatchRequestV1,
    RedactionPolicyV1,
    StoredEventV1,
    new_uuid7,
)
from integration.projection.conftest import neo4j_integration_settings, role_scoped_engine
from integration.projection.projectors.fixtures import session_events
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.cli import TARGET_PASSWORD_ENV, app
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
from agent_context_platform.indexing.tree_sitter import go as go_adapter
from agent_context_platform.indexing.tree_sitter import python as python_adapter
from agent_context_platform.indexing.tree_sitter import typescript as typescript_adapter
from agent_context_platform.indexing.tree_sitter.base import ParsedModule, ParseRequest
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.projection import verify
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.registry import registered_projectors
from agent_context_platform.projection.runtime import ProjectionRunner
from agent_context_platform.projection.verify import (
    GraphDigest,
    NoContentBlobStore,
    compute_graph_digest,
    node_identity,
    verify_projections,
)

pytestmark = pytest.mark.e2e

REPO = "repo-e2e"
T0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
PY_CALC = '''"""Calculator."""


def add(left: int, right: int) -> int:
    return left + right


class Counter:
    def bump(self) -> int:
        return add(1, 2)
'''
PY_CALC_V2 = PY_CALC + "\n\ndef sub(left: int, right: int) -> int:\n    return left - right\n"
PY_UTIL = "from .calc import add\n\n\ndef twice(value: int) -> int:\n    return add(value, value)\n"
TS_APP = (
    "export interface Greeter { greet(name: string): string }\n"
    "export class Hello implements Greeter {\n"
    "  greet(name: string): string { return `hello ${name}`; }\n"
    "}\n"
    "export function main(): string { return new Hello().greet('world'); }\n"
)
GO_MAIN = (
    'package svc\n\nimport "fmt"\n\n'
    "type Server struct{ Name string }\n\n"
    'func (s Server) Describe() string { return fmt.Sprintf("server %s", s.Name) }\n\n'
    'func Start() Server { return Server{Name: "main"} }\n'
)

# The G3 golden graph of this ledger. Every ID in it derives from pinned inputs (git dates, the
# indexer clock, fixed fixture event IDs), so a change here is a real change of the projection.
GOLDEN_DIGEST = "96ce5a138aa4bd6cfd5a97af1ba610f0697df3bb5309600135b95c02d35f5b01"
GOLDEN_EVENTS = 127
GOLDEN_NODES = {
    "Assertion": 31,
    "Branch": 1,
    "Checkout": 1,
    "Commit": 5,
    "Dependency": 1,
    "File": 4,
    "FileRevision": 5,
    "Module": 3,
    "Project": 1,
    "Repository": 2,
    "Session": 1,
    "Symbol": 18,
    "SymbolRevision": 19,
    "ToolCall": 1,
    "Turn": 1,
    "Workspace": 1,
    "WorkspaceSnapshot": 1,
}
GOLDEN_RELATIONSHIPS = {
    "BASED_ON": 1,
    "CALLS": 3,
    "COMPLETED": 3,
    "COVERED_IN": 12,
    "CURRENT_REVISION": 20,
    "DEFINED_IN": 22,
    "DEFINES": 16,
    "DEPENDS_ON": 1,
    "HAS_PARENT": 1,
    "HAS_PROJECT": 1,
    "HAS_REVISION": 25,
    "HAS_SNAPSHOT": 1,
    "HAS_TURN": 1,
    "IMPORTS": 1,
    "INDEXED": 3,
    "INVOKED": 1,
    "IN_MODULE": 4,
    "IN_REPOSITORY": 29,
    "MEMBER_OF": 12,
    "OBSERVED_AT": 2,
    "OBSERVED_IN": 31,
    "OBSERVED_ON": 1,
    "PRODUCED": 2,
    "REFERENCES": 1,
    "TARGETED": 1,
    "USES_REPOSITORY": 1,
}


class InProcess:
    """Run a tree-sitter adapter in this process (the sandboxed child needs Landlock)."""

    def __init__(self, language: str, parse: Any) -> None:
        self.language = language
        self._parse = parse

    def parse(self, request: ParseRequest) -> ParsedModule:
        return self._parse(request)  # type: ignore[no-any-return]


ADAPTERS: dict[str, Any] = {
    "python": InProcess("python", python_adapter.parse_python),
    "typescript": InProcess("typescript", typescript_adapter.parse_typescript),
    "go": InProcess("go", go_adapter.parse_go),
}


def git(root: Path, *args: str) -> str:
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(root.parent),
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "E2E",
        "GIT_AUTHOR_EMAIL": "e2e@example.invalid",
        "GIT_COMMITTER_NAME": "E2E",
        "GIT_COMMITTER_EMAIL": "e2e@example.invalid",
        # Pinned so commit OIDs, and with them every derived ID, are the same on every run.
        "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
        "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
    }
    done = subprocess.run(
        ["git", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false", *args],
        cwd=root,
        env=env,
        capture_output=True,
        check=True,
        text=True,
    )
    return done.stdout.strip()


def write(root: Path, path: str, content: str) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)


def commit(root: Path) -> None:
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "change")


def draft_of(event: StoredEventV1) -> EventDraftV1:
    """The producer draft of a sealed fixture event (the ledger seals it again on ingest)."""
    return EventDraftV1.model_validate(
        event.model_dump(exclude={"stream_sequence", "content_refs", "integrity"})
    )


@dataclass(frozen=True)
class GraphFacts:
    """What must survive a rebuild: digest, counts and every node and relationship ID."""

    digest: str
    nodes: dict[str, int]
    relationships: dict[str, int]
    node_ids: frozenset[str]
    relationship_ids: frozenset[tuple[str, str, str]]


async def read_facts(store: Neo4jStore) -> GraphFacts:
    digest: GraphDigest = await compute_graph_digest(store)

    async def read(tx: Neo4jTransaction) -> tuple[frozenset[str], frozenset[tuple[str, str, str]]]:
        nodes = (
            await tx.run(
                "MATCH (n) RETURN labels(n) AS labels, properties(n) AS props", parameters={}
            )
        ).records
        rels = (
            await tx.run(
                "MATCH (a)-[r]->(b) RETURN labels(a) AS a_labels, properties(a) AS a_props, "
                "type(r) AS type, labels(b) AS b_labels, properties(b) AS b_props",
                parameters={},
            )
        ).records
        return (
            frozenset(node_identity(r["labels"], r["props"]) for r in nodes),
            frozenset(
                (
                    node_identity(r["a_labels"], r["a_props"]),
                    r["type"],
                    node_identity(r["b_labels"], r["b_props"]),
                )
                for r in rels
            ),
        )

    node_ids, relationship_ids = await store.execute_read(read)
    return GraphFacts(
        digest=digest.digest,
        nodes=dict(digest.nodes),
        relationships=dict(digest.relationships),
        node_ids=node_ids,
        relationship_ids=relationship_ids,
    )


async def graph_facts() -> GraphFacts:
    async with Neo4jStore(neo4j_integration_settings()) as store:
        return await read_facts(store)


async def run_graph(statement: str, **parameters: Any) -> None:
    async with Neo4jStore(neo4j_integration_settings()) as store:

        async def run(tx: Neo4jTransaction) -> None:
            await tx.run(statement, parameters=parameters)  # type: ignore[arg-type]

        await store.execute_write(run)


async def sql(owner: AsyncEngine, statement: str, **parameters: Any) -> list[Any]:
    async with owner.begin() as connection:
        result = await connection.execute(text(statement), parameters)
        return list(result.all()) if result.returns_rows else []


async def ingest_drafts(dsn: str, drafts: Sequence[EventDraftV1]) -> None:
    api = create_async_engine(
        dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    try:
        ingestion = IngestionService(
            ContentService(NoContentBlobStore(), RedactionPolicyV1()), session_factory(api)
        )
        outcome = await ingestion.ingest(
            IngestBatchRequestV1(batch_id=new_uuid7(), events=tuple(drafts))
        )
        assert outcome.http_status == 200
    finally:
        await api.dispose()


def new_session_drafts(count: int, prefix: str) -> list[EventDraftV1]:
    """Fresh `agent.session.started` drafts: handled events the runner has not projected yet."""
    template = draft_of(session_events()[2])
    drafts = []
    for index in range(count):
        session = f"{prefix}-{index}"
        data = template.model_dump()
        data.update(
            event_id=new_uuid7(),
            stream_id=f"stream-{session}",
            idempotency_key=f"key-{session}",
            payload={**data["payload"], "session_id": session},
            context={**data["context"], "session_id": session},
        )
        drafts.append(EventDraftV1.model_validate(data))
    return drafts


class BackgroundRunner:
    """A projection runner polling in its own thread, as a deployed one would."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._stop = threading.Event()
        self._thread = threading.Thread(target=lambda: asyncio.run(self._loop()), daemon=True)
        self.delivered = 0

    async def _loop(self) -> None:
        engine = role_scoped_engine(self._dsn, "agent_context_projector")
        try:
            async with Neo4jStore(neo4j_integration_settings()) as store:
                runner = ProjectionRunner(
                    session_factory(engine), store, registered_projectors(), worker_id="e2e-race"
                )
                while not self._stop.is_set():
                    self.delivered += (await runner.run_once(1)).delivered
                    await asyncio.sleep(0.02)
        finally:
            await engine.dispose()

    def __enter__(self) -> BackgroundRunner:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=60)


@dataclass
class World:
    """The ledger after ingestion, plus how to reach it."""

    dsn: str
    owner: AsyncEngine
    facts: GraphFacts
    event_count: int
    roles: dict[str, str]

    def env(
        self,
        *,
        live_uri: str | None = None,
        projector: str = "projector",
        api: str | None = "api",
    ) -> dict[str, str]:
        """The CLI environment: login roles that are members of the two migration roles.

        There is deliberately no shared `AGENT_CONTEXT_POSTGRESQL__DSN` (the owner): the CLI must
        work with the least-privilege roles alone.
        """
        settings = neo4j_integration_settings()
        postgres = {"AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN": self.roles[projector]}
        if api is not None:
            postgres["AGENT_CONTEXT_POSTGRESQL__API_DSN"] = self.roles[api]
        return {
            **postgres,
            "AGENT_CONTEXT_NEO4J__URI": live_uri or str(settings.uri),
            "AGENT_CONTEXT_NEO4J__USERNAME": settings.username or "",
            "AGENT_CONTEXT_NEO4J__PASSWORD": settings.password.get_secret_value()
            if settings.password
            else "",
            "AGENT_CONTEXT_NEO4J__DATABASE": settings.database,
        }

    def target_env(self, *, live_uri: str | None, **roles: Any) -> dict[str, str]:
        settings = neo4j_integration_settings()
        assert settings.password is not None
        return {
            **self.env(live_uri=live_uri, **roles),
            TARGET_PASSWORD_ENV: settings.password.get_secret_value(),
        }

    def target_args(self) -> list[str]:
        settings = neo4j_integration_settings()
        return [
            "--target-uri",
            str(settings.uri),
            "--target-database",
            settings.database,
            "--target-username",
            settings.username or "",
        ]


def invoke(args: Sequence[str], env: dict[str, str]) -> Any:
    return CliRunner().invoke(app, list(args), env=env)


def report(result: Any) -> dict[str, Any]:
    assert result.output.strip(), result.stderr
    return json.loads(result.output)  # type: ignore[no-any-return]


async def project_everything(owner_dsn: str) -> None:
    engine = role_scoped_engine(owner_dsn, "agent_context_projector")
    try:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            runner = ProjectionRunner(
                session_factory(engine), store, registered_projectors(), worker_id="e2e-runner"
            )
            for _ in range(100):
                if (await runner.run_once(200)).claimed == 0:
                    return
    finally:
        await engine.dispose()
    raise AssertionError("the projection runner did not drain the outbox")


async def build_ledger(dsn: str, root: Path) -> int:
    api = create_async_engine(
        dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    sessions = session_factory(api)
    ingestion = IngestionService(
        ContentService(NoContentBlobStore(), RedactionPolicyV1()), sessions
    )
    total = 0
    try:
        fixtures = [draft_of(event) for event in session_events()]
        outcome = await ingestion.ingest(
            IngestBatchRequestV1(batch_id=new_uuid7(), events=tuple(fixtures))
        )
        assert outcome.http_status == 200
        total += len(fixtures)

        lineage: dict[str, uuid.UUID] = {}
        steps = 0

        async def index_head() -> None:
            nonlocal steps, total
            steps += 1
            when = T0 + timedelta(minutes=steps)
            service = IndexingService(
                IndexingConfig(
                    repository_id=REPO,
                    checkout_id="checkout-1",
                    clock=lambda: when,
                    monotonic=lambda: 0.0,
                ),
                ingestion,
                sessions,
            )
            head = git(root, "rev-parse", "HEAD")
            scan = scan_repository(root)
            for item in scan.files:
                lineage.setdefault(item.path, identity.file_logical_id(REPO, item.path, head))
            parsed = parse_structural(ADAPTERS, read_sources(scan)).files
            drafts = service.index(scan, None, list(parsed), file_logical_ids=lineage)
            result = await service.ingest(drafts)
            total += result.submitted

        git(root, "init", "-q")
        write(root, "pkg/calc.py", PY_CALC)
        write(root, "pkg/util.py", PY_UTIL)
        write(root, "web/app.ts", TS_APP)
        write(root, "svc/main.go", GO_MAIN)
        commit(root)
        await index_head()
        write(root, "pkg/calc.py", PY_CALC_V2)  # a content revision
        commit(root)
        await index_head()
        git(root, "mv", "pkg/util.py", "pkg/helpers.py")  # a rename keeps the logical file
        lineage["pkg/helpers.py"] = lineage["pkg/util.py"]
        commit(root)
        await index_head()
    finally:
        await api.dispose()
    return total


@pytest.fixture(scope="module")
def login_roles(postgres_dsn: str, ledger_engine: AsyncEngine) -> Iterator[dict[str, str]]:
    """LOGIN roles that are members of the real migration roles; dropped before the downgrade.

    `projector` and `api` are the roles the CLI needs. `readonly` (projector membership only) has
    no INSERT on the ledger and `apionly` (API membership only) cannot read checkpoints: each is
    the connection that is missing a grant in the preflight tests.
    """
    suffix = uuid.uuid4().hex[:8]
    members = {
        "projector": "agent_context_projector",
        "api": "agent_context_api",
        "readonly": "agent_context_projector",
        "apionly": "agent_context_api",
    }
    password = uuid.uuid4().hex
    base = make_url(postgres_dsn)
    names = {key: f"e2e_{key}_{suffix}" for key in members}

    async def create() -> None:
        engine = create_async_engine(postgres_dsn, poolclass=NullPool, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                for key, parent in members.items():
                    await connection.execute(
                        text(
                            f"CREATE ROLE {names[key]} LOGIN PASSWORD '{password}' IN ROLE {parent}"
                        )
                    )
        finally:
            await engine.dispose()

    async def drop() -> None:
        engine = create_async_engine(postgres_dsn, poolclass=NullPool, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                for name in names.values():
                    await connection.execute(text(f"DROP ROLE IF EXISTS {name}"))
        finally:
            await engine.dispose()

    asyncio.run(create())
    try:
        yield {
            key: base.set(username=name, password=password).render_as_string(hide_password=False)
            for key, name in names.items()
        }
    finally:
        asyncio.run(drop())


@pytest.fixture(scope="module")
def world(
    postgres_dsn: str,
    ledger_engine: AsyncEngine,
    login_roles: dict[str, str],
    tmp_path_factory: Any,
) -> Iterator[World]:
    """Step 1: project the realistic ledger with the real runner and record the graph facts."""
    root = tmp_path_factory.mktemp("g3-repo")
    count = asyncio.run(build_ledger(postgres_dsn, root))
    asyncio.run(project_everything(postgres_dsn))
    owner = create_async_engine(postgres_dsn, poolclass=NullPool)
    facts = asyncio.run(graph_facts())
    assert facts.nodes and facts.relationships, "the fixture ledger projected nothing"
    for label in ("Session", "Turn", "ToolCall", "Commit", "File", "FileRevision", "Symbol"):
        assert label in facts.nodes, label
    assert not [i for i in facts.node_ids if i.startswith("~")], "a projected node has no identity"
    assert count == GOLDEN_EVENTS
    assert facts.nodes == GOLDEN_NODES and facts.relationships == GOLDEN_RELATIONSHIPS
    assert facts.digest == GOLDEN_DIGEST
    pending = asyncio.run(
        sql(owner, "SELECT count(*) FROM projection.outbox WHERE status <> 'delivered'")
    )
    assert pending[0][0] == 0
    yield World(postgres_dsn, owner, facts, count, login_roles)
    asyncio.run(owner.dispose())


def test_in_place_rebuild_reproduces_digest_counts_and_ids(world: World) -> None:
    asyncio.run(run_graph("MATCH (n) DETACH DELETE n"))  # step 2: drop all graph data
    assert asyncio.run(graph_facts()).digest != world.facts.digest

    refused = invoke(["projection", "rebuild", "--in-place", "--json"], world.env())
    assert refused.exit_code == 2  # no --confirm

    result = invoke(
        [
            "projection",
            "rebuild",
            "--in-place",
            "--confirm",
            "neo4j",
            "--runner-quiet-seconds",
            "0",
            "--json",
        ],
        world.env(),
    )
    assert result.exit_code == 0, result.output
    payload = report(result)
    assert payload["outcome"] == "completed" and payload["ok"]
    assert payload["graph_digest"] == world.facts.digest

    rebuilt = asyncio.run(graph_facts())  # step 3
    assert rebuilt == world.facts

    # The report events the rebuild itself recorded are handled by no projector: still caught up.
    after = invoke(
        ["projection", "verify", "--no-record", "--require-caught-up", "--json"], world.env()
    )
    assert after.exit_code == 0, after.output
    assert all(item["ok"] for item in report(after)["projectors"])
    assert report(after)["streams"]["behind"] == 0


def test_standby_rebuild_into_an_empty_target_matches_the_digest(world: World) -> None:
    live = "bolt://live.invalid:7687"
    args = ["projection", "rebuild", *world.target_args(), "--json"]

    # The target (this instance) holds the graph: refused without an explicit wipe.
    assert invoke(args, world.target_env(live_uri=live)).exit_code == 2
    # The target must differ from the live projection.
    same = invoke(args, world.target_env(live_uri=None))
    assert same.exit_code == 2 and "live projection" in same.stderr
    # A wipe needs the database name repeated.
    unconfirmed = invoke([*args, "--wipe-target"], world.target_env(live_uri=live))
    assert unconfirmed.exit_code == 2

    checkpoints = asyncio.run(sql(world.owner, "SELECT * FROM projection.projection_checkpoints"))
    asyncio.run(run_graph("MATCH (n) DETACH DELETE n"))
    result = invoke(args, world.target_env(live_uri=live))  # step 4: a genuinely empty target
    assert result.exit_code == 0, result.output
    payload = report(result)
    assert payload["mode"] == "standby" and payload["verified_target"]
    assert payload["graph_digest"] == world.facts.digest
    assert asyncio.run(graph_facts()) == world.facts

    wiped = invoke([*args, "--wipe-target", "--confirm", "neo4j"], world.target_env(live_uri=live))
    assert wiped.exit_code == 0, wiped.output
    assert report(wiped)["graph_digest"] == world.facts.digest
    assert report(wiped)["wiped_nodes"] == sum(world.facts.nodes.values())
    assert asyncio.run(graph_facts()) == world.facts
    # A standby rebuild never touches the live checkpoints.
    assert (
        asyncio.run(sql(world.owner, "SELECT * FROM projection.projection_checkpoints"))
        == checkpoints
    )


def test_verify_matches_is_idempotent_and_fails_on_tampering(world: World) -> None:
    def recorded() -> int:
        rows = asyncio.run(
            sql(
                world.owner,
                "SELECT count(*) FROM ledger.events WHERE event_type = 'projection.rebuilt'",
            )
        )
        return int(rows[0][0])

    first = invoke(["projection", "verify", "--json"], world.env())
    assert first.exit_code == 0, first.output
    matched = report(first)
    assert matched["ok"] and matched["graph"]["digest"] == world.facts.digest
    assert matched["orphans"]["count"] == 0 and matched["streams"]["mismatched"] == 0
    assert matched["recorded"]["appended"] == len(registered_projectors())
    after_first = recorded()

    again = invoke(["projection", "verify", "--json"], world.env())  # step 6
    assert again.exit_code == 0
    assert report(again)["recorded"] == {"appended": 0, "existing": len(registered_projectors())}
    assert recorded() == after_first

    # An orphan source id in the graph.
    ghost = str(new_uuid7())
    asyncio.run(
        run_graph(
            "MATCH (n:Repository) WITH n LIMIT 1 "
            "SET n.source_event_ids = coalesce(n.source_event_ids, []) + [$ghost]",
            ghost=ghost,
        )
    )
    orphaned = invoke(["projection", "verify", "--json"], world.env())
    assert orphaned.exit_code == 1
    assert report(orphaned)["orphans"]["sample"] == [ghost]
    assert "graph: orphan_source_ids" in report(orphaned)["problems"]
    asyncio.run(
        run_graph(
            "MATCH (n:Repository) WHERE $ghost IN n.source_event_ids "
            "WITH n, [x IN n.source_event_ids WHERE x <> $ghost] AS rest "
            "SET n.source_event_ids = CASE WHEN size(rest) = 0 THEN null ELSE rest END",
            ghost=ghost,
        )
    )
    assert invoke(["projection", "verify", "--no-record"], world.env()).exit_code == 0

    # A regressed checkpoint: the pointer and count move back, the outbox says delivered.
    saved = asyncio.run(
        sql(
            world.owner,
            "SELECT last_outbox_id, last_event_id FROM projection.projection_checkpoints "
            "WHERE projector_name = 'git'",
        )
    )[0]
    asyncio.run(
        sql(
            world.owner,
            "UPDATE projection.projection_checkpoints SET last_outbox_id = o.outbox_id, "
            "last_event_id = o.event_id FROM (SELECT outbox_id, event_id FROM projection.outbox "
            "ORDER BY outbox_id LIMIT 1) o WHERE projector_name = 'git'",
        )
    )
    try:
        regressed = invoke(["projection", "verify", "--no-record", "--json"], world.env())
        assert regressed.exit_code == 1
        git_issues = {item["name"]: item["issues"] for item in report(regressed)["projectors"]}
        assert "checkpoint_regressed" in git_issues["git"]
    finally:
        asyncio.run(
            sql(
                world.owner,
                "UPDATE projection.projection_checkpoints SET last_outbox_id = :o, "
                "last_event_id = :e WHERE projector_name = 'git'",
                o=saved[0],
                e=saved[1],
            )
        )
    assert invoke(["projection", "verify", "--no-record"], world.env()).exit_code == 0


def test_verify_replay_check_compares_a_scratch_replay(world: World) -> None:
    # The CLI refuses a scratch target equal to the live graph, and a replay check needs one.
    refused = invoke(
        ["projection", "verify", "--replay-check", "--no-record", *world.target_args()],
        world.target_env(live_uri=None),
    )
    assert refused.exit_code == 2
    assert invoke(["projection", "verify", "--replay-check"], world.env()).exit_code == 2

    async def replay_check() -> Any:
        # With one Neo4j instance the live graph and the scratch target are the same instance:
        # the live graph is scanned first, then wiped and replayed; the digests must agree.
        engine = create_async_engine(world.dsn, poolclass=NullPool)
        try:
            async with Neo4jStore(neo4j_integration_settings()) as store:
                return await verify_projections(
                    session_factory(engine),
                    store,
                    registered_projectors(),
                    replay_target=store,
                    wipe_replay_target=True,
                )
        finally:
            await engine.dispose()

    checked = asyncio.run(replay_check())
    assert checked.replay_matches is True and checked.ok()
    assert checked.replay_digest == world.facts.digest
    assert asyncio.run(graph_facts()) == world.facts


def test_status_reports_every_projector(world: World) -> None:
    result = invoke(["projection", "status", "--json"], world.env())
    assert result.exit_code == 0, result.output
    statuses = report(result)["projectors"]
    assert [item["name"] for item in statuses] == [p.name for p in registered_projectors()]
    assert all(item["lag"] == 0 for item in statuses)
    assert "password" not in result.output.lower()


def test_the_installed_entry_point_runs_the_cli(world: World) -> None:
    executable = Path(sys.executable).parent / "agent-context"
    assert executable.exists(), "the project scripts are not installed"
    completed = subprocess.run(
        [str(executable), "projection", "verify", "--no-record", "--json"],
        env={"PATH": os.environ.get("PATH", ""), **world.env()},
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["ok"] and payload["graph"]["digest"] == world.facts.digest
    assert urlsplit(str(neo4j_integration_settings().uri)).netloc not in completed.stderr


def test_projector_only_connection_is_enough_when_nothing_is_recorded(world: World) -> None:
    """Record off needs no API connection at all (and no shared DSN)."""
    env = world.env(api=None)

    verified = invoke(["projection", "verify", "--no-record", "--json"], env)
    assert verified.exit_code == 0, verified.output
    standby = invoke(
        [
            "projection",
            "rebuild",
            *world.target_args(),
            "--wipe-target",
            "--confirm",
            "neo4j",
            "--no-record",
            "--json",
        ],
        world.target_env(live_uri="bolt://live.invalid:7687", api=None),
    )
    assert standby.exit_code == 0, standby.output
    assert report(standby)["recorded"] is None and not report(standby)["record_error"]
    in_place = invoke(
        [
            "projection",
            "rebuild",
            "--in-place",
            "--confirm",
            "neo4j",
            "--no-record",
            "--runner-quiet-seconds",
            "0",
            "--json",
        ],
        env,
    )
    assert in_place.exit_code == 0, in_place.output
    assert report(in_place)["graph_digest"] == world.facts.digest
    # Recording is required by default, and its connection is a different one.
    assert invoke(["projection", "verify"], env).exit_code == 2


def test_a_missing_grant_fails_before_any_target_is_wiped_or_checkpoint_reset(world: World) -> None:
    live = "bolt://live.invalid:7687"
    checkpoints = asyncio.run(sql(world.owner, "SELECT * FROM projection.projection_checkpoints"))
    wipe = ["projection", "rebuild", *world.target_args(), "--wipe-target", "--confirm", "neo4j"]

    # The API connection cannot insert into the ledger: recording would fail after the wipe.
    standby = invoke(wipe, world.target_env(live_uri=live, api="readonly"))
    assert standby.exit_code == 1 and "api connection lacks" in standby.stderr
    assert "INSERT on ledger.events" in standby.stderr
    in_place = invoke(
        ["projection", "rebuild", "--in-place", "--confirm", "neo4j"], world.env(api="readonly")
    )
    assert in_place.exit_code == 1 and "api connection lacks" in in_place.stderr
    # The projector connection cannot read checkpoints.
    blind = invoke(["projection", "verify"], world.env(projector="apionly"))
    assert blind.exit_code == 1 and "projector connection lacks" in blind.stderr
    assert "SELECT on projection.projection_checkpoints" in blind.stderr

    assert asyncio.run(graph_facts()) == world.facts  # nothing was wiped
    assert (
        asyncio.run(sql(world.owner, "SELECT * FROM projection.projection_checkpoints"))
        == checkpoints
    )
    for secret in world.roles.values():
        assert secret.split(":")[2].split("@")[0] not in standby.stderr + blind.stderr


def test_a_recording_failure_does_not_mask_a_verified_rebuild(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Broken:
        async def record(self, _runs: Any) -> Any:
            raise RuntimeError("the ledger refused it")

    monkeypatch.setattr(cli, "_recorder", lambda _runtime: Broken())
    args = [
        "projection",
        "rebuild",
        *world.target_args(),
        "--wipe-target",
        "--confirm",
        "neo4j",
        "--json",
    ]

    result = invoke(args, world.target_env(live_uri="bolt://live.invalid:7687"))

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["record_error"] == "record_failed" and payload["ok"]
    assert payload["verified_target"] and payload["graph_digest"] == world.facts.digest
    assert "error: record_failed" in result.stderr and "refused" not in result.output
    assert asyncio.run(graph_facts()) == world.facts


def test_the_runner_guard_refuses_recent_outbox_activity_and_other_projector_connections(
    world: World,
) -> None:
    args = ["projection", "rebuild", "--in-place", "--confirm", "neo4j", "--no-record"]

    # Outbox rows were claimed and delivered moments ago by the fixture's runner.
    recent = invoke([*args, "--runner-quiet-seconds", "86400"], world.env(api=None))
    assert recent.exit_code == 1 and "outbox rows changed" in recent.stderr

    # Another login that is a member of the projector role is connected: a runner may be running.
    sync_dsn = world.roles["readonly"].replace("postgresql+psycopg", "postgresql")
    with psycopg.connect(sync_dsn) as other:
        other.execute("SELECT 1")
        busy = invoke([*args, "--runner-quiet-seconds", "0"], world.env(api=None))
    assert busy.exit_code == 1 and "another projector-role connection" in busy.stderr
    assert asyncio.run(graph_facts()) == world.facts  # nothing was wiped


def test_a_second_rebuild_is_refused_while_one_holds_the_rebuild_lock(world: World) -> None:
    """Two rebuilds must not interleave: the one that finds the advisory lock held is refused."""
    in_place = ["projection", "rebuild", "--in-place", "--confirm", "neo4j", "--no-record"]
    standby = ["projection", "rebuild", *world.target_args(), "--wipe-target", "--confirm", "neo4j"]
    live = "bolt://live.invalid:7687"
    sync_dsn = world.roles["readonly"].replace("postgresql+psycopg", "postgresql")
    checkpoints = asyncio.run(sql(world.owner, "SELECT * FROM projection.projection_checkpoints"))

    # The first rebuild's connection: a session-level lock on the CLI's fixed key.
    with psycopg.connect(sync_dsn, autocommit=True) as first:
        first.execute("SELECT pg_advisory_lock(%s)", (verify.REBUILD_LOCK_KEY,))
        refused = invoke([*in_place, "--runner-quiet-seconds", "0"], world.env(api=None))
        refused_standby = invoke(standby, world.target_env(live_uri=live))
        first.execute("SELECT pg_advisory_unlock(%s)", (verify.REBUILD_LOCK_KEY,))

    assert refused.exit_code == 1 and "rebuild lock" in refused.stderr
    assert refused_standby.exit_code == 1 and "rebuild lock" in refused_standby.stderr
    assert asyncio.run(graph_facts()) == world.facts  # nothing was wiped
    assert (
        asyncio.run(sql(world.owner, "SELECT * FROM projection.projection_checkpoints"))
        == checkpoints
    )
    # Once it is released, a rebuild runs, and releases the lock again when it ends.
    done = invoke([*in_place, "--runner-quiet-seconds", "0"], world.env(api=None))
    assert done.exit_code == 0, done.output
    held = asyncio.run(
        sql(world.owner, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'")
    )
    assert held[0][0] == 0


def test_a_runner_racing_an_in_place_rebuild_never_rolls_checkpoints_back(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bypass the best-effort guard to force the race it exists to prevent.

    A runner keeps projecting new events while the graph is wiped and replayed. The rebuild must
    either detect that and say so, or finish with checkpoints that agree with the outbox.
    """

    async def no_guard(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(verify, "ensure_runner_idle", no_guard)
    asyncio.run(ingest_drafts(world.dsn, new_session_drafts(60, "race")))
    rebuild = ["projection", "rebuild", "--in-place", "--confirm", "neo4j", "--no-record", "--json"]

    with BackgroundRunner(world.dsn) as runner:
        raced = invoke(rebuild, world.env(api=None))
    assert runner.delivered > 0, "the runner was not projecting during the rebuild"
    asyncio.run(project_everything(world.dsn))  # drain what the runner had not reached

    if raced.exit_code == 0:
        assert invoke(["projection", "verify", "--no-record"], world.env(api=None)).exit_code == 0
    else:
        assert "stop the projection runner" in raced.stderr
    monkeypatch.undo()
    # With the runner stopped the rebuild converges, and the graph equals a fresh replay.
    settled = invoke([*rebuild, "--runner-quiet-seconds", "0"], world.env(api=None))
    assert settled.exit_code == 0, settled.output
    digest = report(settled)["graph_digest"]
    again = invoke([*rebuild, "--runner-quiet-seconds", "0"], world.env(api=None))
    assert report(again)["graph_digest"] == digest
    assert invoke(["projection", "verify", "--no-record"], world.env(api=None)).exit_code == 0


def test_a_standby_rebuild_skips_a_dead_lettered_event_like_the_live_projection(
    world: World,
) -> None:
    [poison] = new_session_drafts(1, "poison")
    asyncio.run(ingest_drafts(world.dsn, [poison]))
    asyncio.run(
        sql(
            world.owner,
            "UPDATE projection.outbox SET status = 'dead_lettered', retry_count = 5, "
            "dead_lettered_at = now(), last_error_class = 'PoisonError', updated_at = now() "
            "WHERE event_id = :event",
            event=poison.event_id,
        )
    )
    live = asyncio.run(graph_facts())  # the live runner never projected the poison event

    result = invoke(
        [
            "projection",
            "rebuild",
            *world.target_args(),
            "--wipe-target",
            "--confirm",
            "neo4j",
            "--json",
        ],
        world.target_env(live_uri="bolt://live.invalid:7687"),
    )

    assert result.exit_code == 0, result.output
    payload = report(result)
    assert payload["skipped_dead_lettered"] == {"count": 1, "event_ids": [str(poison.event_id)]}
    assert payload["graph_digest"] == live.digest
    assert asyncio.run(graph_facts()) == live


def test_the_digest_scales_and_ignores_batch_size(world: World) -> None:
    """A few thousand nodes: paged scans of any size give the same digest and counts."""

    async def main() -> None:
        await run_graph("MATCH (n) DETACH DELETE n")
        await run_graph(
            "MERGE (r:Repository {repository_id: 'scale'}) WITH r "
            "UNWIND range(1, 3000) AS i "
            "MERGE (c:Commit {commit_id: 'c' + toString(i)}) "
            "SET c.message = 'm' + toString(i) "
            "MERGE (r)-[x:HAS_COMMIT]->(c) SET x.source_event_ids = []"
        )
        async with Neo4jStore(neo4j_integration_settings()) as store:
            results = [
                await compute_graph_digest(store, batch_size=size) for size in (7, 500, 5000)
            ]
        assert len({item.digest for item in results}) == 1
        assert results[0].nodes == {"Commit": 3000, "Repository": 1}
        assert results[0].relationships == {"HAS_COMMIT": 3000}

    asyncio.run(main())
