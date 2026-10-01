"""Temporal code graph: real emitter output replayed through the `CodeProjector` (PLATFORM-038).

The events come from `IndexingService` over a temp git repository (one index per commit, with its
own clock), sealed with the SDK. The golden assertions are exact summaries of the derived graph
after each index (first index, unchanged re-index, unrelated commit, content revision, rename,
deletion, supersession), and a digest of the WHOLE graph (nodes, relationships, every property)
proves that occurred order, observed order, reversed order and duplicated delivery converge.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import subprocess
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, LiteralString

import pytest
from agent_context_sdk import (
    CodeRelationAssertedV1,
    CodeRelationPredicate,
    DeterministicEvidenceKind,
    EventDraftV1,
    StoredEventV1,
    seal_event,
)

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
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.projectors.code import (
    CodeProjector,
    EntityKind,
    entity_as_of,
    timestamp,
)

from ..conftest import neo4j_integration_settings
from .conftest import project_event

pytestmark = pytest.mark.integration

REPO = "repo-038"
T0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
PROJECTORS = (CodeProjector(),)


def at(step: int) -> datetime:
    return T0 + timedelta(minutes=step)


class InProcessPython:
    language = "python"

    def parse(self, request: ParseRequest) -> ParsedModule:
        return python_adapter.parse_python(request)


class Repo:
    """A real, isolated git repository."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(root.parent),
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Test",
            "GIT_AUTHOR_EMAIL": "test@example.invalid",
            "GIT_COMMITTER_NAME": "Test",
            "GIT_COMMITTER_EMAIL": "test@example.invalid",
        }
        root.mkdir(parents=True)
        self.git("init", "-q")

    def git(self, *args: str) -> str:
        done = subprocess.run(
            ["git", "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false", *args],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            check=True,
            text=True,
        )
        return done.stdout.strip()

    def write(self, path: str, text: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def commit(self) -> str:
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "change")
        return self.git("rev-parse", "HEAD")


def index_events(
    repo: Repo,
    lineage: dict[str, uuid.UUID],
    when: datetime,
    *,
    observed: datetime | None = None,
    seen: set[str] | None = None,
    tweak: Callable[[EventDraftV1], EventDraftV1] | None = None,
) -> list[StoredEventV1]:
    """Sealed events of indexing HEAD at `when`; keys already in `seen` are replays (skipped)."""
    scan = scan_repository(repo.root)
    structural = parse_structural({"python": InProcessPython()}, read_sources(scan))  # type: ignore[dict-item]
    service = IndexingService(
        IndexingConfig(
            repository_id=REPO,
            checkout_id="checkout-1",
            clock=lambda: when,
            monotonic=lambda: 0.0,
        )
    )
    drafts = service.index(scan, None, list(structural.files), file_logical_ids=lineage)
    fresh = [d for d in drafts if seen is None or d.idempotency_key not in seen]
    return seal([d if tweak is None else tweak(d) for d in fresh], when, observed)


def seal(
    drafts: Sequence[EventDraftV1], when: datetime, observed: datetime | None = None
) -> list[StoredEventV1]:
    return [
        seal_event(
            d.model_copy(update={"occurred_at": when, "observed_at": observed or when}),
            [],
            1,
            None,
        )
        for d in drafts
    ]


def lineage_for(
    head: str, paths: Sequence[str], inherited: dict[str, uuid.UUID]
) -> dict[str, uuid.UUID]:
    return {p: inherited.get(p) or identity.file_logical_id(REPO, p, head) for p in paths}


# --- graph helpers (the shared `graph_state` only knows the P025 labels) ---

_KEYS = (
    "file_revision_id",
    "symbol_revision_id",
    "assertion_id",
    "dependency_id",
    "module_id",
    "symbol_id",
    "file_id",
    "commit_id",
    "snapshot_id",
    "repository_id",
)
_NODES: LiteralString = "MATCH (n) RETURN labels(n) AS labels, properties(n) AS props"
_RELS: LiteralString = (
    "MATCH (a)-[r]->(b) RETURN labels(a) AS a_labels, properties(a) AS a_props, type(r) AS type, "
    "properties(r) AS props, labels(b) AS b_labels, properties(b) AS b_props"
)


def _ident(labels: list[str], props: dict[str, Any]) -> str:
    key = next(k for k in _KEYS if k in props)
    return f"{labels[0]}:{props[key]}"


async def graph_state(store: Neo4jStore) -> dict[str, list[Any]]:
    async def read(tx: Neo4jTransaction) -> dict[str, list[Any]]:
        nodes = (await tx.run(_NODES, parameters={})).records
        rels = (await tx.run(_RELS, parameters={})).records
        return {
            "nodes": sorted(
                [{"id": _ident(r["labels"], r["props"]), "props": r["props"]} for r in nodes],
                key=lambda n: n["id"],
            ),
            "relationships": sorted(
                [
                    {
                        "from": _ident(r["a_labels"], r["a_props"]),
                        "type": r["type"],
                        "to": _ident(r["b_labels"], r["b_props"]),
                        "props": r["props"],
                    }
                    for r in rels
                ],
                key=lambda r: (
                    r["from"],
                    r["type"],
                    r["to"],
                    json.dumps(r["props"], sort_keys=True, default=str),
                ),
            ),
        }

    return await store.execute_read(read)


def digest(state: dict[str, list[Any]]) -> str:
    return hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()


async def wipe(store: Neo4jStore) -> None:
    async def run(tx: Neo4jTransaction) -> None:
        await tx.run("MATCH (n) DETACH DELETE n", parameters={})

    await store.execute_write(run)


def with_graph(body: Callable[[Neo4jStore], Awaitable[None]]) -> None:
    async def run() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)
            try:
                await body(store)
            finally:
                await wipe(store)

    asyncio.run(run())


async def deliver(store: Neo4jStore, events: Sequence[StoredEventV1]) -> None:
    for event in events:
        await project_event(store, event, PROJECTORS)


_FILES: LiteralString = (
    "MATCH (f:File) OPTIONAL MATCH (f)-[h:HAS_REVISION]->(fr:FileRevision) "
    "RETURN f.file_id AS id, f.current AS current, f.current_path AS path, f.valid_to AS valid_to, "
    "fr.file_revision_id AS revision, h.path AS row_path, h.valid_from AS vf, h.valid_to AS vt "
    "ORDER BY id, vf"
)
_SYMBOLS: LiteralString = (
    "MATCH (s:Symbol) OPTIONAL MATCH (s)-[h:HAS_REVISION]->(sr:SymbolRevision) "
    "RETURN s.symbol_id AS id, s.current AS current, sr.qualified_name AS name, "
    "sr.symbol_revision_id AS revision, h.valid_from AS vf, h.valid_to AS vt ORDER BY name, vf"
)
_ASSERTIONS: LiteralString = (
    "MATCH (a:Assertion) RETURN a.predicate AS predicate, a.subject_id AS subject, "
    "a.object_id AS object, a.current AS current, a.valid_intervals AS intervals, "
    "a.evidence_kind AS evidence, a.confidence AS confidence"
)
_COUNTS: LiteralString = (
    "MATCH (n) WITH labels(n)[0] AS label, count(n) AS c RETURN label, c ORDER BY label"
)


class Names:
    """Stable names for times, file revisions and symbols, so goldens do not hold UUIDs."""

    def __init__(self) -> None:
        self.times = {timestamp(at(i)): f"t{i}" for i in range(0, 20)}
        self.revisions: dict[str, str] = {}
        self.symbols: dict[str, str] = {}
        self.files: dict[str, str] = {}

    def time(self, value: str | None) -> str | None:
        return None if value is None else self.times[value]

    def revision(self, value: str, stem: str) -> str:
        """`<stem><n>`: the n-th distinct revision named for the file's first path."""
        if value not in self.revisions:
            count = sum(1 for name in self.revisions.values() if name.rstrip("0123456789") == stem)
            self.revisions[value] = f"{stem}{count + 1}"
        return self.revisions[value]


async def summary(store: Neo4jStore, names: Names) -> dict[str, Any]:
    async def read(tx: Neo4jTransaction) -> dict[str, Any]:
        files = (await tx.run(_FILES, parameters={})).records
        symbols = (await tx.run(_SYMBOLS, parameters={})).records
        assertions = (await tx.run(_ASSERTIONS, parameters={})).records
        counts = (await tx.run(_COUNTS, parameters={})).records
        by_file: dict[str, Any] = {}
        for r in files:
            names.files[r["id"]] = f"file:{r['row_path'] or r['path']}"
            entry = by_file.setdefault(
                r["id"],
                {
                    "current": r["current"],
                    "path": r["path"],
                    "valid_to": names.time(r["valid_to"]),
                    "rows": [],
                },
            )
            if r["revision"] is not None:
                entry["rows"].append(
                    (
                        names.revision(
                            r["revision"],
                            Path(entry["rows"][0][1] if entry["rows"] else r["row_path"]).stem,
                        ),
                        r["row_path"],
                        names.time(r["vf"]),
                        names.time(r["vt"]),
                    )
                )
        by_symbol: dict[str, Any] = {}
        for r in symbols:
            if r["name"] is None:
                continue
            names.symbols[r["id"]] = r["name"]
            entry = by_symbol.setdefault(r["name"], {"current": r["current"], "rows": []})
            entry["rows"].append((names.time(r["vf"]), names.time(r["vt"])))
        edges = sorted(
            (
                r["predicate"],
                names.symbols.get(r["subject"]) or names.files.get(r["subject"], "?"),
                names.symbols.get(r["object"], "?"),
                r["evidence"],
                r["current"],
                tuple(
                    i.replace("2026-03-01T", "").replace(".000000Z", "")
                    for i in r["intervals"] or ()
                ),
            )
            for r in assertions
        )
        return {
            "files": {(v["path"] or v["rows"][-1][1]): v for v in by_file.values()},
            "symbols": by_symbol,
            "assertions": edges,
            "counts": {r["label"]: r["c"] for r in counts},
        }

    return await store.execute_read(read)


# --- scenario ---

A0 = "def f():\n    return 1\n"
A1 = "def f():\n    return 2\n"
B = "from .a import f\n\n\ndef g():\n    return f()\n"
C = "def k():\n    return 3\n"


class Timeline:
    """Index runs of one evolving repository, as sealed events per step."""

    def __init__(self, tmp: Path) -> None:
        self.repo = Repo(tmp / "repo")
        self.seen: set[str] = set()
        self.lineage: dict[str, uuid.UUID] = {}
        self.known: dict[str, uuid.UUID] = {}  # every path ever indexed
        self.runs: list[list[StoredEventV1]] = []

    def index(
        self,
        step: int,
        *,
        observed: int | None = None,
        tweak: Callable[[EventDraftV1], EventDraftV1] | None = None,
    ) -> list[StoredEventV1]:
        head = self.repo.git("rev-parse", "HEAD")
        paths = [p for p in self.repo.git("ls-files").splitlines() if p.endswith(".py")]
        self.lineage = lineage_for(head, paths, self.lineage)
        self.known.update(self.lineage)
        events = index_events(
            self.repo,
            self.lineage,
            at(step),
            observed=None if observed is None else at(observed),
            seen=self.seen,
            tweak=tweak,
        )
        self.seen |= {e.idempotency_key for e in events}
        self.runs.append(events)
        return events

    def all_events(self) -> list[StoredEventV1]:
        return [e for run in self.runs for e in run]


def build_timeline(tmp: Path) -> tuple[Timeline, dict[str, list[StoredEventV1]]]:
    line = Timeline(tmp)
    repo = line.repo
    runs: dict[str, list[StoredEventV1]] = {}
    repo.write("pkg/a.py", A0)
    repo.write("pkg/b.py", B)
    repo.commit()
    runs["first"] = line.index(1)
    runs["again"] = line.index(2)  # the same target again: every claim is a replay
    repo.write("pkg/c.py", C)  # an unrelated file: a and b are unchanged
    repo.commit()
    runs["unrelated"] = line.index(3)
    repo.write("pkg/a.py", A1)
    repo.commit()
    runs["revision"] = line.index(4, observed=8)  # observed late: transaction time differs
    repo.git("mv", "pkg/c.py", "pkg/d.py")
    line.lineage["pkg/d.py"] = line.lineage["pkg/c.py"]  # the rename keeps the logical ID
    repo.commit()
    runs["rename"] = line.index(5)
    repo.git("rm", "-q", "pkg/a.py")
    repo.commit()
    runs["deleted"] = line.index(6)
    return line, runs


def supersession_events(line: Timeline) -> list[StoredEventV1]:
    old, new = line.known["pkg/a.py"], line.known["pkg/b.py"]
    payload = CodeRelationAssertedV1(
        assertion_id="supersedes-1",
        evidence_kind=DeterministicEvidenceKind.GIT,
        deterministic=True,
        extractor_name="agent-context-platform-indexer",
        extractor_version="1",
        confidence=1.0,
        valid_from=at(7),
        index_id="idx-supersession",
        subject_id=str(new),
        predicate=CodeRelationPredicate.POSSIBLY_SUPERSEDES,
        object_id=str(old),
    )
    draft = EventDraftV1.model_validate(
        {
            **line.runs[0][0].model_dump(include={"stream_id", "producer", "redaction", "context"}),
            "event_id": uuid.UUID("0198a4b1-98c0-7c28-ae3f-00000000a007"),
            "event_type": "code.relation.asserted",
            "occurred_at": at(7),
            "observed_at": at(7),
            "payload": payload.model_dump(mode="json"),
            "idempotency_key": "supersession-1",
        }
    )
    return seal([draft], at(7))


def test_golden_graph_after_each_index(tmp_path: Path) -> None:
    line, runs = build_timeline(tmp_path)
    names = Names()
    two_intervals = ("12:01:00/12:04:00", "12:04:00/")

    async def body(store: Neo4jStore) -> None:
        # first index
        await deliver(store, runs["first"])
        first = await summary(store, names)
        assert first == {
            "files": {
                "pkg/a.py": {
                    "current": True,
                    "path": "pkg/a.py",
                    "valid_to": None,
                    "rows": [("a1", "pkg/a.py", "t1", None)],
                },
                "pkg/b.py": {
                    "current": True,
                    "path": "pkg/b.py",
                    "valid_to": None,
                    "rows": [("b1", "pkg/b.py", "t1", None)],
                },
            },
            "symbols": {
                "pkg.a": {"current": True, "rows": [("t1", None)]},
                "pkg.a.f": {"current": True, "rows": [("t1", None)]},
                "pkg.b": {"current": True, "rows": [("t1", None)]},
                "pkg.b.g": {"current": True, "rows": [("t1", None)]},
            },
            "assertions": [
                ("CALLS", "pkg.b.g", "pkg.a.f", "tree_sitter", True, ("12:01:00/",)),
                ("DEFINES", "file:pkg/a.py", "pkg.a", "tree_sitter", True, ("12:01:00/",)),
                ("DEFINES", "file:pkg/a.py", "pkg.a.f", "tree_sitter", True, ("12:01:00/",)),
                ("DEFINES", "file:pkg/b.py", "pkg.b", "tree_sitter", True, ("12:01:00/",)),
                ("DEFINES", "file:pkg/b.py", "pkg.b.g", "tree_sitter", True, ("12:01:00/",)),
                ("IMPORTS", "pkg.b", "pkg.a.f", "tree_sitter", True, ("12:01:00/",)),
            ],
            "counts": {
                "Assertion": 6,
                "Commit": 1,
                "File": 2,
                "FileRevision": 2,
                "Module": 1,
                "Repository": 1,
                "Symbol": 4,
                "SymbolRevision": 4,
            },
        }
        state_first = digest(await graph_state(store))

        # unchanged re-index: every claim is a replay, so nothing new reaches the projector
        assert runs["again"] == []
        await deliver(store, runs["first"])  # at-least-once: the same events again
        assert digest(await graph_state(store)) == state_first

        # an unrelated commit: a and b are unchanged (no new revision, assertion or interval)
        await deliver(store, runs["unrelated"])
        unrelated = await summary(store, names)
        assert unrelated["files"]["pkg/a.py"] == first["files"]["pkg/a.py"]
        assert unrelated["files"]["pkg/b.py"] == first["files"]["pkg/b.py"]
        assert unrelated["files"]["pkg/c.py"]["rows"] == [("c1", "pkg/c.py", "t3", None)]
        assert unrelated["counts"] == {
            "Assertion": 8,
            "Commit": 2,
            "File": 3,
            "FileRevision": 3,
            "Module": 1,
            "Repository": 1,
            "Symbol": 6,
            "SymbolRevision": 6,
        }
        assert [e for e in unrelated["assertions"] if "pkg/c.py" not in e[1]] == first["assertions"]

        # a content revision: new revision nodes, the old interval closed (never deleted)
        await deliver(store, runs["revision"])
        revised = await summary(store, names)
        assert revised["files"]["pkg/a.py"]["rows"] == [
            ("a1", "pkg/a.py", "t1", "t4"),
            ("a2", "pkg/a.py", "t4", None),
        ]
        assert revised["symbols"]["pkg.a.f"]["rows"] == [("t1", "t4"), ("t4", None)]
        assert revised["symbols"]["pkg.b.g"] == first["symbols"]["pkg.b.g"]  # unchanged
        defines = [
            e for e in revised["assertions"] if e[:3] == ("DEFINES", "file:pkg/a.py", "pkg.a.f")
        ]
        assert [e[4:] for e in defines] == [(False, ("12:01:00/12:04:00",)), (True, ("12:04:00/",))]
        calls = [e for e in revised["assertions"] if e[0] == "CALLS"]
        assert calls == [("CALLS", "pkg.b.g", "pkg.a.f", "tree_sitter", True, ("12:01:00/",))]
        assert revised["counts"]["FileRevision"] == 4 and revised["counts"]["SymbolRevision"] == 8
        assert two_intervals == ("12:01:00/12:04:00", "12:04:00/")

        # a rename: the logical file keeps its ID and revision and gains a path revision
        await deliver(store, runs["rename"])
        renamed = await summary(store, names)
        assert renamed["files"]["pkg/d.py"] == {
            "current": True,
            "path": "pkg/d.py",
            "valid_to": None,
            "rows": [("c1", "pkg/c.py", "t3", "t5"), ("c1", "pkg/d.py", "t5", None)],
        }
        assert renamed["counts"]["File"] == 3 and renamed["counts"]["FileRevision"] == 4
        # symbols named for the new path appear at the rename; the emitter gives the old-path
        # symbols (same file revision) no per-target membership, so they are not closed
        assert renamed["symbols"]["pkg.d.k"] == {"current": True, "rows": [("t5", None)]}

        # a deletion: validity closes, nothing is removed, the dangling edges go stale
        await deliver(store, runs["deleted"])
        deleted = await summary(store, names)
        assert deleted["files"]["pkg/a.py"] == {
            "current": False,
            "path": None,
            "valid_to": "t6",
            "rows": [("a1", "pkg/a.py", "t1", "t4"), ("a2", "pkg/a.py", "t4", "t6")],
        }
        assert deleted["symbols"]["pkg.a.f"] == {
            "current": False,
            "rows": [("t1", "t4"), ("t4", "t6")],
        }
        assert deleted["symbols"]["pkg.b.g"]["current"] is True
        assert [e for e in deleted["assertions"] if e[0] in ("CALLS", "IMPORTS")] == [
            ("CALLS", "pkg.b.g", "pkg.a.f", "tree_sitter", False, ("12:01:00/12:06:00",)),
            ("IMPORTS", "pkg.b", "pkg.a.f", "tree_sitter", False, ("12:01:00/12:06:00",)),
        ]  # b is unchanged, its target is gone: stale
        assert deleted["counts"] == {**renamed["counts"], "Commit": 5}
        assert await edge_types(store) == {"DEFINES": 6}  # only current assertions are edges

        # supersession is not file-bound: current from its valid_from
        await deliver(store, supersession_events(line))
        assert await edge_types(store) == {"DEFINES": 6, "POSSIBLY_SUPERSEDES": 1}
        assert (await summary(store, names))["counts"]["Assertion"] == 13

    with_graph(body)


async def edge_types(store: Neo4jStore) -> dict[str, int]:
    async def read(tx: Neo4jTransaction) -> dict[str, int]:
        rows = (
            await tx.run(
                "MATCH ()-[r:CALLS|REFERENCES|IMPORTS|DEFINES|POSSIBLY_SUPERSEDES]->() "
                "RETURN type(r) AS t, count(r) AS c ORDER BY t",
                parameters={},
            )
        ).records
        return {r["t"]: r["c"] for r in rows}

    return await store.execute_read(read)


def degraded(file_id: str) -> Callable[[EventDraftV1], EventDraftV1]:
    """Coverage of `file_id` reports a loss, and the run says files_degraded."""

    def tweak(draft: EventDraftV1) -> EventDraftV1:
        payload = dict(draft.payload)
        if draft.event_type == "code.file.coverage_reported" and payload["file_id"] == file_id:
            payload |= {"complete": False, "losses": ["file_degraded"]}
        if draft.event_type == "code.index.completed":
            payload |= {"success": False, "error_class": "files_degraded"}
        return draft.model_copy(update={"payload": payload})

    return tweak


def scan_incomplete(draft: EventDraftV1) -> EventDraftV1:
    if draft.event_type != "code.index.completed":
        return draft
    payload = dict(draft.payload) | {"success": False, "error_class": "scan_incomplete"}
    return draft.model_copy(update={"payload": payload})


def test_absence_is_only_inferred_from_complete_coverage_of_a_complete_scan(tmp_path: Path) -> None:
    line = Timeline(tmp_path)
    repo = line.repo
    repo.write("pkg/a.py", A0)
    repo.write("pkg/b.py", B)
    repo.commit()
    first = line.index(1)
    b_id = str(line.known["pkg/b.py"])
    a_id = str(line.known["pkg/a.py"])
    # b drops g (adds h) but its coverage is degraded: nothing it had may be called absent
    repo.write("pkg/b.py", "def h():\n    return 4\n")
    repo.commit()
    partial = line.index(2, tweak=degraded(b_id))
    # the scan is incomplete: a deleted file and a clean rewrite of b still close nothing
    repo.git("rm", "-q", "pkg/a.py")
    repo.write("pkg/b.py", "def h():\n    return 5\n")
    repo.commit()
    blind = line.index(3, tweak=scan_incomplete)
    # a clean, complete run finally closes both
    repo.write("pkg/b.py", "def h():\n    return 6\n")
    repo.commit()
    clean = line.index(4)
    names = Names()

    async def body(store: Neo4jStore) -> None:
        await deliver(store, first)
        await summary(store, names)  # learn symbol names
        await deliver(store, partial)
        after_partial = await summary(store, names)
        assert after_partial["files"]["pkg/b.py"]["rows"] == [
            ("b1", "pkg/b.py", "t1", "t2"),
            ("b2", "pkg/b.py", "t2", None),
        ]
        assert after_partial["symbols"]["pkg.b.g"] == {"current": True, "rows": [("t1", None)]}
        assert after_partial["symbols"]["pkg.b.h"] == {"current": True, "rows": [("t2", None)]}
        assert [e[:2] + e[4:] for e in after_partial["assertions"] if e[0] == "CALLS"] == [
            ("CALLS", "pkg.b.g", True, ("12:01:00/",))
        ]

        await deliver(store, blind)
        after_blind = await summary(store, names)
        assert after_blind["files"]["pkg/a.py"]["current"] is True  # missing, but not provably gone
        assert after_blind["symbols"]["pkg.b.g"]["current"] is True
        assert after_blind["symbols"]["pkg.a.f"]["current"] is True

        await deliver(store, clean)
        after_clean = await summary(store, names)
        assert after_clean["files"]["pkg/a.py"]["current"] is False
        assert after_clean["files"]["pkg/a.py"]["valid_to"] == "t4"
        assert after_clean["symbols"]["pkg.b.g"] == {"current": False, "rows": [("t1", "t4")]}
        assert after_clean["symbols"]["pkg.a.f"]["rows"] == [("t1", "t4")]
        assert a_id in names.files

    with_graph(body)


def orders(events: list[StoredEventV1]) -> dict[str, list[StoredEventV1]]:
    shuffled = list(events)
    random.Random(38).shuffle(shuffled)
    return {
        "occurred": sorted(events, key=lambda e: (e.occurred_at, str(e.event_id))),
        "observed": sorted(events, key=lambda e: (e.observed_at, str(e.event_id))),
        "reversed": list(reversed(events)),
        "shuffled": shuffled,
        "duplicated": [*events, *shuffled],
    }


Probe = tuple[EntityKind, str]


def test_every_delivery_order_gives_the_same_graph_and_as_of_answers(tmp_path: Path) -> None:
    line, _ = build_timeline(tmp_path)
    events = [*line.all_events(), *supersession_events(line)]
    grid = [(valid, recorded) for valid in (0, 1, 3, 4, 5, 6, 7) for recorded in (None, 2, 5, 7, 9)]

    async def probes(store: Neo4jStore) -> list[Probe]:
        async def read(tx: Neo4jTransaction) -> list[Probe]:
            rows = (
                await tx.run(
                    "MATCH (f:File) RETURN 'file' AS kind, f.file_id AS id UNION "
                    "MATCH (s:Symbol) RETURN 'symbol' AS kind, s.symbol_id AS id UNION "
                    "MATCH (a:Assertion) RETURN 'assertion' AS kind, a.assertion_id AS id",
                    parameters={},
                )
            ).records
            return sorted((r["kind"], r["id"]) for r in rows)

        return await store.execute_read(read)

    async def answers(store: Neo4jStore, items: list[Probe]) -> list[Any]:
        async def read(tx: Neo4jTransaction) -> list[Any]:
            out = []
            for kind, entity in items:
                for valid, recorded in grid:
                    out.append(
                        await entity_as_of(
                            tx,
                            REPO,
                            kind,
                            entity,
                            valid_at=at(valid) + timedelta(seconds=30),
                            recorded_at=None
                            if recorded is None
                            else at(recorded) + timedelta(seconds=30),
                        )
                    )
            return out

        return await store.execute_read(read)

    async def current(store: Neo4jStore) -> list[Any]:
        async def read(tx: Neo4jTransaction) -> list[Any]:
            rows = (
                await tx.run(
                    "MATCH (n)-[:CURRENT_REVISION]->(r) RETURN n.file_id AS f, n.symbol_id AS s, "
                    "coalesce(r.file_revision_id, r.symbol_revision_id) AS r ORDER BY r",
                    parameters={},
                )
            ).records
            return [(x["f"], x["s"], x["r"]) for x in rows]

        return await store.execute_read(read)

    async def body(store: Neo4jStore) -> None:
        results: dict[str, tuple[str, list[Any]]] = {}
        for name, ordered in orders(events).items():
            await wipe(store)
            await deliver(store, ordered)
            state = await graph_state(store)
            items = await probes(store)
            results[name] = (digest(state), [items, await answers(store, items)])
        reference = results["occurred"]
        assert any(answer is not None for answer in reference[1][1])  # the grid is not vacuous
        assert {name: value for name, value in results.items() if value != reference} == {}

        # the stored current pointers agree with the as-of answer "now"
        pointers = await current(store)
        assert pointers  # the unchanged and the changed revisions are current
        far = T0 + timedelta(days=1)

        async def check(tx: Neo4jTransaction) -> None:
            for file_id, symbol_id, revision in pointers:
                kind: EntityKind = "file" if file_id else "symbol"
                found = await entity_as_of(tx, REPO, kind, file_id or symbol_id, valid_at=far)
                assert found == revision

        await store.execute_read(check)

    with_graph(body)


def test_as_of_separates_valid_time_from_transaction_time(tmp_path: Path) -> None:
    line, _ = build_timeline(tmp_path)
    a_id = str(line.known["pkg/a.py"])
    events = line.all_events()

    async def body(store: Neo4jStore) -> None:
        await deliver(store, events)

        async def read(tx: Neo4jTransaction) -> list[str | None]:
            async def ask(valid: int, recorded: int | None) -> str | None:
                return await entity_as_of(
                    tx,
                    REPO,
                    "file",
                    a_id,
                    valid_at=at(valid) + timedelta(seconds=30),
                    recorded_at=None if recorded is None else at(recorded) + timedelta(seconds=30),
                )

            return [
                await ask(0, None),  # before the first index
                await ask(2, None),  # revision 1
                await ask(4, 7),  # the t4 index was observed only at t8: still revision 1
                await ask(4, 9),  # known by t9: revision 2
                await ask(6, 7),  # deleted at t6, known at t6
                await ask(6, None),
            ]

        before, first, late, known, deleted_early, deleted = await store.execute_read(read)
        assert before is None
        assert first is not None and late == first and known not in (None, first)
        assert deleted_early is None and deleted is None

    with_graph(body)
