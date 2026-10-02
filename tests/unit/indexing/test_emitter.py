from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import logging
import os
import shutil
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from agent_context_sdk import EventDraftV1, WorkspaceSnapshotCapturedV1
from agent_context_sdk.identity import (
    SNAPSHOT_CODE_EXTENSIONS,
    gitlink_snapshot_digest,
    workspace_snapshot_id,
)

from agent_context_platform.indexing import emitter, identity, registry
from agent_context_platform.indexing.emitter import (
    INDEXER_PRODUCER_ID,
    IndexingConfig,
    IndexingError,
    IndexingOutcome,
    IndexingService,
    _byte_offset,
    blob_oid,
    event_id_for,
    parse_structural,
    posixpath_join,
    read_sources,
)
from agent_context_platform.indexing.identity import encode_components, repository_namespace
from agent_context_platform.indexing.lineage import derive_lineage
from agent_context_platform.indexing.scanner import (
    GitlinkChange,
    Rejection,
    RepositoryScan,
    ScanLimits,
    scan_repository,
)
from agent_context_platform.indexing.scip import (
    ImportDiagnostic,
    PositionEncoding,
    SemanticIndex,
    SourceRange,
    import_scip,
)
from agent_context_platform.indexing.tree_sitter import python as python_adapter
from agent_context_platform.indexing.tree_sitter.base import (
    ParsedDiagnostic,
    ParsedFile,
    ParsedModule,
    ParsedReference,
    ParseRequest,
    StructuralError,
    StructuralErrorCode,
)

from .conftest import RepoBuilder

pytestmark = pytest.mark.unit

REPO_ID = "repo-037"
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "scip"
SCIP_SOURCES = FIXTURES / "scip-python-basic-src" / "pkg"
CIRCLE = "scip-python python fixturepkg 0.0.1 `pkg.shapes`/Circle#"
NOW = datetime(2026, 3, 1, tzinfo=UTC)


class InProcessPython:
    """The real Python parser without the sandbox: same protocol, no child process."""

    language = "python"

    def parse(self, request: ParseRequest) -> ParsedModule:
        return python_adapter.parse_python(request)


def service(**overrides: object) -> IndexingService:
    config = IndexingConfig(
        repository_id=REPO_ID,
        checkout_id="checkout-1",
        clock=lambda: NOW,
        monotonic=lambda: 0.0,
        **overrides,  # type: ignore[arg-type]
    )
    return IndexingService(config)


def parse(scan: RepositoryScan) -> list[ParsedFile]:
    result = parse_structural({"python": InProcessPython()}, read_sources(scan))
    return list(result.files)


def lineage(scan: RepositoryScan, introducing: str = "") -> dict[str, uuid.UUID]:
    """Lineage derived from the repository's first-parent history (never seeded from HEAD)."""
    return dict(derive_lineage(scan, REPO_ID).file_logical_ids)


def seed(repo: object, extra: dict[str, str] | None = None) -> None:
    """Write the SCIP fixture package (byte for byte, so positions line up) plus ``extra``."""
    write: Callable[[str, bytes | str], Path] = repo.write  # type: ignore[attr-defined]
    for source in sorted(SCIP_SOURCES.iterdir()):
        content = source.read_bytes()
        if source.name == "shapes.py":
            # The committed index was made before the fixture source gained a second blank line
            # after the import; restore the indexed layout so the SCIP positions line up.
            content = content.replace(b"import math\n\n\nclass", b"import math\n\nclass")
        write(f"pkg/{source.name}", content)
    for path, text in (extra or {}).items():
        write(path, text)


def semantic(scan: RepositoryScan, ids: dict[str, uuid.UUID]) -> SemanticIndex:
    assert scan.workspace.head_commit is not None
    return import_scip(
        (FIXTURES / "scip-python-basic.scip").read_bytes(),
        REPO_ID,
        scan.workspace.head_commit,
        file_logical_ids=ids,
    )


def by_type(drafts: Sequence[EventDraftV1], event_type: str) -> list[EventDraftV1]:
    return [draft for draft in drafts if draft.event_type == event_type]


def scan_of(repo: object) -> RepositoryScan:
    return scan_repository(repo.root)  # type: ignore[attr-defined]


HELPER = "from .shapes import describe\n\n\ndef helper() -> None:\n    describe(None)\n"


def _events(
    repo: RepoBuilder, *, with_scip: bool = True
) -> tuple[list[EventDraftV1], list[ParsedFile]]:
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    parsed = parse(scan)
    sem = semantic(scan, ids) if with_scip else None
    return service().index(scan, sem, parsed, file_logical_ids=ids), parsed


@pytest.fixture
def indexed(make_repo: Callable[[str], RepoBuilder]) -> RepoBuilder:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    return repo


def _payloads(drafts: list[EventDraftV1], event_type: str) -> list[dict[str, object]]:
    return [dict(d.payload) for d in by_type(drafts, event_type)]


def test_ids_and_keys_are_stable_across_runs(indexed: RepoBuilder) -> None:
    first, _ = _events(indexed)
    second, _ = _events(indexed)

    assert [d.event_id for d in first] == [d.event_id for d in second]
    assert [d.idempotency_key for d in first] == [d.idempotency_key for d in second]
    assert len({d.idempotency_key for d in first}) == len(first)
    assert {d.producer.producer_id for d in first} == {INDEXER_PRODUCER_ID}
    assert first[0].event_type == "code.index.started"
    assert first[-1].event_type == "code.index.completed"
    assert all(d.event_id.version == 7 for d in first)


def test_scip_names_the_identity_and_treesitter_the_revision(indexed: RepoBuilder) -> None:
    drafts, parsed = _events(indexed)
    scan_symbols = {p.path: p for p in parsed}["pkg/shapes.py"].symbols
    circle = next(s for s in scan_symbols if s.qualified_name.endswith("Circle"))
    event = next(p for p in _payloads(drafts, "code.symbol.indexed") if p["scip_symbol"] == CIRCLE)

    file_id = uuid.UUID(str(event["file_id"]))
    expected_id = identity.symbol_logical_id(
        REPO_ID,
        scip_symbol=CIRCLE,
        language="python",
        file_logical_id=file_id,
        qualified_name=circle.qualified_name,
        kind=circle.kind,
        disambiguator=circle.disambiguator,
    )
    assert event["symbol_id"] == str(expected_id)
    assert event["symbol_revision_id"] == str(
        identity.symbol_revision_id(
            REPO_ID, expected_id, circle.signature_digest, circle.semantic_fingerprint
        )
    )
    assert event["semantic_fingerprint_sha256"] == circle.semantic_fingerprint


def test_without_scip_the_qualified_name_is_the_identity(indexed: RepoBuilder) -> None:
    drafts, _ = _events(indexed, with_scip=False)

    assert all(p["scip_symbol"] is None for p in _payloads(drafts, "code.symbol.indexed"))


def test_relative_import_resolves_at_syntactic_confidence(
    indexed: RepoBuilder,
) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    parsed = parse(scan)
    reference = ParsedReference(
        source=None,
        kind="import",
        target_name="describe",
        relative_level=1,
        qualifier="shapes",
        start_byte=0,
        end_byte=4,
        qualifier_start_byte=5,
        qualifier_end_byte=9,
        evidence_kind="tree_sitter",
        confidence="syntactic",
    )
    parsed = [
        p.model_copy(update={"references": (reference,)}) if p.path == "pkg/util.py" else p
        for p in parsed
    ]

    drafts = service().index(scan, semantic(scan, ids), parsed, file_logical_ids=ids)

    imports = [
        p for p in _payloads(drafts, "code.relation.asserted") if p["predicate"] == "IMPORTS"
    ]
    assert imports
    assert {p["evidence_kind"] for p in imports} == {"tree_sitter"}
    assert all(p["confidence"] == 0.9 and p["deterministic"] is False for p in imports)
    # ``math`` (stdlib, a real import in the fixture) is external; the resolved relative import
    # of ``shapes`` is not.
    external = {p["dependency_id"] for p in _payloads(drafts, "code.dependency.asserted")}
    assert (
        str(
            uuid.uuid5(
                repository_namespace(REPO_ID), encode_components("external_module", "shapes")
            )
        )
        not in external
    )


def test_unresolved_imports_become_dependencies_and_calls_never_claim_scip(
    indexed: RepoBuilder,
) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    parsed = parse(scan)

    def ref(kind: str, name: str, **extra: object) -> ParsedReference:
        return ParsedReference(
            source=None,
            kind=kind,  # type: ignore[arg-type]
            target_name=name,
            start_byte=0,
            end_byte=3,
            evidence_kind="tree_sitter",
            confidence="heuristic" if kind == "call" else "syntactic",
            **extra,  # type: ignore[arg-type]
        )

    refs = (
        ref("import", "requests"),
        ref("import", "os.path"),
        ref("import", "pkg.shapes"),
        ref("call", "describe"),
        ref("call", "nowhere"),
        ref("inherit", "Base"),
    )
    parsed = [
        p.model_copy(update={"references": refs}) if p.path == "pkg/util.py" else p for p in parsed
    ]

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)

    dependencies = _payloads(drafts, "code.dependency.asserted")
    assert len(dependencies) == 3  # requests, os and the fixture's own real ``import math``
    assert {d["dependency_kind"] for d in dependencies} == {"observed"}
    relations = _payloads(drafts, "code.relation.asserted")
    assert not [r for r in relations if r["evidence_kind"] == "scip"]
    assert all(r["confidence"] < 1 for r in relations if r["predicate"] in {"CALLS", "IMPORTS"})


def test_scip_and_treesitter_claims_about_one_edge_are_both_kept(indexed: RepoBuilder) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    parsed = parse(scan)
    main = next(p for p in parsed if p.path == "pkg/main.py")
    run = next(s for s in main.symbols if s.qualified_name.endswith("run"))
    common = {"evidence_kind": "tree_sitter", "start_byte": 0, "end_byte": 3}
    references = (
        ParsedReference(
            kind="import",
            target_name="describe",
            qualifier="pkg.shapes",
            qualifier_start_byte=4,
            qualifier_end_byte=8,
            confidence="syntactic",
            **common,  # type: ignore[arg-type]
        ),
        ParsedReference(
            source=run.ref,
            kind="inherit",
            target_name="describe",
            confidence="syntactic",
            **common,  # type: ignore[arg-type]
        ),
    )
    parsed = [
        p.model_copy(update={"references": references}) if p.path == "pkg/main.py" else p
        for p in parsed
    ]

    drafts = service().index(scan, semantic(scan, ids), parsed, file_logical_ids=ids)

    relations = _payloads(drafts, "code.relation.asserted")
    edges: dict[tuple[object, ...], dict[str, object]] = {}
    for r in relations:
        key = (r["subject_id"], r["predicate"], r["object_id"])
        edges.setdefault(key, {})[str(r["evidence_kind"])] = r["confidence"]
    contested = [claims for claims in edges.values() if {"scip", "tree_sitter"} <= claims.keys()]
    assert contested
    assert contested[0]["scip"] == 1.0 and contested[0]["tree_sitter"] < 1  # type: ignore[operator]
    for r in relations:
        assert (r["confidence"] == 1.0) == (r["evidence_kind"] in {"scip", "git"})


def test_scip_only_file_uses_scip_digests(indexed: RepoBuilder) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    sem = semantic(scan, ids)
    parsed = [p for p in parse(scan) if p.path != "pkg/shapes.py"]

    drafts = service().index(scan, sem, parsed, file_logical_ids=ids)

    document = next(d for d in sem.documents if d.path == "pkg/shapes.py")
    expected = {str(s.revision_id) for s in document.symbols}
    events = [
        p for p in _payloads(drafts, "code.symbol.indexed") if p["symbol_revision_id"] in expected
    ]
    assert events
    assert {p["extractor_name"] for p in events} == {"scip-python"}


def test_dirty_workspace_targets_a_snapshot_and_needs_a_lineage(
    indexed: RepoBuilder,
) -> None:
    indexed.write("pkg/util.py", HELPER + "\n\ndef more() -> None:\n    pass\n")
    indexed.write("pkg/new.py", "def fresh() -> None:\n    pass\n")
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    ids.pop("pkg/main.py")

    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)

    started = _payloads(drafts, "code.index.started")[0]
    assert started["commit_id"] is None and str(started["snapshot_id"]).startswith("snap_")
    paths = {p["path"] for p in _payloads(drafts, "code.file.indexed")}
    assert {"pkg/new.py", "pkg/util.py"} <= paths and "pkg/main.py" not in paths


def test_a_mismatched_semantic_target_is_refused(indexed: RepoBuilder) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    sem = semantic(scan, ids)
    other = service().config.__class__(repository_id="other")

    with pytest.raises(IndexingError) as error:
        type(service())(other).index(scan, sem, [], file_logical_ids=ids)
    assert error.value.code == "semantic_target_mismatch"


def test_supersession_is_only_emitted_at_full_confidence(indexed: RepoBuilder) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    old, new = uuid.uuid4(), uuid.uuid4()
    sure = identity.Supersession(old, new, 1.0, "git", "provisional_committed")
    weak = identity.Supersession(old, uuid.uuid4(), 0.5, "git", "path_reuse")

    drafts = service().index(
        scan, None, parse(scan), file_logical_ids=ids, supersessions=[sure, weak]
    )

    supersedes = [
        p
        for p in _payloads(drafts, "code.relation.asserted")
        if p["predicate"] == "POSSIBLY_SUPERSEDES"
    ]
    assert [(p["subject_id"], p["object_id"]) for p in supersedes] == [(str(new), str(old))]


def test_helpers() -> None:
    assert blob_oid(b"", "sha1") == "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
    assert len(blob_oid(b"x", "sha256")) == 64
    assert event_id_for("k") == event_id_for("k") != event_id_for("j")
    assert posixpath_join("a/b", "../c") == "a/c"
    assert posixpath_join("", "../c") is None
    assert posixpath_join(".", "./x") == "x"
    assert StructuralError(StructuralErrorCode.TIMEOUT).code is StructuralErrorCode.TIMEOUT


def test_scip_positions_honour_the_declared_encoding() -> None:
    source = (
        "a\u00e9\U0001f600b\n".encode()
    )  # a, e-acute (2 bytes), emoji (4 bytes, 2 UTF-16 units), b
    starts = [0, len(source)]

    assert _byte_offset(source, starts, 0, 3, PositionEncoding.UTF8) == 3
    assert _byte_offset(source, starts, 0, 3, PositionEncoding.UTF32) == 7
    assert _byte_offset(source, starts, 0, 4, PositionEncoding.UTF16) == 7  # emoji is two units
    assert _byte_offset(source, starts, 0, 99, PositionEncoding.UTF8) is None
    assert _byte_offset(source, starts, 0, 99, PositionEncoding.UTF32) is None
    assert _byte_offset(source, starts, 0, 99, PositionEncoding.UTF16) is None
    assert _byte_offset(source, starts, 5, 0, PositionEncoding.UTF8) is None
    assert _byte_offset(b"\xff\n", [0, 2], 0, 0, PositionEncoding.UTF16) is None


def test_a_rejected_batch_raises_a_content_free_error(
    indexed: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Session:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *exc: object) -> None:
            return None

    class Rejecting:
        async def ingest(self, batch: object) -> object:
            return SimpleNamespace(http_status=409)

    async def nothing_stored(session: object, keys: object) -> dict[object, object]:
        return {}

    monkeypatch.setattr(
        emitter.LedgerRepository, "get_by_idempotency_keys", staticmethod(nothing_stored)
    )
    drafts, _ = _events(indexed)
    wired = IndexingService(service().config, Rejecting(), Session)  # type: ignore[arg-type]

    with pytest.raises(IndexingError) as error:
        asyncio.run(wired.ingest(drafts))
    assert error.value.code == "ingest_rejected_409"
    assert IndexingOutcome((), 0, 0).existing == 0


def test_read_sources_refuses_a_directory_swapped_for_a_symlink_after_the_scan(
    make_repo: Callable[[str], RepoBuilder], tmp_path: Path
) -> None:
    repo = make_repo("repo")
    repo.write("pkg/a.py", "x = 1\n")
    repo.write("top.py", "y = 2\n")
    repo.commit("seed")
    scan = scan_of(repo)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "a.py").write_text("secret = 1\n")
    (repo.root / "pkg" / "a.py").unlink()
    (repo.root / "pkg").rmdir()
    (repo.root / "pkg").symlink_to(outside)

    sources = read_sources(scan)

    assert sorted(sources) == ["top.py"]  # nothing was read through the symlink


def test_read_sources_does_not_hang_on_a_fifo_leaf_and_skips_a_swapped_file(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    repo.write("fifo.py", "x = 1\n")
    repo.write("swapped.py", "y = 2\n")
    repo.write("same.py", "z = 3\n")
    repo.commit("seed")
    scan = scan_of(repo)
    (repo.root / "fifo.py").unlink()
    os.mkfifo(repo.root / "fifo.py")
    (repo.root / "swapped.py").write_text("y = 22222\n")  # size and digest differ from the scan

    sources = read_sources(scan)

    assert sorted(sources) == ["same.py"]


def test_read_sources_refuses_a_checkout_or_ancestor_swapped_for_a_symlink_after_the_scan(
    make_repo: Callable[[str], RepoBuilder], tmp_path: Path
) -> None:
    repo = make_repo("repo")
    repo.write("a.py", "x = 1\n")
    repo.commit("seed")
    scan = scan_of(repo)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "a.py").write_text("x = 1\n")  # even identical bytes must not be read
    moved = tmp_path / "moved-root"

    # The checkout directory itself becomes a symlink to another directory.
    repo.root.rename(moved)
    repo.root.symlink_to(outside)
    assert read_sources(scan) == {}
    repo.root.unlink()
    moved.rename(repo.root)
    assert sorted(read_sources(scan)) == ["a.py"]  # the genuine root still reads

    # An ancestor becomes a symlink to a copy of the tree elsewhere.
    ancestor = tmp_path / "anc"
    shutil.copytree(repo.root, ancestor / "inner", symlinks=True)
    nested = scan_repository(ancestor / "inner")
    assert sorted(read_sources(nested)) == ["a.py"]
    elsewhere = tmp_path / "elsewhere"
    shutil.copytree(ancestor, elsewhere, symlinks=True)
    ancestor.rename(tmp_path / "displaced-anc")
    ancestor.symlink_to(elsewhere)
    assert read_sources(nested) == {}


def _scip_subjects(drafts: list[EventDraftV1]) -> dict[str, set[str]]:
    """Qualified names on each side of the SCIP relations: subjects and objects."""
    names = {
        p["symbol_id"]: str(p["qualified_name"]) for p in _payloads(drafts, "code.symbol.indexed")
    }
    scip = [r for r in _payloads(drafts, "code.relation.asserted") if r["evidence_kind"] == "scip"]
    return {
        "subjects": {names.get(str(r["subject_id"]), "?") for r in scip},
        "objects": {names.get(str(r["object_id"]), "?") for r in scip},
    }


def test_scip_evidence_binds_only_files_whose_bytes_are_the_scip_commits(
    indexed: RepoBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    clean = _scip_subjects(_events(indexed)[0])
    assert {"pkg.main", "pkg.main.run", "pkg.shapes.describe"} <= clean["subjects"]
    # An uncommitted edit: the SCIP occurrence of ``describe`` in main.py is stale.
    indexed.write(
        "pkg/main.py",
        "from pkg.shapes import Circle, describe\n\n\ndef run() -> float:\n"
        "    circle = Circle(2.0)\n    return foo(circle)\n",
    )
    scan = scan_of(indexed)
    assert scan.workspace.is_dirty
    ids = lineage(scan, scan.workspace.head_commit or "")

    with caplog.at_level(logging.INFO):
        drafts = service().index(scan, semantic(scan, ids), parse(scan), file_logical_ids=ids)

    dirty = _scip_subjects(drafts)
    assert not {"pkg.main", "pkg.main.run"} & dirty["subjects"]  # no SCIP claim about main.py
    assert "pkg.shapes.describe" in dirty["subjects"]  # a clean file keeps its SCIP evidence
    assert "scip_file_not_at_commit" in caplog.text
    completed = _payloads(drafts, "code.index.completed")[0]
    assert completed["success"] is False and completed["error_class"] == "files_degraded"
    # Tree-sitter stands alone for the edited file, and its scip-only symbol claims are gone.
    assert any(
        r["evidence_kind"] == "tree_sitter" and r["predicate"] == "CALLS"
        for r in _payloads(drafts, "code.relation.asserted")
    )


def test_a_dirty_or_untracked_file_loses_its_scip_symbols_too(indexed: RepoBuilder) -> None:
    indexed.write("pkg/shapes.py", (SCIP_SOURCES / "shapes.py").read_text() + "\nEXTRA = 1\n")
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    parsed = [p for p in parse(scan) if p.path != "pkg/shapes.py"]  # would be SCIP-only

    drafts = service().index(scan, semantic(scan, ids), parsed, file_logical_ids=ids)

    assert not {p["scip_symbol"] for p in _payloads(drafts, "code.symbol.indexed")} & {CIRCLE}
    assert "pkg/shapes.py" not in {p["path"] for p in _payloads(drafts, "code.file.indexed")}
    completed = _payloads(drafts, "code.index.completed")[0]
    assert completed["success"] is False and completed["error_class"] == "files_skipped"


def test_a_scip_occurrence_without_a_valid_location_makes_no_claim(indexed: RepoBuilder) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    sem = semantic(scan, ids)
    broken = SourceRange(400, 0, 400, 1)  # a line the file does not have
    documents = tuple(
        dataclasses.replace(
            d,
            occurrences=tuple(
                o if o.is_definition else dataclasses.replace(o, range=broken)
                for o in d.occurrences
            ),
            symbols=tuple(dataclasses.replace(s, relationships=()) for s in d.symbols),
        )
        if d.path == "pkg/main.py"
        else d
        for d in sem.documents
    )

    drafts = service().index(
        scan, dataclasses.replace(sem, documents=documents), parse(scan), file_logical_ids=ids
    )

    subjects = _scip_subjects(drafts)["subjects"]
    assert not {"pkg.main", "pkg.main.run"} & subjects  # never a 1.0 claim owned by the module
    assert "pkg.shapes.describe" in subjects
    completed = _payloads(drafts, "code.index.completed")[0]  # dropped evidence is never a success
    assert completed["success"] is False and completed["error_class"] == "files_degraded"


def test_two_checkouts_with_the_same_untracked_file_share_one_snapshot_and_membership(
    indexed: RepoBuilder,
) -> None:
    indexed.write("pkg/new.py", "def fresh() -> None:\n    pass\n")
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")

    def index(checkout: str) -> list[EventDraftV1]:
        config = dataclasses.replace(service().config, checkout_id=checkout)
        return IndexingService(config).index(scan, None, parse(scan), file_logical_ids=ids)

    first, second = index("checkout-1"), index("checkout-2")

    def membership(drafts: list[EventDraftV1]) -> tuple[str, str, list[str], set[str]]:
        started = _payloads(drafts, "code.index.started")[0]
        files = _payloads(drafts, "code.file.indexed")
        return (
            str(started["snapshot_id"]),
            str(started["index_id"]),
            [str(p["path"]) for p in files],
            {str(p["file_id"]) for p in files if p["path"] == "pkg/new.py"},
        )

    one, two = membership(first), membership(second)
    assert one == two  # the same dirty state IS the same snapshot: one index, one membership
    assert len(one[2]) == len(set(one[2])) and len(one[3]) == 1
    listed = [
        (str(p["index_id"]), p["path"]) for p in _payloads([*first, *second], "code.file.indexed")
    ]
    assert len(set(listed)) == len(listed) // 2  # each path once per index, replayed identically


def test_a_codex_snapshot_built_from_the_same_scan_joins_the_indexed_revisions(
    indexed: RepoBuilder,
) -> None:
    indexed.write("pkg/util.py", HELPER + "\n\ndef more() -> None:\n    pass\n")
    indexed.write("pkg/new.py", "def fresh() -> None:\n    pass\n")
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    tracked = {f.path: f for f in scan.files}
    modified = sorted(
        {str(tracked[p].content_sha256) for p in scan.workspace.modified_paths if p in tracked}
    )
    snapshot = WorkspaceSnapshotCapturedV1(
        repository_id=service().config.repository_id,
        checkout_id="checkout-9",  # Codex's checkout is not the indexer's
        base_commit=scan.workspace.head_commit,
        dirty_patch_sha256=scan.workspace.dirty_state_sha256 or "0" * 64,
        modified_content_sha256=tuple(modified),
        untracked_paths=tuple(sorted(scan.workspace.untracked_paths)),
    )

    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)

    files = {
        str(p["path"]): str(p["content_sha256"]) for p in _payloads(drafts, "code.file.indexed")
    }
    assert sorted(files[p] for p in scan.workspace.modified_paths if p in files) == modified
    assert sorted(p for p in files if p in scan.workspace.untracked_paths) == list(
        snapshot.untracked_paths
    )
    entries = [
        *((p, "modified", files[p]) for p in scan.workspace.modified_paths),
        *((p, "untracked", files[p]) for p in scan.workspace.untracked_paths),
    ]
    started = _payloads(drafts, "code.index.started")[0]
    assert started["snapshot_id"] == workspace_snapshot_id(
        snapshot.repository_id, snapshot.base_commit, entries
    )  # a pure function of what Codex captures: the join key


# --- round 4 ---------------------------------------------------------------------------------


def _completed(drafts: list[EventDraftV1]) -> tuple[bool, str | None]:
    completed = _payloads(drafts, "code.index.completed")[0]
    return bool(completed["success"]), completed["error_class"]


def test_a_scan_that_omitted_files_is_never_a_success(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    for name in ("a", "b", "c"):
        repo.write(f"{name}.py", f"{name} = 1\n")
    repo.commit("seed")
    scan = scan_repository(repo.root, ScanLimits(max_files=2))
    assert scan.truncated and scan.omitted_files == 1
    ids = lineage(scan, scan.workspace.head_commit or "")

    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)

    # The omitted file is in neither ``files`` nor ``untracked``: absence proves nothing.
    assert _completed(drafts) == (False, "scan_incomplete")


def test_a_scan_skip_that_cannot_hide_a_source_file_stays_a_success(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    repo.write("a.py", "a = 1\n")
    repo.write("blob.bin", b"\x00" * 100)
    repo.commit("seed")
    scan = scan_repository(repo.root, ScanLimits(max_file_bytes=10))
    assert scan.truncated and not scan.omitted_files  # too large, but it is not source
    ids = lineage(scan, scan.workspace.head_commit or "")

    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)

    assert _completed(drafts) == (True, None)


def test_a_rejected_path_hides_a_source_file_unless_its_name_says_otherwise(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    repo.write("a.py", "a = 1\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")

    def outcome(*rejections: Rejection) -> tuple[bool, str | None]:
        rejected = dataclasses.replace(scan, rejections=rejections)
        return _completed(service().index(rejected, None, parse(scan), file_logical_ids=ids))

    assert outcome() == (True, None)
    assert outcome(Rejection("unsafe_path", None)) == (False, "scan_incomplete")  # undecodable
    assert outcome(Rejection("unsafe_path", "pkg/../x.py")) == (False, "scan_incomplete")
    assert outcome(Rejection("unsafe_path", "docs/../x.md")) == (True, None)


def test_a_path_the_worktree_changed_without_a_status_is_not_at_the_commit(
    indexed: RepoBuilder,
) -> None:
    # ``pkg/main.py`` is checked in LF but lives in the worktree as CRLF (an eol filter): git
    # status calls it clean, the scanner sees different bytes and lists it as modified.
    main = (indexed.root / "pkg" / "main.py").read_bytes()
    indexed.write(".gitattributes", "pkg/main.py text eol=crlf\n")
    indexed.write("pkg/main.py", main.replace(b"\n", b"\r\n"))
    indexed.commit("eol")
    scan = scan_of(indexed)
    tracked = {f.path: f for f in scan.files}["pkg/main.py"]
    assert tracked.change is None and "pkg/main.py" in scan.workspace.modified_paths
    ids = lineage(scan, scan.workspace.head_commit or "")

    drafts = service().index(scan, semantic(scan, ids), parse(scan), file_logical_ids=ids)

    subjects = _scip_subjects(drafts)["subjects"]
    assert not {"pkg.main", "pkg.main.run"} & subjects  # decided by content, not by status
    assert "pkg.shapes.describe" in subjects
    assert _completed(drafts) == (False, "files_degraded")


def test_a_clean_tracked_path_keeps_its_scip_evidence(indexed: RepoBuilder) -> None:
    drafts = _events(indexed)[0]

    assert {"pkg.main", "pkg.main.run"} <= _scip_subjects(drafts)["subjects"]
    assert _completed(drafts) == (True, None)


def test_a_scip_import_diagnostic_makes_the_run_degraded(indexed: RepoBuilder) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    sem = dataclasses.replace(
        semantic(scan, ids),
        diagnostics=(ImportDiagnostic("unknown_position_encoding", "pkg/main.py"),),
    )

    drafts = service().index(scan, sem, parse(scan), file_logical_ids=ids)

    assert _completed(drafts) == (False, "files_degraded")


def test_a_position_at_the_end_of_a_line_never_names_the_next_line() -> None:
    for source in (b"a\nB", b"a\r\nB"):
        starts = emitter._line_starts(source)
        assert _byte_offset(source, starts, 0, 1, PositionEncoding.UTF8) == 1  # end of line: valid
        assert _byte_offset(source, starts, 0, 2, PositionEncoding.UTF8) is None  # the terminator
        assert _byte_offset(source, starts, 0, 3, PositionEncoding.UTF8) is None


def _calls(make_repo: Callable[[str], RepoBuilder], files: dict[str, str]) -> set[tuple[str, str]]:
    repo = make_repo(f"repo{len(files)}{abs(hash(tuple(files)))}")
    for path, text in files.items():
        repo.write(path, text)
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)
    names = {
        p["symbol_id"]: str(p["qualified_name"]) for p in _payloads(drafts, "code.symbol.indexed")
    }
    return {
        (names[str(r["subject_id"])], names[str(r["object_id"])])
        for r in _payloads(drafts, "code.relation.asserted")
        if r["predicate"] == "CALLS"
    }


_TWO = {"a.py": "def f():\n    return 1\n", "b.py": "def f():\n    return 2\n"}


def test_a_rebound_name_resolves_to_the_latest_binding_before_the_use(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    calls = _calls(
        make_repo,
        {**_TWO, "u.py": "from a import f\nfrom b import f\n\n\ndef run():\n    return f()\n"},
    )

    assert {c for c in calls if c[0] == "u.run"} == {("u.run", "b.f")}


def test_a_reused_module_alias_resolves_to_the_latest_binding(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    calls = _calls(
        make_repo,
        {**_TWO, "u.py": "import a as m\nimport b as m\n\n\ndef run():\n    return m.f()\n"},
    )

    assert {c for c in calls if c[0] == "u.run"} == {("u.run", "b.f")}


def test_a_function_local_import_shadows_the_module_level_one(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    source = "from a import f\n\n\ndef run():\n    from b import f\n    return f()\n\n\n"
    source += "def other():\n    return f()\n"
    calls = _calls(make_repo, {**_TWO, "u.py": source})

    assert ("u.run", "b.f") in calls and ("u.other", "a.f") in calls


def test_a_use_that_a_later_rebinding_may_reach_is_ambiguous_and_dropped(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    source = "from a import f\n\n\ndef run():\n    return f()\n\n\nfrom b import f\n"
    calls = _calls(make_repo, {**_TWO, "u.py": source})  # run() may execute after the rebinding

    assert not {c for c in calls if c[0] == "u.run"}


# --- round 5 ---------------------------------------------------------------------------------


def test_a_scip_definition_without_a_valid_location_is_counted_and_degrades(
    indexed: RepoBuilder,
) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    sem = semantic(scan, ids)
    broken = SourceRange(400, 0, 400, 1)
    documents = tuple(
        dataclasses.replace(
            d,
            occurrences=tuple(
                dataclasses.replace(o, range=broken) if o.is_definition else o
                for o in d.occurrences
            ),
        )
        if d.path == "pkg/shapes.py"
        else d
        for d in sem.documents
    )

    drafts = service().index(
        scan, dataclasses.replace(sem, documents=documents), parse(scan), file_logical_ids=ids
    )

    assert _completed(drafts) == (False, "files_degraded")


def _dirty(repo: RepoBuilder) -> dict[str, tuple[str, str | None]]:
    entries, complete = emitter._dirty_entries(scan_of(repo))
    assert complete
    return {path: (state, digest) for path, state, digest in entries}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_a_staged_deletion_and_a_rename_are_distinct_net_states(indexed: RepoBuilder) -> None:
    indexed.git("rm", "-q", "pkg/util.py")
    indexed.git("mv", "pkg/shapes.py", "pkg/forms.py")

    state = _dirty(indexed)

    assert state["pkg/util.py"] == ("deleted", None)
    assert state["pkg/shapes.py"] == ("deleted", None)  # the rename's source, from the status
    forms = (indexed.root / "pkg" / "forms.py").read_bytes()
    assert state["pkg/forms.py"] == ("added", _sha(forms))


def test_an_unstaged_deletion_and_a_deleted_then_recreated_file(indexed: RepoBuilder) -> None:
    (indexed.root / "pkg" / "util.py").unlink()
    indexed.git("rm", "-q", "--cached", "pkg/shapes.py")  # deleted in the index, still present

    state = _dirty(indexed)

    assert state["pkg/util.py"] == ("deleted", None)
    shapes = (indexed.root / "pkg" / "shapes.py").read_bytes()
    assert state["pkg/shapes.py"] == ("modified", _sha(shapes))  # net: present, not untracked


def test_a_gitlink_change_is_its_own_state_with_its_oids(indexed: RepoBuilder) -> None:
    scan = scan_of(indexed)
    link = GitlinkChange("vendor/sub", "1" * 40, "2" * 40)
    workspace = dataclasses.replace(
        scan.workspace,
        modified_paths=("vendor/sub",),
        gitlinks=(link,),
        untracked_paths=(),
    )

    entries, complete = emitter._dirty_entries(dataclasses.replace(scan, workspace=workspace))

    assert complete
    assert entries == [("vendor/sub", "gitlink", gitlink_snapshot_digest("1" * 40, "2" * 40))]
    assert gitlink_snapshot_digest("1" * 40, "2" * 40) != gitlink_snapshot_digest(
        "1" * 40, "3" * 40
    )


def test_a_large_dirty_file_is_hashed_by_streaming_beyond_the_parse_cap(
    indexed: RepoBuilder,
) -> None:
    big = b"x = 1\n" * (400 * 1024)  # 2.8 MB: skipped by the scan, still hashed for identity
    indexed.write("pkg/big.py", big)

    state = _dirty(indexed)

    assert state["pkg/big.py"] == ("untracked", _sha(big))


def test_a_dirty_file_that_cannot_be_hashed_leaves_the_snapshot_incomplete(
    indexed: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    indexed.write("pkg/big.py", b"x = 1\n" * (400 * 1024))
    monkeypatch.setattr(emitter, "IDENTITY_HASH_CAP", 1024)
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")

    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)

    assert emitter._dirty_entries(scan)[1] is False
    assert _completed(drafts) == (False, "snapshot_incomplete")
    assert {p["path"] for p in _payloads(drafts, "code.file.indexed")}  # file results still emitted
    started = _payloads(drafts, "code.index.started")[0]
    assert started["snapshot_id"] != workspace_snapshot_id(  # never the joinable identity
        service().config.repository_id, scan.workspace.head_commit, []
    )


def _imports(make_repo: Callable[[str], RepoBuilder], files: dict[str, str]) -> list[str]:
    repo = make_repo(f"imports{abs(hash(tuple(files)))}")
    for path, text in files.items():
        repo.write(path, text)
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)
    names = {
        str(p["symbol_id"]): str(p["qualified_name"])
        for p in _payloads(drafts, "code.symbol.indexed")
    }
    return sorted(
        names.get(str(r["object_id"]), "<file>")
        for r in _payloads(drafts, "code.relation.asserted")
        if r["predicate"] == "IMPORTS" and names.get(str(r["subject_id"]), "").startswith("u")
    )


_MOD = "class A:\n    def run(self):\n        pass\n\n\ndef run():\n    pass\n"


def test_an_imported_name_is_the_module_level_symbol_never_a_method(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    got = _imports(make_repo, {"mod.py": _MOD, "u.py": "from mod import run\n"})

    assert got == ["mod.run"]  # not mod.A.run, which shares the last name


def test_an_imported_name_with_no_module_level_match_asserts_no_relation(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    only_a_method = "class A:\n    def run(self):\n        pass\n"

    got = _imports(make_repo, {"mod.py": only_a_method, "u.py": "from mod import run\n"})

    assert got == []


@pytest.mark.parametrize(
    ("code", "degrades"),
    [
        ("symbols_dropped", True),
        ("references_capped", True),
        ("file_degraded", True),
        ("work_budget_exceeded", True),
        ("syntax_recovered", False),
    ],
)
def test_every_adapter_loss_diagnostic_degrades_the_run(
    indexed: RepoBuilder, code: str, degrades: bool
) -> None:
    scan = scan_of(indexed)
    ids = lineage(scan, scan.workspace.head_commit or "")
    parsed = [
        p.model_copy(update={"diagnostics": (ParsedDiagnostic(code=code, count=1),)})
        if p.path == "pkg/util.py"
        else p
        for p in parse(scan)
    ]

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)

    assert _completed(drafts) == ((False, "files_degraded") if degrades else (True, None))


# --- PLATFORM-037C: the SDK snapshot identity and its digest policy ---------------------------


def _started(drafts: list[EventDraftV1]) -> dict[str, object]:
    return dict(_payloads(drafts, "code.index.started")[0])


def _index(repo: RepoBuilder) -> tuple[RepositoryScan, list[EventDraftV1]]:
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    return scan, service().index(scan, None, parse(scan), file_logical_ids=ids)


def _snapshot_id(repo: RepoBuilder) -> str:
    return str(_started(_index(repo)[1])["snapshot_id"])


def test_the_emitted_snapshot_id_is_the_sdk_id_over_the_expected_entries(
    indexed: RepoBuilder,
) -> None:
    indexed.write("pkg/util.py", HELPER + "\n\ndef more() -> None:\n    pass\n")
    indexed.write("pkg/new.py", "def fresh() -> None:\n    pass\n")
    indexed.write(".env", "TOKEN=1\n")
    indexed.git("rm", "-q", "pkg/shapes.py")
    scan, drafts = _index(indexed)

    def raw(path: str) -> str:
        return _sha((indexed.root / path).read_bytes())

    expected = workspace_snapshot_id(
        service().config.repository_id,
        scan.workspace.head_commit,
        [
            (".env", "untracked", None),
            ("pkg/new.py", "untracked", raw("pkg/new.py")),
            ("pkg/shapes.py", "deleted", None),
            ("pkg/util.py", "modified", raw("pkg/util.py")),
        ],
    )

    assert _started(drafts)["snapshot_id"] == expected
    assert _completed(drafts)[1] != "snapshot_incomplete"


def test_a_non_code_dirty_file_contributes_its_path_and_state_but_never_its_content(
    indexed: RepoBuilder,
) -> None:
    indexed.write(".env", "TOKEN=one\n")
    first = _snapshot_id(indexed)
    indexed.write(".env", "TOKEN=two-and-longer\n")
    assert _snapshot_id(indexed) == first  # a content change is invisible

    indexed.write("config.yaml", "a: 1\n")
    assert _snapshot_id(indexed) != first  # adding one is a change
    (indexed.root / "config.yaml").unlink()
    assert _snapshot_id(indexed) == first
    (indexed.root / ".env").rename(indexed.root / ".env.local")
    assert _snapshot_id(indexed) != first  # a rename is a change
    (indexed.root / ".env.local").unlink()
    assert _snapshot_id(indexed) != first  # so is removing the last dirty path

    indexed.write("README.md", "changed\n")  # a tracked non-code file, modified
    tracked = _snapshot_id(indexed)
    indexed.write("README.md", "changed again\n")
    assert _snapshot_id(indexed) == tracked


def test_a_dirty_code_file_contributes_its_content_in_any_letter_case(
    indexed: RepoBuilder,
) -> None:
    indexed.write("pkg/new.py", "a = 1\n")
    indexed.write("pkg/Loud.PY", "a = 1\n")
    first = _snapshot_id(indexed)
    indexed.write("pkg/new.py", "a = 2\n")
    second = _snapshot_id(indexed)
    assert second != first
    indexed.write("pkg/Loud.PY", "a = 2\n")  # not indexed (the registry is case-sensitive) ...
    assert _snapshot_id(indexed) not in (first, second)  # ... but still hashed for the id


def test_a_non_code_file_that_is_unreadable_or_oversized_keeps_the_snapshot_complete(
    indexed: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    indexed.write("secret.env", "TOKEN=1\n")
    indexed.write("blob.bin", b"\0" * (2 * 1024 * 1024))  # past the scan's parse cap: no digest
    monkeypatch.setattr(emitter, "IDENTITY_HASH_CAP", 1024)
    opened: list[str] = []
    real = emitter.hash_worktree_file
    monkeypatch.setattr(
        emitter,
        "hash_worktree_file",
        lambda scan, path, cap: opened.append(path) or real(scan, path, cap),
    )
    (indexed.root / "secret.env").chmod(0)
    try:
        scan, drafts = _index(indexed)
        assert emitter._dirty_entries(scan)[1] is True
    finally:
        (indexed.root / "secret.env").chmod(0o644)

    assert _completed(drafts)[1] != "snapshot_incomplete"
    assert opened == []  # never opened for the snapshot


def test_an_unreadable_code_file_makes_the_snapshot_incomplete(indexed: RepoBuilder) -> None:
    indexed.write("pkg/locked.py", "a = 1\n")
    (indexed.root / "pkg" / "locked.py").chmod(0)
    try:
        scan, drafts = _index(indexed)
        assert emitter._dirty_entries(scan)[1] is False
    finally:
        (indexed.root / "pkg" / "locked.py").chmod(0o644)

    assert _completed(drafts) == (False, "snapshot_incomplete")


def test_the_incomplete_fallback_id_ignores_non_code_content(
    indexed: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(emitter, "IDENTITY_HASH_CAP", 1024)
    indexed.write("pkg/big.py", b"x = 1\n" * (400 * 1024))  # over the cap: cannot be hashed
    indexed.write(".env", "TOKEN=one\n")
    _, drafts = _index(indexed)
    assert _completed(drafts) == (False, "snapshot_incomplete")
    first = _started(drafts)["snapshot_id"]
    indexed.write(".env", "TOKEN=two\n")
    again = _index(indexed)[1]
    assert _completed(again) == (False, "snapshot_incomplete")
    assert _started(again)["snapshot_id"] == first  # non-code content never reaches the id
    indexed.write("other.txt", "x")
    assert _started(_index(indexed)[1])["snapshot_id"] != first


def test_every_indexed_extension_is_hashed_into_the_snapshot_id() -> None:
    registered = {ext for support in registry.LANGUAGES.values() for ext in support.extensions}
    assert registered <= SNAPSHOT_CODE_EXTENSIONS


def test_an_unchanged_dirty_reindex_emits_no_new_event_keys(indexed: RepoBuilder) -> None:
    indexed.write("pkg/new.py", "def fresh() -> None:\n    pass\n")
    indexed.write(".env", "TOKEN=1\n")
    _, first = _index(indexed)
    _, second = _index(indexed)
    assert {d.idempotency_key for d in second} == {d.idempotency_key for d in first}
