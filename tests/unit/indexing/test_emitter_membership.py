"""Assertion membership and per-file coverage (PLATFORM-037B, FU-51)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from agent_context_sdk import EventDraftV1

from agent_context_platform.indexing.emitter import claim_key, parse_structural, read_sources
from agent_context_platform.indexing.tree_sitter.base import ParsedDiagnostic, ParsedFile

from .conftest import RepoBuilder
from .test_emitter import (
    HELPER,
    _events,
    _payloads,
    by_type,
    lineage,
    parse,
    scan_of,
    seed,
    service,
)
from .test_emitter_resolution import TableAdapter, _reference, languages  # noqa: F401

pytestmark = pytest.mark.unit

OBSERVED = "code.assertion.observed"
COVERAGE = "code.file.coverage_reported"


def _coverage(drafts: list[EventDraftV1]) -> dict[str, dict[str, Any]]:
    """Coverage payload per file revision."""
    return {str(p["file_revision_id"]): p for p in _payloads(drafts, COVERAGE)}


def _revisions(drafts: list[EventDraftV1]) -> dict[str, str]:
    return {
        str(p["path"]): str(p["file_revision_id"]) for p in _payloads(drafts, "code.file.indexed")
    }


def test_every_assertion_has_one_observation_per_source_file_revision(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    drafts, _ = _events(repo)

    observed = _payloads(drafts, OBSERVED)
    families = {"relation": "code.relation.asserted", "dependency": "code.dependency.asserted"}
    for family, event_type in families.items():
        asserted = {str(p["assertion_id"]) for p in _payloads(drafts, event_type)}
        seen = {str(p["assertion_id"]) for p in observed if p["assertion_family"] == family}
        assert asserted and seen == asserted  # SCIP, syntactic and heuristic evidence alike
    supersedes = {
        str(p["assertion_id"])
        for p in _payloads(drafts, "code.relation.asserted")
        if p["predicate"] == "POSSIBLY_SUPERSEDES"
    }
    assert not supersedes & {str(p["assertion_id"]) for p in observed}  # not file-bound
    members = {
        (p["file_id"], p["file_revision_id"]) for p in _payloads(drafts, "code.file.indexed")
    }
    assert {(p["file_id"], p["file_revision_id"]) for p in observed} <= members
    assert len({d.idempotency_key for d in by_type(drafts, OBSERVED)}) == len(observed)


def test_an_edited_file_observes_only_the_assertions_it_still_produces(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    repo.write("pkg/a.py", "a\n")
    repo.write("pkg/b.py", "b\n")
    repo.commit("seed")
    first_scan = scan_of(repo)
    ids = lineage(first_scan)
    members = {"pkg/a.py": ("caller", "keeper"), "pkg/b.py": ("callee",)}
    calls = {
        "pkg/a.py": (
            _reference("call", "callee", qualifier="b"),
            _reference("import", "b", level=1),
        )
    }

    def index(scan: Any, table: dict) -> list[EventDraftV1]:  # type: ignore[type-arg]
        adapter = TableAdapter("python", table, members)
        parsed = list(parse_structural({"python": adapter}, read_sources(scan)).files)  # type: ignore[dict-item]
        return service().index(scan, None, parsed, file_logical_ids=ids)

    def observed_by_revision(drafts: list[EventDraftV1], revision: str) -> set[str]:
        return {
            str(p["assertion_id"])
            for p in _payloads(drafts, OBSERVED)
            if p["file_revision_id"] == revision
        }

    def call_assertions(drafts: list[EventDraftV1]) -> set[str]:
        return {
            str(p["assertion_id"])
            for p in _payloads(drafts, "code.relation.asserted")
            if p["predicate"] == "CALLS"
        }

    before = index(first_scan, calls)
    rev1 = _revisions(before)["pkg/a.py"]
    call = call_assertions(before)
    assert call and call <= observed_by_revision(before, rev1)

    repo.write("pkg/a.py", "a rewritten\n")  # the call is gone; the other members remain
    repo.commit("drop the call")
    after = index(scan_of(repo), {})
    rev2 = _revisions(after)["pkg/a.py"]

    assert rev2 != rev1
    assert not call_assertions(after)
    assert not call & observed_by_revision(after, rev2)  # no observation of the removed call
    assert observed_by_revision(after, rev2)  # ...but the DEFINES of rev 2 are observed
    # The unchanged file's observations are the SAME claims as before: nothing new to store.
    keys = lambda drafts: {d.idempotency_key for d in by_type(drafts, OBSERVED)}  # noqa: E731
    new = keys(after) - keys(before)
    assert new and {
        p["file_revision_id"] for p in _payloads(after, OBSERVED) if _key(p, after) in new
    } == {rev2}


def _key(payload: dict[str, Any], drafts: list[EventDraftV1]) -> str:
    return next(
        d.idempotency_key
        for d in by_type(drafts, OBSERVED)
        if d.payload["assertion_id"] == payload["assertion_id"]
    )


def test_an_unchanged_file_revision_at_a_new_commit_emits_no_new_observation(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    first_scan = scan_of(repo)
    ids = lineage(first_scan)  # introduced at the first
    first = service().index(first_scan, None, parse(first_scan), file_logical_ids=ids)
    repo.write("README.md", "docs\n")
    repo.commit("touch a non-source file")
    second_scan = scan_of(repo)
    second = service().index(second_scan, None, parse(second_scan), file_logical_ids=ids)

    assert first[0].payload["commit_id"] != second[0].payload["commit_id"]
    for kind in (OBSERVED, "code.symbol.indexed", "code.relation.asserted"):
        assert {d.idempotency_key for d in by_type(second, kind)} == {
            d.idempotency_key for d in by_type(first, kind)
        }
    # Coverage and membership are per target: the new commit lists its files again.
    assert not {d.idempotency_key for d in by_type(second, COVERAGE)} & {
        d.idempotency_key for d in by_type(first, COVERAGE)
    }


def test_a_clean_file_is_complete_and_a_lossy_one_carries_its_losses(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    lossy = {
        "pkg/util.py": (("work_budget_exceeded", "references_capped", "symbols_dropped"), None),
        "pkg/main.py": (("file_degraded", "work_budget_exceeded"), None),
        "pkg/shapes.py": (("syntax_recovered",), None),  # recovery is not a loss
    }
    parsed: list[ParsedFile] = [
        p.model_copy(
            update={
                "diagnostics": tuple(ParsedDiagnostic(code=c, count=1) for c in lossy[p.path][0])
            }
        )
        if p.path in lossy
        else p
        for p in parse(scan)
    ]

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)
    revisions = _revisions(drafts)
    coverage = _coverage(drafts)

    assert set(coverage) == set(revisions.values())  # one per file of the membership
    assert coverage[revisions["pkg/util.py"]]["losses"] == (
        "references_capped",
        "symbols_dropped",
        "work_budget_exceeded",
    )
    assert coverage[revisions["pkg/main.py"]]["losses"] == (
        "file_degraded",
        "work_budget_exceeded",
    )
    clean = ("pkg/shapes.py", "pkg/__init__.py")
    assert set(clean) <= set(revisions)  # a clean file must be reported, never skipped
    for path in clean:
        assert coverage[revisions[path]]["complete"] is True
        assert coverage[revisions[path]]["losses"] == ()
    assert all(p["complete"] == (not p["losses"]) for p in coverage.values())
    completed = _payloads(drafts, "code.index.completed")[0]  # run-level semantics are unchanged
    assert completed["success"] is False and completed["error_class"] == "files_degraded"


def test_scip_evidence_dropped_for_a_file_not_at_the_commit_is_a_loss(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    from .test_emitter import semantic

    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    repo.write("pkg/main.py", "def run() -> None:\n    return None\n")  # uncommitted edit
    scan = scan_of(repo)
    ids = lineage(scan)

    drafts = service().index(scan, semantic(scan, ids), parse(scan), file_logical_ids=ids)
    revisions = _revisions(drafts)
    coverage = _coverage(drafts)

    assert coverage[revisions["pkg/main.py"]]["losses"] == ("scip_evidence_dropped",)
    assert coverage[revisions["pkg/util.py"]]["complete"] is True


def test_a_file_without_lineage_is_not_reported_per_file(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    ids.pop("pkg/util.py")

    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)

    assert "pkg/util.py" not in _revisions(drafts)
    assert len(_payloads(drafts, COVERAGE)) == len(_payloads(drafts, "code.file.indexed"))
    completed = _payloads(drafts, "code.index.completed")[0]
    assert completed["error_class"] == "files_skipped"  # still counted at run level


def test_an_unchanged_reindex_emits_the_same_claims_of_every_kind(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    first, _ = _events(repo)
    second, _ = _events(repo)

    assert {d.event_type for d in first} >= {OBSERVED, COVERAGE}
    assert [claim_key(d) for d in second] == [d.idempotency_key for d in first]


def test_a_clean_tracked_file_too_large_to_index_is_reported_not_indexed(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.write("pkg/huge.py", "x = 1\n" * 200_000)  # over the 1 MiB scan cap: no bytes, no digest
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)

    drafts = service().index(scan, None, parse(scan), file_logical_ids=ids)
    coverage = _payloads(drafts, COVERAGE)
    indexed = {str(p["file_revision_id"]) for p in _payloads(drafts, "code.file.indexed")}

    lost = [p for p in coverage if p["losses"] == ("not_indexed",)]
    assert len(lost) == 1 and lost[0]["complete"] is False
    assert lost[0]["file_id"] == str(ids["pkg/huge.py"])
    assert lost[0]["file_revision_id"] not in indexed  # coverage only: no code.file.indexed
    completed = _payloads(drafts, "code.index.completed")[0]
    assert completed["file_count"] == len(indexed)  # counts and success are unchanged
    assert completed["error_class"] == "files_skipped"


def test_go_files_get_membership_and_coverage_with_their_own_losses(
    languages: None,  # noqa: F811
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    repo.write("go/main.go", "package main\n")
    repo.write("go/util/util.go", "package util\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    imports = {"go/main.go": (_reference("import", "example.invalid/mod/go/util"),)}
    adapters = {"go": TableAdapter("go", imports)}
    parsed = list(parse_structural(adapters, read_sources(scan)).files)  # type: ignore[arg-type]
    # an oversized import literal is reported by the adapter as references_capped
    parsed = [
        p.model_copy(update={"diagnostics": (ParsedDiagnostic(code="references_capped", count=1),)})
        if p.path == "go/main.go"
        else p
        for p in parsed
    ]

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)
    revisions = _revisions(drafts)
    coverage = _coverage(drafts)

    assert {"go/main.go", "go/util/util.go"} <= set(revisions)
    observed = {d.payload["file_revision_id"] for d in by_type(drafts, "code.assertion.observed")}
    assert {revisions["go/main.go"], revisions["go/util/util.go"]} <= observed
    assert coverage[revisions["go/main.go"]]["complete"] is False
    assert coverage[revisions["go/main.go"]]["losses"] == ("references_capped",)
    assert coverage[revisions["go/util/util.go"]]["complete"] is True
    assert coverage[revisions["go/util/util.go"]]["losses"] == ()
