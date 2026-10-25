"""The revision tools without services (PLATFORM-043): `context_for_task`, `repository_overview`
and `symbol_context` over the in-memory world of `composer_world`, plus the scope and input
errors every tool shares."""

from __future__ import annotations

import contextlib
from dataclasses import replace
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from agent_context_sdk import (
    CommitRevisionV1,
    ContextPackageV1,
    RepositoryScopeV1,
    WorkspaceSnapshotRevisionV1,
)

from agent_context_platform.projection.projectors.git import commit_node_id
from agent_context_platform.retrieval.composer import (
    DEPENDENCIES,
    TOOLS,
    AmbiguousSymbol,
    ContextComposer,
    ContextRequestError,
    ScopeNotFound,
    SymbolNotFound,
    _Candidate,
    _excerpt_owner,
)
from agent_context_platform.retrieval.evidence import Citation
from agent_context_platform.retrieval.graph import GraphScope
from agent_context_platform.retrieval.revision import (
    RevisionNotFound,
    SymbolFact,
    SymbolRevisionFact,
)
from agent_context_platform.retrieval.search import SearchScope
from agent_context_platform.retrieval.temporal import (
    DecisionStanding,
    HistoricalReason,
    TemporalScope,
)

from .composer_world import (
    AS_OF,
    AS_OF_OUTBOX,
    C1,
    C2,
    NOW,
    PRINCIPAL,
    PROJECT,
    REPO,
    SNAP,
    World,
    answer,
    at,
    cited,
    content_item,
    decision,
    decision_input,
    failure_input,
    hit,
    items_by_id,
    neighborhood,
    overview_input,
    seeded,
    snapshot,
    stamp,
    symbol_input,
    task_input,
)

pytestmark = pytest.mark.unit

TASK = "rounding drift in calc add"
C2_END = at(2) - timedelta(microseconds=1)  # C2's window [1, 2) is read just before its end
PRIORITY = {
    "repository": 5,
    "module": 4,
    "decision": 3,
    "failure": 3,
    "change": 2,
    "dependency": 1,
}


def keys(world: World, item: Any) -> set[str]:
    """The ledger keys of the events an item cites."""
    return {world.ledger.key_of(str(ref.source_event_id)) for ref in item.evidence}


def text_of(page: ContextPackageV1) -> str:
    return "\n".join(f"{item.title}\n{item.content}" for item in page.items)


def call(composer: ContextComposer, tool: str, *, text: str | None = None) -> Any:
    """One request per tool, valid against the seeded world unless `text` is given."""
    if tool == "context_for_task":
        return composer.context_for_task(task_input(text or TASK), PRINCIPAL)
    if tool == "repository_overview":
        return composer.repository_overview(overview_input(), PRINCIPAL)
    if tool == "symbol_context":
        return composer.symbol_context(symbol_input(symbol_name=text or "add"), PRINCIPAL)
    if tool == "decision_history":
        return composer.decision_history(decision_input(text or "rounding"), PRINCIPAL)
    return composer.failure_history(failure_input(query=text or "drift"), PRINCIPAL)


# --- what every tool shares ---


@pytest.mark.parametrize("tool", TOOLS)
def test_every_tool_answers_from_one_freshness_read_over_its_own_projectors(tool: str) -> None:
    world = seeded()
    page = answer(call(world.composer(), tool))

    (read,) = world.freshness.calls
    assert read["projectors"] == DEPENDENCIES[tool]
    assert read["now"] == NOW
    if tool == "decision_history":
        assert (read["project_id"], read["repository_id"]) == (PROJECT, None)
    else:
        assert (read["project_id"], read["repository_id"]) == (None, REPO)
    assert page.items and all(item.evidence for item in page.items)
    assert page.as_of_event_id == UUID(AS_OF)
    assert page.projection_checkpoint == "pc1-unit"
    assert page.generated_at == NOW and page.staleness_ms == 0
    assert "projection_dead_letters:code" in page.warnings  # the snapshot's own warnings travel
    assert list(page.warnings) == sorted(set(page.warnings))
    assert page.truncated is False and page.next_cursor is None
    # Every cited event was delivered by the package's snapshot.
    assert world.ledger.loads
    assert all(scope.max_outbox_id == AS_OF_OUTBOX for _, scope in world.ledger.loads)


@pytest.mark.parametrize("tool", TOOLS)
def test_an_unknown_scope_is_a_typed_error_and_nothing_beyond_it_is_read(tool: str) -> None:
    world = seeded()
    world.revisions.repositories.clear()
    world.revisions.projects.clear()

    with pytest.raises(ScopeNotFound):
        answer(call(world.composer(), tool))

    assert world.revisions.called("view") == []
    assert world.temporal.calls == [] and world.search.calls == [] and world.ledger.loads == []


def test_an_unindexed_revision_is_a_typed_error_for_every_revision_tool() -> None:
    world = seeded()
    composer = world.composer()
    with pytest.raises(RevisionNotFound):
        answer(composer.repository_overview(overview_input("c" * 40), PRINCIPAL))
    with pytest.raises(RevisionNotFound):
        answer(composer.context_for_task(task_input(TASK, snapshot("snap-unknown")), PRINCIPAL))
    with pytest.raises(RevisionNotFound):
        answer(composer.symbol_context(symbol_input("c" * 40, symbol_id="s1"), PRINCIPAL))
    assert world.revisions.called("view") == [
        (REPO, "c" * 40),
        (REPO, "snap-unknown"),
        (REPO, "c" * 40),
    ]
    assert world.temporal.calls == []


@pytest.mark.parametrize("tool", ["context_for_task", "symbol_context", *TOOLS[3:]])
def test_query_text_over_the_limit_is_refused_before_anything_is_read(tool: str) -> None:
    world = seeded()
    with contextlib.suppress(SymbolNotFound):  # no symbol has that name, but it was looked up
        answer(call(world.composer(), tool, text="x" * 1000))
    assert world.freshness.calls  # the limit itself is accepted
    world = seeded()
    with pytest.raises(ContextRequestError, match="at most 1000 characters"):
        answer(call(world.composer(), tool, text="x" * 1001))
    assert world.freshness.calls == [] and world.revisions.calls == []


# --- context_for_task ---


def test_a_commit_task_reads_the_code_the_task_names_and_its_neighbours_at_that_commit() -> None:
    world = seeded()
    page = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    by_id = items_by_id(page)

    # The anchors (the symbol and file whose names share the task's words) and what lies within
    # two hops of them in C2's own relations; never the README (unreachable) nor the draft
    # (only in the dirty snapshot).
    code = {item_id for item_id in by_id if item_id.split(":")[0] in {"symbol", "file", "relation"}}
    assert code == {
        "symbol:s1",
        "file:f1",
        "symbol:s2",
        "symbol:s4",
        "file:f2",
        "file:f4",
        "relation:r1",
        "relation:r2",
        "relation:r3",
    }
    assert [item.item_id for item in page.items[:2]] == ["symbol:s1", "file:f1"]
    assert page.scope == RepositoryScopeV1(kind="repository", repository_id=REPO)
    assert page.revision == CommitRevisionV1(kind="commit", commit_id=C2)

    add = by_id["symbol:s1"]
    assert add.content.startswith(
        "symbol pkg.calc.add (function) defined in pkg/calc.py at commit bbbbbbbbbbbb\n"
        "symbol revision: sr1b\n"
        "definition evidence: deterministic (SCIP), confidence 1.0, extractor scip-python 1"
    )
    (ref,) = add.evidence
    assert str(ref.source_event_id) == world.id("c2:symbol:s1")  # C2's claim, not C1's
    assert (ref.evidence_kind, ref.confidence, ref.revision_id) == ("scip", 1.0, "sr1b")
    assert ref.valid_from == at(1)
    total = by_id["symbol:s2"]
    assert "syntactic (Tree-sitter), not semantic, confidence 0.9" in total.content
    assert (total.evidence[0].evidence_kind, total.evidence[0].confidence) == ("tree_sitter", 0.9)
    calc = by_id["file:f1"]
    assert calc.content == (
        "file pkg/calc.py at commit bbbbbbbbbbbb\n"
        "module: m1; language: python; file revision: fr1b\n"
        "membership: deterministic (repository scan of this revision), confidence 1.0"
    )
    assert (calc.evidence[0].evidence_kind, calc.evidence[0].revision_id) == ("git", "fr1b")

    # A relation below confidence 1 is rendered as a heuristic, never as fact.
    assert by_id["relation:r1"].content == (
        "pkg.report.total CALLS pkg.calc.add at commit bbbbbbbbbbbb\n"
        "evidence: deterministic scip evidence, confidence 1.0; extractor scip-python 1"
    )
    assert by_id["relation:r3"].content == (
        "pkg.helpers.round_half REFERENCES pkg.calc.add at commit bbbbbbbbbbbb\n"
        "evidence: heuristic (inferred, not verified): tree_sitter evidence, confidence 0.5; "
        "extractor tree-sitter 1"
    )
    assert by_id["relation:r2"].title == "pkg/report.py IMPORTS pkg/helpers.py"
    assert by_id["relation:r2"].content.endswith("; review: accepted")

    # A closed window never reads the current graph; temporal facts are read just before its end.
    assert world.graph.calls == []
    assert world.temporal.calls == [
        ("decisions", TemporalScope(repository_id=REPO), C2_END, C2_END, False),
        ("failures", TemporalScope(repository_id=REPO), C2_END, C2_END, True),
    ]
    assert world.revisions.called("view") == [(REPO, C2)]


def test_a_commit_task_cites_only_content_of_its_window_and_never_dirty_output() -> None:
    world = seeded()
    page = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    by_id = items_by_id(page)

    scope = SearchScope(PROJECT, REPO)
    assert world.search.calls == [
        ("lexical", "rounding or drift or calc or add", scope, 50),
        ("vector", TASK, scope, 50),
    ]
    assert world.revisions.called("projects_of") == [(REPO, 6)]
    # Every hit's events were checked against C2's window, its snapshot and its kept runs.
    wanted, filtered = world.ledger.loads[0]
    assert world.id("turn:late") in wanted and world.id("ci:dirty-output") in wanted
    assert (filtered.before, filtered.recorded_before) == (at(2), at(2))
    assert (filtered.revision_mode, filtered.snapshot_id) == (True, None)
    assert filtered.kept_runs == {"ci-clean", "ci-flaky-1"}

    turn = by_id[content_item("c-turn")]
    assert (turn.kind, turn.title) == ("assistant_message", "assistant message in session sess-1")
    # Lexical scores are normalized per list; the weaker vector match of the same content loses.
    assert turn.content == (
        "assistant message from session sess-1 (agent.turn.completed) at "
        "2026-09-01T10:30:00.000000Z\n"
        "content: sanitized (sanitized); match score 1.0\n"
        'evidence excerpt (untrusted, quoted): "We should round half even in add."'
    )
    (ref,) = turn.evidence
    assert (ref.evidence_kind, ref.content_ref.content_id) == ("agent", "c-turn")
    assert (world.id("turn:1"), "c-turn") in world.ledger.snippet_requests

    output = by_id[content_item("c-clean-output")]
    assert output.kind == "ci_output"
    assert f", run on commit {C2}\n" in output.content and "match score 0.5" in output.content
    assert output.evidence[0].evidence_kind == "test"
    message = by_id[content_item("c-commit")]
    assert message.kind == "commit_message" and "match score 0.72" in message.content
    finding = by_id[content_item("c-finding")]
    assert finding.kind == "finding" and "run on" not in finding.content

    # Later content, the dirty snapshot's output, a decision the temporal read did not return,
    # the text of a decision that is already an item, and a vector hit below the floor: none.
    for content_id in ("c-late", "c-dirty-output", "c-hidden", "c-dec-round", "c-noise"):
        assert content_item(content_id) not in by_id
    for key in ("turn:late", "ci:dirty-output", "dec:hidden", "turn:noise"):
        assert world.id(key) not in cited(page)
    assert "DIRTYMARK" not in text_of(page) and "HIDDEN" not in text_of(page)


def test_shared_content_is_described_by_its_admitted_event_never_a_dropped_one() -> None:
    world = seeded()
    # The same content was first the dirty snapshot's test output, then part of a turn in C2's
    # window: the search row describes it by that first (dirty) reference.
    dirty = world.ledger.add(
        "ci:dirty-shared",
        "quality.test_run.completed",
        at(1.4),
        {"test_run_id": "t-dirty", "snapshot_id": SNAP},
        content_id="c-shared",
        text="shared rounding note",
    )
    turn = world.ledger.add(
        "turn:shared",
        "agent.turn.completed",
        at(1.55),
        content_id="c-shared",
        text="shared rounding note",
    )
    row = hit(world.ledger, "ci:dirty-shared", "c-shared", 9.0)
    world.search.lexical_hits.append(replace(row, source_event_ids=(dirty, turn)))

    page = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    item = items_by_id(page)[content_item("c-shared")]

    assert (item.kind, item.title) == ("assistant_message", "assistant message in the repository")
    assert "(agent.turn.completed)" in item.content and "test_run" not in item.content
    assert [(ref.evidence_kind, str(ref.source_event_id)) for ref in item.evidence] == [
        ("agent", turn)
    ]
    assert dirty not in cited(page)


def test_a_commit_answer_drops_dirty_runs_that_a_snapshot_answer_keeps() -> None:
    world = seeded()
    clean = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    drift = items_by_id(clean)["failure:fail_drift"]

    # The dirty run is gone, so the resolution derived over it is not claimed; its observation
    # is neither cited nor quoted (the clean one is quoted instead).
    assert drift.title == "failure fail_drift (unknown)"
    assert "resolution: unknown (dirty_runs_excluded)" in drift.content
    assert "- ci_run ci-clean, status failure, ci/test, completed" in drift.content
    assert "ci-dirty" not in drift.content and "DIRTYMARK" not in text_of(clean)
    assert drift.content.endswith('evidence excerpt (untrusted, quoted): "clean drift details"')
    assert keys(world, drift) == {"obs:clean", "ci:clean"}
    kinds = {world.ledger.key_of(str(r.source_event_id)): r.evidence_kind for r in drift.evidence}
    assert kinds == {"obs:clean": "agent", "ci:clean": "test"}
    # A fixed failure the task does not name is history, not context.
    assert "failure:fail_flaky" not in items_by_id(clean)
    assert "fix:fail_flaky" not in items_by_id(clean)

    world = seeded()
    dirty = answer(world.composer().context_for_task(task_input(TASK, snapshot()), PRINCIPAL))
    assert dirty.revision == WorkspaceSnapshotRevisionV1(
        kind="workspace_snapshot", snapshot_id=SNAP
    )
    drift = items_by_id(dirty)["failure:fail_drift"]
    assert "resolution: unresolved (no_passing_run)" in drift.content
    assert (
        "- ci_run ci-dirty, status failure, ci/test, completed 2026-09-01T10:30:00.000000Z, "
        "workspace snapshot snap-1"
    ) in drift.content
    assert keys(world, drift) == {"obs:dirty", "obs:clean", "ci:clean", "ci:dirty"}
    output = items_by_id(dirty)[content_item("c-dirty-output")]
    assert output.kind == "ci_output" and ", run on workspace snapshot snap-1\n" in output.content
    # The snapshot's window is still open: later content is evidence there.
    assert world.id("turn:late") in cited(dirty)
    _, filtered = world.ledger.loads[0]
    assert (filtered.before, filtered.snapshot_id) == (None, SNAP)
    assert filtered.kept_runs == {"ci-clean", "ci-dirty", "ci-flaky-1"}


def test_an_open_snapshot_window_adds_the_current_graphs_neighbours_it_has() -> None:
    world = seeded()
    world.graph.reached["s6"] = neighborhood(
        "s6", [("s6", 0), ("f2", 1), ("ghost", 1)], truncated=True
    )
    page = answer(
        world.composer().context_for_task(task_input("average draft helper", snapshot()), PRINCIPAL)
    )
    by_id = items_by_id(page)

    # `pkg/report.py` is no relation of the draft in the snapshot's own facts: only the current
    # graph puts it one hop away. A node the snapshot does not have is never admitted.
    assert {"symbol:s6", "file:f6", "file:f2"} <= set(by_id)
    assert not any("ghost" in item_id for item_id in by_id)
    assert world.graph.calls == [(GraphScope(REPO), "s6", 2), (GraphScope(REPO), "f6", 2)]
    assert {
        "graph_neighborhood_capped",  # the neighborhood of s6 was cut
        "graph_anchor_not_current",  # f6 is not in the current graph
        "revision_index_incomplete",  # the snapshot's index never completed
    } <= set(page.warnings)
    assert by_id["symbol:s6"].content.startswith(
        "symbol pkg.draft.average (function) defined in pkg/draft.py at workspace snapshot snap-1"
    )
    # An open window is read at the snapshot's own time, not the reader's clock.
    assert [read[2:4] for read in world.temporal.calls] == [(at(3), at(3)), (at(3), at(3))]


def test_search_degrades_with_a_warning_and_never_fails_the_answer() -> None:
    world = seeded()
    world.search.vector_fails = True
    page = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    assert "vector_search_unavailable" in page.warnings
    assert content_item("c-turn") in items_by_id(page)  # the lexical leg still answers
    assert content_item("c-commit") not in items_by_id(page)  # found by vector only

    world = seeded()
    page = answer(world.composer(vector_search=False).context_for_task(task_input(TASK), PRINCIPAL))
    assert [name for name, *_ in world.search.calls] == ["lexical"]  # skipped, not attempted
    assert "vector_search_unavailable" not in page.warnings

    world = seeded()
    world.revisions.links.clear()
    page = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    assert world.search.calls == [] and "search_scope_unresolved" in page.warnings
    assert not any(item.item_id.startswith("content:") for item in page.items)

    world = seeded()
    world.revisions.links[REPO] = [f"prj-{n}" for n in range(1, 8)]
    page = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    assert "search_projects_capped" in page.warnings
    searched = sorted({scope.project_id for _, _, scope, _ in world.search.calls})
    assert searched == ["prj-1", "prj-2", "prj-3", "prj-4", "prj-5"]

    world = seeded()
    world.ledger.cap = 3
    page = answer(world.composer().context_for_task(task_input(TASK), PRINCIPAL))
    assert "evidence_capped" in page.warnings


def test_a_task_without_content_words_is_answered_by_vector_search_alone() -> None:
    world = seeded()
    world.temporal.decision_records.clear()
    world.temporal.failure_records.clear()
    page = answer(world.composer().context_for_task(task_input("what is it for"), PRINCIPAL))

    assert [name for name, *_ in world.search.calls] == ["vector"]
    assert [item.kind for item in page.items] == ["commit_message", "assistant_message"]
    assert "match score 0.4" in page.items[1].content  # 0.5 cosine, weighted 0.8
    # The page cites the events the search filter already loaded: one ledger load in all.
    assert len(world.ledger.loads) == 1


def test_an_outdated_temporal_projection_is_only_a_warning_for_the_revision_tools() -> None:
    world = seeded()
    world.temporal.outdated = True
    composer = world.composer()
    pages = [
        answer(composer.context_for_task(task_input(TASK), PRINCIPAL)),
        answer(composer.repository_overview(overview_input(), PRINCIPAL)),
        answer(composer.symbol_context(symbol_input(symbol_name="add"), PRINCIPAL)),
    ]
    for page in pages:
        assert "temporal_projection_outdated" in page.warnings
        kinds = {item.kind for item in page.items}
        assert kinds and not kinds & {"decision", "failure", "fix_path"}


# --- repository_overview ---


def test_an_overview_summarizes_the_commit_its_modules_changes_and_dependencies() -> None:
    world = seeded()
    page = answer(world.composer().repository_overview(overview_input(), PRINCIPAL))
    by_id = items_by_id(page)

    summary = page.items[0]
    assert (summary.item_id, summary.title) == (
        "repository:repo-1",
        "repository repo-1 at bbbbbbbbbbbb",
    )
    assert summary.content == (
        f"repository repo-1 at commit {C2}: 4 files in 2 modules, 3 symbols, 3 relations "
        "(1 deterministic, 2 heuristic), 2 external dependencies\n"
        "languages: markdown, python; index run: complete"
    )
    assert keys(world, summary) == {"run:c2"}
    assert by_id["module:m1"].title == "module pkg"
    assert by_id["module:m1"].content.endswith(
        "3 files, 3 symbols\nfiles: pkg/calc.py, pkg/helpers.py, pkg/report.py"
    )
    assert by_id["module:m0"].title == "module ."  # the root directory
    assert keys(world, by_id["module:m0"]) == {"c1:file:f5"}  # an unchanged file replays C1

    changes = {item.item_id: item.title for item in page.items if item.kind == "change"}
    assert changes == {
        "change:f1": "modified pkg/calc.py",
        "change:f2": "added pkg/report.py",
        "change:f3": "removed pkg/legacy.py",
        "change:f4": "renamed pkg/util.py to pkg/helpers.py (content changed)",
    }
    assert by_id["change:f1"].content == (
        "modified pkg/calc.py: between commit aaaaaaaaaaaa and commit bbbbbbbbbbbb "
        "(deterministic, from the two index runs' file membership)"
    )
    assert keys(world, by_id["change:f3"]) == {"c1:file:f3"}  # a removal cites the old claim
    assert keys(world, by_id["change:f4"]) == {"c2:file:f4", "c1:file:f4"}
    assert world.revisions.called("run_files") == [(REPO, "idx-1")]

    assert by_id["dependency:numpy"].content == (
        "external dependency numpy (pypi), 2.1.0, declared in pkg/calc.py at commit "
        "bbbbbbbbbbbb; extractor manifest"
    )
    assert by_id["dependency:requests"].content == (
        "external dependency requests (pypi), >=2.31, declared in pkg/report.py at commit "
        "bbbbbbbbbbbb (heuristic, confidence 0.6); extractor manifest"
    )
    # Active decisions and open failures; a fixed failure is history, not an overview.
    assert {"decision:dec_round", "decision:dec_logging", "failure:fail_drift"} <= set(by_id)
    assert "failure:fail_flaky" not in by_id
    assert "resolution: unknown (dirty_runs_excluded)" in by_id["failure:fail_drift"].content
    # The documented section order: summary, modules, temporal facts, changes, dependencies.
    ranks = [PRIORITY[item.kind] for item in page.items]
    assert ranks == sorted(ranks, reverse=True) and set(ranks) == {1, 2, 3, 4, 5}


def test_an_overview_of_the_first_commit_has_no_changes() -> None:
    world = seeded()
    world.temporal.decision_records.clear()
    world.temporal.failure_records.clear()
    page = answer(world.composer().repository_overview(overview_input(C1), PRINCIPAL))

    assert [item.kind for item in page.items] == ["repository", "module", "module"]
    assert page.items[0].content.startswith(
        f"repository repo-1 at commit {C1}: 4 files in 2 modules, 1 symbols, 0 relations"
    )
    assert world.revisions.called("run_files") == []
    end = at(1) - timedelta(microseconds=1)
    assert [read[2:4] for read in world.temporal.calls] == [(end, end), (end, end)]


def test_an_overview_of_an_incomplete_or_capped_index_says_so() -> None:
    world = seeded()
    view = world.revisions.views[C2]
    world.revisions.views[C2] = replace(view, run=replace(view.run, complete=False), truncated=True)
    world.temporal.truncated = True
    page = answer(world.composer().repository_overview(overview_input(), PRINCIPAL))

    assert page.items[0].content.endswith("index run: incomplete (absence not inferred)")
    assert {
        "revision_index_incomplete",
        "revision_view_capped",
        "decisions_capped",
        "failures_capped",
    } <= set(page.warnings)


# --- symbol_context ---


def test_symbol_context_puts_the_definition_first_then_relations_history_and_decisions() -> None:
    world = seeded()
    ledger = world.ledger
    world.revisions.histories["s1"] = (
        SymbolRevisionFact(
            "sr1a",
            "pkg.calc.add",
            stamp(at(0)),
            stamp(at(1)),
            "Commit",
            commit_node_id(REPO, C1),
            (world.id("c1:symbol:s1"),),
        ),
        SymbolRevisionFact(
            "sr1b",
            "pkg.calc.add",
            stamp(at(1)),
            None,
            "Commit",
            commit_node_id(REPO, C2),
            (world.id("c2:symbol:s1"),),
        ),
    )
    ledger.add("dec:file", "knowledge.decision.recorded", at(0.1))
    ledger.add("dec:module", "knowledge.decision.recorded", at(0.2))
    world.temporal.decision_records += [
        decision(
            "dec_file",
            [world.id("dec:file")],
            subjects=["pkg/calc.py"],
            standing=DecisionStanding.HISTORICAL,
            reason=HistoricalReason.SUPERSEDED,
            superseded_by="dec_round",
            valid_from=at(0.1),
            effective_valid_to=at(0.5),
        ),
        decision(
            "dec_module",
            [world.id("dec:module")],
            subjects=["m1"],
            standing=DecisionStanding.HISTORICAL,
            reason=HistoricalReason.EXPIRED,
            valid_from=at(0.2),
            effective_valid_to=at(0.9),
        ),
    ]
    page = answer(world.composer().symbol_context(symbol_input(symbol_name="add"), PRINCIPAL))
    by_id = items_by_id(page)
    order = [item.item_id for item in page.items]

    # The short name resolves to the one symbol; only it and what is about it are items.
    assert order[0] == "symbol:s1"
    assert set(by_id) == {
        "symbol:s1",
        "relation:r1",
        "relation:r3",
        "relation:r2",
        "symbol_history:s1",
        "decision:dec_round",
        "decision:dec_file",
        "decision:dec_module",
    }
    # Relations that touch the symbol come before those between its neighbours.
    assert max(order.index("relation:r1"), order.index("relation:r3")) < order.index("relation:r2")

    history = by_id["symbol_history:s1"]
    assert history.title == "history of pkg.calc.add"
    assert history.content == (
        "revisions of pkg.calc.add up to commit bbbbbbbbbbbb:\n"
        "- revision sr1a as pkg.calc.add: from 2026-09-01T09:00:00.000000Z (commit aaaaaaaaaaaa)"
        " until 2026-09-01T10:00:00.000000Z\n"
        "- revision sr1b as pkg.calc.add: from 2026-09-01T10:00:00.000000Z (commit bbbbbbbbbbbb)"
        ", still held at commit bbbbbbbbbbbb"
    )
    assert {(ref.revision_id, ref.evidence_kind) for ref in history.evidence} == {
        ("sr1a", "scip"),
        ("sr1b", "scip"),
    }
    assert world.revisions.called("symbol_history") == [("idx-2", "s1")]

    # Decisions about the symbol, its file or its module, with their standing at the commit.
    assert "decision:dec_logging" not in by_id
    assert "HISTORICAL at the requested time: superseded by dec_round" in (
        by_id["decision:dec_file"].content
    )
    assert "HISTORICAL at the requested time: expired at 2026-09-01T09:54:00.000000Z" in (
        by_id["decision:dec_module"].content
    )
    assert by_id["decision:dec_round"].content.endswith('"Round half even in add."')
    assert world.temporal.calls == [
        ("decisions", TemporalScope(repository_id=REPO), C2_END, C2_END, True)
    ]


def test_symbol_lookups_fail_typed_and_a_symbol_without_history_has_no_history_item() -> None:
    world = seeded()
    page = answer(world.composer().symbol_context(symbol_input(symbol_id="s2"), PRINCIPAL))
    assert page.items[0].item_id == "symbol:s2"
    assert "symbol_history" not in {item.kind for item in page.items}
    assert {"relation:r1", "relation:r2"} <= set(items_by_id(page))

    with pytest.raises(SymbolNotFound):
        answer(world.composer().symbol_context(symbol_input(symbol_id="s9"), PRINCIPAL))
    with pytest.raises(SymbolNotFound):
        answer(world.composer().symbol_context(symbol_input(symbol_name="draft"), PRINCIPAL))
    view = world.revisions.views[C2]
    twin = SymbolFact(
        "s7",
        "sr7",
        "pkg.helpers.add",
        "function",
        "f4",
        "fr4b",
        None,
        None,
        None,
        stamp(at(1)),
        (world.id("c2:symbol:s4"),),
    )
    world.revisions.views[C2] = replace(view, symbols=(*view.symbols, twin))
    with pytest.raises(AmbiguousSymbol, match="2 symbols have that name"):
        answer(world.composer().symbol_context(symbol_input(symbol_name="add"), PRINCIPAL))
    # The exact qualified name is never ambiguous.
    page = answer(
        world.composer().symbol_context(symbol_input(symbol_name="pkg.helpers.add"), PRINCIPAL)
    )
    assert page.items[0].item_id == "symbol:s7"
    assert page.items[0].content.startswith(
        "symbol pkg.helpers.add (function) defined in pkg/helpers.py at commit bbbbbbbbbbbb"
    )
    assert "extractor unknown" in page.items[0].content


# --- excerpts ---


def test_an_item_quotes_its_named_event_else_a_cited_event_with_the_same_content() -> None:
    world = seeded()
    events = {world.id(key): world.ledger.stored[world.id(key)] for key in ("turn:1", "obs:clean")}

    def candidate(excerpt: tuple[str, str | None] | None, *cites: Citation) -> _Candidate:
        return _Candidate("x", "file", "t", "b", cites, 1, excerpt=excerpt)

    turn, clean, absent = world.id("turn:1"), world.id("obs:clean"), world.id("obs:dirty")
    named = candidate((turn, "c-turn"), Citation(turn, "agent", 1.0))
    assert _excerpt_owner(named, events) is events[turn]
    # The named event is not citable: another cited event that carries the same content is.
    same = candidate(
        (absent, "c-turn"),
        Citation(absent, "agent", 1.0, content_id="c-turn"),
        Citation(turn, "agent", 1.0, content_id="c-turn"),
    )
    assert _excerpt_owner(same, events) is events[turn]
    other = candidate((absent, "c-turn"), Citation(clean, "agent", 1.0, content_id="c-other"))
    assert _excerpt_owner(other, events) is None
    # With no content ID, the event's own content: any cited event will do.
    own = candidate((absent, None), Citation(absent, "agent", 1.0), Citation(clean, "agent", 1.0))
    assert _excerpt_owner(own, events) is events[clean]
    assert _excerpt_owner(candidate(None, Citation(turn, "agent", 1.0)), events) is None
