"""The history tools and the shared paging without services (PLATFORM-043): `decision_history`,
`failure_history`, budgets, truncation and cursors over the in-memory world of `composer_world`."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from agent_context_sdk import ProjectScopeV1, RepositoryScopeV1
from sqlalchemy.exc import OperationalError

from agent_context_platform.retrieval import composer as composer_module
from agent_context_platform.retrieval.composer import (
    BACKLOG_WARNING,
    ContextBudgetTooSmall,
    ContextUnavailable,
    RequestContext,
    ScopeNotFound,
)
from agent_context_platform.retrieval.cursor import (
    CursorExpired,
    CursorInvalid,
    CursorMismatch,
    CursorStale,
)
from agent_context_platform.retrieval.evidence import EvidenceScope
from agent_context_platform.retrieval.temporal import (
    DecisionStanding,
    FixPath,
    FixResolution,
    FixStatus,
    HistoricalReason,
    TemporalProjectionOutdated,
    TemporalScope,
)

from .composer_world import (
    AS_OF_OUTBOX,
    C2,
    NOW,
    PRINCIPAL,
    PROJECT,
    REPO,
    SNAP,
    World,
    answer,
    at,
    ci_run,
    cited,
    decision,
    decision_input,
    failure,
    failure_input,
    items_by_id,
    overview_input,
    seeded,
    task_input,
)

pytestmark = pytest.mark.unit


def keys(world: World, item: Any) -> set[str]:
    return {world.ledger.key_of(str(ref.source_event_id)) for ref in item.evidence}


def ids(page: Any) -> list[str]:
    return [item.item_id for item in page.items]


# --- decision_history ---


def history_world() -> World:
    """A rounding decision superseded by a later one, and an unrelated decision whose only event
    belongs to another repository of the same project."""
    world = seeded()
    ledger = world.ledger
    ledger.add("dec:v1", "knowledge.decision.recorded", at(0.2), text="Round half up in add.")
    ledger.add("dec:v2", "knowledge.decision.recorded", at(0.8), text="Round half even in add.")
    ledger.add("dec:log", "knowledge.decision.recorded", at(0.3), repository="repo-2")
    world.temporal.decision_records = [
        decision(
            "dec_v1",
            [world.id("dec:v1")],
            subjects=["pkg.calc.add"],
            standing=DecisionStanding.HISTORICAL,
            reason=HistoricalReason.SUPERSEDED,
            superseded_by="dec_v2",
            valid_from=at(0.2),
            effective_valid_to=at(0.8),
        ),
        decision(
            "dec_v2",
            [world.id("dec:v2")],
            subjects=["pkg.calc.add"],
            supersedes="dec_v1",
            valid_from=at(0.8),
        ),
        decision("dec_log", [world.id("dec:log")], subjects=["pkg.log"], valid_from=at(0.3)),
    ]
    ledger.ranks["policy"] = {world.id("dec:v1"): 0.5}  # only v1's text mentions a policy
    return world


def test_decision_history_follows_supersession_from_the_decisions_the_query_matches() -> None:
    world = history_world()
    page = answer(world.composer().decision_history(decision_input("rounding policy"), PRINCIPAL))

    # Only v1 matches, lexically; v2 joins it through the supersession link, the active one first.
    assert ids(page) == ["decision:dec_v2", "decision:dec_v1"]
    v2, v1 = page.items
    assert v1.title == "decision dec_v1 (historical)"
    assert v1.content.startswith(
        "decision dec_v1: HISTORICAL at the requested time: superseded by dec_v2\n"
        "recorded status: accepted; valid from 2026-09-01T09:12:00.000000Z to "
        "2026-09-01T09:48:00.000000Z; recorded at 2026-09-01T09:12:00.000000Z\n"
        "supersedes: none; subjects: pkg.calc.add"
    )
    assert v1.content.endswith('evidence excerpt (untrusted, quoted): "Round half up in add."')
    (ref,) = v1.evidence
    assert (ref.evidence_kind, ref.valid_from, ref.valid_to) == ("agent", at(0.2), at(0.8))
    assert "ACTIVE at the requested time" in v2.content and "supersedes: dec_v1" in v2.content
    assert "query_matched_nothing" not in page.warnings

    events = (world.id("dec:v1"), world.id("dec:v2"), world.id("dec:log"))
    assert world.ledger.rank_calls == [(events, "rounding or policy", 200)]
    # Read at the requested valid time, as known at the snapshot's own time, with history.
    assert world.temporal.calls == [
        ("decisions", TemporalScope(project_id=PROJECT), at(3), at(3), True)
    ]
    assert world.revisions.called("project_exists") == [PROJECT]
    assert page.scope == ProjectScopeV1(kind="project", project_id=PROJECT)
    assert page.revision is None


def test_a_decision_query_that_matches_nothing_returns_every_decision_and_says_so() -> None:
    world = history_world()
    page = answer(world.composer().decision_history(decision_input("unrelated words"), PRINCIPAL))

    assert set(ids(page)) == {"decision:dec_v1", "decision:dec_v2", "decision:dec_log"}
    assert "query_matched_nothing" in page.warnings
    assert world.ledger.rank_calls[0][1] == "unrelated or words"
    # In a project scope, an event of another repository of the project is evidence.
    assert world.id("dec:log") in cited(page)


def test_a_repository_scoped_history_cites_only_that_repositorys_events() -> None:
    world = history_world()
    scope = {"kind": "repository", "repository_id": REPO}
    page = answer(
        world.composer().decision_history(decision_input("unrelated", scope=scope), PRINCIPAL)
    )

    assert world.revisions.called("repository_exists") == [REPO]
    assert world.revisions.called("project_exists") == []
    assert world.temporal.calls[0][1] == TemporalScope(repository_id=REPO)
    assert world.freshness.calls[0]["repository_id"] == REPO
    assert page.scope == RepositoryScopeV1(kind="repository", repository_id=REPO)
    # dec_log's only event belongs to repo-2: no evidence, no item, and the package says so.
    assert set(ids(page)) == {"decision:dec_v1", "decision:dec_v2"}
    assert "items_without_evidence" in page.warnings
    _, filtered = world.ledger.loads[0]
    assert (filtered.repository_id, filtered.project_id) == (REPO, None)


def test_decision_history_is_known_only_up_to_the_projection_backlog() -> None:
    world = history_world()
    world.freshness.fresh = replace(world.freshness.fresh, known_until=at(2.5))
    world.temporal.truncated = True
    page = answer(world.composer().decision_history(decision_input("policy"), PRINCIPAL))

    (read,) = world.temporal.calls
    assert read[2:] == (at(3), at(2.5), True)  # valid at the request, recorded at the backlog
    assert {BACKLOG_WARNING, "decisions_capped"} <= set(page.warnings)


def test_a_decision_delivered_after_the_snapshot_is_never_cited() -> None:
    world = history_world()
    world.ledger.outbox[world.id("dec:v2")] = AS_OF_OUTBOX + 1  # past a still-pending row
    page = answer(world.composer().decision_history(decision_input("rounding policy"), PRINCIPAL))

    assert ids(page) == ["decision:dec_v1"]
    assert world.id("dec:v2") not in cited(page)
    assert "items_without_evidence" in page.warnings
    assert all(scope.max_outbox_id == AS_OF_OUTBOX for _, scope in world.ledger.loads)


def test_the_history_tools_fail_on_an_outdated_temporal_projection() -> None:
    world = history_world()
    world.temporal.outdated = True
    with pytest.raises(TemporalProjectionOutdated):
        answer(world.composer().decision_history(decision_input("policy"), PRINCIPAL))
    with pytest.raises(TemporalProjectionOutdated):
        answer(world.composer().failure_history(failure_input(query="drift"), PRINCIPAL))
    assert world.ledger.loads == []


# --- failure_history ---


def failures_world() -> World:
    """The seeded failures plus one from long before the window and one whose fix path starts
    at a run of the dirty snapshot."""
    world = seeded()
    ledger = world.ledger
    ledger.add("obs:old", "knowledge.failure.observed", at(-5), {"ci_run_id": "ci-old"})
    ledger.add(
        "ci:fixd-dirty",
        "quality.ci_run.completed",
        at(1.6),
        {"ci_run_id": "ci-fixd-dirty", "snapshot_id": SNAP},
    )
    ledger.add(
        "ci:fixd-pass",
        "quality.ci_run.completed",
        at(1.8),
        {"ci_run_id": "ci-fixd-pass", "commit_id": C2},
    )
    ledger.add(
        "obs:fixd",
        "knowledge.failure.observed",
        at(1.6),
        {"ci_run_id": "ci-fixd-dirty"},
        text="wobbly fixture",
    )
    old = failure(
        "fail_old",
        [world.id("obs:old")],
        [ci_run("ci-old", "failure", at(-5), [world.id("obs:old")], commit=C2)],
        FixResolution(FixStatus.UNKNOWN, None),
        valid_from=at(-5),
    )
    fixd = failure(
        "fail_fixd",
        [world.id("obs:fixd")],
        [ci_run("ci-fixd-dirty", "failure", at(1.6), [world.id("ci:fixd-dirty")], snapshot=SNAP)],
        FixResolution(
            FixStatus.RESOLVED,
            None,
            FixPath(
                "fail_fixd",
                "ci-fixd-dirty",
                "ci-fixd-pass",
                "ci",
                "test",
                at(1.6),
                at(1.8),
                C2,
                None,
                True,
                (world.id("ci:fixd-pass"),),
            ),
        ),
        operation="test_fixd",
        valid_from=at(1.6),
    )
    world.temporal.failure_records += [old, fixd]
    ledger.ranks["wobbly"] = {world.id("obs:fixd"): 0.4}
    return world


def test_failure_history_by_fingerprint_counts_every_run_of_the_repository() -> None:
    world = failures_world()
    page = answer(world.composer().failure_history(failure_input(fingerprint="1" * 64), PRINCIPAL))

    assert ids(page) == ["failure:fail_drift"]
    (drift,) = page.items
    assert drift.title == "failure fail_drift (unresolved)"
    assert drift.content.startswith(
        "failure fail_drift: AssertionError in pytest/test_add\n"
        "first observed 2026-09-01T10:12:00.000000Z; recorded 2026-09-01T10:12:00.000000Z; "
        f"fingerprint {'1' * 64}\n"
        "resolution: unresolved (no_passing_run)\n"
        "observed in:\n"
        "- ci_run ci-clean, status failure, ci/test, completed 2026-09-01T10:12:00.000000Z, "
        f"commit {C2}\n"
        "- ci_run ci-dirty, status failure, ci/test, completed 2026-09-01T10:30:00.000000Z, "
        "workspace snapshot snap-1\n"
    )
    # Outside a revision every run of the repository is evidence, the dirty one included.
    kinds = {world.ledger.key_of(str(r.source_event_id)): r.evidence_kind for r in drift.evidence}
    assert kinds == {
        "obs:dirty": "agent",
        "obs:clean": "agent",
        "ci:clean": "test",
        "ci:dirty": "test",
    }
    assert drift.content.endswith('"DIRTYMARK drift details from the dirty workspace"')
    assert world.ledger.rank_calls == []  # a fingerprint is not ranked

    end = at(4) - timedelta(microseconds=1)
    assert world.temporal.calls == [
        ("failures", TemporalScope(repository_id=REPO), end, at(3), True)
    ]
    _, filtered = world.ledger.loads[0]
    assert filtered == EvidenceScope(repository_id=REPO, before=at(4), max_outbox_id=AS_OF_OUTBOX)
    assert page.scope == RepositoryScopeV1(kind="repository", repository_id=REPO)
    assert page.revision is None


def test_failure_history_by_query_ranks_failures_of_the_window_and_their_fix_paths() -> None:
    world = failures_world()
    query = "flaky lint timeout wobbly"
    page = answer(world.composer().failure_history(failure_input(query=query), PRINCIPAL))
    by_id = items_by_id(page)

    # flaky by its names (lint, timeout, TimeoutError), fixd by the lexical rank of its text;
    # the drift matches neither, and the old failure lies outside [valid_from, valid_to).
    assert set(by_id) == {
        "failure:fail_flaky",
        "fix:fail_flaky",
        "failure:fail_fixd",
        "fix:fail_fixd",
    }
    in_window = tuple(world.id(key) for key in ("obs:dirty", "obs:clean", "obs:flaky", "obs:fixd"))
    assert world.ledger.rank_calls == [(in_window, "flaky or lint or timeout or wobbly", 200)]

    flaky = by_id["fix:fail_flaky"]
    assert flaky.kind == "fix_path"
    assert flaky.title == "CI lint/ruff passed after failure fail_flaky"
    assert flaky.content == (
        "failure fail_flaky: CI lint/ruff failed in run ci-flaky-1 (2026-09-01T10:18:00.000000Z)"
        " and passed in run ci-flaky-2 (2026-09-01T10:24:00.000000Z)\n"
        f"validated revision: {C2}\n"
        "a re-run passed on the SAME commit as the failing run: no code change is evidenced "
        "(the failure may be flaky)"
    )
    assert keys(world, flaky) == {"ci:flaky-pass"}
    assert flaky.evidence[0].evidence_kind == "test"
    assert by_id["failure:fail_flaky"].title == "failure fail_flaky (resolved)"
    # Outside a revision, a fix path that starts at a dirty run is what really happened.
    fixd = by_id["fix:fail_fixd"].content
    assert "failed in run ci-fixd-dirty" in fixd and "a different revision" in fixd
    assert "query_matched_nothing" not in page.warnings


def test_a_failure_query_that_matches_nothing_returns_every_failure_of_the_window() -> None:
    world = failures_world()
    world.temporal.truncated = True
    page = answer(world.composer().failure_history(failure_input(query="nothing here"), PRINCIPAL))

    failures = {item_id for item_id in ids(page) if item_id.startswith("failure:")}
    assert failures == {"failure:fail_drift", "failure:fail_flaky", "failure:fail_fixd"}
    assert {"query_matched_nothing", "failures_capped"} <= set(page.warnings)


def test_failure_history_of_an_unknown_repository_reads_no_failures() -> None:
    world = failures_world()
    with pytest.raises(ScopeNotFound):
        answer(
            world.composer().failure_history(
                failure_input(query="drift", repository_id="repo-absent"), PRINCIPAL
            )
        )
    assert world.temporal.calls == []


# --- pages, budgets and cursors ---

SMALL = {"max_items": 2, "max_bytes": 400_000, "max_tokens": 100_000}


def test_pages_are_contiguous_until_the_last_which_has_no_cursor() -> None:
    world = seeded()
    composer = world.composer()
    everything = answer(composer.repository_overview(overview_input(), PRINCIPAL))
    pages = [answer(composer.repository_overview(overview_input(budget=SMALL), PRINCIPAL))]
    while pages[-1].next_cursor is not None:
        assert pages[-1].truncated and len(pages[-1].items) == 2
        cursor = pages[-1].next_cursor
        pages.append(
            answer(
                composer.repository_overview(overview_input(budget=SMALL, cursor=cursor), PRINCIPAL)
            )
        )

    assert [item_id for page in pages for item_id in ids(page)] == ids(everything)
    assert len(pages) == -(-len(everything.items) // 2) and len(pages) > 2
    assert pages[-1].truncated is False
    assert {page.as_of_event_id for page in pages} == {everything.as_of_event_id}


def test_a_cursor_is_bound_to_its_tool_query_principal_snapshot_and_expiry() -> None:
    world = seeded()
    composer = world.composer()
    first = answer(composer.repository_overview(overview_input(budget=SMALL), PRINCIPAL))
    cursor = first.next_cursor
    assert cursor is not None

    tampered = cursor[:-2] + ("AA" if not cursor.endswith("AA") else "BB")
    with pytest.raises(CursorInvalid):
        answer(
            composer.repository_overview(overview_input(budget=SMALL, cursor=tampered), PRINCIPAL)
        )
    with pytest.raises(CursorMismatch):  # another budget is another query
        answer(composer.repository_overview(overview_input(cursor=cursor), PRINCIPAL))
    with pytest.raises(CursorMismatch):  # another principal
        answer(
            composer.repository_overview(
                overview_input(budget=SMALL, cursor=cursor), RequestContext("someone-else")
            )
        )
    with pytest.raises(CursorMismatch):  # another tool
        answer(composer.context_for_task(task_input("add", budget=SMALL, cursor=cursor), PRINCIPAL))
    later = world.composer(clock=NOW + timedelta(days=1))
    with pytest.raises(CursorExpired):
        answer(later.repository_overview(overview_input(budget=SMALL, cursor=cursor), PRINCIPAL))
    # A delivery that moves only the checkpoints (the watermark waits on an earlier row) still
    # changes the answer, so the cursor is stale.
    world.freshness.fresh = replace(world.freshness.fresh, projection_checkpoint="pc1-moved")
    with pytest.raises(CursorStale):
        answer(composer.repository_overview(overview_input(budget=SMALL, cursor=cursor), PRINCIPAL))
    assert len(world.temporal.calls) == 2  # a refused cursor never reaches the candidates


def test_budgets_are_exact_over_the_serialized_items_and_too_small_is_a_typed_error() -> None:
    world = seeded()
    composer = world.composer()
    full = answer(composer.repository_overview(overview_input(), PRINCIPAL))
    sizes = [len(item.model_dump_json().encode()) for item in full.items]
    pair = sizes[0] + sizes[1] + 3  # "[a,b]"

    def page(max_bytes: int, max_tokens: int = 100_000) -> Any:
        budget = {"max_items": 60, "max_bytes": max_bytes, "max_tokens": max_tokens}
        return answer(composer.repository_overview(overview_input(budget=budget), PRINCIPAL))

    exact = page(pair)
    assert ids(exact) == ids(full)[:2] and exact.truncated and exact.next_cursor is not None
    assert ids(page(pair - 1)) == ids(full)[:1]
    assert ids(page(400_000, -(-pair // 4))) == ids(full)[:2]
    assert ids(page(400_000, -(-pair // 4) - 1)) == ids(full)[:1]  # ceil(bytes / 4) tokens
    with pytest.raises(ContextBudgetTooSmall):
        page(50)


def test_candidates_beyond_the_ceiling_are_dropped_and_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = seeded()
    everything = answer(world.composer().repository_overview(overview_input(), PRINCIPAL))
    monkeypatch.setattr(composer_module, "MAX_CANDIDATES", 3)
    capped = answer(world.composer().repository_overview(overview_input(), PRINCIPAL))

    assert ids(capped) == ids(everything)[:3]
    assert "candidates_capped" in capped.warnings
    assert capped.truncated is False and capped.next_cursor is None  # nothing ranked remains


def test_the_same_request_gives_the_same_package_but_for_its_identity() -> None:
    world = seeded()
    composer = world.composer()
    first, second = (
        answer(composer.context_for_task(task_input("rounding drift in calc add"), PRINCIPAL))
        for _ in range(2)
    )
    assert first.query_id != second.query_id
    assert first.model_dump(exclude={"query_id"}) == second.model_dump(exclude={"query_id"})


def test_a_ledger_failure_mid_request_is_unavailable_and_carries_no_statement() -> None:
    world = seeded()

    async def failing(*_: object, **__: object) -> Any:
        raise OperationalError(
            "SELECT * FROM ledger.events WHERE task = %(task)s",
            {"task": "secret words"},
            Exception("server closed the connection unexpectedly"),
        )

    world.ledger.events = failing  # type: ignore[method-assign]
    with pytest.raises(ContextUnavailable) as raised:
        answer(world.composer().context_for_task(task_input("secret words"), PRINCIPAL))
    assert "SELECT" not in str(raised.value) and "secret" not in str(raised.value)
    assert raised.value.__cause__ is None and raised.value.__suppress_context__
    assert world.search.calls  # it failed after the search, in the evidence read
