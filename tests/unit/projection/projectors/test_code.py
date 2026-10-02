"""The pure derivation behind the temporal code projector: no graph, no services."""

from __future__ import annotations

from dataclasses import replace

import pytest

from agent_context_platform.projection.projectors.code import (
    AssertionFact,
    Facts,
    Member,
    Run,
    derive_assertion,
    derive_file,
    derive_symbol,
    interval_at,
    is_current,
    resolve_edges,
)

pytestmark = pytest.mark.unit


def run(n: int, *, error_class: str | None = None, completed: bool = True) -> Run:
    return Run(
        f"idx{n}|t{n}",
        f"2026-03-01T12:0{n}:00.000000Z",
        f"2026-03-01T12:0{n}:00.000000Z",
        completed,
        error_class,
    )


def facts(runs: list[Run], members: dict[int, str | None], complete: dict[int, bool]) -> Facts:
    """One file `f` with symbol `x` and a call assertion `a` from `f2` to `x`.

    `members[n]` is the revision of `f` in run n (None: absent); `complete[n]` its coverage.
    """
    result = Facts(runs=runs)
    for n, revision in members.items():
        key = run(n).key
        if revision is not None:
            result.members[key] = {"f": Member(revision, "p.py", "m")}
            result.coverage[key] = {"f": {revision: complete.get(n, True)}}
        result.revision_file["r1"] = result.revision_file["r2"] = "f"
    result.defs["x"] = {"r1": "x-rev"}
    result.since[("x", "r1")] = run(1).occurred_at
    return result


def test_a_missing_file_closes_validity_only_when_the_run_may_infer_absence() -> None:
    runs = [run(1), run(2, error_class="scan_incomplete"), run(3, completed=False), run(4)]
    state = facts(runs, {1: "r1", 2: None, 3: None, 4: None}, {})
    intervals = derive_file(state, "f")
    assert [(i.key, i.start.key, i.end and i.end.key) for i in intervals] == [
        ("r1", "idx1|t1", "idx4|t4")  # only the complete, successful-outcome run closes it
    ]
    assert [(i.key, i.end and i.end.key) for i in derive_symbol(state, "x")] == [
        ("x-rev", "idx4|t4")
    ]


def test_a_replaced_file_revision_closes_its_symbols_only_with_complete_coverage() -> None:
    runs = [run(1), run(2), run(3)]
    state = facts(runs, {1: "r1", 2: "r2", 3: "r2"}, {2: False, 3: True})
    (symbol,) = derive_symbol(state, "x")
    assert symbol.end is not None and symbol.end.key == "idx3|t3"  # not run 2: it is degraded


def fact(
    predicate: str = "CALLS",
    *,
    valid_from: str | None = None,
    valid_to: str | None = None,
    revisions: dict[str, str] | None = None,
) -> AssertionFact:
    return AssertionFact(
        family="relation",
        subject_id="s",
        predicate=predicate,
        object_id="x",
        valid_from=valid_from or run(1).occurred_at,
        valid_to=valid_to,
        observed_at=run(1).observed_at,
        revisions={} if revisions is None else revisions,
    )


def test_a_relation_is_stale_when_its_target_symbol_is_not_in_the_membership() -> None:
    runs = [run(1), run(2)]
    state = facts(runs, {1: "r1", 2: "r1"}, {})
    state.members["idx2|t2"]["g"] = Member("g1", "q.py", "m")
    state.revision_file["g1"] = "g"
    state.assertions["a"] = fact(revisions={"g1": run(2).occurred_at})
    assert [(i.start.key, i.end) for i in derive_assertion(state, "a")] == [("idx2|t2", None)]
    state.defs["x"] = {}  # the target is gone from the membership: the source's edge is stale
    assert derive_assertion(state, "a") == []


def test_as_of_picks_the_half_open_interval() -> None:
    state = facts([run(1), run(2)], {1: "r1", 2: None}, {})
    intervals = derive_file(state, "f")
    found = interval_at(intervals, run(1).occurred_at)
    assert found is not None and found.key == "r1"
    assert interval_at(intervals, run(2).occurred_at) is None
    assert interval_at(intervals, "2026-03-01T11:00:00.000000Z") is None


def _relation_state() -> Facts:
    """Source file `g` (unchanged, revision g1) calls symbol `x`, defined in file `f`."""
    state = facts([run(1), run(2)], {1: "r1", 2: "r2"}, {2: False})
    for n in (1, 2):
        state.members[run(n).key]["g"] = Member("g1", "q.py", "m")
        state.coverage[run(n).key]["g"] = {"g1": True}
    state.revision_file["g1"] = "g"
    state.assertions["a"] = fact(revisions={"g1": run(1).occurred_at})
    return state


def test_a_relation_stays_current_while_its_target_is_unknown() -> None:
    # the target file got a new revision with degraded coverage: not an explicit absence
    state = _relation_state()
    assert [(i.start.key, i.end) for i in derive_assertion(state, "a")] == [("idx1|t1", None)]
    state.coverage[run(2).key]["f"]["r2"] = True  # complete and without x: now it is stale
    (item,) = derive_assertion(state, "a")
    assert item.end is not None and item.end.key == "idx2|t2"


def test_the_declared_valid_to_clips_every_interval() -> None:
    clip = "2026-03-01T12:01:30.000000Z"
    state = _relation_state()
    state.assertions["a"] = fact(valid_to=clip, revisions={"g1": run(1).occurred_at})
    (item,) = derive_assertion(state, "a")
    assert item.end is not None and item.end.occurred_at == clip
    assert interval_at([item], "2026-03-01T12:01:20.000000Z") is item
    assert interval_at([item], clip) is None
    state.assertions["super"] = fact("POSSIBLY_SUPERSEDES", valid_to=clip)
    (closed,) = derive_assertion(state, "super")
    assert closed.end is not None  # a supersession does not stay open forever
    state.assertions["early"] = fact("POSSIBLY_SUPERSEDES", valid_to=run(1).occurred_at)
    assert derive_assertion(state, "early") == []  # declared closed before it opened


def test_a_declared_valid_from_later_than_the_first_run_clamps_the_start() -> None:
    later = "2026-03-01T12:01:30.000000Z"
    state = _relation_state()
    state.assertions["a"] = fact(valid_from=later, revisions={"g1": run(1).occurred_at})
    (item,) = derive_assertion(state, "a")
    assert item.start.occurred_at == later and item.end is None
    assert interval_at([item], run(1).occurred_at) is None


def test_current_means_valid_at_the_latest_run_not_unbounded() -> None:
    state = _relation_state()
    future = "2026-03-01T12:30:00.000000Z"
    state.assertions["a"] = fact(valid_to=future, revisions={"g1": run(1).occurred_at})
    intervals = derive_assertion(state, "a")
    assert intervals[-1].end is not None and is_current(intervals, state.horizon)
    state.assertions["a"] = fact(
        valid_to="2026-03-01T12:01:30.000000Z", revisions={"g1": run(1).occurred_at}
    )
    assert not is_current(derive_assertion(state, "a"), state.horizon)


def test_recorded_times_come_from_the_evidence_not_the_run() -> None:
    # run 2 started at 12:02 (the run's own observed_at) but its completion and coverage were
    # observed at 12:05: the closure of x is recorded at 12:05
    late = "2026-03-01T12:05:00.000000Z"
    runs = [run(1), replace(run(2), outcome_observed_at=late)]
    state = facts(runs, {1: "r1", 2: "r2"}, {})
    state.members[runs[0].key]["f"] = Member("r1", "p.py", "m", "2026-03-01T12:01:10.000000Z")
    state.members[runs[1].key]["f"] = Member("r2", "p.py", "m", "2026-03-01T12:02:10.000000Z")
    state.coverage_observed[(runs[1].key, "r2")] = late
    (item,) = derive_symbol(state, "x")
    assert item.recorded_from == "2026-03-01T12:01:10.000000Z"
    assert item.recorded_to == late


def _current(
    aid: str, subject: str, predicate: str, obj: str, kind: str, confidence: float
) -> dict:  # type: ignore[type-arg]
    return {
        "assertion_id": aid,
        "subject_id": subject,
        "predicate": predicate,
        "object_id": obj,
        "evidence_kind": kind,
        "confidence": confidence,
        "deterministic": kind == "scip",
        "extractor_name": "x",
        "extractor_version": "1",
        "valid_from": "t",
        "review_status": "unreviewed",
    }


def test_evidence_competes_within_a_triple_not_across_objects() -> None:
    edges = resolve_edges(
        [
            _current("a1", "f", "CALLS", "g", "scip", 1.0),
            _current("a2", "f", "CALLS", "h", "tree_sitter", 0.5),
            _current("a3", "f", "CALLS", "g", "tree_sitter", 0.5),  # lower evidence, same triple
            _current("d1", "file", "DEFINES", "s1", "tree_sitter", 0.9),
            _current("d2", "file", "DEFINES", "s2", "tree_sitter", 0.9),
            _current("i1", "file", "IMPORTS", "m", "tree_sitter", 0.9),
        ]
    )
    assert sorted(edges) == [
        ("f", "CALLS", "g"),
        ("f", "CALLS", "h"),
        ("file", "DEFINES", "s1"),
        ("file", "DEFINES", "s2"),
        ("file", "IMPORTS", "m"),
    ]
    assert all(edge["resolved"] for edge in edges.values())
    call = edges[("f", "CALLS", "g")]
    assert (call["evidence_kind"], call["resolved_assertion_id"]) == ("scip", "a1")
    assert call["lower_evidence_assertion_ids"] == ["a3"] and call["assertion_ids"] == ["a1", "a3"]
    assert edges[("f", "CALLS", "h")]["evidence_kind"] == "tree_sitter"


def test_a_rename_closes_items_whose_identity_spells_the_old_path() -> None:
    runs = [run(1), run(2)]
    state = Facts(runs=runs)
    state.members[runs[0].key] = {"f": Member("r1", "pkg/c.py", "m")}
    state.members[runs[1].key] = {"f": Member("r1", "pkg/d.py", "m")}  # same revision, new path
    state.coverage = {r.key: {"f": {"r1": True}} for r in runs}
    state.revision_file["r1"] = "f"
    state.claimed["r1"] = {runs[0].key, runs[1].key}  # claimed anew under the new path
    items = {
        "named": None,  # no SCIP symbol: named from the module path
        "scip-python": "scip-python python p 0 `pkg.c`/k().",
        "scip-typescript": "scip-typescript npm p 0 src/`pkg/c`/k().",
        "go": "gomod example.com/m 0 example.com/m/pkg/K().",  # no file descriptor
    }
    for item, scip in items.items():
        state.defs[item] = {"r1": f"{item}-rev"}
        state.since[(item, "r1")] = runs[0].occurred_at
        state.claim_run[(item, "r1")] = runs[0].key
        if scip is None:
            state.path_bound.add(item)
        else:
            state.scips[item] = {scip}
    ends = {item: derive_symbol(state, item)[0].end for item in items}
    assert {item: end is not None for item, end in ends.items()} == {
        "named": True,
        "scip-python": True,
        "scip-typescript": True,
        "go": False,  # a package-path symbol is not changed by a rename within its package
    }


def test_an_assertion_that_starts_after_the_latest_run_is_not_current() -> None:
    state = _relation_state()
    after = "2026-03-01T12:30:00.000000Z"
    state.assertions["a"] = fact(valid_from=after, revisions={"g1": run(1).occurred_at})
    intervals = derive_assertion(state, "a")
    assert intervals and not is_current(intervals, state.horizon)


def test_the_edge_starts_where_the_assertion_became_live() -> None:
    declared = _current("a1", "f", "CALLS", "g", "scip", 1.0)
    declared |= {
        "valid_from": "t1",
        "recorded_from": "r1",
        "current_from": "t2",
        "current_recorded_from": "r2",
    }
    (edge,) = resolve_edges([declared]).values()
    assert (edge["valid_from"], edge["recorded_from"]) == ("t2", "r2")
