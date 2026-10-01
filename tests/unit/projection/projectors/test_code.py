"""The pure derivation behind the temporal code projector: no graph, no services."""

from __future__ import annotations

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


def test_a_relation_is_stale_when_its_target_symbol_is_not_in_the_membership() -> None:
    runs = [run(1), run(2)]
    state = facts(runs, {1: "r1", 2: "r1"}, {})
    state.members["idx2|t2"]["g"] = Member("g1", "q.py", "m")
    state.revision_file["g1"] = "g"
    state.since[("x", "r1")] = run(1).occurred_at
    state.defs["x"] = {"r1": "x-rev"}
    state.assertions["a"] = AssertionFact(
        "relation",
        "s",
        "CALLS",
        "x",
        run(1).occurred_at,
        run(1).observed_at,
        {"g1": run(2).occurred_at},
    )
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
