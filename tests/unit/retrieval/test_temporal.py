"""Temporal read validation and the pure classification of decisions, without a database."""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from agent_context_platform.projection.projectors.knowledge import (
    DecisionVersion,
    active_decisions,
    decision_states,
)
from agent_context_platform.retrieval import temporal
from agent_context_platform.retrieval.temporal import (
    DecisionStanding,
    HistoricalReason,
    TemporalRequestError,
    TemporalScope,
    TemporalService,
)

pytestmark = pytest.mark.unit

T0 = datetime(2026, 8, 13, 13, 0, 0, tzinfo=UTC)


def at(days: float) -> datetime:
    return T0 + timedelta(days=days)


def version(
    decision_id: str,
    order: int,
    *,
    status: str = "accepted",
    valid_from: float = 0,
    valid_to: float | None = None,
    supersedes: str | None = None,
    recorded: float | None = None,
) -> temporal._VersionRow:
    return temporal._VersionRow(
        DecisionVersion(
            decision_id=decision_id,
            subjects=("scope",),
            status=status,
            valid_from=at(valid_from),
            valid_to=None if valid_to is None else at(valid_to),
            recorded_at=at(order if recorded is None else recorded),
            supersedes_id=supersedes,
            order=f"{order:04d}",
        ),
        f"event-{decision_id}-{order}",
        ("mod_a",),
    )


def test_a_scope_needs_one_non_blank_identifier() -> None:
    with pytest.raises(TemporalRequestError):
        TemporalScope()
    with pytest.raises(TemporalRequestError):
        TemporalScope(project_id="")
    with pytest.raises(TemporalRequestError):
        TemporalScope(repository_id="a", project_id=" ")
    assert TemporalScope(repository_id="r").parameters() == {
        "project_id": None,
        "repository_id": "r",
    }


def test_the_service_rejects_a_non_positive_default_deadline() -> None:
    with pytest.raises(TemporalRequestError):
        TemporalService(object(), database="neo4j", default_deadline_seconds=0)  # type: ignore[arg-type]


def test_a_naive_time_and_an_empty_decision_id_are_rejected_before_a_transaction() -> None:
    class NoDriver:
        def session(self, **_kwargs: Any) -> Any:
            raise AssertionError("no transaction may open")

    service = TemporalService(NoDriver(), database="neo4j")  # type: ignore[arg-type]

    async def run() -> None:
        with pytest.raises(TemporalRequestError, match="valid_at"):
            await service.decisions(TemporalScope("p"), datetime(2026, 1, 1))
        with pytest.raises(TemporalRequestError, match="recorded_at"):
            await service.failures(TemporalScope("p"), recorded_at=datetime(2026, 1, 1))
        with pytest.raises(TemporalRequestError):
            await service.decision_history(" ", TemporalScope("p"))

    asyncio.run(run())


def test_every_cypher_statement_is_static_and_scoped() -> None:
    statements = [
        temporal._VERSIONS,
        temporal._EARLIER,
        temporal._LATER,
        temporal._FAILURES,
        temporal._NEXT_PASS,
    ]
    for statement in statements:
        assert "$project_id IS NULL OR" in statement
        assert "$repository_id IS NULL OR" in statement
        assert not re.search(r"\{[a-z_]+\}", statement)
    assert "LIMIT $limit" in temporal._VERSIONS and "LIMIT $limit" in temporal._FAILURES
    assert "LIMIT $limit" in temporal._EARLIER and "LIMIT $limit" in temporal._LATER


def test_classification_agrees_with_active_decisions_for_every_probe() -> None:
    rows = [
        version("a", 1),
        version("b", 2, supersedes="a", valid_from=10),
        version("b", 3, supersedes="a", valid_from=10, status="rejected"),
        version("c", 4, valid_from=2, valid_to=12),
        version("d", 5, supersedes="c", valid_from=8),
        version("e", 6, status="proposed"),
        version("f", 7, status="superseded"),
    ]
    versions = [row.version for row in rows]
    for valid in (-1, 0, 5, 9, 10, 12, 15):
        for recorded in (0.5, 2.5, 3.5, 6.5, 50):
            expected = active_decisions(
                versions, scope="scope", valid_at=at(valid), recorded_at=at(recorded)
            )
            records, truncated = temporal._classify(rows, at(valid), at(recorded), True)
            assert not truncated
            found = [r.decision_id for r in records if r.standing is DecisionStanding.ACTIVE]
            assert found == expected, (valid, recorded)
            for record in records:
                if record.standing is DecisionStanding.ACTIVE:
                    assert record.reason is None and record.superseded_by is None
                else:
                    assert record.reason is not None
            plain, _ = temporal._classify(rows, at(valid), at(recorded), False)
            assert [r.decision_id for r in plain] == expected


def test_historical_reasons_name_the_superseder_or_the_expiry() -> None:
    rows = [
        version("a", 1),
        version("b", 2, supersedes="a", valid_from=10),
        version("c", 3, valid_to=5),
        version("d", 4, valid_to=10, supersedes=None),
        version("e", 5, supersedes="d", valid_from=10),
        version("f", 6, status="superseded"),
    ]
    records, _ = temporal._classify(rows, at(20), at(50), True)
    reasons = {r.decision_id: (r.reason, r.superseded_by) for r in records}
    assert reasons == {
        "b": (None, None),
        "e": (None, None),
        "a": (HistoricalReason.SUPERSEDED, "b"),
        "c": (HistoricalReason.EXPIRED, None),
        # closed by its own valid_to and by a superseder at the same instant: superseded wins
        "d": (HistoricalReason.SUPERSEDED, "e"),
        "f": (HistoricalReason.SUPERSEDED_UNDATED, None),
    }
    assert [r.decision_id for r in records][:2] == ["b", "e"]  # active first, then by ID


def test_the_decision_cap_truncates_and_keeps_active_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(temporal, "MAX_DECISIONS", 2)
    rows = [version(name, n) for n, name in enumerate("abc", start=1)]
    records, truncated = temporal._classify(rows, at(5), at(50), False)
    assert truncated and [r.decision_id for r in records] == ["a", "b"]


def test_decision_states_pick_the_earliest_current_superseder() -> None:
    rows = [
        version("a", 1),
        version("z", 2, supersedes="a", valid_from=9),
        version("b", 3, supersedes="a", valid_from=9),
        version("c", 4, supersedes="a", valid_from=7),
    ]
    states = decision_states([r.version for r in rows], recorded_at=at(50))
    assert (states["a"].superseded_by, states["a"].closed_at) == ("c", at(7))
    early = decision_states([r.version for r in rows], recorded_at=at(2.5))["a"]
    assert (early.superseded_by, early.end) == ("z", at(9))  # only z is known by then
    states = decision_states([r.version for r in rows[:3]], recorded_at=at(50))
    assert states["a"].superseded_by == "b"  # a tie on valid_from breaks on the decision ID
