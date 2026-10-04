"""Unit tests for the knowledge projector: routing, locking and the bi-temporal as-of read."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from agent_context_sdk import (
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content import ContentDisposition

from agent_context_platform.projection.projectors import event_lock_keys
from agent_context_platform.projection.projectors.knowledge import (
    KNOWLEDGE_EVENT_TYPES,
    DecisionVersion,
    KnowledgeProjector,
    active_decisions,
    decision_version,
)

T0 = datetime(2026, 8, 13, 13, 0, 0, tzinfo=UTC)


def _at(days: int) -> datetime:
    return T0 + timedelta(days=days)


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _event(
    event_type: str, payload: dict[str, object], *, number: int = 1, observed: datetime = T0
) -> StoredEventV1:
    draft = EventDraftV1(
        event_id=UUID(f"0198a4b1-98c0-7c28-ae3f-{number:012x}"),
        event_type=event_type,
        stream_id="stream",
        occurred_at=T0,
        observed_at=observed,
        producer=ProducerV1(producer_id="p", name="p", version="1"),
        context=EventContextV1(),
        payload=payload,
        redaction=EventRedactionSummaryV1(
            policy_version="v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"key-{number}",
    )
    return seal_event(draft, [], 1, None)


def _decision(
    decision_id: str,
    *,
    status: str = "accepted",
    supersedes: str | None = None,
    valid_from: int = 0,
    valid_to: int | None = None,
    observed: int = 0,
    subjects: tuple[str, ...] = ("mod_a",),
    number: int = 1,
) -> StoredEventV1:
    return _event(
        "knowledge.decision.recorded",
        {
            "decision_id": decision_id,
            "status": status,
            "supersedes_id": supersedes,
            "subjects": list(subjects),
            "content_id": f"content_{decision_id}",
            "valid_from": _iso(_at(valid_from)),
            "valid_to": None if valid_to is None else _iso(_at(valid_to)),
            "recorded_at": _iso(_at(observed)),
        },
        number=number,
        observed=_at(observed),
    )


def test_projector_handles_exactly_the_knowledge_event_types() -> None:
    projector = KnowledgeProjector()
    assert {
        "knowledge.decision.recorded",
        "knowledge.constraint.recorded",
        "knowledge.failure.observed",
        "knowledge.summary.recorded",
    } == KNOWLEDGE_EVENT_TYPES
    assert all(projector.handles(item) for item in KNOWLEDGE_EVENT_TYPES)
    assert not projector.handles("quality.finding.observed")
    assert not projector.handles("git.commit.observed")


def test_a_superseding_decision_locks_the_superseded_decision() -> None:
    event = _decision("dec_b", supersedes="dec_a")
    assert event_lock_keys(event) == [
        ("Decision", "dec_a"),
        ("Decision", "dec_b"),
        ("DecisionVersion", str(event.event_id)),
    ]
    proposed = _decision("dec_b", status="proposed", supersedes="dec_a")
    assert event_lock_keys(proposed) == [
        ("Decision", "dec_b"),
        ("DecisionVersion", str(proposed.event_id)),
    ]


def test_a_failure_locks_every_node_it_links_to() -> None:
    event = _event(
        "knowledge.failure.observed",
        {
            "failure_id": "fail_1",
            "component": "api",
            "operation": "ingest",
            "error_class": "Timeout",
            "fingerprint_version": "1",
            "fingerprint_sha256": "e" * 64,
            "session_id": "sess_1",
            "turn_id": "turn_1",
            "test_run_id": "tr_1",
            "ci_run_id": "ci_1",
        },
    )
    assert event_lock_keys(event) == [
        ("CIRun", "ci_1"),
        ("Failure", "fail_1"),
        ("Session", "sess_1"),
        ("TestRun", "tr_1"),
    ]


def test_projecting_a_decision_writes_content_ids_and_no_text() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.statements: list[dict[str, Any]] = []

        async def run(self, query: str, *, parameters: dict[str, Any]) -> SimpleNamespace:
            self.statements.append(parameters)
            return SimpleNamespace(records=[])

    tx = Recorder()
    asyncio.run(KnowledgeProjector().project(tx, _decision("dec_a")))  # type: ignore[arg-type]
    written = [item for item in tx.statements if "content_id" in item]
    assert [item["content_id"] for item in written] == ["content_dec_a"]


def _versions() -> list[DecisionVersion]:
    """A (valid day 0) is superseded by B (valid from day 10), recorded on day 12."""
    return [
        decision_version(_decision("dec_a", number=1, observed=1)),
        decision_version(_decision("dec_b", supersedes="dec_a", valid_from=10, observed=12)),
    ]


@pytest.mark.parametrize(
    ("valid_day", "recorded_day", "expected"),
    [
        (5, 20, ["dec_a"]),  # B is valid later, A still holds
        (10, 20, ["dec_b"]),  # A closes at B's valid_from; B opens at the same instant
        (15, 20, ["dec_b"]),
        (15, 11, ["dec_a"]),  # B was not yet recorded: only A is known, with no end
        (-1, 20, []),  # before either decision holds
        (5, 0, []),  # nothing recorded yet
    ],
)
def test_as_of_answers_across_valid_and_recorded_time(
    valid_day: int, recorded_day: int, expected: list[str]
) -> None:
    assert (
        active_decisions(
            _versions(), scope="mod_a", valid_at=_at(valid_day), recorded_at=_at(recorded_day)
        )
        == expected
    )


def test_as_of_is_independent_of_the_order_the_versions_are_given_in() -> None:
    versions = _versions()
    forward = active_decisions(versions, scope="mod_a", valid_at=_at(12), recorded_at=_at(20))
    backward = active_decisions(
        reversed(versions), scope="mod_a", valid_at=_at(12), recorded_at=_at(20)
    )
    assert forward == backward == ["dec_b"]


def test_as_of_uses_the_newest_version_known_at_the_recorded_time() -> None:
    versions = [
        decision_version(_decision("dec_a", status="proposed", number=1, observed=1)),
        decision_version(_decision("dec_a", status="accepted", number=2, observed=5)),
        decision_version(_decision("dec_a", status="rejected", number=3, observed=9)),
    ]
    at = _at(3)
    assert active_decisions(versions, scope="mod_a", valid_at=at, recorded_at=_at(2)) == []
    assert active_decisions(versions, scope="mod_a", valid_at=at, recorded_at=_at(6)) == ["dec_a"]
    assert active_decisions(versions, scope="mod_a", valid_at=at, recorded_at=_at(9)) == []


def test_as_of_honours_an_own_valid_to_and_ignores_a_proposed_superseder() -> None:
    versions = [
        decision_version(_decision("dec_a", valid_to=8, observed=1, number=1)),
        decision_version(
            _decision(
                "dec_b", status="proposed", supersedes="dec_a", valid_from=4, observed=2, number=2
            )
        ),
    ]
    kwargs = {"scope": "mod_a", "recorded_at": _at(20)}
    assert active_decisions(versions, valid_at=_at(6), **kwargs) == ["dec_a"]
    assert active_decisions(versions, valid_at=_at(8), **kwargs) == []


def test_as_of_filters_by_scope_and_needs_a_closing_time_for_a_superseded_status() -> None:
    versions = [
        decision_version(_decision("dec_a", subjects=("mod_a", "mod_b"), observed=1)),
        decision_version(_decision("dec_x", status="superseded", subjects=("mod_a",), observed=1)),
    ]
    kwargs = {"valid_at": _at(2), "recorded_at": _at(3)}
    assert active_decisions(versions, scope="mod_b", **kwargs) == ["dec_a"]
    assert active_decisions(versions, scope="mod_c", **kwargs) == []
    assert active_decisions(versions, scope="mod_a", **kwargs) == ["dec_a"]


def test_a_superseder_re_recorded_as_rejected_or_proposed_reopens_the_decision() -> None:
    base = [
        decision_version(_decision("dec_a", observed=1, number=1)),
        decision_version(
            _decision("dec_b", supersedes="dec_a", valid_from=10, observed=2, number=2)
        ),
    ]
    kwargs = {"scope": "mod_a", "valid_at": _at(15), "recorded_at": _at(20)}
    assert active_decisions(base, **kwargs) == ["dec_b"]
    for status in ("rejected", "proposed"):
        again = decision_version(
            _decision(
                "dec_b", status=status, supersedes="dec_a", valid_from=10, observed=5, number=3
            )
        )
        assert active_decisions([*base, again], **kwargs) == ["dec_a"]
        assert active_decisions([again, *base], **kwargs) == ["dec_a"]
        # As known before the re-recording, A was still closed.
        known = active_decisions(
            [*base, again], scope="mod_a", valid_at=_at(15), recorded_at=_at(3)
        )
        assert known == ["dec_b"]


def test_a_superseder_re_pointed_at_another_decision_stops_closing_the_old_one() -> None:
    versions = [
        decision_version(_decision("dec_a", observed=1, number=1)),
        decision_version(_decision("dec_x", observed=1, number=2)),
        decision_version(
            _decision("dec_b", supersedes="dec_a", valid_from=10, observed=2, number=3)
        ),
        decision_version(
            _decision("dec_b", supersedes="dec_x", valid_from=10, observed=3, number=4)
        ),
    ]
    got = active_decisions(versions, scope="mod_a", valid_at=_at(15), recorded_at=_at(20))
    assert got == ["dec_a", "dec_b"]


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, *, parameters: dict[str, Any]) -> SimpleNamespace:
        self.statements.append((query, parameters))
        return SimpleNamespace(records=[])


def _project(event: StoredEventV1) -> list[tuple[str, dict[str, Any]]]:
    tx = _Recorder()
    asyncio.run(KnowledgeProjector().project(tx, event))  # type: ignore[arg-type]
    return tx.statements


def test_a_decision_recording_is_kept_as_a_version_with_the_scope_and_its_event() -> None:
    event = _decision("dec_a", supersedes="dec_z")
    scoped = _event(
        "knowledge.decision.recorded",
        dict(event.payload),
        number=2,
    )
    statements = _project(scoped)
    version = next(p for _, p in statements if "source_event_ids" in p)
    assert version["node_id"] == str(scoped.event_id)
    assert version["source_event_ids"] == [str(scoped.event_id)]
    assert version["decision_id"] == "dec_a" and version["supersedes_id"] == "dec_z"
    assert version["status"] == "accepted" and version["state_order"].endswith(str(scoped.event_id))
    assert "content_id" not in version  # text and content never enter the history node
    assert any("HAS_VERSION" in q for q, _ in statements)
    assert (version["project_id"], version["repository_id"]) == (None, None)


def test_failures_constraints_and_summaries_project_with_recorded_parameters() -> None:
    failure = _event(
        "knowledge.failure.observed",
        {
            "failure_id": "fail_1",
            "component": "api",
            "operation": "ingest",
            "error_class": "Timeout",
            "fingerprint_version": "1",
            "fingerprint_sha256": "e" * 64,
            "session_id": "sess_1",
            "test_run_id": "tr_1",
            "ci_run_id": "ci_1",
        },
    )
    statements = _project(failure)
    written = next(p for _, p in statements if p.get("failure_id") is None and "component" in p)
    assert written["node_id"] == "fail_1" and written["project_id"] is None
    assert sum("OBSERVED_IN" in q for q, _ in statements) == 3
    constraint = _event(
        "knowledge.constraint.recorded",
        {
            "constraint_id": "con_1",
            "subjects": ["mod_a"],
            "content_id": "c",
            "valid_from": _iso(T0),
            "recorded_at": _iso(T0),
        },
    )
    assert any(
        p.get("node_id") == "con_1" and "supersedes_id" in p for _, p in _project(constraint)
    )
    summary = _event(
        "knowledge.summary.recorded",
        {
            "summary_id": "sum_1",
            "source_event_ids": [str(constraint.event_id)],
            "subjects": ["mod_a"],
            "content_id": "c",
            "valid_from": _iso(T0),
            "recorded_at": _iso(T0),
        },
    )
    assert any(
        p.get("grounded_event_ids") == [str(constraint.event_id)] for _, p in _project(summary)
    )
    assert event_lock_keys(constraint) == [("Constraint", "con_1")]
    assert event_lock_keys(summary) == [("Summary", "sum_1")]
