"""Temporal read validation and the pure classification of decisions, without a database."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from neo4j.exceptions import ClientError, ConfigurationError, Neo4jError

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
from agent_context_platform.settings import Neo4jSettings

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
        temporal._OBSERVATIONS,
        temporal._CI_RUNS,
        temporal._TEST_RUNS,
        temporal._NEXT_PASS,
    ]
    for statement in statements:
        assert "$project_id IS NULL OR" in statement
        assert "$repository_id IS NULL OR" in statement
        assert not re.search(r"\{[a-z_]+\}", statement)
        assert "recorded_at <= $recorded_at" in statement
    assert "LIMIT $limit" in temporal._VERSIONS and "LIMIT $limit" in temporal._OBSERVATIONS
    for guard in (temporal._UNPROJECTED_DECISIONS, temporal._UNPROJECTED_FAILURES):
        assert "LIMIT 1" in guard and "$" not in guard


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


# --- the read path, against a scripted transaction (no database) ---


class _ServerError(ClientError):
    """A server failure with a chosen Neo4j status code."""

    def __init__(self, status: str) -> None:
        super().__init__("server said no")
        self._status = status

    @property
    def code(self) -> str:
        return self._status


class _Tx:
    def __init__(self, script: Callable[[str, dict[str, Any]], list[dict[str, Any]]]) -> None:
        self.script = script
        self.stale: list[dict[str, Any]] = []
        self.calls: list[dict[str, Any]] = []

    async def run(self, query: str, parameters: dict[str, Any]) -> Any:
        self.calls.append(parameters)
        if query in (temporal._UNPROJECTED_DECISIONS, temporal._UNPROJECTED_FAILURES):
            records: list[dict[str, Any]] = self.stale
        else:
            records = self.script(query, parameters)

        class Result:
            async def to_eager_result(self) -> Any:
                return SimpleNamespace(records=records)

        return Result()


class _Driver:
    def __init__(self, tx: _Tx) -> None:
        self.tx = tx

    def session(self, **_kwargs: Any) -> _Driver:
        return self

    async def __aenter__(self) -> _Driver:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute_read(self, work: Callable[[Any], Awaitable[Any]]) -> Any:
        return await work(self.tx)


def _service(script: Callable[[str, dict[str, Any]], list[dict[str, Any]]]) -> TemporalService:
    return TemporalService(_Driver(_Tx(script)), database="neo4j", clock=lambda: at(30))  # type: ignore[arg-type]


def stamp(days: float) -> str:
    return f"{at(days):%Y-%m-%dT%H:%M:%S.%fZ}"


def version_row(
    decision_id: str, event: int, *, status: str = "accepted", valid_from: float = 0, **more: Any
) -> dict[str, Any]:
    return {
        "event_id": f"ev{event}",
        "decision_id": decision_id,
        "status": status,
        "subjects": ["mod_a"],
        "valid_from": stamp(valid_from),
        "valid_to": None,
        "supersedes_id": more.get("supersedes"),
        "recorded_at": stamp(event),
        "state_order": f"{event:04d}",
    }


def test_decisions_reads_versions_for_the_scope_and_defaults_to_now() -> None:
    rows = [
        version_row("a", 1),
        version_row("b", 2, valid_from=10, supersedes="a"),
        version_row("a", 3, status="accepted"),
    ]
    service = _service(lambda _q, _p: rows)
    result = asyncio.run(service.decisions(TemporalScope("p"), include_history=True))
    assert result.valid_at == result.recorded_at == at(30)
    assert [(d.decision_id, d.standing.value) for d in result.decisions] == [
        ("b", "active"),
        ("a", "historical"),
    ]
    assert result.decisions[1].source_event_ids == ("ev1", "ev3")
    tx = service._driver.tx  # type: ignore[attr-defined]
    assert tx.calls[-1]["project_id"] == "p" and tx.calls[-1]["repository_id"] is None
    assert (
        tx.calls[-1]["recorded_at"] == stamp(30)
        and tx.calls[-1]["limit"] == temporal.MAX_VERSIONS + 1
    )


def test_decisions_flag_a_version_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(temporal, "MAX_VERSIONS", 1)
    service = _service(lambda _q, _p: [version_row("a", 1), version_row("b", 2)])
    assert asyncio.run(service.decisions(TemporalScope("p"))).truncated


def test_decision_history_orders_older_links_self_then_newer_links() -> None:
    rows = [
        version_row("z", 1),
        version_row("a", 2, supersedes="z", valid_from=3),
        version_row("b", 3, supersedes="a", valid_from=6),
        version_row("b", 4, supersedes="a", valid_from=7),
        version_row("c", 5, supersedes="b", valid_from=9),
        version_row("x", 6, supersedes="b", status="rejected", valid_from=9),
        version_row("q", 7),
    ]
    history = asyncio.run(_service(lambda _q, _p: rows).decision_history("b", TemporalScope("p")))
    assert [(e.decision_id, e.relation.value, e.distance) for e in history.entries] == [
        ("z", "earlier", 2),
        ("a", "earlier", 1),
        ("b", "self", 0),
        ("c", "later", 1),
    ]
    middle = history.entries[2]
    assert [v.event_id for v in middle.versions] == ["ev3", "ev4"]
    assert middle.source_event_ids == ("ev3", "ev4") and middle.valid_from == at(7)
    assert (middle.effective_valid_to, middle.superseded_at) == (at(9), at(9))
    assert history.entries[1].effective_valid_to == at(7) and not history.truncated


def test_decision_history_flags_caps_and_missing_decisions(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(temporal.TemporalDecisionNotFound):
        asyncio.run(_service(lambda _q, _p: []).decision_history("b", TemporalScope("p")))
    chain = [
        version_row("d0", 1),
        *[version_row(f"d{n}", n + 1, supersedes=f"d{n - 1}", valid_from=n) for n in range(1, 4)],
    ]
    script = _service(lambda _q, _p: chain)
    monkeypatch.setattr(temporal, "MAX_DEPTH", 2)
    deep = asyncio.run(script.decision_history("d0", TemporalScope("p")))
    assert deep.truncated and [e.decision_id for e in deep.entries] == ["d0", "d1", "d2"]
    monkeypatch.setattr(temporal, "MAX_DEPTH", 50)
    monkeypatch.setattr(temporal, "MAX_CHAIN", 1)
    wide = asyncio.run(script.decision_history("d0", TemporalScope("p")))
    assert wide.truncated and [e.decision_id for e in wide.entries] == ["d0", "d1"]
    monkeypatch.setattr(temporal, "MAX_CHAIN", 100)
    monkeypatch.setattr(temporal, "MAX_ENTRY_VERSIONS", 1)
    many = _service(lambda _q, _p: [version_row("a", 1), version_row("a", 2)])
    shown = asyncio.run(many.decision_history("a", TemporalScope("p")))
    assert shown.truncated and [v.event_id for v in shown.entries[0].versions] == ["ev2"]


def test_a_graph_without_history_nodes_is_refused_until_rebuilt() -> None:
    tx = _Tx(lambda _q, _p: [version_row("a", 1)])
    tx.stale = [{"id": "old"}]
    service = TemporalService(_Driver(tx), database="neo4j", clock=lambda: at(30))  # type: ignore[arg-type]
    for call in (
        lambda: service.decisions(TemporalScope("p")),
        lambda: service.decision_history("a", TemporalScope("p")),
        lambda: service.failures(TemporalScope("p")),
    ):
        with pytest.raises(temporal.TemporalProjectionOutdated, match="rebuild"):
            asyncio.run(call())
    tx.stale = []
    assert asyncio.run(service.decisions(TemporalScope("p"))).decisions
    tx.stale = [{"id": "again"}]  # a healthy graph is checked once per instance
    assert asyncio.run(service.decisions(TemporalScope("p"))).decisions


def observation(
    failure_id: str, event: int, *, ci: str | None = None, test: str | None = None, **more: Any
) -> dict[str, Any]:
    return {
        "event_id": f"evo{event}",
        "failure_id": failure_id,
        "component": "api",
        "operation": "ingest",
        "error_class": "Timeout",
        "fingerprint_sha256": "e" * 64,
        "valid_from": stamp(event),
        "recorded_at": stamp(event + 1),
        "session_id": more.get("session"),
        "test_run_id": test,
        "ci_run_id": ci,
    }


def ci_row(run_id: str, **more: Any) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "status": "failure",
        "workflow": "ci",
        "job": "test",
        "completed_at": stamp(1),
        "commit_id": more.get("commit"),
        "snapshot_id": None,
        "result_order": f"t|evr_{run_id}",
    }


def pass_row(failing: str, passing: str | None, **more: Any) -> dict[str, Any]:
    return {
        "failing_id": failing,
        "workflow": "ci",
        "job": "test",
        "failing_completed_at": stamp(1),
        "failing_commit_id": more.get("failing_commit"),
        "failing_order": "t|evr1",
        "passing_id": passing,
        "passing_completed_at": None if passing is None else stamp(5),
        "commit_id": more.get("commit"),
        "snapshot_id": more.get("snapshot"),
        "passing_order": "t|evp1",
    }


def test_failures_classify_each_resolution_and_hide_resolved_by_default() -> None:
    observations = [
        observation("f_fixed", 1, ci="ci_1"),
        observation("f_fixed", 2, ci="ci_1", session="sess_1"),
        observation("f_open", 3, ci="ci_2"),
        observation("f_test", 4, test="tr_1"),
        observation("f_session", 5, session="sess_1"),
        observation("f_bare", 6, ci="ci_gone"),  # its run is not visible at the cut
        observation("f_snap", 7, ci="ci_3"),
    ]
    runs = [ci_row("ci_1", commit="a" * 40), ci_row("ci_2"), ci_row("ci_3")]
    tests = [{**ci_row("tr_1"), "workflow": None, "job": None}]
    passes = [
        pass_row("ci_1", "ci_9", commit="b" * 40, failing_commit="a" * 40),
        pass_row("ci_2", None),
        pass_row("ci_3", "ci_8", snapshot="snap_1"),
    ]

    def script(query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if query is temporal._OBSERVATIONS:
            return observations
        if query is temporal._CI_RUNS:
            assert params["run_ids"] == ["ci_1", "ci_2", "ci_3", "ci_gone"]
            return runs
        if query is temporal._TEST_RUNS:
            return tests
        assert params["run_ids"] == ["ci_1", "ci_2", "ci_3"]
        return passes

    service = _service(script)
    scope = TemporalScope(repository_id="r")
    shown = asyncio.run(service.failures(scope, include_resolved=True)).failures
    by_id = {f.failure_id: f for f in shown}
    assert {name: f.resolution.status.value for name, f in by_id.items()} == {
        "f_fixed": "resolved",
        "f_open": "unresolved",
        "f_test": "unsupported",
        "f_session": "unknown",
        "f_bare": "unknown",
        "f_snap": "resolved",
    }
    path = by_id["f_fixed"].resolution.path
    assert path is not None
    assert (path.failing_run_id, path.passing_run_id, path.commit_changed) == ("ci_1", "ci_9", True)
    assert path.source_event_ids == ("evp1", "evr1") and path.validated_snapshot_id is None
    snap = by_id["f_snap"].resolution.path
    assert (
        snap is not None and snap.commit_changed is None and snap.validated_snapshot_id == "snap_1"
    )
    fixed = by_id["f_fixed"]
    assert [(r.kind.value, r.run_id) for r in fixed.observed_in] == [
        ("ci_run", "ci_1"),
        ("session", "sess_1"),
    ]
    assert fixed.observed_in[0].source_event_ids == ("evr_ci_1",)
    assert fixed.source_event_ids == ("evo1", "evo2")  # every visible observation
    assert (fixed.valid_from, fixed.recorded_at) == (at(1), at(2))
    # a failure seen in no visible run still carries the events that observed it
    assert by_id["f_bare"].observed_in == () and by_id["f_bare"].source_event_ids == ("evo6",)
    default = asyncio.run(service.failures(scope)).failures
    assert [f.failure_id for f in default] == ["f_open", "f_test", "f_session", "f_bare"]


def test_failures_flag_the_failure_and_observation_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [observation("f1", 1), observation("f2", 2), observation("f2", 3)]
    service = _service(lambda _q, _p: rows)
    monkeypatch.setattr(temporal, "MAX_FAILURES", 1)
    result = asyncio.run(service.failures(TemporalScope("p")))
    assert result.truncated and [f.failure_id for f in result.failures] == ["f1"]
    monkeypatch.setattr(temporal, "MAX_FAILURES", 500)
    monkeypatch.setattr(temporal, "MAX_OBSERVATIONS", 2)
    cut = asyncio.run(service.failures(TemporalScope("p")))
    assert cut.truncated and [f.failure_id for f in cut.failures] == ["f1", "f2"]


def test_from_settings_needs_complete_connection_settings() -> None:
    async def run() -> None:
        with pytest.raises(ConfigurationError):
            TemporalService.from_settings(Neo4jSettings())
        complete = Neo4jSettings.model_validate(
            {"uri": "bolt://127.0.0.1:1", "username": "u", "password": "p"}
        )
        async with TemporalService.from_settings(complete):  # opens no connection
            pass

    asyncio.run(run())


def test_a_database_side_timeout_is_a_deadline_error() -> None:
    class Slow(_Driver):
        async def execute_read(self, work: Callable[[Any], Awaitable[Any]]) -> Any:
            raise _ServerError("Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration")

    service = TemporalService(Slow(_Tx(lambda _q, _p: [])), database="neo4j")  # type: ignore[arg-type]
    with pytest.raises(temporal.RetrievalDeadlineExceeded):
        asyncio.run(service.decisions(TemporalScope("p")))

    class Broken(_Driver):
        async def execute_read(self, work: Callable[[Any], Awaitable[Any]]) -> Any:
            raise _ServerError("Neo.ClientError.Statement.SyntaxError")

    service = TemporalService(Broken(_Tx(lambda _q, _p: [])), database="neo4j")  # type: ignore[arg-type]
    with pytest.raises(Neo4jError):
        asyncio.run(service.decisions(TemporalScope("p")))


def test_a_non_positive_per_call_deadline_is_rejected() -> None:
    service = _service(lambda _q, _p: [])
    with pytest.raises(TemporalRequestError, match="deadline"):
        asyncio.run(service.decisions(TemporalScope("p"), deadline_seconds=0))


def test_an_undated_superseded_decision_is_history_only_once_it_was_in_force() -> None:
    rows = [version("f", 1, status="superseded", valid_from=10)]
    early, _ = temporal._classify(rows, at(5), at(50), True)
    assert early == ()
    later, _ = temporal._classify(rows, at(10), at(50), True)
    assert [(r.decision_id, r.reason) for r in later] == [
        ("f", HistoricalReason.SUPERSEDED_UNDATED)
    ]
