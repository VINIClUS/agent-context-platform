"""Unit tests for graph digest, verification, replay and rebuild (PLATFORM-039).

The Neo4j side runs against an in-memory graph that answers the exact queries the scan issues; the
PostgreSQL side against a router that answers by the SQL it is given. The real SQL and real Neo4j
are proven by `tests/e2e/test_rebuild.py`; these tests pin the logic around them.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from agent_context_sdk import new_uuid7
from agent_context_sdk.events.system import ProjectionMode, ProjectionOutcome, ProjectionRebuiltV1
from sqlalchemy.dialects import postgresql

from agent_context_platform.ledger.service import IngestOutcome
from agent_context_platform.projection import verify
from agent_context_platform.projection.models import ProjectionState
from agent_context_platform.projection.runtime import EventIntegrityError, OrphanedOutboxRowError
from agent_context_platform.projection.verify import (
    NODE_KEYS,
    CheckpointView,
    EventTypeStats,
    GraphDigest,
    ProjectorVerification,
    RecordingError,
    RecordResult,
    ReplayProgress,
    ReplayResult,
    RunRecord,
    StreamHeadCheck,
    VerificationReport,
    classify_projector,
    classify_replay,
    compute_graph_digest,
    node_identity,
    primary_label,
    source_ids,
    system_event_key,
)

from ._doubles import build_stored_event

pytestmark = [pytest.mark.unit, pytest.mark.anyio]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


def clock() -> datetime:
    return NOW


# --------------------------------------------------------------------------------------------
# An in-memory graph answering the scan's queries
# --------------------------------------------------------------------------------------------


class Rec(dict[str, Any]):
    def data(self) -> dict[str, Any]:
        return dict(self)


class Rows:
    def __init__(self, records: list[Rec]) -> None:
        self.records = records


class FakeGraph:
    def __init__(self) -> None:
        self.nodes: dict[int, tuple[list[str], dict[str, Any]]] = {}
        self.rels: list[tuple[int, str, int, dict[str, Any]]] = []
        self._next = 0

    def node(self, *labels: str, **props: Any) -> int:
        self._next += 1
        self.nodes[self._next] = (list(labels), props)
        return self._next

    def rel(self, source: int, kind: str, target: int, **props: Any) -> None:
        self.rels.append((source, kind, target, props))

    def rels_from(self, ids: set[int]) -> list[Rec]:
        return [
            Rec(
                a_labels=self.nodes[a][0],
                a_props=self.nodes[a][1],
                type=kind,
                props=props,
                b_labels=self.nodes[b][0],
                b_props=self.nodes[b][1],
            )
            for a, kind, b, props in self.rels
            if a in ids
        ]


class FakeTx:
    def __init__(self, graph: FakeGraph) -> None:
        self.graph = graph

    async def run(self, query: str, *, parameters: dict[str, Any]) -> Rows:
        graph = self.graph
        if "count(n) AS nodes" in query:
            return Rows([Rec(nodes=len(graph.nodes))])
        if "DETACH DELETE" in query:
            doomed = list(graph.nodes)[: parameters["limit"]]
            for node_id in doomed:
                del graph.nodes[node_id]
            graph.rels = [r for r in graph.rels if r[0] in graph.nodes and r[2] in graph.nodes]
            return Rows([Rec(deleted=len(doomed))])
        if query.startswith("MATCH (n) WHERE NOT any"):
            keyed = set(parameters["keyed"])
            after = parameters["after"]
            ids = sorted(i for i, (labels, _) in graph.nodes.items() if not keyed & set(labels))
            ids = [i for i in ids if after is None or f"e{i:06d}" > after][: parameters["limit"]]
            return Rows(
                [
                    Rec(cursor=f"e{i:06d}", labels=graph.nodes[i][0], props=graph.nodes[i][1])
                    for i in ids
                ]
            )
        if query.startswith("MATCH (a) WHERE elementId"):
            wanted = {int(c[1:]) for c in parameters["cursors"]}
            return Rows(graph.rels_from(wanted))
        node_match = re.match(r"MATCH \(n:`(\w+)`\)", query)
        if node_match:
            label = node_match.group(1)
            key = NODE_KEYS[label]
            after = parameters["after"]
            found = sorted(
                (props[key], labels, props)
                for labels, props in graph.nodes.values()
                if label in labels and key in props and (after is None or props[key] > after)
            )[: parameters["limit"]]
            return Rows([Rec(cursor=c, labels=labels, props=props) for c, labels, props in found])
        rel_match = re.match(r"MATCH \(a:`(\w+)`\)", query)
        assert rel_match, query
        label = rel_match.group(1)
        key = NODE_KEYS[label]
        ids = {
            i
            for i, (labels, props) in graph.nodes.items()
            if label in labels and props.get(key) in parameters["cursors"]
        }
        return Rows(graph.rels_from(ids))


class FakeStore:
    def __init__(self, graph: FakeGraph | None = None) -> None:
        self.graph = graph or FakeGraph()
        self.writes = 0
        self.closed = False

    async def execute_read(self, callback: Callable[[FakeTx], Any]) -> Any:
        return await callback(FakeTx(self.graph))

    async def execute_write(self, callback: Callable[[FakeTx], Any]) -> Any:
        self.writes += 1
        return await callback(FakeTx(self.graph))

    async def close(self) -> None:
        self.closed = True


def sample_graph(*, lock: bool = False, extra: bool = False) -> FakeGraph:
    graph = FakeGraph()
    ws = graph.node("Workspace", workspace_id="ws_1", **({"_lock": True} if lock else {}))
    repo = graph.node("Repository", repository_id="repo_1", source_event_ids=["a", "b"])
    commit = graph.node("Commit", commit_id="c1", source_event_id="a")
    both = graph.node("Commit", "Checkout", commit_id="c2", checkout_id="co_2")
    loose = graph.node("Scratch", note="x")
    graph.rel(repo, "IN_WORKSPACE", ws, source_event_ids=["a"])
    graph.rel(commit, "IN_REPOSITORY", repo)
    graph.rel(both, "IN_REPOSITORY", repo)
    graph.rel(loose, "NEAR", repo)
    if extra:
        graph.node("Commit", commit_id="c3")
    return graph


async def test_digest_counts_every_node_and_relationship() -> None:
    result = await compute_graph_digest(FakeStore(sample_graph()), batch_size=2)

    assert result.node_count == 5 and result.relationship_count == 4
    assert result.nodes == {
        "Workspace": 1,
        "Repository": 1,
        "Commit": 1,
        "Checkout:Commit": 1,
        "Scratch": 1,
    }
    assert result.relationships == {"IN_WORKSPACE": 1, "IN_REPOSITORY": 2, "NEAR": 1}
    assert re.fullmatch(r"[0-9a-f]{64}", result.digest)
    assert result.to_dict()["node_count"] == 5


async def test_digest_is_independent_of_batch_size_and_insertion_order() -> None:
    digests = {
        (await compute_graph_digest(FakeStore(sample_graph()), batch_size=size)).digest
        for size in (1, 2, 3, 100)
    }
    reversed_graph = sample_graph()
    reversed_graph.nodes = dict(reversed(list(reversed_graph.nodes.items())))
    reversed_graph.rels.reverse()
    digests.add((await compute_graph_digest(FakeStore(reversed_graph), batch_size=2)).digest)

    assert len(digests) == 1


async def test_digest_ignores_volatile_properties_and_sees_every_other_change() -> None:
    base = (await compute_graph_digest(FakeStore(sample_graph()))).digest

    assert (await compute_graph_digest(FakeStore(sample_graph(lock=True)))).digest == base
    assert (await compute_graph_digest(FakeStore(sample_graph(extra=True)))).digest != base
    changed = sample_graph()
    changed.rels[1] = (changed.rels[1][0], "IN_REPOSITORY", changed.rels[1][2], {"x": 1})
    assert (await compute_graph_digest(FakeStore(changed))).digest != base


async def test_digest_of_an_empty_graph_is_stable() -> None:
    first = await compute_graph_digest(FakeStore())
    second = await compute_graph_digest(FakeStore())

    assert first.digest == second.digest and first.node_count == 0


async def test_digest_rejects_a_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        await compute_graph_digest(FakeStore(), batch_size=0)


async def test_digest_hands_source_ids_to_the_sink_in_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(verify, "_SOURCE_ID_FLUSH", 1)
    seen: list[set[str]] = []

    async def sink(ids: set[str]) -> None:
        seen.append(ids)

    await compute_graph_digest(FakeStore(sample_graph()), batch_size=1, on_source_ids=sink)

    assert set().union(*seen) == {"a", "b"}
    assert len(seen) > 1  # flushed whenever the pending set reached the threshold


async def test_digest_without_a_sink_still_drops_pending_source_ids() -> None:
    result = await compute_graph_digest(FakeStore(sample_graph()), batch_size=1)

    assert result.node_count == 5


def test_a_non_identifier_in_the_schema_manifest_is_refused() -> None:
    with pytest.raises(verify.GraphScanError):
        verify._identifier("bad`label")


def test_identity_primary_label_and_source_ids() -> None:
    assert (
        primary_label(["Commit", "Checkout"], {"commit_id": "c", "checkout_id": "k"}) == "Checkout"
    )
    assert primary_label(["Commit"], {}) is None
    assert node_identity(["Commit"], {"commit_id": "c"}) == "Commit:c"
    assert node_identity(["B", "A"], {"x": 1}).startswith("~A:B:")
    assert node_identity(["A"], {"x": 1, "_lock": True}) == node_identity(["A"], {"x": 1})
    assert source_ids({"source_event_id": "a", "source_event_ids": ["b", 3, "c"]}) == [
        "a",
        "b",
        "c",
    ]
    assert source_ids({"source_event_ids": ("d",)}) == ["d"]
    assert source_ids({"source_event_id": 7}) == []


# --------------------------------------------------------------------------------------------
# A PostgreSQL router
# --------------------------------------------------------------------------------------------


def sql_of(statement: Any) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))


class FakeSession:
    def __init__(self, answer: Callable[[str, str], Any]) -> None:
        self._answer = answer
        self.statements: list[str] = []

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def begin(self) -> FakeSession:
        return self

    async def execute(self, statement: Any) -> Any:
        self.statements.append(sql_of(statement))
        return self._answer("execute", self.statements[-1])

    async def scalar(self, statement: Any, _params: Any = None) -> Any:
        self.statements.append(sql_of(statement))
        return self._answer("scalar", self.statements[-1])

    async def scalars(self, statement: Any) -> Any:
        self.statements.append(sql_of(statement))
        return self._answer("scalars", self.statements[-1])

    async def get(self, _model: Any, key: Any) -> Any:
        return self._answer("get", str(key))


class Executed:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows


def factory(answer: Callable[[str, str], Any]) -> Callable[[], FakeSession]:
    sessions: list[FakeSession] = []

    def make() -> FakeSession:
        sessions.append(FakeSession(answer))
        return sessions[-1]

    make.sessions = sessions  # type: ignore[attr-defined]
    return make


class FakeProjector:
    def __init__(self, name: str, *types: str, version: str = "1", fail: bool = False) -> None:
        self.name = name
        self.version = version
        self._types = set(types)
        self._fail = fail
        self.projected: list[UUID] = []

    def handles(self, event_type: str) -> bool:
        return event_type in self._types

    async def project(self, tx: Any, event: Any) -> None:
        if self._fail:
            raise RuntimeError("boom")
        self.projected.append(event.event_id)


# --------------------------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------------------------

EVENT_A, EVENT_B = new_uuid7(), new_uuid7()


def stats(event_type: str = "t.a", **overrides: Any) -> EventTypeStats:
    values: dict[str, Any] = {
        "event_type": event_type,
        "total": 3,
        "unqueued": 0,
        "covered": 3,
        "delivered_covered": 3,
        "dead_covered": 0,
        "delivered_beyond": 0,
        "first_event_id": str(EVENT_A),
        "last_event_id": str(EVENT_B),
    }
    return EventTypeStats(**{**values, **overrides})


def checkpoint(**overrides: Any) -> CheckpointView:
    values: dict[str, Any] = {
        "outbox_id": 9,
        "event_id": EVENT_B,
        "processed_count": 3,
        "state": "active",
        "updated_at": NOW,
        "pointer_event_id": EVENT_B,
    }
    return CheckpointView(**{**values, **overrides})


def test_a_consistent_checkpoint_verifies() -> None:
    result = classify_projector(FakeProjector("p", "t.a"), checkpoint(), [stats(), stats("other")])

    assert result.ok and result.issues == ()
    assert (result.handled_events, result.covered_events, result.lag) == (3, 3, 0)
    assert (result.from_event_id, result.through_event_id) == (EVENT_A, EVENT_B)
    assert result.to_dict()["ok"] is True


@pytest.mark.parametrize(
    ("view", "row", "issue"),
    [
        (checkpoint(pointer_event_id=uuid4()), stats(), "checkpoint_pointer_mismatch"),
        (checkpoint(), stats(delivered_beyond=1, total=4), "checkpoint_regressed"),
        (None, stats(covered=0, delivered_covered=0, delivered_beyond=3), "checkpoint_regressed"),
        (
            checkpoint(),
            stats(dead_covered=1, delivered_covered=2),
            "dead_lettered_below_checkpoint",
        ),
        (checkpoint(processed_count=2), stats(), "processed_count_mismatch"),
        (checkpoint(), stats(unqueued=1, total=4), "unqueued_events"),
    ],
)
def test_each_inconsistency_is_reported(
    view: CheckpointView | None, row: EventTypeStats, issue: str
) -> None:
    result = classify_projector(FakeProjector("p", "t.a"), view, [row])

    assert issue in result.issues and not result.ok


def test_lag_and_in_flight_rows_only_fail_when_a_caught_up_projection_is_required() -> None:
    row = stats(total=5, covered=3, delivered_covered=2)  # one in flight below, two beyond
    view = checkpoint(processed_count=2)

    relaxed = classify_projector(FakeProjector("p", "t.a"), view, [row])
    strict = classify_projector(FakeProjector("p", "t.a"), view, [row], require_caught_up=True)

    assert relaxed.ok and (relaxed.lag, relaxed.in_flight) == (2, 1)
    assert {"lagging", "in_flight_below_checkpoint"} <= set(strict.issues)


def test_a_projector_with_nothing_to_handle_has_an_empty_range() -> None:
    result = classify_projector(FakeProjector("p", "none"), None, [stats()])

    assert result.ok and result.handled_events == 0
    assert (result.from_event_id, result.through_event_id) == (None, None)
    assert result.checkpoint_outbox_id is None


def test_a_checkpoint_without_progress_has_no_pointer_to_check() -> None:
    view = checkpoint(outbox_id=None, event_id=None, processed_count=0, pointer_event_id=None)

    result = classify_projector(FakeProjector("p", "none"), view, [])

    assert result.ok


def test_replay_classification_requires_every_event_up_to_the_head_projected() -> None:
    projector = FakeProjector("p", "t.a")
    done = ReplayProgress(count=3, last_outbox_id=9, last_event_id=EVENT_B)

    assert classify_replay(projector, done, [stats()]).ok
    short = classify_replay(projector, ReplayProgress(count=2), [stats()])
    assert short.issues == ("replay_incomplete",)
    missing = classify_replay(projector, None, [stats(unqueued=1, total=4)])
    assert set(missing.issues) == {"replay_incomplete", "unqueued_events"}


def test_events_pending_after_the_replay_head_are_lag_and_delivered_ones_fail() -> None:
    projector = FakeProjector("p", "t.a")
    done = ReplayProgress(count=3, last_outbox_id=9, last_event_id=EVENT_B)

    pending = classify_replay(projector, done, [stats(total=5)])  # two appended, still pending
    assert pending.ok and pending.lag == 2 and pending.handled_events == 5

    delivered = classify_replay(projector, done, [stats(total=5, delivered_beyond=2)])
    assert delivered.issues == ("delivered_after_replay",) and delivered.lag == 0


# --------------------------------------------------------------------------------------------
# SQL layer
# --------------------------------------------------------------------------------------------


async def test_event_type_stats_maps_the_aggregate_rows() -> None:
    row = ("t.a", 3, 0, 3, 3, 0, 0, str(EVENT_A), str(EVENT_B))

    def answer(kind: str, text: str) -> Any:
        assert "GROUP BY ledger.events.event_type" in text and "FILTER" in text
        return Executed([row])

    result = await verify.event_type_stats(factory(answer)(), None)

    assert result == [stats()]


async def test_checkpoint_view_reads_the_row_and_its_outbox_pointer() -> None:
    row = type(
        "Row",
        (),
        {
            "last_outbox_id": 9,
            "last_event_id": EVENT_B,
            "processed_count": 3,
            "state": ProjectionState.ACTIVE,
            "updated_at": NOW,
        },
    )()
    projector = FakeProjector("p")

    def answer(kind: str, text: str) -> Any:
        return row if kind == "get" else EVENT_B

    assert await verify._checkpoint_view(factory(answer)(), projector) == checkpoint()
    row.last_outbox_id = None
    assert (await verify._checkpoint_view(factory(answer)(), projector)).pointer_event_id is None
    assert await verify._checkpoint_view(factory(lambda *_: None)(), projector) is None


async def test_stream_head_check_reports_mismatches_and_lag() -> None:
    counts = iter([7, 2])  # streams, then streams behind

    def answer(kind: str, text: str) -> Any:
        return ["stream-b", "stream-a"] if kind == "scalars" else next(counts)

    result = await verify.check_stream_heads(factory(answer)(), {"t.a"})

    assert (result.streams, result.behind, result.mismatched) == (7, 2, 2)
    assert result.mismatched_sample == ("stream-b", "stream-a")
    assert result.to_dict()["mismatched"] == 2


async def test_a_stream_is_never_behind_when_no_projector_handles_any_event_type() -> None:
    queries: list[str] = []
    counts = iter([4])

    def answer(kind: str, text: str) -> Any:
        queries.append(text)
        return [] if kind == "scalars" else next(counts)

    result = await verify.check_stream_heads(factory(answer)(), set())

    assert (result.streams, result.behind) == (4, 0)
    assert all("projection.outbox" not in text for text in queries)


def test_handled_event_types_are_the_ledger_types_some_projector_handles() -> None:
    first, second = FakeProjector("p", "t.a"), FakeProjector("q", "t.b")
    collected = [
        (first, None, [stats("t.a"), stats("t.system")]),
        (second, None, [stats("t.a"), stats("t.b")]),
    ]

    assert verify.handled_event_types(collected) == {"t.a", "t.b"}


async def test_stream_head_check_treats_empty_counts_as_zero() -> None:
    def answer(kind: str, text: str) -> Any:
        return [] if kind == "scalars" else None

    result = await verify.check_stream_heads(factory(answer)(), {"t.a"})

    assert (result.streams, result.mismatched, result.behind) == (0, 0, 0)


async def test_ledger_head_and_active_lease_count() -> None:
    assert await verify.ledger_head(factory(lambda *_: 12)()) == 12
    assert await verify.active_lease_count(factory(lambda *_: 2)()) == 2
    assert await verify.active_lease_count(factory(lambda *_: None)()) == 0


async def test_orphan_collector_flags_unknown_and_malformed_ids() -> None:
    known, unknown = uuid4(), uuid4()

    def answer(kind: str, text: str) -> Any:
        assert "ledger.events.event_id IN" in text
        return [known]

    collector = verify.OrphanCollector(factory(answer))
    await collector.add({str(known), str(unknown), "not-a-uuid"})
    await collector.add({"also-bad"})
    await collector.add(set())

    assert collector.orphans == {str(unknown), "not-a-uuid", "also-bad"}
    assert collector.checked == 4


async def test_collect_ledger_state_gathers_head_checkpoint_and_stats() -> None:
    projectors = [FakeProjector("p", "t.a"), FakeProjector("q", "t.a")]

    def answer(kind: str, text: str) -> Any:
        if kind == "get":
            return None
        if kind == "scalar":
            return 5
        return Executed([("t.a", 1, 0, 0, 0, 0, 0, str(EVENT_A), str(EVENT_A))])

    collected, head = await verify._collect_ledger_state(factory(answer), projectors)

    assert head == 5 and [item[0] for item in collected] == projectors
    assert collected[0][1] is None and collected[0][2][0].total == 1


async def test_write_checkpoints_upserts_one_row_per_projector() -> None:
    make = factory(lambda *_: None)
    progress = {("p", "1"): ReplayProgress(count=2, last_outbox_id=4, last_event_id=EVENT_A)}

    await verify._write_checkpoints(
        make,
        [FakeProjector("p"), FakeProjector("q")],
        progress,
        state=ProjectionState.ACTIVE,
        now=NOW,
    )

    statements = make.sessions[0].statements  # type: ignore[attr-defined]
    assert len(statements) == 2 and all("ON CONFLICT" in text for text in statements)


async def test_projection_status_reports_checkpoint_lag_and_last_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projector = FakeProjector("p", "t.a")

    async def collect(_factory: Any, _projectors: Any) -> Any:
        return [(projector, checkpoint(), [stats(total=5, covered=3, delivered_covered=3)])], 11

    monkeypatch.setattr(verify, "_collect_ledger_state", collect)

    [status] = await verify.projection_status(factory(lambda *_: "RuntimeError"), [projector])

    assert (status.name, status.state, status.lag, status.ledger_head_outbox_id) == (
        "p",
        "active",
        2,
        11,
    )
    assert status.last_error_class == "RuntimeError" and status.last_run_at == NOW
    assert status.to_dict()["last_run_at"] == NOW.isoformat()

    async def fresh(_factory: Any, _projectors: Any) -> Any:
        return [(projector, None, [])], None

    monkeypatch.setattr(verify, "_collect_ledger_state", fresh)
    [never] = await verify.projection_status(factory(lambda *_: None), [projector])
    assert (
        never.state is None and never.last_run_at is None and never.to_dict()["last_run_at"] is None
    )


# --------------------------------------------------------------------------------------------
# Reporting `projection.rebuilt`
# --------------------------------------------------------------------------------------------


def run_record(**overrides: Any) -> RunRecord:
    values: dict[str, Any] = {
        "projector_name": "git",
        "projector_version": "1",
        "mode": ProjectionMode.VERIFY,
        "outcome": ProjectionOutcome.MATCHED,
        "started_at": NOW,
        "completed_at": NOW + timedelta(seconds=1),
        "from_event_id": EVENT_A,
        "through_event_id": EVENT_B,
        "event_count": 3,
        "graph_digest": "a" * 64,
    }
    return RunRecord(**{**values, **overrides})


def test_the_system_event_key_ignores_times_and_tracks_everything_else() -> None:
    base = system_event_key(run_record())

    assert system_event_key(run_record(started_at=NOW - timedelta(days=1))) == base
    for change in (
        {"projector_name": "code"},
        {"mode": ProjectionMode.REBUILD, "outcome": ProjectionOutcome.COMPLETED},
        {"outcome": ProjectionOutcome.MISMATCHED},
        {"event_count": 4},
        {"graph_digest": "b" * 64},
        {"through_event_id": new_uuid7()},
        {"from_event_id": None, "through_event_id": None, "event_count": 0},
    ):
        assert system_event_key(run_record(**change)) != base
    assert base.startswith("projection.rebuilt:")


def test_a_draft_is_a_valid_deterministic_projection_rebuilt_event() -> None:
    draft = verify.build_system_draft(run_record(), observed_at=NOW)
    again = verify.build_system_draft(run_record(), observed_at=NOW + timedelta(hours=1))

    assert draft.event_id == again.event_id and draft.idempotency_key == again.idempotency_key
    assert draft.event_id.version == 7
    assert draft.event_type == "projection.rebuilt" and draft.producer.producer_id == (
        verify.PLATFORM_PRODUCER_ID
    )
    payload = ProjectionRebuiltV1.model_validate(dict(draft.payload))
    assert payload.outcome is ProjectionOutcome.MATCHED and payload.graph_digest == "a" * 64
    failed = verify.build_system_draft(
        run_record(outcome=ProjectionOutcome.FAILED, graph_digest=None, error_class="Boom"),
        observed_at=NOW,
    )
    assert failed.payload["error_class"] == "Boom"


def test_error_classes_are_made_public_or_replaced() -> None:
    assert verify.public_error_class(ValueError("secret")) == "ValueError"
    assert verify.public_error_class(type("_bad", (Exception,), {})()) == "ProjectionError"


async def test_the_no_content_store_refuses_every_use() -> None:
    store = verify.NoContentBlobStore()

    for call in (
        store.put_verified(b"", "text/plain"),
        store.get_verified("k", "d"),
        store.delete("k"),
    ):
        with pytest.raises(verify.BlobStoreError):
            await call


class FakeIngestion:
    def __init__(self, outcome: IngestOutcome | None = None) -> None:
        self.batches: list[Any] = []
        self.outcome = outcome

    async def ingest(self, batch: Any) -> IngestOutcome:
        self.batches.append(batch)
        if self.outcome is not None:
            return self.outcome
        return IngestOutcome(200, type("R", (), {"rejected": ()})())  # type: ignore[arg-type]


def stored_keys(monkeypatch: pytest.MonkeyPatch, known: set[str]) -> None:
    async def lookup(_session: Any, keys: Any) -> dict[tuple[str, str], Any]:
        return {key: object() for key in keys if key[1] in known}

    monkeypatch.setattr(verify.LedgerRepository, "get_by_idempotency_keys", staticmethod(lookup))


def rejection(status: int, *codes: str) -> IngestOutcome:
    rejected = tuple(type("E", (), {"error_code": code})() for code in codes)
    return IngestOutcome(status, type("R", (), {"rejected": rejected})())  # type: ignore[arg-type]


async def test_recording_appends_new_events_and_skips_ones_already_in_the_ledger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runs = [run_record(), run_record(projector_name="code")]
    known = {system_event_key(runs[0])}
    stored_keys(monkeypatch, known)
    ingestion = FakeIngestion()
    recorder = verify.ProjectionEventRecorder(ingestion, factory(lambda *_: None), clock)  # type: ignore[arg-type]

    result = await recorder.record(runs)

    assert result == RecordResult(appended=1, existing=1) and result.to_dict() == {
        "appended": 1,
        "existing": 1,
    }
    assert len(ingestion.batches[0].events) == 1

    stored_keys(monkeypatch, {system_event_key(run) for run in runs})
    assert await recorder.record(runs) == RecordResult(appended=0, existing=2)
    assert len(ingestion.batches) == 1


async def test_recording_treats_a_raced_identical_report_as_recorded_and_other_rejections_as_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored_keys(monkeypatch, set())
    raced = verify.ProjectionEventRecorder(
        FakeIngestion(rejection(409, "idempotency_conflict", "batch_rejected")),  # type: ignore[arg-type]
        factory(lambda *_: None),
    )
    assert await raced.record([run_record()]) == RecordResult(appended=0, existing=1)

    refused = verify.ProjectionEventRecorder(
        FakeIngestion(rejection(422, "bad")),  # type: ignore[arg-type]
        factory(lambda *_: None),
    )
    with pytest.raises(RecordingError, match="bad"):
        await refused.record([run_record()])


def test_an_in_process_recorder_uses_the_platform_producer_and_no_blob_store() -> None:
    recorder = verify.ProjectionEventRecorder.in_process(factory(lambda *_: None), clock)

    assert isinstance(recorder, verify.ProjectionEventRecorder)


# --------------------------------------------------------------------------------------------
# Verification orchestration
# --------------------------------------------------------------------------------------------


class FakeRecorder:
    def __init__(self, fail: bool = False) -> None:
        self.runs: list[list[RunRecord]] = []
        self._fail = fail

    async def record(self, runs: list[RunRecord]) -> RecordResult:
        if self._fail:
            raise RecordingError("no")
        self.runs.append(list(runs))
        return RecordResult(appended=len(runs), existing=0)


def digest(value: str = "d" * 64) -> GraphDigest:
    return GraphDigest(value, 4, 3, {"Commit": 4}, {"HAS": 3})


def verification(name: str = "p", *issues: str) -> ProjectorVerification:
    return ProjectorVerification(
        name=name,
        version="1",
        checkpoint_outbox_id=9,
        processed_count=3,
        handled_events=3,
        covered_events=3,
        lag=0,
        in_flight=0,
        dead_lettered=0,
        unqueued=0,
        from_event_id=EVENT_A,
        through_event_id=EVENT_B,
        issues=issues,
    )


def patch_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    issues: tuple[str, ...] = (),
    orphans: set[str] | None = None,
    graph: GraphDigest | None = None,
    behind: int = 0,
    mismatched: int = 0,
) -> list[FakeProjector]:
    projectors = [FakeProjector("p", "t.a"), FakeProjector("q", "t.a")]

    async def collect(_factory: Any, _projectors: Any) -> Any:
        return [
            (item, checkpoint(), [stats(**({"dead_covered": 1} if issues else {}))])
            for item in projectors
        ], 9

    async def heads(_session: Any, _types: Any) -> StreamHeadCheck:
        return StreamHeadCheck(
            streams=3, mismatched=mismatched, mismatched_sample=(), behind=behind
        )

    async def scan(_store: Any, _factory: Any, _batch: int) -> Any:
        collector = verify.OrphanCollector(factory(lambda *_: []))
        collector.orphans = set(orphans or ())
        collector.checked = 10
        return graph or digest(), collector

    monkeypatch.setattr(verify, "_collect_ledger_state", collect)
    monkeypatch.setattr(verify, "check_stream_heads", heads)
    monkeypatch.setattr(verify, "_scan_with_orphans", scan)
    return projectors


async def test_a_clean_verification_matches_and_records_one_event_per_projector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projectors = patch_state(monkeypatch)
    recorder = FakeRecorder()

    report = await verify.verify_projections(
        factory(lambda *_: None),
        FakeStore(),
        projectors,
        recorder=recorder,
        clock=clock,  # type: ignore[arg-type]
    )

    assert report.ok() and report.problems() == []
    assert report.recorded == RecordResult(appended=2, existing=0)
    runs = recorder.runs[0]
    assert {run.outcome for run in runs} == {ProjectionOutcome.MATCHED}
    assert {run.mode for run in runs} == {ProjectionMode.VERIFY}
    assert {run.graph_digest for run in runs} == {"d" * 64}
    payload = report.to_dict()
    assert payload["ok"] and payload["orphans"] == {"checked": 10, "count": 0, "sample": []}
    assert payload["replay_matches"] is None and payload["recorded"]["appended"] == 2


async def test_verification_without_a_recorder_records_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projectors = patch_state(monkeypatch)

    report = await verify.verify_projections(factory(lambda *_: None), FakeStore(), projectors)  # type: ignore[arg-type]

    assert report.recorded is None and report.to_dict()["recorded"] is None


async def test_every_kind_of_mismatch_is_a_problem_and_recorded_as_mismatched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projectors = patch_state(monkeypatch, issues=("x",), orphans={"ghost"}, mismatched=1, behind=2)
    recorder = FakeRecorder()

    report = await verify.verify_projections(
        factory(lambda *_: None),
        FakeStore(),
        projectors,
        recorder=recorder,
        require_caught_up=True,  # type: ignore[arg-type]
    )

    problems = report.problems(require_caught_up=True)
    assert "p: dead_lettered_below_checkpoint" in problems
    assert {"streams: head_mismatch", "streams: behind", "graph: orphan_source_ids"} <= set(
        problems
    )
    assert {run.outcome for run in recorder.runs[0]} == {ProjectionOutcome.MISMATCHED}
    assert report.orphan_sample == ("ghost",)
    assert "streams: behind" not in report.problems()


async def test_a_replay_check_compares_the_scratch_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    projectors = patch_state(monkeypatch)
    seen: dict[str, Any] = {}

    async def scratch(_factory: Any, target: Any, _projectors: Any, **kwargs: Any) -> str:
        seen.update(kwargs, target=target)
        return "d" * 64 if not seen.get("differ") else "e" * 64

    monkeypatch.setattr(verify, "_replay_digest", scratch)
    target = FakeStore()

    matching = await verify.verify_projections(
        factory(lambda *_: None),
        FakeStore(),
        projectors,
        replay_target=target,
        wipe_replay_target=True,  # type: ignore[arg-type]
    )
    assert matching.replay_matches is True and matching.ok() and seen["wipe"] is True

    seen["differ"] = True
    differing = await verify.verify_projections(
        factory(lambda *_: None),
        FakeStore(),
        projectors,
        replay_target=target,  # type: ignore[arg-type]
    )
    assert differing.replay_matches is False
    assert "graph: replay_digest_mismatch" in differing.problems()


@pytest.mark.parametrize("recorder_fails", [False, True])
async def test_a_graph_failure_is_recorded_as_failed_and_re_raised(
    monkeypatch: pytest.MonkeyPatch, recorder_fails: bool
) -> None:
    projectors = patch_state(monkeypatch)

    async def broken(*_args: Any) -> Any:
        raise ConnectionError("down")

    monkeypatch.setattr(verify, "_scan_with_orphans", broken)
    recorder = FakeRecorder(fail=recorder_fails)

    with pytest.raises(ConnectionError):
        await verify.verify_projections(
            factory(lambda *_: None),
            FakeStore(),
            projectors,
            recorder=recorder,
            clock=clock,  # type: ignore[arg-type]
        )

    if not recorder_fails:
        [failed] = recorder.runs
        assert {(run.outcome, run.error_class, run.graph_digest) for run in failed} == {
            (ProjectionOutcome.FAILED, "ConnectionError", None)
        }


async def test_scan_with_orphans_checks_the_graph_source_ids() -> None:
    found = uuid4()
    graph = FakeGraph()
    graph.node("Commit", commit_id="c1", source_event_ids=[str(found), "not-a-uuid"])

    result, collector = await verify._scan_with_orphans(
        FakeStore(graph),  # type: ignore[arg-type]
        factory(lambda *_: [found]),  # type: ignore[arg-type]
        100,
    )

    assert result.node_count == 1
    assert collector.checked == 2 and collector.orphans == {"not-a-uuid"}


def test_report_serializes_problems_and_replay_results() -> None:
    report = VerificationReport(
        projectors=(verification("p", "lagging"),),
        streams=StreamHeadCheck(1, 0, (), 0),
        graph=digest(),
        orphan_count=0,
        orphan_sample=(),
        source_ids_checked=0,
        ledger_head_outbox_id=None,
        replay_digest="f" * 64,
    )

    assert report.replay_matches is False
    assert report.problems() == ["p: lagging", "graph: replay_digest_mismatch"]
    assert replace(report, replay_digest=digest().digest).replay_matches is True


# --------------------------------------------------------------------------------------------
# Replay and rebuild
# --------------------------------------------------------------------------------------------


def events_for_replay() -> list[Any]:
    return [
        build_stored_event(event_type="t.a", stream_id="s1"),
        build_stored_event(event_type="t.none", stream_id="s2"),
        build_stored_event(event_type="t.a", stream_id="s3"),
    ]


def replay_fixture(
    monkeypatch: pytest.MonkeyPatch,
    events: list[Any],
    *,
    missing: bool = False,
    corrupt: bool = False,
) -> Any:
    pages = [[(i + 1, event.event_id) for i, event in enumerate(events)]]
    served: list[str] = []

    def answer(kind: str, text: str) -> Any:
        served.append(text)
        return Executed(pages.pop(0) if pages else [])

    async def get_event(_session: Any, event_id: Any) -> Any:
        if missing:
            return None
        found = next(event for event in events if event.event_id == event_id)
        return found.model_copy(update={"stream_id": "tampered"}) if corrupt else found

    monkeypatch.setattr(verify.LedgerRepository, "get_event", staticmethod(get_event))
    make = factory(answer)
    make.served = served  # type: ignore[attr-defined]
    return make


async def test_replay_projects_matching_events_in_ledger_order_without_touching_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = events_for_replay()
    make = replay_fixture(monkeypatch, events)
    projector, other = FakeProjector("p", "t.a"), FakeProjector("q", "t.none")
    target = FakeStore()

    result = await verify.replay_ledger(make, target, [projector, other], delivered_only=False)  # type: ignore[arg-type]

    assert projector.projected == [events[0].event_id, events[2].event_id]
    assert other.projected == [events[1].event_id]
    assert (result.rows, result.projected_events, result.head_outbox_id) == (3, 3, 3)
    assert result.progress[("p", "1")] == ReplayProgress(2, 3, events[2].event_id)
    assert target.writes == 3
    assert all("outbox.status" not in text for text in make.served)


async def test_a_delivered_only_replay_filters_on_the_outbox_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make = replay_fixture(monkeypatch, [])

    result = await verify.replay_ledger(
        make, FakeStore(), [FakeProjector("p")], delivered_only=True
    )  # type: ignore[arg-type]

    assert result.rows == 0 and result.head_outbox_id is None
    assert "projection.outbox.status" in make.served[0]


async def test_replay_stops_on_a_missing_or_corrupt_event(monkeypatch: pytest.MonkeyPatch) -> None:
    events = events_for_replay()
    with pytest.raises(OrphanedOutboxRowError):
        await verify.replay_ledger(
            replay_fixture(monkeypatch, events, missing=True),
            FakeStore(),
            [FakeProjector("p")],
            delivered_only=False,  # type: ignore[arg-type]
        )
    with pytest.raises(EventIntegrityError):
        await verify.replay_ledger(
            replay_fixture(monkeypatch, events, corrupt=True),
            FakeStore(),
            [FakeProjector("p")],
            delivered_only=False,  # type: ignore[arg-type]
        )


async def test_count_and_wipe_work_in_bounded_batches() -> None:
    graph = sample_graph()
    store = FakeStore(graph)

    assert await verify.count_nodes(store) == 5  # type: ignore[arg-type]
    assert await verify.wipe_graph(store, batch=2) == 5  # type: ignore[arg-type]
    assert await verify.count_nodes(store) == 0  # type: ignore[arg-type]
    assert store.writes == 4  # 2 + 2 + 1, then an empty batch


async def test_a_scratch_replay_needs_an_empty_or_confirmed_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def schema(_store: Any) -> None:
        calls.append("schema")

    async def replay(*_args: Any, **kwargs: Any) -> ReplayResult:
        calls.append(f"replay delivered_only={kwargs['delivered_only']}")
        return ReplayResult(0, 0, None, {})

    monkeypatch.setattr(verify, "ensure_schema", schema)
    monkeypatch.setattr(verify, "replay_ledger", replay)
    full = FakeStore(sample_graph())

    with pytest.raises(verify.TargetNotEmptyError):
        await verify._replay_digest(None, full, [], wipe=False, batch_size=10)  # type: ignore[arg-type]

    digest_value = await verify._replay_digest(None, full, [], wipe=True, batch_size=10)  # type: ignore[arg-type]

    assert re.fullmatch(r"[0-9a-f]{64}", digest_value)
    assert calls == ["schema", "replay delivered_only=True"] and not full.graph.nodes
    await verify._replay_digest(None, FakeStore(), [], wipe=False, batch_size=10)  # type: ignore[arg-type]


class Steps:
    """Records the order of a rebuild's side effects."""

    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, *, leases: int = 0, ok: bool = True
    ) -> None:
        self.calls: list[str] = []
        self.projectors = [FakeProjector("p", "t.a"), FakeProjector("q", "t.a")]
        self.ok = ok
        self.fail_replay = False
        progress = {
            ("p", "1"): ReplayProgress(3, 7, EVENT_B),
            ("q", "1"): ReplayProgress(3, 7, EVENT_B),
        }

        async def lease_count(_session: Any) -> int:
            return leases

        async def write(
            _factory: Any, _projectors: Any, _progress: Any, *, state: Any, now: Any
        ) -> None:
            self.calls.append(f"checkpoints {state.value}")

        async def schema(_store: Any) -> None:
            self.calls.append("schema")

        async def replay(
            _factory: Any, _target: Any, _projectors: Any, **kwargs: Any
        ) -> ReplayResult:
            self.calls.append(f"replay delivered_only={kwargs['delivered_only']}")
            if self.fail_replay:
                raise RuntimeError("secret detail")
            return ReplayResult(7, 6, 7, progress)

        async def verify_live(*_args: Any, **_kwargs: Any) -> VerificationReport:
            self.calls.append("verify live")
            return self.report()

        async def verify_target(*_args: Any) -> VerificationReport:
            self.calls.append("verify target")
            return self.report()

        async def collect(_factory: Any, projectors: Any) -> Any:
            return [(item, None, [stats()]) for item in projectors], 7

        async def wipe(_store: Any) -> int:
            self.calls.append("wipe")
            return 9

        monkeypatch.setattr(verify, "active_lease_count", lease_count)
        monkeypatch.setattr(verify, "_write_checkpoints", write)
        monkeypatch.setattr(verify, "ensure_schema", schema)
        monkeypatch.setattr(verify, "replay_ledger", replay)
        monkeypatch.setattr(verify, "verify_projections", verify_live)
        monkeypatch.setattr(verify, "_verify_replayed_target", verify_target)
        monkeypatch.setattr(verify, "_collect_ledger_state", collect)
        monkeypatch.setattr(verify, "wipe_graph", wipe)

    def report(self) -> VerificationReport:
        return VerificationReport(
            projectors=tuple(
                verification(item.name, *([] if self.ok else ["replay_incomplete"]))
                for item in self.projectors
            ),
            streams=StreamHeadCheck(1, 0, (), 0),
            graph=digest(),
            orphan_count=0,
            orphan_sample=(),
            source_ids_checked=0,
            ledger_head_outbox_id=7,
        )


async def rebuild(steps: Steps, **kwargs: Any) -> Any:
    session = FakeSession(lambda *_: None)
    return await verify.rebuild_projections(
        lambda: session,  # type: ignore[arg-type]
        kwargs.pop("target", FakeStore()),  # type: ignore[arg-type]
        steps.projectors,
        target_description="host:7687/neo4j",
        clock=clock,
        **kwargs,
    )


async def test_an_in_place_rebuild_resets_checkpoints_before_wiping_then_replays_and_verifies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps = Steps(monkeypatch)
    recorder = FakeRecorder()

    result = await rebuild(steps, in_place=True, recorder=recorder)

    assert steps.calls == [
        "checkpoints rebuilding",
        "wipe",
        "schema",
        "replay delivered_only=True",
        "checkpoints active",
        "verify live",
    ]
    assert result.ok and result.mode == "in_place" and result.wiped_nodes == 9
    assert {run.mode for run in recorder.runs[0]} == {ProjectionMode.REBUILD}
    assert {run.outcome for run in recorder.runs[0]} == {ProjectionOutcome.COMPLETED}
    payload = result.to_dict()
    assert payload["verified_target"] == "host:7687/neo4j" and payload["graph_digest"] == "d" * 64
    assert payload["recorded"]["appended"] == 2


async def test_an_in_place_rebuild_refuses_while_the_runner_holds_leases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps = Steps(monkeypatch, leases=2)

    with pytest.raises(verify.RunnerActiveError):
        await rebuild(steps, in_place=True)

    assert steps.calls == []


async def test_a_standby_rebuild_never_touches_checkpoints_and_replays_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps = Steps(monkeypatch)

    result = await rebuild(steps, in_place=False)

    assert steps.calls == ["schema", "replay delivered_only=False", "verify target"]
    assert result.ok and result.mode == "standby" and result.wiped_nodes == 0
    assert result.recorded is None


async def test_a_standby_rebuild_refuses_a_non_empty_target_unless_wiping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps = Steps(monkeypatch)
    full = FakeStore(sample_graph())

    with pytest.raises(verify.TargetNotEmptyError):
        await rebuild(steps, in_place=False, target=full)
    assert steps.calls == []

    result = await rebuild(steps, in_place=False, target=full, wipe_target=True)

    assert steps.calls[0] == "wipe" and result.wiped_nodes == 9


async def test_a_failed_verification_makes_the_rebuild_failed_and_is_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps = Steps(monkeypatch, ok=False)
    recorder = FakeRecorder()

    result = await rebuild(steps, in_place=False, recorder=recorder)

    assert not result.ok and result.to_dict()["verified_target"] is None
    assert {(run.outcome, run.error_class) for run in recorder.runs[0]} == {
        (ProjectionOutcome.FAILED, "VerificationFailed")
    }


@pytest.mark.parametrize("recorder_fails", [False, True])
async def test_a_replay_failure_is_recorded_as_failed_without_its_message(
    monkeypatch: pytest.MonkeyPatch, recorder_fails: bool
) -> None:
    steps = Steps(monkeypatch)
    steps.fail_replay = True
    recorder = FakeRecorder(fail=recorder_fails)

    with pytest.raises(RuntimeError):
        await rebuild(steps, in_place=False, recorder=recorder)

    if not recorder_fails:
        [failed] = recorder.runs
        assert {(run.outcome, run.error_class, run.graph_digest) for run in failed} == {
            (ProjectionOutcome.FAILED, "RuntimeError", None)
        }


async def test_a_target_is_verified_against_the_ledger_as_of_the_replay_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projector = FakeProjector("p", "t.a")
    positions: list[int | None] = []
    rows = {"value": [stats()]}

    async def collect(_factory: Any, _projectors: Any, position: int | None) -> Any:
        positions.append(position)
        return [(projector, None, rows["value"])], 12

    async def heads(_session: Any, _types: Any) -> StreamHeadCheck:
        return StreamHeadCheck(1, 0, (), 0)

    async def scan(*_args: Any) -> Any:
        return digest(), verify.OrphanCollector(factory(lambda *_: []))

    monkeypatch.setattr(verify, "_collect_at", collect)
    monkeypatch.setattr(verify, "check_stream_heads", heads)
    monkeypatch.setattr(verify, "_scan_with_orphans", scan)
    progress = {("p", "1"): ReplayProgress(3, 9, EVENT_B)}
    result = ReplayResult(9, 3, 9, progress)

    steady = await verify._verify_replayed_target(
        factory(lambda *_: None),
        FakeStore(),
        [projector],
        result,
        10,  # type: ignore[arg-type]
    )
    rows["value"] = [stats(total=5)]  # appended while replaying, still pending: only lag
    appended = await verify._verify_replayed_target(
        factory(lambda *_: None),
        FakeStore(),
        [projector],
        result,
        10,  # type: ignore[arg-type]
    )
    rows["value"] = [stats(total=5, delivered_beyond=2)]  # the live runner delivered them
    lost = await verify._verify_replayed_target(
        factory(lambda *_: None),
        FakeStore(),
        [projector],
        result,
        10,  # type: ignore[arg-type]
    )

    assert positions == [9, 9, 9]
    assert steady.ok() and steady.ledger_head_outbox_id == 12
    assert appended.ok() and appended.projectors[0].lag == 2
    assert lost.problems() == ["p: delivered_after_replay"]


def test_rebuild_report_serialization_marks_the_adopted_target() -> None:
    report = verify.RebuildReport(
        mode="standby",
        target="host:7687/neo4j",
        projected_events=4,
        verification=VerificationReport(
            projectors=(),
            streams=StreamHeadCheck(0, 0, (), 0),
            graph=digest(),
            orphan_count=0,
            orphan_sample=(),
            source_ids_checked=0,
            ledger_head_outbox_id=None,
        ),
        wiped_nodes=0,
        outcome=ProjectionOutcome.COMPLETED,
        recorded=None,
    )

    assert report.ok and report.to_dict()["recorded"] is None


async def test_a_recording_failure_leaves_the_verification_result_intact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projectors = patch_state(monkeypatch)

    report = await verify.verify_projections(
        factory(lambda *_: None),  # type: ignore[arg-type]
        FakeStore(),  # type: ignore[arg-type]
        projectors,
        recorder=FakeRecorder(fail=True),  # type: ignore[arg-type]
        clock=clock,
    )

    assert report.ok() and report.recorded is None
    assert report.record_error == "record_failed" == verify.RECORD_FAILED
    assert report.to_dict()["record_error"] == "record_failed"


async def test_a_recording_failure_leaves_the_rebuild_and_its_verified_target_intact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps = Steps(monkeypatch)

    result = await rebuild(steps, in_place=False, recorder=FakeRecorder(fail=True))

    assert result.ok and result.recorded is None and result.record_error == "record_failed"
    payload = result.to_dict()
    assert payload["verified_target"] == "host:7687/neo4j" and payload["graph_digest"] == "d" * 64
    assert payload["record_error"] == "record_failed"


def grant_answer(missing: set[str]) -> Callable[[str, str], Any]:
    def answer(kind: str, text: str) -> Any:
        return not any(name in text for name in missing)

    return answer


async def test_preflight_passes_when_every_grant_is_held() -> None:
    make = factory(grant_answer(set()))

    await verify.preflight(make, make, write_checkpoints=True)  # type: ignore[arg-type]
    await verify.preflight(make, None, write_checkpoints=False)  # type: ignore[arg-type]

    texts = [text for session in make.sessions for text in session.statements]  # type: ignore[attr-defined]
    assert any("has_table_privilege" in t for t in texts)
    assert any("has_column_privilege" in t for t in texts)
    assert any("has_sequence_privilege" in t for t in texts)


async def test_preflight_names_every_missing_grant_per_role() -> None:
    projector = factory(grant_answer({"has_column_privilege"}))
    recorder = factory(grant_answer({"has_sequence_privilege", "has_table_privilege"}))

    with pytest.raises(verify.MissingGrantError) as no_write:
        await verify.preflight(projector, None, write_checkpoints=True)  # type: ignore[arg-type]
    assert no_write.value.role == "projector"
    assert "UPDATE on projection.projection_checkpoints.state" in no_write.value.missing
    await verify.preflight(projector, None, write_checkpoints=False)  # type: ignore[arg-type]

    with pytest.raises(verify.MissingGrantError) as no_insert:
        await verify.preflight(factory(grant_answer(set())), recorder, write_checkpoints=False)  # type: ignore[arg-type]
    assert no_insert.value.role == "api"
    assert "INSERT on ledger.events" in str(no_insert.value)
    assert "USAGE on projection.outbox_outbox_id_seq" in no_insert.value.missing
