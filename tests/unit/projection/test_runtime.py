from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from agent_context_platform.ledger.models import (
    ContentDisposition,
    ContentStorage,
    EventContentRefRow,
    EventRow,
)
from agent_context_platform.projection import runtime
from agent_context_platform.projection.checkpoints import CheckpointRepository
from agent_context_platform.projection.models import OutboxRow, OutboxStatus
from agent_context_platform.projection.runtime import (
    ClaimedOutboxRow,
    OrphanedOutboxRowError,
    ProjectionRunner,
    ProjectionRunReport,
    Projector,
)

from ._doubles import (
    FakeExecuteResult,
    FakeNeo4jStore,
    FakeScalarResult,
    FakeSession,
    FakeSessionFactory,
    RecordingProjector,
    build_stored_event,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _outbox_row(**overrides: object) -> OutboxRow:
    """Build a raw, pre-claim `OutboxRow`, the shape the ORM hands back from
    `session.scalars(...)` before `_claim_batch` converts it into a
    `ClaimedOutboxRow`. Only used to seed `_claim_batch`'s scripted results.
    """
    defaults: dict[str, object] = {
        "outbox_id": 1,
        "event_id": uuid4(),
        "status": OutboxStatus.LEASED,
        "retry_count": 0,
        "available_at": NOW,
        "lease_owner": "worker-a",
        "lease_expires_at": NOW + timedelta(seconds=30),
        "delivered_at": None,
        "dead_lettered_at": None,
        "last_error_class": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    defaults.update(overrides)
    return OutboxRow(**defaults)  # type: ignore[arg-type]


def _claimed_row(**overrides: object) -> ClaimedOutboxRow:
    """Build a `ClaimedOutboxRow`, the session-independent snapshot every
    method downstream of `_claim_batch` (`_process_claimed_row`,
    `_finalize_success`, `_finalize_failure`) actually operates on.
    """
    defaults: dict[str, object] = {
        "outbox_id": 1,
        "event_id": uuid4(),
        "retry_count": 0,
        "lease_expires_at": NOW + timedelta(seconds=30),
    }
    defaults.update(overrides)
    return ClaimedOutboxRow(**defaults)  # type: ignore[arg-type]


def _event_row_from(sealed: object) -> EventRow:
    assert hasattr(sealed, "model_dump")
    dumped = sealed.model_dump(mode="json")  # type: ignore[attr-defined]
    return EventRow(
        event_id=sealed.event_id,  # type: ignore[attr-defined]
        event_type=sealed.event_type,  # type: ignore[attr-defined]
        schema_version=sealed.schema_version,  # type: ignore[attr-defined]
        stream_id=sealed.stream_id,  # type: ignore[attr-defined]
        stream_sequence=sealed.stream_sequence,  # type: ignore[attr-defined]
        producer_id=sealed.producer.producer_id,  # type: ignore[attr-defined]
        idempotency_key=sealed.idempotency_key,  # type: ignore[attr-defined]
        occurred_at=sealed.occurred_at,  # type: ignore[attr-defined]
        observed_at=sealed.observed_at,  # type: ignore[attr-defined]
        recorded_at=sealed.observed_at,  # type: ignore[attr-defined]
        producer=dumped["producer"],
        context=dumped["context"],
        trace=dumped["trace"],
        payload=dumped["payload"],
        redaction=dumped["redaction"],
        payload_sha256=sealed.integrity.payload_sha256,  # type: ignore[attr-defined]
        previous_event_sha256=sealed.integrity.previous_event_sha256,  # type: ignore[attr-defined]
        event_sha256=sealed.integrity.event_sha256,  # type: ignore[attr-defined]
    )


def _async_return(value: object) -> object:
    async def _inner(*_args: object, **_kwargs: object) -> object:
        return value

    return _inner


# --- constructor validation -------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"worker_id": "   "}, "worker_id"),
        ({"worker_id": "worker-a", "max_attempts": 0}, "max_attempts"),
        ({"worker_id": "worker-a", "lease_duration": timedelta(0)}, "lease_duration"),
        ({"worker_id": "worker-a", "base_retry_delay": timedelta(0)}, "base_retry_delay"),
    ],
)
def test_constructor_rejects_invalid_configuration(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ProjectionRunner(FakeSessionFactory([]), FakeNeo4jStore(), [], **kwargs)  # type: ignore[arg-type]


def test_constructor_rejects_duplicate_projector_identities() -> None:
    projectors = [
        RecordingProjector("graph.test", "1", handled_types=["a"]),
        RecordingProjector("graph.test", "1", handled_types=["b"]),
    ]

    with pytest.raises(ValueError, match="unique"):
        ProjectionRunner(FakeSessionFactory([]), FakeNeo4jStore(), projectors, worker_id="worker-a")


# --- clock validation --------------------------------------------------------


def test_now_rejects_a_naive_clock_value() -> None:
    runner = ProjectionRunner(
        FakeSessionFactory([]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        clock=lambda: datetime(2026, 1, 1),
    )

    with pytest.raises(ValueError, match="UTC-aware"):
        runner._now()


def test_now_rejects_a_non_utc_offset_clock_value() -> None:
    non_utc = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2)))
    runner = ProjectionRunner(
        FakeSessionFactory([]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        clock=lambda: non_utc,
    )

    with pytest.raises(ValueError, match="UTC-aware"):
        runner._now()


def test_now_passes_through_a_valid_utc_clock_value() -> None:
    runner = ProjectionRunner(
        FakeSessionFactory([]), FakeNeo4jStore(), [], worker_id="worker-a", clock=lambda: NOW
    )

    assert runner._now() == NOW


# --- retry backoff ------------------------------------------------------------


@pytest.mark.parametrize(
    ("retry_count", "expected_seconds"),
    [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0)],
)
def test_retry_delay_grows_exponentially(retry_count: int, expected_seconds: float) -> None:
    delay = ProjectionRunner._retry_delay(timedelta(seconds=1), retry_count)

    assert delay == timedelta(seconds=expected_seconds)


# --- run_once orchestration ---------------------------------------------------


def test_run_once_rejects_a_non_positive_limit() -> None:
    runner = ProjectionRunner(FakeSessionFactory([]), FakeNeo4jStore(), [], worker_id="worker-a")

    with pytest.raises(ValueError, match="limit"):
        asyncio.run(runner.run_once(0))


def test_run_once_aggregates_outcomes_into_the_report() -> None:
    rows = [_claimed_row(outbox_id=n) for n in range(1, 5)]
    runner = ProjectionRunner(
        FakeSessionFactory([]), FakeNeo4jStore(), [], worker_id="worker-a", clock=lambda: NOW
    )

    async def fake_claim_batch(limit: int) -> list[ClaimedOutboxRow]:
        assert limit == 10
        return rows

    outcomes = iter(["delivered", "retried", "dead_lettered", "lost_lease"])

    async def fake_process_claimed_row(_row: ClaimedOutboxRow) -> str:
        return next(outcomes)

    runner._claim_batch = fake_claim_batch  # type: ignore[method-assign]
    runner._process_claimed_row = fake_process_claimed_row  # type: ignore[method-assign]

    report = asyncio.run(runner.run_once(10))

    assert report == ProjectionRunReport(
        claimed=4, delivered=1, retried=1, dead_lettered=1, lost_leases=1
    )


# --- _process_claimed_row orchestration ---------------------------------------


def test_process_claimed_row_finalizes_success_without_neo4j_when_nothing_matches() -> None:
    event = build_stored_event(event_type="unhandled.event")

    async def fake_load_event(_session: object, _event_id: object) -> object:
        return event

    original_load_event = runtime._load_event
    runtime._load_event = fake_load_event  # type: ignore[assignment]
    try:
        neo4j = FakeNeo4jStore()
        projector = RecordingProjector("graph.test", "1", handled_types=["other.event"])
        runner = ProjectionRunner(
            FakeSessionFactory([FakeSession()]),
            neo4j,
            [projector],
            worker_id="worker-a",
            clock=lambda: NOW,
        )
        calls: list[tuple[ClaimedOutboxRow, object, tuple[Projector, ...]]] = []

        async def fake_finalize_success(
            *, row: ClaimedOutboxRow, event: object, matching: Sequence[Projector]
        ) -> bool:
            calls.append((row, event, tuple(matching)))
            return True

        runner._finalize_success = fake_finalize_success  # type: ignore[method-assign]

        row = _claimed_row()
        outcome = asyncio.run(runner._process_claimed_row(row))
    finally:
        runtime._load_event = original_load_event  # type: ignore[assignment]

    assert outcome == "delivered"
    assert neo4j.write_calls == 0
    assert projector.project_calls == []
    [(recorded_row, recorded_event, recorded_matching)] = calls
    assert recorded_row is row
    assert recorded_event is event
    assert recorded_matching == ()


def test_process_claimed_row_runs_matching_projectors_and_finalizes_success() -> None:
    event = build_stored_event(event_type="graph.node.created")

    async def fake_load_event(_session: object, _event_id: object) -> object:
        return event

    original_load_event = runtime._load_event
    runtime._load_event = fake_load_event  # type: ignore[assignment]
    try:
        neo4j = FakeNeo4jStore()
        matching = RecordingProjector("graph.test", "1", handled_types=["graph.node.created"])
        other = RecordingProjector("graph.other", "1", handled_types=["something.else"])
        runner = ProjectionRunner(
            FakeSessionFactory([FakeSession()]),
            neo4j,
            [matching, other],
            worker_id="worker-a",
            clock=lambda: NOW,
        )
        calls: list[tuple[Projector, ...]] = []

        async def fake_finalize_success(
            *, row: ClaimedOutboxRow, event: object, matching: Sequence[Projector]
        ) -> bool:
            calls.append(tuple(matching))
            return True

        runner._finalize_success = fake_finalize_success  # type: ignore[method-assign]

        outcome = asyncio.run(runner._process_claimed_row(_claimed_row()))
    finally:
        runtime._load_event = original_load_event  # type: ignore[assignment]

    assert outcome == "delivered"
    assert neo4j.write_calls == 1
    assert matching.project_calls == [event]
    assert other.project_calls == []
    assert calls == [(matching,)]


def test_process_claimed_row_attributes_failure_to_the_projector_that_raised() -> None:
    event = build_stored_event(event_type="graph.node.created")

    async def fake_load_event(_session: object, _event_id: object) -> object:
        return event

    original_load_event = runtime._load_event
    runtime._load_event = fake_load_event  # type: ignore[assignment]
    try:
        boom = RuntimeError("boom")
        first = RecordingProjector("graph.first", "1", handled_types=["graph.node.created"])
        second = RecordingProjector(
            "graph.second", "1", handled_types=["graph.node.created"], fail_with=boom
        )
        runner = ProjectionRunner(
            FakeSessionFactory([FakeSession()]),
            FakeNeo4jStore(),
            [first, second],
            worker_id="worker-a",
            clock=lambda: NOW,
        )
        calls: list[tuple[object, Projector | None, BaseException]] = []

        async def fake_finalize_failure(
            *,
            row: ClaimedOutboxRow,
            event_id: object,
            failing_projector: Projector | None,
            error: BaseException,
        ) -> str:
            calls.append((event_id, failing_projector, error))
            return "retried"

        runner._finalize_failure = fake_finalize_failure  # type: ignore[method-assign]

        outcome = asyncio.run(runner._process_claimed_row(_claimed_row()))
    finally:
        runtime._load_event = original_load_event  # type: ignore[assignment]

    assert outcome == "retried"
    assert first.project_calls == [event]
    assert second.project_calls == [event]
    [(recorded_event_id, recorded_failing_projector, recorded_error)] = calls
    assert recorded_event_id == event.event_id
    assert recorded_failing_projector is second
    assert recorded_error is boom


def test_process_claimed_row_routes_orphaned_outbox_rows_to_finalize_failure() -> None:
    async def fake_load_event(_session: object, _event_id: object) -> object:
        raise OrphanedOutboxRowError("missing")

    original_load_event = runtime._load_event
    runtime._load_event = fake_load_event  # type: ignore[assignment]
    try:
        neo4j = FakeNeo4jStore()
        runner = ProjectionRunner(
            FakeSessionFactory([FakeSession()]),
            neo4j,
            [],
            worker_id="worker-a",
            clock=lambda: NOW,
        )
        calls: list[tuple[object, Projector | None, BaseException]] = []

        async def fake_finalize_failure(
            *,
            row: ClaimedOutboxRow,
            event_id: object,
            failing_projector: Projector | None,
            error: BaseException,
        ) -> str:
            calls.append((event_id, failing_projector, error))
            return "dead_lettered"

        runner._finalize_failure = fake_finalize_failure  # type: ignore[method-assign]

        row = _claimed_row()
        outcome = asyncio.run(runner._process_claimed_row(row))
    finally:
        runtime._load_event = original_load_event  # type: ignore[assignment]

    assert outcome == "dead_lettered"
    assert neo4j.write_calls == 0
    [(recorded_event_id, recorded_failing_projector, recorded_error)] = calls
    assert recorded_event_id == row.event_id
    assert recorded_failing_projector is None
    assert isinstance(recorded_error, OrphanedOutboxRowError)


# --- _fenced_update -------------------------------------------------------------


def test_fenced_update_returns_true_when_the_lease_still_matches() -> None:
    session = FakeSession(execute_results=[FakeExecuteResult(scalar=1)])
    runner = ProjectionRunner(FakeSessionFactory([]), FakeNeo4jStore(), [], worker_id="worker-a")

    applied = asyncio.run(
        runner._fenced_update(
            session,  # type: ignore[arg-type]
            outbox_id=1,
            claimed_lease_expires_at=NOW,
            values={"status": OutboxStatus.DELIVERED},
        )
    )

    assert applied is True


def test_fenced_update_returns_false_when_the_lease_was_lost() -> None:
    session = FakeSession(execute_results=[FakeExecuteResult(scalar=None)])
    runner = ProjectionRunner(FakeSessionFactory([]), FakeNeo4jStore(), [], worker_id="worker-a")

    applied = asyncio.run(
        runner._fenced_update(
            session,  # type: ignore[arg-type]
            outbox_id=1,
            claimed_lease_expires_at=NOW,
            values={"status": OutboxStatus.DELIVERED},
        )
    )

    assert applied is False


# --- _finalize_success -----------------------------------------------------------


def test_finalize_success_advances_every_matching_checkpoint_and_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession(execute_results=[FakeExecuteResult(scalar=1)])
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        clock=lambda: NOW,
    )
    advance_calls: list[dict[str, object]] = []

    async def fake_advance(_session: object, **kwargs: object) -> None:
        advance_calls.append(kwargs)

    monkeypatch.setattr(CheckpointRepository, "advance", fake_advance)

    row = _claimed_row()
    event = build_stored_event()
    projectors = (
        RecordingProjector("graph.a", "1", handled_types=["x"]),
        RecordingProjector("graph.b", "1", handled_types=["x"]),
    )

    applied = asyncio.run(runner._finalize_success(row=row, event=event, matching=projectors))

    assert applied is True
    assert session.commit_calls == 1
    assert session.rollback_calls == 0
    assert [call["projector_name"] for call in advance_calls] == ["graph.a", "graph.b"]


def test_finalize_success_rolls_back_and_skips_checkpoints_when_the_lease_was_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession(execute_results=[FakeExecuteResult(scalar=None)])
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        clock=lambda: NOW,
    )
    advance_calls: list[dict[str, object]] = []

    async def fake_advance(_session: object, **kwargs: object) -> None:
        advance_calls.append(kwargs)

    monkeypatch.setattr(CheckpointRepository, "advance", fake_advance)

    row = _claimed_row()
    event = build_stored_event()
    projectors = (RecordingProjector("graph.a", "1", handled_types=["x"]),)

    applied = asyncio.run(runner._finalize_success(row=row, event=event, matching=projectors))

    assert applied is False
    assert session.commit_calls == 0
    assert session.rollback_calls == 1
    assert advance_calls == []


# --- _finalize_failure -----------------------------------------------------------


def test_finalize_failure_schedules_a_retry_below_the_attempt_ceiling() -> None:
    session = FakeSession(execute_results=[FakeExecuteResult(scalar=1)])
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        max_attempts=5,
        base_retry_delay=timedelta(seconds=2),
        clock=lambda: NOW,
    )
    row = _claimed_row(retry_count=1)

    outcome = asyncio.run(
        runner._finalize_failure(
            row=row, event_id=row.event_id, failing_projector=None, error=RuntimeError("boom")
        )
    )

    assert outcome == "retried"
    assert session.commit_calls == 1
    assert len(session.executed_statements) == 1


def test_finalize_failure_dead_letters_at_the_ceiling_without_leaking_the_message() -> None:
    session = FakeSession(
        execute_results=[FakeExecuteResult(scalar=1), FakeExecuteResult(scalar=None)]
    )
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        max_attempts=2,
        clock=lambda: NOW,
    )
    row = _claimed_row(retry_count=1)
    projector = RecordingProjector("graph.a", "1", handled_types=["x"])

    class PoisonError(RuntimeError):
        pass

    outcome = asyncio.run(
        runner._finalize_failure(
            row=row,
            event_id=row.event_id,
            failing_projector=projector,
            error=PoisonError("do not leak this message"),
        )
    )

    assert outcome == "dead_lettered"
    assert session.commit_calls == 1
    [_fenced_statement, dlq_statement] = session.executed_statements
    bound_params = dlq_statement.compile().params
    assert "do not leak this message" not in str(bound_params)
    assert bound_params["error_class"] == "PoisonError"
    assert bound_params["projector_name"] == "graph.a"


def test_finalize_failure_reports_lost_lease_without_writing_a_dead_letter() -> None:
    session = FakeSession(execute_results=[FakeExecuteResult(scalar=None)])
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        max_attempts=1,
        clock=lambda: NOW,
    )
    row = _claimed_row(retry_count=0)

    outcome = asyncio.run(
        runner._finalize_failure(
            row=row, event_id=row.event_id, failing_projector=None, error=RuntimeError("boom")
        )
    )

    assert outcome == "lost_lease"
    assert session.commit_calls == 0
    assert session.rollback_calls == 1
    assert len(session.executed_statements) == 1


def test_finalize_failure_reports_lost_lease_when_a_retry_update_loses_the_fence() -> None:
    session = FakeSession(execute_results=[FakeExecuteResult(scalar=None)])
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        max_attempts=5,
        clock=lambda: NOW,
    )
    row = _claimed_row(retry_count=0)

    outcome = asyncio.run(
        runner._finalize_failure(
            row=row, event_id=row.event_id, failing_projector=None, error=RuntimeError("boom")
        )
    )

    assert outcome == "lost_lease"
    assert session.commit_calls == 0
    assert session.rollback_calls == 1
    assert len(session.executed_statements) == 1


def test_finalize_failure_attributes_unmatched_projector_placeholders_when_unattributed() -> None:
    session = FakeSession(
        execute_results=[FakeExecuteResult(scalar=1), FakeExecuteResult(scalar=None)]
    )
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        max_attempts=1,
        clock=lambda: NOW,
    )
    row = _claimed_row(retry_count=0)

    outcome = asyncio.run(
        runner._finalize_failure(
            row=row,
            event_id=row.event_id,
            failing_projector=None,
            error=OrphanedOutboxRowError("missing"),
        )
    )

    assert outcome == "dead_lettered"
    [_fenced_statement, dlq_statement] = session.executed_statements
    bound_params = dlq_statement.compile().params
    assert bound_params["projector_name"]
    assert bound_params["projector_version"]


# --- _claim_batch -----------------------------------------------------------------


def test_claim_batch_returns_claimed_rows_sorted_by_outbox_id() -> None:
    raw_rows = [_outbox_row(outbox_id=3), _outbox_row(outbox_id=1), _outbox_row(outbox_id=2)]
    session = FakeSession(scalars_results=[FakeScalarResult(raw_rows)])
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        clock=lambda: NOW,
    )

    rows = asyncio.run(runner._claim_batch(5))

    assert [row.outbox_id for row in rows] == [1, 2, 3]
    assert session.commit_calls == 1
    assert all(isinstance(row, ClaimedOutboxRow) for row in rows)
    # `_claim_batch` copies fields out of the raw ORM rows into
    # session-independent snapshots, stamping the exact lease it just
    # computed and wrote -- not whatever `lease_expires_at` happened to be
    # scripted onto the fake pre-claim row.
    assert all(row.lease_expires_at == NOW + timedelta(seconds=30) for row in rows)
    by_id = {row.outbox_id: row for row in rows}
    assert by_id[1].event_id == raw_rows[1].event_id
    assert by_id[1].retry_count == raw_rows[1].retry_count


def test_claim_batch_returns_no_rows_when_nothing_is_claimable() -> None:
    session = FakeSession(scalars_results=[FakeScalarResult([])])
    runner = ProjectionRunner(
        FakeSessionFactory([session]),
        FakeNeo4jStore(),
        [],
        worker_id="worker-a",
        clock=lambda: NOW,
    )

    rows = asyncio.run(runner._claim_batch(5))

    assert rows == []


# --- _load_event -------------------------------------------------------------------


def test_load_event_raises_for_an_orphaned_outbox_row() -> None:
    session = FakeSession(get_results=[None])

    with pytest.raises(OrphanedOutboxRowError):
        asyncio.run(runtime._load_event(session, uuid4()))  # type: ignore[arg-type]


def test_load_event_reconstructs_the_stored_event_without_content_refs() -> None:
    sealed = build_stored_event(event_type="graph.node.created", payload={"key": "value"})
    event_row = _event_row_from(sealed)
    session = FakeSession(get_results=[event_row], scalars_results=[FakeScalarResult([])])

    reconstructed = asyncio.run(runtime._load_event(session, sealed.event_id))  # type: ignore[arg-type]

    assert reconstructed.event_id == sealed.event_id
    assert reconstructed.payload == sealed.payload
    assert reconstructed.content_refs == ()
    assert reconstructed.integrity.event_sha256 == sealed.integrity.event_sha256


def test_load_event_reconstructs_content_refs_when_present() -> None:
    sealed = build_stored_event()
    event_row = _event_row_from(sealed)
    content_ref_row = EventContentRefRow(
        event_id=sealed.event_id,
        content_id="artifact-1",
        content_sha256="a" * 64,
        media_type="text/plain",
        uncompressed_bytes=3,
        disposition=ContentDisposition.SANITIZED,
        storage=ContentStorage.INLINE,
        inline_id="inline-1",
        object_key=None,
        encoding=None,
    )
    session = FakeSession(
        get_results=[event_row], scalars_results=[FakeScalarResult([content_ref_row])]
    )

    reconstructed = asyncio.run(runtime._load_event(session, sealed.event_id))  # type: ignore[arg-type]

    [ref] = reconstructed.content_refs
    assert ref.content_id == "artifact-1"
    assert ref.storage.value == "inline"
    assert ref.disposition.value == "sanitized"


def test_load_event_orders_content_refs_by_code_point_not_sql_collation() -> None:
    from agent_context_sdk import (
        ContentClaimV1,
        ContentRefV1,
        EventDraftV1,
        seal_event,
        verify_event,
    )

    sealed_shape = build_stored_event()
    claims = (
        ContentClaimV1(
            content_id="B-ref",
            content_sha256="b" * 64,
            media_type="text/plain",
            uncompressed_bytes=3,
        ),
        ContentClaimV1(
            content_id="a-ref",
            content_sha256="a" * 64,
            media_type="text/plain",
            uncompressed_bytes=3,
        ),
    )
    draft = EventDraftV1.model_validate(
        {
            **sealed_shape.model_dump(
                mode="json",
                include={
                    "event_type",
                    "stream_id",
                    "occurred_at",
                    "observed_at",
                    "producer",
                    "payload",
                    "redaction",
                    "idempotency_key",
                },
            ),
            "content_claims": [claim.model_dump(mode="json") for claim in claims],
        }
    )
    refs = [
        ContentRefV1(
            **claim.model_dump(),
            disposition="sanitized",
            storage="inline",
            inline_id=f"inline-{claim.content_id}",
        )
        for claim in claims
    ]
    # Sealed in canonical code-point order ("B-ref" < "a-ref"), as the ledger does.
    sealed = seal_event(draft, refs, 1, None)
    rows = [
        EventContentRefRow(
            event_id=sealed.event_id,
            content_id=ref.content_id,
            content_sha256=ref.content_sha256,
            media_type=ref.media_type,
            uncompressed_bytes=ref.uncompressed_bytes,
            disposition=ContentDisposition.SANITIZED,
            storage=ContentStorage.INLINE,
            inline_id=ref.inline_id,
            object_key=None,
            encoding=None,
        )
        for ref in reversed(sealed.content_refs)  # a case-insensitive collation's order
    ]
    session = FakeSession(
        get_results=[_event_row_from(sealed)], scalars_results=[FakeScalarResult(rows)]
    )

    reconstructed = asyncio.run(runtime._load_event(session, sealed.event_id))  # type: ignore[arg-type]

    assert [ref.content_id for ref in reconstructed.content_refs] == ["B-ref", "a-ref"]
    assert verify_event(reconstructed)


def test_process_claimed_row_retries_when_event_reconstruction_fails() -> None:
    async def fake_load_event(_session: object, _event_id: object) -> object:
        raise ValueError("stored event no longer validates")

    original_load_event = runtime._load_event
    runtime._load_event = fake_load_event  # type: ignore[assignment]
    try:
        runner = ProjectionRunner(
            FakeSessionFactory([FakeSession()]),
            FakeNeo4jStore(),
            [RecordingProjector("graph.test", "1", handled_types=["graph.node.created"])],
            worker_id="worker-a",
            clock=lambda: NOW,
        )
        failures: list[tuple[object, BaseException]] = []

        async def fake_finalize_failure(
            *,
            row: ClaimedOutboxRow,
            event_id: object,
            failing_projector: object,
            error: BaseException,
        ) -> str:
            failures.append((failing_projector, error))
            return "retried"

        runner._finalize_failure = fake_finalize_failure  # type: ignore[method-assign]

        outcome = asyncio.run(runner._process_claimed_row(_claimed_row()))
    finally:
        runtime._load_event = original_load_event  # type: ignore[assignment]

    assert outcome == "retried"
    [(failing_projector, error)] = failures
    assert failing_projector is None
    assert isinstance(error, ValueError)


class _Ünïcode_Error(RuntimeError):  # deliberately hostile class name
    pass


class _PrivateError(RuntimeError):
    pass


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ValueError(), "ValueError"),
        (_PrivateError(), "PrivateError"),
        (_Ünïcode_Error(), "n_code_Error"),
        (type("X" * 200, (RuntimeError,), {})(), "X" * 128),
        (type("123", (RuntimeError,), {})(), "UnnamedProjectionError"),
    ],
)
def test_public_error_class_is_always_constraint_safe(error: BaseException, expected: str) -> None:
    assert runtime._public_error_class(error) == expected


class _ExplodingClassifier:
    name = "graph.exploding"
    version = "1"

    def handles(self, event_type: str) -> bool:
        raise KeyError(event_type)

    async def project(self, tx: object, event: object) -> None:  # pragma: no cover
        raise AssertionError("never called")


def test_process_claimed_row_retries_when_a_projector_cannot_classify_the_event() -> None:
    event = build_stored_event(event_type="graph.node.created")

    async def fake_load_event(_session: object, _event_id: object) -> object:
        return event

    original_load_event = runtime._load_event
    runtime._load_event = fake_load_event  # type: ignore[assignment]
    try:
        exploding = _ExplodingClassifier()
        neo4j = FakeNeo4jStore()
        runner = ProjectionRunner(
            FakeSessionFactory([FakeSession()]),
            neo4j,
            [exploding],
            worker_id="worker-a",
            clock=lambda: NOW,
        )
        failures: list[tuple[object, BaseException]] = []

        async def fake_finalize_failure(
            *,
            row: ClaimedOutboxRow,
            event_id: object,
            failing_projector: object,
            error: BaseException,
        ) -> str:
            failures.append((failing_projector, error))
            return "retried"

        runner._finalize_failure = fake_finalize_failure  # type: ignore[method-assign]

        outcome = asyncio.run(runner._process_claimed_row(_claimed_row()))
    finally:
        runtime._load_event = original_load_event  # type: ignore[assignment]

    assert outcome == "retried"
    assert neo4j.write_calls == 0
    [(failing_projector, error)] = failures
    assert failing_projector is exploding
    assert isinstance(error, KeyError)


@pytest.mark.parametrize(
    ("name", "version"),
    [("", "1"), ("n" * 256, "1"), ("graph.test", ""), ("graph.test", "v" * 65)],
)
def test_runner_rejects_projector_identities_that_cannot_be_persisted(
    name: str, version: str
) -> None:
    projector = RecordingProjector(name, version, handled_types=[])
    with pytest.raises(ValueError, match=r"projector (name|version)"):
        ProjectionRunner(
            FakeSessionFactory([]),
            FakeNeo4jStore(),
            [projector],
            worker_id="worker-a",
            clock=lambda: NOW,
        )
