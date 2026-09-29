"""Unit tests for :mod:`agent_context_platform.ledger.repository`.

Every test here runs without a database: pure helpers are exercised
directly, and the async orchestration functions are driven through
``unittest.mock.AsyncMock(spec=AsyncSession)`` -- the same pattern used by
``tests/unit/catalog/test_models.py`` for ``CatalogRepository``. Real
database behavior (locking, SAVEPOINT recovery, least privilege, hash-chain
verification against actual rows) is covered by ``tests/integration/ledger``.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentRefV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    RedactionReportV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content.models import ContentDisposition as SdkContentDisposition
from agent_context_sdk.content.models import ContentStorage as SdkContentStorage
from agent_context_sdk.events.envelope import new_uuid7
from agent_context_sdk.redaction.models import RedactionFindingSummaryV1
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from agent_context_platform.ledger.models import (
    ContentDisposition,
    ContentStorage,
    EventContentRefRow,
    MetadataOnlyReason,
    StreamStatus,
)
from agent_context_platform.ledger.repository import (
    IdempotencyConflictError,
    LedgerRepository,
    ResolvedEvent,
    StreamQuarantinedError,
    _canonicalize_draft,
    _content_ref_from_row,
    _content_ref_rows_from_sealed,
    _draft_identity,
    _event_row_from_sealed,
    _existing_draft_identity,
    _idempotency_lock_key,
    _insert_or_recover,
    _lock_idempotency_keys,
    _lock_streams,
    _outbox_row,
    _plan_batch,
    _recover_existing,
    _redaction_report_rows,
    _row_to_stored_event,
    _StreamState,
    _update_stream_head,
    _validate_resolved_event,
    _violated_constraint_name,
)

pytestmark = pytest.mark.unit

_OCCURRED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _producer(producer_id: str = "agent-1") -> ProducerV1:
    return ProducerV1(producer_id=producer_id, name="Agent One", version="1.0.0")


def _redaction_summary() -> EventRedactionSummaryV1:
    return EventRedactionSummaryV1(
        policy_version="policy-v1",
        disposition=SdkContentDisposition.SANITIZED,
        finding_counts={},
    )


def _sha256(content_id: str) -> str:
    return hashlib.sha256(content_id.encode()).hexdigest()


def _object_key(sha256: str) -> str:
    return f"sha256/{sha256[:2]}/{sha256[2:4]}/{sha256}.zst"


def _claim(content_id: str) -> ContentClaimV1:
    return ContentClaimV1(
        content_id=content_id,
        content_sha256=_sha256(content_id),
        media_type="text/plain",
        uncompressed_bytes=10,
    )


def _content_ref(
    content_id: str, *, disposition: SdkContentDisposition = SdkContentDisposition.SANITIZED
) -> ContentRefV1:
    sha = _sha256(content_id)
    return ContentRefV1(
        content_id=content_id,
        content_sha256=sha,
        media_type="text/plain",
        uncompressed_bytes=10,
        disposition=disposition,
        storage=SdkContentStorage.OBJECT,
        object_key=_object_key(sha),
        encoding="zstd",
    )


def _draft(
    *,
    stream_id: str = "stream-a",
    idempotency_key: str = "key-1",
    claims: tuple[ContentClaimV1, ...] = (),
    payload: dict[str, object] | None = None,
    event_id: UUID | None = None,
    producer_id: str = "agent-1",
) -> EventDraftV1:
    kwargs: dict[str, object] = {
        "event_type": "test.event",
        "stream_id": stream_id,
        "occurred_at": _OCCURRED_AT,
        "observed_at": _OCCURRED_AT,
        "producer": _producer(producer_id),
        "payload": payload if payload is not None else {"hello": "world"},
        "redaction": _redaction_summary(),
        "idempotency_key": idempotency_key,
        "content_claims": claims,
    }
    if event_id is not None:
        kwargs["event_id"] = event_id
    return EventDraftV1(**kwargs)


def _one(value: object) -> MagicMock:
    """Build a fake ``Result`` whose ``.one()`` returns ``value``."""
    result = MagicMock()
    result.one.return_value = value
    return result


def _scalars_all(values: list[object]) -> MagicMock:
    """Build a fake ``Result`` whose ``.scalars().all()`` returns ``values``."""
    result = MagicMock()
    result.scalars.return_value.all.return_value = values
    return result


def _scalar_one(value: object) -> MagicMock:
    """Build a fake ``Result`` whose ``.scalar_one()`` returns ``value``."""
    result = MagicMock()
    result.scalar_one.return_value = value
    return result


def _fake_integrity_error(constraint_name: str | None) -> IntegrityError:
    orig = Exception("duplicate key value violates unique constraint")
    if constraint_name is not None:
        diag = MagicMock()
        diag.constraint_name = constraint_name
        orig.diag = diag  # type: ignore[attr-defined]
    return IntegrityError("INSERT INTO ledger.events ...", {}, orig)


# --------------------------------------------------------------------------
# _canonicalize_draft / _draft_identity / _existing_draft_identity
# --------------------------------------------------------------------------


def test_canonicalize_draft_sorts_claims_by_content_id() -> None:
    draft = _draft(claims=(_claim("b"), _claim("a")))

    canonical = _canonicalize_draft(draft)

    assert [claim.content_id for claim in canonical.content_claims] == ["a", "b"]


def test_canonicalize_draft_leaves_already_sorted_claims_untouched() -> None:
    draft = _draft(claims=(_claim("a"), _claim("b")))

    canonical = _canonicalize_draft(draft)

    assert [claim.content_id for claim in canonical.content_claims] == ["a", "b"]


def test_draft_identity_ignores_claim_submission_order() -> None:
    forward = _draft(claims=(_claim("a"), _claim("b")))
    reversed_claims = _draft(claims=(_claim("b"), _claim("a")))

    assert _draft_identity(forward) == _draft_identity(reversed_claims)


def test_draft_identity_ignores_event_id() -> None:
    first = _draft(event_id=new_uuid7())
    second = _draft(event_id=new_uuid7())

    assert _draft_identity(first) == _draft_identity(second)


def test_draft_identity_detects_payload_difference() -> None:
    first = _draft(payload={"a": 1})
    second = _draft(payload={"a": 2})

    assert _draft_identity(first) != _draft_identity(second)


def test_existing_draft_identity_matches_the_draft_it_was_built_from() -> None:
    draft = _draft(claims=(_claim("b"), _claim("a")))
    sealed = seal_event(_canonicalize_draft(draft), (_content_ref("a"), _content_ref("b")), 1, None)
    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)
    refs = _content_ref_rows_from_sealed(sealed)

    assert _existing_draft_identity(row, refs) == _draft_identity(draft)


# --------------------------------------------------------------------------
# _plan_batch
# --------------------------------------------------------------------------


def test_plan_batch_empty_batch() -> None:
    assert _plan_batch([]) == []


def test_plan_batch_all_distinct_keys_are_their_own_representative() -> None:
    events = [
        ResolvedEvent(draft=_draft(idempotency_key="k1")),
        ResolvedEvent(draft=_draft(idempotency_key="k2")),
        ResolvedEvent(draft=_draft(idempotency_key="k3")),
    ]

    assert _plan_batch(events) == [0, 1, 2]


def test_plan_batch_same_draft_repeated_points_to_first_index() -> None:
    events = [
        ResolvedEvent(draft=_draft(idempotency_key="dup", claims=(_claim("a"), _claim("b")))),
        ResolvedEvent(draft=_draft(idempotency_key="dup", claims=(_claim("b"), _claim("a")))),
    ]

    assert _plan_batch(events) == [0, 0]


def test_plan_batch_conflicting_draft_raises_before_any_db_access() -> None:
    first = _draft(idempotency_key="dup", payload={"a": 1})
    second = _draft(idempotency_key="dup", payload={"a": 2})
    events = [ResolvedEvent(draft=first), ResolvedEvent(draft=second)]

    with pytest.raises(IdempotencyConflictError) as excinfo:
        _plan_batch(events)

    assert excinfo.value.producer_id == "agent-1"
    assert excinfo.value.idempotency_key == "dup"
    assert excinfo.value.existing_event_id == first.event_id


# --------------------------------------------------------------------------
# _validate_resolved_event
# --------------------------------------------------------------------------


def test_validate_resolved_event_allows_reports_matching_known_content_ids() -> None:
    report = RedactionReportV1(policy_version="v1", disposition=SdkContentDisposition.SANITIZED)
    resolved = ResolvedEvent(
        draft=_draft(claims=(_claim("a"),)),
        content_refs=(_content_ref("a"),),
        redaction_reports={"a": report},
    )

    _validate_resolved_event(resolved)  # must not raise


def test_validate_resolved_event_rejects_unknown_redaction_content_id() -> None:
    report = RedactionReportV1(policy_version="v1", disposition=SdkContentDisposition.SANITIZED)
    resolved = ResolvedEvent(
        draft=_draft(claims=(_claim("a"),)),
        content_refs=(_content_ref("a"),),
        redaction_reports={"missing": report},
    )

    with pytest.raises(ValueError, match="redaction_reports"):
        _validate_resolved_event(resolved)


# --------------------------------------------------------------------------
# _violated_constraint_name
# --------------------------------------------------------------------------


def test_violated_constraint_name_reads_psycopg_diag() -> None:
    error = _fake_integrity_error("uq_events_producer_id")

    assert _violated_constraint_name(error) == "uq_events_producer_id"


def test_violated_constraint_name_returns_none_without_diag() -> None:
    error = _fake_integrity_error(None)

    assert _violated_constraint_name(error) is None


# --------------------------------------------------------------------------
# row <-> SDK mapping helpers
# --------------------------------------------------------------------------


def test_content_ref_from_row_exposes_enum_members_as_plain_strings() -> None:
    row = EventContentRefRow(
        event_id=uuid4(),
        content_id="a",
        content_sha256=_sha256("a"),
        media_type="text/plain",
        uncompressed_bytes=10,
        disposition=ContentDisposition.SANITIZED,
        storage=ContentStorage.OBJECT,
        inline_id=None,
        object_key=_object_key(_sha256("a")),
        encoding="zstd",
    )

    mapped = _content_ref_from_row(row)

    assert mapped["disposition"] == "sanitized"
    assert mapped["storage"] == "object"
    assert mapped["content_id"] == "a"


def test_row_to_stored_event_resorts_refs_by_content_id() -> None:
    draft = _draft(claims=(_claim("a"), _claim("b")))
    sealed = seal_event(draft, (_content_ref("a"), _content_ref("b")), 1, None)
    event_row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)
    # Rows come back from the database in an arbitrary order; feed them in
    # reverse to prove the function re-sorts rather than trusting input order.
    ref_rows = list(reversed(_content_ref_rows_from_sealed(sealed)))

    reconstructed = _row_to_stored_event(event_row, ref_rows)

    assert [ref.content_id for ref in reconstructed.content_refs] == ["a", "b"]
    assert reconstructed.event_id == sealed.event_id
    assert reconstructed.integrity.event_sha256 == sealed.integrity.event_sha256


def test_event_row_from_sealed_maps_scalar_and_json_fields() -> None:
    draft = _draft()
    sealed = seal_event(draft, (), 1, None)

    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)

    assert row.event_id == sealed.event_id
    assert row.stream_sequence == 1
    assert row.producer_id == "agent-1"
    assert row.payload == {"hello": "world"}
    assert row.payload_sha256 == sealed.integrity.payload_sha256
    assert row.previous_event_sha256 is None
    assert row.recorded_at == _OCCURRED_AT


def test_event_row_from_sealed_serializes_trace_as_none_when_absent() -> None:
    draft = _draft()
    sealed = seal_event(draft, (), 1, None)

    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)

    assert row.trace is None


def test_content_ref_rows_from_sealed_round_trips_ledger_enums() -> None:
    draft = _draft(claims=(_claim("a"),))
    sealed = seal_event(draft, (_content_ref("a"),), 1, None)

    rows = _content_ref_rows_from_sealed(sealed)

    assert len(rows) == 1
    assert rows[0].disposition is ContentDisposition.SANITIZED
    assert rows[0].storage is ContentStorage.OBJECT
    assert rows[0].object_key == _object_key(_sha256("a"))


def test_redaction_report_rows_maps_findings_and_metadata_only_reason() -> None:
    finding = RedactionFindingSummaryV1(
        detector="email", detector_version="1.0.0", category="pii", count=2
    )
    report = RedactionReportV1(
        policy_version="policy-v1",
        disposition=SdkContentDisposition.METADATA_ONLY,
        findings=(finding,),
        metadata_only_reason="source_unmapped",
    )
    event_id = uuid4()

    rows = _redaction_report_rows(event_id, {"a": report})

    assert len(rows) == 1
    row = rows[0]
    assert row.event_id == event_id
    assert row.content_id == "a"
    assert row.disposition is ContentDisposition.METADATA_ONLY
    assert row.metadata_only_reason is MetadataOnlyReason.SOURCE_UNMAPPED
    assert row.findings == [
        {
            "detector": "email",
            "detector_version": "1.0.0",
            "category": "pii",
            "count": 2,
            "correlation_labels": [],
        }
    ]


def test_redaction_report_rows_leaves_metadata_only_reason_none_when_absent() -> None:
    report = RedactionReportV1(policy_version="v1", disposition=SdkContentDisposition.SANITIZED)

    rows = _redaction_report_rows(uuid4(), {"a": report})

    assert rows[0].metadata_only_reason is None


def test_outbox_row_defaults_to_pending_and_zero_retries() -> None:
    event_id = uuid4()

    row = _outbox_row(event_id, now=_OCCURRED_AT)

    assert row.event_id == event_id
    assert row.status.value == "pending"
    assert row.retry_count == 0
    assert row.available_at == _OCCURRED_AT
    assert row.lease_owner is None
    assert row.created_at == row.updated_at == _OCCURRED_AT


# --------------------------------------------------------------------------
# _lock_streams / _update_stream_head
# --------------------------------------------------------------------------


def test_lock_streams_refuses_a_quarantined_stream() -> None:
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [
        MagicMock(),  # INSERT ... ON CONFLICT DO NOTHING for "stream-q"
        _one((4, uuid4(), "e" * 64, StreamStatus.QUARANTINED)),  # SELECT ... FOR UPDATE
    ]

    with pytest.raises(StreamQuarantinedError) as caught:
        asyncio.run(_lock_streams(session, ["stream-q"]))

    assert caught.value.stream_id == "stream-q"
    assert session.execute.await_count == 2


def test_lock_streams_locks_every_distinct_stream_in_sorted_order() -> None:
    session = AsyncMock(spec=AsyncSession)
    existing_head = uuid4()
    session.execute.side_effect = [
        MagicMock(),  # INSERT ... ON CONFLICT DO NOTHING for "stream-a"
        _one((0, None, None, StreamStatus.ACTIVE)),  # SELECT ... FOR UPDATE for "stream-a"
        MagicMock(),  # INSERT ... ON CONFLICT DO NOTHING for "stream-b"
        _one(
            (3, existing_head, "f" * 64, StreamStatus.ACTIVE)
        ),  # SELECT ... FOR UPDATE for "stream-b"
    ]

    result = asyncio.run(_lock_streams(session, ["stream-b", "stream-a", "stream-b"]))

    assert result == {
        "stream-a": _StreamState(0, None, None),
        "stream-b": _StreamState(3, existing_head, "f" * 64),
    }
    assert session.execute.await_count == 4


def test_idempotency_lock_key_is_a_stable_signed_int4() -> None:
    # Pinned values: API replicas on different versions during a rollout
    # must derive the same lock for the same key.
    assert _idempotency_lock_key("agent-1", "k1") == -1529213843
    assert _idempotency_lock_key("agent-1", "k2") == 1840744207
    assert _idempotency_lock_key("agent-2", "k1") != _idempotency_lock_key("agent-1", "k1")


def test_lock_idempotency_keys_locks_each_distinct_key_once_in_lock_key_order() -> None:
    session = AsyncMock(spec=AsyncSession)
    keys = [("agent-1", "dup"), ("agent-1", "k1"), ("agent-1", "dup")]

    asyncio.run(_lock_idempotency_keys(session, keys))

    # "dup" sorts before "k1" as a string, but its lock key is larger.
    calls = session.execute.await_args_list
    assert [call.args[1] for call in calls] == [
        {"namespace": 0x4944_454D, "lock_key": _idempotency_lock_key("agent-1", "k1")},
        {"namespace": 0x4944_454D, "lock_key": _idempotency_lock_key("agent-1", "dup")},
    ]
    assert _idempotency_lock_key("agent-1", "k1") < _idempotency_lock_key("agent-1", "dup")
    assert all("pg_advisory_xact_lock" in str(call.args[0]) for call in calls)


def test_update_stream_head_issues_one_update(monkeypatch: pytest.MonkeyPatch) -> None:
    session = AsyncMock(spec=AsyncSession)
    state = _StreamState(sequence=2, head_event_id=uuid4(), head_sha256="a" * 64)

    asyncio.run(_update_stream_head(session, "stream-a", state))

    assert session.execute.await_count == 1
    statement = session.execute.await_args.args[0]
    compiled = str(statement)
    assert "event_streams" in compiled
    assert "last_sequence" in compiled


# --------------------------------------------------------------------------
# _recover_existing
# --------------------------------------------------------------------------


def test_recover_existing_returns_stored_event_for_matching_draft() -> None:
    draft = _draft(claims=(_claim("a"),))
    sealed = seal_event(draft, (_content_ref("a"),), 1, None)
    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)
    refs = _content_ref_rows_from_sealed(sealed)

    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [_scalar_one(row), _scalars_all(refs)]

    result = asyncio.run(_recover_existing(session, draft))

    assert result.event_id == sealed.event_id
    assert result.integrity.event_sha256 == sealed.integrity.event_sha256


def test_recover_existing_raises_for_conflicting_draft() -> None:
    stored_draft = _draft(payload={"a": 1})
    sealed = seal_event(stored_draft, (), 1, None)
    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)

    conflicting_draft = _draft(payload={"a": 2})
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [_scalar_one(row), _scalars_all([])]

    with pytest.raises(IdempotencyConflictError):
        asyncio.run(_recover_existing(session, conflicting_draft))


def test_recover_existing_looks_up_by_event_id_when_given() -> None:
    """A literal same-``event_id`` resubmission (the ``pk_events`` conflict
    path) must look up by ``event_id``, not by ``(producer_id,
    idempotency_key)`` -- so it never risks a ``NoResultFound`` on a lookup
    key that was not actually violated.
    """
    draft = _draft(claims=(_claim("a"),))
    sealed = seal_event(draft, (_content_ref("a"),), 1, None)
    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)
    refs = _content_ref_rows_from_sealed(sealed)

    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [_scalar_one(row), _scalars_all(refs)]

    result = asyncio.run(_recover_existing(session, draft, event_id=sealed.event_id))

    assert result.event_id == sealed.event_id
    statement = session.execute.await_args_list[0].args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    where_clause = sql.split("WHERE", 1)[1]
    assert "events.event_id = " in where_clause
    assert "producer_id" not in where_clause


def test_recover_existing_raises_for_conflicting_draft_by_event_id() -> None:
    stored_draft = _draft(payload={"a": 1})
    sealed = seal_event(stored_draft, (), 1, None)
    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)

    conflicting_draft = _draft(payload={"a": 2})
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [_scalar_one(row), _scalars_all([])]

    with pytest.raises(IdempotencyConflictError):
        asyncio.run(_recover_existing(session, conflicting_draft, event_id=sealed.event_id))


# --------------------------------------------------------------------------
# _insert_or_recover
# --------------------------------------------------------------------------


def test_insert_or_recover_happy_path_advances_stream_state() -> None:
    draft = _draft(claims=(_claim("a"),))
    resolved = ResolvedEvent(draft=draft, content_refs=(_content_ref("a"),))
    canonical_draft = _canonicalize_draft(draft)
    stream_state = {"stream-a": _StreamState(0, None, None)}
    touched: set[str] = set()
    session = AsyncMock(spec=AsyncSession)

    result = asyncio.run(
        _insert_or_recover(session, resolved, canonical_draft, stream_state, touched)
    )

    assert isinstance(result.stored, StoredEventV1)
    assert result.created is True
    assert result.stored.stream_sequence == 1
    assert stream_state["stream-a"].sequence == 1
    assert stream_state["stream-a"].head_event_id == result.stored.event_id
    assert touched == {"stream-a"}
    assert session.add.call_count == 2  # event row + outbox row
    assert session.add_all.call_count == 2  # content refs + redaction reports
    assert session.flush.await_count == 3  # events, then content refs, then reports+outbox


def test_insert_or_recover_recovers_on_idempotency_conflict() -> None:
    draft = _draft(claims=(_claim("a"),))
    resolved = ResolvedEvent(draft=draft, content_refs=(_content_ref("a"),))
    canonical_draft = _canonicalize_draft(draft)
    stream_state = {"stream-a": _StreamState(0, None, None)}
    touched: set[str] = set()

    existing_sealed = seal_event(draft, (_content_ref("a"),), 1, None)
    existing_row = _event_row_from_sealed(existing_sealed, recorded_at=_OCCURRED_AT)
    existing_refs = _content_ref_rows_from_sealed(existing_sealed)

    session = AsyncMock(spec=AsyncSession)
    session.flush.side_effect = _fake_integrity_error("uq_events_producer_id")
    session.execute.side_effect = [_scalar_one(existing_row), _scalars_all(existing_refs)]

    result = asyncio.run(
        _insert_or_recover(session, resolved, canonical_draft, stream_state, touched)
    )

    assert result.stored.event_id == existing_sealed.event_id
    assert result.created is False
    # No sequence allocated for the recovered duplicate.
    assert stream_state["stream-a"].sequence == 0
    assert touched == set()
    assert session.execute.await_count == 2


def test_insert_or_recover_recovers_on_event_id_primary_key_conflict() -> None:
    """100 concurrent callers submitting the literal same draft (same
    ``event_id``) hit ``pk_events``, not ``uq_events_producer_id`` -- this
    must recover exactly like the idempotency-key conflict path.
    """
    draft = _draft(claims=(_claim("a"),))
    resolved = ResolvedEvent(draft=draft, content_refs=(_content_ref("a"),))
    canonical_draft = _canonicalize_draft(draft)
    stream_state = {"stream-a": _StreamState(0, None, None)}
    touched: set[str] = set()

    existing_sealed = seal_event(draft, (_content_ref("a"),), 1, None)
    existing_row = _event_row_from_sealed(existing_sealed, recorded_at=_OCCURRED_AT)
    existing_refs = _content_ref_rows_from_sealed(existing_sealed)

    session = AsyncMock(spec=AsyncSession)
    session.flush.side_effect = _fake_integrity_error("pk_events")
    session.execute.side_effect = [_scalar_one(existing_row), _scalars_all(existing_refs)]

    result = asyncio.run(
        _insert_or_recover(session, resolved, canonical_draft, stream_state, touched)
    )

    assert result.stored.event_id == existing_sealed.event_id
    assert result.created is False
    assert stream_state["stream-a"].sequence == 0
    assert touched == set()


def test_insert_or_recover_reraises_unrelated_integrity_errors() -> None:
    draft = _draft(claims=(_claim("a"),))
    resolved = ResolvedEvent(draft=draft, content_refs=(_content_ref("a"),))
    canonical_draft = _canonicalize_draft(draft)
    stream_state = {"stream-a": _StreamState(0, None, None)}
    touched: set[str] = set()

    session = AsyncMock(spec=AsyncSession)
    session.flush.side_effect = _fake_integrity_error("uq_events_stream_id")

    with pytest.raises(IntegrityError):
        asyncio.run(_insert_or_recover(session, resolved, canonical_draft, stream_state, touched))


# --------------------------------------------------------------------------
# LedgerRepository.append
# --------------------------------------------------------------------------


def test_append_returns_immediately_for_an_empty_batch() -> None:
    session = AsyncMock(spec=AsyncSession)

    result = asyncio.run(LedgerRepository.append(session, []))

    assert result == []
    session.execute.assert_not_awaited()


def test_append_validates_every_event_before_touching_the_session() -> None:
    report = RedactionReportV1(policy_version="v1", disposition=SdkContentDisposition.SANITIZED)
    bad = ResolvedEvent(draft=_draft(), content_refs=(), redaction_reports={"missing": report})
    session = AsyncMock(spec=AsyncSession)

    with pytest.raises(ValueError, match="redaction_reports"):
        asyncio.run(LedgerRepository.append(session, [bad]))

    session.execute.assert_not_awaited()


def test_append_rejects_conflicting_batch_local_duplicate_before_touching_the_session() -> None:
    first = ResolvedEvent(draft=_draft(idempotency_key="dup", payload={"a": 1}))
    second = ResolvedEvent(draft=_draft(idempotency_key="dup", payload={"a": 2}))
    session = AsyncMock(spec=AsyncSession)

    with pytest.raises(IdempotencyConflictError):
        asyncio.run(LedgerRepository.append(session, [first, second]))

    session.execute.assert_not_awaited()


def test_append_chains_two_new_events_on_the_same_stream() -> None:
    first = ResolvedEvent(draft=_draft(idempotency_key="k1"))
    second = ResolvedEvent(draft=_draft(idempotency_key="k2"))
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [
        MagicMock(),  # lock insert
        _one((0, None, None, StreamStatus.ACTIVE)),  # lock select
        MagicMock(),  # idempotency lock for "k1"
        MagicMock(),  # idempotency lock for "k2"
        MagicMock(),  # head update
    ]

    results = asyncio.run(LedgerRepository.append(session, [first, second]))

    assert len(results) == 2
    assert results[0].stream_sequence == 1
    assert results[1].stream_sequence == 2
    assert results[1].integrity.previous_event_sha256 == results[0].integrity.event_sha256
    assert session.execute.await_count == 5


def test_append_deduplicates_batch_local_repeats_without_a_second_insert() -> None:
    draft = _draft(idempotency_key="dup", claims=(_claim("a"), _claim("b")))
    reordered = _draft(idempotency_key="dup", claims=(_claim("b"), _claim("a")))
    first = ResolvedEvent(draft=draft, content_refs=(_content_ref("a"), _content_ref("b")))
    second = ResolvedEvent(draft=reordered, content_refs=(_content_ref("b"), _content_ref("a")))
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [
        MagicMock(),  # lock insert
        _one((0, None, None, StreamStatus.ACTIVE)),  # lock select
        MagicMock(),  # one idempotency lock for the repeated "dup"
        MagicMock(),  # head update
    ]

    results = asyncio.run(LedgerRepository.append(session, [first, second]))

    assert results[0].event_id == results[1].event_id
    assert session.add.call_count == 2  # one event row + one outbox row, not two
    assert session.execute.await_count == 4


# --------------------------------------------------------------------------
# LedgerRepository.get_by_idempotency_keys
# --------------------------------------------------------------------------


def test_get_by_idempotency_keys_short_circuits_for_no_keys() -> None:
    session = AsyncMock(spec=AsyncSession)

    result = asyncio.run(LedgerRepository.get_by_idempotency_keys(session, []))

    assert result == {}
    session.execute.assert_not_awaited()


def test_get_by_idempotency_keys_short_circuits_when_nothing_matches() -> None:
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [_scalars_all([])]

    result = asyncio.run(
        LedgerRepository.get_by_idempotency_keys(session, [("agent-1", "missing")])
    )

    assert result == {}
    assert session.execute.await_count == 1


def test_get_by_idempotency_keys_maps_rows_and_sorts_their_refs() -> None:
    draft = _draft(idempotency_key="k1", claims=(_claim("a"), _claim("b")))
    sealed = seal_event(draft, (_content_ref("a"), _content_ref("b")), 1, None)
    row = _event_row_from_sealed(sealed, recorded_at=_OCCURRED_AT)
    refs = list(reversed(_content_ref_rows_from_sealed(sealed)))

    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [_scalars_all([row]), _scalars_all(refs)]

    result = asyncio.run(LedgerRepository.get_by_idempotency_keys(session, [("agent-1", "k1")]))

    stored = result[("agent-1", "k1")]
    assert [ref.content_id for ref in stored.content_refs] == ["a", "b"]
