from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.ids import new_uuid7
from sqlalchemy.exc import OperationalError

from agent_context_platform.content.blob_store import BlobNotFoundError
from agent_context_platform.content.service import (
    ContentRequiresRedactionError,
    ContentResolutionError,
)
from agent_context_platform.ledger.repository import (
    IdempotencyConflictError,
    LedgerRepository,
    ResolvedEvent,
    StreamQuarantinedError,
)
from agent_context_platform.ledger.service import IngestionService, _same_event

pytestmark = pytest.mark.unit

OBSERVED = datetime(2026, 1, 1, tzinfo=UTC)


def _draft(key: str, *, stream_id: str = "stream-1", claims: tuple[ContentClaimV1, ...] = ()):
    return EventDraftV1(
        event_type="agent.session.started",
        stream_id=stream_id,
        occurred_at=OBSERVED,
        observed_at=OBSERVED,
        producer=ProducerV1(producer_id="p1", name="Codex", version="1.0.0"),
        payload={"source": "codex", "session_id": "s"},
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
        ),
        idempotency_key=key,
        content_claims=claims,
    )


def _batch(*drafts: EventDraftV1) -> IngestBatchRequestV1:
    return IngestBatchRequestV1(batch_id=new_uuid7(), events=drafts)


def _stored(draft: EventDraftV1, sequence: int, *, event_id: UUID | None = None) -> StoredEventV1:
    sealed = seal_event(draft, (), sequence, None if sequence == 1 else "0" * 64)
    return sealed if event_id is None else sealed.model_copy(update={"event_id": event_id})


class FakeTransaction:
    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> None:
        self._session.log.append("begin")

    async def __aexit__(self, exc_type: object, *_: object) -> None:
        self._session.log.append("rollback" if exc_type else "commit")


class FakeSession:
    def __init__(self, log: list[str], created: set[UUID]) -> None:
        self.log = log
        self._created = created

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def begin(self) -> FakeTransaction:
        return FakeTransaction(self)

    async def execute(self, _statement: object, parameters: dict[str, Any]) -> list[tuple[UUID]]:
        return [(event_id,) for event_id in parameters["event_ids"] if event_id in self._created]


class FakeContent:
    def __init__(self, log: list[str], prepare_error: Exception | None = None) -> None:
        self._log = log
        self._prepare_error = prepare_error
        self.attach_error: Exception | None = None

    async def prepare(self, items: Sequence[object]) -> FakePrepared:
        self._log.append("prepare")
        if self._prepare_error is not None:
            raise self._prepare_error
        return FakePrepared()

    async def attach(self, _session: object, _prepared: object) -> None:
        self._log.append("attach")
        if self.attach_error is not None:
            raise self.attach_error


class FakePrepared:
    def resolve(self, claims: Sequence[ContentClaimV1]) -> tuple[()]:
        assert not claims
        return ()

    def report_for(self, _content_id: str) -> None:
        raise AssertionError("no content")


class Harness:
    def __init__(self, *, prepare_error: Exception | None = None) -> None:
        self.log: list[str] = []
        self.created: set[UUID] = set()
        self.content = FakeContent(self.log, prepare_error)
        self.service = IngestionService(self.content, lambda: FakeSession(self.log, self.created))  # type: ignore[arg-type]
        self.existing: dict[tuple[str, str], StoredEventV1] = {}
        self.append_result: list[StoredEventV1] | Exception = []


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> Harness:
    state = Harness()
    return _install(state, monkeypatch)


def _install(state: Harness, monkeypatch: pytest.MonkeyPatch) -> Harness:
    async def append(_session: object, resolved: Sequence[ResolvedEvent]) -> list[StoredEventV1]:
        state.log.append("append")
        if isinstance(state.append_result, Exception):
            raise state.append_result
        return state.append_result

    async def lookup(_session: object, _keys: object) -> dict[tuple[str, str], StoredEventV1]:
        state.log.append("lookup")
        return state.existing

    monkeypatch.setattr(LedgerRepository, "append", staticmethod(append))
    monkeypatch.setattr(LedgerRepository, "get_by_idempotency_keys", staticmethod(lookup))
    return state


def _run(harness: Harness, batch: IngestBatchRequestV1):
    return asyncio.run(harness.service.ingest(batch))


def test_prepare_then_attach_then_append_in_one_transaction(harness: Harness) -> None:
    draft = _draft("k1")
    stored = _stored(draft, 1)
    harness.created = {stored.event_id}
    harness.append_result = [stored]

    outcome = _run(harness, _batch(draft))

    assert harness.log == ["prepare", "begin", "attach", "append", "commit"]
    assert outcome.http_status == 200
    [accepted] = outcome.response.accepted
    assert (accepted.event_id, accepted.status, accepted.stream_sequence) == (
        stored.event_id,
        "accepted",
        1,
    )


def test_events_stored_by_another_transaction_are_reported_existing_with_stored_id(
    harness: Harness,
) -> None:
    fresh = _draft("k1")
    replay = _draft("k2")
    stored_fresh = _stored(fresh, 1)
    stored_replay = _stored(replay, 2, event_id=new_uuid7())
    harness.created = {stored_fresh.event_id}
    harness.append_result = [stored_fresh, stored_replay]

    outcome = _run(harness, _batch(fresh, replay))

    assert [(item.event_id, item.status) for item in outcome.response.accepted] == [
        (stored_fresh.event_id, "accepted"),
        (stored_replay.event_id, "existing"),
    ]
    assert outcome.response.accepted[1].stream_sequence == 2


def test_duplicate_idempotency_keys_in_one_batch_are_rejected_before_any_work(
    harness: Harness,
) -> None:
    first, second = _draft("same"), _draft("same")

    outcome = _run(harness, _batch(first, second))

    assert outcome.http_status == 422
    assert {item.error_code for item in outcome.response.rejected} == {"duplicate_idempotency_key"}
    assert "prepare" not in harness.log


def test_content_failure_rejects_every_new_event_and_keeps_existing_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(Harness(prepare_error=ContentRequiresRedactionError("x")), monkeypatch)
    old, new = _draft("k1"), _draft("k2")
    harness.existing = {("p1", "k1"): _stored(old, 1, event_id=new_uuid7())}

    outcome = _run(harness, _batch(old, new))

    assert outcome.http_status == 422
    [existing] = outcome.response.accepted
    assert existing.status == "existing"
    [rejected] = outcome.response.rejected
    assert (rejected.event_id, rejected.error_code, rejected.retryable) == (
        new.event_id,
        "content_requires_redaction",
        False,
    )
    assert "begin" not in harness.log


def test_resolution_mismatch_inside_the_transaction_rolls_back(harness: Harness) -> None:
    harness.append_result = ContentResolutionError("x")

    outcome = _run(harness, _batch(_draft("k1")))

    assert outcome.http_status == 422
    assert outcome.response.rejected[0].error_code == "content_resolution_mismatch"
    assert "rollback" in harness.log


def test_idempotency_conflict_names_only_the_offender(harness: Harness) -> None:
    good, bad = _draft("k1"), _draft("k2")
    harness.append_result = IdempotencyConflictError(
        producer_id="p1", idempotency_key="k2", existing_event_id=new_uuid7()
    )

    outcome = _run(harness, _batch(good, bad))

    assert outcome.http_status == 409
    codes = {item.event_id: item.error_code for item in outcome.response.rejected}
    assert codes == {good.event_id: "batch_rejected", bad.event_id: "idempotency_conflict"}
    assert all(item.retryable is False for item in outcome.response.rejected)
    assert harness.log[-2] == "rollback"


def test_quarantined_stream_rejects_events_on_that_stream(harness: Harness) -> None:
    fine = _draft("k1", stream_id="ok")
    stuck = _draft("k2", stream_id="quarantined")
    harness.append_result = StreamQuarantinedError("quarantined")

    outcome = _run(harness, _batch(fine, stuck))

    assert outcome.http_status == 409
    codes = {item.event_id: item.error_code for item in outcome.response.rejected}
    assert codes == {fine.event_id: "batch_rejected", stuck.event_id: "stream_quarantined"}


@pytest.mark.parametrize(
    "error", [BlobNotFoundError("x"), OperationalError("SELECT 1", {}, Exception("boom"))]
)
def test_transient_storage_and_database_failures_are_retryable_503s(
    harness: Harness, error: Exception
) -> None:
    harness.content.attach_error = error

    outcome = _run(harness, _batch(_draft("k1")))

    assert outcome.http_status == 503
    assert outcome.retry_after_seconds == 5
    assert [(r.error_code, r.retryable) for r in outcome.response.rejected] == [
        ("service_unavailable", True)
    ]
    assert not outcome.response.accepted


def test_existing_lookup_failure_degrades_to_no_existing_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install(Harness(prepare_error=ContentRequiresRedactionError("x")), monkeypatch)

    async def broken(*_: object) -> None:
        raise OperationalError("SELECT 1", {}, Exception("down"))

    monkeypatch.setattr(LedgerRepository, "get_by_idempotency_keys", staticmethod(broken))

    outcome = _run(harness, _batch(_draft("k1")))

    assert outcome.http_status == 422
    assert not outcome.response.accepted


def test_same_event_ignores_event_id_and_claim_order_but_not_content() -> None:
    draft = _draft("k1")
    stored = _stored(draft, 1, event_id=new_uuid7())

    assert _same_event(draft, stored)
    assert not _same_event(_draft("k1", stream_id="other"), stored)
    claim = ContentClaimV1(
        content_id="c", content_sha256="a" * 64, media_type="text/plain", uncompressed_bytes=1
    )
    assert not _same_event(_draft("k1", claims=(claim,)), stored)
