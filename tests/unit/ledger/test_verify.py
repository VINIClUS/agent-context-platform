"""Each ledger integrity check against rows built by the real sealing code (PLATFORM-056)."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    ContentRefV1,
    ContentStorage,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.canonical import canonical_json_bytes, sha256_hex
from agent_context_sdk.events.envelope import _event_sha256, _serialized_payload
from sqlalchemy.exc import OperationalError, ProgrammingError
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.ledger.models import EventContentRefRow, EventRow, EventStreamRow
from agent_context_platform.ledger.repository import (
    _content_ref_rows_from_sealed,
    _event_row_from_sealed,
    _row_to_stored_event,
)
from agent_context_platform.ledger.verify import (
    CHECKS,
    IntegrityFailure,
    LedgerVerification,
    StreamVerification,
    _check_event,
    _check_head,
)
from agent_context_platform.projection.verify import MissingGrantError

pytestmark = pytest.mark.unit

T0 = datetime(2026, 9, 1, tzinfo=UTC)
PRODUCER = ProducerV1(producer_id="verify-test", name="Codex", version="1.0.0")


def _draft(number: int, body: bytes | None = None) -> tuple[EventDraftV1, list[ContentRefV1]]:
    claims: tuple[ContentClaimV1, ...] = ()
    refs: list[ContentRefV1] = []
    payload: dict[str, Any] = {"source": "codex", "session_id": "s-1", "turn_id": f"t-{number}"}
    if body is not None:
        digest = hashlib.sha256(body).hexdigest()
        claims = (
            ContentClaimV1(
                content_id=f"c-{number}",
                content_sha256=digest,
                media_type="text/plain",
                uncompressed_bytes=len(body),
            ),
        )
        refs = [
            ContentRefV1(
                content_id=f"c-{number}",
                content_sha256=digest,
                media_type="text/plain",
                uncompressed_bytes=len(body),
                disposition=ContentDisposition.SANITIZED,
                storage=ContentStorage.INLINE,
                inline_id=digest,
            )
        ]
        payload["user_message_content_id"] = f"c-{number}"
    moment = T0 + timedelta(seconds=number)
    draft = EventDraftV1(
        event_type="agent.turn.started",
        stream_id="stream-1",
        occurred_at=moment,
        observed_at=moment,
        producer=PRODUCER,
        payload=payload,
        context=EventContextV1(session_id="s-1", turn_id=f"t-{number}"),
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"key-{number}",
        content_claims=claims,
    )
    return draft, refs


def chain(length: int = 3) -> list[tuple[EventRow, list[EventContentRefRow]]]:
    """`length` events of one stream sealed by `seal_event`, as the repository stores them."""
    rows = []
    previous: str | None = None
    for number in range(1, length + 1):
        draft, refs = _draft(number, b"sanitized text %d" % number if number % 2 else None)
        sealed: StoredEventV1 = seal_event(draft, refs, number, previous)
        previous = sealed.integrity.event_sha256
        rows.append(
            (
                _event_row_from_sealed(sealed, recorded_at=T0),
                _content_ref_rows_from_sealed(sealed),
            )
        )
    return rows


def walk(rows: list[tuple[EventRow, list[EventContentRefRow]]]) -> IntegrityFailure | None:
    expected, previous = 1, None
    refs = {row.event_id: row_refs for row, row_refs in rows}
    for row, _row_refs in rows:
        failure = _check_event(row, expected, previous, refs)
        if failure is not None:
            return failure
        expected, previous = row.stream_sequence + 1, row.event_sha256
    return None


def test_a_sealed_chain_verifies() -> None:
    assert walk(chain()) is None


def test_a_changed_payload_fails_payload_sha256() -> None:
    rows = chain()
    row = rows[1][0]
    row.payload = {**row.payload, "turn_id": "t-edited"}

    failure = walk(rows)

    assert failure is not None
    assert (failure.check, failure.stream_sequence) == ("payload_sha256", 2)
    assert failure.actual == row.payload_sha256 and failure.expected != failure.actual


def test_a_replaced_middle_digest_with_the_next_link_repaired_fails_event_sha256() -> None:
    rows = chain()
    forged = "f" * 64
    original = rows[1][0].event_sha256
    rows[1][0].event_sha256 = forged
    rows[2][0].previous_event_sha256 = forged  # the next link is repaired to match

    failure = walk(rows)

    assert failure is not None
    assert (failure.check, failure.stream_sequence) == ("event_sha256", 2)
    assert (failure.expected, failure.actual) == (original, forged)


def test_a_broken_link_fails_previous_event_sha256() -> None:
    rows = chain()
    rows[2][0].previous_event_sha256 = "a" * 64

    failure = walk(rows)

    assert failure is not None
    assert (failure.check, failure.stream_sequence) == ("previous_event_sha256", 3)
    assert failure.expected == rows[1][0].event_sha256


def test_a_first_event_with_a_link_fails_previous_event_sha256() -> None:
    rows = chain(1)
    rows[0][0].previous_event_sha256 = "a" * 64

    failure = walk(rows)

    assert failure is not None and failure.check == "previous_event_sha256"
    assert failure.expected is None


@pytest.mark.parametrize("sequence", [3, 1])  # a gap, then a duplicate of the first
def test_a_gap_or_duplicate_fails_sequence(sequence: int) -> None:
    rows = chain()
    rows[1][0].stream_sequence = sequence

    failure = walk(rows)

    assert failure is not None
    assert (failure.check, failure.expected, failure.actual) == ("sequence", "2", str(sequence))


def test_an_edited_content_ref_digest_fails_event_sha256() -> None:
    rows = chain()
    rows[0][1][0].content_sha256 = "b" * 64  # the refs are part of the sealed envelope

    failure = walk(rows)

    assert failure is not None and (failure.check, failure.stream_sequence) == ("event_sha256", 1)


def test_purged_content_keeps_its_ref_and_still_verifies() -> None:
    # Purge deletes stored bytes and appends a tombstone; it never touches an event or its refs,
    # and the verifier never reads content bytes.
    rows = chain()

    assert rows[0][1] and walk(rows) is None


def test_rows_that_do_not_rebuild_an_event_fail_event_decode() -> None:
    rows = chain()
    rows[1][0].context = {"unexpected_field": True}

    failure = walk(rows)

    assert failure is not None and (failure.check, failure.stream_sequence) == ("event_decode", 2)
    assert failure.expected is None and failure.actual is None


def _head(**values: Any) -> EventStreamRow:
    return EventStreamRow(stream_id="stream-1", status="active", **values)


def test_the_stream_head_must_name_the_last_event() -> None:
    rows = chain()
    last = rows[-1][0]
    good = _head(last_sequence=3, last_event_id=last.event_id, last_event_sha256=last.event_sha256)
    assert _check_head(good, last) is None

    behind = _head(last_sequence=2, last_event_id=last.event_id, last_event_sha256="0" * 64)
    failure = _check_head(behind, last)
    assert failure is not None and (failure.check, failure.expected, failure.actual) == (
        "stream_head",
        "3",
        "2",
    )
    wrong = _head(last_sequence=3, last_event_id=uuid4(), last_event_sha256=last.event_sha256)
    assert _check_head(wrong, last) == IntegrityFailure(
        "stream_head",
        stream_sequence=3,
        event_id=str(last.event_id),
        expected=last.event_sha256,
        actual=last.event_sha256,
    )


def test_an_empty_stream_needs_an_empty_head() -> None:
    assert _check_head(_head(last_sequence=0), None) is None
    for head in (_head(last_sequence=2), _head(last_sequence=0, last_event_sha256="0" * 64)):
        failure = _check_head(head, None)
        assert failure is not None and failure.check == "stream_head"


def test_the_producer_id_column_must_match_the_envelope() -> None:
    rows = chain()
    rows[1][0].producer_id = "someone-else"  # what the idempotency lookups read

    failure = walk(rows)

    assert failure is not None
    assert (failure.check, failure.stream_sequence) == ("producer_id", 2)
    assert (failure.expected, failure.actual) == ("verify-test", "someone-else")


def test_a_non_canonical_stored_column_fails_event_decode() -> None:
    # The model decodes both forms identically; only the stored bytes differ.
    rows = chain()
    context = dict(rows[1][0].context)
    dropped = next(key for key, value in context.items() if value is None)
    del context[dropped]
    rows[1][0].context = context

    failure = walk(rows)

    assert failure is not None and (failure.check, failure.stream_sequence) == ("event_decode", 2)


GOLDEN = Path(__file__).parents[2] / "fixtures" / "ledger" / "golden_event.json"
GOLDEN_PAYLOAD_SHA256 = "88d1f4e11e0985082dc8fa64df0cdb8d66ede4d099e50c8df47b020ac5707259"
GOLDEN_EVENT_SHA256 = "815689d06ac447d0ade1304eb990c29cdb3463eb3fe20b7f625815343f743853"


def test_a_frozen_event_keeps_its_exact_digests() -> None:
    """An SDK release that changes the seal-time digests must fail here, loudly."""
    sealed = StoredEventV1.model_validate(json.loads(GOLDEN.read_text(encoding="utf-8")))
    row = _event_row_from_sealed(sealed, recorded_at=T0)
    refs = list(reversed(_content_ref_rows_from_sealed(sealed)))  # storage order is arbitrary
    assert len(refs) == 3

    failure = _check_event(row, 7, "a" * 64, {row.event_id: refs})

    assert failure is None
    assert (row.payload_sha256, row.event_sha256) == (GOLDEN_PAYLOAD_SHA256, GOLDEN_EVENT_SHA256)
    rebuilt = _row_to_stored_event(row, refs)
    assert sha256_hex(canonical_json_bytes(_serialized_payload(rebuilt))) == GOLDEN_PAYLOAD_SHA256
    assert _event_sha256(rebuilt) == GOLDEN_EVENT_SHA256
    for dropped in range(3):  # every ref is sealed, not only the first
        fewer = refs[:dropped] + refs[dropped + 1 :]
        failure = _check_event(row, 7, "a" * 64, {row.event_id: fewer})
        assert failure is not None and failure.check == "event_sha256"


def test_the_scope_and_a_ledger_wide_failure_are_reported() -> None:
    report = LedgerVerification(
        [StreamVerification("s", 2, 3, 4)],
        since_sequence=3,
        requested_streams=("s",),
        failure=IntegrityFailure("event_count", expected="5", actual="2"),
    )

    summary = report.to_dict()

    assert report.ok is False
    assert summary["scope"] == {"full": False, "since_sequence": 3, "streams": ["s"]}
    assert summary["failure"]["check"] == "event_count"
    assert summary["totals"]["streams_failed"] == 0


def test_the_report_shape_is_stable() -> None:
    ok = StreamVerification("s-ok", events=3, first_sequence=1, last_sequence=3)
    bad = StreamVerification(
        "s-bad",
        events=1,
        first_sequence=1,
        last_sequence=1,
        failure=IntegrityFailure("event_sha256", 2, "e-2", "a" * 64, "b" * 64),
    )

    assert LedgerVerification([ok, bad]).to_dict() == {
        "ok": False,
        "checks": list(CHECKS),
        "scope": {"full": True, "since_sequence": 1, "streams": None},
        "totals": {"streams": 2, "streams_failed": 1, "events": 4},
        "failure": None,
        "streams": [
            {
                "stream_id": "s-ok",
                "ok": True,
                "events": 3,
                "first_sequence": 1,
                "last_sequence": 3,
                "failure": None,
            },
            {
                "stream_id": "s-bad",
                "ok": False,
                "events": 1,
                "first_sequence": 1,
                "last_sequence": 1,
                "failure": {
                    "check": "event_sha256",
                    "stream_sequence": 2,
                    "event_id": "e-2",
                    "expected": "a" * 64,
                    "actual": "b" * 64,
                },
            },
        ],
    }


# --- the CLI ---


@pytest.fixture
def ledger_cli(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {"report": LedgerVerification(), "grant_error": None, "error": None}
    monkeypatch.setenv("AGENT_CONTEXT_POSTGRESQL__DSN", "postgresql+psycopg://u:hunter2@db.test/a")

    async def check_grants(*_args: Any, **kwargs: Any) -> None:
        state["role"] = kwargs["role"]
        if state["grant_error"] is not None:
            raise state["grant_error"]

    async def verify(*_args: Any, **kwargs: Any) -> LedgerVerification:
        state["kwargs"] = kwargs
        if state["error"] is not None:
            raise state["error"]
        report: LedgerVerification = state["report"]
        return report

    monkeypatch.setattr(cli, "check_grants", check_grants)
    monkeypatch.setattr(cli, "verify_ledger", verify)
    return state


def test_verified_ledger_exits_0_with_the_json_report(ledger_cli: dict[str, Any]) -> None:
    ledger_cli["report"] = LedgerVerification([StreamVerification("s", 2, 1, 2)])

    result = CliRunner().invoke(
        cli.app,
        ["ledger", "verify", "--json", "--stream", "s", "--stream", "t", "--since-sequence", "2"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["ok"] is True
    assert ledger_cli["kwargs"]["streams"] == ("s", "t")
    assert ledger_cli["kwargs"]["since_sequence"] == 2
    assert ledger_cli["role"] == "projector"


def test_an_integrity_failure_exits_1_and_names_the_check(ledger_cli: dict[str, Any]) -> None:
    failure = IntegrityFailure("previous_event_sha256", 4, "e-4", "a" * 64, "c" * 64)
    ledger_cli["report"] = LedgerVerification([StreamVerification("s", 3, 1, 3, failure)])

    result = CliRunner().invoke(cli.app, ["ledger", "verify"])

    assert result.exit_code == 1
    assert "stream s: previous_event_sha256 failed at sequence 4" in result.stdout
    assert "INTEGRITY FAILURE" in result.stdout


@pytest.mark.parametrize(
    "error",
    [
        "grant",
        OperationalError("SELECT * FROM ledger.events WHERE secret = 'hunter2'", {}, OSError()),
        ConnectionRefusedError("refused"),
    ],
)
def test_grant_connection_and_database_errors_exit_2_without_detail(
    ledger_cli: dict[str, Any], error: Any
) -> None:
    if error == "grant":
        ledger_cli["grant_error"] = MissingGrantError("projector", ["SELECT on ledger.events"])
    else:
        ledger_cli["error"] = error

    result = CliRunner().invoke(cli.app, ["ledger", "verify"])

    assert result.exit_code == 2
    assert "hunter2" not in result.output


class _Refused(Exception):
    sqlstate = "42501"


def test_permission_denied_names_the_grants_without_driver_detail(
    ledger_cli: dict[str, Any],
) -> None:
    ledger_cli["grant_error"] = ProgrammingError(
        "SELECT has_table_privilege('hunter2')", {}, _Refused("permission denied for schema")
    )

    result = CliRunner().invoke(cli.app, ["ledger", "verify"])

    assert result.exit_code == 2
    assert "permission denied" in result.output and "USAGE on schema ledger" in result.output
    assert "hunter2" not in result.output and "has_table_privilege" not in result.output


def test_a_partial_run_and_a_ledger_wide_failure_are_named_in_text(
    ledger_cli: dict[str, Any],
) -> None:
    ledger_cli["report"] = LedgerVerification(
        [StreamVerification("s", 1, 4, 4)],
        since_sequence=4,
        requested_streams=("s",),
        failure=IntegrityFailure("event_count", expected="3", actual="1"),
    )

    result = CliRunner().invoke(cli.app, ["ledger", "verify", "--stream", "s"])

    assert result.exit_code == 1
    assert "(partial: from sequence 4, 1 named streams)" in result.stdout
    assert "ledger: event_count failed (expected 3, actual 1)" in result.stdout


def test_an_unexpected_error_is_no_verdict_and_exits_2(ledger_cli: dict[str, Any]) -> None:
    ledger_cli["error"] = RuntimeError("hunter2 leaked in a message")

    result = CliRunner().invoke(cli.app, ["ledger", "verify"])

    assert result.exit_code == 2
    assert "no verdict" in result.output and "RuntimeError" in result.output
    assert "hunter2" not in result.output


def test_bad_options_are_usage_errors(ledger_cli: dict[str, Any]) -> None:
    assert CliRunner().invoke(cli.app, ["ledger", "verify", "--since-sequence", "0"]).exit_code == 2
    assert CliRunner().invoke(cli.app, ["ledger", "verify", "--batch-size", "0"]).exit_code == 2
    assert "kwargs" not in ledger_cli
