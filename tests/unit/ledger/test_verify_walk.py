"""`verify_ledger` walking an in-memory ledger: the stream walk without PostgreSQL (PLATFORM-056).

The rows are built by the real sealing code (`seal_event` and the repository's row mapping), and
the session double answers the verifier's statements by evaluating them over those rows: the
filters, ordering, DISTINCT, UNION and LIMIT of the subset of SQLAlchemy Core the verifier emits
(anything else fails loudly). The SQL itself is proven against PostgreSQL by
`tests/integration/ledger/test_ledger_verify.py`; here every outcome of the walk is asserted on
the JSON report, with no content in it.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import operator
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
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
from sqlalchemy import Table
from sqlalchemy.sql import functions, operators
from sqlalchemy.sql.elements import (
    BinaryExpression,
    BindParameter,
    BooleanClauseList,
    Label,
    UnaryExpression,
)
from sqlalchemy.sql.selectable import CompoundSelect, Select, Subquery

from agent_context_platform.ledger import verify
from agent_context_platform.ledger.models import EventContentRefRow, EventRow, EventStreamRow
from agent_context_platform.ledger.repository import (
    _content_ref_rows_from_sealed,
    _event_row_from_sealed,
    _row_to_stored_event,
)

pytestmark = pytest.mark.unit

T0 = datetime(2026, 9, 1, tzinfo=UTC)
PRODUCER = ProducerV1(producer_id="walk-test", name="Codex", version="1.0.0")
# Every payload value, context value and content body carries this marker; no report may.
PRIVATE = "PRIVATE"
LEDGER_TABLES = {"ledger.event_streams", "ledger.events", "ledger.event_content_refs"}
GOLDEN = Path(__file__).parents[2] / "fixtures" / "ledger" / "golden_event.json"
GOLDEN_EVENT_SHA256 = "815689d06ac447d0ade1304eb990c29cdb3463eb3fe20b7f625815343f743853"

Sealed = tuple[EventRow, list[EventContentRefRow]]


def _draft(stream_id: str, number: int) -> tuple[EventDraftV1, list[ContentRefV1]]:
    session_id, turn_id = f"session-{PRIVATE}", f"turn-{PRIVATE}-{number}"
    payload: dict[str, Any] = {"source": "codex", "session_id": session_id, "turn_id": turn_id}
    claims: tuple[ContentClaimV1, ...] = ()
    refs: list[ContentRefV1] = []
    if number % 2:  # odd events carry a content ref (the refs are sealed into the event)
        body = f"{PRIVATE} body {stream_id} {number}".encode()
        digest = hashlib.sha256(body).hexdigest()
        content_id = f"content-{PRIVATE}-{number}"
        claims = (
            ContentClaimV1(
                content_id=content_id,
                content_sha256=digest,
                media_type="text/plain",
                uncompressed_bytes=len(body),
            ),
        )
        refs = [
            ContentRefV1(
                content_id=content_id,
                content_sha256=digest,
                media_type="text/plain",
                uncompressed_bytes=len(body),
                disposition=ContentDisposition.SANITIZED,
                storage=ContentStorage.INLINE,
                inline_id=digest,
            )
        ]
        payload["user_message_content_id"] = content_id
    moment = T0 + timedelta(seconds=number)
    draft = EventDraftV1(
        event_type="agent.turn.started",
        stream_id=stream_id,
        occurred_at=moment,
        observed_at=moment,
        producer=PRODUCER,
        payload=payload,
        context=EventContextV1(session_id=session_id, turn_id=turn_id),
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"{stream_id}-{number}",
        content_claims=claims,
    )
    return draft, refs


def sealed_stream(stream_id: str, length: int) -> list[Sealed]:
    """`length` events of one stream sealed by `seal_event`, as the repository stores them."""
    rows: list[Sealed] = []
    previous: str | None = None
    for number in range(1, length + 1):
        draft, refs = _draft(stream_id, number)
        sealed = seal_event(draft, refs, number, previous)
        previous = sealed.integrity.event_sha256
        rows.append(
            (_event_row_from_sealed(sealed, recorded_at=T0), _content_ref_rows_from_sealed(sealed))
        )
    return rows


def head_for(stream_id: str, last: EventRow | None) -> EventStreamRow:
    if last is None:
        return EventStreamRow(stream_id=stream_id, status="active", last_sequence=0)
    return EventStreamRow(
        stream_id=stream_id,
        status="active",
        last_sequence=last.stream_sequence,
        last_event_id=last.event_id,
        last_event_sha256=last.event_sha256,
    )


# --- a session double that evaluates the verifier's statements over in-memory rows ----------

_COMPARISONS: dict[Any, Callable[[Any, Any], bool]] = {
    operators.eq: operator.eq,
    operators.gt: operator.gt,
    operators.ge: operator.ge,
    operators.in_op: lambda value, values: value in values,
}


def _value(expression: Any, row: Any) -> Any:
    if isinstance(expression, BindParameter):
        return expression.effective_value
    return getattr(row, expression.name)


def _holds(clause: Any, row: Any) -> bool:
    if clause is None:
        return True
    if isinstance(clause, BooleanClauseList) and clause.operator is operators.and_:
        return all(_holds(item, row) for item in clause.clauses)
    if isinstance(clause, BinaryExpression) and clause.operator in _COMPARISONS:
        return _COMPARISONS[clause.operator](_value(clause.left, row), _value(clause.right, row))
    raise NotImplementedError(f"the fake ledger cannot evaluate {clause!r}")


class FakeStream:
    """`AsyncSession.stream_scalars`: the rows in partitions, and a close that is recorded."""

    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows
        self.closed = False

    async def partitions(self, size: int) -> AsyncIterator[list[Any]]:
        for start in range(0, len(self.rows), size):
            yield self.rows[start : start + size]

    async def close(self) -> None:
        self.closed = True


class FakeLedger:
    """The rows of `ledger.*`, and an `async_sessionmaker` double whose sessions read them."""

    def __init__(self) -> None:
        self.heads: dict[str, EventStreamRow] = {}
        self.events: list[EventRow] = []
        self.refs: list[EventContentRefRow] = []
        self.sessions_opened = 0
        self.log: list[str] = []  # transaction boundaries and raw SQL, in order
        self.tables_read: set[str] = set()
        self.ref_lookups: list[int] = []  # event IDs per content-ref read
        self.streams: list[FakeStream] = []
        self.yield_per: list[Any] = []
        self.handed_out: list[Any] = []
        self.expunged: list[Any] = []

    def add(self, stream_id: str, rows: list[Sealed], *, head: bool = True) -> None:
        for row, refs in rows:
            self.events.append(row)
            self.refs.extend(refs)
        if head:
            self.heads[stream_id] = head_for(stream_id, rows[-1][0] if rows else None)

    def delete_event(self, stream_id: str, sequence: int) -> None:
        self.events = [
            row
            for row in self.events
            if (row.stream_id, row.stream_sequence) != (stream_id, sequence)
        ]

    def __call__(self) -> FakeSession:
        self.sessions_opened += 1
        return FakeSession(self)

    def _source(self, source: Any) -> list[Any]:
        if isinstance(source, Table):
            self.tables_read.add(source.fullname)
            tables: dict[str, list[Any]] = {
                "ledger.event_streams": list(self.heads.values()),
                "ledger.events": self.events,
                "ledger.event_content_refs": self.refs,
            }
            return list(tables[source.fullname])
        if isinstance(source, Subquery) and isinstance(source.element, CompoundSelect):
            assert source.element.keyword.value == "UNION"
            (name,) = source.c.keys()
            values: dict[Any, None] = {}
            for part in source.element.selects:
                values.update(dict.fromkeys(self.select(part.element)))
            return [SimpleNamespace(**{name: value}) for value in values]
        raise NotImplementedError(f"the fake ledger cannot read from {source!r}")

    def select(self, statement: Select[Any]) -> list[Any]:
        (source,) = statement.get_final_froms()
        rows = [row for row in self._source(source) if _holds(statement.whereclause, row)]
        for clause in reversed(statement._order_by_clauses):
            descending = (
                isinstance(clause, UnaryExpression) and clause.modifier is operators.desc_op
            )
            column = clause.element if isinstance(clause, UnaryExpression) else clause
            rows.sort(key=lambda row, column=column: _value(column, row), reverse=descending)
        (description,) = statement.column_descriptions
        expression = description["expr"]
        if isinstance(expression, functions.count):
            return [len(rows)]
        if isinstance(expression, type):  # a mapped entity
            values = rows
            self.handed_out.extend(rows)
        else:
            column = expression.element if isinstance(expression, Label) else expression
            values = [_value(column, row) for row in rows]
        if statement._distinct:
            values = list(dict.fromkeys(values))
        limit = statement._limit
        return values if limit is None else values[:limit]


class FakeSession:
    def __init__(self, ledger: FakeLedger) -> None:
        self.ledger = ledger

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self.ledger.log.append("close")

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[None]:
        self.ledger.log.append("begin")
        yield
        self.ledger.log.append("end")

    async def execute(self, statement: Any) -> None:
        self.ledger.log.append(str(statement))

    async def get(self, model: type[Any], key: str) -> EventStreamRow | None:
        assert model is EventStreamRow
        self.ledger.tables_read.add("ledger.event_streams")
        head = self.ledger.heads.get(key)
        if head is not None:
            self.ledger.handed_out.append(head)
        return head

    async def scalar(self, statement: Select[Any]) -> Any:
        rows = self.ledger.select(statement)
        return rows[0] if rows else None

    async def scalars(self, statement: Select[Any]) -> list[Any]:
        rows = self.ledger.select(statement)
        if statement.column_descriptions[0]["expr"] is EventContentRefRow:
            self.ledger.ref_lookups.append(len(statement.whereclause.right.effective_value))
        return rows

    async def stream_scalars(self, statement: Select[Any]) -> FakeStream:
        self.ledger.yield_per.append(statement.get_execution_options().get("yield_per"))
        stream = FakeStream(self.ledger.select(statement))
        self.ledger.streams.append(stream)
        return stream

    def expunge(self, instance: Any) -> None:
        self.ledger.expunged.append(instance)


# --- helpers ---------------------------------------------------------------------------------


def two_streams() -> tuple[FakeLedger, list[Sealed], list[Sealed]]:
    a, b = sealed_stream("stream-a", 4), sealed_stream("stream-b", 2)
    ledger = FakeLedger()
    ledger.add("stream-a", a)
    ledger.add("stream-b", b)
    return ledger, a, b


def run(ledger: FakeLedger, **options: Any) -> dict[str, Any]:
    report = asyncio.run(verify.verify_ledger(ledger, **options))  # type: ignore[arg-type]
    summary = report.to_dict()
    serialized = json.dumps(summary)
    assert PRIVATE not in serialized  # IDs, sequences, check names and digests only
    assert summary["ok"] is report.ok
    return summary


def stream(
    stream_id: str,
    events: int,
    first: int | None,
    last: int | None,
    failed: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stream_id": stream_id,
        "ok": failed is None,
        "events": events,
        "first_sequence": first,
        "last_sequence": last,
        "failure": failed,
    }


def failure(
    check: str,
    sequence: int | None = None,
    event_id: Any = None,
    expected: str | None = None,
    actual: str | None = None,
) -> dict[str, Any]:
    return {
        "check": check,
        "stream_sequence": sequence,
        "event_id": None if event_id is None else str(event_id),
        "expected": expected,
        "actual": actual,
    }


# --- the walk ----------------------------------------------------------------------------------


def test_an_intact_ledger_verifies_in_one_read_only_snapshot_and_bounded_batches() -> None:
    ledger, _a, _b = two_streams()

    summary = run(ledger, batch_size=3)

    assert summary == {
        "ok": True,
        "checks": list(verify.CHECKS),
        "scope": {"full": True, "since_sequence": 1, "streams": None},
        "totals": {"streams": 2, "streams_failed": 0, "events": 6},
        "failure": None,
        "streams": [stream("stream-a", 4, 1, 4), stream("stream-b", 2, 1, 2)],
    }
    # One session, one transaction, opened read-only before any read.
    assert ledger.sessions_opened == 1
    assert ledger.log == ["begin", verify._READ_ONLY, "end", "close"]
    # Each stream's events come through a server-side cursor in batches of `batch_size`, and their
    # content refs are read per batch: never more than one batch of events in memory.
    assert ledger.yield_per == [3, 3]
    assert ledger.ref_lookups == [3, 1, 2]
    assert all(cursor.closed for cursor in ledger.streams)
    assert {id(row) for row in ledger.handed_out} <= {id(row) for row in ledger.expunged}
    # Only the sealed envelope and its refs are read, never content storage.
    assert ledger.tables_read == LEDGER_TABLES


def _edit_payload(rows: list[Sealed]) -> dict[str, Any]:
    row, refs = rows[1]
    row.payload = {**row.payload, "turn_id": f"turn-{PRIVATE}-edited"}
    recomputed = sha256_hex(
        canonical_json_bytes(_serialized_payload(_row_to_stored_event(row, refs)))
    )
    return failure("payload_sha256", 2, row.event_id, recomputed, row.payload_sha256)


def _replace_middle_digest(rows: list[Sealed]) -> dict[str, Any]:
    original = rows[1][0].event_sha256
    rows[1][0].event_sha256 = "f" * 64
    rows[2][0].previous_event_sha256 = "f" * 64  # the next link is repaired to match
    return failure("event_sha256", 2, rows[1][0].event_id, original, "f" * 64)


def _break_link(rows: list[Sealed]) -> dict[str, Any]:
    rows[2][0].previous_event_sha256 = "a" * 64
    return failure(
        "previous_event_sha256", 3, rows[2][0].event_id, rows[1][0].event_sha256, "a" * 64
    )


def _open_a_gap(rows: list[Sealed]) -> dict[str, Any]:
    rows[3][0].stream_sequence = 5
    return failure("sequence", 5, rows[3][0].event_id, "4", "5")


def _non_canonical_context(rows: list[Sealed]) -> dict[str, Any]:
    # The model decodes both forms identically; only the stored JSONB differs.
    context = dict(rows[1][0].context)
    del context[next(key for key, value in context.items() if value is None)]
    rows[1][0].context = context
    return failure("event_decode", 2, rows[1][0].event_id)


def _undecodable_context(rows: list[Sealed]) -> dict[str, Any]:
    rows[1][0].context = {"unexpected_field": PRIVATE}
    return failure("event_decode", 2, rows[1][0].event_id)


def _other_producer(rows: list[Sealed]) -> dict[str, Any]:
    rows[1][0].producer_id = "someone-else"  # the column the idempotency lookups read
    return failure("producer_id", 2, rows[1][0].event_id, PRODUCER.producer_id, "someone-else")


def _edit_content_ref(rows: list[Sealed]) -> dict[str, Any]:
    row, refs = rows[0]
    refs[0].media_type = "text/markdown"  # the refs are part of the sealed envelope
    recomputed = _event_sha256(_row_to_stored_event(row, refs))
    return failure("event_sha256", 1, row.event_id, recomputed, row.event_sha256)


CORRUPTIONS: dict[str, tuple[Callable[[list[Sealed]], dict[str, Any]], int]] = {
    # name: (corruption returning the expected failure, events verified before it)
    "payload_byte": (_edit_payload, 1),
    "middle_digest_with_next_link_repaired": (_replace_middle_digest, 1),
    "broken_link": (_break_link, 2),
    "sequence_gap": (_open_a_gap, 3),
    "non_canonical_jsonb": (_non_canonical_context, 1),
    "undecodable_row": (_undecodable_context, 1),
    "producer_id_column": (_other_producer, 1),
    "content_ref_metadata": (_edit_content_ref, 0),
}


@pytest.mark.parametrize("case", sorted(CORRUPTIONS))
def test_each_corruption_fails_its_check_and_never_hides_another_stream(case: str) -> None:
    ledger, a, _b = two_streams()
    corrupt, verified = CORRUPTIONS[case]
    expected = corrupt(a)

    summary = run(ledger, batch_size=2)

    assert summary["ok"] is False
    assert summary["failure"] is None  # the ledger-wide count only runs once every stream verified
    assert summary["totals"] == {"streams": 2, "streams_failed": 1, "events": verified + 2}
    failed, other = summary["streams"]
    assert failed == stream(
        "stream-a", verified, 1 if verified else None, verified or None, expected
    )
    assert other == stream("stream-b", 2, 1, 2)
    # The first failure ends the stream's walk, and its cursor is closed all the same.
    assert all(cursor.closed for cursor in ledger.streams)


def test_a_head_that_does_not_name_the_last_event_fails_stream_head() -> None:
    ledger, a, _b = two_streams()
    last = a[-1][0]
    ledger.heads["stream-a"].last_event_sha256 = "c" * 64

    tampered = run(ledger)

    assert tampered["streams"][0] == stream(
        "stream-a",
        4,
        1,
        4,
        failure("stream_head", 4, last.event_id, last.event_sha256, "c" * 64),
    )
    ledger.heads["stream-a"] = head_for("stream-a", last)
    ledger.heads["stream-a"].last_sequence = 3  # a head left behind by a partial restore

    behind = run(ledger)

    assert behind["streams"][0]["failure"] == failure("stream_head", 4, last.event_id, "4", "3")
    assert behind["totals"] == {"streams": 2, "streams_failed": 1, "events": 6}


def test_events_without_a_head_row_are_never_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    # A partial restore: the head rows are gone, the events are not.
    monkeypatch.setattr(verify, "STREAM_PAGE_SIZE", 1)  # page across both tables, one ID a page
    ledger, a, _b = two_streams()
    _edit_payload(a)  # damage the head-less stream too: the missing head is what is reported
    del ledger.heads["stream-a"]

    one = run(ledger)

    assert one["ok"] is False and one["failure"] is None
    assert one["streams"] == [
        stream("stream-a", 0, None, None, failure("stream_head_missing")),
        stream("stream-b", 2, 1, 2),
    ]
    ledger.heads.clear()

    every = run(ledger)

    assert every["totals"] == {"streams": 2, "streams_failed": 2, "events": 0}
    assert [item["failure"]["check"] for item in every["streams"]] == ["stream_head_missing"] * 2


def test_the_enumeration_pages_through_head_only_and_head_less_streams_once_each(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(verify, "STREAM_PAGE_SIZE", 2)
    ledger = FakeLedger()
    for name, length, head in (("s-1", 1, True), ("s-2", 0, True), ("s-3", 2, False)):
        ledger.add(name, sealed_stream(name, length), head=head)
    ledger.add("s-4", sealed_stream("s-4", 3))

    summary = run(ledger)

    assert [(item["stream_id"], item["ok"], item["events"]) for item in summary["streams"]] == [
        ("s-1", True, 1),
        ("s-2", True, 0),  # an empty stream with an empty head is intact
        ("s-3", False, 0),
        ("s-4", True, 3),
    ]
    assert summary["streams"][2]["failure"]["check"] == "stream_head_missing"


def test_an_empty_head_must_say_so() -> None:
    ledger = FakeLedger()
    ledger.add("s-empty", [])
    ledger.heads["s-empty"].last_sequence = 2

    summary = run(ledger)

    assert summary["streams"] == [
        stream("s-empty", 0, None, None, failure("stream_head", expected="0", actual="2"))
    ]
    ledger.heads["s-empty"].last_sequence = 0
    assert run(ledger)["ok"] is True
    assert run(ledger, since_sequence=2)["ok"] is True  # nothing before or in the range


def test_named_streams_are_deduplicated_sorted_and_a_missing_one_fails_stream_exists() -> None:
    ledger, _a, _b = two_streams()
    ledger.add("head-less", sealed_stream("head-less", 1), head=False)

    summary = run(ledger, streams=["stream-b", "no-such-stream", "head-less", "stream-b"])

    assert summary["scope"] == {
        "full": False,
        "since_sequence": 1,
        "streams": ["head-less", "no-such-stream", "stream-b"],
    }
    assert summary["streams"] == [
        stream("head-less", 0, None, None, failure("stream_head_missing")),
        stream("no-such-stream", 0, None, None, failure("stream_exists")),
        stream("stream-b", 2, 1, 2),
    ]
    assert summary["totals"] == {"streams": 3, "streams_failed": 2, "events": 2}


def test_since_sequence_links_the_range_to_the_stored_event_before_it() -> None:
    ledger, a, _b = two_streams()
    a[0][0].event_sha256 = "f" * 64  # event 1 is damaged, but outside the range

    tail = run(ledger, streams=["stream-a"], since_sequence=3)

    assert tail == {
        "ok": True,
        "checks": list(verify.CHECKS),
        "scope": {"full": False, "since_sequence": 3, "streams": ["stream-a"]},
        "totals": {"streams": 1, "streams_failed": 0, "events": 2},
        "failure": None,
        "streams": [stream("stream-a", 2, 3, 4)],
    }
    # stream-b ends (at 2) before the range: nothing is walked, and its head is still checked.
    late = run(ledger, since_sequence=4)
    assert late["ok"] is True
    assert late["streams"] == [stream("stream-a", 1, 4, 4), stream("stream-b", 0, None, None)]


def test_a_head_past_the_range_must_name_the_real_last_event() -> None:
    ledger, _a, b = two_streams()
    ledger.heads["stream-b"].last_event_sha256 = "c" * 64

    summary = run(ledger, since_sequence=4)

    last = b[-1][0]
    assert summary["streams"] == [
        stream("stream-a", 1, 4, 4),
        stream(
            "stream-b",
            0,
            None,
            None,
            failure("stream_head", 2, last.event_id, last.event_sha256, "c" * 64),
        ),
    ]


def test_a_missing_event_just_before_the_range_is_a_sequence_gap() -> None:
    ledger, _a, _b = two_streams()
    ledger.delete_event("stream-a", 2)

    summary = run(ledger, streams=["stream-a"], since_sequence=3)

    assert summary["streams"] == [
        stream("stream-a", 0, None, None, failure("sequence", 3, None, "2", "3"))
    ]


def test_events_outside_the_walk_fail_event_count(monkeypatch: pytest.MonkeyPatch) -> None:
    # Defence in depth: should the enumeration ever miss a stream, the snapshot's count catches it.
    enumerate_page = verify._stream_ids

    async def lossy(session: Any, after: str) -> list[str]:
        return [item for item in await enumerate_page(session, after) if item != "stream-b"]

    monkeypatch.setattr(verify, "_stream_ids", lossy)
    ledger, _a, _b = two_streams()

    summary = run(ledger)

    assert summary["ok"] is False
    assert summary["failure"] == failure("event_count", expected="6", actual="4")
    assert summary["totals"] == {"streams": 1, "streams_failed": 0, "events": 4}
    # Scoped to the named streams and the range, the count agrees with the walk.
    assert run(ledger, streams=["stream-a"], since_sequence=2)["failure"] is None


def test_the_frozen_event_verifies_through_the_walk_from_its_own_sequence() -> None:
    """The golden event (sequence 7, linked to `a` * 64) walked with `--since-sequence 7`."""
    sealed = StoredEventV1.model_validate(json.loads(GOLDEN.read_text(encoding="utf-8")))
    row = _event_row_from_sealed(sealed, recorded_at=T0)
    before = EventRow(
        event_id=uuid4(), stream_id=row.stream_id, stream_sequence=6, event_sha256="a" * 64
    )
    ledger = FakeLedger()
    ledger.events = [before, row]
    ledger.refs = list(reversed(_content_ref_rows_from_sealed(sealed)))  # storage order is free
    ledger.heads[row.stream_id] = head_for(row.stream_id, row)

    summary = run(ledger, since_sequence=7)

    assert row.event_sha256 == GOLDEN_EVENT_SHA256
    assert summary["ok"] is True
    assert summary["streams"] == [stream(row.stream_id, 1, 7, 7)]
    before.event_sha256 = "b" * 64  # the stored event before the range is the link
    relinked = run(ledger, since_sequence=7)
    assert relinked["streams"][0]["failure"] == failure(
        "previous_event_sha256", 7, row.event_id, "b" * 64, "a" * 64
    )


@pytest.mark.parametrize("options", [{"since_sequence": 0}, {"batch_size": 0}])
def test_bounds_are_refused_before_any_session_opens(options: dict[str, int]) -> None:
    ledger = FakeLedger()

    with pytest.raises(ValueError, match="at least 1"):
        asyncio.run(verify.verify_ledger(ledger, **options))  # type: ignore[arg-type]

    assert ledger.sessions_opened == 0
