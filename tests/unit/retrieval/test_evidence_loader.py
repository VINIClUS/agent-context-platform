"""Evidence loading and gated content reads against a scripted ledger, without PostgreSQL.

`Ledger` holds rows built by the ledger's own sealing and row code (`seal_event`,
`_event_row_from_sealed`) and answers the loader's statements as the tables would: outbox
delivery, event and content-reference rows, purge records and lexical ranking. Every statement is
recorded with its parameters and the transaction (session) it ran in.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

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
from agent_context_sdk.canonical import canonical_json_bytes

from agent_context_platform.content.service import ContentUnavailableError
from agent_context_platform.ledger.models import EventContentRefRow, EventRow
from agent_context_platform.ledger.repository import (
    _canonicalize_draft,
    _content_ref_rows_from_sealed,
    _event_row_from_sealed,
)
from agent_context_platform.retrieval import evidence
from agent_context_platform.retrieval.evidence import (
    MAX_READ_BYTES,
    MAX_SNIPPET_CHARS,
    Citation,
    EvidenceLedger,
    EvidenceScope,
    LedgerEvent,
    LoadedEvents,
    evidence_ref,
    metadata_ref,
    resource_uri,
)

from .fakes import row

pytestmark = pytest.mark.unit

REPO = "repo-1"
T0 = datetime(2026, 9, 1, 12, tzinfo=UTC)
PRODUCER = ProducerV1(producer_id="evidence-test", name="unit", version="1.0.0")
SCOPE = EvidenceScope(repository_id=REPO)


def event_id(number: int) -> UUID:
    """A UUIDv7 whose order is `number`'s."""
    return UUID(f"0198a4b1-98c0-7c28-ae3f-{number:012x}")


def ref(
    content_id: str,
    data: bytes = b"",
    *,
    media_type: str = "text/plain",
    disposition: ContentDisposition = ContentDisposition.SANITIZED,
    size: int | None = None,
) -> ContentRefV1:
    digest = hashlib.sha256(data).hexdigest()
    if size is None:
        return ContentRefV1(
            content_id=content_id,
            content_sha256=digest,
            media_type=media_type,
            uncompressed_bytes=len(data),
            disposition=disposition,
            storage=ContentStorage.INLINE,
            inline_id=digest,
        )
    return ContentRefV1(
        content_id=content_id,
        content_sha256=digest,
        media_type=media_type,
        uncompressed_bytes=size,
        disposition=disposition,
        storage=ContentStorage.OBJECT,
        object_key=f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst",
        encoding="zstd",
    )


def sealed(number: int, *refs: ContentRefV1, repository: str = REPO) -> StoredEventV1:
    moment = T0 + timedelta(minutes=number)
    draft = EventDraftV1(
        event_id=event_id(number),
        event_type="agent.turn.started",
        stream_id=f"stream-{number}",
        occurred_at=moment,
        observed_at=moment,
        producer=PRODUCER,
        payload={"source": "codex", "session_id": "s-1", "turn_id": f"t-{number}"},
        context=EventContextV1(
            project_id="prj-1", repository_id=repository, session_id="s-1", turn_id=f"t-{number}"
        ),
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"key-{number}",
        content_claims=tuple(
            ContentClaimV1(
                content_id=r.content_id,
                content_sha256=r.content_sha256,
                media_type=r.media_type,
                uncompressed_bytes=r.uncompressed_bytes,
            )
            for r in refs
        ),
    )
    # Sealed as the ledger seals: claims in content ID order, so the rows rebuild it exactly.
    return seal_event(_canonicalize_draft(draft), list(refs), 1, None)


class Session:
    """One transaction against the ledger."""

    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger
        self.log: list[tuple[str, dict[str, Any]]] = []

    async def __aenter__(self) -> Session:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def execute(self, statement: Any, params: Mapping[str, Any]) -> list[SimpleNamespace]:
        sql, values = str(statement), dict(params)
        self.log.append((sql, values))
        ledger = self.ledger
        if sql == evidence._TIMEOUT:
            return []
        if sql == evidence._DELIVERED:
            wanted = {UUID(value) for value in values["ids"]}
            bound = values["max"]
            return [
                row(event_id=key)
                for key, (position, status) in ledger.outbox.items()
                if key in wanted and status == "delivered" and (bound is None or position <= bound)
            ]
        if sql == evidence._PURGED:
            return [row(content_id=c) for c in values["content_ids"] if c in ledger.purged]
        if sql == evidence._RANK:
            return ledger.ranked
        raise AssertionError(f"unexpected statement: {sql[:80]}")

    async def scalars(self, statement: Any) -> SimpleNamespace:
        entity = statement.column_descriptions[0]["entity"]
        wanted = set(statement.whereclause.right.value)  # `event_id IN (...)`
        self.log.append((entity.__tablename__, {"event_ids": sorted(wanted)}))
        rows: list[EventRow] | list[EventContentRefRow] = (
            list(self.ledger.events.values()) if entity is EventRow else self.ledger.refs
        )
        return SimpleNamespace(all=lambda: [r for r in rows if r.event_id in wanted])


class Ledger:
    """`ledger.events`, `ledger.event_content_refs`, `projection.outbox` and purge records."""

    def __init__(self) -> None:
        self.events: dict[UUID, EventRow] = {}
        self.refs: list[EventContentRefRow] = []
        self.outbox: dict[UUID, tuple[int, str]] = {}  # event -> (outbox_id, status)
        self.purged: set[str] = set()
        self.ranked: list[SimpleNamespace] = []
        self.sessions: list[Session] = []

    def add(self, event: StoredEventV1, *, status: str = "delivered") -> StoredEventV1:
        self.events[event.event_id] = _event_row_from_sealed(event, recorded_at=event.observed_at)
        self.refs += _content_ref_rows_from_sealed(event)
        self.outbox[event.event_id] = (len(self.outbox) + 1, status)
        return event

    def __call__(self) -> Session:
        session = Session(self)
        self.sessions.append(session)
        return session

    def statements(self, sql: str) -> list[dict[str, Any]]:
        return [params for s in self.sessions for query, params in s.log if query == sql]


class Contents:
    """`ContentService.read`: stored bytes per content ID, else a content-free failure."""

    def __init__(self, stored: Mapping[str, bytes] | None = None) -> None:
        self.stored = dict(stored or {})
        self.reads: list[tuple[Session, str]] = []

    async def read(self, session: Session, ref: ContentRefV1) -> bytes:
        self.reads.append((session, ref.content_id))
        if ref.content_id not in self.stored:
            raise ContentUnavailableError("content unavailable")
        return self.stored[ref.content_id]


def service(ledger: Ledger, contents: Contents | None = None, **kw: Any) -> EvidenceLedger:
    return EvidenceLedger(ledger, contents or Contents(), **kw)  # type: ignore[arg-type]


def load(ledger: Ledger, ids: list[str], scope: EvidenceScope = SCOPE, **kw: Any) -> LoadedEvents:
    return asyncio.run(service(ledger, **kw).events(ids, scope))


# --- loading cited events ---


def test_only_delivered_verified_events_inside_the_scope_are_loaded() -> None:
    ledger = Ledger()
    cited = ledger.add(sealed(1, ref("c-1", b"hello")))
    ledger.add(sealed(2), status="pending")
    ledger.add(sealed(3), status="dead_lettered")
    ledger.add(sealed(4, ref("c-4", b"elsewhere"), repository="repo-2"))
    forged = ledger.add(sealed(5))
    ledger.events[forged.event_id].payload = {
        "source": "codex",
        "session_id": "s-1",
        "turn_id": "t-x",
    }
    ids = [str(event_id(n)) for n in (5, 4, 3, 2, 1, 9)]  # 9 was never recorded

    loaded = load(ledger, [*ids, str(event_id(1)).upper(), "not-a-uuid", ""])

    assert list(loaded) == [str(event_id(1))] and loaded.capped is False
    item = loaded[str(event_id(1))]
    assert item.event == cited  # rebuilt from its rows exactly as sealed
    assert item.event_id == str(event_id(1)) and item.purged == frozenset()
    # One transaction: bounded, then delivery, the rows of delivered events, then purges.
    assert len(ledger.sessions) == 1
    assert [query for query, _ in ledger.sessions[0].log] == [
        evidence._TIMEOUT,
        evidence._DELIVERED,
        "events",
        "event_content_refs",
        evidence._PURGED,
    ]
    assert ledger.statements(evidence._TIMEOUT) == [{"millis": "5000"}]
    assert ledger.statements(evidence._DELIVERED) == [
        {"ids": sorted(ids), "max": None}  # each valid ID once, whatever its spelling
    ]
    delivered = [event_id(1), event_id(4), event_id(5)]
    assert ledger.statements("events") == [{"event_ids": delivered}]
    assert ledger.statements("event_content_refs") == [{"event_ids": delivered}]
    # Only the content of citable events is checked: never the foreign event's.
    assert ledger.statements(evidence._PURGED) == [{"content_ids": ["c-1"]}]


def test_the_metadata_reference_names_the_bytes_the_event_digest_covers() -> None:
    ledger = Ledger()
    event = ledger.add(sealed(1))

    item = load(ledger, [str(event.event_id)])[str(event.event_id)]

    preimage = canonical_json_bytes(
        event.model_dump(mode="json", exclude={"integrity": {"event_sha256"}})
    )
    assert hashlib.sha256(preimage).hexdigest() == event.integrity.event_sha256
    assert item.preimage_bytes == len(preimage)
    ref_ = metadata_ref(item)
    assert (ref_.content_sha256, ref_.uncompressed_bytes) == (
        event.integrity.event_sha256,
        len(preimage),
    )
    # An event without content needs no purge check.
    assert ledger.statements(evidence._PURGED) == []


def test_delivery_is_bounded_by_the_snapshots_outbox_position() -> None:
    ledger = Ledger()
    for number in (1, 2, 3):
        ledger.add(sealed(number))
    ids = [str(event_id(n)) for n in (1, 2, 3)]

    bounded = load(ledger, ids, EvidenceScope(repository_id=REPO, max_outbox_id=2))

    assert list(bounded) == ids[:2]  # delivered past the snapshot: never cited
    assert ledger.statements(evidence._DELIVERED)[0]["max"] == 2
    assert list(load(ledger, ids)) == ids


def test_nothing_is_read_for_invalid_ids_and_no_rows_for_undelivered_events() -> None:
    ledger = Ledger()
    ledger.add(sealed(1), status="pending")

    assert load(ledger, ["", "not-a-uuid", "1234"]) == {}
    assert ledger.sessions == []  # no transaction at all

    assert load(ledger, [str(event_id(1))]) == {}
    assert [query for query, _ in ledger.sessions[0].log] == [
        evidence._TIMEOUT,
        evidence._DELIVERED,
    ]


def test_the_newest_events_are_loaded_when_too_many_are_cited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = Ledger()
    for number in (1, 2, 3):
        ledger.add(sealed(number))
    monkeypatch.setattr(evidence, "MAX_EVENTS_PER_LOAD", 2)

    loaded = load(ledger, [str(event_id(n)) for n in (3, 1, 2)])

    assert list(loaded) == [str(event_id(2)), str(event_id(3))]  # UUIDv7 order: the newest
    assert loaded.capped is True
    assert ledger.statements(evidence._DELIVERED)[0]["ids"] == [
        str(event_id(2)),
        str(event_id(3)),
    ]
    exactly = load(ledger, [str(event_id(n)) for n in (1, 2)])
    assert exactly.capped is False and len(exactly) == 2


def test_purged_content_is_recorded_on_the_event_and_never_cited() -> None:
    ledger = Ledger()
    text = ref("c-a", b"kept text")
    gone = ref("c-0", b"purged text")
    # Marked purged on the reference itself, though no `content.purged` event names it.
    marked = ref("c-p", b"{}", media_type="application/json", disposition=ContentDisposition.PURGED)
    event = ledger.add(sealed(1, text, gone, marked))
    ledger.purged.add("c-0")

    item = load(ledger, [str(event.event_id)])[str(event.event_id)]

    assert item.purged == frozenset({"c-0"})
    assert ledger.statements(evidence._PURGED) == [{"content_ids": ["c-0", "c-a", "c-p"]}]
    assert item.content_ref("c-0") == gone and item.content_ref("c-x") is None
    assert item.citable_content("c-0") is None  # purged
    assert item.citable_content("c-p") is None  # not sanitized
    assert item.citable_content("c-x") is None  # not the event's
    assert item.citable_content("c-a") == text
    assert item.citable_content(None) == text  # the first citable one by content ID
    # A citation of the purged content falls back to the event's own metadata.
    citation = Citation(item.event_id, "session", 0.9, revision_id="rev-1", content_id="c-0")
    fallback = evidence_ref(item, citation)
    assert fallback is not None
    assert fallback.content_ref == metadata_ref(item)
    assert fallback.resource_uri == resource_uri(item.event_id)
    quoted = evidence_ref(item, Citation(item.event_id, "session", 0.9, content_id="c-a"))
    assert quoted is not None and quoted.content_ref == text
    assert quoted.resource_uri == resource_uri(item.event_id, "c-a")
    assert quoted.valid_from == event.occurred_at


def test_an_event_with_no_citable_content_is_cited_by_its_metadata() -> None:
    ledger = Ledger()
    redacted = ref("c-r", b"x", disposition=ContentDisposition.METADATA_ONLY)
    event = ledger.add(sealed(1, redacted))

    item = load(ledger, [str(event.event_id)])[str(event.event_id)]

    assert item.citable_content(None) is None
    found = evidence_ref(item, Citation(item.event_id, "session", 0.5))
    assert found is not None and found.content_ref == metadata_ref(item)


def test_every_transaction_is_bounded_by_the_statement_timeout() -> None:
    ledger = Ledger()
    ledger.add(sealed(1))

    load(ledger, [str(event_id(1))], statement_timeout_seconds=0.25)
    load(ledger, [str(event_id(1))], statement_timeout_seconds=0.0001)

    assert ledger.statements(evidence._TIMEOUT) == [{"millis": "250"}, {"millis": "1"}]
    for bad in (0.0, -1.0, float("nan")):
        with pytest.raises(ValueError, match="positive"):
            service(ledger, statement_timeout_seconds=bad)


# --- gated content reads ---


TEXT = b"Line one\x00 with\xe2\x80\xae  bidi\n\ttabs"  # NUL, a bidi override, whitespace
JSON = b'{"title": "Flaky", "steps": ["run", {"cmd": "pytest -k avg"}], "n": 3}'


def _gated() -> tuple[Ledger, Contents, LedgerEvent]:
    ledger = Ledger()
    refs = [
        ref("c-text", TEXT, media_type="text/plain; charset=utf-8"),
        ref("c-json", JSON, media_type="application/json"),
        ref("c-edge", b"x" * MAX_READ_BYTES, media_type="TEXT/PLAIN", size=MAX_READ_BYTES),
        ref("c-big", b"y", size=MAX_READ_BYTES + 1),
        ref("c-png", b"\x89PNG", media_type="image/png"),
        ref("c-meta", b"held", disposition=ContentDisposition.METADATA_ONLY),
        ref("c-gone", b"lost"),
        ref("c-blank", b" \x00\xe2\x80\x8b "),  # only control and format characters
    ]
    event = ledger.add(sealed(1, *refs))
    contents = Contents(
        {
            "c-text": TEXT,
            "c-json": JSON,
            "c-edge": b"x" * MAX_READ_BYTES,
            "c-blank": b" \x00\xe2\x80\x8b ",
        }
    )
    item = load(ledger, [str(event.event_id)])[str(event.event_id)]
    return ledger, contents, item


def test_snippets_read_only_gated_text_in_one_transaction() -> None:
    ledger, contents, item = _gated()
    names = ["c-text", "c-json", "c-edge", "c-big", "c-png", "c-meta", "c-gone", "c-blank"]
    requests: list[tuple[LedgerEvent, str | None]] = [(item, n) for n in [*names, "c-x", None]]

    found = asyncio.run(service(ledger, contents).snippets(requests))

    key = item.event_id
    assert found == {
        (key, "c-text"): "Line one with bidi tabs",
        (key, "c-json"): "Flaky run pytest -k avg",  # the string values of the document
        (key, "c-edge"): "x" * (MAX_SNIPPET_CHARS - 1) + "…",  # at the limit: read, bounded
        (key, "c-big"): None,  # larger than the read limit: never read
        (key, "c-png"): None,  # not text
        (key, "c-meta"): None,  # not sanitized
        (key, "c-gone"): None,  # the content service could not read it
        (key, "c-blank"): None,  # nothing printable
        (key, "c-x"): None,  # not the event's content
        (key, None): None,  # its first citable content (c-big) is too large
    }
    session = ledger.sessions[-1]  # one transaction after the load
    assert len(ledger.sessions) == 2
    assert [query for query, _ in session.log] == [evidence._TIMEOUT, evidence._PURGED]
    assert session.log[1][1] == {"content_ids": ["c-blank", "c-edge", "c-gone", "c-json", "c-text"]}
    assert contents.reads == [
        (session, "c-blank"),
        (session, "c-edge"),
        (session, "c-gone"),
        (session, "c-json"),
        (session, "c-text"),
    ]


def test_a_purge_recorded_after_the_load_still_wins() -> None:
    ledger, contents, item = _gated()
    ledger.purged.add("c-text")  # recorded between `events` and `snippets`
    ledger_service = service(ledger, contents)

    assert asyncio.run(ledger_service.snippet(item, "c-text")) is None
    assert asyncio.run(ledger_service.snippet(item, "c-json")) == "Flaky run pytest -k avg"
    assert [name for _, name in contents.reads] == ["c-json"]  # purged bytes are never read


def test_the_first_citable_content_skips_content_purged_before_the_load() -> None:
    ledger = Ledger()
    event = ledger.add(sealed(1, ref("c-0", b"purged"), ref("c-1", b"second text")))
    ledger.purged.add("c-0")
    contents = Contents({"c-0": b"purged", "c-1": b"second text"})
    item = load(ledger, [str(event.event_id)])[str(event.event_id)]

    assert asyncio.run(service(ledger, contents).snippet(item, None)) == "second text"
    assert [name for _, name in contents.reads] == ["c-1"]
    assert ledger.statements(evidence._PURGED)[-1] == {"content_ids": ["c-1"]}


def test_no_transaction_opens_when_no_content_passes_the_gates() -> None:
    ledger, contents, item = _gated()
    opened = len(ledger.sessions)

    found = asyncio.run(
        service(ledger, contents).snippets([(item, "c-png"), (item, "c-meta"), (item, "c-big")])
    )

    assert set(found.values()) == {None} and len(found) == 3
    assert len(ledger.sessions) == opened and contents.reads == []


# --- lexical ranking of a scoped set ---


def test_ranking_scores_each_scoped_event_by_its_best_matching_document() -> None:
    ledger = Ledger()
    one, two, outside = (str(event_id(n)) for n in (1, 2, 3))
    ledger.ranked = [
        row(source_event_ids=[event_id(1), event_id(3)], score=0.25),
        row(source_event_ids=[event_id(1), event_id(2)], score=0.75),
        row(source_event_ids=[event_id(3)], score=0.9),
    ]

    scores = asyncio.run(
        service(ledger).rank_documents([two, "junk", one, one.upper()], "average helper", limit=7)
    )

    assert scores == {one: 0.75, two: 0.75}
    assert outside not in scores
    assert all(isinstance(value, float) for value in scores.values())
    assert [query for query, _ in ledger.sessions[0].log] == [evidence._TIMEOUT, evidence._RANK]
    assert ledger.statements(evidence._RANK) == [
        {"query": "average helper", "event_ids": [one, two], "limit": 7}
    ]


def test_ranking_needs_a_valid_event_id_and_a_query() -> None:
    ledger = Ledger()
    ranked = service(ledger)

    assert asyncio.run(ranked.rank_documents([], "average", limit=5)) == {}
    assert asyncio.run(ranked.rank_documents(["junk"], "average", limit=5)) == {}
    assert asyncio.run(ranked.rank_documents([str(event_id(1))], "  \t", limit=5)) == {}
    assert ledger.sessions == []


def test_the_statements_gate_on_delivery_purges_and_the_scoped_ids() -> None:
    """The contract `Ledger` answers by: what each statement filters on."""
    assert "status = 'delivered'" in evidence._DELIVERED
    assert "outbox_id <= CAST(:max AS bigint)" in evidence._DELIVERED
    assert "event_type = 'content.purged'" in evidence._PURGED
    assert "= ANY(:content_ids)" in evidence._PURGED
    assert "&& CAST(:event_ids AS uuid[])" in evidence._RANK
    assert "o.status = 'delivered'" in evidence._RANK
    assert "LIMIT :limit" in evidence._RANK
