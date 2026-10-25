"""`agent-context ledger verify` against a real ledger (PLATFORM-056).

Events are ingested through the real `IngestionService` as the API role; the CLI verifies them as
a LOGIN member of `agent_context_projector` (SELECT-only on the ledger). Each corruption is made
in place by the schema owner, exactly what a bad restore or a tampering operator could do, and
must be reported with exit 1 and the right check name.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import uuid
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    ProducerV1,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
)
from agent_context_sdk.ids import new_uuid7
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.content.blob_store import BlobStoreError, StoredBlob
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.ledger import verify as ledger_verify
from agent_context_platform.ledger.service import IngestionService

pytestmark = pytest.mark.integration

T0 = datetime(2026, 9, 1, tzinfo=UTC)
PRODUCER = ProducerV1(producer_id="ledger-verify-test", name="Codex", version="1.0.0")


class InlineOnlyBlobStore:
    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        raise BlobStoreError("test content is inline")

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        raise BlobStoreError("test content is inline")

    async def delete(self, object_key: str) -> None:
        raise BlobStoreError("test content is inline")


def _login(postgres_dsn: str, owner: AsyncEngine, parent: str) -> Iterator[str]:
    name = f"ledger_verify_{parent.rsplit('_', 1)[-1]}_{uuid.uuid4().hex[:8]}"
    password = uuid.uuid4().hex

    async def run(statement: str) -> None:
        async with owner.begin() as connection:
            await connection.execute(text(statement))

    asyncio.run(run(f"CREATE ROLE {name} LOGIN PASSWORD '{password}' IN ROLE {parent}"))
    try:
        yield (
            make_url(postgres_dsn)
            .set(username=name, password=password)
            .render_as_string(hide_password=False)
        )
    finally:
        asyncio.run(run(f"DROP ROLE {name}"))


@pytest.fixture(scope="module")
def projector_login(postgres_dsn: str, ledger_engine: AsyncEngine) -> Iterator[str]:
    yield from _login(postgres_dsn, ledger_engine, "agent_context_projector")


@pytest.fixture(scope="module")
def api_login(postgres_dsn: str, ledger_engine: AsyncEngine) -> Iterator[str]:
    yield from _login(postgres_dsn, ledger_engine, "agent_context_api")


def _draft(stream: str, number: int, body: bytes | None) -> tuple[EventDraftV1, list[Any]]:
    payload: dict[str, Any] = {"source": "codex", "session_id": "s-1", "turn_id": f"t-{number}"}
    claims: tuple[ContentClaimV1, ...] = ()
    items: list[Any] = []
    if body is not None:
        # The first event carries three refs, claimed out of order (the ledger sorts them).
        suffixes = ("-z", "", "-a") if number == 1 else ("",)
        for suffix in suffixes:
            content = body + suffix.encode()
            claim = ContentClaimV1(
                content_id=f"{stream}-c-{number}{suffix}",
                content_sha256=hashlib.sha256(content).hexdigest(),
                media_type="text/plain",
                uncompressed_bytes=len(content),
            )
            claims += (claim,)
            items.append(
                SanitizedContentItemV1(
                    claim=claim,
                    sanitized_bytes_base64=base64.b64encode(content).decode(),
                    redaction_report=RedactionReportV1(
                        policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
                    ),
                )
            )
        payload["user_message_content_id"] = f"{stream}-c-{number}"
    moment = T0 + timedelta(seconds=number)
    draft = EventDraftV1(
        event_type="agent.turn.started",
        stream_id=stream,
        occurred_at=moment,
        observed_at=moment,
        producer=PRODUCER,
        payload=payload,
        context=EventContextV1(session_id="s-1", turn_id=f"t-{number}"),
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=f"{stream}-{number}",
        content_claims=claims,
    )
    return draft, items


async def ingest(api_login: str, stream: str, count: int, *, chunk: int = 100) -> None:
    engine = create_async_engine(api_login, poolclass=NullPool)
    try:
        service = IngestionService(
            ContentService(InlineOnlyBlobStore(), RedactionPolicyV1()), session_factory(engine)
        )
        for start in range(1, count + 1, chunk):
            drafts, items = [], []
            for number in range(start, min(start + chunk, count + 1)):
                draft, content = _draft(
                    stream, number, b"text %d" % number if number % 3 == 1 else None
                )
                drafts.append(draft)
                items.extend(content)
            outcome = await service.ingest(
                IngestBatchRequestV1(
                    batch_id=new_uuid7(), events=tuple(drafts), content_items=tuple(items)
                )
            )
            assert outcome.http_status < 300, outcome.response
    finally:
        await engine.dispose()


async def owner_sql(owner: AsyncEngine, *statements: str) -> None:
    async with owner.begin() as connection:
        await connection.execute(text("SET LOCAL session_replication_role = replica"))
        for statement in statements:
            await connection.execute(text(statement))


async def reset(owner: AsyncEngine) -> None:
    async with owner.begin() as connection:
        await connection.execute(text("TRUNCATE ledger.event_streams, ledger.events CASCADE"))


def verify_cli(projector_login: str, monkeypatch: pytest.MonkeyPatch, *args: str) -> Any:
    monkeypatch.setenv("AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN", projector_login)
    monkeypatch.delenv("AGENT_CONTEXT_POSTGRESQL__DSN", raising=False)
    return CliRunner().invoke(cli.app, ["ledger", "verify", "--json", *args])


@pytest.fixture
def ledger(
    ledger_engine: AsyncEngine, api_login: str, projector_login: str
) -> Iterator[AsyncEngine]:
    asyncio.run(reset(ledger_engine))
    asyncio.run(ingest(api_login, "stream-a", 4))
    asyncio.run(ingest(api_login, "stream-b", 2))
    yield ledger_engine
    asyncio.run(reset(ledger_engine))


def _where(sequence: int) -> str:
    return f"stream_id = 'stream-a' AND stream_sequence = {sequence}"


def test_an_intact_ledger_verifies_with_exit_0(
    ledger: AsyncEngine, projector_login: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = verify_cli(projector_login, monkeypatch)

    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["ok"] is True
    assert report["totals"] == {"streams": 2, "streams_failed": 0, "events": 6}
    assert [(s["stream_id"], s["events"], s["last_sequence"]) for s in report["streams"]] == [
        ("stream-a", 4, 4),
        ("stream-b", 2, 2),
    ]


CORRUPTIONS: dict[str, tuple[tuple[str, ...], str, int | None]] = {
    "payload_byte": (
        (
            f"UPDATE ledger.events SET payload = jsonb_set(payload, '{{turn_id}}', '\"t-X\"') "
            f"WHERE {_where(2)}",
        ),
        "payload_sha256",
        2,
    ),
    "middle_digest_with_next_link_repaired": (
        (
            f"UPDATE ledger.events SET event_sha256 = repeat('f', 64) WHERE {_where(2)}",
            f"UPDATE ledger.events SET previous_event_sha256 = repeat('f', 64) WHERE {_where(3)}",
        ),
        "event_sha256",
        2,
    ),
    "broken_link": (
        (f"UPDATE ledger.events SET previous_event_sha256 = repeat('a', 64) WHERE {_where(3)}",),
        "previous_event_sha256",
        3,
    ),
    "sequence_gap": (
        (
            "UPDATE ledger.events SET stream_sequence = 5 WHERE stream_id = 'stream-a' "
            "AND stream_sequence = 4",
        ),
        "sequence",
        5,
    ),
    "content_ref_metadata": (
        (
            "UPDATE ledger.event_content_refs SET media_type = 'text/markdown' "
            "WHERE content_id = 'stream-a-c-1'",
        ),
        "event_sha256",
        1,
    ),
    "content_ref_deleted": (
        ("DELETE FROM ledger.event_content_refs WHERE content_id = 'stream-a-c-1-z'",),
        "event_sha256",
        1,
    ),
    "stream_head": (
        (
            "UPDATE ledger.event_streams SET last_event_sha256 = repeat('c', 64) "
            "WHERE stream_id = 'stream-a'",
        ),
        "stream_head",
        4,
    ),
}


@pytest.mark.parametrize("case", sorted(CORRUPTIONS))
def test_each_corruption_is_detected_with_exit_1_and_its_check(
    ledger: AsyncEngine, projector_login: str, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    statements, check, sequence = CORRUPTIONS[case]
    asyncio.run(owner_sql(ledger, *statements))

    result = verify_cli(projector_login, monkeypatch)

    assert result.exit_code == 1, result.output
    report = json.loads(result.stdout)
    assert report["ok"] is False and report["totals"]["streams_failed"] == 1
    failed = {s["stream_id"]: s for s in report["streams"]}
    failure = failed["stream-a"]["failure"]
    assert (failure["check"], failure["stream_sequence"]) == (check, sequence)
    assert failed["stream-b"]["ok"] is True  # one stream's damage never hides another's state
    assert "text 1" not in result.output  # never content


def test_the_walk_never_reads_content_so_purged_bytes_cannot_matter(
    ledger: AsyncEngine, projector_login: str
) -> None:
    # Purge is not implemented yet; it is specified to delete stored bytes and append a
    # `content.purged` event, never to touch an event or its refs. Deleting bytes here would need
    # the FKs disabled, so instead prove the verifier issues no statement against content storage.
    statements: list[str] = []

    async def run() -> ledger_verify.LedgerVerification:
        engine = create_async_engine(projector_login, poolclass=NullPool)
        event.listen(
            engine.sync_engine,
            "before_cursor_execute",
            lambda _conn, _cursor, statement, *_rest: statements.append(statement),
        )
        try:
            return await ledger_verify.verify_ledger(session_factory(engine))
        finally:
            await engine.dispose()

    report = asyncio.run(run())

    assert report.ok and report.to_dict()["totals"]["events"] == 6
    assert statements and not [s for s in statements if "catalog." in s or "content_objects" in s]
    assert any("ledger.event_content_refs" in s for s in statements)


def test_events_without_a_head_row_are_never_skipped(
    ledger: AsyncEngine, projector_login: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A partial restore (pg_restore continues after errors): the head rows are gone, the events
    # are not, and one of them was tampered with.
    monkeypatch.setattr(ledger_verify, "STREAM_PAGE_SIZE", 1)  # page across both tables
    asyncio.run(
        owner_sql(
            ledger,
            "UPDATE ledger.events SET payload = jsonb_set(payload, '{turn_id}', '\"t-X\"') "
            f"WHERE {_where(2)}",
            "DELETE FROM ledger.event_streams WHERE stream_id = 'stream-a'",
        )
    )

    one = verify_cli(projector_login, monkeypatch)
    assert one.exit_code == 1, one.output
    streams = {s["stream_id"]: s for s in json.loads(one.stdout)["streams"]}
    assert streams["stream-a"]["failure"]["check"] == "stream_head_missing"
    assert streams["stream-b"]["ok"] is True
    named = verify_cli(projector_login, monkeypatch, "--stream", "stream-a")
    assert named.exit_code == 1
    assert json.loads(named.stdout)["streams"][0]["failure"]["check"] == "stream_head_missing"

    asyncio.run(owner_sql(ledger, "DELETE FROM ledger.event_streams"))
    every = verify_cli(projector_login, monkeypatch)
    assert every.exit_code == 1, every.output
    report = json.loads(every.stdout)
    assert report["totals"] == {"streams": 2, "streams_failed": 2, "events": 0}
    assert {s["failure"]["check"] for s in report["streams"]} == {"stream_head_missing"}


def test_events_outside_the_walk_fail_event_count(
    ledger: AsyncEngine, projector_login: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Defence in depth: should the enumeration ever miss a stream, the snapshot's count catches it.
    enumerate_page = ledger_verify._stream_ids

    async def lossy(session: Any, after: str) -> list[str]:
        return [stream for stream in await enumerate_page(session, after) if stream != "stream-b"]

    monkeypatch.setattr(ledger_verify, "_stream_ids", lossy)

    result = verify_cli(projector_login, monkeypatch)

    assert result.exit_code == 1, result.output
    report = json.loads(result.stdout)
    assert report["failure"] == {
        "check": "event_count",
        "stream_sequence": None,
        "event_id": None,
        "expected": "6",
        "actual": "4",
    }
    assert report["totals"]["streams_failed"] == 0


def test_a_head_past_the_range_must_name_the_real_last_event(
    ledger: AsyncEngine, projector_login: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    asyncio.run(
        owner_sql(
            ledger,
            "UPDATE ledger.event_streams SET last_event_sha256 = repeat('c', 64) "
            "WHERE stream_id = 'stream-b'",
        )
    )

    # stream-b ends at sequence 2, before the range: no event is walked, its head still is.
    result = verify_cli(projector_login, monkeypatch, "--since-sequence", "4")

    assert result.exit_code == 1, result.output
    streams = {s["stream_id"]: s for s in json.loads(result.stdout)["streams"]}
    assert streams["stream-a"]["ok"] is True
    failure = streams["stream-b"]["failure"]
    assert (failure["check"], failure["stream_sequence"], failure["actual"]) == (
        "stream_head",
        2,
        "c" * 64,
    )


def test_streams_and_since_sequence_narrow_the_walk(
    ledger: AsyncEngine, projector_login: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    asyncio.run(
        owner_sql(
            ledger, f"UPDATE ledger.events SET event_sha256 = repeat('f', 64) WHERE {_where(1)}"
        )
    )

    only_b = verify_cli(projector_login, monkeypatch, "--stream", "stream-b")
    assert only_b.exit_code == 0 and json.loads(only_b.stdout)["totals"]["streams"] == 1
    # From sequence 3: event 2's stored digest is the link, so event 1's damage is out of range.
    tail = verify_cli(projector_login, monkeypatch, "--stream", "stream-a", "--since-sequence", "3")
    assert tail.exit_code == 0, tail.output
    assert json.loads(tail.stdout)["streams"][0]["first_sequence"] == 3
    # stream-b ends (sequence 2) before the range: nothing to walk, and that is not a failure.
    late = verify_cli(projector_login, monkeypatch, "--since-sequence", "4")
    assert late.exit_code == 0, late.output
    assert [s["events"] for s in json.loads(late.stdout)["streams"]] == [1, 0]
    asyncio.run(owner_sql(ledger, f"DELETE FROM ledger.events WHERE {_where(2)}"))
    gap = verify_cli(projector_login, monkeypatch, "--stream", "stream-a", "--since-sequence", "3")
    assert gap.exit_code == 1
    failure = json.loads(gap.stdout)["streams"][0]["failure"]
    assert (failure["check"], failure["stream_sequence"]) == ("sequence", 3)
    missing = verify_cli(projector_login, monkeypatch, "--stream", "no-such-stream")
    assert missing.exit_code == 1
    assert json.loads(missing.stdout)["streams"][0]["failure"]["check"] == "stream_exists"


def test_a_role_without_ledger_select_is_a_usage_error(
    ledger: AsyncEngine, postgres_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    for login in _login(postgres_dsn, ledger, "pg_monitor"):  # a role with no ledger grant
        result = verify_cli(login, monkeypatch)
        assert result.exit_code == 2
        assert "permission denied" in result.output
        assert "SELECT on ledger.events" in result.output


def test_the_walk_is_one_read_only_snapshot(
    ledger: AsyncEngine, projector_login: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[tuple[str, str]] = []
    fetch = ledger_verify._content_refs

    async def spy(session: Any, event_ids: Sequence[Any]) -> Any:
        read_only = await session.scalar(text("SHOW transaction_read_only"))
        isolation = await session.scalar(text("SHOW transaction_isolation"))
        seen.append((read_only, isolation))
        return await fetch(session, event_ids)

    monkeypatch.setattr(ledger_verify, "_content_refs", spy)

    async def run() -> None:
        engine = create_async_engine(projector_login, poolclass=NullPool)
        try:
            assert (await ledger_verify.verify_ledger(session_factory(engine))).ok
            async with session_factory(engine)() as session, session.begin():
                await session.execute(text(ledger_verify._READ_ONLY))
                with pytest.raises(DBAPIError, match="read-only transaction"):
                    await session.execute(text("CREATE TEMPORARY TABLE probe (x int)"))
        finally:
            await engine.dispose()

    asyncio.run(run())
    assert seen and set(seen) == {("on", "repeatable read")}


def test_a_large_ledger_is_streamed_in_bounded_batches(
    ledger_engine: AsyncEngine,
    api_login: str,
    projector_login: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(reset(ledger_engine))
    count, batch = 1200, 50
    asyncio.run(ingest(api_login, "stream-large", count))
    sizes: list[int] = []
    fetch = ledger_verify._content_refs

    async def spy(session: Any, event_ids: Sequence[Any]) -> Any:
        sizes.append(len(event_ids))
        return await fetch(session, event_ids)

    monkeypatch.setattr(ledger_verify, "_content_refs", spy)

    async def run() -> ledger_verify.LedgerVerification:
        engine = create_async_engine(projector_login, poolclass=NullPool)
        try:
            return await ledger_verify.verify_ledger(session_factory(engine), batch_size=batch)
        finally:
            await engine.dispose()

    try:
        report = asyncio.run(run())
    finally:
        asyncio.run(reset(ledger_engine))

    assert report.ok and report.to_dict()["totals"]["events"] == count
    assert report.batches == math.ceil(count / batch) == len(sizes)
    assert max(sizes) == batch  # never more than one batch of events in memory
