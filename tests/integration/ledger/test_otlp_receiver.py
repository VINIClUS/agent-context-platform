"""Integration: a Codex trace-safe OTLP export becomes a canonical ledger event (PLATFORM-053).

The full stack runs in process against the migrated schema as the least-privileged
``agent_context_api`` role: real producer authentication, the receiver, the mapper and the
ingestion service. Retried exports are idempotent, platform-origin resources never reach the
ledger, and canary attribute values are never persisted or logged.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx2
import pytest
from agent_context_sdk import RedactionPolicyV1
from argon2 import PasswordHasher
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.app import create_app
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    SqlProducerLookup,
)
from agent_context_platform.ledger.otlp_api import TRACES_PATH, OtlpReceiverRuntime
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.mcp.auth import TokenBucketLimiter
from agent_context_platform.projection.verify import NoContentBlobStore
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.integration

PRODUCER_ID = "otel-integration-producer"
PREFIX = "otelint1"
EVENT_TIME_NS = 1_786_626_000_200_000_000


def _pairs(attributes: dict[str, Any]) -> list[KeyValue]:
    return [
        KeyValue(key=key, value=AnyValue(string_value=str(value)))
        for key, value in attributes.items()
    ]


def _export(resource: dict[str, Any], attributes: dict[str, Any]) -> bytes:
    span = Span(
        trace_id=bytes.fromhex("4bf92f3577b34da6a3ce929d0e0e4736"),
        span_id=bytes.fromhex("00f067aa0ba902b7"),
        name="session_task",
        start_time_unix_nano=EVENT_TIME_NS - 1_000,
        end_time_unix_nano=EVENT_TIME_NS,
    )
    span.events.add(name="event", time_unix_nano=EVENT_TIME_NS, attributes=_pairs(attributes))
    return ExportTraceServiceRequest(
        resource_spans=[
            ResourceSpans(
                resource=Resource(attributes=_pairs(resource)),
                scope_spans=[ScopeSpans(spans=[span])],
            )
        ]
    ).SerializeToString()


class Receiver:
    def __init__(self, owner: AsyncEngine, app: Any, token: str) -> None:
        self.owner = owner
        self.app = app
        self.token = token

    async def post(self, body: bytes) -> httpx2.Response:
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=self.app), base_url="http://127.0.0.1:8000"
        ) as client:
            return await client.post(
                TRACES_PATH,
                content=body,
                headers={
                    "authorization": f"Bearer {self.token}",
                    "content-type": "application/x-protobuf",
                },
            )

    async def events(self, session_id: str) -> list[dict[str, Any]]:
        async with self.owner.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT event_type, stream_id, producer_id, payload::text AS payload "
                    "FROM ledger.events WHERE producer_id = :producer "
                    "AND payload->>'session_id' = :session"
                ),
                {"producer": PRODUCER_ID, "session": session_id},
            )
            return [dict(row._mapping) for row in rows]

    async def all_text(self) -> str:
        """Every ledger and outbox column of this producer's events, as text."""
        async with self.owner.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT e::text FROM ledger.events e WHERE producer_id = :producer "
                    "UNION ALL SELECT s::text FROM ledger.event_streams s "
                    "WHERE stream_id LIKE 'codex-otel:%'"
                ),
                {"producer": PRODUCER_ID},
            )
            return "\n".join(str(row[0]) for row in rows)


@pytest.fixture
def receiver(ledger_engine: AsyncEngine, postgres_dsn: str) -> Iterator[Receiver]:
    token = f"{PREFIX}.{secrets.token_urlsafe(32)}"
    now = datetime.now(UTC)

    async def register() -> None:
        async with ledger_engine.begin() as connection:
            await connection.execute(
                text("DELETE FROM operations.registered_producers WHERE producer_id = :id"),
                {"id": PRODUCER_ID},
            )
            await connection.execute(
                text(
                    "INSERT INTO operations.registered_producers (producer_id, token_prefix, "
                    "token_verifier, scope, expires_at, created_at, updated_at) VALUES "
                    "(:id, :prefix, :verifier, 'events:ingest', :expires, :now, :now)"
                ),
                {
                    "id": PRODUCER_ID,
                    "prefix": PREFIX,
                    "verifier": PasswordHasher(time_cost=1, memory_cost=8, parallelism=1).hash(
                        token
                    ),
                    "expires": now + timedelta(days=1),
                    "now": now,
                },
            )

    asyncio.run(register())
    api_engine = create_async_engine(
        postgres_dsn, poolclass=NullPool, connect_args={"options": "-c role=agent_context_api"}
    )
    sessions = session_factory(api_engine)
    runtime = OtlpReceiverRuntime(
        authenticator=ProducerAuthenticator(
            SqlProducerLookup(sessions),
            Argon2Verifier(
                time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2, max_queue_depth=8
            ),
        ),
        service=IngestionService(
            ContentService(NoContentBlobStore(), RedactionPolicyV1()), sessions
        ),
        limiter=TokenBucketLimiter(rate_per_second=100, burst=100),
        hmac_key=secrets.token_bytes(32),
        max_request_body_bytes=1_000_000,
        max_decompressed_bytes=4_000_000,
        max_spans=500,
    )
    yield Receiver(
        ledger_engine, create_app(Settings(environment="test"), otlp_receiver=runtime), token
    )
    asyncio.run(api_engine.dispose())


def _fixture() -> dict[str, Any]:
    from pathlib import Path

    path = Path(__file__).parents[2] / "fixtures" / "codex_otel" / "conversation_starts.trace.json"
    return json.loads(path.read_text())


def test_a_codex_export_becomes_one_canonical_event_idempotently(
    receiver: Receiver, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    fixture = _fixture()
    session_id = f"0198a4b1-98c0-7c28-ae3f-{secrets.token_hex(6)}"
    canaries = [f"CANARYotel{label}{secrets.token_hex(8)}" for label in ("error", "prompt", "host")]
    attributes = {
        **fixture["attributes"],
        "conversation.id": session_id,
        "error.message": canaries[0],
        "prompt": canaries[1],
    }
    resource = {**fixture["resource"], "host.name": canaries[2]}
    body = _export(resource, attributes)

    async def exercise() -> None:
        first = await receiver.post(body)
        retried = await receiver.post(body)  # the collector retries the same export
        for response in (first, retried):
            assert response.status_code == 200, response.status_code
            assert not ExportTraceServiceResponse.FromString(response.content).HasField(
                "partial_success"
            )

        (event,) = await receiver.events(session_id)
        assert event["event_type"] == "agent.session.started"
        assert event["stream_id"] == f"codex-otel:{PRODUCER_ID}:{session_id}"
        payload = json.loads(event["payload"])
        assert payload["session_id"] == session_id
        assert payload["source"] == "codex_otel"

        stored = await receiver.all_text()
        for canary in canaries:
            assert canary not in stored
            assert canary not in caplog.text
            assert all(canary not in json.dumps(r.__dict__, default=str) for r in caplog.records)

    asyncio.run(exercise())


def test_a_platform_origin_export_never_reaches_the_ledger(receiver: Receiver) -> None:
    fixture = _fixture()
    session_id = f"0198a4b1-98c0-7c28-ae3f-{secrets.token_hex(6)}"
    body = _export(
        {"service.name": "agent-context-platform", "agent_context.origin": "platform"},
        {**fixture["attributes"], "conversation.id": session_id},
    )

    async def exercise() -> None:
        response = await receiver.post(body)
        partial = ExportTraceServiceResponse.FromString(response.content).partial_success
        assert (partial.rejected_spans, partial.error_message) == (
            1,
            "spans rejected: platform_origin",
        )
        assert await receiver.events(session_id) == []

    asyncio.run(exercise())


def _multi_export(resource: dict[str, Any], records: list[tuple[str, dict[str, Any]]]) -> bytes:
    """One span per record; the span id is given per record (it is part of the trace context)."""
    spans = []
    for span_id, attributes in records:
        span = Span(
            trace_id=bytes.fromhex("4bf92f3577b34da6a3ce929d0e0e4736"),
            span_id=bytes.fromhex(span_id),
            name="session_task",
            start_time_unix_nano=EVENT_TIME_NS - 1_000,
            end_time_unix_nano=EVENT_TIME_NS,
        )
        span.events.add(name="event", time_unix_nano=EVENT_TIME_NS, attributes=_pairs(attributes))
        spans.append(span)
    return ExportTraceServiceRequest(
        resource_spans=[
            ResourceSpans(
                resource=Resource(attributes=_pairs(resource)),
                scope_spans=[ScopeSpans(spans=spans)],
            )
        ]
    ).SerializeToString()


def test_one_conflicting_record_never_drops_the_rest_of_its_export(receiver: Receiver) -> None:
    """Review S1: A = [X, Q]; B = [X again, Q under another span id, Y new]."""
    fixture = _fixture()
    ids = {name: f"0198a4b1-98c0-7c28-ae3f-{secrets.token_hex(6)}" for name in ("x", "q", "y")}

    def record(name: str) -> dict[str, Any]:
        return {**fixture["attributes"], "conversation.id": ids[name]}

    first = _multi_export(
        fixture["resource"], [("00f067aa0ba902b7", record("x")), ("00f067aa0ba902b8", record("q"))]
    )
    second = _multi_export(
        fixture["resource"],
        [
            ("00f067aa0ba902b7", record("x")),  # identical: already stored, `existing`
            ("1111111111111111", record("q")),  # same key, other trace context: a conflict
            ("00f067aa0ba902b9", record("y")),  # new
        ],
    )

    async def exercise() -> None:
        assert (await receiver.post(first)).status_code == 200
        response = await receiver.post(second)
        assert response.status_code == 200
        partial = ExportTraceServiceResponse.FromString(response.content).partial_success
        assert (partial.rejected_spans, partial.error_message) == (
            1,
            "spans rejected: idempotency_conflict",
        )
        for name in ("x", "q", "y"):
            assert len(await receiver.events(ids[name])) == 1, name

    asyncio.run(exercise())
