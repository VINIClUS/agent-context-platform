"""Contract tests for ``POST /v1/ingestion/batches`` over the real ASGI app.

Authentication, request parsing, limits and error mapping run for real; only
the ingestion service is a fake (its database behaviour is covered by the
integration tests). No sockets and no services are needed.
"""

from __future__ import annotations

import base64
import hashlib
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx2
import pytest
from agent_context_sdk import (
    AcceptedEventV1,
    ContentClaimV1,
    ContentDisposition,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    IngestBatchResponseV1,
    ProducerV1,
    RedactionReportV1,
    RejectedEventV1,
    SanitizedContentItemV1,
)
from agent_context_sdk.ids import new_uuid7
from argon2 import PasswordHasher
from sqlalchemy.exc import OperationalError

from agent_context_platform.app import create_app
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    ProducerRegistration,
)
from agent_context_platform.ledger.service import IngestOutcome
from agent_context_platform.settings import Settings

pytestmark = [pytest.mark.contract, pytest.mark.anyio]

NOW = datetime(2026, 9, 1, tzinfo=UTC)
PREFIX = "prod0001"
TOKEN = f"{PREFIX}.{'k' * 43}"
PRODUCER_ID = "codex-producer-1"
CANARY = "AGENT_CONTEXT_CANARY_ingest_7f3a"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class FakeService:
    """Records batches and answers with a scripted outcome."""

    def __init__(self) -> None:
        self.batches: list[IngestBatchRequestV1] = []
        self.outcome: IngestOutcome | Exception | None = None

    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        self.batches.append(batch)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        if self.outcome is not None:
            return self.outcome
        accepted = tuple(
            AcceptedEventV1(event_id=event.event_id, status="accepted", stream_sequence=index + 1)
            for index, event in enumerate(batch.events)
        )
        return IngestOutcome(200, IngestBatchResponseV1(batch_id=batch.batch_id, accepted=accepted))


def _registration(**overrides: Any) -> ProducerRegistration:
    values: dict[str, Any] = {
        "producer_id": PRODUCER_ID,
        "token_verifier": _HASHER.hash(TOKEN),
        "scope": "events:ingest",
        "expires_at": NOW + timedelta(days=3650),
        "revoked_at": None,
    }
    values.update(overrides)
    return ProducerRegistration(**values)


class Harness:
    def __init__(self, registration: ProducerRegistration | None, max_body: int) -> None:
        self.service = FakeService()
        self.lookups: list[str] = []
        stored = registration

        async def lookup(prefix: str) -> ProducerRegistration | None:
            self.lookups.append(prefix)
            return stored if prefix == PREFIX else None

        self.runtime = IngestionRuntime(
            authenticator=ProducerAuthenticator(
                lookup,
                Argon2Verifier(time_cost=1, memory_cost_kib=8, parallelism=1, max_concurrency=2),
                clock=lambda: NOW,
            ),
            service=self.service,  # type: ignore[arg-type]
            max_request_body_bytes=max_body,
        )
        self.app = create_app(Settings(environment="test"), ingestion=self.runtime)

    def client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=self.app), base_url="http://127.0.0.1:8000"
        )


@pytest.fixture
def harness() -> Harness:
    return Harness(_registration(), max_body=1_000_000)


def _draft(
    *,
    producer_id: str = PRODUCER_ID,
    claims: tuple[ContentClaimV1, ...] = (),
    session_id: str = "session-1",
) -> EventDraftV1:
    observed = datetime(2026, 1, 1, tzinfo=UTC)
    return EventDraftV1(
        event_type="agent.session.started",
        stream_id="stream-1",
        occurred_at=observed,
        observed_at=observed,
        producer=ProducerV1(producer_id=producer_id, name="Codex", version="1.0.0"),
        payload={"source": "codex", "session_id": session_id},
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
        ),
        idempotency_key=f"key-{uuid4()}",
        content_claims=claims,
    )


def _batch(count: int = 1, **kwargs: Any) -> IngestBatchRequestV1:
    return IngestBatchRequestV1(
        batch_id=new_uuid7(), events=tuple(_draft(**kwargs) for _ in range(count))
    )


def _headers(batch_id: UUID | str, token: str | None = TOKEN, **extra: str) -> dict[str, str]:
    headers = {"content-type": "application/json", "idempotency-key": str(batch_id), **extra}
    if token is not None:
        headers["authorization"] = f"Bearer {token}"
    return headers


async def _post(
    harness: Harness, batch: IngestBatchRequestV1, **header_overrides: str
) -> httpx2.Response:
    async with harness.client() as client:
        return await client.post(
            "/v1/ingestion/batches",
            content=batch.model_dump_json(),
            headers=_headers(batch.batch_id, **header_overrides),
        )


@pytest.mark.parametrize("count", [1, 2, 500])
async def test_accepts_one_to_five_hundred_events(harness: Harness, count: int) -> None:
    batch = _batch(count)

    response = await _post(harness, batch)

    assert response.status_code == 200
    body = IngestBatchResponseV1.model_validate(response.json())
    assert body.batch_id == batch.batch_id
    assert [item.event_id for item in body.accepted] == [event.event_id for event in batch.events]
    assert harness.service.batches == [batch]


async def test_rejects_empty_and_oversized_event_lists(harness: Harness) -> None:
    for count in (0, 501):
        events = [_draft().model_dump(mode="json") for _ in range(count)]
        batch_id = str(new_uuid7())
        async with harness.client() as client:
            response = await client.post(
                "/v1/ingestion/batches",
                json={"batch_id": batch_id, "events": events},
                headers=_headers(batch_id),
            )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "invalid_request_schema"
    assert harness.service.batches == []


async def test_rejects_declared_oversized_body_before_reading_it() -> None:
    harness = Harness(_registration(), max_body=512)

    response = await _post(harness, _batch(3))

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_too_large"
    assert harness.service.batches == []


async def test_counts_streamed_bytes_when_content_length_is_absent() -> None:
    harness = Harness(_registration(), max_body=512)
    batch = _batch(3)
    payload = batch.model_dump_json().encode()

    async def chunks() -> AsyncIterator[bytes]:
        for start in range(0, len(payload), 100):
            yield payload[start : start + 100]

    async with harness.client() as client:
        response = await client.post(
            "/v1/ingestion/batches", content=chunks(), headers=_headers(batch.batch_id)
        )

    assert response.status_code == 413
    assert harness.service.batches == []


async def test_authentication_precedes_the_size_limit() -> None:
    harness = Harness(_registration(), max_body=16)

    response = await _post(harness, _batch(), token=None)

    assert response.status_code == 401


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"{}",
        b'{"batch_id": "nope", "events": []}',
        b"[" * 100_000,
    ],
)
async def test_invalid_schema_is_a_fixed_422_that_echoes_nothing(
    harness: Harness, body: bytes
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/ingestion/batches", content=body, headers=_headers(new_uuid7())
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request_schema"
    assert set(response.json()) == {"error", "request_id"}


async def test_unknown_event_type_and_bad_payload_are_schema_errors(harness: Harness) -> None:
    for event_type, payload in (
        ("agent.unknown.event", {"source": "codex"}),
        ("agent.session.started", {"source": "codex"}),
    ):
        draft = _draft().model_dump(mode="json")
        draft["event_type"] = event_type
        draft["payload"] = payload
        batch_id = str(new_uuid7())
        async with harness.client() as client:
            response = await client.post(
                "/v1/ingestion/batches",
                json={"batch_id": batch_id, "events": [draft]},
                headers=_headers(batch_id),
            )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "invalid_request_schema"
    assert harness.service.batches == []


async def test_schema_errors_do_not_echo_submitted_values(harness: Harness) -> None:
    draft = _draft().model_dump(mode="json")
    draft["payload"] = {"source": CANARY}
    draft["unexpected"] = CANARY
    batch_id = str(new_uuid7())

    async with harness.client() as client:
        response = await client.post(
            "/v1/ingestion/batches",
            json={"batch_id": batch_id, "events": [draft]},
            headers=_headers(batch_id),
        )

    assert response.status_code == 422
    assert CANARY not in response.text


async def test_absent_credential_is_401(harness: Harness) -> None:
    response = await _post(harness, _batch(), token=None)

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "missing_credential"
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.parametrize("token", ["garbage", f"{PREFIX}.{'w' * 43}", f"other001.{'k' * 43}"])
async def test_invalid_or_unregistered_credentials_are_401(harness: Harness, token: str) -> None:
    response = await _post(harness, _batch(), token=token)

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_credential"
    assert harness.service.batches == []


async def test_missing_registration_is_401_after_an_argon2_verification(harness: Harness) -> None:
    response = await _post(harness, _batch(), token=f"nobody01.{'k' * 43}")

    assert response.status_code == 401
    assert harness.lookups == ["nobody01"]


@pytest.mark.parametrize(
    "overrides",
    [{"revoked_at": NOW - timedelta(days=1)}, {"expires_at": NOW - timedelta(seconds=1)}],
)
async def test_revoked_and_expired_credentials_are_401(overrides: dict[str, Any]) -> None:
    harness = Harness(_registration(**overrides), max_body=1_000_000)

    response = await _post(harness, _batch())

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_credential"
    assert harness.service.batches == []


async def test_memory_read_registration_is_rejected_on_the_ingestion_route() -> None:
    harness = Harness(_registration(scope="memory:read"), max_body=1_000_000)

    response = await _post(harness, _batch())

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "insufficient_scope"
    assert harness.service.batches == []


async def test_producer_id_mismatch_is_403_and_stores_nothing(harness: Harness) -> None:
    batch = IngestBatchRequestV1(
        batch_id=new_uuid7(), events=(_draft(), _draft(producer_id="someone-else"))
    )

    response = await _post(harness, batch)

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "producer_mismatch"
    assert harness.service.batches == []


async def test_idempotency_key_is_required(harness: Harness) -> None:
    batch = _batch()
    headers = _headers(batch.batch_id)
    del headers["idempotency-key"]
    async with harness.client() as client:
        response = await client.post(
            "/v1/ingestion/batches", content=batch.model_dump_json(), headers=headers
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_key_required"


@pytest.mark.parametrize("key", ["not-a-uuid", str(uuid4()), ""])
async def test_idempotency_key_must_equal_batch_id(harness: Harness, key: str) -> None:
    batch = _batch()
    async with harness.client() as client:
        response = await client.post(
            "/v1/ingestion/batches",
            content=batch.model_dump_json(),
            headers=_headers(key),
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "idempotency_key_mismatch"
    assert harness.service.batches == []


async def test_idempotency_key_comparison_ignores_uuid_formatting(harness: Harness) -> None:
    batch = _batch()

    response = await _post(harness, batch, **{"idempotency-key": str(batch.batch_id).upper()})

    assert response.status_code == 200


async def test_partial_duplicate_batch_reports_existing_and_new_events(harness: Harness) -> None:
    batch = _batch(2)
    first, second = batch.events
    harness.service.outcome = IngestOutcome(
        200,
        IngestBatchResponseV1(
            batch_id=batch.batch_id,
            accepted=(
                AcceptedEventV1(event_id=first.event_id, status="existing", stream_sequence=4),
                AcceptedEventV1(event_id=second.event_id, status="accepted", stream_sequence=5),
            ),
        ),
    )

    response = await _post(harness, batch)

    assert response.status_code == 200
    statuses = {item["event_id"]: item["status"] for item in response.json()["accepted"]}
    assert statuses == {str(first.event_id): "existing", str(second.event_id): "accepted"}


async def test_rejected_batch_keeps_service_status_and_lists_only_existing(
    harness: Harness,
) -> None:
    batch = _batch(2)
    first, second = batch.events
    harness.service.outcome = IngestOutcome(
        409,
        IngestBatchResponseV1(
            batch_id=batch.batch_id,
            accepted=(
                AcceptedEventV1(event_id=first.event_id, status="existing", stream_sequence=1),
            ),
            rejected=(
                RejectedEventV1(
                    event_id=second.event_id, error_code="idempotency_conflict", retryable=False
                ),
            ),
        ),
    )

    response = await _post(harness, batch)

    assert response.status_code == 409
    assert response.json()["rejected"][0]["error_code"] == "idempotency_conflict"


async def test_retryable_outcome_carries_retry_after(harness: Harness) -> None:
    batch = _batch()
    harness.service.outcome = IngestOutcome(
        503,
        IngestBatchResponseV1(
            batch_id=batch.batch_id,
            rejected=(
                RejectedEventV1(
                    event_id=batch.events[0].event_id,
                    error_code="service_unavailable",
                    retryable=True,
                ),
            ),
        ),
        retry_after_seconds=7,
    )

    response = await _post(harness, batch)

    assert response.status_code == 503
    assert response.headers["retry-after"] == "7"
    assert response.json()["rejected"][0]["retryable"] is True


async def test_database_outage_is_a_retryable_503(harness: Harness) -> None:
    harness.service.outcome = OperationalError("SELECT 1", {"secret": CANARY}, Exception(CANARY))

    response = await _post(harness, _batch())

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
    assert "retry-after" in response.headers
    assert CANARY not in response.text


async def test_unexpected_failure_is_a_fixed_500_that_leaks_nothing(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    harness.service.outcome = RuntimeError(CANARY)
    caplog.set_level(logging.DEBUG)

    response = await _post(harness, _batch())

    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert CANARY not in response.text
    assert CANARY not in caplog.text


async def test_unconfigured_ingestion_is_503() -> None:
    application = create_app(Settings(environment="test"))
    batch = _batch()

    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=application), base_url="http://127.0.0.1:8000"
    ) as client:
        response = await client.post(
            "/v1/ingestion/batches",
            content=batch.model_dump_json(),
            headers=_headers(batch.batch_id),
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "ingestion_unavailable"


async def test_request_id_is_generated_or_accepted_when_safe(harness: Harness) -> None:
    generated = await _post(harness, _batch())
    accepted = await _post(harness, _batch(), **{"x-request-id": "req-123.abc_9"})
    replaced = await _post(harness, _batch(), **{"x-request-id": "bad id\twith spaces"})

    assert len(generated.headers["x-request-id"]) == 32
    assert accepted.headers["x-request-id"] == "req-123.abc_9"
    assert replaced.headers["x-request-id"] != "bad id\twith spaces"
    assert len(replaced.headers["x-request-id"]) == 32


async def test_error_bodies_carry_the_request_id(harness: Harness) -> None:
    response = await _post(harness, _batch(), token=None, **{"x-request-id": "req-77"})

    assert response.json()["request_id"] == "req-77"


async def test_logs_carry_ids_and_trace_but_never_request_content(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    batch = IngestBatchRequestV1(batch_id=new_uuid7(), events=(_draft(session_id=CANARY),))
    secret_token = f"{PREFIX}.{'k' * 43}"

    ok = await _post(harness, batch, **{"x-request-id": "req-log-1", "traceparent": TRACEPARENT})
    bad = await _post(harness, batch, token=f"{PREFIX}.{'z' * 43}")

    assert ok.status_code == 200
    assert bad.status_code == 401
    assert "req-log-1" in caplog.text
    assert "request_id" in caplog.records[0].__dict__
    assert {record.__dict__.get("trace_id") for record in caplog.records} >= {
        "4bf92f3577b34da6a3ce929d0e0e4736"
    }
    for forbidden in (CANARY, secret_token, "k" * 43, "z" * 43, "Bearer"):
        assert forbidden not in caplog.text
        assert all(forbidden not in str(record.__dict__) for record in caplog.records)


async def test_content_items_travel_through_the_route(harness: Harness) -> None:
    data = b"hello world"
    claim = ContentClaimV1(
        content_id="c1",
        content_sha256=hashlib.sha256(data).hexdigest(),
        media_type="text/plain",
        uncompressed_bytes=len(data),
    )
    item = SanitizedContentItemV1(
        claim=claim,
        sanitized_bytes_base64=base64.b64encode(data).decode(),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )
    batch = IngestBatchRequestV1(
        batch_id=new_uuid7(), events=(_draft(claims=(claim,)),), content_items=(item,)
    )

    response = await _post(harness, batch)

    assert response.status_code == 200
    assert harness.service.batches[0].content_items == (item,)


async def test_mcp_route_stays_mounted_and_gets_a_request_id(harness: Harness) -> None:
    async with harness.client() as client:
        response = await client.get("/mcp", headers={"x-request-id": "mcp-req-1"})

    assert response.status_code == 405
    assert response.headers["x-request-id"] == "mcp-req-1"
