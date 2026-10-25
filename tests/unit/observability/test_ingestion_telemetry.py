"""Ingestion and HTTP telemetry through real requests to the real application.

Authentication, parsing and the error mapping run for real (the ingestion service is a fake, as
in `tests/contract/test_ingestion_api.py`). Canaries go into content, payloads, the bearer token,
the configured DSN, query strings and paths; none may appear in any exported span, metric data
point or log record.
"""

from __future__ import annotations

import base64
import hashlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

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
from opentelemetry.trace import SpanKind, StatusCode
from sqlalchemy.exc import OperationalError

from agent_context_platform.app import create_app
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    ProducerRegistration,
)
from agent_context_platform.ledger.service import IngestOutcome
from agent_context_platform.observability import contract
from agent_context_platform.settings import Settings

from ._capture import Canaries, Capture, log_text

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

NOW = datetime(2026, 9, 1, tzinfo=UTC)
PREFIX = "prod0001"
PRODUCER_ID = "codex-producer-1"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class ScriptedService:
    def __init__(self) -> None:
        self.outcome: IngestOutcome | Exception | None = None

    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        if isinstance(self.outcome, Exception):
            raise self.outcome
        if self.outcome is not None:
            return self.outcome
        accepted = tuple(
            AcceptedEventV1(
                event_id=event.event_id,
                status="existing" if index == 0 else "accepted",
                stream_sequence=index + 1,
            )
            for index, event in enumerate(batch.events)
        )
        return IngestOutcome(200, IngestBatchResponseV1(batch_id=batch.batch_id, accepted=accepted))


class Harness:
    def __init__(self, canaries: Canaries) -> None:
        self.token = f"{PREFIX}.{canaries.token_secret}"
        registration = ProducerRegistration(
            producer_id=PRODUCER_ID,
            token_verifier=_HASHER.hash(self.token),
            scope="events:ingest",
            expires_at=NOW + timedelta(days=1),
            revoked_at=None,
        )

        async def lookup(prefix: str) -> ProducerRegistration | None:
            return registration if prefix == PREFIX else None

        self.service = ScriptedService()
        runtime = IngestionRuntime(
            authenticator=ProducerAuthenticator(
                lookup,
                Argon2Verifier(
                    time_cost=1,
                    memory_cost_kib=8,
                    parallelism=1,
                    max_concurrency=2,
                    max_queue_depth=8,
                ),
                clock=lambda: NOW,
            ),
            service=self.service,  # type: ignore[arg-type]
            max_request_body_bytes=200_000,
        )
        # A DSN whose password is a canary: configuration must never reach telemetry.
        settings = Settings(
            environment="test",
            postgresql={
                "dsn": f"postgresql+psycopg://platform:{canaries.dsn_password}@db/agent_context"
            },
        )
        self.app = create_app(settings, ingestion=runtime)

    async def post(
        self, body: bytes | str, *, batch_id: Any = None, token: str | None = None, **headers: str
    ) -> httpx2.Response:
        request_headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {token or self.token}",
            **headers,
        }
        if batch_id is not None:
            request_headers["idempotency-key"] = str(batch_id)
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=self.app), base_url="http://127.0.0.1:8000"
        ) as client:
            return await client.post("/v1/ingestion/batches", content=body, headers=request_headers)


def _batch(canaries: Canaries, *, count: int = 2, producer_id: str = PRODUCER_ID) -> Any:
    data = f'{{"prompt": "{canaries.prompt}", "content": "{canaries.content}"}}'.encode()
    claim = ContentClaimV1(
        content_id="c1",
        content_sha256=hashlib.sha256(data).hexdigest(),
        media_type="application/json",
        uncompressed_bytes=len(data),
    )
    item = SanitizedContentItemV1(
        claim=claim,
        sanitized_bytes_base64=base64.b64encode(data).decode(),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )
    observed = datetime(2026, 1, 1, tzinfo=UTC)
    events = tuple(
        EventDraftV1(
            event_type="agent.session.started",
            stream_id="stream-1",
            occurred_at=observed,
            observed_at=observed,
            producer=ProducerV1(producer_id=producer_id, name="Codex", version="1.0.0"),
            payload={"source": "codex", "session_id": f"{canaries.tool_argument}-{index}"},
            redaction=EventRedactionSummaryV1(
                policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
            ),
            idempotency_key=f"key-{index}-{canaries.content}",
            content_claims=(claim,) if index == 0 else (),
        )
        for index in range(count)
    )
    return IngestBatchRequestV1(batch_id=new_uuid7(), events=events, content_items=(item,))


@pytest.fixture
def harness(canaries: Canaries) -> Harness:
    return Harness(canaries)


async def test_an_accepted_batch_is_traced_and_counted_without_content(
    harness: Harness,
    telemetry: Capture,
    canaries: Canaries,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    batch = _batch(canaries)

    response = await harness.post(
        batch.model_dump_json(), batch_id=batch.batch_id, traceparent=TRACEPARENT
    )

    assert response.status_code == 200
    assert telemetry.value(contract.INGESTION_BATCHES, outcome="accepted", reason="none") == 1
    assert telemetry.value(contract.INGESTION_DURATION, outcome="accepted") == 1
    assert telemetry.value(contract.INGESTION_EVENTS, result="created") == 1
    assert telemetry.value(contract.INGESTION_EVENTS, result="existing") == 1

    server = telemetry.span("POST /v1/ingestion/batches")
    ingest = telemetry.span("agent_context.ingest_batch")
    assert server.kind is SpanKind.SERVER
    assert dict(server.attributes or {}) == {
        "http.request.method": "POST",
        "http.route": "/v1/ingestion/batches",
        "http.response.status_code": 200,
    }
    # The caller's W3C trace context is the parent of the whole request.
    assert format(server.context.trace_id, "032x") == TRACEPARENT.split("-")[1]
    assert ingest.parent is not None and ingest.parent.span_id == server.context.span_id
    assert dict(ingest.attributes or {}) == {
        "agent_context.batch_id": str(batch.batch_id),
        "agent_context.event_count": 2,
        "agent_context.outcome": "accepted",
        "agent_context.reason": "none",
    }
    assert telemetry.value(
        contract.HTTP_SERVER_DURATION,
        **{
            "http.request.method": "POST",
            "http.route": "/v1/ingestion/batches",
            "http.response.status_code": 200,
        },
    )
    canaries.assert_absent(telemetry.dump(), log_text(caplog))


@pytest.mark.parametrize(
    ("scenario", "status", "outcome", "reason"),
    [
        ("bad_token", 401, "rejected", "auth"),
        ("no_idempotency_key", 400, "rejected", "schema"),
        ("bad_schema", 422, "rejected", "schema"),
        ("key_mismatch", 422, "rejected", "schema"),
        ("producer_mismatch", 403, "rejected", "producer_mismatch"),
        ("too_large", 413, "rejected", "too_large"),
        ("redaction_required", 422, "rejected", "redaction_required"),
        ("conflict", 409, "rejected", "conflict"),
        ("outage", 503, "error", "internal"),
        ("crash", 500, "error", "internal"),
    ],
)
async def test_every_rejection_has_a_bounded_reason(
    harness: Harness,
    telemetry: Capture,
    canaries: Canaries,
    caplog: pytest.LogCaptureFixture,
    scenario: str,
    status: int,
    outcome: str,
    reason: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    batch = _batch(canaries)
    body: bytes | str = batch.model_dump_json()
    batch_id: Any = batch.batch_id
    token = None
    rejected = tuple(
        RejectedEventV1(event_id=event.event_id, error_code=code, retryable=False)
        for event, code in zip(
            batch.events, ("content_requires_redaction", "batch_rejected"), strict=True
        )
    )
    if scenario == "bad_token":
        token = f"{PREFIX}.{canaries.prompt}-wrong-secret-value"
    elif scenario == "no_idempotency_key":
        batch_id = None
    elif scenario == "bad_schema":
        body = f'{{"batch_id": "{canaries.content}"}}'
    elif scenario == "key_mismatch":
        batch_id = new_uuid7()
    elif scenario == "producer_mismatch":
        batch = _batch(canaries, producer_id=f"other-{canaries.content}")
        body, batch_id = batch.model_dump_json(), batch.batch_id
    elif scenario == "too_large":
        body = "x" * 200_001
    elif scenario == "redaction_required":
        harness.service.outcome = IngestOutcome(
            422, IngestBatchResponseV1(batch_id=batch.batch_id, rejected=rejected)
        )
    elif scenario == "conflict":
        conflict = tuple(
            item.model_copy(update={"error_code": "idempotency_conflict"}) for item in rejected
        )
        harness.service.outcome = IngestOutcome(
            409, IngestBatchResponseV1(batch_id=batch.batch_id, rejected=conflict)
        )
    elif scenario == "outage":
        harness.service.outcome = OperationalError(
            f"SELECT '{canaries.content}'", {}, Exception(canaries.dsn_password)
        )
    else:
        harness.service.outcome = RuntimeError(f"boom {canaries.content}")

    response = await harness.post(body, batch_id=batch_id, token=token)

    assert response.status_code == status
    assert telemetry.points(contract.INGESTION_BATCHES) == {
        (("outcome", outcome), ("reason", reason)): 1
    }
    ingest = telemetry.span("agent_context.ingest_batch")
    assert (ingest.attributes or {})["agent_context.reason"] == reason
    if outcome == "error":
        assert ingest.status.status_code is StatusCode.ERROR
    canaries.assert_absent(telemetry.dump(), log_text(caplog))


async def test_unconfigured_ingestion_is_an_internal_error(
    telemetry: Capture, canaries: Canaries
) -> None:
    app = create_app(Settings(environment="test"))
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        response = await client.post("/v1/ingestion/batches", content=b"{}")
    assert response.status_code == 503
    assert telemetry.value(contract.INGESTION_BATCHES, outcome="error", reason="internal") == 1


async def test_routes_are_templates_never_raw_paths_or_queries(
    harness: Harness, telemetry: Capture, canaries: Canaries
) -> None:
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=harness.app), base_url="http://127.0.0.1:8000"
    ) as client:
        assert (await client.get(f"/health/live?token={canaries.token_secret}")).status_code == 200
        assert (await client.get(canaries.path)).status_code == 404
        assert (await client.get(f"/v1/ingestion/{canaries.content}")).status_code == 404
        assert (await client.request("BREW", "/health/live")).status_code == 405
        assert (await client.get("/mcp")).status_code == 401

    servers = [span for span in telemetry.spans() if span.kind is SpanKind.SERVER]
    routes = {(span.attributes or {}).get("http.route") for span in servers}
    assert routes == {"/health/live", "other", "/mcp"}
    names = sorted(span.name for span in servers)
    assert names == [
        "GET /health/live",
        "GET /mcp",
        "GET other",
        "GET other",
        "_OTHER /health/live",
    ]
    canaries.assert_absent(telemetry.dump())


async def test_an_application_exception_still_ends_the_server_span(
    telemetry: Capture, canaries: Canaries
) -> None:
    app = create_app(Settings(environment="test"))

    @app.get("/v1/explode")
    def explode() -> None:
        raise RuntimeError(canaries.content)

    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://127.0.0.1:8000",
    ) as client:
        assert (await client.get("/v1/explode")).status_code == 500

    server = telemetry.span("GET /v1/explode")
    assert server.status.status_code is StatusCode.ERROR
    assert (server.attributes or {})["error.type"] == "RuntimeError"
    assert (server.attributes or {})["http.response.status_code"] == 500
    canaries.assert_absent(telemetry.dump())
