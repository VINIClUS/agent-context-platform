"""Contract tests for ``POST /v1/otel/traces`` over the real ASGI app (PLATFORM-053).

Authentication, the rate limit, media type, size and inflation bounds, protobuf parsing and the
mapper run for real; only the ingestion service is a fake (the database path is covered by
``tests/integration/ledger/test_otlp_receiver.py``). Canary attribute values must never appear
in a response, a log record or exported telemetry.
"""

from __future__ import annotations

import gzip
import json
import logging
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx2
import pytest
from agent_context_sdk import (
    AcceptedEventV1,
    IngestBatchRequestV1,
    IngestBatchResponseV1,
    RejectedEventV1,
)
from argon2 import PasswordHasher
from google.rpc.status_pb2 import Status
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, ArrayValue, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span
from sqlalchemy.exc import OperationalError

from agent_context_platform.app import create_app
from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    ProducerAuthenticator,
    ProducerRegistration,
)
from agent_context_platform.ledger.otlp_api import (
    PRODUCER_NAME,
    STREAM_PREFIX,
    TRACES_PATH,
    OtlpReceiverRuntime,
    stream_id,
)
from agent_context_platform.ledger.service import IngestOutcome
from agent_context_platform.mcp.auth import TokenBucketLimiter
from agent_context_platform.settings import Settings

pytestmark = [pytest.mark.contract, pytest.mark.anyio]

FIXTURES = Path(__file__).parents[1] / "fixtures" / "codex_otel"
NOW = datetime(2026, 9, 1, tzinfo=UTC)
PREFIX = "otel0001"
PRODUCER_ID = "codex-otel-producer"
PROTOBUF = {"content-type": "application/x-protobuf"}
EVENT_TIME_NS = 1_786_626_000_200_000_000  # after the fixture's event.timestamp
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _canary(label: str) -> str:
    return f"CANARY{label}{secrets.token_hex(8)}"


def _value(value: Any) -> AnyValue:
    if isinstance(value, bool):
        return AnyValue(bool_value=value)
    if isinstance(value, int):
        return AnyValue(int_value=value)
    if isinstance(value, float):
        return AnyValue(double_value=value)
    if isinstance(value, list):
        return AnyValue(array_value=ArrayValue(values=[_value(item) for item in value]))
    return AnyValue(string_value=str(value))


def _pairs(attributes: dict[str, Any]) -> list[KeyValue]:
    return [KeyValue(key=key, value=_value(value)) for key, value in attributes.items()]


def _fixture(name: str = "conversation_starts.trace.json") -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())


def _span(
    attributes: dict[str, Any] | None = None,
    *,
    events: list[dict[str, Any]] = (),  # type: ignore[assignment]
    event_time: int = EVENT_TIME_NS,
    name: str = "session_task",
) -> Span:
    span = Span(
        trace_id=bytes.fromhex("4bf92f3577b34da6a3ce929d0e0e4736"),
        span_id=bytes.fromhex("00f067aa0ba902b7"),
        name=name,
        start_time_unix_nano=max(event_time - 1_000, 0),
        end_time_unix_nano=event_time,
        attributes=_pairs(attributes or {}),
    )
    for event in events:
        span.events.add(name="event", time_unix_nano=event_time, attributes=_pairs(event))
    return span


def _export(*spans: Span, resource: dict[str, Any] | None = None) -> ExportTraceServiceRequest:
    return ExportTraceServiceRequest(
        resource_spans=[
            ResourceSpans(
                resource=Resource(
                    attributes=_pairs(_fixture()["resource"] if resource is None else resource)
                ),
                scope_spans=[ScopeSpans(spans=list(spans))],
            )
        ]
    )


class ScriptedService:
    def __init__(self) -> None:
        self.batches: list[IngestBatchRequestV1] = []
        self.status = 200
        self.error: Exception | None = None

    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        self.batches.append(batch)
        if self.error is not None:
            raise self.error
        if self.status == 200:
            accepted = tuple(
                AcceptedEventV1(event_id=event.event_id, status="accepted", stream_sequence=i + 1)
                for i, event in enumerate(batch.events)
            )
            return IngestOutcome(
                200, IngestBatchResponseV1(batch_id=batch.batch_id, accepted=accepted)
            )
        code = "idempotency_conflict" if self.status == 409 else "service_unavailable"
        rejected = tuple(
            RejectedEventV1(event_id=event.event_id, error_code=code, retryable=self.status == 503)
            for event in batch.events
        )
        return IngestOutcome(
            self.status, IngestBatchResponseV1(batch_id=batch.batch_id, rejected=rejected)
        )


class Harness:
    def __init__(
        self,
        *,
        scope: str = "events:ingest",
        revoked: bool = False,
        burst: int = 100,
        max_body: int = 1_000_000,
        max_inflated: int = 1_000_000,
        max_spans: int = 10,
        **limits: Any,
    ) -> None:
        # A fresh secret per harness (the token shape is `<prefix>.<43 url-safe chars>`).
        self.token = f"{PREFIX}.{secrets.token_urlsafe(32)}"
        registration = ProducerRegistration(
            producer_id=PRODUCER_ID,
            token_verifier=_HASHER.hash(self.token),
            scope=scope,
            expires_at=NOW + timedelta(days=1),
            revoked_at=NOW - timedelta(minutes=1) if revoked else None,
        )

        async def lookup(prefix: str) -> ProducerRegistration | None:
            return registration if prefix == PREFIX else None

        self.service = ScriptedService()
        self.runtime = OtlpReceiverRuntime(
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
            limiter=TokenBucketLimiter(rate_per_second=0.001, burst=burst),
            hmac_key=b"h" * 32,
            max_request_body_bytes=max_body,
            max_decompressed_bytes=max_inflated,
            max_spans=max_spans,
            **limits,
        )
        self.app = create_app(Settings(environment="test"), otlp_receiver=self.runtime)

    async def post(
        self, body: bytes, *, token: str | None = "default", **headers: str
    ) -> httpx2.Response:
        request_headers = {**PROTOBUF, **headers}
        if token is not None:
            request_headers["authorization"] = (
                f"Bearer {self.token if token == 'default' else token}"
            )
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=self.app), base_url="http://127.0.0.1:8000"
        ) as client:
            return await client.post(TRACES_PATH, content=body, headers=request_headers)


def _response(response: httpx2.Response) -> ExportTraceServiceResponse:
    assert response.status_code == 200, response.status_code
    assert response.headers["content-type"] == "application/x-protobuf"
    return ExportTraceServiceResponse.FromString(response.content)


def _status(response: httpx2.Response) -> Status:
    return Status.FromString(response.content)


def _codex_event(**overrides: Any) -> dict[str, Any]:
    return {**_fixture()["attributes"], **overrides}


async def test_a_codex_event_becomes_one_canonical_draft_in_one_batch() -> None:
    harness = Harness()
    export = _export(
        _span(events=[_codex_event()]),  # trace_event! inside a span: a span event
        _span({"event.name": "codex.api_request", "duration_ms": 3}),  # documented gap
        _span({"thread": "main"}),  # an ordinary span: no Codex event at all
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert not response.HasField("partial_success")
    (batch,) = harness.service.batches
    (draft,) = batch.events
    assert draft.event_type == "agent.session.started"
    assert draft.producer.producer_id == PRODUCER_ID
    assert draft.producer.name == PRODUCER_NAME
    session_id = _fixture()["attributes"]["conversation.id"]
    # One stream per producer and Codex session.
    assert draft.stream_id == stream_id(PRODUCER_ID, session_id)
    assert draft.stream_id.startswith(STREAM_PREFIX)
    assert draft.payload["session_id"] == _fixture()["attributes"]["conversation.id"]
    assert draft.trace is not None and draft.trace.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    # The record's own OTLP time, not the arrival time: a retry maps to the identical draft.
    assert draft.observed_at == datetime.fromtimestamp(EVENT_TIME_NS / 1e9, tz=UTC)


async def test_a_retried_export_maps_to_identical_drafts() -> None:
    harness = Harness()
    body = _export(_span(events=[_codex_event()])).SerializeToString()

    _response(await harness.post(body))
    _response(await harness.post(body))

    first, second = (batch.events[0] for batch in harness.service.batches)
    assert first.model_dump(exclude={"event_id"}) == second.model_dump(exclude={"event_id"})


async def test_span_attributes_also_carry_a_codex_event_and_duplicates_collapse() -> None:
    harness = Harness()
    export = _export(
        _span(_codex_event()), _span(events=[_codex_event()], event_time=EVENT_TIME_NS)
    )

    _response(await harness.post(export.SerializeToString()))

    (batch,) = harness.service.batches
    assert len(batch.events) == 1


@pytest.mark.parametrize(
    ("resource", "reason"),
    [
        (
            {"service.name": "agent-context-platform", "agent_context.origin": "platform"},
            "platform_origin",
        ),
        ({"service.name": "codex_cli_rs", "agent_context.origin": "platform"}, "platform_origin"),
        ({"service.name": "otelcol"}, "unknown_service"),
        ({}, "missing_service"),
    ],
)
async def test_a_refused_resource_rejects_all_its_spans_without_ingesting(
    resource: dict[str, Any], reason: str
) -> None:
    harness = Harness()
    export = _export(_span(events=[_codex_event()]), _span({"x": 1}), resource=resource)

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 2
    assert response.partial_success.error_message == f"spans rejected: {reason}"
    assert harness.service.batches == []


async def test_refused_records_are_rejected_spans_and_the_rest_is_ingested() -> None:
    harness = Harness()
    export = _export(
        _span(events=[_codex_event()]),
        _span(events=[_codex_event(**{"conversation.id": "bad id with spaces"})]),
        _span(events=[_codex_event(model=["not", "scalar"])]),
        _span(events=[_codex_event()], event_time=0),  # no usable OTLP time
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 3
    assert response.partial_success.error_message == (
        "spans rejected: invalid_attribute,missing_timestamp,non_scalar_attribute"
    )
    (batch,) = harness.service.batches
    assert len(batch.events) == 1


@pytest.mark.parametrize(("status", "expected"), [(409, 200), (503, 503)])
async def test_an_ingestion_refusal_is_partial_and_an_outage_is_retryable(
    status: int, expected: int
) -> None:
    harness = Harness()
    harness.service.status = status

    response = await harness.post(_export(_span(events=[_codex_event()])).SerializeToString())

    assert response.status_code == expected
    if expected == 200:
        partial = _response(response).partial_success
        assert (partial.rejected_spans, partial.error_message) == (
            1,
            "spans rejected: idempotency_conflict",
        )
    else:
        assert response.headers["retry-after"] == "5"
        assert _status(response).message == "The service is temporarily unavailable."


@pytest.mark.parametrize(
    ("harness_options", "token", "status", "message"),
    [
        ({}, None, 401, "A bearer credential is required."),
        ({}, f"{PREFIX}.{'w' * 43}", 401, "The credential is not valid."),
        ({}, f"unknown1.{'w' * 43}", 401, "The credential is not valid."),
        ({"revoked": True}, "default", 401, "The credential is not valid."),
        ({"scope": "memory:read"}, "default", 403, "The credential may not ingest events."),
    ],
)
async def test_only_events_ingest_producers_may_export(
    harness_options: dict[str, Any], token: str | None, status: int, message: str
) -> None:
    harness = Harness(**harness_options)

    response = await harness.post(
        _export(_span(events=[_codex_event()])).SerializeToString(), token=token
    )

    assert response.status_code == status
    assert _status(response).message == message
    if status == 401:
        assert response.headers["www-authenticate"] == "Bearer"
    assert harness.service.batches == []


async def test_each_producer_is_rate_limited() -> None:
    harness = Harness(burst=1)
    body = _export(_span({"x": 1})).SerializeToString()

    assert (await harness.post(body)).status_code == 200
    limited = await harness.post(body)

    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "5"


@pytest.mark.parametrize(
    "headers",
    [
        {"content-type": "application/json"},
        {"content-type": "text/plain"},
        {"content-encoding": "br"},
        {"content-encoding": "deflate"},
    ],
)
async def test_only_protobuf_identity_or_gzip_is_accepted(headers: dict[str, str]) -> None:
    harness = Harness()
    response = await harness.post(_export(_span({"x": 1})).SerializeToString(), **headers)
    assert response.status_code == 415


async def test_gzip_bodies_are_inflated() -> None:
    harness = Harness()
    body = gzip.compress(_export(_span(events=[_codex_event()])).SerializeToString())

    _response(await harness.post(body, **{"content-encoding": "gzip"}))

    assert len(harness.service.batches) == 1


@pytest.mark.parametrize(
    ("body", "status"),
    [
        (gzip.compress(b"\x00" * 8_000_000), 413),  # a bomb: ~8 KiB inflating to 8 MB
        (gzip.compress(b"x" * 1000)[:-12], 400),  # truncated member
        (gzip.compress(b"") + b"trailing", 400),  # data after the member
        (b"not gzip at all", 400),
    ],
)
async def test_gzip_bombs_and_broken_members_are_refused(body: bytes, status: int) -> None:
    harness = Harness(max_inflated=1_000_000)
    assert len(body) < 1_000_000

    response = await harness.post(body, **{"content-encoding": "gzip"})

    assert response.status_code == status
    assert harness.service.batches == []


async def test_bodies_over_the_limit_are_refused_while_streaming() -> None:
    harness = Harness(max_body=1_000)
    big = _export(*(_span({"x": "y" * 200}) for _ in range(8))).SerializeToString()
    assert len(big) > 1_000

    assert (await harness.post(big)).status_code == 413
    declared = await harness.post(b"", **{"content-length": "5000"})
    assert declared.status_code == 413


async def test_too_many_spans_are_refused() -> None:
    harness = Harness(max_spans=2)
    body = _export(*(_span({"x": 1}) for _ in range(3))).SerializeToString()

    response = await harness.post(body)

    assert response.status_code == 413
    assert _status(response).message == "too_many_spans: span limit exceeded."


@pytest.mark.parametrize("body", [b"\xff\xff\xff\xff", b"\x0a\x05abc"])
async def test_malformed_protobuf_is_a_fixed_400(body: bytes) -> None:
    harness = Harness()
    response = await harness.post(body)
    assert response.status_code == 400
    assert _status(response).message == "The request is not a valid OTLP export."


async def test_an_unconfigured_receiver_answers_503() -> None:
    app = create_app(Settings(environment="test"))
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
    ) as client:
        response = await client.post(TRACES_PATH, content=b"", headers=PROTOBUF)
    assert response.status_code == 503
    assert response.headers["retry-after"] == "5"


async def test_attribute_values_are_never_echoed_logged_or_exported(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from unit.observability._capture import capture_telemetry

    from agent_context_platform.observability import contract

    caplog.set_level(logging.DEBUG)
    canaries = [_canary(label) for label in ("error", "prompt", "args", "name", "resource")]
    harness = Harness()
    export = _export(
        _span(
            events=[
                _codex_event(
                    **{
                        "error.message": canaries[0],
                        "prompt": canaries[1],
                        "arguments": canaries[2],
                    }
                ),
                _codex_event(**{"conversation.id": canaries[3] + " !"}),
            ]
        ),
        resource={**_fixture()["resource"], "host.name": canaries[4]},
    )
    with capture_telemetry() as telemetry:
        ok = await harness.post(export.SerializeToString())
        refused = await harness.post(
            _export(_span({"x": 1}), resource={"service.name": canaries[4]}).SerializeToString()
        )
        bad = await harness.post(canaries[0].encode())
        dump = telemetry.dump()
        assert telemetry.value(contract.OTEL_REQUESTS, outcome="accepted", reason="none") == 2
        assert telemetry.value(contract.OTEL_REQUESTS, outcome="rejected", reason="malformed") == 1
        assert telemetry.value(contract.OTEL_SPANS, result="rejected") == 2

    stored = harness.service.batches[0].model_dump_json()
    texts = [
        ok.content.decode("latin-1"),
        refused.content.decode("latin-1"),
        bad.content.decode("latin-1"),
        dump,
        caplog.text,
    ]
    texts += [json.dumps(record.__dict__, default=str) for record in caplog.records]
    for canary in canaries:
        assert canary not in stored
        for text in texts:
            assert canary not in text
    assert harness.token not in caplog.text


KEY = "Zq8r1Lx0VmPa4Tb7Nc2Wd5Ye9Kf3Hg6Uj"


def test_receiver_settings_require_a_long_stable_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError

    from agent_context_platform.settings import OtelReceiverSettings

    for weak in ("short-secret-value", "k" * 40, "ab" * 20):
        with pytest.raises(ValidationError) as info:
            OtelReceiverSettings(idempotency_hmac_key=weak)
        assert weak not in str(info.value)
    monkeypatch.setenv("AGENT_CONTEXT_OTEL_RECEIVER__IDEMPOTENCY_HMAC_KEY", KEY)
    monkeypatch.setenv("AGENT_CONTEXT_OTEL_RECEIVER__MAX_SPANS", "100")
    receiver = Settings().otel_receiver
    assert receiver.max_spans == 100
    assert KEY not in repr(receiver)


def test_the_app_builds_the_receiver_only_with_ingestion_and_a_key() -> None:
    from types import SimpleNamespace

    from agent_context_platform.app import _build_otlp_receiver
    from agent_context_platform.settings import OtelReceiverSettings

    ingestion = SimpleNamespace(authenticator=object(), service=object())
    keyed = Settings(
        environment="test", otel_receiver=OtelReceiverSettings(idempotency_hmac_key=KEY)
    )
    assert _build_otlp_receiver(Settings(environment="test"), ingestion) is None  # type: ignore[arg-type]
    assert _build_otlp_receiver(keyed, None) is None
    runtime = _build_otlp_receiver(keyed, ingestion)  # type: ignore[arg-type]
    assert runtime is not None
    assert runtime.hmac_key == KEY.encode()
    assert runtime.authenticator is ingestion.authenticator


@pytest.mark.parametrize(
    ("error", "status", "message"),
    [
        (
            OperationalError("SELECT secret", {}, Exception("dsn")),
            503,
            "The service is temporarily unavailable.",
        ),
        (RuntimeError("boom with detail"), 500, "The request could not be processed."),
    ],
)
async def test_failures_answer_fixed_statuses(
    error: Exception, status: int, message: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    harness = Harness()
    harness.service.error = error

    response = await harness.post(_export(_span(events=[_codex_event()])).SerializeToString())

    assert response.status_code == status
    assert _status(response).message == message
    assert "boom with detail" not in caplog.text
    assert "SELECT secret" not in caplog.text
