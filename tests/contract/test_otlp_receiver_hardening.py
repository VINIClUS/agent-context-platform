"""PLATFORM-053 review round 2: bounded work, the loop marker at every level, bounded times,
conflict isolation and the checks the first suite could not tell apart from mutants."""

from __future__ import annotations

import gzip
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from agent_context_sdk import (
    AcceptedEventV1,
    IngestBatchRequestV1,
    IngestBatchResponseV1,
    RejectedEventV1,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import InstrumentationScope
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans

from agent_context_platform.ledger import otlp_api
from agent_context_platform.ledger.service import IngestOutcome

from .test_otlp_receiver import (
    EVENT_TIME_NS,
    Harness,
    _codex_event,
    _export,
    _fixture,
    _pairs,
    _response,
    _span,
    _status,
)

pytestmark = [pytest.mark.contract, pytest.mark.anyio]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _session(number: int) -> str:
    return f"0198a4b1-98c0-7c28-ae3f-{number:012d}"


# --- B1: bounded work ----------------------------------------------------------------------------


async def test_a_flood_of_span_events_is_refused_before_any_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def never(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no record may be mapped")

    monkeypatch.setattr(otlp_api.codex_otel, "map_record", never)
    # The production 8 MiB inflation cap: the structural check must trip, not the size limit.
    harness = Harness(max_inflated=8_388_608, max_body=4_194_304)
    span = _span()
    minimal = {
        "event.name": "codex.conversation_starts",
        "conversation.id": _session(1),
        "event.timestamp": "2026-08-13T13:00:00.124Z",
    }
    template = span.events.add(time_unix_nano=EVENT_TIME_NS, attributes=_pairs(minimal))
    for _ in range(40_000 - 1):
        span.events.add().CopyFrom(template)
    raw = _export(span).SerializeToString()
    assert 4_000_000 < len(raw) < 8_000_000
    body = gzip.compress(raw)
    assert len(body) < 1_000_000

    started = time.perf_counter()
    response = await harness.post(body, **{"content-encoding": "gzip"})
    elapsed = time.perf_counter() - started

    assert response.status_code == 413
    assert _status(response).message.startswith("too_many_events:")
    assert harness.service.batches == []
    # Inflating and parsing still cost something; the per-record work never happens.
    assert elapsed < 3.0


@pytest.mark.parametrize(
    ("options", "export", "code"),
    [
        (
            {"max_records": 5},
            lambda: _export(_span(events=[{"x": 1}] * 3), _span(events=[{"x": 1}] * 3)),
            "too_many_records",
        ),
        (
            {"max_events_per_span": 2},
            lambda: _export(_span(events=[{"x": 1}] * 3)),
            "too_many_events",
        ),
        (
            {"max_attributes": 4},
            lambda: _export(_span({f"a{i}": i for i in range(5)})),
            "too_many_attributes",
        ),
        (
            {"max_attributes": 4},
            lambda: _export(_span(events=[{f"a{i}": i for i in range(5)}])),
            "too_many_attributes",
        ),
        (
            {"max_attributes": 4},
            lambda: _export(_span({"x": 1}), resource={f"r{i}": i for i in range(5)}),
            "too_many_attributes",
        ),
    ],
)
async def test_each_structural_limit_has_its_own_fixed_413(
    options: dict[str, Any], export: Any, code: str
) -> None:
    harness = Harness(**options)
    response = await harness.post(export().SerializeToString())
    assert response.status_code == 413
    assert _status(response).message.startswith(f"{code}:")


async def test_scope_attribute_count_is_bounded() -> None:
    harness = Harness(max_attributes=2)
    export = ExportTraceServiceRequest(
        resource_spans=[
            ResourceSpans(
                resource=Resource(attributes=_pairs(_fixture()["resource"])),
                scope_spans=[
                    ScopeSpans(
                        scope=InstrumentationScope(attributes=_pairs({"a": 1, "b": 2, "c": 3})),
                        spans=[_span({"x": 1})],
                    )
                ],
            )
        ]
    )
    response = await harness.post(export.SerializeToString())
    assert response.status_code == 413
    assert _status(response).message.startswith("too_many_attributes:")


async def test_more_than_500_distinct_events_cannot_form_one_batch() -> None:
    harness = Harness(max_spans=500)
    events = [_codex_event(**{"conversation.id": _session(i)}) for i in range(501)]
    spans = [_span(events=events[i : i + 50]) for i in range(0, 501, 50)]

    response = await harness.post(_export(*spans).SerializeToString())

    assert response.status_code == 413
    assert _status(response).message.startswith("too_many_drafts:")
    assert harness.service.batches == []


async def test_over_long_values_reject_their_record_only() -> None:
    harness = Harness()
    export = _export(
        _span(events=[_codex_event(model="m" * (otlp_api.MAX_ATTRIBUTE_VALUE_CHARS + 1))]),
        _span(events=[_codex_event(**{"conversation.id": _session(7)})]),
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 1
    assert len(harness.service.batches[0].events) == 1


# --- S3: the loop-prevention marker at every level -------------------------------------------------


@pytest.mark.parametrize("level", ["scope", "span", "event", "event_only_marker"])
async def test_the_platform_marker_rejects_at_every_level(level: str) -> None:
    harness = Harness()
    marker = {"agent_context.origin": "platform"}
    scope = InstrumentationScope()
    if level == "scope":
        scope = InstrumentationScope(attributes=_pairs(marker))
        span = _span(events=[_codex_event()])
    elif level == "span":
        span = _span({**_codex_event(), **marker})
    elif level == "event":
        span = _span(events=[{**_codex_event(), **marker}])
    else:
        span = _span(events=[{"x": 1, **marker}])
    export = ExportTraceServiceRequest(
        resource_spans=[
            ResourceSpans(
                resource=Resource(attributes=_pairs(_fixture()["resource"])),
                scope_spans=[ScopeSpans(scope=scope, spans=[span])],
            )
        ]
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 1
    assert response.partial_success.error_message == "spans rejected: platform_origin"
    assert harness.service.batches == []


# --- S4: bounded, deterministic times --------------------------------------------------------------


async def test_records_from_the_future_are_rejected() -> None:
    harness = Harness()
    future = int((datetime.now(UTC) + timedelta(days=30)).timestamp() * 1e9)
    far_timestamp = (datetime.now(UTC) + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    export = _export(
        _span(events=[_codex_event()], event_time=future),
        _span(
            events=[
                _codex_event(**{"conversation.id": _session(2), "event.timestamp": far_timestamp})
            ]
        ),
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 2
    assert response.partial_success.error_message == "spans rejected: future_timestamp"


async def test_observed_at_never_precedes_occurred_at() -> None:
    harness = Harness()
    early = int(datetime(1971, 1, 1, tzinfo=UTC).timestamp() * 1e9)

    _response(
        await harness.post(
            _export(_span(events=[_codex_event()], event_time=early)).SerializeToString()
        )
    )

    (draft,) = harness.service.batches[0].events
    assert draft.observed_at == draft.occurred_at


# --- S1: one conflicting record never drops the rest ----------------------------------------------


class ConflictOnce:
    """Refuses the first batch: one offender, the rest ``batch_rejected``, one ``existing``."""

    def __init__(self, offender_session: str, existing_session: str) -> None:
        self.batches: list[IngestBatchRequestV1] = []
        self._offender = offender_session
        self._existing = existing_session
        self.error: Exception | None = None
        self.status = 200

    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        self.batches.append(batch)
        accepted: list[AcceptedEventV1] = []
        rejected: list[RejectedEventV1] = []
        first = len(self.batches) == 1
        for event in batch.events:
            session = event.payload["session_id"]
            if first and session == self._existing:
                accepted.append(
                    AcceptedEventV1(event_id=event.event_id, status="existing", stream_sequence=1)
                )
            elif first:
                code = "idempotency_conflict" if session == self._offender else "batch_rejected"
                rejected.append(
                    RejectedEventV1(event_id=event.event_id, error_code=code, retryable=False)
                )
            else:
                accepted.append(
                    AcceptedEventV1(event_id=event.event_id, status="accepted", stream_sequence=1)
                )
        status = 409 if rejected else 200
        return IngestOutcome(
            status,
            IngestBatchResponseV1(
                batch_id=batch.batch_id, accepted=tuple(accepted), rejected=tuple(rejected)
            ),
        )


async def test_a_conflict_rejects_only_its_offender_and_the_rest_is_retried_once() -> None:
    harness = Harness()
    service = ConflictOnce(offender_session=_session(1), existing_session=_session(2))
    object.__setattr__(harness.runtime, "service", service)
    export = _export(
        *(_span(events=[_codex_event(**{"conversation.id": _session(i)})]) for i in (1, 2, 3))
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 1  # the offender only; `existing` is fine
    assert response.partial_success.error_message == "spans rejected: idempotency_conflict"
    first, retry = service.batches
    assert len(first.events) == 3
    assert [event.payload["session_id"] for event in retry.events] == [_session(3)]


# --- S5: checks the first suite could not distinguish ----------------------------------------------


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        (b"\xff\xff not protobuf", {}),
        (gzip.compress(b"\x00" * 4_000_000), {"content-encoding": "gzip"}),
        (b"{}", {"content-type": "application/json"}),
    ],
)
async def test_nothing_about_the_body_is_considered_before_authentication(
    body: bytes, headers: dict[str, str]
) -> None:
    harness = Harness(max_inflated=1_000)
    response = await harness.post(body, token=None, **headers)
    assert response.status_code == 401


async def test_the_route_keys_idempotency_with_the_runtime_key() -> None:
    body = _export(_span(events=[_codex_event()])).SerializeToString()
    keys = []
    for key in (b"a" * 16 + b"b" * 16, b"c" * 16 + b"d" * 16):
        harness = Harness()
        object.__setattr__(harness.runtime, "hmac_key", key)
        _response(await harness.post(body))
        keys.append(harness.service.batches[0].events[0].idempotency_key)
    assert keys[0] != keys[1]


async def test_resource_attributes_never_leak_into_the_record() -> None:
    harness = Harness()
    resource = {**_fixture()["resource"], "model": "resource-model"}
    _response(
        await harness.post(
            _export(_span(events=[_codex_event(model="")]), resource=resource).SerializeToString()
        )
    )
    assert harness.service.batches[0].events[0].payload["model"] is None
