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

from agent_context_platform.ledger import otlp_api, otlp_wire
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
    # One span with 40k events: a request-level record flood, refused on the wire bytes.
    assert _status(response).message.startswith("too_many_records:")
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
            {"max_attributes": 4},
            lambda: _export(_span({"x": 1}), resource={f"r{i}": i for i in range(5)}),
            "too_many_attributes",
        ),
        (
            {"max_resources": 2},
            lambda: ExportTraceServiceRequest(
                resource_spans=[ResourceSpans(resource=Resource()) for _ in range(3)]
            ),
            "too_many_resources",
        ),
        (
            {"max_scopes": 2},
            lambda: ExportTraceServiceRequest(
                resource_spans=[ResourceSpans(scope_spans=[ScopeSpans() for _ in range(3)])]
            ),
            "too_many_scopes",
        ),
        (
            {"max_links": 2},
            lambda: _export(_linked_span(3)),
            "too_many_links",
        ),
        (
            {"max_total_attributes": 10},
            lambda: _export(*(_span({f"a{i}": i for i in range(4)}) for _ in range(3))),
            "too_many_attributes",
        ),
        (
            {"max_total_attributes": 10},
            lambda: _export(_span({"list": list(range(20))})),  # nested values count too
            "too_many_attributes",
        ),
        (
            {},
            lambda: _export(_span({"deep": _nested(otlp_wire.MAX_VALUE_DEPTH + 2)})),
            "too_many_attributes",
        ),
    ],
)
async def test_each_request_level_limit_has_its_own_fixed_413(
    options: dict[str, Any], export: Any, code: str
) -> None:
    harness = Harness(**options)
    response = await harness.post(export().SerializeToString())
    assert response.status_code == 413
    assert _status(response).message.startswith(f"{code}:")
    assert harness.service.batches == []


def _linked_span(links: int) -> Any:
    span = _span({"x": 1})
    for _ in range(links):
        span.links.add(trace_id=b"\x01" * 16, span_id=b"\x02" * 8)
    return span


def _nested(depth: int) -> Any:
    value: Any = 1
    for _ in range(depth):
        value = [value]
    return value


@pytest.mark.parametrize(
    ("options", "span", "reason"),
    [
        # max_attributes 30: above the 22 attributes of the other span's Codex event.
        ({"max_events_per_span": 2}, lambda: _span(events=[{"x": 1}] * 3), "too_many_events"),
        (
            {"max_attributes": 30},
            lambda: _span({f"a{i}": i for i in range(31)}),
            "too_many_attributes",
        ),
        (
            {"max_attributes": 30},
            lambda: _span(events=[{f"a{i}": i for i in range(31)}]),
            "too_many_attributes",
        ),
    ],
)
async def test_per_span_limits_reject_that_span_only(
    options: dict[str, Any], span: Any, reason: str
) -> None:
    harness = Harness(**options)
    export = _export(span(), _span(events=[_codex_event()]))

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 1
    assert response.partial_success.error_message == f"spans rejected: {reason}"
    assert len(harness.service.batches[0].events) == 1


async def test_malformed_wire_bytes_are_400_before_parsing() -> None:
    harness = Harness()
    for body in (b"\x0a\xff\xff\xff\xff\x0f", b"\x0b\x00", b"\x00\x00"):
        assert (await harness.post(body)).status_code == 400


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


# --- round 3: admission gate, multi-conflict loop, stream names, remaining mutants -----------------


class _Connected:
    async def is_disconnected(self) -> bool:
        return False


async def test_the_gate_runs_two_decodes_at_a_time_and_caps_its_queue() -> None:
    import asyncio
    import threading

    gate = otlp_api.DecodeGate(workers=2, queue_depth=4)
    running = peak = 0
    lock = threading.Lock()

    def work() -> str:
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
        time.sleep(0.2)
        with lock:
            running -= 1
        return "done"

    async def one(producer: str) -> Any:
        try:
            return await gate.run(producer, _Connected(), work)  # type: ignore[arg-type]
        except otlp_api._RequestError as error:
            return error.status

    started = time.perf_counter()
    tasks = [asyncio.create_task(one(f"producer-{i}")) for i in range(8)]
    await asyncio.sleep(0.05)
    loop_latency = time.perf_counter()
    await asyncio.sleep(0)
    loop_latency = time.perf_counter() - loop_latency
    results = await asyncio.gather(*tasks)

    assert peak == 2
    assert results.count("done") == 6
    assert results.count(503) == 2
    assert loop_latency < 0.05  # decodes ran on threads
    assert time.perf_counter() - started < 2.0
    assert gate.admitted == 0


async def test_one_producer_holds_at_most_one_running_and_one_waiting_place(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    import httpx2

    real = otlp_api.decode_and_map

    def slow(*args: Any) -> Any:
        time.sleep(0.3)
        return real(*args)

    monkeypatch.setattr(otlp_api, "decode_and_map", slow)
    harness = Harness(burst=100)
    body = _export(_span({"x": 1})).SerializeToString()

    exports = [asyncio.create_task(harness.post(body)) for _ in range(4)]
    await asyncio.sleep(0.1)
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=harness.app), base_url="http://127.0.0.1:8000"
    ) as client:
        started = time.perf_counter()
        assert (await client.get("/health/live")).status_code == 200
        health_seconds = time.perf_counter() - started
    statuses = sorted(response.status_code for response in await asyncio.gather(*exports))

    assert statuses == [200, 200, 503, 503]
    assert health_seconds < 0.25  # the event loop stayed responsive while decodes ran


async def test_places_are_given_back_so_sequential_requests_never_run_out() -> None:
    harness = Harness(burst=100)
    body = _export(_span({"x": 1})).SerializeToString()
    for _ in range(otlp_api.DECODE_WORKERS + otlp_api.DECODE_QUEUE_DEPTH + 4):
        assert (await harness.post(body)).status_code == 200
    assert harness.runtime.decode_gate.admitted == 0


async def test_a_cancelled_request_keeps_its_slot_until_the_thread_finishes() -> None:
    import asyncio
    import threading

    gate = otlp_api.DecodeGate(workers=1, queue_depth=0, per_producer=1)
    release = threading.Event()
    task = asyncio.create_task(gate.run("p", _Connected(), lambda: release.wait(5)))  # type: ignore[arg-type]
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The thread still runs: its place is still taken.
    assert gate.admitted == 1
    with pytest.raises(otlp_api._RequestError):
        await gate.run("q", _Connected(), lambda: None)  # type: ignore[arg-type]
    release.set()
    for _ in range(50):
        if gate.admitted == 0:
            break
        await asyncio.sleep(0.02)
    assert gate.admitted == 0
    assert await gate.run("q", _Connected(), lambda: "ok") == "ok"  # type: ignore[arg-type]


async def test_a_queued_request_whose_client_left_is_not_decoded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from starlette.requests import Request

    calls: list[int] = []

    def counted(*args: Any) -> Any:
        calls.append(1)
        raise AssertionError("must not decode")

    async def gone(_self: Request) -> bool:
        return True

    monkeypatch.setattr(otlp_api, "decode_and_map", counted)
    monkeypatch.setattr(Request, "is_disconnected", gone)
    harness = Harness()

    response = await harness.post(_export(_span({"x": 1})).SerializeToString())

    assert response.status_code == 499
    assert calls == []


class ConflictEach:
    """The ledger names one conflict per batch: each listed session conflicts in turn."""

    def __init__(self, conflicting: list[str]) -> None:
        self.batches: list[IngestBatchRequestV1] = []
        self._conflicting = list(conflicting)
        self.error: Exception | None = None
        self.status = 200

    async def ingest(self, batch: IngestBatchRequestV1) -> IngestOutcome:
        self.batches.append(batch)
        sessions = [event.payload["session_id"] for event in batch.events]
        offender = next((s for s in self._conflicting if s in sessions), None)
        if offender is None:
            accepted = tuple(
                AcceptedEventV1(event_id=e.event_id, status="accepted", stream_sequence=1)
                for e in batch.events
            )
            return IngestOutcome(
                200, IngestBatchResponseV1(batch_id=batch.batch_id, accepted=accepted)
            )
        rejected = tuple(
            RejectedEventV1(
                event_id=event.event_id,
                error_code="idempotency_conflict"
                if event.payload["session_id"] == offender
                else "batch_rejected",
                retryable=False,
            )
            for event in batch.events
        )
        return IngestOutcome(409, IngestBatchResponseV1(batch_id=batch.batch_id, rejected=rejected))


async def test_every_conflict_is_isolated_and_the_new_events_still_land() -> None:
    harness = Harness()
    service = ConflictEach([_session(1), _session(2)])
    object.__setattr__(harness.runtime, "service", service)
    export = _export(
        *(_span(events=[_codex_event(**{"conversation.id": _session(i)})]) for i in (1, 2, 3, 4))
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert response.partial_success.rejected_spans == 2
    assert response.partial_success.error_message == "spans rejected: idempotency_conflict"
    assert [len(batch.events) for batch in service.batches] == [4, 3, 2]
    assert {event.payload["session_id"] for event in service.batches[-1].events} == {
        _session(3),
        _session(4),
    }


async def test_conflicts_beyond_the_attempt_cap_reject_the_rest() -> None:
    harness = Harness(max_spans=20)
    sessions = [_session(i) for i in range(otlp_api.MAX_INGEST_ATTEMPTS + 2)]
    service = ConflictEach(sessions)
    object.__setattr__(harness.runtime, "service", service)
    export = _export(
        *(_span(events=[_codex_event(**{"conversation.id": session})]) for session in sessions)
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert len(service.batches) == otlp_api.MAX_INGEST_ATTEMPTS
    assert response.partial_success.rejected_spans == len(sessions)
    assert response.partial_success.error_message == (
        "spans rejected: batch_rejected,idempotency_conflict"
    )


async def test_stream_names_encode_their_components_unambiguously() -> None:
    harness = Harness()
    session = "x:y"
    export = _export(_span(events=[_codex_event(**{"conversation.id": session})]))

    _response(await harness.post(export.SerializeToString()))

    (draft,) = harness.service.batches[0].events
    assert draft.stream_id == otlp_api.stream_id("codex-otel-producer", session)
    # Length-prefixed parts: the pairs ("p", "x:y") and ("p:x", "y") never share a stream.
    assert otlp_api.stream_id("p", "x:y") != otlp_api.stream_id("p:x", "y")
    assert otlp_api.stream_id("p", "") != otlp_api.stream_id("p", None)
    assert otlp_api.stream_id("p", "s") != otlp_api.stream_id("q", "s")


@pytest.mark.parametrize(
    ("producer", "session"),
    [("p" * 255, "a:" * 64), ("é" * 255, "0198a4b1-98c0-7c28-ae3f-000000000001"), ("ü", None)],
)
def test_stream_ids_have_a_fixed_length_whatever_the_ids(
    producer: str, session: str | None
) -> None:
    value = otlp_api.stream_id(producer, session)
    assert len(value) == len(otlp_api.STREAM_PREFIX) + 40
    assert value.isascii()


async def test_the_span_marker_rejects_a_span_whose_child_event_is_a_codex_event() -> None:
    harness = Harness()
    span = _span({"agent_context.origin": "platform"}, events=[_codex_event()])

    response = _response(await harness.post(_export(span).SerializeToString()))

    assert response.partial_success.error_message == "spans rejected: platform_origin"
    assert harness.service.batches == []


async def test_an_over_long_value_the_mapper_would_accept_is_refused_by_name() -> None:
    harness = Harness()
    # error.message is never read by the mapper, and its own cap is 1 MiB: only the receiver's
    # 16 KiB guard refuses it.
    big = _codex_event(**{"error.message": "e" * (otlp_api.MAX_ATTRIBUTE_VALUE_CHARS + 1)})

    response = _response(await harness.post(_export(_span(events=[big])).SerializeToString()))

    assert response.partial_success.error_message == "spans rejected: attribute_value_too_long"


async def test_event_attributes_win_over_resource_attributes() -> None:
    harness = Harness()
    resource = {**_fixture()["resource"], "model": "resource-model"}
    span = _span(events=[_codex_event(model="event-model")])

    _response(await harness.post(_export(span, resource=resource).SerializeToString()))

    assert harness.service.batches[0].events[0].payload["model"] == "event-model"


@pytest.mark.parametrize("seed", range(6))
def test_the_wire_scan_counts_what_the_parser_parses(seed: int) -> None:
    import random

    generator = random.Random(seed)
    spans = []
    for _ in range(generator.randint(1, 6)):
        span = _span(
            {
                f"s{i}": generator.choice(["v", 1, 2.5, True, [1, [2]]])
                for i in range(generator.randint(0, 5))
            },
            events=[
                {f"e{i}": "x" * generator.randint(0, 200) for i in range(generator.randint(0, 4))}
                for _ in range(generator.randint(0, 5))
            ],
        )
        for _ in range(generator.randint(0, 3)):
            span.links.add(trace_id=b"\x01" * 16, span_id=b"\x02" * 8, attributes=_pairs({"l": 1}))
        spans.append(span)
    data = _export(*spans).SerializeToString()
    limits = otlp_wire.WireLimits(16, 64, 500, 1000, 1000, 65_536, 128)
    scanner = otlp_wire._Scanner(data, limits)
    scanner.request()
    parsed = ExportTraceServiceRequest.FromString(data)

    def nested(value: Any) -> int:
        kind = value.WhichOneof("value")
        if kind == "array_value":
            return sum(1 + nested(item) for item in value.array_value.values)
        if kind == "kvlist_value":
            return sum(1 + nested(item.value) for item in value.kvlist_value.values)
        return 0

    def count(pairs: Any) -> int:
        return sum(1 + nested(pair.value) for pair in pairs)

    expected_attributes = sum(
        count(rs.resource.attributes)
        + sum(
            count(ss.scope.attributes)
            + sum(
                count(sp.attributes)
                + sum(count(ev.attributes) for ev in sp.events)
                + sum(count(link.attributes) for link in sp.links)
                for sp in ss.spans
            )
            for ss in rs.scope_spans
        )
        for rs in parsed.resource_spans
    )
    spans_parsed = [sp for rs in parsed.resource_spans for ss in rs.scope_spans for sp in ss.spans]
    assert scanner.counts.spans == len(spans_parsed)
    assert scanner.counts.records == len(spans_parsed) + sum(len(sp.events) for sp in spans_parsed)
    assert scanner.counts.links == sum(len(sp.links) for sp in spans_parsed)
    assert scanner.counts.attributes == expected_attributes


async def test_span_less_resources_are_never_converted_or_judged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    converted: list[int] = []
    real = otlp_api._attributes

    def counting(pairs: Any) -> Any:
        converted.append(1)
        return real(pairs)

    monkeypatch.setattr(otlp_api, "_attributes", counting)
    harness = Harness()
    export = _export(_span(events=[_codex_event()]))
    # A span-less resource that would be refused (platform origin) if it were judged.
    export.resource_spans.add().resource.attributes.extend(
        _pairs({"service.name": "agent-context-platform", "agent_context.origin": "platform"})
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert not response.HasField("partial_success")
    assert len(harness.service.batches[0].events) == 1
    # One resource and one event converted; the span-less resource never.
    assert len(converted) == 2


async def test_a_refusal_naming_no_offender_is_not_resubmitted() -> None:
    harness = Harness()
    service = ConflictEach([])
    calls: list[int] = []

    async def refuse_all(batch: IngestBatchRequestV1) -> IngestOutcome:
        calls.append(len(batch.events))
        rejected = tuple(
            RejectedEventV1(event_id=e.event_id, error_code="batch_rejected", retryable=False)
            for e in batch.events
        )
        return IngestOutcome(409, IngestBatchResponseV1(batch_id=batch.batch_id, rejected=rejected))

    object.__setattr__(service, "ingest", refuse_all)
    object.__setattr__(harness.runtime, "service", service)
    export = _export(
        *(_span(events=[_codex_event(**{"conversation.id": _session(i)})]) for i in (1, 2))
    )

    response = _response(await harness.post(export.SerializeToString()))

    assert calls == [2]  # one batch, never resubmitted
    assert response.partial_success.rejected_spans == 2


async def test_a_differing_event_model_wins_and_the_resource_value_never_appears() -> None:
    harness = Harness()
    resource = {**_fixture()["resource"], "model": "resource-model-canary"}
    span = _span(events=[_codex_event(model="event-model")])

    _response(await harness.post(_export(span, resource=resource).SerializeToString()))

    (draft,) = harness.service.batches[0].events
    assert draft.payload["model"] == "event-model"
    assert "resource-model-canary" not in draft.model_dump_json()


@pytest.mark.parametrize(("events", "rejected"), [(2, 0), (3, 1)])
async def test_events_per_span_admits_exactly_its_value(events: int, rejected: int) -> None:
    harness = Harness(max_events_per_span=2)
    response = _response(
        await harness.post(_export(_span(events=[{"x": 1}] * events)).SerializeToString())
    )
    assert response.partial_success.rejected_spans == rejected
