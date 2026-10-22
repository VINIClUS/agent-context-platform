"""The OTLP wire scan (PLATFORM-053): sound for every descriptor field, bounded by its limits.

A generator fills EVERY field of every message reachable from the request descriptor (including
``Resource.entity_refs``), then mutates the wire bytes (merged singular fields, unknown fields,
byte flips). Whatever the input, the scan either refuses it with a fixed code or accepts it, and
when it accepts, its counts equal what the parser actually builds. Unknown fields are counted and
skipped, never refused or descended into.
"""

from __future__ import annotations

import random
import time
from typing import Any

import pytest
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import DecodeError, Message
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, ArrayValue, KeyValue, KeyValueList
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

from agent_context_platform.ledger import otlp_wire

pytestmark = pytest.mark.unit

LIMITS = otlp_wire.WireLimits(
    max_resources=16,
    max_scopes=64,
    max_spans=500,
    max_records=1000,
    max_links=1000,
    max_total_attributes=65_536,
    max_container_attributes=128,
)


def _limits(**overrides: int) -> otlp_wire.WireLimits:
    values = {name: getattr(LIMITS, name) for name in LIMITS.__dataclass_fields__}
    return otlp_wire.WireLimits(**{**values, **overrides})


def _request(*resource_spans: ResourceSpans) -> bytes:
    return ExportTraceServiceRequest(resource_spans=list(resource_spans)).SerializeToString()


def _kv(key: str, value: AnyValue) -> KeyValue:
    return KeyValue(key=key, value=value)


def _scan(data: bytes, **overrides: int) -> otlp_wire.Counts:
    return otlp_wire.scan(data, _limits(**overrides))


def _code(data: bytes, **overrides: int) -> str:
    with pytest.raises((otlp_wire.WireLimitExceeded, otlp_wire.WireRefused)) as info:
        _scan(data, **overrides)
    return str(info.value.code)  # type: ignore[union-attr]


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _field(number: int, payload: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(payload)) + payload


# --- refusals ------------------------------------------------------------------------------------


def _nested(span: bytes) -> bytes:
    """A request holding ``span`` as the only span: ResourceSpans.scope_spans.spans."""
    return _field(1, _field(2, _field(2, span)))


def test_unknown_fields_are_counted_and_skipped_at_every_level() -> None:
    known = _nested(Span(name="s").SerializeToString())
    base = _scan(known)
    # Every unknown wire type a parser keeps: varint, fixed64, length-delimited, fixed32. The
    # length-delimited one holds bytes that would be malformed if they were descended into.
    unknown = (
        _varint((99 << 3) | 0)
        + b"\x01"
        + _varint((98 << 3) | 1)
        + bytes(8)
        + _field(97, b"\x0b\xff\xff")
        + _varint((96 << 3) | 5)
        + bytes(4)
    )
    for data in (
        known + unknown,  # request level
        _nested(Span(name="s").SerializeToString() + unknown),  # inside a span
    ):
        counts = _scan(data)
        assert counts.fields == base.fields + 4
        assert counts.objects == base.objects
        assert counts.spans == 1
        parsed = ExportTraceServiceRequest.FromString(data)  # the parser keeps them as bytes
        assert parsed.resource_spans[0].scope_spans[0].spans[0].name == "s"


def test_an_unknown_field_flood_hits_the_field_budget() -> None:
    flood = (_varint((99 << 3) | 0) + b"\x00") * 1000
    assert _scan(flood, max_fields=1000).fields == 1000
    assert _code(flood, max_fields=999) == "too_many_fields"
    nested = _nested(Span().SerializeToString() + flood)
    assert _code(nested, max_fields=1000) == "too_many_fields"


def test_entity_refs_are_scanned_counted_and_bounded() -> None:
    resource = Resource()
    for index in range(3):
        resource.entity_refs.add(
            schema_url="u", type="t", id_keys=[f"i{index}", "j"], description_keys=["d"]
        )
    data = _request(ResourceSpans(resource=resource))

    counts = _scan(data)

    parsed = ExportTraceServiceRequest.FromString(data)
    # One ResourceSpans, three EntityRefs and their nine repeated strings, as the parser builds.
    assert counts.objects == _objects(parsed) == 13
    assert _code(data, max_objects=12) == "too_many_objects"
    assert _code(data, max_fields=counts.fields - 1) == "too_many_fields"
    _scan(data, max_objects=13, max_fields=counts.fields)


def test_a_known_field_with_a_mismatched_wire_type_is_refused() -> None:
    # Span.name (field 5) is a string: as a varint it does not match.
    assert _code(_nested(_varint((5 << 3) | 0) + b"\x01")) == "malformed_request"
    # Span.flags (field 16) is a fixed32: length-delimited does not match.
    assert _code(_nested(_field(16, b"\x00\x00\x00\x00"))) == "malformed_request"


def test_a_repeated_singular_message_field_is_refused() -> None:
    resource = _field(
        1, Resource(attributes=[_kv("a", AnyValue(string_value="v"))]).SerializeToString()
    )
    resource_spans = resource + resource  # the parser would merge both into one Resource
    assert _code(_field(1, resource_spans)) == "repeated_singular_field"


@pytest.mark.parametrize(
    "data",
    [
        b"\x0a\xff\xff\xff\xff\x0f",  # length beyond the input
        b"\x0b\x00",  # group start (wire type 3)
        b"\x0c",  # group end (wire type 4)
        b"\x0e\x00",  # reserved wire type 6
        b"\x08\x01",  # resource_spans with a varint wire type
        b"\xfb\x06",  # unknown field 111 as a group start
        b"\xff\x06",  # unknown field 111 with reserved wire type 7
        b"\xfe\x06",  # unknown field 111 with reserved wire type 6
        b"\xfa\x06\x05\x00",  # unknown length-delimited field beyond the input
        b"\xf9\x06\x00\x00",  # unknown fixed64 field beyond the input
        b"\x0a\x01" + b"\x08",  # truncated varint inside
        b"\x0a" + b"\xff" * 11,  # varint longer than 64 bits
    ],
)
def test_malformed_wire_data_is_refused(data: bytes) -> None:
    assert _code(data) == "malformed_request"


# --- request-level limits at the boundary --------------------------------------------------------


def _resources(count: int) -> bytes:
    return _request(*(ResourceSpans() for _ in range(count)))


def _spans(count: int) -> bytes:
    return _request(ResourceSpans(scope_spans=[ScopeSpans(spans=[Span() for _ in range(count)])]))


def _linked(count: int) -> bytes:
    span = Span()
    for _ in range(count):
        span.links.add()
    return _request(ResourceSpans(scope_spans=[ScopeSpans(spans=[span])]))


def _events(count: int) -> bytes:
    span = Span()
    for _ in range(count):
        span.events.add()
    return _request(ResourceSpans(scope_spans=[ScopeSpans(spans=[span])]))


@pytest.mark.parametrize(
    ("build", "limit", "code"),
    [
        (_resources, "max_resources", "too_many_resources"),
        (_spans, "max_spans", "too_many_spans"),
        (_linked, "max_links", "too_many_links"),
        (_events, "max_records", "too_many_records"),
    ],
)
@pytest.mark.parametrize("at", [3, 7])
def test_each_limit_admits_exactly_its_value(build: Any, limit: str, code: str, at: int) -> None:
    # A span is itself a record: `max_records` of `at` admits one span plus at - 1 events.
    count = at - 1 if limit == "max_records" else at
    _scan(build(count), **{limit: at})
    assert _code(build(count + 1), **{limit: at}) == code


def test_scopes_and_container_attributes_are_bounded() -> None:
    scopes = _request(ResourceSpans(scope_spans=[ScopeSpans() for _ in range(3)]))
    _scan(scopes, max_scopes=3)
    assert _code(scopes, max_scopes=2) == "too_many_scopes"
    attributes = [_kv(f"k{i}", AnyValue(string_value="v")) for i in range(4)]
    resource = _request(ResourceSpans(resource=Resource(attributes=attributes)))
    _scan(resource, max_container_attributes=4)
    assert _code(resource, max_container_attributes=3) == "too_many_attributes"


def test_nested_values_are_counted_and_their_depth_is_bounded() -> None:
    inner = AnyValue(
        kvlist_value=KeyValueList(
            values=[
                _kv("a", AnyValue(array_value=ArrayValue(values=[AnyValue(int_value=1)] * 3))),
                _kv("b", AnyValue(string_value="v")),
            ]
        )
    )
    span = Span(attributes=[_kv("outer", inner)])
    data = _request(ResourceSpans(scope_spans=[ScopeSpans(spans=[span])]))

    counts = _scan(data)

    # outer + the kvlist's two KeyValues + the array's three elements.
    assert counts.attributes == 6
    assert _code(data, max_total_attributes=5) == "too_many_attributes"

    value = AnyValue(int_value=1)
    for depth in range(otlp_wire.MAX_VALUE_DEPTH + 1):
        value = (
            AnyValue(array_value=ArrayValue(values=[value]))
            if depth % 2
            else AnyValue(kvlist_value=KeyValueList(values=[_kv("k", value)]))
        )
    deep = _request(
        ResourceSpans(scope_spans=[ScopeSpans(spans=[Span(attributes=[_kv("deep", value)])])])
    )
    assert _code(deep) == "too_many_attributes"


def test_unusual_key_value_shapes_take_the_generic_walk_and_count_the_same() -> None:
    # value before key, a multi-byte key length and an array value: none is the inline shape.
    array = AnyValue(array_value=ArrayValue(values=[AnyValue(string_value="x")] * 2))
    swapped = _field(2, array.SerializeToString()) + _field(1, b"k")
    long_key = KeyValue(key="k" * 200, value=AnyValue(string_value="v")).SerializeToString()
    span = Span().SerializeToString() + _field(9, swapped) + _field(9, long_key)
    data = _field(1, _field(2, _field(2, span)))

    counts = _scan(data)

    assert counts.attributes == 4  # two KeyValues plus two array elements
    parsed = ExportTraceServiceRequest.FromString(data)
    assert [kv.key for kv in parsed.resource_spans[0].scope_spans[0].spans[0].attributes] == [
        "k",
        "k" * 200,
    ]


def test_objects_and_fields_have_global_budgets() -> None:
    data = _spans(50)
    counts = _scan(data)
    assert _code(data, max_objects=counts.objects - 1) == "too_many_objects"
    assert _code(data, max_fields=counts.fields - 1) == "too_many_fields"


def test_the_scan_cost_is_bounded_by_the_field_budget_not_the_bytes() -> None:
    # Six megabytes of the smallest possible fields: the scan stops at its field budget.
    tiny = _varint((16 << 3) | 5) + b"\x00\x00\x00\x00"  # Span.flags (fixed32), repeated
    span = tiny * 600_000
    data = _field(1, _field(2, _field(2, span)))
    assert len(data) > 3_000_000

    started = time.perf_counter()
    assert _code(data) == "too_many_fields"
    assert time.perf_counter() - started < 2.0


# --- soundness over every descriptor field -------------------------------------------------------


def _fill(message: Message, generator: random.Random, depth: int = 0) -> None:
    descriptor: Descriptor = message.DESCRIPTOR
    for item in descriptor.fields:
        if generator.random() < 0.3:
            continue
        if item.is_repeated:
            container = getattr(message, item.name)
            for _ in range(generator.randint(0, 3)):
                if item.message_type is not None:
                    if depth < 6:
                        _fill(container.add(), generator, depth + 1)
                else:
                    container.append(_scalar(item, generator))
        elif item.message_type is not None:
            if depth < 6:
                _fill(getattr(message, item.name), generator, depth + 1)
                # Touching an empty sub-message does not set it; mark it present.
                getattr(message, item.name).SetInParent()
        else:
            setattr(message, item.name, _scalar(item, generator))


def _scalar(item: FieldDescriptor, generator: random.Random) -> Any:
    if item.type == FieldDescriptor.TYPE_STRING:
        return generator.choice(["", "x", "é" * 3, "k" * 200])
    if item.type == FieldDescriptor.TYPE_BYTES:
        return bytes(generator.randrange(256) for _ in range(generator.choice([0, 8, 16])))
    if item.type == FieldDescriptor.TYPE_BOOL:
        return generator.random() < 0.5
    if item.type in (FieldDescriptor.TYPE_DOUBLE, FieldDescriptor.TYPE_FLOAT):
        return generator.random()
    if item.type == FieldDescriptor.TYPE_ENUM:
        return item.enum_type.values[0].number
    if item.type in (FieldDescriptor.TYPE_INT32, FieldDescriptor.TYPE_INT64):
        return generator.choice([0, 1, -1, 2**31 - 1])
    return generator.choice([0, 1, 2**31 - 1])


def _objects(message: Message) -> int:
    """Every repeated element the parser built, at every level."""
    total = 0
    for item, value in message.ListFields():
        if item.is_repeated:
            total += len(value)
            if item.message_type is not None:
                total += sum(_objects(element) for element in value)
        elif item.message_type is not None:
            total += _objects(value)
    return total


def _mutate(data: bytes, generator: random.Random) -> bytes:
    choice = generator.randrange(5)
    if choice == 0 or not data:
        return data
    if choice == 1:  # an unknown field somewhere at the top
        return data + _varint((77 << 3) | 0) + b"\x01"
    if choice == 2:  # a singular message field twice: the parser would merge it
        resource = _field(1, Resource().SerializeToString())
        return data + _field(1, resource + resource)
    if choice == 3:  # flip a byte
        index = generator.randrange(len(data))
        return (
            data[:index] + bytes([data[index] ^ (1 << generator.randrange(8))]) + data[index + 1 :]
        )
    return data + data  # a merge: resource_spans are repeated, so this is a valid duplicate


SEEDS = range(300)


def _compare(seed: int) -> bool:
    """Scan and parse one generated input; ``True`` when both accepted it and counts agree."""
    generator = random.Random(seed)
    message = ExportTraceServiceRequest()
    _fill(message, generator)
    data = _mutate(message.SerializeToString(), generator)

    try:
        counts = otlp_wire.scan(data, LIMITS)
    except (otlp_wire.WireLimitExceeded, otlp_wire.WireRefused):
        return False
    try:
        parsed = ExportTraceServiceRequest.FromString(data)
    except DecodeError:
        return False  # e.g. invalid UTF-8 in a string: the receiver answers 400 too
    assert counts.objects == _objects(parsed)
    assert counts.spans == sum(
        len(scope.spans) for rs in parsed.resource_spans for scope in rs.scope_spans
    )
    return True


@pytest.mark.parametrize("seed", SEEDS)
def test_whatever_the_input_the_scan_refuses_it_or_counts_what_the_parser_builds(
    seed: int,
) -> None:
    _compare(seed)


def test_most_generated_inputs_reach_the_comparison() -> None:
    # Only the deliberately merged singular fields and a few byte flips are refused.
    assert sum(_compare(seed) for seed in SEEDS) >= 240


def test_overlong_varints_are_accepted_like_the_parser_does() -> None:
    span = Span(name="s").SerializeToString()
    # The same span with its length encoded in three bytes instead of one.
    overlong = b"\x12" + bytes([len(span) | 0x80, 0x80, 0x00]) + span
    data = _field(1, _field(2, overlong))
    counts = _scan(data)
    assert counts.spans == 1
    assert (
        ExportTraceServiceRequest.FromString(data).resource_spans[0].scope_spans[0].spans[0].name
        == "s"
    )


# --- exact boundaries of the global budgets, the depth and the varint cap -------------------------


def test_fields_objects_and_attributes_admit_exactly_their_limit() -> None:
    attributes = [_kv(f"k{i}", AnyValue(string_value="v")) for i in range(5)]
    span = Span(attributes=attributes)
    data = _request(ResourceSpans(scope_spans=[ScopeSpans(spans=[span, Span()])]))
    counts = _scan(data)
    assert counts.attributes == 5

    for limit, code, value in (
        ("max_fields", "too_many_fields", counts.fields),
        ("max_objects", "too_many_objects", counts.objects),
        ("max_total_attributes", "too_many_attributes", counts.attributes),
    ):
        assert _scan(data, **{limit: value}) == counts
        assert _code(data, **{limit: value - 1}) == code


def _value_of_depth(depth: int) -> AnyValue:
    """An attribute value nested in ``depth`` alternating key-value lists and arrays."""
    value = AnyValue(int_value=1)
    for level in range(depth):
        value = (
            AnyValue(array_value=ArrayValue(values=[value]))
            if level % 2
            else AnyValue(kvlist_value=KeyValueList(values=[_kv("k", value)]))
        )
    return value


@pytest.mark.parametrize("depth", [otlp_wire.MAX_VALUE_DEPTH - 1, otlp_wire.MAX_VALUE_DEPTH])
def test_values_nested_up_to_the_depth_limit_are_admitted(depth: int) -> None:
    span = Span(attributes=[_kv("deep", _value_of_depth(depth))])
    _scan(_request(ResourceSpans(scope_spans=[ScopeSpans(spans=[span])])))
    assert otlp_wire.MAX_VALUE_DEPTH == 4


def test_a_value_nested_one_level_deeper_is_refused() -> None:
    span = Span(attributes=[_kv("deep", _value_of_depth(otlp_wire.MAX_VALUE_DEPTH + 1))])
    data = _request(ResourceSpans(scope_spans=[ScopeSpans(spans=[span])]))
    assert _code(data) == "too_many_attributes"


def _overlong(value: int, size: int) -> bytes:
    """``value`` (below 128) as a varint padded to ``size`` bytes."""
    return bytes([value | 0x80]) + b"\x80" * (size - 2) + b"\x00"


def test_a_ten_byte_varint_is_admitted_and_an_eleven_byte_one_refused() -> None:
    def dropped_attributes(size: int) -> bytes:
        # Span.dropped_attributes_count (field 10, varint) = 1, padded to `size` bytes.
        return _nested(_varint((10 << 3) | 0) + _overlong(1, size))

    admitted = dropped_attributes(10)
    assert _scan(admitted).spans == 1
    parsed = ExportTraceServiceRequest.FromString(admitted)  # the parser takes ten bytes too
    assert parsed.resource_spans[0].scope_spans[0].spans[0].dropped_attributes_count == 1
    refused = dropped_attributes(11)
    assert _code(refused) == "malformed_request"
    with pytest.raises(DecodeError):
        ExportTraceServiceRequest.FromString(refused)  # and refuses eleven


def test_the_tables_refuse_a_schema_with_a_repeated_scalar() -> None:
    from google.protobuf.descriptor_pb2 import SourceCodeInfo

    with pytest.raises(TypeError, match="repeated scalar"):
        otlp_wire._tables(SourceCodeInfo.DESCRIPTOR)  # `Location.path` is a packed int32


# --- the inline attribute read counts exactly like the generic walk ------------------------------


class _GenericOnly(otlp_wire._Scanner):
    def _plain_attribute(self, start: int, end: int, depth: int) -> bool:
        return False


def _random_value(generator: random.Random, depth: int) -> AnyValue:
    choice = generator.randrange(5 if depth < 3 else 2)
    if choice == 0:
        length = generator.choice([0, 1, 60, 120, 125, 126, 127, 128, 300])
        return AnyValue(string_value="v" * length)
    if choice == 1:
        return AnyValue(int_value=generator.randrange(-5, 5))
    if choice == 2:
        return AnyValue(string_value=generator.choice(["", "é", "x" * 127]))
    if choice == 3:
        return AnyValue(
            array_value=ArrayValue(
                values=[_random_value(generator, depth + 1) for _ in range(generator.randrange(3))]
            )
        )
    return AnyValue(
        kvlist_value=KeyValueList(
            values=[_random_kv(generator, depth + 1) for _ in range(generator.randrange(3))]
        )
    )


def _random_kv(generator: random.Random, depth: int) -> KeyValue:
    key = "k" * generator.choice([0, 1, 100, 124, 125, 126, 127, 128])
    if generator.random() < 0.1:
        return KeyValue(key=key)  # no value at all
    return _kv(key, _random_value(generator, depth))


@pytest.mark.parametrize("seed", range(40))
def test_the_inline_attribute_read_counts_like_the_generic_walk(seed: int) -> None:
    generator = random.Random(seed)
    resource = Resource(attributes=[_random_kv(generator, 0) for _ in range(5)])
    span = Span(attributes=[_random_kv(generator, 0) for _ in range(20)])
    data = _request(ResourceSpans(resource=resource, scope_spans=[ScopeSpans(spans=[span])]))

    fast = otlp_wire._Scanner(data, LIMITS)
    fast.request()
    generic = _GenericOnly(data, LIMITS)
    generic.request()

    assert fast.counts == generic.counts
