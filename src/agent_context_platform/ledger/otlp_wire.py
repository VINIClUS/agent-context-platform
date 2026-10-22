"""Bounded structural scan of an OTLP ``ExportTraceServiceRequest`` before it is parsed.

Parsing allocates per message and per repeated element, so a few kilobytes of gzip can inflate
into millions of objects. This scanner walks the protobuf wire format with the generated
descriptors as its only schema, and is sound for every field the schema has:

- it visits EVERY field occurrence of every message reachable from the request descriptor and
  counts it against a field budget, so its own cost is bounded by the limits, not by the bytes;
- it counts EVERY repeated element (message, string or bytes; the traces schema has no other
  repeated field, and the descriptor tables refuse to build if it gains one) against an object
  budget, so the parser never allocates more than the limits allow;
- an unknown field (a newer schema) is counted and skipped by its length, never descended into:
  the parser keeps it as raw bytes, so OTLP stays forward compatible;
- it refuses only what it cannot bound otherwise: a second occurrence of a singular message field
  (the parser would merge it into one container), and a wire type that does not match a known
  field, groups and reserved wire types included;
- on top, domain hooks bound resources, scopes, spans, records (spans plus events), links,
  attributes (every level, nested values included), attributes per resource or scope, and the
  nesting depth of attribute values.

It stops at the first exceeded limit. ``WireLimitExceeded`` carries a fixed 413 code,
``WireRefused`` a fixed 400 code.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    ArrayValue,
    InstrumentationScope,
    KeyValue,
    KeyValueList,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

_VARINT: Final = 0
_FIXED64: Final = 1
_LENGTH_DELIMITED: Final = 2
_FIXED32: Final = 5

# Nested attribute values (arrays, key-value lists) deeper than this are refused.
MAX_VALUE_DEPTH: Final = 4

_WIRE_TYPE_BY_FIELD_TYPE: Final[Mapping[int, int]] = {
    FieldDescriptor.TYPE_DOUBLE: _FIXED64,
    FieldDescriptor.TYPE_FIXED64: _FIXED64,
    FieldDescriptor.TYPE_SFIXED64: _FIXED64,
    FieldDescriptor.TYPE_FLOAT: _FIXED32,
    FieldDescriptor.TYPE_FIXED32: _FIXED32,
    FieldDescriptor.TYPE_SFIXED32: _FIXED32,
    FieldDescriptor.TYPE_INT32: _VARINT,
    FieldDescriptor.TYPE_INT64: _VARINT,
    FieldDescriptor.TYPE_UINT32: _VARINT,
    FieldDescriptor.TYPE_UINT64: _VARINT,
    FieldDescriptor.TYPE_SINT32: _VARINT,
    FieldDescriptor.TYPE_SINT64: _VARINT,
    FieldDescriptor.TYPE_BOOL: _VARINT,
    FieldDescriptor.TYPE_ENUM: _VARINT,
    FieldDescriptor.TYPE_STRING: _LENGTH_DELIMITED,
    FieldDescriptor.TYPE_BYTES: _LENGTH_DELIMITED,
    FieldDescriptor.TYPE_MESSAGE: _LENGTH_DELIMITED,
}


@dataclass(frozen=True, slots=True)
class _Field:
    number: int
    wire_type: int
    repeated: bool
    message: Descriptor | None


def _table(descriptor: Descriptor) -> dict[int, _Field]:
    return {
        item.number: _Field(
            number=item.number,
            wire_type=_WIRE_TYPE_BY_FIELD_TYPE[item.type],
            repeated=item.is_repeated,
            message=item.message_type,
        )
        for item in descriptor.fields
    }


def _tables(root: Descriptor) -> dict[str, dict[int, _Field]]:
    tables: dict[str, dict[int, _Field]] = {}
    pending = [root]
    while pending:
        descriptor = pending.pop()
        if descriptor.full_name in tables:
            continue
        table = _table(descriptor)
        for spec in table.values():
            if spec.repeated and spec.wire_type != _LENGTH_DELIMITED:
                # A packable repeated scalar (none in OTLP traces today): the walk below does not
                # count packed elements, so a schema that adds one must extend it first.
                raise TypeError(f"{descriptor.full_name}.{spec.number} is a repeated scalar")
        tables[descriptor.full_name] = table
        pending.extend(item.message_type for item in descriptor.fields if item.message_type)
    return tables


_TABLES: Final = _tables(ExportTraceServiceRequest.DESCRIPTOR)
_REQUEST: Final = ExportTraceServiceRequest.DESCRIPTOR.full_name
_RESOURCE_SPANS: Final = ResourceSpans.DESCRIPTOR.full_name
_SCOPE_SPANS: Final = ScopeSpans.DESCRIPTOR.full_name
_SPAN: Final = Span.DESCRIPTOR.full_name
_EVENT: Final = Span.Event.DESCRIPTOR.full_name
_LINK: Final = Span.Link.DESCRIPTOR.full_name
_KEY_VALUE: Final = KeyValue.DESCRIPTOR.full_name
_ARRAY_VALUE: Final = ArrayValue.DESCRIPTOR.full_name
_NESTING: Final = frozenset({_ARRAY_VALUE, KeyValueList.DESCRIPTOR.full_name})
# Containers whose own attributes are bounded by `max_container_attributes`.
_CONTAINERS: Final = frozenset(
    {Resource.DESCRIPTOR.full_name, InstrumentationScope.DESCRIPTOR.full_name}
)
_CONTAINER_ATTRIBUTES: Final = frozenset(
    {
        Resource.DESCRIPTOR.fields_by_name["attributes"].number,
        InstrumentationScope.DESCRIPTOR.fields_by_name["attributes"].number,
    }
)
# The common attribute shape, read inline: KeyValue{1: key, 2: AnyValue{1: string}}.
_KEY_TAG: Final = (KeyValue.DESCRIPTOR.fields_by_name["key"].number << 3) | _LENGTH_DELIMITED
_VALUE_TAG: Final = (KeyValue.DESCRIPTOR.fields_by_name["value"].number << 3) | _LENGTH_DELIMITED
_STRING_TAG: Final = (
    AnyValue.DESCRIPTOR.fields_by_name["string_value"].number << 3
) | _LENGTH_DELIMITED


@dataclass(frozen=True, slots=True)
class WireLimits:
    """Request-level structural limits enforced before parsing."""

    max_resources: int
    max_scopes: int
    max_spans: int
    max_records: int
    max_links: int
    max_total_attributes: int
    max_container_attributes: int
    # Every field occurrence visited (bounds the scan's own cost).
    max_fields: int = 300_000
    # Every repeated element, message or scalar (bounds what the parser allocates).
    max_objects: int = 200_000


class WireLimitExceeded(Exception):
    """A limit was exceeded; ``code`` is the fixed 413 code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class WireRefused(Exception):
    """Well-formed or not, the bytes hold something the receiver refuses; ``code`` is for 400."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


# The previous name of the malformed-input error; every refusal is a 400.
WireMalformed = WireRefused


@dataclass(slots=True)
class Counts:
    resources: int = 0
    scopes: int = 0
    spans: int = 0
    records: int = 0
    links: int = 0
    attributes: int = 0
    fields: int = 0
    objects: int = 0


class _Scanner:
    def __init__(self, data: bytes, limits: WireLimits) -> None:
        self._data = data
        self._limits = limits
        self.counts = Counts()

    def request(self) -> None:
        self._message(0, len(self._data), _REQUEST, 0)

    # --- primitives ---------------------------------------------------------------------------

    def _varint(self, position: int, end: int) -> tuple[int, int]:
        data = self._data
        result = shift = 0
        while True:
            if position >= end or shift > 63:
                raise WireRefused("malformed_request")
            byte = data[position]
            position += 1
            result |= (byte & 0x7F) << shift
            if byte < 0x80:
                return result, position
            shift += 7

    def _field_visited(self) -> None:
        counts = self.counts
        counts.fields += 1
        if counts.fields > self._limits.max_fields:
            raise WireLimitExceeded("too_many_fields")

    def _objects(self, number: int) -> None:
        counts = self.counts
        counts.objects += number
        if counts.objects > self._limits.max_objects:
            raise WireLimitExceeded("too_many_objects")

    # --- the generic walk ---------------------------------------------------------------------

    def _message(self, start: int, end: int, kind: str, depth: int) -> None:
        """Walk one message of type ``kind``: every field checked, counted and, if a message,
        descended into. ``depth`` is the nesting depth of attribute values."""
        self._enter(kind, depth)
        table = _TABLES[kind]
        data = self._data
        singular_messages: set[int] = set()
        container_attributes = 0
        position = start
        while position < end:
            self._field_visited()
            tag = data[position]
            if tag < 0x80:
                position += 1
            else:
                tag, position = self._varint(position, end)
            number, wire_type = tag >> 3, tag & 0x7
            spec = table.get(number)
            if spec is None:
                # Unknown (a newer schema): counted above, never descended into; the parser keeps
                # it as raw bytes, bounded by the inflation cap. OTLP stays forward compatible.
                position = self._skip_unknown(position, end, wire_type)
                continue
            if wire_type == _LENGTH_DELIMITED:
                if position < end and data[position] < 0x80:
                    length = data[position]
                    position += 1
                else:
                    length, position = self._varint(position, end)
                field_end = position + length
                if field_end > end:
                    raise WireRefused("malformed_request")
                if spec.wire_type == _LENGTH_DELIMITED:
                    if spec.message is not None:
                        if spec.repeated:
                            self._objects(1)
                        elif number in singular_messages:
                            # The parser would merge both into one container: refuse instead.
                            raise WireRefused("repeated_singular_field")
                        else:
                            singular_messages.add(number)
                        if kind in _CONTAINERS and number in _CONTAINER_ATTRIBUTES:
                            container_attributes += 1
                            if container_attributes > self._limits.max_container_attributes:
                                raise WireLimitExceeded("too_many_attributes")
                        child = spec.message.full_name
                        child_depth = depth + 1 if child in _NESTING else depth
                        if kind == _ARRAY_VALUE:
                            self._attribute(depth)  # an array element is an attribute value
                        if not (
                            child == _KEY_VALUE
                            and self._plain_attribute(position, field_end, child_depth)
                        ):
                            self._message(position, field_end, child, child_depth)
                    elif spec.repeated:
                        self._objects(1)  # a repeated string or bytes element
                else:
                    raise WireRefused("malformed_request")  # wire type mismatch
                position = field_end
            elif wire_type == spec.wire_type:  # a singular scalar (no repeated scalar, see _tables)
                position = self._skip(position, end, wire_type)
            else:
                raise WireRefused("malformed_request")  # wire type mismatch, groups, reserved

    def _enter(self, kind: str, depth: int) -> None:
        counts = self.counts
        limits = self._limits
        if kind == _KEY_VALUE:
            self._attribute(depth)
        elif kind == _SPAN:
            counts.spans += 1
            if counts.spans > limits.max_spans:
                raise WireLimitExceeded("too_many_spans")
            self._record()
        elif kind == _EVENT:
            self._record()
        elif kind == _LINK:
            counts.links += 1
            if counts.links > limits.max_links:
                raise WireLimitExceeded("too_many_links")
        elif kind == _SCOPE_SPANS:
            counts.scopes += 1
            if counts.scopes > limits.max_scopes:
                raise WireLimitExceeded("too_many_scopes")
        elif kind == _RESOURCE_SPANS:
            counts.resources += 1
            if counts.resources > limits.max_resources:
                raise WireLimitExceeded("too_many_resources")
        elif kind in _NESTING and depth > MAX_VALUE_DEPTH:
            raise WireLimitExceeded("too_many_attributes")

    def _attribute(self, depth: int) -> None:
        counts = self.counts
        counts.attributes += 1
        if counts.attributes > self._limits.max_total_attributes or depth > MAX_VALUE_DEPTH:
            raise WireLimitExceeded("too_many_attributes")

    def _record(self) -> None:
        counts = self.counts
        counts.records += 1
        if counts.records > self._limits.max_records:
            raise WireLimitExceeded("too_many_records")

    def _plain_attribute(self, start: int, end: int, depth: int) -> bool:
        """The common attribute, read inline: exactly a key and a single string value.

        Returns ``False`` (nothing counted) for any other shape, which then takes the generic
        walk. Counts the same fields and checks as the generic walk would.
        """
        data = self._data
        if end - start < 4 or data[start] != _KEY_TAG or data[start + 1] >= 0x80:
            return False
        value_tag_at = start + 2 + data[start + 1]
        if value_tag_at + 3 > end or data[value_tag_at] != _VALUE_TAG:
            return False
        value_length = data[value_tag_at + 1]
        value_start = value_tag_at + 2
        if value_length >= 0x80 or value_start + value_length != end:
            return False
        if data[value_start] != _STRING_TAG or value_start + 1 >= end:
            return False
        string_length = data[value_start + 1]
        if string_length >= 0x80 or value_start + 2 + string_length != end:
            return False
        self._attribute(depth)
        for _ in range(3):  # key, value, string_value
            self._field_visited()
        return True

    def _skip_unknown(self, position: int, end: int, wire_type: int) -> int:
        if wire_type == _LENGTH_DELIMITED:
            length, position = self._varint(position, end)
            if position + length > end:
                raise WireRefused("malformed_request")
            return position + length
        if wire_type in (_VARINT, _FIXED64, _FIXED32):
            return self._skip(position, end, wire_type)
        raise WireRefused("malformed_request")  # groups (3, 4) and reserved types (6, 7)

    def _skip(self, position: int, end: int, wire_type: int) -> int:
        if wire_type == _VARINT:
            _, position = self._varint(position, end)
        elif wire_type == _FIXED64:
            position += 8
        else:
            position += 4
        if position > end:
            raise WireRefused("malformed_request")
        return position


def scan(data: bytes, limits: WireLimits) -> Counts:
    """Raise ``WireLimitExceeded`` or ``WireRefused`` unless ``data`` is within ``limits``."""
    scanner = _Scanner(data, limits)
    scanner.request()
    return scanner.counts
