"""Bounded structural scan of an OTLP ``ExportTraceServiceRequest`` before it is parsed.

Parsing allocates one object per message, so a few kilobytes of gzip can inflate into millions of
tiny links, attributes or nested array values whose parsing alone costs hundreds of MiB. This
scanner walks the protobuf wire format instead: it descends only into the container fields
(resource spans, scopes, spans, span events, links, attributes and nested attribute values),
skips every other field by its length, and stops at the first exceeded limit. Its work is
therefore bounded by the limits themselves, whatever the input, and the parser only ever sees a
message whose object count is within them.

Field numbers come from the generated descriptors, never from hand-written constants.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Final

from google.protobuf.descriptor import Descriptor
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


def _number(descriptor: Descriptor, name: str) -> int:
    return int(descriptor.fields_by_name[name].number)


_REQUEST_RESOURCE_SPANS: Final = _number(ExportTraceServiceRequest.DESCRIPTOR, "resource_spans")
_RESOURCE_SPANS_RESOURCE: Final = _number(ResourceSpans.DESCRIPTOR, "resource")
_RESOURCE_SPANS_SCOPE_SPANS: Final = _number(ResourceSpans.DESCRIPTOR, "scope_spans")
_RESOURCE_ATTRIBUTES: Final = _number(Resource.DESCRIPTOR, "attributes")
_SCOPE_SPANS_SCOPE: Final = _number(ScopeSpans.DESCRIPTOR, "scope")
_SCOPE_SPANS_SPANS: Final = _number(ScopeSpans.DESCRIPTOR, "spans")
_SCOPE_ATTRIBUTES: Final = _number(InstrumentationScope.DESCRIPTOR, "attributes")
_SPAN_ATTRIBUTES: Final = _number(Span.DESCRIPTOR, "attributes")
_SPAN_EVENTS: Final = _number(Span.DESCRIPTOR, "events")
_SPAN_LINKS: Final = _number(Span.DESCRIPTOR, "links")
_EVENT_ATTRIBUTES: Final = _number(Span.Event.DESCRIPTOR, "attributes")
_LINK_ATTRIBUTES: Final = _number(Span.Link.DESCRIPTOR, "attributes")
_KEY_VALUE_VALUE: Final = _number(KeyValue.DESCRIPTOR, "value")
_ANY_ARRAY: Final = _number(AnyValue.DESCRIPTOR, "array_value")
_ANY_KVLIST: Final = _number(AnyValue.DESCRIPTOR, "kvlist_value")
# The one-byte tag of a length-delimited `string_value` field.
_ANY_STRING_TAG: Final = (_number(AnyValue.DESCRIPTOR, "string_value") << 3) | 2
_ARRAY_VALUES: Final = _number(ArrayValue.DESCRIPTOR, "values")
_KVLIST_VALUES: Final = _number(KeyValueList.DESCRIPTOR, "values")

# Visits one length-delimited field: (start, end) offsets of its payload.
Handler = Callable[[int, int], None]

_VARINT: Final = 0
_FIXED64: Final = 1
_LENGTH_DELIMITED: Final = 2
_FIXED32: Final = 5
# Nested attribute values (arrays, key-value lists) deeper than this are refused.
MAX_VALUE_DEPTH: Final = 4


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


class WireLimitExceeded(Exception):
    """A limit was exceeded; ``code`` is the fixed 413 code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class WireMalformed(Exception):
    """The bytes are not a well-formed protobuf message."""


@dataclass(slots=True)
class _Counts:
    resources: int = 0
    scopes: int = 0
    spans: int = 0
    records: int = 0
    links: int = 0
    attributes: int = 0


class _Scanner:
    def __init__(self, data: bytes, limits: WireLimits) -> None:
        self._data = data
        self._limits = limits
        self.counts = _Counts()

    # --- primitives ---------------------------------------------------------------------------

    def _varint(self, position: int, end: int) -> tuple[int, int]:
        data = self._data
        result = shift = 0
        while True:
            if position >= end or shift > 63:
                raise WireMalformed
            byte = data[position]
            position += 1
            result |= (byte & 0x7F) << shift
            if byte < 0x80:
                return result, position
            shift += 7

    def _walk(self, start: int, end: int, visit: Mapping[int, Handler]) -> None:
        """Walk one message region; length-delimited fields in ``visit`` are handed over."""
        data = self._data
        position = start
        while position < end:
            tag = data[position]
            if tag < 0x80:  # one-byte tag: every field this scanner visits
                position += 1
            else:
                tag, position = self._varint(position, end)
            number, wire_type = tag >> 3, tag & 0x7
            if number == 0:
                raise WireMalformed
            if wire_type != _LENGTH_DELIMITED:
                position = self._skip(position, end, wire_type)
                continue
            if position < end and data[position] < 0x80:
                length = data[position]
                position += 1
            else:
                length, position = self._varint(position, end)
            field_end = position + length
            if field_end > end:
                raise WireMalformed
            handler = visit.get(number)
            if handler is not None:
                handler(position, field_end)
            position = field_end

    # --- limits -------------------------------------------------------------------------------

    def _attribute(self, depth: int) -> None:
        self.counts.attributes += 1
        if self.counts.attributes > self._limits.max_total_attributes:
            raise WireLimitExceeded("too_many_attributes")
        if depth > MAX_VALUE_DEPTH:
            raise WireLimitExceeded("too_many_attributes")

    # --- messages -----------------------------------------------------------------------------

    def request(self) -> None:
        self._walk(0, len(self._data), {_REQUEST_RESOURCE_SPANS: self._resource_spans})

    def _resource_spans(self, start: int, end: int) -> None:
        self.counts.resources += 1
        if self.counts.resources > self._limits.max_resources:
            raise WireLimitExceeded("too_many_resources")
        self._walk(
            start,
            end,
            {
                _RESOURCE_SPANS_RESOURCE: self._resource,
                _RESOURCE_SPANS_SCOPE_SPANS: self._scope_spans,
            },
        )

    def _resource(self, start: int, end: int) -> None:
        self._container(start, end, _RESOURCE_ATTRIBUTES)

    def _scope_spans(self, start: int, end: int) -> None:
        self.counts.scopes += 1
        if self.counts.scopes > self._limits.max_scopes:
            raise WireLimitExceeded("too_many_scopes")
        self._walk(start, end, {_SCOPE_SPANS_SCOPE: self._scope, _SCOPE_SPANS_SPANS: self._span})

    def _scope(self, start: int, end: int) -> None:
        self._container(start, end, _SCOPE_ATTRIBUTES)

    def _container(self, start: int, end: int, field_number: int) -> None:
        """A resource or scope: its own attribute count is bounded request-wide."""
        before = self.counts.attributes
        self._walk(start, end, {field_number: self._key_value_at(0)})
        if self.counts.attributes - before > self._limits.max_container_attributes:
            raise WireLimitExceeded("too_many_attributes")

    def _span(self, start: int, end: int) -> None:
        self.counts.spans += 1
        if self.counts.spans > self._limits.max_spans:
            raise WireLimitExceeded("too_many_spans")
        self._record()
        self._walk(
            start,
            end,
            {
                _SPAN_ATTRIBUTES: self._key_value_at(0),
                _SPAN_EVENTS: self._event,
                _SPAN_LINKS: self._link,
            },
        )

    def _record(self) -> None:
        self.counts.records += 1
        if self.counts.records > self._limits.max_records:
            raise WireLimitExceeded("too_many_records")

    def _event(self, start: int, end: int) -> None:
        self._record()
        self._walk(start, end, {_EVENT_ATTRIBUTES: self._key_value_at(0)})

    def _link(self, start: int, end: int) -> None:
        self.counts.links += 1
        if self.counts.links > self._limits.max_links:
            raise WireLimitExceeded("too_many_links")
        self._walk(start, end, {_LINK_ATTRIBUTES: self._key_value_at(0)})

    def _key_value_at(self, depth: int) -> Handler:
        return partial(self._key_value, depth=depth)

    def _key_value(self, start: int, end: int, depth: int) -> None:
        """One attribute: counted, and its value inspected only when it may nest values.

        The hot path of the scan (every attribute of every record passes here): one-byte tags and
        lengths are read inline, and a value that is exactly one scalar field is skipped whole.
        """
        counts = self.counts
        counts.attributes += 1
        if counts.attributes > self._limits.max_total_attributes or depth > MAX_VALUE_DEPTH:
            raise WireLimitExceeded("too_many_attributes")
        data = self._data
        position = start
        while position < end:
            tag = data[position]
            if tag >= 0x80 or tag & 0x7 != _LENGTH_DELIMITED:
                # Unusual shape: let the general walker finish this message.
                self._walk(position, end, {_KEY_VALUE_VALUE: self._any_value_at(depth)})
                return
            position += 1
            if position < end and data[position] < 0x80:
                length = data[position]
                position += 1
            else:
                length, position = self._varint(position, end)
            field_end = position + length
            if field_end > end:
                raise WireMalformed
            if (
                tag >> 3 == _KEY_VALUE_VALUE
                and position < field_end
                and not self._single_scalar(position, field_end)
            ):
                self._any_value(position, field_end, depth)
            position = field_end

    def _single_scalar(self, start: int, end: int) -> bool:
        """Whether an AnyValue payload is exactly one string field (the common case)."""
        data = self._data
        if data[start] != _ANY_STRING_TAG:
            return False
        position = start + 1
        if position < end and data[position] < 0x80:
            return position + 1 + data[position] == end
        length, position = self._varint(position, end)
        return position + length == end

    def _any_value(self, start: int, end: int, depth: int) -> None:
        """A scalar is skipped by length; an array or key-value list is descended into."""
        data = self._data
        position = start
        while position < end:
            tag, position = self._varint(position, end)
            number, wire_type = tag >> 3, tag & 0x7
            if wire_type != _LENGTH_DELIMITED:
                position = self._skip(position, end, wire_type)
                continue
            length, position = self._varint(position, end)
            field_end = position + length
            if field_end > end:
                raise WireMalformed
            if number == _ANY_ARRAY:
                self._walk(position, field_end, {_ARRAY_VALUES: self._element_at(depth + 1)})
            elif number == _ANY_KVLIST:
                self._walk(position, field_end, {_KVLIST_VALUES: self._key_value_at(depth + 1)})
            position = field_end
        del data

    def _any_value_at(self, depth: int) -> Handler:
        return lambda start, end: self._any_value(start, end, depth)

    def _element_at(self, depth: int) -> Handler:
        def element(start: int, end: int) -> None:
            self._attribute(depth)
            self._any_value(start, end, depth)

        return element

    def _skip(self, position: int, end: int, wire_type: int) -> int:
        """Skip a non-length-delimited field's payload; groups and unknown types are malformed."""
        if wire_type == _VARINT:
            _, position = self._varint(position, end)
        elif wire_type == _FIXED64:
            position += 8
        elif wire_type == _FIXED32:
            position += 4
        else:
            raise WireMalformed
        if position > end:
            raise WireMalformed
        return position


def scan(data: bytes, limits: WireLimits) -> None:
    """Raise ``WireLimitExceeded`` or ``WireMalformed`` unless ``data`` is within ``limits``."""
    _Scanner(data, limits).request()
