"""FU-34: conflicting duplicate local SCIP symbols are diagnosed and dropped."""

from __future__ import annotations

import pytest

from agent_context_platform.indexing.emitter import _line_starts, _range_offsets
from agent_context_platform.indexing.scip import (
    PositionEncoding,
    SourceRange,
    import_scip,
    position_encoding,
)
from agent_context_platform.indexing.scip_pb import scip_pb2

pytestmark = pytest.mark.unit

LOCAL = "local 7"


def _index(*kinds: int) -> bytes:
    index = scip_pb2.Index()
    index.metadata.tool_info.name = "unit"
    index.metadata.tool_info.version = "1"
    doc = index.documents.add()
    doc.relative_path = "src/a.py"
    doc.language = "python"
    doc.text = "abcdef\n"
    for offset, kind in enumerate(kinds):
        occurrence = doc.occurrences.add()
        occurrence.range.extend([0, offset, offset + 1])
        occurrence.symbol = LOCAL
        occurrence.symbol_roles = 1
        info = doc.symbols.add()
        info.symbol = LOCAL
        info.kind = kind
    return index.SerializeToString()


def test_conflicting_local_declarations_are_dropped_and_diagnosed_once() -> None:
    result = import_scip(_index(12, 17, 12), "repo", "a" * 40)

    codes = [item.code for item in result.diagnostics]
    assert codes.count("conflicting_local_declaration") == 1
    document = result.documents[0]
    assert all(symbol.symbol != LOCAL for symbol in document.symbols)


def test_a_repeated_identical_local_declaration_is_not_a_conflict() -> None:
    result = import_scip(_index(12, 12), "repo", "a" * 40)

    assert "conflicting_local_declaration" not in [item.code for item in result.diagnostics]


def test_position_encodings_are_normalized_once_into_a_closed_enum() -> None:
    assert position_encoding(scip_pb2.UTF8CodeUnitOffsetFromLineStart) is PositionEncoding.UTF8
    assert position_encoding(scip_pb2.UTF16CodeUnitOffsetFromLineStart) is PositionEncoding.UTF16
    assert position_encoding(scip_pb2.UTF32CodeUnitOffsetFromLineStart) is PositionEncoding.UTF32
    assert position_encoding(0) is PositionEncoding.UNSPECIFIED
    assert position_encoding(99) is PositionEncoding.UNKNOWN


def test_every_encoding_maps_a_column_after_non_ascii_text_to_the_same_byte() -> None:
    # e-acute (2 bytes, 1 unit), rocket (4 bytes; 2 UTF-16 units; 1 code point), then ``x``.
    source = "\u00e9\U0001f680x = 1\n".encode()
    starts = _line_starts(source)
    columns = {
        PositionEncoding.UTF8: 6,
        PositionEncoding.UTF16: 3,
        PositionEncoding.UTF32: 2,
    }
    for encoding, column in columns.items():
        located = SourceRange(0, column, 0, column + 1)
        assert _range_offsets(source, starts, located, encoding) == (6, 7), encoding


def test_unspecified_is_trusted_only_on_ascii_lines_and_unknown_never() -> None:
    ascii_source = b"abc = 1\n"
    unicode_source = "\u00e9bc = 1\n".encode()
    located = SourceRange(0, 1, 0, 2)
    assert _range_offsets(ascii_source, [0, 8], located, PositionEncoding.UNSPECIFIED) == (1, 2)
    assert _range_offsets(unicode_source, [0, 9], located, PositionEncoding.UNSPECIFIED) is None
    assert _range_offsets(ascii_source, [0, 8], located, PositionEncoding.UNKNOWN) is None


def test_an_unknown_encoding_skips_the_documents_positions_with_a_diagnostic() -> None:
    index = scip_pb2.Index()
    index.metadata.tool_info.name = "unit"
    index.metadata.tool_info.version = "1"
    doc = index.documents.add()
    doc.relative_path = "src/a.py"
    doc.language = "python"
    doc.text = "abcdef\n"
    doc.position_encoding = 99  # type: ignore[assignment]
    occurrence = doc.occurrences.add()
    occurrence.range.extend([0, 0, 1])
    occurrence.symbol = "scip-python python pkg 1.0 a/f()."
    occurrence.symbol_roles = 1

    result = import_scip(index.SerializeToString(), "repo", "a" * 40)

    assert result.documents[0].position_encoding is PositionEncoding.UNKNOWN
    assert result.documents[0].occurrences == ()
    assert [d.code for d in result.diagnostics] == ["unknown_position_encoding"]


def test_a_position_inside_a_multi_unit_character_is_rejected_in_every_encoding() -> None:
    # The rocket is 4 UTF-8 bytes and 2 UTF-16 units; an offset in the middle names no boundary.
    source = "a\U0001f680b\n".encode()
    starts = _line_starts(source)

    def offsets(encoding: PositionEncoding, column: int) -> tuple[int, int] | None:
        return _range_offsets(source, starts, SourceRange(0, column, 0, column), encoding)

    assert offsets(PositionEncoding.UTF16, 2) is None  # between the surrogate halves
    assert offsets(PositionEncoding.UTF16, 1) == (1, 1)
    assert offsets(PositionEncoding.UTF16, 3) == (5, 5)
    for column in (2, 3, 4):  # continuation bytes
        assert offsets(PositionEncoding.UTF8, column) is None
    assert offsets(PositionEncoding.UTF8, 1) == (1, 1)
    assert offsets(PositionEncoding.UTF8, 5) == (5, 5)
    assert offsets(PositionEncoding.UTF16, 6) is None  # past the end


# --- lookups stay linear (a 1 MB document can hold tens of thousands of occurrences) ---------


def _brute(spans: list[tuple[int, int, int]], begin: int, end: int) -> int | None:
    inside = [(e - s, position, item) for position, (s, e, item) in enumerate(spans)]
    fitting = [
        (size, position, item)
        for (size, position, item), (s, e, _) in zip(inside, spans, strict=True)
        if s <= begin and end <= e
    ]
    return min(fitting)[2] if fitting else None


def test_intervals_agree_with_a_scan_and_keep_the_first_of_equal_spans() -> None:
    import random

    from agent_context_platform.indexing.emitter import _Intervals

    rng = random.Random(7)
    spans: list[tuple[int, int, int]] = []

    def nest(start: int, end: int, depth: int) -> None:
        spans.append((start, end, len(spans)))
        if depth == 0 or end - start < 8:
            return
        cursor = start + 1
        while cursor + 4 < end and rng.random() < 0.9:
            stop = min(end - 1, cursor + rng.randint(3, (end - start) // 2))
            nest(cursor, stop, depth - 1)
            if rng.random() < 0.2:
                spans.append((cursor, stop, len(spans)))  # an equal span
            cursor = stop + rng.randint(0, 2)

    nest(0, 4000, 5)
    index = _Intervals(spans)
    for _ in range(3000):
        begin = rng.randint(0, 4000)
        end = begin + rng.randint(0, 40)
        assert index.innermost(begin, end) == _brute(spans, begin, end)


def test_symbol_lookups_are_not_quadratic_in_symbols_or_occurrences() -> None:
    import time

    from agent_context_platform.indexing.emitter import _DefinitionOwners, _Intervals
    from agent_context_platform.indexing.tree_sitter.base import ParsedFile, ParsedSymbol

    count = 20_000  # 20k symbols, all named ``get``, and 20k occurrences to place
    digest = "1" * 64
    symbols = tuple(
        ParsedSymbol(
            ref=str(number),
            language="python",
            qualified_name="m.get",
            kind="function",
            start_byte=number * 4,
            end_byte=number * 4 + 3,
            signature="",
            signature_digest=digest,
            semantic_fingerprint=digest,
            evidence_kind="tree_sitter",
        )
        for number in range(count)
    )
    parsed = ParsedFile(path="m.py", language="python", parser_fingerprint=digest, symbols=symbols)
    source = b"get\n" * count
    begun = time.monotonic()
    owners = _DefinitionOwners.build(parsed)
    found = [owners.owner(source, (number * 4, number * 4 + 3)) for number in range(count)]
    spans = _Intervals((number * 4, number * 4 + 3, number) for number in range(count))
    placed = [spans.innermost(number * 4 + 1, number * 4 + 2) for number in range(count)]
    elapsed = time.monotonic() - begun

    assert found == list(range(count)) and placed == list(range(count))
    assert elapsed < 10  # a scan per lookup takes minutes here


def test_a_megabyte_line_is_measured_once_per_line_not_once_per_position() -> None:
    import time

    source = ("é\U0001f680" * 100_000 + "x\n").encode()  # one ~500 KB line, non-ASCII
    starts = _line_starts(source)
    units = 3 * 100_000  # UTF-16 units before ``x``
    cache: dict = {}
    begun = time.monotonic()
    for step in range(0, units, 3):
        for encoding, column in (
            (PositionEncoding.UTF16, step + 1),
            (PositionEncoding.UTF32, step // 3 * 2 + 1),
        ):
            located = SourceRange(0, column, 0, column)
            assert _range_offsets(source, starts, located, encoding, cache) is not None
    assert time.monotonic() - begun < 10  # re-decoding the line per position takes minutes
    assert len(cache) == 1
    x = SourceRange(0, units, 0, units + 1)
    assert _range_offsets(source, starts, x, PositionEncoding.UTF16, cache) == (
        len(source) - 2,
        len(source) - 1,
    )
