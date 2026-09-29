from __future__ import annotations

import time
import tracemalloc
import uuid
from pathlib import Path

import pytest

from agent_context_platform.indexing import identity
from agent_context_platform.indexing.scip import (
    DEFAULT_LIMITS,
    SCIP_CONFIDENCE,
    ScipImportError,
    ScipLimits,
    SemanticIndex,
    SourceRange,
    evidence_outranks,
    import_scip,
    is_valid_symbol,
)
from agent_context_platform.indexing.scip_pb import scip_pb2

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "scip"
REPO = "repo-036"
COMMIT = "a" * 40
CIRCLE = "scip-python python fixturepkg 0.0.1 `pkg.shapes`/Circle#"
DESCRIBE = "scip-python python fixturepkg 0.0.1 `pkg.shapes`/describe()."
PI = "scip-python python python-stdlib 3.11 math/pi.pi."
GLOBAL = "scip-python python pkg 1.0 a/f()."


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def real(**kwargs: object) -> SemanticIndex:
    return import_scip(fixture("scip-python-basic.scip"), REPO, COMMIT, **kwargs)  # type: ignore[arg-type]


def build(*documents: scip_pb2.Document, external: list[scip_pb2.SymbolInformation] | None = None):
    index = scip_pb2.Index()
    index.metadata.tool_info.name = "unit"
    index.metadata.tool_info.version = "1"
    index.documents.extend(documents)
    index.external_symbols.extend(external or [])
    return index


def document(path: str = "src/a.py", text: str = "") -> scip_pb2.Document:
    doc = scip_pb2.Document()
    doc.relative_path = path
    doc.language = "python"
    doc.text = text
    return doc


def add(
    doc: scip_pb2.Document, rng: list[int], symbol: str = GLOBAL, roles: int = 0
) -> scip_pb2.Occurrence:
    occurrence = doc.occurrences.add()
    occurrence.range.extend(rng)
    occurrence.symbol = symbol
    occurrence.symbol_roles = roles
    return occurrence


def run(index: scip_pb2.Index, **kwargs: object) -> SemanticIndex:
    return import_scip(index.SerializeToString(), REPO, COMMIT, **kwargs)  # type: ignore[arg-type]


def codes(result: SemanticIndex) -> list[str]:
    return [item.code for item in result.diagnostics]


def test_records_run_metadata_without_trusting_project_root() -> None:
    result = real(toolchain="node v24.18.0")

    assert result.repository_id == REPO
    assert result.commit == COMMIT
    assert result.run.tool_name == "scip-python"
    assert result.run.tool_version == "0.6.6"
    assert result.run.tool_arguments == ()
    assert result.run.toolchain == "node v24.18.0"
    assert result.run.project_root_name == "py"
    assert "/tmp" not in repr(result.run)
    assert real().run.toolchain is None


def test_definitions_and_references_from_real_index() -> None:
    result = real()
    shapes = next(doc for doc in result.documents if doc.path == "pkg/shapes.py")

    definition = next(o for o in shapes.occurrences if o.symbol == CIRCLE and o.is_definition)
    assert (definition.range.start_line, definition.range.start_character) == (4, 6)
    assert (definition.range.end_line, definition.range.end_character) == (4, 12)
    assert definition.enclosing_range is not None
    assert definition.enclosing_range.end_line == 12

    main = next(doc for doc in result.documents if doc.path == "pkg/main.py")
    reference = next(o for o in main.occurrences if o.symbol == DESCRIBE and not o.is_definition)
    assert reference.symbol_id == identity.symbol_logical_id(
        REPO,
        scip_symbol=DESCRIBE,
        language="python",
        file_logical_id=uuid.UUID(int=0),
        qualified_name="",
        kind="",
    )
    assert (reference.evidence_kind, reference.confidence) == ("scip", SCIP_CONFIDENCE)
    assert set(codes(result)) == {"local_symbol_without_file_identity"}


def test_global_symbol_identity_is_exactly_the_scip_string() -> None:
    result = real()
    shapes = next(doc for doc in result.documents if doc.path == "pkg/shapes.py")
    symbol = next(s for s in shapes.symbols if s.symbol == CIRCLE)

    expected = identity._derive(REPO, "symbol_scip", CIRCLE)
    assert symbol.symbol_id == expected
    # No normalization: a near-identical spelling is a different identity.
    assert symbol.symbol_id != identity._derive(REPO, "symbol_scip", CIRCLE.strip("#"))
    assert symbol.revision_id != symbol.symbol_id
    assert symbol.kind == "UnspecifiedKind"


def test_documentation_is_preserved() -> None:
    shapes = next(doc for doc in real().documents if doc.path == "pkg/shapes.py")
    circle = next(s for s in shapes.symbols if s.symbol == CIRCLE)

    assert circle.documentation == ("```python\nclass Circle:\n```", "A circle.")


def test_external_symbols_are_kept_with_identity() -> None:
    result = real()
    external = {s.symbol: s for s in result.external_symbols}

    assert PI in external
    assert external[PI].documentation == ("```python\n(variable) pi: float\n```",)
    assert external[PI].symbol_id == identity._derive(REPO, "symbol_scip", PI)
    assert external[PI].document_path is None


def test_local_symbols_need_a_file_identity() -> None:
    without = real()
    main = next(doc for doc in without.documents if doc.path == "pkg/main.py")
    assert all(not o.symbol.startswith("local ") for o in main.occurrences)
    assert codes(without).count("local_symbol_without_file_identity") == 3

    file_id = uuid.UUID(int=7)
    with_ids = real(file_logical_ids={"pkg/main.py": file_id})
    main = next(doc for doc in with_ids.documents if doc.path == "pkg/main.py")
    local = [o for o in main.occurrences if o.symbol == "local 0"]
    assert len(local) == 2
    assert main.file_logical_id == file_id
    assert local[0].symbol_id == identity.symbol_fallback_id(
        REPO, "", file_id, "local 0", "UnspecifiedKind"
    )
    assert local[0].symbol_id == local[1].symbol_id
    assert codes(with_ids).count("local_symbol_without_file_identity") == 0


def test_local_symbol_ids_differ_per_file() -> None:
    first, second = document("a.py"), document("b.py")
    for doc in (first, second):
        add(doc, [0, 0, 1], symbol="local 1", roles=1)
    result = run(
        build(first, second),
        file_logical_ids={"a.py": uuid.UUID(int=1), "b.py": uuid.UUID(int=2)},
    )
    ids = {doc.path: doc.occurrences[0].symbol_id for doc in result.documents}
    assert ids["a.py"] != ids["b.py"]


def test_relationships_carry_scip_evidence() -> None:
    doc = document()
    info = doc.symbols.add()
    info.symbol = GLOBAL
    info.display_name = "f"
    info.kind = scip_pb2.SymbolInformation.Function
    info.signature_documentation.language = "python"
    info.signature_documentation.text = "def f()"
    relationship = info.relationships.add()
    relationship.symbol = "scip-python python pkg 1.0 base/f()."
    relationship.is_implementation = True
    relationship.is_reference = True
    bad = info.relationships.add()
    bad.symbol = ""
    result = run(build(doc))

    symbol = result.documents[0].symbols[0]
    assert (symbol.kind, symbol.display_name, symbol.signature_text) == (
        "Function",
        "f",
        "def f()",
    )
    (edge,) = symbol.relationships
    assert edge.source_symbol == GLOBAL
    assert edge.target_symbol == "scip-python python pkg 1.0 base/f()."
    assert edge.kinds == ("reference", "implementation")
    assert (edge.evidence_kind, edge.confidence) == ("scip", 1.0)
    assert codes(result) == ["invalid_symbol"]


def test_signature_change_makes_a_new_symbol_revision() -> None:
    def revision(text: str) -> uuid.UUID:
        doc = document()
        info = doc.symbols.add()
        info.symbol = GLOBAL
        info.signature_documentation.text = text
        return run(build(doc)).documents[0].symbols[0].revision_id

    assert revision("def f()") == revision("def f()")
    assert revision("def f()") != revision("def f(x)")


def test_invalid_ranges_are_discarded_with_diagnostics_not_edges() -> None:
    result = import_scip(fixture("handcrafted-invalid.scip"), REPO, COMMIT)

    assert [d.path for d in result.documents] == ["src/a.py"]
    kept = result.documents[0].occurrences
    assert [(o.range.start_line, o.range.start_character) for o in kept] == [(0, 4), (1, 11)]
    assert [(d.code, d.occurrence_index) for d in result.diagnostics] == [
        ("out_of_document_range", 2),
        ("out_of_document_range", 3),
        ("invalid_range", 4),
        ("invalid_range", 5),
        ("invalid_range", 6),
        ("invalid_symbol", 7),
        ("unsafe_path", None),
    ]
    assert result.diagnostics[-1].path is None


def test_range_forms_and_document_bounds() -> None:
    doc = document(text="héllo\nab")
    doc.position_encoding = scip_pb2.UTF8CodeUnitOffsetFromLineStart
    single = doc.occurrences.add()
    single.symbol = GLOBAL
    single.single_line_range.line = 0
    single.single_line_range.start_character = 0
    single.single_line_range.end_character = 6  # "héllo" is 6 UTF-8 bytes
    multi = doc.occurrences.add()
    multi.symbol = GLOBAL
    multi.multi_line_range.start_line = 0
    multi.multi_line_range.start_character = 1
    multi.multi_line_range.end_line = 1
    multi.multi_line_range.end_character = 2
    over = doc.occurrences.add()
    over.symbol = GLOBAL
    over.single_line_range.line = 0
    over.single_line_range.end_character = 7
    zero = add(doc, [1, 1, 1])

    result = run(build(doc))
    ranges = [
        (o.range.start_line, o.range.start_character, o.range.end_line, o.range.end_character)
        for o in result.documents[0].occurrences
    ]
    assert ranges == [(0, 0, 0, 6), (0, 1, 1, 2), (1, 1, 1, 1)]
    assert codes(result) == ["out_of_document_range"]
    assert zero.range == [1, 1, 1]


def test_position_encoding_decides_column_units() -> None:
    def imported(encoding: int, end: int) -> SemanticIndex:
        doc = document(text="a\U0001f680b")  # rocket: 4 UTF-8 bytes, 2 UTF-16 units, 1 point
        doc.position_encoding = encoding
        add(doc, [0, 0, end])
        return run(build(doc))

    assert codes(imported(scip_pb2.UTF8CodeUnitOffsetFromLineStart, 6)) == []
    assert codes(imported(scip_pb2.UTF8CodeUnitOffsetFromLineStart, 7)) == ["out_of_document_range"]
    assert codes(imported(scip_pb2.UTF16CodeUnitOffsetFromLineStart, 4)) == []
    assert codes(imported(scip_pb2.UTF16CodeUnitOffsetFromLineStart, 5)) == [
        "out_of_document_range"
    ]
    assert codes(imported(scip_pb2.UTF32CodeUnitOffsetFromLineStart, 3)) == []
    assert codes(imported(scip_pb2.UTF32CodeUnitOffsetFromLineStart, 4)) == [
        "out_of_document_range"
    ]
    # Unspecified encoding: the column unit is unknown, so only the line is checked.
    assert codes(imported(scip_pb2.UnspecifiedPositionEncoding, 99)) == []


def test_caller_sources_override_document_text() -> None:
    doc = document(text="a much longer line than the caller's source")
    doc.position_encoding = scip_pb2.UTF32CodeUnitOffsetFromLineStart
    add(doc, [0, 0, 10])

    assert codes(run(build(doc))) == []
    assert codes(run(build(doc), sources={"src/a.py": "abc"})) == ["out_of_document_range"]


def test_invalid_roles_and_duplicate_documents_and_bad_locals() -> None:
    first, second = document(), document()
    add(first, [0, 0, 1], roles=-1)
    add(first, [0, 0, 1], symbol="local not valid!", roles=1)
    add(first, [0, 0, 1], symbol="bad\x01symbol")
    add(first, [0, 0, 1], symbol="scip-python python pkg 1.0 a/g().", roles=0x1 | 0x8)
    result = run(build(first, second))

    assert len(result.documents) == 1
    assert [o.roles for o in result.documents[0].occurrences] == [0x9]
    assert codes(result) == [
        "invalid_roles",
        "invalid_symbol",
        "invalid_symbol",
        "duplicate_document",
    ]


@pytest.mark.parametrize(
    "name",
    ["malformed-truncated.scip", "malformed-garbage.scip", "malformed-bad-utf8.scip"],
)
def test_malformed_bytes_fail_closed_with_content_free_error(name: str) -> None:
    with pytest.raises(ScipImportError) as caught:
        import_scip(fixture(name), REPO, COMMIT)

    assert caught.value.reason == "malformed"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__
    assert str(caught.value) == "malformed"


def test_missing_metadata_fails_closed() -> None:
    with pytest.raises(ScipImportError) as caught:
        import_scip(scip_pb2.Index().SerializeToString(), REPO, COMMIT)
    assert caught.value.reason == "missing_metadata"


@pytest.mark.parametrize(
    ("limits", "reason"),
    [
        (ScipLimits(max_index_bytes=10), "index_too_large"),
        (ScipLimits(max_documents=2), "too_many_documents"),
        (ScipLimits(max_occurrences_per_document=3), "too_many_occurrences"),
        (ScipLimits(max_symbol_length=20), "symbol_too_long"),
        (ScipLimits(max_symbols_per_document=2), "too_many_symbols"),
        (ScipLimits(max_external_symbols=1), "too_many_external_symbols"),
        (ScipLimits(max_relationships_per_symbol=0), "too_many_relationships"),
        (ScipLimits(max_tool_arguments=0), "tool_info_too_large"),
    ],
)
def test_limits_fail_closed(limits: ScipLimits, reason: str) -> None:
    index = scip_pb2.Index()
    index.ParseFromString(fixture("scip-python-basic.scip"))
    index.metadata.tool_info.arguments.append("index")
    info = index.documents[0].symbols[0]
    info.relationships.add().symbol = "scip-python python pkg 1.0 a/f()."
    with pytest.raises(ScipImportError) as caught:
        import_scip(index.SerializeToString(), REPO, COMMIT, limits=limits)

    assert caught.value.reason == reason
    assert str(caught.value) == reason
    assert DEFAULT_LIMITS.max_index_bytes >= 1024 * 1024


def test_size_limit_is_checked_before_parsing() -> None:
    with pytest.raises(ScipImportError) as caught:
        import_scip(b"\xff" * 11, REPO, COMMIT, limits=ScipLimits(max_index_bytes=10))
    assert caught.value.reason == "index_too_large"


def test_rejects_bad_commit_and_repository() -> None:
    data = fixture("scip-python-basic.scip")
    with pytest.raises(ValueError, match="commit"):
        import_scip(data, REPO, "HEAD")
    with pytest.raises(ValueError, match="repository_id"):
        import_scip(data, "", COMMIT)


def test_scip_outranks_a_conflicting_tree_sitter_candidate() -> None:
    # SCIP resolves the call to one target, Tree-sitter guesses another with high confidence.
    assert evidence_outranks("scip", SCIP_CONFIDENCE, "tree_sitter", 0.95)
    assert not evidence_outranks("tree_sitter", 0.95, "scip", SCIP_CONFIDENCE)
    # Kind dominates: even a weak SCIP claim beats a strong heuristic one.
    assert evidence_outranks("scip", 0.5, "tree_sitter", 0.99)
    assert evidence_outranks("tree_sitter", 0.6, "llm_inference", 0.9)
    assert evidence_outranks("agent", 0.9, "llm_inference", 0.9)
    # Same kind: higher confidence wins; ties are not outranked.
    assert evidence_outranks("tree_sitter", 0.7, "tree_sitter", 0.6)
    assert not evidence_outranks("tree_sitter", 0.6, "tree_sitter", 0.6)


def test_ranking_rejects_kinds_outside_semantic_evidence() -> None:
    with pytest.raises(ValueError, match="evidence kind"):
        evidence_outranks("git", 1.0, "scip", 1.0)
    with pytest.raises(ValueError, match="confidence"):
        evidence_outranks("scip", 1.5, "tree_sitter", 0.5)


def test_symbols_of_the_real_index_follow_the_grammar() -> None:
    index = scip_pb2.Index()
    index.ParseFromString(fixture("scip-python-basic.scip"))
    symbols = {o.symbol for d in index.documents for o in d.occurrences if o.symbol}
    symbols |= {i.symbol for d in index.documents for i in d.symbols}
    symbols |= {i.symbol for i in index.external_symbols}

    assert len(symbols) > 15
    assert all(is_valid_symbol(symbol) for symbol in symbols)


@pytest.mark.parametrize(
    "symbol",
    [
        "scip-python python pkg 1.0 a/f().",
        "scip-python python pkg 1.0 `a b`/C#m(x).(p)",
        "scheme  with  spaces mgr  x . 1 a/b.[T]",
        "s . . . `a``b`.",
        "s m n v ns/Type#term.meta:macro!",
        "local 12",
    ],
)
def test_valid_symbols(symbol: str) -> None:
    assert is_valid_symbol(symbol)


@pytest.mark.parametrize(
    "symbol",
    [
        "garbage",
        "",
        "s m n v ",  # missing descriptor
        "s m n",
        "local",
        "local a b",
        "localx m n v a/",  # scheme must not start with local
        "s m n v `a`b`/",  # bad escape: lone backtick inside an escaped name
        "s m n v `unterminated/",
        "s m n v ``/",  # empty escaped name
        "s m n v f(x/",  # unbalanced parentheses
        "s m n v f(.",
        "s m n v (x.",
        "s m n v [T.",
        "s m n v f()",  # method needs the trailing dot
        "s m n v name",  # no descriptor suffix
        "s m n v a/\x00",
    ],
)
def test_invalid_symbols(symbol: str) -> None:
    assert not is_valid_symbol(symbol)


HUGE = 200_000
HUGE_SYMBOLS = {
    "open_parens": "s m n v " + "(" * HUGE,
    "backticks": "s m n v " + "`" * (HUGE + 1),
    "methods": "s m n v " + "a(" * HUGE,
    "spaces": " " * HUGE,
    "scheme_spaces": "s" + " " * HUGE,
    "valid_long": "s m n v " + "a/" * HUGE + "b.",
    "escaped_pairs": "s m n v `" + "``" * HUGE + "`.",
}


@pytest.mark.parametrize("name", list(HUGE_SYMBOLS))
def test_symbol_parsing_is_bounded_in_time(name: str) -> None:
    # A backtracking parser would need minutes here; the generous bound keeps slow CI green.
    started = time.perf_counter()
    is_valid_symbol(HUGE_SYMBOLS[name])
    assert time.perf_counter() - started < 10.0


def test_import_drops_ungrammatical_global_symbols() -> None:
    doc = document(text="abc")
    add(doc, [0, 0, 1], symbol="garbage", roles=1)
    add(doc, [0, 0, 1], symbol="s m n v f(x/")
    info = doc.symbols.add()
    info.symbol = "garbage"
    related = doc.symbols.add()
    related.symbol = GLOBAL
    related.relationships.add().symbol = "garbage"
    result = run(build(doc))

    assert result.documents[0].occurrences == ()
    assert [s.symbol for s in result.documents[0].symbols] == [GLOBAL]
    assert result.documents[0].symbols[0].relationships == ()
    assert codes(result) == ["invalid_symbol"] * 4


def test_local_relationship_target_uses_the_declared_kind() -> None:
    doc = document(text="abc")
    add(doc, [0, 0, 1], symbol="local 1", roles=1)
    local = doc.symbols.add()
    local.symbol = "local 1"
    local.kind = scip_pb2.SymbolInformation.Class
    owner = doc.symbols.add()
    owner.symbol = GLOBAL
    owner.relationships.add().symbol = "local 1"
    file_ids = {"src/a.py": uuid.UUID(int=9)}
    result = run(build(doc), file_logical_ids=file_ids)

    kept = result.documents[0]
    by_symbol = {s.symbol: s for s in kept.symbols}
    (edge,) = by_symbol[GLOBAL].relationships
    node_id = by_symbol["local 1"].symbol_id
    assert by_symbol["local 1"].kind == "Class"
    assert edge.target_id == node_id == kept.occurrences[0].symbol_id
    assert node_id == identity.symbol_fallback_id(
        REPO, "python", file_ids["src/a.py"], "local 1", "Class"
    )


def test_empty_source_text_has_no_valid_range() -> None:
    doc = document(text="unused")
    add(doc, [0, 0, 0])
    add(doc, [0, 0, 1])
    result = run(build(doc), sources={"src/a.py": ""})

    assert result.documents[0].occurrences == ()
    assert codes(result) == ["out_of_document_range"] * 2
    # No text at all (neither supplied nor in the index) means structure only.
    absent = document(text="")
    add(absent, [0, 0, 0])
    assert codes(run(build(absent))) == []


def _index_with(*fields: bytes) -> bytes:
    metadata = scip_pb2.Metadata()
    metadata.tool_info.name = "unit"
    return (
        b"\x0a"
        + bytes([len(metadata.SerializeToString())])
        + metadata.SerializeToString()
        + b"".join(fields)
    )


def _peak_refusal(data: bytes, reason: str) -> tuple[float, int]:
    tracemalloc.start()
    started = time.perf_counter()
    try:
        with pytest.raises(ScipImportError) as caught:
            import_scip(data, REPO, COMMIT)
        elapsed = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert caught.value.reason == reason
    return elapsed, peak


def test_ten_million_empty_documents_are_refused_without_materializing() -> None:
    data = _index_with(b"\x12\x00" * 10_000_000)
    elapsed, peak = _peak_refusal(data, "too_many_documents")

    assert elapsed < 10.0
    assert peak < 20 * 1024 * 1024


def test_millions_of_empty_occurrences_are_refused_without_materializing() -> None:
    document_bytes = b"\x0a\x01a" + b"\x12\x00" * 3_000_000
    data = _index_with(b"\x12" + _length(document_bytes) + document_bytes)
    elapsed, peak = _peak_refusal(data, "too_many_occurrences")

    assert elapsed < 10.0
    assert peak < 20 * 1024 * 1024


def test_total_occurrences_and_symbols_across_documents_are_capped() -> None:
    body = b"\x0a\x01a" + b"\x12\x00" * 600
    data = _index_with((b"\x12" + _length(body) + body) * 2)
    with pytest.raises(ScipImportError) as caught:
        import_scip(data, REPO, COMMIT, limits=ScipLimits(max_total_occurrences=1_000))
    assert caught.value.reason == "too_many_occurrences"

    symbols = b"\x0a\x01a" + b"\x1a\x00" * 600
    data = _index_with((b"\x12" + _length(symbols) + symbols) * 2)
    with pytest.raises(ScipImportError) as caught:
        import_scip(data, REPO, COMMIT, limits=ScipLimits(max_total_symbols=1_000))
    assert caught.value.reason == "too_many_symbols"


def test_nested_repeats_and_truncation_are_refused() -> None:
    occurrence = b"\x32\x00" * 1_500  # diagnostics
    document_bytes = b"\x12" + _length(occurrence) + occurrence
    data = _index_with(b"\x12" + _length(document_bytes) + document_bytes)
    _peak_refusal(data, "too_many_repeated_items")
    _peak_refusal(_index_with(b"\x12\x05ab"), "malformed")  # length overruns the buffer
    _peak_refusal(_index_with(b"\x1b"), "malformed")  # group wire type
    _peak_refusal(_index_with(b"\x80"), "malformed")  # truncated varint


def _length(data: bytes) -> bytes:
    out = bytearray()
    size = len(data)
    while size >= 0x80:
        out.append(size & 0x7F | 0x80)
        size >>= 7
    out.append(size)
    return bytes(out)


def test_long_line_ranges_use_cached_widths() -> None:
    doc = document(text="x" * 2_000_000 + "\n")
    doc.position_encoding = scip_pb2.UTF8CodeUnitOffsetFromLineStart
    for column in range(10_000):
        add(doc, [0, column, column + 1])
    started = time.perf_counter()
    result = run(build(doc))

    assert len(result.documents[0].occurrences) == 10_000
    assert time.perf_counter() - started < 10.0


def test_enclosing_range_must_contain_the_occurrence() -> None:
    doc = document(text="0123456789\nabcdefghij\n")
    doc.position_encoding = scip_pb2.UTF32CodeUnitOffsetFromLineStart
    contained = add(doc, [0, 2, 4])
    contained.enclosing_range.extend([0, 0, 1, 5])
    disjoint = add(doc, [0, 2, 4])
    disjoint.enclosing_range.extend([1, 0, 1, 3])
    partial = add(doc, [0, 2, 4])
    partial.enclosing_range.extend([0, 3, 0, 9])
    outside = add(doc, [0, 2, 4])
    outside.enclosing_range.extend([0, 0, 9, 1])
    garbled = add(doc, [0, 2, 4])
    garbled.enclosing_range.extend([0, 0])
    typed = add(doc, [0, 2, 4])
    typed.multi_line_enclosing_range.end_line = 1
    typed.multi_line_enclosing_range.end_character = 2
    result = run(build(doc))

    kept = [o.enclosing_range for o in result.documents[0].occurrences]
    assert kept[0] == SourceRange(0, 0, 1, 5)
    assert kept[1:5] == [None, None, None, None]
    assert kept[5] == SourceRange(0, 0, 1, 2)
    assert [(d.code, d.occurrence_index) for d in result.diagnostics] == [
        ("invalid_range", 1),
        ("invalid_range", 2),
        ("out_of_document_range", 3),
        ("invalid_range", 4),
    ]
    assert len(result.documents[0].occurrences) == 6
