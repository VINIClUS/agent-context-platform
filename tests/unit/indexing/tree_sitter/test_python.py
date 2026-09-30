"""Python structural adapter: golden output, fingerprints, robustness, real sandbox path."""

from __future__ import annotations

import json
import os
import resource
import subprocess
import sys
import time
from importlib import metadata
from pathlib import Path

import pytest

from agent_context_platform.indexing.tree_sitter import _common, landlock, python, runner
from agent_context_platform.indexing.tree_sitter._common import normalize_module
from agent_context_platform.indexing.tree_sitter.base import (
    MAX_SOURCE_BYTES,
    ParsedFile,
    ParsedModule,
    ParseRequest,
    SourceFile,
    StructuralAdapter,
    parser_fingerprint,
    validate_module,
)
from agent_context_platform.indexing.tree_sitter.python import (
    ADAPTER_NAME,
    ADAPTER_VERSION,
    FINGERPRINT,
    GRAMMAR_VERSIONS,
    parse_python,
    python_adapter,
)
from agent_context_platform.indexing.tree_sitter.runner import Limits

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).with_name("fixtures") / "python"


def source(path: str, content: bytes | str) -> SourceFile:
    raw = content.encode() if isinstance(content, str) else content
    return SourceFile.from_bytes(path, "python", raw)


def request(*files: SourceFile) -> ParseRequest:
    return ParseRequest(files=files)


def fixture_request() -> ParseRequest:
    paths = sorted(FIXTURES.rglob("*.py"))
    return request(*(source(str(p.relative_to(FIXTURES)), p.read_bytes()) for p in paths))


def in_process(req: ParseRequest) -> ParsedModule:
    return validate_module(req, parse_python(req), expected_fingerprint=FINGERPRINT)


def symbols_of(content: bytes | str, path: str = "pkg/mod.py") -> dict[str, ParsedFile]:
    return {item.path: item for item in in_process(request(source(path, content))).files}


def diagnostics_of(parsed: ParsedFile) -> dict[str, int]:
    return {item.code: item.count for item in parsed.diagnostics}


def assert_intact_or_reported(
    file: ParsedFile, symbols: int | None = None, references: int | None = None
) -> None:
    """Load-independent: the file has its structure, or ``file_degraded`` plus a reason."""
    found = diagnostics_of(file)
    if "file_degraded" in found:
        assert file.symbols == () and file.references == ()
        assert found.keys() & {"work_budget_exceeded", "symbols_dropped"}
        return
    if symbols is not None:
        assert len(file.symbols) == symbols
    if references is not None:
        assert len(file.references) == references


DEGRADED_BY_BUDGET = {"file_degraded": 1, "work_budget_exceeded": 1}


def refs_of(content: bytes | str, path: str = "pkg/mod.py") -> list[tuple[object, ...]]:
    """(source name, kind, level, qualifier, target, target text, qualifier text), sorted."""
    raw = content.encode() if isinstance(content, str) else content
    parsed = symbols_of(raw, path)[path]
    names = {item.ref: item.qualified_name for item in parsed.symbols}
    return ordered(
        (
            None if item.source is None else names[item.source],
            item.kind,
            item.relative_level,
            item.qualifier,
            item.target_name,
            raw[item.start_byte : item.end_byte].decode(),
            None
            if item.qualifier is None
            else raw[item.qualifier_start_byte : item.qualifier_end_byte].decode(),  # type: ignore[index]
        )
        for item in parsed.references
    )


def ordered(items: object) -> list[tuple[object, ...]]:
    return sorted(items, key=repr)  # type: ignore[call-overload]


def edges_of(content: bytes | str) -> set[tuple[str, str, str]]:
    parsed = symbols_of(content)["pkg/mod.py"]
    names = {item.ref: item.qualified_name for item in parsed.symbols}
    return {(names[r.source_ref], names[r.target_ref], r.kind) for r in parsed.relations}


def names_of(content: bytes | str, path: str = "pkg/mod.py") -> set[str]:
    return {item.qualified_name for item in symbols_of(content, path)[path].symbols}


def digests(content: str, name: str) -> tuple[str, str]:
    parsed = symbols_of(content)["pkg/mod.py"]
    (item,) = (s for s in parsed.symbols if s.qualified_name == f"pkg.mod.{name}")
    return item.signature_digest, item.semantic_fingerprint


# --- golden output ------------------------------------------------------------------------


def test_golden_through_the_real_sandboxed_subprocess() -> None:
    expected = json.loads((FIXTURES / "expected.json").read_text())
    module = python_adapter().parse(fixture_request())
    assert normalize_module(module) == expected


def test_in_process_output_equals_the_sandboxed_output() -> None:
    req = fixture_request()
    assert normalize_module(in_process(req)) == normalize_module(python_adapter().parse(req))


def test_adapter_contract() -> None:
    adapter = python_adapter()
    assert isinstance(adapter, StructuralAdapter)
    assert adapter.language == "python"
    assert adapter.fingerprint == parser_fingerprint(
        ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS
    )
    req = fixture_request()
    module = adapter.parse(req)
    assert [item.path for item in module.files] == [item.path for item in req.files]
    for parsed in module.files:
        assert parsed.parser_fingerprint == adapter.fingerprint
        assert [item.ref for item in parsed.symbols] == [str(n) for n in range(len(parsed.symbols))]
        assert {item.evidence_kind for item in parsed.symbols} <= {"tree_sitter"}
        assert {item.evidence_kind for item in parsed.relations} <= {"tree_sitter"}
        assert {item.kind for item in parsed.relations} <= {"calls", "inherits"}


def test_grammar_versions_match_the_installed_distributions() -> None:
    for name, version in GRAMMAR_VERSIONS.items():
        assert metadata.version(name) == version
    assert parser_fingerprint(ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS) == FINGERPRINT


def test_module_and_package_names_come_from_the_path() -> None:
    assert _common.module_name("pkg/sub/mod.py") == "pkg.sub.mod"
    assert _common.module_name("pkg/__init__.py") == "pkg"
    assert _common.module_name("__init__.py") == ""
    assert _common.module_name("src/my-pkg/mod.py") == "mod"
    assert _common.module_name("a/2024/mod.py") == "mod"
    assert _common.module_name("pkg/my-mod.py") == ""
    assert _common.module_name(".github/x.py") == "x"
    assert _common.module_name("pkg/m\u00f3dulo.py") == "pkg.m\u00f3dulo"
    assert _common.module_name("caf\u00e9/\u4e2d\u6587/__init__.py") == "caf\u00e9.\u4e2d\u6587"


def test_calls_are_same_file_heuristics_and_imports_yield_no_relation() -> None:
    parsed = symbols_of(
        "import os\nfrom x import y\n\n\ndef a():\n    os.f()\n    y()\n    b()\n\n\ndef b():\n    pass\n"
    )
    (relation,) = parsed["pkg/mod.py"].relations
    names = {s.ref: s.qualified_name for s in parsed["pkg/mod.py"].symbols}
    assert (names[relation.source_ref], names[relation.target_ref], relation.kind) == (
        "pkg.mod.a",
        "pkg.mod.b",
        "calls",
    )
    assert relation.evidence_kind == "tree_sitter"


def test_class_scope_names_are_not_visible_from_methods() -> None:
    code = "def g():\n    pass\n\n\nclass C:\n    def g(self):\n        pass\n\n    def m(self):\n        g()\n        self.g()\n"
    parsed = symbols_of(code)["pkg/mod.py"]
    names = {s.ref: (s.qualified_name, s.kind) for s in parsed.symbols}
    edges = sorted((names[r.source_ref][0], names[r.target_ref][0]) for r in parsed.relations)
    assert edges == [("pkg.mod.C.m", "pkg.mod.C.g"), ("pkg.mod.C.m", "pkg.mod.g")]


# --- fingerprints (SymbolRevision identity) -------------------------------------------------

PLAIN = 'def f(a, b, c=1):\n    """Doc."""\n    x = a + b  # note\n    return "s" + str(c)\n'
BLACK = (
    "def f(\n    a,\n    b,\n    c=1,\n):\n    '''Doc.'''\n\n    x = a + b\n"
    "    # another note\n    return 's' + str(c)\n"
)


def test_reformatting_keeps_both_digests() -> None:
    assert digests(PLAIN, "f") == digests(BLACK, "f")


def test_changed_docstring_or_comment_keeps_the_revision() -> None:
    other = PLAIN.replace("Doc.", "A different doc.").replace("# note", "# other")
    assert digests(PLAIN, "f") == digests(other, "f")


def test_changed_body_changes_the_fingerprint_not_the_signature() -> None:
    signature, body = digests(PLAIN, "f")
    for changed in (PLAIN.replace("a + b", "a - b"), PLAIN.replace('"s"', '"t"')):
        new_signature, new_body = digests(changed, "f")
        assert new_signature == signature
        assert new_body != body


def test_changed_signature_or_decorator_changes_the_revision() -> None:
    assert digests(PLAIN, "f")[0] != digests(PLAIN.replace("c=1", "c=2"), "f")[0]
    cls = "class K:\n    @staticmethod\n    def m(x):\n        return x\n"
    assert digests(cls, "K.m") != digests(cls.replace("staticmethod", "classmethod"), "K.m")


def test_digests_are_deterministic_across_runs() -> None:
    assert digests(PLAIN, "f") == digests(PLAIN, "f")
    req = fixture_request()
    assert normalize_module(in_process(req)) == normalize_module(in_process(req))


# --- robustness -----------------------------------------------------------------------------


def test_broken_symbols_are_skipped_but_their_siblings_and_enclosing_class_kept() -> None:
    code = (
        "class C:\n    def good(self):\n        return 1\n\n    def bad(self):\n        x = = 1\n\n"
        "    def after(self):\n        return 2\n\n\ndef top():\n    return 3\n"
    )
    parsed = symbols_of(code)["pkg/mod.py"]
    names = {s.qualified_name for s in parsed.symbols}
    assert "pkg.mod.C" in names
    assert "pkg.mod.C.good" in names
    assert "pkg.mod.C.bad" not in names
    assert {"pkg.mod.C.after", "pkg.mod.top"} <= names
    assert "pkg.mod" in names


def test_children_of_a_skipped_symbol_are_skipped_too() -> None:
    code = "def outer(:\n    def inner():\n        pass\n\n\ndef fine():\n    pass\n"
    names = {s.qualified_name for s in symbols_of(code)["pkg/mod.py"].symbols}
    assert "pkg.mod.fine" in names
    assert not {n for n in names if n.startswith("pkg.mod.outer")}


def test_garbage_never_crashes() -> None:
    for junk in (b"\x00\x01\x02", b")))(((", b"def\n", b"class :", b"@\n@\n", b"\xff\xfe" * 50):
        assert in_process(request(source("pkg/mod.py", junk))).files


def test_invalid_utf8_and_names_glued_to_non_ascii_bytes_are_skipped() -> None:
    code = (
        b"def ok():\n    pass\n\n\ndef caf\xe9():\n    pass\n\n\nx = '\xff'\ndef y():\n    pass\n"
    )
    names = {s.qualified_name for s in symbols_of(code)["pkg/mod.py"].symbols}
    assert {"pkg.mod.ok", "pkg.mod.y"} <= names
    assert not any("caf" in n for n in names)


def test_unicode_identifiers_are_kept() -> None:
    names = {
        s.qualified_name for s in symbols_of("def caf\u00e9():\n    pass\n")["pkg/mod.py"].symbols
    }
    assert "pkg.mod.caf\u00e9" in names


def test_crlf_and_bom_offsets_refer_to_the_raw_bytes() -> None:
    lf = "def f(a):\n    return a\n"
    crlf = lf.replace("\n", "\r\n").encode()
    parsed = symbols_of(b"\xef\xbb\xbf" + crlf)["pkg/mod.py"]
    (item,) = (s for s in parsed.symbols if s.kind == "function")
    assert item.signature == "def f(a):"
    assert item.start_byte == 3
    assert digests(lf, "f")[1] == digests(crlf.decode(), "f")[1]


def test_empty_and_whitespace_only_files() -> None:
    assert symbols_of(b"")["pkg/mod.py"].symbols == ()
    assert [s.kind for s in symbols_of(b"\n\n")["pkg/mod.py"].symbols] == ["module"]
    assert symbols_of(b"", "pkg/__init__.py")["pkg/__init__.py"].symbols == ()


def test_non_identifier_paths_do_not_get_the_batch_refused() -> None:
    files = [
        source("my-pkg/2024/x-y.py", "def f():\n    pass\n"),
        source("src/my-pkg/mod.py", "def g():\n    pass\n"),
        source("__init__.py", "class C:\n    pass\n"),
    ]
    module = in_process(request(*files))
    names = [{s.qualified_name for s in parsed.symbols} for parsed in module.files]
    assert names == [{"f"}, {"mod", "mod.g"}, {"C"}]


def test_deep_nesting_uses_no_recursion() -> None:
    depth = 400
    code = "".join(f"{' ' * i}def f{i}():\n" for i in range(depth)) + f"{' ' * depth}pass\n"
    assert len(symbols_of(code)["pkg/mod.py"].symbols) >= 1
    assert in_process(request(source("pkg/mod.py", "x = " + "(" * 3000 + "1" + ")" * 3000)))
    assert in_process(request(source("pkg/mod.py", "x = " + "[" * 20000)))


def test_huge_file_through_the_sandbox_within_limits() -> None:
    body = "def f{n}(a, b):\n    return a + b + {n}\n\n\n"
    code = "".join(body.format(n=n) for n in range(30_000))[:MAX_SOURCE_BYTES]
    started = time.monotonic()
    module = python_adapter().parse(request(source("pkg/big.py", code)))
    assert time.monotonic() - started < 15
    assert 1_000 < len(module.files[0].symbols) <= 10_000


def test_huge_names_and_signatures_stay_within_the_contract() -> None:
    code = f"def {'a' * 600}():\n    pass\n\n\ndef ok({'p, ' * 400}q):\n    pass\n"
    parsed = symbols_of(code)["pkg/mod.py"]
    names = {s.qualified_name for s in parsed.symbols}
    assert "pkg.mod.ok" in names
    assert not any("a" * 600 in n for n in names)
    (ok,) = (s for s in parsed.symbols if s.qualified_name == "pkg.mod.ok")
    assert 0 < len(ok.signature.encode()) <= 256


def test_control_characters_in_signatures_are_cut() -> None:
    code = b"def f(a=b'\x01\x02'):\n    pass\n"
    (item,) = (s for s in symbols_of(code)["pkg/mod.py"].symbols if s.kind == "function")
    assert item.signature.startswith("def f(a=b'")
    assert "\x01" not in item.signature


def test_a_file_over_the_work_budget_degrades_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common.Budget, "MAX_NODES_PER_FILE", 200)
    big = "x = [" + ",".join("1" for _ in range(500)) + "]\n"
    module = in_process(
        request(
            source("pkg/a.py", "def a():\n    pass\n"),
            source("pkg/big.py", big + "def b():\n    pass\n"),
            source("pkg/c.py", "def c():\n    pass\n"),
        )
    )
    counts = {item.path: len(item.symbols) for item in module.files}
    assert counts == {"pkg/a.py": 2, "pkg/big.py": 0, "pkg/c.py": 2}
    assert diagnostics_of(module.files[1]) == DEGRADED_BY_BUDGET
    assert diagnostics_of(module.files[0]) == diagnostics_of(module.files[2]) == {}


def test_a_real_1mib_file_over_the_default_budget_degrades_and_the_batch_survives() -> None:
    literal = "x = [" + ",".join("1" for _ in range(MAX_SOURCE_BYTES // 2 - 10)) + "]\n"
    module = python_adapter().parse(
        request(source("pkg/big.py", literal), source("pkg/ok.py", "def a():\n    pass\n"))
    )
    assert [item.path for item in module.files] == ["pkg/big.py", "pkg/ok.py"]
    # The node budget (not the clock) degrades the big file; the batch answered either way.
    assert module.files[0].symbols == ()
    assert diagnostics_of(module.files[0]) == DEGRADED_BY_BUDGET
    assert_intact_or_reported(module.files[1], symbols=2)


def test_bugs_in_the_adapter_are_not_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_args: object) -> ParsedFile:
        raise KeyError("bug")

    monkeypatch.setattr(python, "_parse_file", broken)
    with pytest.raises(KeyError):
        parse_python(request(source("pkg/a.py", "x = 1\n")))


def test_a_one_megabyte_call_chain_is_linear_through_the_sandbox() -> None:
    chain = "a" + ".f()" * (MAX_SOURCE_BYTES // 4 - 8) + "\n"
    started = time.monotonic()
    module = python_adapter().parse(
        request(source("pkg/chain.py", chain), source("pkg/ok.py", "def a():\n    pass\n"))
    )
    assert time.monotonic() - started < 12
    assert len(module.files[1].symbols) == 2


def test_many_same_named_definitions_and_calls_resolve_fast() -> None:
    defs = "".join("def f():\n    pass\n" for _ in range(10_000))
    calls = "def caller():\n" + "".join("    f()\n" for _ in range(30_000))
    code = (defs + calls)[:MAX_SOURCE_BYTES]
    started = time.monotonic()
    parsed = symbols_of(code)["pkg/mod.py"]
    assert time.monotonic() - started < 8
    assert parsed.relations == ()  # more than 8 candidates: ambiguous, no edge


def test_few_same_named_candidates_are_all_edges_and_resolution_stops_at_the_cap() -> None:
    code = "def f():\n    pass\n\n\ndef f():\n    pass\n\n\ndef caller():\n    f()\n"
    assert len(symbols_of(code)["pkg/mod.py"].relations) == 2
    many = "".join(f"def g{n}():\n    pass\n" for n in range(100))
    calls = "def caller():\n" + "".join(f"    g{n}()\n" for n in range(100))
    assert len(symbols_of(many + calls)["pkg/mod.py"].relations) == 64


def test_duplicate_targets_in_one_assignment_never_duplicate_a_symbol() -> None:
    parsed = symbols_of("a = a = 1\nb, b = 1, 2\nc, (c, d) = 1, (2, 3)\n")["pkg/mod.py"]
    names = sorted(s.qualified_name for s in parsed.symbols)
    assert names == ["pkg.mod", "pkg.mod.a", "pkg.mod.b", "pkg.mod.c", "pkg.mod.d"]


def test_dropped_broken_symbols_release_their_budget() -> None:
    broken = "".join(f"def b{n}(self):\n    x = = 1\n\n\n" for n in range(12_000))
    long_name = "n" * 500
    heavy = "".join(f"def {long_name}{n}(self):\n    x = = 1\n\n\n" for n in range(400))
    valid = "".join(f"def ok{n}():\n    pass\n\n\n" for n in range(50))
    files = [
        source("pkg/a.py", broken + valid),
        source("pkg/b.py", heavy + valid),
        source("pkg/c.py", valid),
    ]
    module = in_process(request(*files))
    for parsed in module.files:
        names = {s.qualified_name for s in parsed.symbols}
        assert {f"pkg.{parsed.path[4]}.ok{n}" for n in range(50)} <= names


def test_crlf_conversion_inside_string_literals_keeps_the_revision() -> None:
    lf = 'def f():\n    return """a\nb\n  c"""\n'
    crlf = lf.replace("\n", "\r\n")
    assert digests(lf, "f") == digests(crlf, "f")
    assert digests(lf, "f") != digests(lf.replace("b", "x"), "f")


def test_an_error_between_class_members_drops_the_class() -> None:
    code = "class C:\n    def a(self):\n        pass\n    ))\n    def b(self):\n        pass\n"
    names = {s.qualified_name for s in symbols_of(code)["pkg/mod.py"].symbols}
    assert not any(n.startswith("pkg.mod.C") for n in names)


def test_relation_and_symbol_counts_are_bounded() -> None:
    calls = "".join(f"    f{n}()\n" for n in range(200))
    defs = "".join(f"def f{n}():\n    pass\n\n\n" for n in range(200))
    parsed = symbols_of(defs + "def caller():\n" + calls)["pkg/mod.py"]
    assert len(parsed.relations) == 64


def test_first_call_site_per_target_is_kept() -> None:
    parsed = symbols_of("def a():\n    b()\n    b()\n\n\ndef b():\n    pass\n")["pkg/mod.py"]
    assert len(parsed.relations) == 1


def test_module_level_kinds() -> None:
    parsed = symbols_of("A = 1\nb = 2\nc: int\nd = e = 3\ntype T = int\n\n\nclass C:\n    K = 1\n")
    kinds = {s.qualified_name: s.kind for s in parsed["pkg/mod.py"].symbols}
    assert kinds == {
        "pkg.mod": "module",
        "pkg.mod.A": "constant",
        "pkg.mod.b": "variable",
        "pkg.mod.c": "variable",
        "pkg.mod.d": "variable",
        "pkg.mod.e": "variable",
        "pkg.mod.T": "type",
        "pkg.mod.C": "class",
        "pkg.mod.C.K": "field",
    }


def dense_calls_file() -> str:
    """About 1 MiB: many short defs, some of which call 64 distinct same-file functions."""
    callees = 3_000
    defs = "".join(f"def f{n}():\n    pass\n\n" for n in range(callees))
    body = "".join(f"    f{n}()\n" for n in range(64))
    callers = "".join(f"def c{n}():\n{body}\n" for n in range(2_000))
    return (defs + callers)[:MAX_SOURCE_BYTES]


def test_dense_relations_never_push_the_answer_over_the_output_bound() -> None:
    req = request(
        source("pkg/dense.py", dense_calls_file()), source("pkg/ok.py", "def a():\n    pass\n")
    )
    module = python_adapter().parse(req)
    assert len(module.model_dump_json()) <= _common.OUTPUT_BUDGET
    assert {item.path: bool(item.symbols) for item in module.files}["pkg/ok.py"]


def test_output_over_budget_sheds_relations_then_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    req = request(
        source("pkg/a.py", "def a():\n    b()\n\n\ndef b():\n    pass\n"),
        source("pkg/c.py", "def c():\n    pass\n"),
    )
    full = len(parse_python(req).files[0].model_dump_json())
    monkeypatch.setattr(_common, "OUTPUT_BUDGET", full - 1)
    module = parse_python(req)
    assert module.files[0].relations == ()
    assert module.files[0].symbols
    monkeypatch.setattr(_common, "OUTPUT_BUDGET", 10)
    assert [item.symbols for item in parse_python(req).files] == [(), ()]


def test_deeply_nested_definitions_around_a_large_body_hash_in_linear_time() -> None:
    depth = 100
    literal = "x = [" + ",".join("a" for _ in range(200_000)) + "]\n"
    code = "".join(f"{' ' * i}def f{i}():\n" for i in range(depth)) + " " * depth + literal
    started = time.monotonic()
    module = python_adapter().parse(request(source("pkg/deep.py", code)))
    assert time.monotonic() - started < 15
    assert len(module.files[0].symbols) >= depth


def test_outer_revision_still_changes_when_an_inner_body_changes() -> None:
    code = "def outer():\n    def inner():\n        return 1\n    return inner\n"
    assert digests(code, "outer")[1] != digests(code.replace("return 1", "return 2"), "outer")[1]
    assert digests(code, "outer")[0] == digests(code.replace("return 1", "return 2"), "outer")[0]


def test_nine_variable_assignments_do_not_suppress_the_edge_to_a_function() -> None:
    code = "".join("x = 1\n" for _ in range(9)) + "def x():\n    pass\n\n\ndef caller():\n    x()\n"
    module = in_process(request(source("pkg/mod.py", code)))
    (parsed,) = module.files
    names = {s.ref: s.qualified_name for s in parsed.symbols}
    edges = {(names[r.source_ref], names[r.target_ref]) for r in parsed.relations}
    assert ("pkg.mod.caller", "pkg.mod.x") in edges


def test_more_than_eight_callable_candidates_still_yield_no_edge() -> None:
    code = "".join("def x():\n    pass\n" for _ in range(9)) + "def caller():\n    x()\n"
    (parsed,) = in_process(request(source("pkg/mod.py", code))).files
    assert parsed.relations == ()


def test_a_batch_of_garbage_files_degrades_the_tail_not_the_process() -> None:
    """6 x 1 MB of parser-hostile bytes: the CPU backstop degrades files, never nonzero_exit."""
    garbage = (b"def (\n" * 200_000)[:1_000_000]  # never finishes parsing
    files = [source(f"pkg/g{n}.py", garbage) for n in range(6)]
    files.append(source("pkg/ok.py", "def a():\n    pass\n"))
    module = python_adapter().parse(request(*files))
    assert [item.path for item in module.files] == [f.path for f in files]
    # Which files the CPU backstop reaches depends on machine load: only say that every file
    # was answered, and that a file without structure says why.
    for item in module.files:
        assert_intact_or_reported(item)


def test_the_cpu_backstop_is_checked_before_and_during_a_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_common.Budget, "CPU_SOFT_LIMIT", -1.0)
    module = in_process(request(source("pkg/a.py", "def a():\n    pass\n")))
    assert module.files[0].symbols == ()
    assert diagnostics_of(module.files[0]) == DEGRADED_BY_BUDGET


@pytest.mark.skipif(landlock.abi_version() < 1, reason="kernel lacks Landlock")
def test_the_real_adapter_runs_confined_and_cannot_read_a_sibling_file(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    sibling = tmp_path / "sibling.txt"
    sibling.write_text("secret sibling")
    limits = Limits(checkout_roots=(str(checkout),), wall_seconds=15.0)
    assert landlock.abi_version() >= 1
    module = python_adapter(limits=limits).parse(fixture_request())
    assert normalize_module(module) == json.loads((FIXTURES / "expected.json").read_text())
    assert any(item.symbols for item in module.files)
    # Its read set (interpreter, site-packages, its own package) has no room for the sibling.
    command = [sys.executable, "-m", python.__name__]
    rules = landlock.read_set(command)
    real = os.path.realpath(sibling)
    assert not any(real == r or real.startswith(r.rstrip("/") + "/") for r in rules.read)
    # And a child under the same limits and interpreter is refused the file (EACCES).
    fake = str(Path(__file__).with_name("fake_adapter.py"))
    raw = runner._run(
        [sys.executable, fake, "read_other_file", str(sibling)],
        fixture_request().model_dump_json().encode(),
        limits,
        {},
    )
    assert json.loads(raw)["probe"][f"read:{sibling}"] == "EACCES"


def test_a_hostile_parse_is_cut_inside_the_parse_by_the_cpu_backstop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Error recovery on this input is quadratic (seconds for 1 MB): the read callback ends it."""
    monkeypatch.setattr(_common.Budget, "CPU_SOFT_LIMIT", 0.5)
    hostile = (b"def (\n" * 200_000)[:1_000_000]
    started = time.process_time()
    module = in_process(request(source("pkg/h.py", hostile)))
    assert time.process_time() - started < 3
    assert module.files[0].symbols == ()
    assert diagnostics_of(module.files[0]) == DEGRADED_BY_BUDGET


# --- references and diagnostics through the shared helpers ----------------------------------


def _with_references(source: SourceFile, budget: object, output: list[int]) -> ParsedFile:
    refs = [
        {
            "source": None,
            "kind": kind,
            "target_name": name,
            "relative_level": 0,
            "start_byte": start,
            "end_byte": start + len(name),
            "evidence_kind": "tree_sitter",
            "confidence": "heuristic" if kind == "call" else "syntactic",
        }
        for kind, name, start in (("call", "os", 13), ("import", "sys", 4), ("call", "a", 0))
    ]
    return ParsedFile.model_validate_json(
        json.dumps(
            {
                "path": source.path,
                "language": source.language,
                "parser_fingerprint": FINGERPRINT,
                "symbols": [],
                "references": refs,
                "diagnostics": [{"code": "syntax_recovered", "count": 2}],
            }
        )
    )


REFERENCE_SOURCE = b"a = sys; y = os.x\n"


def _reference_request() -> ParseRequest:
    return ParseRequest(files=(SourceFile.from_bytes("pkg/m.py", "python", REFERENCE_SOURCE),))


def test_safe_module_carries_references_and_diagnostics_through() -> None:
    module = _common.safe_module(_reference_request(), FINGERPRINT, _with_references)
    file = module.files[0]
    assert module.protocol_version == 2
    assert [item.target_name for item in file.references] == ["os", "sys", "a"]
    assert [(item.code, item.count) for item in file.diagnostics] == [("syntax_recovered", 2)]


def test_normalize_module_sorts_references_and_diagnostics_deterministically() -> None:
    module = _common.safe_module(_reference_request(), FINGERPRINT, _with_references)
    view = normalize_module(module)["files"][0]
    assert [item["target"] for item in view["references"]] == sorted(
        item["target"] for item in view["references"]
    ) or [item["range"] for item in view["references"]] == sorted(
        item["range"] for item in view["references"]
    )
    reordered = module.model_copy(
        update={
            "files": (
                module.files[0].model_copy(update={"references": module.files[0].references[::-1]}),
            )
        }
    )
    assert normalize_module(reordered) == normalize_module(module)
    dropped = module.files[0].model_copy(update={"references": module.files[0].references[:2]})
    assert normalize_module(module.model_copy(update={"files": (dropped,)})) != normalize_module(
        module
    )
    changed = module.files[0].model_copy(update={"diagnostics": ()})
    assert normalize_module(module.model_copy(update={"files": (changed,)})) != normalize_module(
        module
    )


def test_output_over_budget_sheds_references_and_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    full = _common.safe_module(_reference_request(), FINGERPRINT, _with_references).files[0]
    lean = full.model_copy(update={"references": ()})
    monkeypatch.setattr(_common, "OUTPUT_BUDGET", len(lean.model_dump_json()) + 60)
    shed = _common.safe_module(_reference_request(), FINGERPRINT, _with_references).files[0]
    assert shed.references == ()
    assert {item.code for item in shed.diagnostics} == {"syntax_recovered", "references_capped"}
    assert [item.count for item in shed.diagnostics if item.code == "references_capped"] == [3]


# --- completeness regressions (the PR #30 review) --------------------------------------------


def test_an_error_in_a_decorator_drops_only_its_own_definition() -> None:
    inside = "class C:\n    @foo(a b)\n    def f(self):\n        pass\n\n    def g(self):\n        pass\n"
    names = names_of(inside)
    assert {"pkg.mod.C", "pkg.mod.C.g"} <= names
    assert "pkg.mod.C.f" not in names
    top = "@foo(a b)\ndef f():\n    pass\n\n\ndef g():\n    pass\n"
    names = names_of(top)
    assert "pkg.mod.g" in names
    assert "pkg.mod.f" not in names
    # A decorator that is fine keeps its definition, and so does an error in the body of another.
    fine = "@foo(a)\ndef f():\n    pass\n"
    assert "pkg.mod.f" in names_of(fine)


def test_a_dropped_decorated_definition_takes_its_references_with_it() -> None:
    code = "class C:\n    @foo(a b)\n    def f(self):\n        bar()\n\n    def g(self):\n        baz()\n"
    calls = {(source, target) for source, _, _, _, target, _, _ in refs_of(code) if source}
    assert calls == {("pkg.mod.C.g", "baz")}


def test_assignments_in_blocks_bind_at_module_and_class_scope() -> None:
    code = (
        "if TYPE_CHECKING:\n    Alias = int\nelse:\n    Alias = str\n"
        "try:\n    T = 1\nexcept E:\n    T = 2\nfinally:\n    F = 3\n"
        "with ctx:\n    W = 1\nfor i in y:\n    L = 1\nwhile z:\n    Q = 1\n"
        "match v:\n    case 1:\n        M = 1\n\n\n"
        "class C:\n    if enabled:\n        field = 1\n    else:\n        field = 2\n\n"
        "    def m(self):\n        if x:\n            nope = 1\n\n\n"
        "def f():\n    if x:\n        local = 1\n    for i in y:\n        loop = 2\n"
    )
    kinds = sorted(
        (item.qualified_name, item.kind) for item in symbols_of(code)["pkg/mod.py"].symbols
    )
    assert kinds == sorted(
        [
            ("pkg.mod", "module"),
            ("pkg.mod.Alias", "variable"),
            ("pkg.mod.Alias", "variable"),
            ("pkg.mod.T", "constant"),
            ("pkg.mod.T", "constant"),
            ("pkg.mod.F", "constant"),
            ("pkg.mod.W", "constant"),
            ("pkg.mod.L", "constant"),
            ("pkg.mod.Q", "constant"),
            ("pkg.mod.M", "constant"),
            ("pkg.mod.C", "class"),
            ("pkg.mod.C.field", "field"),
            ("pkg.mod.C.field", "field"),
            ("pkg.mod.C.m", "method"),
            ("pkg.mod.f", "function"),
        ]
    )


def test_starred_targets_bind() -> None:
    code = "head, *tail = values\n[a, *b] = v\n(c, *d) = v\n*e, = v\nx, (y, *z) = v\n"
    assert names_of(code) == {
        "pkg.mod",
        *(f"pkg.mod.{name}" for name in ("head", "tail", "a", "b", "c", "d", "e", "x", "y", "z")),
    }


def test_unicode_path_components_are_kept_in_module_names() -> None:
    assert names_of("class A:\n    pass\n", "pkg/m\u00f3dulo.py") == {
        "pkg.m\u00f3dulo",
        "pkg.m\u00f3dulo.A",
    }
    assert names_of("x = 1\n", "\u4e2d\u6587/mod.py") == {"\u4e2d\u6587.mod", "\u4e2d\u6587.mod.x"}
    assert names_of("x = 1\n", "pkg/caf\u00e9/__init__.py") == {"pkg.caf\u00e9", "pkg.caf\u00e9.x"}


def test_generic_bases_unwrap_to_the_base() -> None:
    same_file = "class Base:\n    pass\n\n\nclass A(Base[T]):\n    pass\n"
    assert edges_of(same_file) == {("pkg.mod.A", "pkg.mod.Base", "inherits")}
    assert refs_of(same_file) == []
    code = "class A(Base[T], m.Other[int], Generic[T], Deep[T][U], metaclass=M, *rest):\n    pass\n"
    inherits = [item for item in refs_of(code) if item[1] == "inherit"]
    assert inherits == ordered(
        [
            ("pkg.mod.A", "inherit", 0, None, "Base", "Base", None),
            ("pkg.mod.A", "inherit", 0, "m", "Other", "Other", "m"),
            ("pkg.mod.A", "inherit", 0, None, "Generic", "Generic", None),
            ("pkg.mod.A", "inherit", 0, None, "Deep", "Deep", None),
        ]
    )


# --- references -------------------------------------------------------------------------------


def test_import_forms() -> None:
    code = (
        "import a.b\nimport c.d as e, f\nimport g . h\nfrom ..pkg import x\n"
        "from a.b import c, d as dd\nfrom . import y\nfrom .. import z\nfrom .p.q import w\n"
        "from . . import sp\nfrom m import (\n    n,\n    o as p,\n)\n"
    )
    imp = "import"
    assert refs_of(code) == ordered(
        [
            (None, imp, 0, None, "a.b", "a.b", None),
            (None, imp, 0, None, "c.d", "c.d", None),
            (None, imp, 0, None, "f", "f", None),
            (None, imp, 0, None, "g.h", "g . h", None),
            (None, imp, 2, "pkg", "x", "x", "pkg"),
            (None, imp, 0, "a.b", "c", "c", "a.b"),
            (None, imp, 0, "a.b", "d", "d", "a.b"),
            (None, imp, 1, None, "y", "y", None),
            (None, imp, 2, None, "z", "z", None),
            (None, imp, 1, "p.q", "w", "w", "p.q"),
            (None, imp, 2, None, "sp", "sp", None),
            (None, imp, 0, "m", "n", "n", "m"),
            (None, imp, 0, "m", "o", "o", "m"),
        ]
    )


def test_an_aliased_import_reports_the_name_it_binds() -> None:
    code = "import a.b as c\nimport d.e\nfrom p.q import r as s, t\nfrom . import u as v\n"
    parsed = symbols_of(code.encode(), "pkg/mod.py")["pkg/mod.py"]
    bound = sorted(
        (item.qualifier or "", item.target_name, item.alias)
        for item in parsed.references
        if item.kind == "import"
    )
    for item in parsed.references:  # the alias is its own token, at its own range
        if item.alias is not None:
            assert code.encode()[item.alias_start_byte : item.alias_end_byte].decode() == item.alias
    assert bound == [
        ("", "a.b", "c"),
        ("", "d.e", None),
        ("", "u", "v"),
        ("p.q", "r", "s"),
        ("p.q", "t", None),
    ]


def test_star_imports_reference_the_module_and_nameless_ones_nothing() -> None:
    code = "from a.b import *\nfrom .pkg import *\nfrom . import *\nfrom .. import *\n"
    assert refs_of(code) == ordered(
        [
            (None, "import", 0, None, "a.b", "a.b", None),
            (None, "import", 1, None, "pkg", "pkg", None),
        ]
    )


def test_imports_belong_to_the_scope_they_sit_in() -> None:
    code = (
        "if TYPE_CHECKING:\n    from a import b\n\n\nclass C:\n    import c\n\n"
        "    def m(self):\n        import d\n        from e import f\n"
    )
    assert refs_of(code) == ordered(
        [
            (None, "import", 0, "a", "b", "b", "a"),
            ("pkg.mod.C", "import", 0, None, "c", "c", None),
            ("pkg.mod.C.m", "import", 0, None, "d", "d", None),
            ("pkg.mod.C.m", "import", 0, "e", "f", "f", "e"),
        ]
    )


def test_relative_levels_beyond_the_contract_and_broken_imports_are_skipped() -> None:
    code = (
        f"from {'.' * 17} import x\nfrom {'.' * 16} import ok\nfrom x import\nimport a.\n"
        "from . import\nimport (b)\n"
    )
    assert refs_of(code) == [(None, "import", 16, None, "ok", "ok", None)]


def test_calls_to_names_this_file_does_not_bind_are_heuristic_references() -> None:
    code = (
        "import os\nfrom x import y\n\n\ndef a():\n    os.f()\n    y()\n    print()\n    b()\n"
        "    os.path.join()\n    self.z()\n    f().g()\n    x[0].h()\n    (p).q()\n\n\ndef b():\n    pass\n"
    )
    calls = [item for item in refs_of(code) if item[1] == "call"]
    assert calls == ordered(
        [
            ("pkg.mod.a", "call", 0, "os", "f", "f", "os"),
            ("pkg.mod.a", "call", 0, None, "y", "y", None),
            ("pkg.mod.a", "call", 0, None, "print", "print", None),
            ("pkg.mod.a", "call", 0, "os.path", "join", "join", "os.path"),
            ("pkg.mod.a", "call", 0, None, "f", "f", None),
        ]
    )
    assert edges_of(code) == {("pkg.mod.a", "pkg.mod.b", "calls")}
    parsed = symbols_of(code)["pkg/mod.py"]
    assert {r.confidence for r in parsed.references if r.kind == "call"} == {"heuristic"}
    assert {r.confidence for r in parsed.references if r.kind == "import"} == {"syntactic"}


def test_a_same_file_edge_is_a_relation_or_nothing_never_also_a_reference() -> None:
    code = (
        "class K:\n    def build(self):\n        pass\n\n    def m(self):\n        g()\n"
        "        self.build()\n\n\ndef g():\n    pass\n\n\ndef a():\n    K.build()\n"
        "    g()\n    obj.go()\n    K()\n"
    )
    assert [item for item in refs_of(code) if item[1] == "call"] == [
        ("pkg.mod.a", "call", 0, "obj", "go", "go", "obj")
    ]
    assert edges_of(code) == {
        ("pkg.mod.K.m", "pkg.mod.K.build", "calls"),
        ("pkg.mod.K.m", "pkg.mod.g", "calls"),
        ("pkg.mod.a", "pkg.mod.g", "calls"),
        ("pkg.mod.a", "pkg.mod.K", "calls"),
    }
    ambiguous = "".join("def x():\n    pass\n" for _ in range(9)) + "def caller():\n    x()\n"
    assert refs_of(ambiguous) == []
    assert not [e for e in edges_of(ambiguous) if e[1].endswith(".x")]


def test_class_scope_names_are_not_visible_from_methods_so_the_call_is_a_reference() -> None:
    code = "class C:\n    def g(self):\n        pass\n\n    def m(self):\n        g()\n"
    assert refs_of(code) == [("pkg.mod.C.m", "call", 0, None, "g", "g", None)]
    assert edges_of(code) == set()


def test_bases_defined_elsewhere_are_inherit_references() -> None:
    code = "import abc\n\n\nclass A(abc.ABC, Missing, deep.mod.Base):\n    pass\n"
    inherits = [item for item in refs_of(code) if item[1] == "inherit"]
    assert inherits == ordered(
        [
            ("pkg.mod.A", "inherit", 0, "abc", "ABC", "ABC", "abc"),
            ("pkg.mod.A", "inherit", 0, None, "Missing", "Missing", None),
            ("pkg.mod.A", "inherit", 0, "deep.mod", "Base", "Base", "deep.mod"),
        ]
    )


def test_references_are_deduplicated_per_source_and_target() -> None:
    code = "def a():\n" + "    print()\n" * 100 + "\n\ndef b():\n    print()\n"
    assert refs_of(code) == ordered(
        [
            ("pkg.mod.a", "call", 0, None, "print", "print", None),
            ("pkg.mod.b", "call", 0, None, "print", "print", None),
        ]
    )
    assert refs_of("import a\nimport a\n") != []
    assert len(refs_of("import a\nimport a\n")) == 1


def test_per_symbol_and_module_caps_report_references_capped() -> None:
    code = "def a():\n" + "".join(f"    n{n}()\n" for n in range(100))
    parsed = symbols_of(code)["pkg/mod.py"]
    assert len(parsed.references) == 64
    assert diagnostics_of(parsed) == {"references_capped": 36}
    many = "".join(f"import m{n}\n" for n in range(4200))
    parsed = symbols_of(many)["pkg/mod.py"]
    assert len(parsed.references) == 4096
    assert diagnostics_of(parsed) == {"references_capped": 104}


def test_imports_win_over_calls_when_a_scope_is_full() -> None:
    code = "".join(f"f{n}()\n" for n in range(4100)) + "import late\n"
    targets = {item[4] for item in refs_of(code)}
    assert "late" in targets
    assert len([item for item in refs_of(code) if item[1] == "call"]) == 4095


def test_references_inside_dropped_symbols_are_dropped() -> None:
    code = "def f(:\n    g()\n    import x\n\n\ndef ok():\n    h()\n"
    assert refs_of(code) == [("pkg.mod.ok", "call", 0, None, "h", "h", None)]


def test_reference_names_share_the_name_budget_with_symbols() -> None:
    """Long unresolved names must be capped and reported, never refuse the whole file."""
    long_name = "n" * 480
    body = "".join(f"    {long_name}{n}()\n" for n in range(64))
    code = "".join(f"def f{n}():\n{body}\n\n" for n in range(30))[:MAX_SOURCE_BYTES]
    parsed = symbols_of(code)["pkg/mod.py"]
    assert len(parsed.symbols) > 20
    assert 100 < len(parsed.references) < 30 * 64
    assert diagnostics_of(parsed).keys() == {"references_capped"}


def test_references_survive_crlf_bom_unicode_and_invalid_utf8() -> None:
    raw = b"\xef\xbb\xbfimport os\r\nfrom x import y\r\nx = '\xff'\r\nfrom m\xc3\xb3dulo import gr\xc3\xb6\xc3\x9fe\r\n"
    raw += b"gr\xc3\xb6\xc3\x9fe()\r\n"
    assert refs_of(raw) == ordered(
        [
            (None, "import", 0, None, "os", "os", None),
            (None, "import", 0, "x", "y", "y", "x"),
            (None, "import", 0, "m\u00f3dulo", "gr\u00f6\u00dfe", "gr\u00f6\u00dfe", "m\u00f3dulo"),
            (None, "call", 0, None, "gr\u00f6\u00dfe", "gr\u00f6\u00dfe", None),
        ]
    )


@pytest.mark.parametrize(
    "code",
    [
        "from x import\n",
        "import a.\n",
        "from . import\n",
        "a.()\n",
        "x = a.b(\n",
        "from ..\n",
        "class A(:\n    pass\n",
        "import a . # c\n b\n",
        "a.\\\nb.c()\n",
        "(a.\\\n b).c()\n",
        "class A(B[\n",
        "from a import (b,\n",
        "@d(\ndef f():\n    pass\n",
    ],
)
def test_broken_reference_syntax_never_degrades_the_file(code: str) -> None:
    parsed = symbols_of("def keep():\n    pass\n\n" + code)["pkg/mod.py"]
    assert "pkg.mod.keep" in {item.qualified_name for item in parsed.symbols}
    assert "file_degraded" not in diagnostics_of(parsed)


# --- diagnostics ------------------------------------------------------------------------------


def test_a_clean_file_has_no_diagnostics_and_an_empty_one_is_not_degraded() -> None:
    assert diagnostics_of(symbols_of("def f():\n    pass\n")["pkg/mod.py"]) == {}
    assert diagnostics_of(symbols_of(b"")["pkg/mod.py"]) == {}


def test_syntax_recovery_and_dropped_symbols_are_reported() -> None:
    parsed = symbols_of("x = = 1\n\n\ndef f(:\n    pass\n\n\ndef ok():\n    pass\n")["pkg/mod.py"]
    diagnostics = diagnostics_of(parsed)
    assert diagnostics["syntax_recovered"] >= 1
    assert diagnostics["symbols_dropped"] == 2
    assert "file_degraded" not in diagnostics
    assert "pkg.mod.ok" in {item.qualified_name for item in parsed.symbols}


def test_unrepresentable_and_over_limit_symbols_are_reported_as_dropped() -> None:
    parsed = symbols_of(f"def {'a' * 600}():\n    pass\n\n\ndef ok():\n    pass\n")["pkg/mod.py"]
    assert diagnostics_of(parsed) == {"symbols_dropped": 1}


def test_a_module_name_that_cannot_be_charged_degrades_the_file() -> None:
    parsed = symbols_of("x = 1\n", path=f"{'a' * 600}/mod.py")[f"{'a' * 600}/mod.py"]
    assert parsed.symbols == ()
    assert diagnostics_of(parsed) == {"file_degraded": 1, "symbols_dropped": 1}


def test_a_refused_answer_degrades_with_a_reason_instead_of_looking_empty() -> None:
    def refused(source: SourceFile, _budget: object, _output: list[int]) -> ParsedFile:
        bad = _common.diagnostics(syntax_recovered=1)
        return ParsedFile.model_validate(
            {
                "path": source.path,
                "language": source.language,
                "parser_fingerprint": FINGERPRINT,
                "symbols": [
                    {
                        "ref": "0",
                        "language": "python",
                        "qualified_name": "nothing_like_it",
                        "kind": "function",
                        "start_byte": 0,
                        "end_byte": 3,
                        "signature": "",
                        "signature_digest": "0" * 64,
                        "semantic_fingerprint": "0" * 64,
                        "evidence_kind": "tree_sitter",
                    }
                ],
                "diagnostics": [item.model_dump() for item in bad],
            }
        )

    file = _common.safe_module(_reference_request(), FINGERPRINT, refused).files[0]
    assert file.symbols == ()
    assert diagnostics_of(file) == {"file_degraded": 1, "symbols_dropped": 1}


def test_output_over_the_bound_degrades_with_a_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common, "OUTPUT_BUDGET", 10)
    module = parse_python(request(source("pkg/a.py", "def a():\n    pass\n")))
    assert diagnostics_of(module.files[0]) == {"file_degraded": 1, "symbols_dropped": 1}


# --- adversarial inputs through the real confined adapter -------------------------------------


def confined(*files: SourceFile) -> tuple[ParsedModule, float]:
    """Run through ``SandboxedAdapter`` (Landlock where the kernel has it); child CPU seconds."""
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    module = python_adapter().parse(request(*files))
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    return module, (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)


def _chain() -> bytes:
    return ("a" + ".f()" * (MAX_SOURCE_BYTES // 4 - 8) + "\n").encode()


def _member_chain() -> bytes:
    return ("a" + ".b" * (MAX_SOURCE_BYTES // 2 - 8) + "\n").encode()


def _same_named() -> bytes:
    defs = "".join("def f():\n    pass\n" for _ in range(10_000))
    calls = "def caller():\n" + "".join("    f()\n" for _ in range(30_000))
    return (defs + calls)[:MAX_SOURCE_BYTES].encode()


def _deep_nesting() -> bytes:
    depth = 300
    head = "".join(f"{' ' * i}def f{i}():\n" for i in range(depth))
    calls = "".join(f"{' ' * depth}g{n}()\n" for n in range(60_000))
    return (head + calls)[:MAX_SOURCE_BYTES].encode()


def _deep_parens() -> bytes:
    return ("x = " + "(" * 400_000 + "1" + ")" * 400_000 + "\n").encode()


def _huge_expression() -> bytes:
    return ("x = " + "1+" * 450_000 + "1\n").encode()


def _hostile_bytes() -> bytes:
    body = b"import os\r\nfrom x import y\r\nx = '\xff'\r\ndef caf\xe9():\r\n    os.f(y)\r\n"
    return b"\xef\xbb\xbf" + body * 5_000


def _distinct_imports() -> bytes:
    return "".join(f"import m{n}\n" for n in range(MAX_SOURCE_BYTES // 15)).encode()


def _dotted_import() -> bytes:
    return ("import " + ".".join("a" for _ in range(MAX_SOURCE_BYTES // 2 - 8)) + "\n").encode()


def _long_unresolved_names() -> bytes:
    body = "".join(f"    {'n' * 480}{n}()\n" for n in range(64))
    return "".join(f"def f{n}():\n{body}\n\n" for n in range(30)).encode()[:MAX_SOURCE_BYTES]


ADVERSARIAL = {
    "call_chain": _chain,
    "member_chain": _member_chain,
    "same_named_defs_and_calls": _same_named,
    "deep_nesting_many_unresolved_calls": _deep_nesting,
    "deep_parentheses": _deep_parens,
    "huge_single_expression": _huge_expression,
    "utf8_crlf_bom": _hostile_bytes,
    "distinct_imports": _distinct_imports,
    "dotted_import": _dotted_import,
    "long_unresolved_names": _long_unresolved_names,
}


@pytest.mark.parametrize("case", sorted(ADVERSARIAL))
def test_adversarial_input_through_the_confined_adapter(case: str) -> None:
    hostile = source("pkg/hostile.py", ADVERSARIAL[case]())
    ok = source("pkg/ok.py", "import os\n\n\ndef a():\n    os.f()\n")
    module, cpu = confined(hostile, ok)
    print(f"cpu[{case}] = {cpu:.2f}s")
    assert cpu < 8
    assert [item.path for item in module.files] == ["pkg/hostile.py", "pkg/ok.py"]
    assert_intact_or_reported(module.files[1], symbols=2, references=2)  # load-independent
    # Whatever happened to the hostile file was said: it kept structure or it was degraded.
    file = module.files[0]
    assert file.symbols or "file_degraded" in diagnostics_of(file)
    assert diagnostics_of(file).get("references_capped", 0) <= 1000


def test_distinct_imports_hit_the_module_cap_and_say_so() -> None:
    module, _ = confined(source("pkg/imports.py", _distinct_imports()))
    file = module.files[0]
    assert len(file.references) == 4096
    assert diagnostics_of(file)["references_capped"] == 1000


def test_hostile_expression_files_report_the_work_budget() -> None:
    module, _ = confined(source("pkg/x.py", _huge_expression()))
    assert diagnostics_of(module.files[0]) == DEGRADED_BY_BUDGET


def test_hostile_garbage_batch_reports_degradation_per_file_with_cpu() -> None:
    garbage = (b"def (\n" * 200_000)[:1_000_000]
    files = [source(f"pkg/g{n}.py", garbage) for n in range(6)] + [
        source("pkg/ok.py", "def a():\n    pass\n")
    ]
    module, cpu = confined(*files)
    print(f"cpu[garbage_batch] = {cpu:.2f}s")
    assert cpu < 9
    assert [item.path for item in module.files] == [f.path for f in files]
    for item in module.files:
        assert_intact_or_reported(item)


def test_the_golden_references_are_what_a_reviewer_expects_for_service() -> None:
    module = python_adapter().parse(fixture_request())
    service = next(item for item in module.files if item.path == "pkg/service.py")
    names = {item.ref: item.qualified_name for item in service.symbols}
    seen = {
        (
            names.get(item.source or ""),
            item.kind,
            item.relative_level,
            item.qualifier,
            item.target_name,
        )
        for item in service.references
    }
    assert seen == {
        (None, "import", 1, None, "models"),
        (None, "import", 1, "models", "Account"),
        (None, "import", 1, "models", "Ledger"),
        (None, "import", 0, None, "os.path"),
        (None, "import", 0, None, "sys"),
        ("pkg.service.run", "call", 0, None, "Ledger"),
        ("pkg.service.run", "call", 0, "ledger", "total"),
        ("pkg.service.run", "call", 0, "Acc", "build"),
        ("pkg.service.run", "call", 0, "models", "helper"),
        ("pkg.service.run", "call", 0, None, "print"),
        ("pkg.service.run", "call", 0, "os.path", "join"),
    }


def test_fingerprints_ignore_trivia_and_do_not_depend_on_the_process() -> None:
    plain = "def f(a):\n    return g(a)\n"
    noisy = 'def f(a):\n    """Doc."""\n    # comment\n    return  g( a )   \n'

    def fingerprint(code: str) -> str:
        symbols = symbols_of(code)["pkg/mod.py"].symbols
        return next(
            item.semantic_fingerprint for item in symbols if item.qualified_name == "pkg.mod.f"
        )

    assert fingerprint(plain) == fingerprint(noisy)
    program = (
        "import sys; sys.path.insert(0, sys.argv[1])\n"
        "from test_python import symbols_of\n"
        "code = 'import a\\nclass C(B):\\n    def m(self):\\n        return x.y()\\n'\n"
        "print([s.semantic_fingerprint for s in symbols_of(code)['pkg/mod.py'].symbols])\n"
    )
    outputs = set()
    for seed in ("0", "1", "12345"):
        done = subprocess.run(
            [sys.executable, "-c", program, str(Path(__file__).parent)],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        )
        outputs.add(done.stdout)
    assert len(outputs) == 1


def test_a_stray_byte_inside_an_import_loses_the_statement_and_says_so() -> None:
    """Documented limit: an ERROR node is skipped whole, and the loss is reported."""
    raw = b"import bad\xff\r\ngo()\r\n"
    module, _ = confined(source("pkg/mod.py", raw))
    assert module.files[0].references == ()
    assert diagnostics_of(module.files[0]).get("syntax_recovered", 0) >= 1


def test_a_name_used_as_base_and_as_call_target_is_resolved_per_kind() -> None:
    # ``Base`` is a function: not a class base, but a callable target in the same scope.
    code = "def Base():\n    pass\n\n\nclass A(Base):\n    def m(self):\n        pass\n\n\ndef caller():\n    Base()\n"
    assert edges_of(code) == {("pkg.mod.caller", "pkg.mod.Base", "calls")}
    # A function binds the name in this file: not a class to link, and not an external base.
    assert refs_of(code) == []
    # And the other way round: bases first must not poison the call lookup of a class.
    code = "class K:\n    pass\n\n\nclass B(K):\n    pass\n\n\ndef f():\n    K()\n"
    assert edges_of(code) == {
        ("pkg.mod.B", "pkg.mod.K", "inherits"),
        ("pkg.mod.f", "pkg.mod.K", "calls"),
    }


def test_references_are_still_emitted_after_the_relation_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(python, "MAX_RELATIONS_PER_FILE", 2)
    defs = "".join(f"def t{n}():\n    pass\n\n\n" for n in range(5))
    calls = "def caller():\n" + "".join(f"    t{n}()\n" for n in range(5)) + "    later()\n"
    parsed = symbols_of(defs + calls)["pkg/mod.py"]
    assert len(parsed.relations) == 2
    assert [item.target_name for item in parsed.references] == ["later"]


def test_a_tainted_assignment_takes_its_call_sites_with_it() -> None:
    code = "x = foo(1 2)\n\n\nclass C:\n    y = bar(1 2)\n    z = ok()\n\n\nw = baz()\n"
    parsed = symbols_of(code)["pkg/mod.py"]
    targets = {item.target_name for item in parsed.references}
    assert "foo" not in targets
    assert "bar" not in targets
    assert {"baz"} <= targets


def test_bases_past_the_cap_are_reported() -> None:
    bases = ", ".join(f"B{n}" for n in range(70))
    parsed = symbols_of(f"class A({bases}):\n    pass\n")["pkg/mod.py"]
    assert len([item for item in parsed.references if item.kind == "inherit"]) == 64
    assert diagnostics_of(parsed) == {"references_capped": 6}
    exact = ", ".join(f"B{n}" for n in range(64))
    assert diagnostics_of(symbols_of(f"class A({exact}):\n    pass\n")["pkg/mod.py"]) == {}


def test_every_silent_skip_of_a_name_is_counted() -> None:
    deep = "Base" + "[T]" * 20
    assert diagnostics_of(symbols_of(f"class A({deep}):\n    pass\n")["pkg/mod.py"]) == {
        "references_capped": 1
    }
    assert diagnostics_of(symbols_of(f"from {'.' * 17} import x\n")["pkg/mod.py"]) == {
        "references_capped": 1
    }
    ambiguous = "".join("def x():\n    pass\n" for _ in range(9)) + "def caller():\n    x()\n"
    assert diagnostics_of(symbols_of(ambiguous)["pkg/mod.py"]) == {"references_capped": 1}
    long_owner = ".".join("a" * 100 for _ in range(6))
    assert diagnostics_of(symbols_of(f"def f():\n    {long_owner}.g()\n")["pkg/mod.py"]) == {
        "references_capped": 1
    }


def test_relations_dropped_by_a_cap_are_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(python, "MAX_RELATIONS_PER_FILE", 2)
    defs = "".join(f"def t{n}():\n    pass\n\n\n" for n in range(5))
    calls = "def caller():\n" + "".join(f"    t{n}()\n" for n in range(5))
    parsed = symbols_of(defs + calls)["pkg/mod.py"]
    assert len(parsed.relations) == 2
    assert diagnostics_of(parsed) == {"references_capped": 3}


def test_shedding_steps_are_reported_and_add_to_what_the_adapter_reported() -> None:
    bases = ", ".join(f"B{n}" for n in range(70))
    code = f"def a():\n    pass\n\n\ndef b():\n    a()\n\n\nclass A({bases}):\n    pass\n"
    full = symbols_of(code)["pkg/mod.py"]
    assert (len(full.references), len(full.relations)) == (64, 1)
    refused = _common.degraded_file(
        SourceFile.from_bytes("pkg/mod.py", "python", b""), FINGERPRINT, "symbols_dropped"
    )
    steps = list(_common._reductions(full, refused))
    assert [(len(item.references), len(item.relations)) for item in steps] == [
        (64, 1),
        (0, 1),
        (0, 0),
        (0, 0),
    ]
    assert diagnostics_of(steps[1]) == {"references_capped": 6 + 64}
    assert diagnostics_of(steps[2]) == {"references_capped": 6 + 64 + 1}
    assert len(steps[2].symbols) == 4
    relations_only = full.model_copy(update={"references": (), "diagnostics": ()})
    only = list(_common._reductions(relations_only, refused))
    assert diagnostics_of(only[1]) == {"references_capped": 1}
    assert only[1].relations == ()


# PLATFORM-037 sizes requests by bytes as well as file count: a normal request carries at most
# 1 MiB of source in up to 64 files, which keeps it well inside the CPU backstop and the output bound.
NORMAL_REQUEST_SOURCE_BYTES = 1 << 20


def _realistic_module(n: int) -> str:
    """A ~25 KB module: imports, dataclass-style classes with decorators, methods, calls."""
    parts = [
        f'"""Module {n}: accounts and ledgers."""\n\n',
        "from __future__ import annotations\n\nimport os\nimport sys\n",
        "from dataclasses import dataclass, field\nfrom typing import Any, Generic, TypeVar\n",
        f"from . import helpers{n % 7}\nfrom ..core.models import Base, Mixin\n\n",
        "T = TypeVar('T')\nDEFAULT_LIMIT = 100\n\n\n",
    ]
    for k in range(6):
        parts.append(
            f"@dataclass\nclass Account{k}(Base, Generic[T]):\n"
            f'    """Account {k}."""\n\n'
            f"    name: str = ''\n    items: list[Any] = field(default_factory=list)\n\n"
            f"    @property\n    def total(self) -> int:\n"
            f"        return sum(len(x) for x in self.items) + DEFAULT_LIMIT\n\n"
            f"    def add(self, item: Any, *, limit: int = DEFAULT_LIMIT) -> None:\n"
            f"        if len(self.items) >= limit:\n            raise ValueError(item)\n"
            f"        self.items.append(helpers{n % 7}.wrap(item))\n"
            f"        os.path.join(self.name, str(item))\n"
            f"        # Keep a running tally of what was added so the audit trail stays complete.\n"
            f"        tally = {{'count': len(self.items), 'name': self.name, 'limit': limit}}\n"
            f"        for key, value in sorted(tally.items()):\n"
            f"            if value is None or key.startswith('_'):\n                continue\n"
            f"            self.items.append((key, value, 'audit entry for account {k} in module {n}'))\n"
            f"        message = 'account {k} now holds %d items under limit %d' % (len(self.items), limit)\n"
            f"        if self.name and len(message) > 200:\n            print(message)\n"
            f"        # Keep a running tally of what was added so the audit trail stays complete.\n"
            f"        tally = {{'count': len(self.items), 'name': self.name, 'limit': limit}}\n"
            f"        for key, value in sorted(tally.items()):\n"
            f"            if value is None or key.startswith('_'):\n                continue\n"
            f"            self.items.append((key, value, 'audit entry for account {k} in module {n}'))\n"
            f"        message = 'account {k} now holds %d items under limit %d' % (len(self.items), limit)\n"
            f"        if self.name and len(message) > 200:\n            print(message)\n"
            f"        # Keep a running tally of what was added so the audit trail stays complete.\n"
            f"        tally = {{'count': len(self.items), 'name': self.name, 'limit': limit}}\n"
            f"        for key, value in sorted(tally.items()):\n"
            f"            if value is None or key.startswith('_'):\n                continue\n"
            f"            self.items.append((key, value, 'audit entry for account {k} in module {n}'))\n"
            f"        message = 'account {k} now holds %d items under limit %d' % (len(self.items), limit)\n"
            f"        if self.name and len(message) > 200:\n            print(message)\n"
            f"        # Keep a running tally of what was added so the audit trail stays complete.\n"
            f"        tally = {{'count': len(self.items), 'name': self.name, 'limit': limit}}\n"
            f"        for key, value in sorted(tally.items()):\n"
            f"            if value is None or key.startswith('_'):\n                continue\n"
            f"            self.items.append((key, value, 'audit entry for account {k} in module {n}'))\n"
            f"        message = 'account {k} now holds %d items under limit %d' % (len(self.items), limit)\n"
            f"        if self.name and len(message) > 200:\n            print(message)\n\n"
            f"    @staticmethod\n    def build(name: str) -> 'Account{k}':\n"
            f"        return Account{k}(name=name)\n\n\n"
            f"def make_account{k}(name: str) -> Account{k}:\n"
            f"    account = Account{k}.build(name)\n    account.add(sys.argv)\n    return account\n\n\n"
        )
    return "".join(parts)


def test_a_normal_one_mebibyte_request_degrades_nothing_through_the_sandbox() -> None:
    files = [source(f"app/pkg{n % 9}/mod{n}.py", _realistic_module(n)) for n in range(64)]
    total = sum(len(item.content()) for item in files)
    assert 0.9 * NORMAL_REQUEST_SOURCE_BYTES < total <= 1.1 * NORMAL_REQUEST_SOURCE_BYTES
    module, cpu = confined(*files)
    print(f"cpu[normal_corpus_{total // 1000}KB] = {cpu:.2f}s")
    assert [item.path for item in module.files] == [item.path for item in files]
    for item in module.files:
        assert diagnostics_of(item) == {}, item.path
        assert len(item.symbols) > 40
        assert item.references
        assert item.relations


class _Token:
    """A stand-in tree-sitter child: only ``type`` is read for an import prefix."""

    def __init__(self, type: str) -> None:
        self.type = type


class _Prefix:
    type = "import_prefix"

    def __init__(self, *tokens: str) -> None:
        self.children = [_Token(item) for item in tokens]
        self.child_count = len(self.children)


class _Header:
    def __init__(self, *tokens: str) -> None:
        self._prefix = _Prefix(*tokens)
        self.child_count = 1

    def child(self, index: int) -> object:
        return self._prefix if index == 0 else None


@pytest.mark.parametrize(
    ("tokens", "level"),
    [
        ((".",), 1),
        ((".", "."), 2),
        (("...",), 3),  # a grammar that emits the ellipsis as one token
        (("...", "."), 4),
        (("...", "..."), 6),
        (("...", "...", "...", "...", "...", "..."), 18),
    ],
)
def test_the_relative_level_is_the_total_number_of_dots(
    tokens: tuple[str, ...], level: int
) -> None:
    relative = python._relative_module(_Header(*tokens))  # type: ignore[arg-type]
    assert relative is not None and relative[0] == level


@pytest.mark.parametrize(
    ("prefix", "level"),
    [("...", 3), ("....", 4), ("......", 6), (". . .", 3), (". ...", 4)],
)
def test_multi_dot_relative_imports_keep_their_level(prefix: str, level: int) -> None:
    assert refs_of(f"from {prefix}pkg import x\n") == [
        (None, "import", level, "pkg", "x", "x", "pkg")
    ]
    assert refs_of(f"from {prefix} import x\n") == [(None, "import", level, None, "x", "x", None)]


def test_relative_prefixes_over_the_maximum_are_capped_and_reported() -> None:
    for dots in ("." * 17, "..." * 6, "." * 200):
        parsed = symbols_of(f"from {dots}pkg import x\n")["pkg/mod.py"]
        assert parsed.references == ()
        assert diagnostics_of(parsed) == {"references_capped": 1}


def test_a_call_through_a_variable_binding_is_not_an_external_reference() -> None:
    code = (
        "from lib import imported_factory\n"
        "factory = imported_factory\n\n\n"
        "def f():\n    factory()\n\n\n"
        "class C:\n    build = imported_factory\n\n    def m(self):\n        build()\n"
    )
    calls = [item for item in refs_of(code) if item[1] == "call"]
    assert calls == [("pkg.mod.C.m", "call", 0, None, "build", "build", None)]  # class scope hidden
    assert edges_of(code) == set()  # a variable is neither linked nor reported


def test_a_base_bound_by_a_variable_is_not_an_external_inherit() -> None:
    code = "from lib import External\nAlias = External\n\n\nclass C(Alias):\n    pass\n"
    assert [item for item in refs_of(code) if item[1] == "inherit"] == []
    assert edges_of(code) == set()
    # A class alias of nothing local still reports the true external, and a class stays linked.
    assert [item for item in refs_of("class C(Missing):\n    pass\n") if item[1] == "inherit"]
    linked = "class B:\n    pass\n\n\nclass C(B):\n    pass\n"
    assert edges_of(linked) == {("pkg.mod.C", "pkg.mod.B", "inherits")}


def test_an_oversized_import_name_or_qualifier_is_counted() -> None:
    long_module = ".".join("a" * 100 for _ in range(6))
    for code in (
        f"import {long_module}\n",
        f"from {long_module} import x\n",
        f"from .{long_module} import x\n",
        f"from pkg import {'n' * 600}\n",
    ):
        parsed = symbols_of(code)["pkg/mod.py"]
        assert parsed.references == (), code
        assert diagnostics_of(parsed) == {"references_capped": 1}, code
