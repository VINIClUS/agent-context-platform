"""Python structural adapter: golden output, fingerprints, robustness, real sandbox path."""

from __future__ import annotations

import json
import os
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


def test_a_real_1mib_file_over_the_default_budget_degrades_and_the_batch_survives() -> None:
    literal = "x = [" + ",".join("1" for _ in range(MAX_SOURCE_BYTES // 2 - 10)) + "]\n"
    started = time.monotonic()
    module = python_adapter().parse(
        request(source("pkg/big.py", literal), source("pkg/ok.py", "def a():\n    pass\n"))
    )
    assert time.monotonic() - started < 15
    assert {item.path: len(item.symbols) for item in module.files} == {
        "pkg/big.py": 0,
        "pkg/ok.py": 2,
    }


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
    started = time.monotonic()
    module = python_adapter().parse(request(*files))
    assert time.monotonic() - started < 19
    assert [item.path for item in module.files] == [f.path for f in files]
    assert module.files[-1].symbols == ()  # the tail was degraded, and the batch returned


def test_the_cpu_backstop_is_checked_before_and_during_a_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_common.Budget, "CPU_SOFT_LIMIT", -1.0)
    module = in_process(request(source("pkg/a.py", "def a():\n    pass\n")))
    assert module.files[0].symbols == ()


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
