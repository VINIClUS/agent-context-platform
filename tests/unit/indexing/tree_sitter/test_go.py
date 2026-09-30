"""Go structural adapter: golden output, fingerprints, robustness, real sandbox path."""

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

from agent_context_platform.indexing.tree_sitter import _common, go, landlock, runner
from agent_context_platform.indexing.tree_sitter._common import normalize_module
from agent_context_platform.indexing.tree_sitter.base import (
    MAX_MODULE_REFERENCES,
    MAX_REFERENCES_PER_SYMBOL,
    MAX_SOURCE_BYTES,
    ParsedFile,
    ParsedModule,
    ParseRequest,
    SourceFile,
    StructuralAdapter,
    StructuralError,
    parser_fingerprint,
    validate_module,
)
from agent_context_platform.indexing.tree_sitter.go import (
    ADAPTER_NAME,
    ADAPTER_VERSION,
    FINGERPRINT,
    GRAMMAR_VERSIONS,
    go_adapter,
    parse_go,
)
from agent_context_platform.indexing.tree_sitter.runner import Limits

from .test_typescript import MEMORY_BLOWUP_INPUT

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).with_name("fixtures") / "go"
PATH = "pkg/mod.go"
HEAD = "package mod\n\n"


# Sandbox wall timeout: generous, so a loaded machine never turns a slow run into a failure.
PATIENT = Limits(wall_seconds=180.0)


def source(path: str, content: bytes | str) -> SourceFile:
    raw = content.encode() if isinstance(content, str) else content
    return SourceFile.from_bytes(path, "go", raw)


def request(*files: SourceFile) -> ParseRequest:
    return ParseRequest(files=files)


def fixture_request() -> ParseRequest:
    paths = sorted(FIXTURES.rglob("*.go"))
    return request(*(source(str(p.relative_to(FIXTURES)), p.read_bytes()) for p in paths))


def in_process(req: ParseRequest) -> ParsedModule:
    return validate_module(req, parse_go(req), expected_fingerprint=FINGERPRINT)


def parsed_of(content: bytes | str, path: str = PATH) -> ParsedFile:
    (parsed,) = in_process(request(source(path, content))).files
    return parsed


def names_of(parsed: ParsedFile) -> dict[str, str]:
    return {item.qualified_name: item.kind for item in parsed.symbols}


def edges_of(parsed: ParsedFile) -> set[tuple[str, str, str]]:
    names = {item.ref: item.qualified_name for item in parsed.symbols}
    return {(names[r.source_ref], r.kind, names[r.target_ref]) for r in parsed.relations}


def refs_of(parsed: ParsedFile) -> set[tuple[str, str, str | None, str]]:
    names = {item.ref: item.qualified_name for item in parsed.symbols}
    return {
        ("" if r.source is None else names[r.source], r.kind, r.qualifier, r.target_name)
        for r in parsed.references
    }


def codes_of(parsed: ParsedFile) -> dict[str, int]:
    return {item.code: item.count for item in parsed.diagnostics}


def digests(content: str, name: str) -> tuple[str, str]:
    parsed = parsed_of(content)
    (item,) = (s for s in parsed.symbols if s.qualified_name == f"pkg.mod.{name}")
    return item.signature_digest, item.semantic_fingerprint


def child_cpu() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


# --- golden output ------------------------------------------------------------------------


def test_golden_through_the_real_sandboxed_subprocess() -> None:
    expected = json.loads((FIXTURES / "expected.json").read_text())
    module = go_adapter(limits=PATIENT).parse(fixture_request())
    assert normalize_module(module) == expected


def test_in_process_output_equals_the_sandboxed_output() -> None:
    req = fixture_request()
    assert normalize_module(in_process(req)) == normalize_module(
        go_adapter(limits=PATIENT).parse(req)
    )


def test_adapter_contract() -> None:
    adapter = go_adapter(limits=PATIENT)
    assert isinstance(adapter, StructuralAdapter)
    assert adapter.language == "go"
    assert adapter.fingerprint == parser_fingerprint(
        ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS
    )
    req = fixture_request()
    module = adapter.parse(req)
    assert module.protocol_version == 2
    assert [item.path for item in module.files] == [item.path for item in req.files]
    for parsed in module.files:
        assert parsed.parser_fingerprint == adapter.fingerprint
        assert [item.ref for item in parsed.symbols] == [str(n) for n in range(len(parsed.symbols))]
        assert {item.evidence_kind for item in parsed.symbols} <= {"tree_sitter"}
        assert {item.evidence_kind for item in parsed.relations} <= {"tree_sitter"}
        assert {item.kind for item in parsed.relations} <= {"calls", "inherits"}
        assert {item.evidence_kind for item in parsed.references} <= {"tree_sitter"}
        for reference in parsed.references:  # calls are never anything but heuristic
            assert (reference.confidence == "heuristic") == (reference.kind == "call")


def test_grammar_versions_match_the_installed_distributions() -> None:
    for name, version in GRAMMAR_VERSIONS.items():
        assert metadata.version(name) == version
    assert parser_fingerprint(ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS) == FINGERPRINT


# --- naming ---------------------------------------------------------------------------------


def test_module_names_come_from_the_directory_and_the_package_clause() -> None:
    cases = {
        "internal/store/db.go": ("package store\n", "internal.store"),
        "cmd/tool/main.go": ("package main\n", "cmd.tool.main"),
        "main.go": ("package main\n", "main"),
        "src/my-pkg/x.go": ("package x\n", "x"),
        "a/2024/x.go": ("package y\n", "y"),
        "a/b/x_test.go": ("package b_test\n", "a.b.b_test"),
        "a/b/x.go": ("// nothing\nfunc f() {}\n", "a.b"),
    }
    for path, (code, module) in cases.items():
        assert module in names_of(parsed_of(code, path)) or module == ""
        assert names_of(parsed_of(code, path)).get(module) in ("module", None)
    assert names_of(parsed_of("package x\n", "src/my-pkg/x.go")) == {"x": "module"}


def test_a_file_without_any_usable_name_still_yields_unqualified_symbols() -> None:
    assert names_of(parsed_of("func f() {}\n", "my-dir/x.go")) == {"f": "function"}
    assert names_of(parsed_of("package\nfunc f() {}\n", "x.go")) == {"f": "function"}


def test_symbol_kinds() -> None:
    code = (
        HEAD
        + "type S struct{}\ntype I interface{ M() }\ntype A = S\ntype D int\ntype G[T any] []T\n"
        + "const C = 1\nvar V = 2\nfunc F() {}\nfunc (S) M() {}\nfunc (s *S) P() {}\n"
        + "func (g G[T]) Q() {}\nfunc (s (S)) R() {}\n"
    )
    assert names_of(parsed_of(code)) == {
        "pkg.mod": "module",
        "pkg.mod.S": "class",
        "pkg.mod.I": "interface",
        "pkg.mod.I.M": "method",
        "pkg.mod.A": "type",
        "pkg.mod.D": "type",
        "pkg.mod.G": "type",
        "pkg.mod.C": "constant",
        "pkg.mod.V": "variable",
        "pkg.mod.F": "function",
        "pkg.mod.S.M": "method",
        "pkg.mod.S.P": "method",
        "pkg.mod.G.Q": "method",
        "pkg.mod.S.R": "method",
    }


def test_a_method_of_an_unemitted_type_is_qualified_through_its_receiver() -> None:
    parsed = parsed_of(HEAD + "func (x *Elsewhere) M() {}\n")
    assert "pkg.mod.Elsewhere.M" in names_of(parsed)


def test_local_declarations_and_fields_are_not_symbols() -> None:
    code = (
        HEAD + "type T struct{ a int }\nfunc f() {\n\tvar x = 1\n\ttype L int\n\tconst c = 2\n}\n"
    )
    assert set(names_of(parsed_of(code))) == {"pkg.mod", "pkg.mod.T", "pkg.mod.f"}
    nested = HEAD + "var g = func() { var y = 1; type Q int; _ = y }\n"
    assert set(names_of(parsed_of(nested))) == {"pkg.mod", "pkg.mod.g"}


def test_several_inits_and_blank_names_never_collide() -> None:
    parsed = parsed_of(
        HEAD + "func init() {}\nfunc init() {}\nvar _ = 1\nfunc _() {}\nvar a, a = 1, 2\n"
    )
    assert sorted(item.qualified_name for item in parsed.symbols) == [
        "pkg.mod",
        "pkg.mod.a",
        "pkg.mod.init",
        "pkg.mod.init",
    ]


# --- imports, calls and embedded types ---------------------------------------------------------


def test_every_import_form_is_a_syntactic_reference_at_module_level() -> None:
    code = (
        'package mod\n\nimport "fmt"\nimport (\n\tstr "strings"\n\t. "math"\n\t_ "embed"\n'
        '\t"net/http"\n)\nimport `os`\nimport "example.com/a-b/c-d"\nimport "9x/y"\n'
    )
    parsed = parsed_of(code)
    imports = {(r.qualifier, r.target_name) for r in parsed.references if r.kind == "import"}
    assert imports == {
        ("fmt", "fmt"),
        ("strings", "str"),
        ("math", "math"),
        ("embed", "_"),
        ("net.http", "http"),
        ("os", "os"),
        (None, "d"),  # not expressible as identifiers: only the last token is kept
        (None, "y"),
    }
    assert {r.source for r in parsed.references} == {None}
    assert {r.confidence for r in parsed.references} == {"syntactic"}


def test_an_unrepresentable_import_never_takes_the_file_down() -> None:
    code = (
        HEAD + 'import "github.com/foo-bar/x"\nimport "a\\u00e9/b"\nimport "a b/c"\nfunc f() {}\n'
    )
    parsed = parsed_of(code)
    assert "pkg.mod.f" in names_of(parsed)
    assert "file_degraded" not in codes_of(parsed)


def test_calls_are_qualified_by_the_imported_package_and_always_heuristic() -> None:
    code = (
        'package mod\n\nimport (\n\t"fmt"\n\tp "os/path"\n\t"gopkg.in/yaml.v3"\n\t"a/lib/v2"\n)\n\n'
        "func f(x T) {\n\tfmt.Println()\n\tp.Join()\n\tyaml.Marshal()\n\tlib.Do()\n\tx.Method()\n"
        "\tother.Call()\n\tmissing()\n\tf()\n}\n"
    )
    parsed = parsed_of(code)
    assert refs_of(parsed) >= {
        ("pkg.mod.f", "call", "fmt", "Println"),
        ("pkg.mod.f", "call", "p", "Join"),
        ("pkg.mod.f", "call", "yaml", "Marshal"),
        ("pkg.mod.f", "call", "lib", "Do"),
        ("pkg.mod.f", "call", None, "missing"),
    }
    calls = {item for item in refs_of(parsed) if item[1] == "call"}
    assert len(calls) == 5  # x.Method, other.Call (not imports) and the local f() are absent
    assert edges_of(parsed) == {("pkg.mod.f", "calls", "pkg.mod.f")}
    assert {r.confidence for r in parsed.references if r.kind == "call"} == {"heuristic"}


def test_receiver_calls_resolve_to_the_types_methods_only() -> None:
    code = (
        HEAD
        + "type T struct{}\nfunc (t *T) A() { t.B(); t.Missing(); u.B() }\nfunc (t T) B() {}\n"
        + "func (u *U) B() {}\nfunc (u *U) C() { u.B() }\n"
    )
    assert edges_of(parsed_of(code)) == {
        ("pkg.mod.T.A", "calls", "pkg.mod.T.B"),
        ("pkg.mod.U.C", "calls", "pkg.mod.U.B"),
    }


def test_embedded_types_are_inherit_relations_or_references() -> None:
    code = (
        'package mod\n\nimport "io"\n\ntype B struct{}\ntype I interface{ M() }\n'
        "type S struct {\n\tB\n\t*Other\n\tio.Reader\n\tG[int]\n\tf int\n\tg, h int\n}\n"
        "type J interface {\n\tI\n\tio.Closer\n\tint | string\n\tX()\n}\n"
    )
    parsed = parsed_of(code)
    assert edges_of(parsed) == {
        ("pkg.mod.S", "inherits", "pkg.mod.B"),
        ("pkg.mod.J", "inherits", "pkg.mod.I"),
    }
    assert {item for item in refs_of(parsed) if item[1] == "inherit"} == {
        ("pkg.mod.S", "inherit", None, "Other"),
        ("pkg.mod.S", "inherit", "io", "Reader"),
        ("pkg.mod.S", "inherit", None, "G"),
        ("pkg.mod.J", "inherit", "io", "Closer"),
    }


def test_calls_at_package_level_belong_to_the_module() -> None:
    code = 'package mod\n\nimport "os"\n\nvar home = os.Getenv("H")\nvar x = g()\n\nfunc g() int { return 1 }\n'
    parsed = parsed_of(code)
    assert ("", "call", "os", "Getenv") in refs_of(parsed)
    assert edges_of(parsed) == {("pkg.mod", "calls", "pkg.mod.g")}


def test_calls_inside_a_dropped_declaration_are_not_attributed_to_anyone() -> None:
    code = HEAD + "func bad( {\n\tmissing()\n}\n\nfunc ok() {}\n"
    parsed = parsed_of(code)
    assert refs_of(parsed) == set()
    assert {"pkg.mod.ok", "pkg.mod"} == set(names_of(parsed))


def test_more_than_eight_candidates_yield_no_edge_and_no_reference() -> None:
    code = HEAD + "".join("func x() {}\n" for _ in range(9)) + "func caller() { x() }\n"
    parsed = parsed_of(code)
    assert parsed.relations == ()
    assert parsed.references == ()
    few = HEAD + "func x() {}\nfunc x() {}\nfunc caller() { x() }\n"
    assert len(parsed_of(few).relations) == 2


def test_first_call_site_per_target_is_kept_and_edges_are_capped() -> None:
    assert len(parsed_of(HEAD + "func a() { b(); b() }\nfunc b() {}\n").relations) == 1
    many = "".join(f"func g{n}() {{}}\n" for n in range(100))
    calls = "func caller() {\n" + "".join(f"\tg{n}()\n" for n in range(100)) + "}\n"
    assert len(parsed_of(HEAD + many + calls).relations) == 64


def test_reference_caps_are_reported_not_fatal() -> None:
    calls = "func caller() {\n" + "".join(f"\tunknown{n}()\n" for n in range(200)) + "}\n"
    parsed = parsed_of(HEAD + calls)
    assert len(parsed.references) == MAX_REFERENCES_PER_SYMBOL
    assert codes_of(parsed) == {"references_capped": 200 - MAX_REFERENCES_PER_SYMBOL}
    imports = "".join(f'import "p{n}/q"\n' for n in range(MAX_MODULE_REFERENCES + 10))
    module_level = parsed_of(HEAD + imports)
    assert len(module_level.references) == MAX_MODULE_REFERENCES
    assert codes_of(module_level)["references_capped"] == 10


# --- fingerprints (SymbolRevision identity) -------------------------------------------------

PLAIN = (
    HEAD
    + "// F adds.\nfunc F(a, b int, c string) int {\n\t// note\n\tx := a + b\n"
    + '\ts := `raw\nline`\n\treturn x + len(c) + len(s + "s")\n}\n'
)
FORMATTED = (
    HEAD
    + "/* F adds. */\nfunc F(\n\ta, b int,\n\tc string,\n) int {\n\tx := a + b // other\n"
    + '\ts := `raw\nline`\n\treturn x + len(c) + len(s + "s")\n}\n'
)


def test_reformatting_and_comments_keep_both_digests() -> None:
    assert digests(PLAIN, "F") == digests(FORMATTED, "F")


def test_changed_body_changes_the_fingerprint_not_the_signature() -> None:
    signature, body = digests(PLAIN, "F")
    for changed in (PLAIN.replace("a + b", "a - b"), PLAIN.replace('"s"', '"t"')):
        new_signature, new_body = digests(changed, "F")
        assert new_signature == signature
        assert new_body != body


def test_changed_signature_changes_the_revision() -> None:
    assert digests(PLAIN, "F")[0] != digests(PLAIN.replace("c string", "c []byte"), "F")[0]
    method = HEAD + "type T struct{}\nfunc (t T) M() {}\n"
    assert digests(method, "T.M")[0] != digests(method.replace("(t T)", "(t *T)"), "T.M")[0]


def test_crlf_keeps_the_revision_including_inside_raw_strings() -> None:
    crlf = PLAIN.replace("\n", "\r\n")
    assert digests(PLAIN, "F") == digests(crlf, "F")
    assert digests(PLAIN, "F") != digests(PLAIN.replace("raw", "rew"), "F")


def test_digests_are_deterministic_across_runs_and_hash_seeds() -> None:
    assert digests(PLAIN, "F") == digests(PLAIN, "F")
    script = (
        "import json,sys\n"
        "from agent_context_platform.indexing.tree_sitter.go import parse_go\n"
        "from agent_context_platform.indexing.tree_sitter.base import ParseRequest, SourceFile\n"
        "req = ParseRequest(files=(SourceFile.from_bytes('pkg/mod.go','go',sys.stdin.buffer.read()),))\n"
        "print(json.dumps([[s.signature_digest, s.semantic_fingerprint] for s in parse_go(req).files[0].symbols]))\n"
    )
    outputs = set()
    for seed in ("0", "1", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        run = subprocess.run(
            [sys.executable, "-c", script],
            input=PLAIN.encode(),
            capture_output=True,
            env=env,
            check=True,
        )
        outputs.add(run.stdout)
    assert len(outputs) == 1


def test_outer_module_revision_changes_when_a_function_body_changes() -> None:
    def module_digest(code: str) -> str:
        return next(s for s in parsed_of(code).symbols if s.kind == "module").semantic_fingerprint

    assert module_digest(PLAIN) != module_digest(PLAIN.replace("a + b", "a - b"))
    assert module_digest(PLAIN) == module_digest(PLAIN.replace("// note", "// other"))


# --- robustness -----------------------------------------------------------------------------


def test_broken_declarations_are_dropped_and_reported_siblings_kept() -> None:
    code = (
        HEAD
        + "func good() { x := 1 }\nfunc bad() {\n\tx := := 1\n}\nfunc after() {}\n"
        + "type (\n\tOK struct{}\n\tBroken struct {\n\t\ta int ]\n\t}\n\tAlso int\n)\n"
    )
    parsed = parsed_of(code)
    names = set(names_of(parsed))
    assert {"pkg.mod", "pkg.mod.good", "pkg.mod.after", "pkg.mod.OK"} <= names
    assert "pkg.mod.bad" not in names
    assert codes_of(parsed).get("syntax_recovered", 0) >= 1
    assert not {n for n in names if "Broken" in n}


def test_garbage_never_crashes() -> None:
    for junk in (b"\x00\x01\x02", b")))(((", b"func\n", b"type :", b"@\n@\n", b"\xff\xfe" * 50):
        assert in_process(request(source(PATH, junk))).files


def test_invalid_utf8_and_names_glued_to_non_ascii_bytes_are_skipped() -> None:
    code = b'package mod\n\nfunc ok() {}\n\nfunc caf\xe9() {}\n\nvar x = "\xff"\nfunc y() {}\n'
    names = set(names_of(parsed_of(code)))
    assert {"pkg.mod.ok", "pkg.mod.y"} <= names
    assert not any("caf" in n for n in names)


def test_unicode_identifiers_are_kept() -> None:
    assert "pkg.mod.café" in names_of(parsed_of("package mod\n\nfunc café() {}\n"))


def test_crlf_and_bom_offsets_refer_to_the_raw_bytes() -> None:
    lf = HEAD + "func f(a int) int {\n\treturn a\n}\n"
    crlf = lf.replace("\n", "\r\n").encode()
    parsed = parsed_of(b"\xef\xbb\xbf" + crlf)
    (item,) = (s for s in parsed.symbols if s.kind == "function")
    assert item.signature == "func f(a int) int"
    assert item.start_byte == 3 + len("package mod\r\n\r\n")
    assert "syntax_recovered" not in codes_of(parsed)
    assert digests(lf, "f")[1] == digests(crlf.decode(), "f")[1]


def test_empty_and_whitespace_only_files() -> None:
    assert parsed_of(b"").symbols == ()
    assert codes_of(parsed_of(b"")) == {}
    assert names_of(parsed_of(b"\n\n")) == {"pkg": "module"}
    assert [s.kind for s in parsed_of(b"package mod\n").symbols] == ["module"]


def test_non_identifier_paths_do_not_get_the_batch_refused() -> None:
    files = [
        source("my-pkg/2024/x-y.go", "package p\nfunc f() {}\n"),
        source("src/my-pkg/mod.go", "package mod\nfunc g() {}\n"),
        source("main.go", "package main\ntype C struct{}\n"),
    ]
    module = in_process(request(*files))
    names = [{s.qualified_name for s in parsed.symbols} for parsed in module.files]
    assert names == [{"p", "p.f"}, {"mod", "mod.g"}, {"main", "main.C"}]


def test_deep_nesting_uses_no_recursion() -> None:
    depth = 400
    nested = "func f() {\n" + "if true {\n" * depth + "}\n" * depth + "}\n"
    assert "pkg.mod.f" in names_of(parsed_of(HEAD + nested))
    for expr in ("(" * 3000 + "1" + ")" * 3000, "[]int{" * 5000 + "}" * 5000):
        assert in_process(request(source(PATH, HEAD + "var x = " + expr)))


def _contained(module: ParsedModule, paths: list[str]) -> None:
    """Outcome check that does not depend on machine speed: every file answered, and a file
    without structure says why."""
    assert [item.path for item in module.files] == paths
    for item in module.files:
        if not item.symbols:
            assert "file_degraded" in codes_of(item), item.path


def test_a_huge_single_expression_through_the_sandbox() -> None:
    code = HEAD + "var x = 1" + " + 1" * ((MAX_SOURCE_BYTES - 40) // 4) + "\nfunc after() {}\n"
    files = [source("pkg/ok.go", HEAD + "func a() {}\n"), source(PATH, code)]
    module = go_adapter(limits=PATIENT).parse(request(*files))
    _contained(module, [f.path for f in files])
    assert len(module.files[0].symbols) == 2


def test_huge_file_of_definitions_through_the_sandbox_within_limits() -> None:
    code = HEAD + "".join(
        f"func f{n}(a, b int) int {{\n\treturn a + b + {n}\n}}\n" for n in range(30_000)
    )
    module = go_adapter(limits=PATIENT).parse(request(source(PATH, code[:MAX_SOURCE_BYTES])))
    _contained(module, [PATH])
    assert len(module.files[0].symbols) <= 10_000


def test_huge_names_and_signatures_stay_within_the_contract() -> None:
    code = HEAD + f"func {'a' * 600}() {{}}\n\nfunc ok({'p int, ' * 400}q int) {{}}\n"
    parsed = parsed_of(code)
    names = set(names_of(parsed))
    assert "pkg.mod.ok" in names
    assert not any("a" * 600 in n for n in names)
    assert codes_of(parsed)["symbols_dropped"] >= 1
    (ok,) = (s for s in parsed.symbols if s.qualified_name == "pkg.mod.ok")
    assert 0 < len(ok.signature.encode()) <= 256


def test_control_characters_in_signatures_are_cut() -> None:
    code = b'package mod\n\nvar x = "\x01\x02"\n'
    (item,) = (s for s in parsed_of(code).symbols if s.kind == "variable")
    assert "\x01" not in item.signature


def test_a_file_over_the_work_budget_degrades_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common.Budget, "MAX_NODES_PER_FILE", 200)
    big = "var x = []int{" + ",".join("1" for _ in range(500)) + "}\n"
    module = in_process(
        request(
            source("pkg/a.go", "package a\nfunc a() {}\n"),
            source("pkg/big.go", "package big\n" + big + "func b() {}\n"),
            source("pkg/c.go", "package c\nfunc c() {}\n"),
        )
    )
    assert {item.path: len(item.symbols) for item in module.files} == {
        "pkg/a.go": 2,
        "pkg/big.go": 0,
        "pkg/c.go": 2,
    }
    degraded = module.files[1]
    assert codes_of(degraded) == {"file_degraded": 1, "work_budget_exceeded": 1}
    assert module.files[0].diagnostics == ()


def test_a_real_1mib_file_over_the_default_budget_degrades_and_the_batch_survives() -> None:
    literal = "var x = []int{" + ",".join("1" for _ in range(MAX_SOURCE_BYTES // 2 - 20)) + "}\n"
    files = [source("pkg/ok.go", HEAD + "func a() {}\n"), source("pkg/big.go", HEAD + literal)]
    module = go_adapter(limits=PATIENT).parse(request(*files))
    _contained(module, [f.path for f in files])
    assert len(module.files[0].symbols) == 2
    assert module.files[1].symbols == ()
    assert "work_budget_exceeded" in codes_of(module.files[1])


def test_bugs_in_the_adapter_are_not_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_args: object) -> ParsedFile:
        raise KeyError("bug")

    monkeypatch.setattr(go, "_parse_file", broken)
    with pytest.raises(KeyError):
        parse_go(request(source(PATH, HEAD)))


def test_a_one_megabyte_chained_call_is_contained_through_the_sandbox() -> None:
    chain = HEAD + "func f() {\n\ta" + ".f()" * (MAX_SOURCE_BYTES // 4 - 8) + "\n}\n"
    files = [source("pkg/ok.go", HEAD + "func a() {}\n"), source(PATH, chain)]
    before = child_cpu()
    module = go_adapter(limits=PATIENT).parse(request(*files))
    print("chain child cpu", child_cpu() - before)
    _contained(module, [f.path for f in files])
    assert len(module.files[0].symbols) == 2


def test_a_one_megabyte_member_expression_is_contained_through_the_sandbox() -> None:
    chain = HEAD + "var x = a" + ".b" * (MAX_SOURCE_BYTES // 2 - 20) + "\n"
    files = [source("pkg/ok.go", HEAD + "func a() {}\n"), source(PATH, chain)]
    module = go_adapter(limits=PATIENT).parse(request(*files))
    _contained(module, [f.path for f in files])
    assert len(module.files[0].symbols) == 2


def test_many_same_named_definitions_and_calls_over_the_candidate_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # In process with the CPU limits lifted: the outcome is decided by the candidate cap alone.
    monkeypatch.setattr(_common.Budget, "CPU_SOFT_LIMIT", 1e9)
    monkeypatch.setattr(_common.Budget, "PARSE_SOFT_LIMIT", 1e9)
    calls = "func caller() {\n" + "".join("\tf()\n\tlen(x)\n" for _ in range(30_000)) + "}\n"
    defs = "".join("func f() {}\n" for _ in range(9_000))
    code = (HEAD + calls + defs)[:MAX_SOURCE_BYTES]
    (parsed,) = in_process(request(source(PATH, code))).files
    assert any(s.qualified_name.endswith(".caller") for s in parsed.symbols)
    assert len(parsed.symbols) > 1_000
    assert parsed.relations == ()  # more than 8 candidates: no edge
    assert not any(r.target_name == "len" for r in parsed.references)


def test_a_long_license_header_does_not_hide_the_package_clause() -> None:
    header = "".join(f"// license line {n}\n" for n in range(40))
    (parsed,) = in_process(
        request(source("cmd/x/main.go", header + "package main\n\nfunc run() {}\n"))
    ).files
    assert {s.qualified_name for s in parsed.symbols} >= {"cmd.x.main", "cmd.x.main.run"}


def test_imports_are_bound_by_their_alias_or_last_non_version_token() -> None:
    code = (
        'package p\n\nimport (\n\tstr "strings"\n\t. "math"\n\t_ "embed"\n\t"fmt"\n'
        '\t"gopkg.in/yaml.v3"\n\t"example.com/lib/v2"\n)\n'
    )
    (parsed,) = in_process(request(source("p/a.go", code))).files
    got = {
        (r.qualifier, r.target_name, code[r.start_byte : r.end_byte])
        for r in parsed.references
        if r.kind == "import"
    }
    assert got == {
        ("strings", "str", "str"),
        ("math", "math", "math"),
        ("embed", "_", "_"),
        ("fmt", "fmt", "fmt"),
        ("gopkg.in.yaml.v3", "yaml", "yaml"),
        ("example.com.lib.v2", "lib", "lib"),
    }


def test_dropped_broken_declarations_never_charge_their_budget() -> None:
    broken = "".join(f"func b{n}() {{\n\tx := := 1\n}}\n" for n in range(9_000))
    long_name = "n" * 400
    heavy = "".join(f"func {long_name}{n}() {{\n\tx := := 1\n}}\n" for n in range(400))
    valid = "".join(f"func ok{n}() {{}}\n" for n in range(50))
    files = [
        source("pkg/a.go", "package a\n" + broken + valid),
        source("pkg/b.go", "package b\n" + heavy + valid),
    ]
    for parsed in in_process(request(*files)).files:
        names = {s.qualified_name for s in parsed.symbols}
        stem = parsed.path[4]
        assert {f"pkg.{stem}.ok{n}" for n in range(50)} <= names
        assert "symbols_dropped" not in codes_of(parsed)


def test_one_construct_never_duplicates_a_symbol_or_reference() -> None:
    parsed = parsed_of(
        HEAD
        + "var a, a = 1, 2\nconst b, b = 1, 2\n"
        + 'import "fmt"\nimport "fmt"\nfunc f() { fmt.X(); fmt.X() }\n'
    )
    assert sorted(s.qualified_name for s in parsed.symbols) == [
        "pkg.mod",
        "pkg.mod.a",
        "pkg.mod.b",
        "pkg.mod.f",
    ]
    assert len(parsed.references) == 2  # one import, one call: repeats are one reference


def test_output_over_budget_sheds_references_then_relations_and_says_so(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    req = request(
        source("pkg/a.go", 'package a\nimport "os"\nfunc a() { b(); os.Exit(1) }\nfunc b() {}\n'),
        source("pkg/c.go", "package c\nfunc c() {}\n"),
    )
    full = parse_go(req).files[0]
    assert full.references and full.relations
    lean = full.model_copy(update={"references": ()})
    monkeypatch.setattr(_common, "OUTPUT_BUDGET", len(lean.model_dump_json()) + 60)
    shed = parse_go(req).files[0]
    assert shed.references == () and shed.relations and shed.symbols
    assert "references_capped" in codes_of(shed)
    monkeypatch.setattr(_common, "OUTPUT_BUDGET", 10)
    assert [item.symbols for item in parse_go(req).files] == [(), ()]


def test_a_batch_of_garbage_files_is_contained_not_a_child_death() -> None:
    """6 x 1 MB of parser-hostile bytes: files degrade with a reason, the child never dies."""
    garbage = (b"x := (\n" * 200_000)[:1_000_000]  # error recovery is superlinear here
    files = [source("pkg/ok.go", HEAD + "func a() {}\n")]
    files += [source(f"pkg/g{n}.go", garbage) for n in range(6)]
    before = child_cpu()
    module = go_adapter(limits=PATIENT).parse(request(*files))
    print("garbage child cpu", child_cpu() - before)
    _contained(module, [f.path for f in files])
    assert len(module.files[0].symbols) == 2
    assert all(codes_of(item).get("work_budget_exceeded") for item in module.files[1:])


def test_the_cpu_backstop_is_checked_before_and_during_a_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_common.Budget, "CPU_SOFT_LIMIT", -1.0)
    module = in_process(request(source(PATH, HEAD + "func a() {}\n")))
    assert module.files[0].symbols == ()
    assert "work_budget_exceeded" in codes_of(module.files[0])


@pytest.mark.parametrize("limit", ["CPU_SOFT_LIMIT", "PARSE_SOFT_LIMIT"])
def test_a_hostile_parse_is_cut_inside_the_parse(
    monkeypatch: pytest.MonkeyPatch, limit: str
) -> None:
    monkeypatch.setattr(_common.Budget, limit, 0.3)
    hostile = (b"x := (\n" * 200_000)[:1_000_000]
    module = in_process(request(source(PATH, hostile)))
    assert module.files[0].symbols == ()
    assert codes_of(module.files[0]) == {"file_degraded": 1, "work_budget_exceeded": 1}


@pytest.mark.skipif(landlock.abi_version() < 1, reason="kernel lacks Landlock")
def test_the_real_adapter_runs_confined_and_cannot_read_a_sibling_file(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    sibling = tmp_path / "sibling.txt"
    sibling.write_text("secret sibling")
    limits = Limits(checkout_roots=(str(checkout),), wall_seconds=15.0)
    module = go_adapter(limits=limits).parse(fixture_request())
    assert normalize_module(module) == json.loads((FIXTURES / "expected.json").read_text())
    assert any(item.symbols for item in module.files)
    command = [sys.executable, "-m", go.__name__]
    rules = landlock.read_set(command)
    real = os.path.realpath(sibling)
    assert not any(real == r or real.startswith(r.rstrip("/") + "/") for r in rules.read)
    fake = str(Path(__file__).with_name("fake_adapter.py"))
    raw = runner._run(
        [sys.executable, fake, "read_other_file", str(sibling)],
        fixture_request().model_dump_json().encode(),
        limits,
        {},
    )
    assert json.loads(raw)["probe"][f"read:{sibling}"] == "EACCES"


def test_a_memory_starved_child_fails_contained_not_hung() -> None:
    limits = Limits(address_space_bytes=64 * 1024 * 1024, wall_seconds=15.0)
    time.monotonic()
    with pytest.raises(StructuralError):
        go_adapter(limits=limits).parse(request(source(PATH, HEAD + "func f() {}\n" * 50_000)))


def _refs(parsed: ParsedFile, kind: str) -> set[tuple[str | None, str]]:
    return {(r.qualifier, r.target_name) for r in parsed.references if r.kind == kind}


def test_an_oversized_import_alias_or_qualifier_never_degrades_the_file() -> None:
    long_alias = "a" * 600
    path = "/".join(["d" * 200] * 4)  # under the 1024-byte import limit, qualifier over 512
    code = f'package p\n\nimport {long_alias} "os"\nimport "{path}"\nimport "{"t" * 600}"\nfunc F() {{}}\n'
    parsed = parsed_of(code)
    assert any(s.qualified_name.endswith(".F") for s in parsed.symbols)
    assert not {d.code for d in parsed.diagnostics} & {"file_degraded", "symbols_dropped"}
    imports = [r for r in parsed.references if r.kind == "import"]
    assert [r.target_name for r in imports] == ["d" * 200]
    assert imports[0].qualifier is None


def test_an_escaped_import_path_keeps_its_final_element() -> None:
    code = 'package p\n\nimport "example.com/a\\u002db/pkg"\nfunc F() { pkg.G() }\n'
    parsed = parsed_of(code)
    assert _refs(parsed, "import") == {(None, "pkg")}
    assert ("pkg", "G") in _refs(parsed, "call")
    assert parsed_of('package p\n\nimport "a\\u002db"\n').references == ()


def test_a_rejected_module_symbol_is_reported_as_degradation() -> None:
    directory = "/".join(["d" * 200] * 4)
    (parsed,) = in_process(request(source(f"{directory}/x.go", "package p\n\nfunc F() {}\n"))).files
    assert parsed.symbols == ()
    assert {d.code for d in parsed.diagnostics} == {"file_degraded", "symbols_dropped"}


def test_generic_instantiations_are_calls() -> None:
    code = (
        'package p\n\nimport "slices"\n\nfunc F[T any]() {}\n'
        "func G() { F[int](); slices.Max[int](nil); H[string](1) }\n"
    )
    parsed = parsed_of(code)
    assert any(r.kind == "calls" for r in parsed.relations)
    assert _refs(parsed, "call") == {("slices", "Max"), (None, "H")}


def test_a_receiver_call_without_a_local_method_is_an_unresolved_reference() -> None:
    code = "package p\n\ntype T struct{}\nfunc (t T) A() { t.B(); t.A() }\n"
    parsed = parsed_of(code)
    assert _refs(parsed, "call") == {("t", "B")}
    assert len(parsed.relations) == 1  # t.A() resolves inside the file


def _revisions(code: str) -> dict[str, tuple[str, str]]:
    parsed = parsed_of("package mod\n\n" + code)
    return {
        s.qualified_name: (s.semantic_fingerprint, s.signature_digest)
        for s in parsed.symbols
        if s.kind != "module"
    }


def _changed(before: str, after: str) -> set[str]:
    old, new = _revisions(before), _revisions(after)
    assert old.keys() == new.keys()
    return {name for name in old if old[name] != new[name]}


def test_editing_one_declarator_of_a_multi_name_spec_leaves_the_others_alone() -> None:
    before = "var a, b, c = func() { x() }, func() { y() }, 3\n"
    assert _changed(before, before.replace("y()", "z()")) == {"pkg.mod.b"}
    assert _changed(before, before.replace(", 3", ", 4")) == {"pkg.mod.c"}
    assert _changed(before, before.replace("x()", "w()")) == {"pkg.mod.a"}
    assert len({v for v in _revisions(before).values()}) == 3


def test_a_shared_type_or_value_changes_every_name_of_the_spec() -> None:
    assert _changed("var a, b int\n", "var a, b int64\n") == {"pkg.mod.a", "pkg.mod.b"}
    assert _changed("var a, b = f()\n", "var a, b = g()\n") == {"pkg.mod.a", "pkg.mod.b"}
    assert _changed("var a, _, b = 1, 2, 3\n", "var a, _, b = 1, 9, 3\n") == set()


def test_grouped_declarations_are_digested_per_spec() -> None:
    group = (
        "var (\n\tc = 1\n\td = 2\n)\nconst (\n\te = 1\n\tf = 2\n)\ntype (\n\tG int\n\tH string\n)\n"
    )
    assert _changed(group, group.replace("d = 2", "d = 3")) == {"pkg.mod.d"}
    assert _changed(group, group.replace("f = 2", "f = 3")) == {"pkg.mod.f"}
    assert _changed(group, group.replace("H string", "H bool")) == {"pkg.mod.H"}
    assert _changed(group, group.replace("(\n\tc = 1", "(\n\tc = 1 // note")) == set()


def _realistic_go(n: int) -> str:
    parts = [
        f"// Package svc{n} is generated for the corpus test.\npackage svc{n}\n\n"
        'import (\n\t"context"\n\t"fmt"\n\tstr "strings"\n\t"example.com/lib/v2"\n)\n\n'
    ]
    for k in range(26):
        parts.append(
            f"// Handler{k} handles one kind of request.\n"
            f"type Handler{k} struct {{\n\tname string\n\tlimit int\n\tnext *Handler{(k + 1) % 26}\n}}\n\n"
            f"func New{k}(name string) *Handler{k} {{\n\treturn &Handler{k}{{name: name, limit: {k}}}\n}}\n\n"
            f"func (h *Handler{k}) Serve(ctx context.Context, arg string) (string, error) {{\n"
            f'\tif h.limit > len(arg) {{\n\t\treturn "", fmt.Errorf("short %s", arg)\n\t}}\n'
            f"\tfor i := 0; i < h.limit; i++ {{\n\t\targ = str.ToUpper(arg) + h.name\n\t\tlog{k}(arg)\n\t}}\n"
            f"\tout := lib.Do(ctx, arg)\n\treturn out, h.check{k}(out)\n}}\n\n"
            f'func (h *Handler{k}) check{k}(v string) error {{\n\tif v == "" {{\n\t\treturn fmt.Errorf("empty")\n\t}}\n\treturn nil\n}}\n\n'
            f"func log{k}(v string) {{\n\tfmt.Println(v, {k})\n}}\n\n"
        )
    return "".join(parts)


def test_a_normal_one_mebibyte_request_degrades_nothing_through_the_sandbox() -> None:
    files = [source(f"app/pkg{n % 9}/svc{n}.go", _realistic_go(n)) for n in range(64)]
    total = sum(len(item.content()) for item in files)
    assert 0.9 * 1024 * 1024 < total <= 1.1 * 1024 * 1024, total
    before = child_cpu()
    module = go_adapter(limits=PATIENT).parse(request(*files))
    print(f"cpu[normal_corpus_{total // 1000}KB] = {child_cpu() - before:.2f}s")
    assert [item.path for item in module.files] == [item.path for item in files]
    for item in module.files:
        assert codes_of(item) == {}, item.path
        assert len(item.symbols) > 40
        assert item.references
        assert item.relations


def test_the_typescript_memory_blowup_input_is_contained_in_the_go_child_too() -> None:
    # Not Go syntax: error recovery on hostile generic-like text. The outcome is what matters.
    hostile = MEMORY_BLOWUP_INPUT
    files = [source("pkg/ok.go", HEAD + "func a() {}\n"), source("pkg/blow.go", hostile)]
    try:
        module = go_adapter(limits=Limits(wall_seconds=60.0)).parse(request(*files))
    except StructuralError:
        return  # child died under RLIMIT_AS: contained, not a hang
    _contained(module, [f.path for f in files])


def test_import_limits_are_measured_in_utf8_bytes() -> None:
    wide = "é" * 300  # 300 characters, 600 bytes
    code = f'package p\n\nimport "{wide}"\nimport "a/{"é" * 200}/{"é" * 200}"\nfunc F() {{}}\n'
    parsed = parsed_of(code)
    assert any(s.qualified_name.endswith(".F") for s in parsed.symbols)
    assert "file_degraded" not in codes_of(parsed)
    assert codes_of(parsed).get("references_capped", 0) >= 1
    assert all(len(r.target_name.encode()) <= 512 for r in parsed.references)
    assert all(r.qualifier is None or len(r.qualifier.encode()) <= 512 for r in parsed.references)


def test_a_rejected_package_name_degrades_instead_of_guessing_an_identity() -> None:
    code = f"package {'p' * 600}\n\nfunc F() {{}}\n"
    (parsed,) = in_process(request(source("src/x/a.go", code))).files
    assert parsed.symbols == ()
    assert codes_of(parsed) == {"file_degraded": 1, "symbols_dropped": 1}


def test_unicode_directory_components_stay_in_the_module_name() -> None:
    (parsed,) = in_process(request(source("src/café/x.go", "package p\n\nfunc F() {}\n"))).files
    assert {s.qualified_name for s in parsed.symbols} == {
        "src.café.p",
        "src.café.p.F",
    }
    assert "file_degraded" not in codes_of(parsed)


def test_parenthesized_and_nested_callees_are_calls() -> None:
    code = (
        'package p\n\nimport "fmt"\n\nfunc F() {}\n'
        "func G() { (F)(); ((fmt.Println))(1); (F[int])() }\n"
    )
    parsed = parsed_of(code)
    assert ("fmt", "Println") in _refs(parsed, "call")
    assert any(r.kind == "calls" for r in parsed.relations)
