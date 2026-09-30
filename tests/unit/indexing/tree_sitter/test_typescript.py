"""TypeScript/JavaScript structural adapter: golden output through the real sandbox."""

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

from agent_context_platform.indexing.tree_sitter import _common, landlock, typescript
from agent_context_platform.indexing.tree_sitter._common import normalize_module
from agent_context_platform.indexing.tree_sitter.base import (
    MAX_SOURCE_BYTES,
    ParsedFile,
    ParsedModule,
    ParseRequest,
    SourceFile,
    StructuralAdapter,
    StructuralError,
    StructuralErrorCode,
    parser_fingerprint,
    validate_module,
)
from agent_context_platform.indexing.tree_sitter.runner import Limits
from agent_context_platform.indexing.tree_sitter.typescript import (
    ADAPTER_NAME,
    ADAPTER_VERSION,
    FINGERPRINT,
    GRAMMAR_VERSIONS,
    parse_typescript,
    typescript_adapter,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).with_name("fixtures") / "typescript"


def fixture_request() -> ParseRequest:
    paths = sorted(p for p in FIXTURES.rglob("*") if p.is_file() and p.suffix != ".json")
    return ParseRequest(
        files=tuple(
            SourceFile.from_bytes(str(p.relative_to(FIXTURES)), "typescript", p.read_bytes())
            for p in paths
        )
    )


def test_golden_through_the_real_sandboxed_subprocess() -> None:
    expected = json.loads((FIXTURES / "expected.json").read_text())
    assert normalize_module(typescript_adapter().parse(fixture_request())) == expected


def source(path: str, content: bytes | str) -> SourceFile:
    raw = content.encode() if isinstance(content, str) else content
    return SourceFile.from_bytes(path, "typescript", raw)


def request(*files: SourceFile) -> ParseRequest:
    return ParseRequest(files=files)


def in_process(req: ParseRequest) -> ParsedModule:
    return validate_module(req, parse_typescript(req), expected_fingerprint=FINGERPRINT)


def parsed(content: bytes | str, path: str = "src/mod.ts") -> ParsedFile:
    return in_process(request(source(path, content))).files[0]


def names(file: ParsedFile) -> dict[str, str]:
    return {item.qualified_name: item.kind for item in file.symbols}


def codes(file: ParsedFile) -> dict[str, int]:
    return {item.code: item.count for item in file.diagnostics}


def refs(file: ParsedFile, kind: str) -> list[tuple[str | None, str, str | None, int]]:
    return [
        (item.source, item.target_name, item.qualifier, item.relative_level)
        for item in file.references
        if item.kind == kind
    ]


def digests(content: str, name: str, path: str = "src/mod.ts") -> tuple[str, str]:
    item = next(x for x in parsed(content, path).symbols if x.qualified_name == name)
    return item.signature_digest, item.semantic_fingerprint


def children_cpu() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


# --- contract -------------------------------------------------------------------------------


def test_in_process_output_equals_the_sandboxed_output() -> None:
    req = fixture_request()
    assert normalize_module(in_process(req)) == normalize_module(typescript_adapter().parse(req))


def test_adapter_contract() -> None:
    adapter = typescript_adapter()
    assert isinstance(adapter, StructuralAdapter)
    assert adapter.language == "typescript"
    assert adapter.fingerprint == parser_fingerprint(
        ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS
    )
    req = fixture_request()
    module = adapter.parse(req)
    assert [item.path for item in module.files] == [item.path for item in req.files]
    for item in module.files:
        assert item.parser_fingerprint == adapter.fingerprint
        assert [x.ref for x in item.symbols] == [str(n) for n in range(len(item.symbols))]
        assert {x.evidence_kind for x in item.symbols} <= {"tree_sitter"}
        assert {x.kind for x in item.relations} <= {"calls", "inherits"}
        assert {x.kind for x in item.references} <= {"import", "call", "inherit"}


def test_every_extension_routes_to_a_grammar() -> None:
    for extension in ("ts", "tsx", "js", "jsx", "mjs", "cjs", "mts", "cts"):
        item = parsed("export function f() { return 1 }\n", f"src/m.{extension}")
        assert names(item) == {"src.m": "module", "src.m.f": "function"}, extension
    assert names(parsed("const a = <div>{x}</div>;\n", "src/v.jsx")) == {
        "src.v": "module",
        "src.v.a": "constant",
    }


def test_grammar_versions_match_the_installed_distributions() -> None:
    for name, version in GRAMMAR_VERSIONS.items():
        assert metadata.version(name) == version
    assert parser_fingerprint(ADAPTER_NAME, ADAPTER_VERSION, GRAMMAR_VERSIONS) == FINGERPRINT


def test_module_names_come_from_the_path() -> None:
    assert typescript._module_name("src/a/b.service.ts") == "src.a.b.service"
    assert typescript._module_name("src/a/index.tsx") == "src.a"
    assert typescript._module_name("index.js") == ""
    assert typescript._module_name("src/lib.d.ts") == "src.lib"
    assert typescript._module_name("src/user-service.ts") == ""
    assert typescript._module_name("Makefile") == "Makefile"


# --- kinds ----------------------------------------------------------------------------------

KINDS = """
export interface I extends J {}
type T = string;
export enum E { A }
declare namespace N.M { function g(): void }
abstract class C extends B implements I {
  x = 1;
  static y: number;
  m() {}
  get v() { return 1 }
  set v(a) {}
  arrow = () => 1;
  abstract z(): void;
  #p = 2;
  constructor(public q: number) { super() }
}
export default class {}
const a = () => 1, b = function () {};
let c = 1;
var d;
const {e} = obj;
function* gen() {}
export const K = class {};
"""


def test_kinds_and_qualified_names() -> None:
    assert names(parsed(KINDS)) == {
        "src.mod": "module",
        "src.mod.I": "interface",
        "src.mod.T": "type",
        "src.mod.E": "type",
        "src.mod.N": "module",
        "src.mod.N.M": "module",
        "src.mod.N.M.g": "function",
        "src.mod.C": "class",
        "src.mod.C.x": "field",
        "src.mod.C.y": "field",
        "src.mod.C.m": "method",
        "src.mod.C.v": "property",
        "src.mod.C.arrow": "method",
        "src.mod.C.z": "method",
        "src.mod.C.constructor": "method",
        "src.mod.a": "function",
        "src.mod.b": "function",
        "src.mod.c": "variable",
        "src.mod.d": "variable",
        "src.mod.gen": "function",
        "src.mod.K": "class",
    }


def test_inherits_and_calls_are_same_file_edges_and_the_rest_are_references() -> None:
    item = parsed(
        "class B {}\ninterface I {}\nclass C extends B implements I, Ext {\n"
        "  m() { this.n(); helper(); other.go(); new B() }\n  n() {}\n}\n"
        "function helper() {}\n"
    )
    by_ref = {x.ref: x.qualified_name for x in item.symbols}
    edges = {(by_ref[e.source_ref], by_ref[e.target_ref], e.kind) for e in item.relations}
    assert edges == {
        ("src.mod.C", "src.mod.B", "inherits"),
        ("src.mod.C", "src.mod.I", "inherits"),
        ("src.mod.C.m", "src.mod.C.n", "calls"),
        ("src.mod.C.m", "src.mod.helper", "calls"),
        ("src.mod.C.m", "src.mod.B", "calls"),
    }
    assert refs(item, "inherit") == [("3", "Ext", None, 0)]
    assert [x[1] for x in refs(item, "call")] == ["other.go"]
    assert {x.confidence for x in item.references if x.kind == "call"} == {"heuristic"}
    assert {x.confidence for x in item.references if x.kind == "inherit"} == {"syntactic"}


def test_commonjs_require_is_a_heuristic_call_not_an_import() -> None:
    item = parsed("const fs = require('fs');\nmodule.exports = { a };\n", "src/x.cjs")
    assert refs(item, "import") == []
    assert [(x.target_name, x.confidence) for x in item.references] == [("require", "heuristic")]


# --- import forms ---------------------------------------------------------------------------

IMPORT_FORMS = [
    ("import a from './x';", [("a_", None, 0)]),
    ("import def, {n} from 'pkg';", [("pkg", None, 0), ("n", "pkg", 0)]),
    ("import {n as m} from '../up/mod';", [("n", "up.mod", 2)]),
    ("import * as ns from '../../lib';", [("lib", None, 3)]),
    ("import 'side/effect';", [("effect", "side", 0)]),
    ("import type {T} from './types';", [("T", "types", 1)]),
    ("import {type T, U} from '@scope/pkg';", [("T", "scope.pkg", 0), ("U", "scope.pkg", 0)]),
    ("export {a, b as c} from './x';", [("a", "x", 1), ("b", "x", 1)]),
    ("export * from './x';", [("x", None, 1)]),
    ("export * as ns from './x';", [("x", None, 1)]),
    ("import x = require('./y');", [("y", None, 1)]),
    ("import {a} from 'lodash-es';", []),
    ("import fs from 'node:fs';", []),
    ("import a from 'a/b/c';", []),
    ("import a from `tpl`;", []),
    ("export {a};", []),
]


@pytest.mark.parametrize(("code", "expected"), IMPORT_FORMS)
def test_import_forms(code: str, expected: list[tuple[str, str | None, int]]) -> None:
    item = parsed(code + "\n")
    got = [(x[1], x[2], x[3]) for x in refs(item, "import")]
    expected = [
        (("a" if name == "a_" else name), qualifier, level) for name, qualifier, level in expected
    ]
    if code.startswith("import a from './x'"):
        expected = [("x", None, 1)]
    assert sorted(got, key=str) == sorted(expected, key=str)


def test_unrepresentable_specifiers_are_counted_not_silently_lost() -> None:
    item = parsed("import {a} from 'lodash-es';\nimport b from 'node:fs';\n")
    assert codes(item) == {"references_capped": 2}


# --- fingerprints ---------------------------------------------------------------------------

PLAIN = """
/** Doc. */
export function f(a: number, b = 2): number {
  // note
  return a + b; // tail
}
"""


def test_reformatting_comments_and_jsdoc_keep_both_digests() -> None:
    reformatted = (
        "export function f(a: number,\n   b = 2): number\n{\n\treturn a+b\n}\n/* trailing */"
    )
    assert digests(PLAIN, "src.mod.f") == digests(reformatted, "src.mod.f")
    assert digests(PLAIN, "src.mod.f") == digests(PLAIN.replace("\n", "\r\n"), "src.mod.f")
    assert digests(PLAIN, "src.mod.f") == digests(PLAIN.replace("(a:", "(a :"), "src.mod.f")


def test_changed_body_changes_the_fingerprint_not_the_signature() -> None:
    sig, sem = digests(PLAIN, "src.mod.f")
    sig2, sem2 = digests(PLAIN.replace("a + b", "a - b"), "src.mod.f")
    assert sig2 == sig
    assert sem2 != sem


def test_changed_signature_or_decorator_changes_the_revision() -> None:
    sig = digests(PLAIN, "src.mod.f")[0]
    assert digests(PLAIN.replace("b = 2", "b = 3"), "src.mod.f")[0] != sig
    assert digests(PLAIN.replace("): number", "): string"), "src.mod.f")[0] != sig
    cls = "@dec(1)\nclass K { @m() run() {} }\n"
    base = digests(cls, "src.mod.K")
    assert digests(cls.replace("dec(1)", "dec(2)"), "src.mod.K") != base
    assert (
        digests(cls.replace("@m()", "@n()"), "src.mod.K.run")[1] != digests(cls, "src.mod.K.run")[1]
    )


def test_string_literals_are_not_normalized_but_crlf_inside_them_is() -> None:
    lf = "const s = `a\nb`;\n"
    assert digests(lf, "src.mod.s") == digests(lf.replace("\n", "\r\n"), "src.mod.s")
    assert digests(lf, "src.mod.s") != digests("const s = `a b`;\n", "src.mod.s")


def test_nested_definitions_are_part_of_the_enclosing_fingerprint() -> None:
    src = "class K { m() { return 1 } n() {} }\n"
    _, cls = digests(src, "src.mod.K")
    _, cls2 = digests(src.replace("return 1", "return 2"), "src.mod.K")
    assert cls != cls2


def test_digests_are_deterministic_across_runs_and_hash_seeds() -> None:
    req = fixture_request()
    assert normalize_module(in_process(req)) == normalize_module(in_process(req))
    program = (
        "import json,sys;from tests.unit.indexing.tree_sitter.test_typescript import *;"
        "print(json.dumps(normalize_module(in_process(fixture_request())),sort_keys=True))"
    )
    outputs = set()
    for seed in ("0", "1", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        result = subprocess.run(
            [sys.executable, "-c", program],
            capture_output=True,
            check=True,
            env=env,
            cwd=Path(__file__).parents[4],
        )
        outputs.add(result.stdout)
    assert len(outputs) == 1
    assert json.loads(outputs.pop()) == json.loads((FIXTURES / "expected.json").read_text())


# --- robustness -----------------------------------------------------------------------------


def test_broken_symbols_are_skipped_but_valid_siblings_kept() -> None:
    item = parsed("function ok() {}\nfunction bad() { let = ; }\nclass Fine { m() {} }\n")
    assert "src.mod.ok" in names(item)
    assert "src.mod.bad" not in names(item)
    assert codes(item)["syntax_recovered"] >= 1
    assert codes(item)["symbols_dropped"] >= 1


def test_children_of_a_skipped_symbol_are_skipped_too() -> None:
    item = parsed("class K {\n  m() { let = ; }\n  n() {}\n}\n")
    assert "src.mod.K.m" not in names(item)
    assert "src.mod.K.n" in names(item)
    item = parsed("function bad() { function inner() {} let = ; }\n")
    assert not [n for n in names(item) if "bad" in n or "inner" in n]


def test_garbage_never_crashes() -> None:
    for blob in (b"\x00\xff\xfe" * 5000, b"}{)(" * 5000, b"<<<<<<< HEAD\n" * 2000, b"`${" * 4000):
        for path in ("g.ts", "g.tsx", "g.js"):
            item = parsed(blob, path)
            assert item.path == path


def test_invalid_utf8_and_names_glued_to_non_ascii_bytes_are_skipped() -> None:
    item = parsed(b"function ok() {}\nfunction b\xff() {}\nfunction c\xc3() {}\n")
    assert "src.mod.ok" in names(item)
    assert not [n for n in names(item) if n.startswith(("src.mod.b", "src.mod.c"))]
    assert names(parsed("function caf\u00e9() {}\n"))["src.mod.caf\u00e9"] == "function"


def test_crlf_and_bom_offsets_refer_to_the_raw_bytes() -> None:
    raw = b"\xef\xbb\xbffunction f() {\r\n  return 1;\r\n}\r\n"
    item = parsed(raw)
    func = next(x for x in item.symbols if x.qualified_name == "src.mod.f")
    assert raw[func.start_byte : func.end_byte].startswith(b"function f")
    assert raw[func.start_byte : func.end_byte].endswith(b"}")


def test_empty_and_whitespace_only_files() -> None:
    assert parsed("").symbols == ()
    assert names(parsed("  \n\n")) == {"src.mod": "module"}


def test_deeply_nested_input_is_handled_without_recursion() -> None:
    depth = 20_000
    for code in (
        "[" * depth + "]" * depth,
        "(" * depth + ")" * depth,
        "{a:" * depth + "1" + "}" * depth,
    ):
        item = parsed(f"function f() {{ return {code} }}\nfunction g() {{}}\n")
        assert item.path == "src/mod.ts"
    nested = "".join(f"function f{i}() {{" for i in range(300)) + "}" * 300
    assert len(parsed(nested).symbols) > 1


def test_huge_and_control_character_names_are_skipped() -> None:
    long = "a" * 600
    item = parsed(f"function {long}() {{}}\nfunction ok() {{}}\nclass {long} {{}}\n")
    assert "src.mod.ok" in names(item)
    assert not [n for n in names(item) if long in n]
    item = parsed("function f() {}\x01\nfunction g() {}\n")
    assert all("\x01" not in x.signature for x in item.symbols)


def test_duplicate_definitions_do_not_trip_the_validator() -> None:
    item = parsed(
        "function f() {}\nfunction f() {}\nvar a = 1, a = 2;\nclass K { m() {} m() {} }\n"
    )
    keys = [x.qualified_name for x in item.symbols]
    assert keys.count("src.mod.f") == 2
    assert len(item.symbols) == len(keys)


def test_more_than_eight_same_scope_candidates_yield_no_edge() -> None:
    many = "".join("function f() {}\n" for _ in range(9))
    item = parsed(many + "function g() { f() }\n")
    assert item.relations == ()
    assert refs(item, "call") == []
    assert codes(item)["references_capped"] == 1  # the ambiguity is counted, never silent
    few = "".join("function f() {}\n" for _ in range(8))
    assert len(parsed(few + "function g() { f() }\n").relations) == 8


def test_a_call_name_too_long_for_the_contract_is_counted() -> None:
    item = parsed("function g() { " + "a" * 600 + "() }\n")
    assert refs(item, "call") == []
    assert codes(item)["references_capped"] == 1


def test_a_file_over_the_work_budget_degrades_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common.Budget, "MAX_NODES_PER_FILE", 200)
    big = "const x = [" + ",".join("1" for _ in range(500)) + "];\n"
    module = in_process(
        request(
            source("src/a.ts", "function a() {}\n"),
            source("src/big.ts", big + "function b() {}\n"),
            source("src/c.js", "function c() {}\n"),
        )
    )
    assert {x.path: len(x.symbols) for x in module.files} == {
        "src/a.ts": 2,
        "src/big.ts": 0,
        "src/c.js": 2,
    }
    degraded = module.files[1]
    assert codes(degraded) == {"file_degraded": 1, "work_budget_exceeded": 1}
    assert degraded.relations == () and degraded.references == ()


def test_the_cpu_backstop_degrades_with_a_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common.Budget, "CPU_SOFT_LIMIT", -1.0)
    item = in_process(request(source("src/a.ts", "function a() {}\n"))).files[0]
    assert item.symbols == ()
    assert codes(item) == {"file_degraded": 1, "work_budget_exceeded": 1}


def test_bugs_in_the_adapter_are_not_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_args: object) -> ParsedFile:
        raise KeyError("bug")

    monkeypatch.setattr(typescript, "_parse", broken)
    with pytest.raises(KeyError):
        parse_typescript(request(source("src/a.ts", "let x;\n")))


def test_dropped_symbols_release_budget() -> None:
    bad = "".join(f"function b{i}() {{ let = ; }}\n" for i in range(50))
    item = parsed(bad + "function ok() {}\n")
    assert "src.mod.ok" in names(item)


# --- adversarial inputs through the real sandbox (Landlock + rlimits) ------------------------


def timed(req: ParseRequest) -> tuple[ParsedModule, float, float]:
    started, cpu = time.monotonic(), children_cpu()
    # A generous wall clock: the CPU rlimit still applies, but machine load must not time out.
    module = typescript_adapter(limits=Limits(wall_seconds=180.0)).parse(req)
    elapsed, used = time.monotonic() - started, children_cpu() - cpu
    print(f"wall={elapsed:.2f}s child_cpu={used:.2f}s")
    return module, elapsed, used


def test_a_one_megabyte_chained_call_and_member_expression_is_linear() -> None:
    call = "a" + ".f()" * (MAX_SOURCE_BYTES // 4 - 8) + ";\n"
    member = "a" + ".b" * (MAX_SOURCE_BYTES // 2 - 8) + ";\n"
    for name, text in (("call", call), ("member", member)):
        module, _elapsed, _used = timed(
            request(source("src/ok.ts", "function a() {}\n"), source(f"src/{name}.ts", text))
        )
        assert len(module.files[0].symbols) == 2
        big = module.files[1]  # parsed, or degraded with its reason: never a dead child
        assert big.symbols or codes(big).get("work_budget_exceeded") == 1


TS_MODULE = """
import { Base, type Shape } from './base';
import * as util from '../util/helpers';
import def, { a as b, c } from '@scope/pkg';
export * from './more';

export interface Repo{n}<T> extends Shape {{
  find(id: string): Promise<T | undefined>;
}}

export enum Kind{n} {{ A, B = 'b' }}

export abstract class Service{n} extends Base implements Shape {{
  private cache = new Map<string, number>();
  static count = {n};
  constructor(public readonly name: string, protected opts: Record<string, unknown> = {{}}) {{
    super();
  }}
  get size(): number {{ return this.cache.size; }}
  set size(value: number) {{ this.cache.clear(); }}
  async load(id: string): Promise<number> {{
    const hit = this.cache.get(id);
    if (hit !== undefined) {{ return hit; }}
    const value = await util.fetchValue(id, this.opts);
    this.cache.set(id, value);
    return value;
  }}
  abstract render(): string;
  handler = (event: Event): void => {{ this.load(String(event.type)); }};
}}

export function helper{n}<T extends object>(items: T[], pick: (t: T) => string): string[] {{
  return items.map((item) => pick(item)).filter(Boolean).sort();
}}

export const fmt{n} = async (text: string) => `${{text.trim()}}-{n}`;
namespace Inner{n}.Deep {{ export const x = {n}; }}
"""

JSX_MODULE = """
import React, {{ useState }} from 'react';
import {{ Button }} from './button';

export function Panel{n}({{ title, items }}) {{
  const [open, setOpen] = useState(false);
  return (
    <section className="panel" data-n={{{n}}}>
      <h2>{{title}}</h2>
      {{items.map((item) => <Item key={{item.id}} {{...item}} />)}}
      <Button onClick={{() => setOpen(!open)}}>{{open ? 'close' : 'open'}}</Button>
    </section>
  );
}}

class Legacy{n} extends React.Component {{
  state = {{ n: {n} }};
  render() {{ return <div>{{this.state.n}}</div>; }}
}}
module.exports = {{ Panel{n}, Legacy{n} }};
const dep = require('./dep{n}');
"""


def render(template: str, n: int) -> str:
    return template.replace("{n}", str(n)).replace("{{", "{").replace("}}", "}")


def realistic_corpus() -> list[SourceFile]:
    """64 files of about 32 KB each (2 MB): modules, classes, JSX and imports."""
    kinds = (("src/svc/mod{i}.ts", TS_MODULE), ("src/ui/panel{i}.tsx", JSX_MODULE))
    files = []
    for i in range(64):
        path, template = kinds[i % 2] if i % 3 else ("src/lib/legacy{i}.js", JSX_MODULE)
        text = "".join(render(template, i * 100 + k) for k in range(40))
        files.append(source(path.format(i=i), text))
    return files


def test_a_realistic_two_megabyte_corpus_degrades_nothing() -> None:
    """The CPU backstop must never touch legitimate code, also on a loaded machine.

    The walk costs about 0.27 MB per CPU-second, so a request of ~0.75 MB (24 files) needs
    ~3 s of the 5 s backstop: the caller chunks a big corpus into requests of that size
    (PLATFORM-037), and one request of the whole 2 MB would not fit (measured ~7 s).
    """
    files = realistic_corpus()
    assert sum(len(f.content()) for f in files) > 1_900_000
    for start in range(0, len(files), 24):
        req = request(*files[start : start + 24])
        module, _elapsed, _used = timed(req)
        assert [x.path for x in module.files] == [f.path for f in req.files]
        for item in module.files:
            assert item.symbols, item.path
            # No degradation, no syntax trouble, no dropped symbol.
            assert set(codes(item)) <= {"references_capped"}, item.path


def hostile_batch() -> ParseRequest:
    hostile = (b"function (\n" * 200_000)[:1_000_000]
    others = (b"class {{ }}<<< =>\n" * 100_000)[:1_000_000]
    files = [source(f"src/h{i}.ts", hostile if i % 2 else others) for i in range(6)]
    return request(*files, source("src/ok.ts", "function a() {}\n"))


def jsx_batch() -> ParseRequest:
    """The reviewer's shape: 6 x 1 MB of unterminated JSX openers (slow error recovery)."""
    blob = (b"<div <a <b {x " * 80_000)[:1_000_000]
    files = [source(f"src/j{i}.tsx", blob) for i in range(6)]
    return request(*files, source("src/ok.tsx", "export function a() {}\n"))


def single_jsx() -> ParseRequest:
    blob = (b"<a <b <c {" * 120_000)[:1_000_000]
    return request(source("src/one.tsx", blob), source("src/ok.tsx", "export function a() {}\n"))


@pytest.mark.parametrize("make", [hostile_batch, jsx_batch, single_jsx])
def test_hostile_batches_degrade_files_and_never_kill_the_child(make: object) -> None:
    """Asserts the outcome (no StructuralError, every file answered, the tail parsed or degraded
    with a reason), not a CPU figure: the CPU time is printed for the record only."""
    req = make()  # type: ignore[operator]
    module, _elapsed, _used = timed(req)
    assert [x.path for x in module.files] == [f.path for f in req.files]
    for item in module.files:
        if not item.symbols:
            assert codes(item).get("work_budget_exceeded") == 1, item.path
            assert codes(item).get("file_degraded") == 1
        assert "nonzero_exit" not in codes(item)
    tail = module.files[-1]
    assert len(tail.symbols) == 2 or codes(tail).get("work_budget_exceeded") == 1


# FU-40: the parent (PLATFORM-037) bisects a batch on ``nonzero_exit`` down to single files. The
# adapter does not police memory itself: a peak-RSS check never goes down, so after one heavy
# file every later file would trip it. PLATFORM-037 can reuse this input.
MEMORY_BLOWUP_INPUT = ("a<b<c<d>>>(" * 50_000)[:500_000].encode()


def child_processes() -> list[int]:
    found = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            except OSError:
                continue
            if int(fields[1]) == os.getpid():
                found.append(int(entry.name))
    return found


def test_memory_blowup_input_alone_is_a_contained_nonzero_exit() -> None:
    before = child_processes()
    started = time.monotonic()
    with pytest.raises(StructuralError) as caught:
        typescript_adapter(limits=Limits(wall_seconds=60.0)).parse(
            request(source("src/blow.ts", MEMORY_BLOWUP_INPUT))
        )
    assert caught.value.code is StructuralErrorCode.NONZERO_EXIT
    assert time.monotonic() - started < 60  # inside the wall timeout: no hang
    assert child_processes() == before  # no leaked process


def heritage(item: ParsedFile) -> set[tuple[str, str, str]]:
    by_ref = {x.ref: x.qualified_name for x in item.symbols}
    edges = {(by_ref[e.source_ref], by_ref[e.target_ref], e.kind) for e in item.relations}
    unresolved = {(by_ref.get(x.source or "", ""), x.target_name, x.kind) for x in item.references}
    return edges | {x for x in unresolved if x[2] == "inherit"}


BASES = "class Base {}\ninterface Face {}\n"


@pytest.mark.parametrize(
    "code",
    [
        "const D = class extends Base {};",
        "let D = class extends Base {};",
        "var D = class extends Base {};",
        "const D = class Named extends Base {};",
        "export const D = class extends Base {};",
        "abstract class D extends Base {}",
        "export abstract class D extends Base {}",
        "export default class D extends Base {}",
        "declare class D extends Base {}",
        "class D<T> extends Base<T> {}",
    ],
)
def test_every_class_form_that_can_extend_yields_the_inherits_edge(code: str) -> None:
    item = parsed(BASES + code + "\n")
    assert ("src.mod.D", "src.mod.Base", "inherits") in heritage(item), code


@pytest.mark.parametrize(
    "code",
    [
        "const D = class extends Other {};",
        "export default class D extends Other {}",
        "abstract class D extends Other {}",
    ],
)
def test_unresolved_bases_become_inherit_references(code: str) -> None:
    assert ("src.mod.D", "Other", "inherit") in heritage(parsed(code + "\n"))


def test_interfaces_with_several_extends_and_class_implements() -> None:
    item = parsed(
        BASES + "interface Two extends Face, Ext, Base {}\nclass K implements Face, Ext2 {}\n"
    )
    got = heritage(item)
    assert {
        ("src.mod.Two", "src.mod.Face", "inherits"),
        ("src.mod.Two", "src.mod.Base", "inherits"),
        ("src.mod.Two", "Ext", "inherit"),
        ("src.mod.K", "src.mod.Face", "inherits"),
        ("src.mod.K", "Ext2", "inherit"),
    } <= got


def test_anonymous_classes_emit_no_symbol_and_therefore_no_relation() -> None:
    for code in (
        "export default class extends Base {}",
        "X.y = class extends Base {};",
        "module.exports = class extends Base {};",
        "exports.K = class extends Base {};",
    ):
        item = parsed("class Base {}\n" + code + "\n", "src/mod.js")
        assert set(names(item)) == {"src.mod", "src.mod.Base"}, code
        assert not [e for e in item.relations if e.kind == "inherits"], code
        assert not [x for x in item.references if x.kind == "inherit"], code


def test_ten_thousand_same_named_definitions_and_thirty_thousand_calls() -> None:
    text = "".join("function dup() { dup(); dup(); dup() }\n" for _ in range(10_000))
    module, _elapsed, _used = timed(request(source("src/dup.ts", text)))
    item = module.files[0]
    assert item.symbols or codes(item).get("work_budget_exceeded") == 1
    assert len({x.ref for x in item.symbols}) == len(item.symbols)


def test_dense_relations_are_capped() -> None:
    body = "".join(f"function t{i}() {{}}\n" for i in range(300))
    calls = "function caller() { " + " ".join(f"t{i}();" for i in range(300)) + " }\n"
    item = in_process(request(source("src/d.ts", body + calls))).files[0]
    assert len(item.relations) == 64


def test_a_huge_single_expression_degrades_or_survives_but_the_batch_returns() -> None:
    expr = "const x = " + "1 + " * 250_000 + "1;\n"
    module, _elapsed, _used = timed(
        request(source("src/ok.ts", "let a;\n"), source("src/e.ts", expr))
    )
    assert len(module.files[0].symbols) == 2
    assert module.files[1].symbols or codes(module.files[1]).get("work_budget_exceeded") == 1


def test_invalid_utf8_crlf_and_bom_through_the_sandbox() -> None:
    module = typescript_adapter().parse(
        request(
            source("src/a.ts", b"\xef\xbb\xbffunction f() {}\r\nfunction g\xff() {}\r\n"),
            source("src/b.ts", b"\xff\xfe\x00"),
        )
    )
    assert "src.a.f" in names(module.files[0])


@pytest.mark.skipif(landlock.abi_version() < 1, reason="kernel lacks Landlock")
def test_the_real_adapter_runs_confined(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    limits = Limits(checkout_roots=(str(checkout),), wall_seconds=15.0)
    module = typescript_adapter(limits=limits).parse(fixture_request())
    assert normalize_module(module) == json.loads((FIXTURES / "expected.json").read_text())
    rules = landlock.read_set([sys.executable, "-m", typescript.__name__])
    assert not any(str(tmp_path).startswith(r.rstrip("/") + "/") for r in rules.read if r != "/")


# --- round 2: per-symbol fingerprints, members, qualified targets -----------------------------

TWO = "const a = () => 1, b = () => 2;\n"


def test_editing_one_declarator_revises_only_that_symbol() -> None:
    before_a, before_b = digests(TWO, "src.mod.a"), digests(TWO, "src.mod.b")
    edited = "const a = () => 1, b = () => 3;\n"
    assert digests(edited, "src.mod.a") == before_a
    after_b = digests(edited, "src.mod.b")
    assert after_b[1] != before_b[1]
    assert before_a != before_b  # never the same identity


def test_editing_the_first_declarator_leaves_the_second_alone() -> None:
    before_b = digests(TWO, "src.mod.b")
    assert digests("const a = () => 9, b = () => 2;\n", "src.mod.b") == before_b


def test_changing_the_parameters_of_one_declarator_changes_only_its_signature() -> None:
    before_a, before_b = digests(TWO, "src.mod.a"), digests(TWO, "src.mod.b")
    edited = "const a = () => 1, b = (x: number) => 2;\n"
    assert digests(edited, "src.mod.a") == before_a
    assert digests(edited, "src.mod.b")[0] != before_b[0]


def test_a_body_edit_keeps_the_signature_of_a_declarator() -> None:
    before = digests("const a = () => 1, b = (x) => 2;\n", "src.mod.b")
    after = digests("const a = () => 1, b = (x) => 3;\n", "src.mod.b")
    assert after[0] == before[0]
    assert after[1] != before[1]


def test_a_class_expression_body_edit_changes_its_fingerprint() -> None:
    before = digests("const C = class { m() { return 1 } };\n", "src.mod.C")
    after = digests("const C = class { m() { return 2 } };\n", "src.mod.C")
    assert after[1] != before[1]
    assert after[0] == before[0]  # the header is unchanged


def test_class_expression_declarators_are_independent() -> None:
    src = "const A = class { m() { return 1 } }, B = class { n() { return 1 } };\n"
    before_a = digests(src, "src.mod.A")
    assert digests(src.replace("n() { return 1", "n() { return 2"), "src.mod.A") == before_a


def test_const_and_let_are_different_revisions() -> None:
    assert digests("const a = () => 1;\n", "src.mod.a") != digests(
        "let a = () => 1;\n", "src.mod.a"
    )


def test_multi_declarator_ranges_are_each_declarators_own() -> None:
    src = "export const a = () => 1, b = () => 2, c = 3;\n"
    item = parsed(src)
    span = {
        x.qualified_name: src.encode()[x.start_byte : x.end_byte].decode() for x in item.symbols
    }
    assert span["src.mod.a"].startswith("export const a")
    assert span["src.mod.b"] == "b = () => 2"
    assert span["src.mod.c"].endswith("c = 3;")


def test_abstract_and_declared_fields_and_class_overloads_are_emitted() -> None:
    item = parsed(
        "abstract class A {\n  abstract value: number;\n  declare d: string;\n"
        "  m(a: string): void;\n  m(a: number): void;\n  m(a: any) {}\n}\n"
    )
    kinds = names(item)
    assert kinds["src.mod.A.value"] == "field"
    assert kinds["src.mod.A.d"] == "field"
    assert kinds["src.mod.A.m"] == "method"
    # index signatures and static blocks have no name to carry: skipped, not an error
    assert names(parsed("class A { [k: string]: any; static { init() } }\n")) == {
        "src.mod": "module",
        "src.mod.A": "class",
    }


def test_a_qualified_base_resolves_through_a_same_file_namespace() -> None:
    item = parsed("namespace N { export class Base {} }\nclass D extends N.Base {}\n")
    assert heritage(item) == {("src.mod.D", "src.mod.N.Base", "inherits")}
    assert refs(item, "inherit") == []


def test_a_qualified_call_resolves_through_a_same_file_namespace() -> None:
    item = parsed(
        "namespace N { export namespace M { export function f() {} } }\nfunction g() { N.M.f() }\n"
    )
    by_ref = {x.ref: x.qualified_name for x in item.symbols}
    assert {(by_ref[e.source_ref], by_ref[e.target_ref], e.kind) for e in item.relations} == {
        ("src.mod.g", "src.mod.N.M.f", "calls")
    }
    assert refs(item, "call") == []


def test_an_unresolvable_qualified_target_stays_a_reference() -> None:
    item = parsed("namespace N { }\nclass D extends N.Base {}\nfunction g() { other.f() }\n")
    assert [x[1] for x in refs(item, "inherit")] == ["N.Base"]
    assert [x[1] for x in refs(item, "call")] == ["other.f"]
