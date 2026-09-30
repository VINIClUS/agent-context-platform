"""FU-40 through the real ``SandboxedAdapter``: a deterministic child crash degrades one file."""

from __future__ import annotations

import dataclasses
import sys
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

import agent_context_platform
from agent_context_platform.indexing import registry
from agent_context_platform.indexing.emitter import (
    StructuralResult,
    parse_structural,
    read_sources,
)
from agent_context_platform.indexing.identity import encode_components, repository_namespace
from agent_context_platform.indexing.tree_sitter import landlock, python
from agent_context_platform.indexing.tree_sitter.runner import Limits, SandboxedAdapter
from agent_context_platform.settings import Settings

from .conftest import RepoBuilder
from .test_emitter import by_type, lineage, scan_of, service
from .tree_sitter.test_typescript import MEMORY_BLOWUP_INPUT

pytestmark = pytest.mark.unit

# The real Python adapter, except that a marker in a file misbehaves the child deterministically.
_CRASHING_ADAPTER = """
import base64, os, signal, sys, time
from agent_context_platform.indexing.tree_sitter import _common, python, runner
from agent_context_platform.indexing.tree_sitter.base import ParsedDiagnostic, ParsedModule, ParseRequest

def parse(request):
    files, cut = [], False
    for item in request.files:
        text = base64.b64decode(item.content_b64)
        if b"CRASH_ME" in text:
            os.kill(os.getpid(), signal.SIGSEGV)
        if b"HANG_ME" in text:
            time.sleep(600)
        if b"GARBLE_ME" in text:
            os.write(1, b"{not json")
            os._exit(0)
        if b"BUDGET_ME" in text:
            # The CPU backstop is per request: once spent, this and later files come back cut.
            _common.Budget.CPU_SOFT_LIMIT = -1.0
            cut = True
        parsed = python.parse_python(ParseRequest(files=(item,))).files[0]
        if cut:  # what adapters report once PLATFORM-033b lands: a reasoned degradation
            parsed = parsed.model_copy(update={"diagnostics": (
                ParsedDiagnostic(code="file_degraded", count=1),
                ParsedDiagnostic(code="work_budget_exceeded", count=1))})
        files.append(parsed)
    return ParsedModule(files=tuple(files))

sys.exit(runner.serve(parse))
"""
_PACKAGE = Path(agent_context_platform.__file__).resolve().parent
GOOD = {
    "pkg/a.py": b"def a():\n    return 1\n",
    "pkg/c.py": b"class C:\n    pass\n",
    "pkg/d.py": b"def d():\n    return 4\n",
}


def _settings(tmp_path: Path) -> Settings:
    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    return Settings(
        indexer_checkout_roots=(str(checkout),),
        indexer_allow_unconfined_adapters=landlock.abi_version() < 1,
    )


def _adapter(tmp_path: Path, wall_seconds: float = 20.0) -> SandboxedAdapter:
    limits = dataclasses.replace(
        Limits.from_settings(_settings(tmp_path)),
        wall_seconds=wall_seconds,
        extra_read_paths=(str(_PACKAGE), str(_PACKAGE.parent)),
    )
    return SandboxedAdapter(
        [sys.executable, "-c", _CRASHING_ADAPTER],
        language=python.LANGUAGE,
        parser_name=python.ADAPTER_NAME,
        parser_version=python.ADAPTER_VERSION,
        parser_config=python.GRAMMAR_VERSIONS,
        limits=limits,
    )


def _degraded(result: StructuralResult) -> list[str]:
    return [
        item.path
        for item in result.files
        if any(d.code == "file_degraded" for d in item.diagnostics)
    ]


def _check_only(result: StructuralResult, hostile: str, sources: dict[str, bytes]) -> None:
    by_path = {item.path: item for item in result.files}
    assert sorted(by_path) == sorted(sources)
    assert _degraded(result) == [hostile]
    assert by_path[hostile].symbols == ()
    assert by_path[hostile].parser_fingerprint == registry.LANGUAGES["python"].parser_fingerprint
    for path in sources:
        if path != hostile:
            assert by_path[path].symbols
            assert not by_path[path].diagnostics


def test_a_crashing_file_is_degraded_alone_through_the_real_sandbox(tmp_path: Path) -> None:
    sources = {**GOOD, "pkg/b.py": b"# CRASH_ME\ndef b():\n    return 2\n"}

    result = parse_structural({"python": _adapter(tmp_path)}, sources)

    _check_only(result, "pkg/b.py", sources)
    assert result.diagnostics["file_degraded"] == 1
    assert result.diagnostics["adapter_nonzero_exit"] >= 1


def test_a_real_child_timeout_is_bisected(tmp_path: Path) -> None:
    sources = {**GOOD, "pkg/b.py": b"# HANG_ME\n"}

    result = parse_structural({"python": _adapter(tmp_path, wall_seconds=3.0)}, sources)

    _check_only(result, "pkg/b.py", sources)
    assert result.diagnostics["adapter_timeout"] >= 1


def test_a_real_child_with_malformed_output_is_bisected(tmp_path: Path) -> None:
    sources = {**GOOD, "pkg/b.py": b"# GARBLE_ME\n"}

    result = parse_structural({"python": _adapter(tmp_path)}, sources)

    _check_only(result, "pkg/b.py", sources)
    assert result.diagnostics["adapter_schema_violation"] >= 1


def test_files_cut_by_the_request_budget_are_retried_alone_in_a_real_child(
    tmp_path: Path,
) -> None:
    # The hostile file sorts first, so it spends the request's CPU budget before the good ones.
    sources = {"pkg/0.py": b"# BUDGET_ME\ndef z():\n    return 0\n", **GOOD}

    result = parse_structural({"python": _adapter(tmp_path)}, sources)

    _check_only(result, "pkg/0.py", sources)
    assert result.diagnostics["solo_retry"] == len(sources)
    assert result.diagnostics["file_degraded"] == 1


def test_adapters_built_from_settings_parse_for_real(tmp_path: Path) -> None:
    adapters = registry.build_adapters(_settings(tmp_path))

    result = parse_structural(adapters, GOOD)

    assert [item.path for item in result.files] == sorted(GOOD)
    assert all(item.symbols for item in result.files)
    assert not result.diagnostics


def test_a_real_python_index_resolves_imports_calls_and_external_dependencies(
    tmp_path: Path, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    repo.write("pkg/__init__.py", "")
    repo.write("pkg/util.py", "def helper():\n    return 1\n")
    repo.write(
        "pkg/main.py",
        "import requests\nfrom pkg.util import helper\n\n\ndef run():\n    return helper()\n",
    )
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    adapters = registry.build_adapters(_settings(tmp_path))

    structural = parse_structural(adapters, read_sources(scan))
    drafts = service().index(scan, None, list(structural.files), file_logical_ids=ids)

    assert not structural.diagnostics  # the real adapter, no injected references
    symbols = {
        d.payload["qualified_name"]: d.payload["symbol_id"]
        for d in by_type(drafts, "code.symbol.indexed")
    }
    relations = [dict(d.payload) for d in by_type(drafts, "code.relation.asserted")]
    imports = [r for r in relations if r["predicate"] == "IMPORTS"]
    calls = [r for r in relations if r["predicate"] == "CALLS"]
    assert any(r["object_id"] == symbols["pkg.util.helper"] for r in imports)  # cross-file
    resolved = [r for r in calls if r["object_id"] == symbols["pkg.util.helper"]]
    assert resolved and all(r["confidence"] < 1 for r in resolved)  # heuristic
    dependencies = by_type(drafts, "code.dependency.asserted")
    assert len(dependencies) == 1  # ``requests`` only: pkg.util resolved inside the repository
    assert dependencies[0].payload["dependency_kind"] == "observed"


def test_a_real_python_alias_and_dotted_import_bind_through_the_sandbox(
    tmp_path: Path, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    repo.write("pkg/__init__.py", "")
    repo.write("pkg/util.py", "def helper():\n    return 1\n")
    repo.write("pkg/aliased.py", "import pkg.util as u\n\n\ndef run():\n    return u.helper()\n")
    repo.write("pkg/dotted.py", "import pkg.util\n\n\ndef run():\n    return pkg.util.helper()\n")
    repo.write("pkg/wrong.py", "import pkg.util\n\n\ndef run():\n    return util.helper()\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    structural = parse_structural(registry.build_adapters(_settings(tmp_path)), read_sources(scan))

    drafts = service().index(scan, None, list(structural.files), file_logical_ids=ids)

    by_name = {
        d.payload["qualified_name"]: d.payload["symbol_id"]
        for d in by_type(drafts, "code.symbol.indexed")
    }
    callers = {
        str(r.payload["subject_id"])
        for r in by_type(drafts, "code.relation.asserted")
        if r.payload["predicate"] == "CALLS"
        and r.payload["object_id"] == by_name["pkg.util.helper"]
    }
    assert callers == {by_name["pkg.aliased.run"], by_name["pkg.dotted.run"]}  # not pkg.wrong.run


def test_a_ts_and_a_js_file_index_through_the_real_typescript_adapter(
    tmp_path: Path, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    repo.write(
        "src/util.ts",
        "export function helper(): number {\n  return 1;\n}\n\n"
        "export class Box {\n  #hidden = 1;\n  static make(): Box {\n    return new Box();\n  }\n"
        "  #open(): number {\n    return this.#hidden;\n  }\n}\n"
        "export class Other {\n  static make(): Other {\n    return new Other();\n  }\n}\n",
    )
    repo.write(
        "src/main.js",
        'import { helper, Box } from "./util";\nimport lodash from "lodash";\n\n'
        "export function run() {\n  Box.make();\n  return helper();\n}\n",
    )
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    structural = parse_structural(registry.build_adapters(_settings(tmp_path)), read_sources(scan))

    drafts = service().index(scan, None, list(structural.files), file_logical_ids=ids)

    labels = {
        d.payload["path"]: d.payload["language"] for d in by_type(drafts, "code.file.indexed")
    }
    assert labels == {"src/util.ts": "typescript", "src/main.js": "javascript"}
    by_name = {
        d.payload["qualified_name"]: d.payload["symbol_id"]
        for d in by_type(drafts, "code.symbol.indexed")
    }
    calls = {
        (str(r.payload["subject_id"]), str(r.payload["object_id"]))
        for r in by_type(drafts, "code.relation.asserted")
        if r.payload["predicate"] == "CALLS"
    }
    assert by_name["src.util.Box#hidden"] and by_name["src.util.Box#open"]  # `#` members exist
    assert (by_name["src.main.run"], by_name["src.util.helper"]) in calls
    # The owner chain picks Box.make, not Other.make, with `#` members in the same class.
    assert (by_name["src.main.run"], by_name["src.util.Box.make"]) in calls
    assert (by_name["src.main.run"], by_name["src.util.Other.make"]) not in calls
    assert len(by_type(drafts, "code.dependency.asserted")) == 1  # lodash only


def test_a_ts_import_binds_the_local_name_not_the_exported_one(
    tmp_path: Path, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    repo.write(
        "x.ts",
        "export function foo() {}\nexport function helper() {}\nexport function x() {}\n",
    )
    repo.write(
        "a.js",
        'import helper from "./x";\nimport * as ns from "./x";\n'
        'import { foo as bar } from "./x";\n\n'
        "export function viaDefault() {\n  helper();\n}\n"
        "export function viaNamespace() {\n  ns.foo();\n}\n"
        "export function viaAs() {\n  bar();\n}\n"
        "export function viaOriginal() {\n  foo();\n}\n"
        "export function viaFile() {\n  x();\n}\n"
        "export function viaFileNs() {\n  x.foo();\n}\n",
    )
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    structural = parse_structural(registry.build_adapters(_settings(tmp_path)), read_sources(scan))

    drafts = service().index(scan, None, list(structural.files), file_logical_ids=ids)

    names = {
        str(d.payload["symbol_id"]): str(d.payload["qualified_name"])
        for d in by_type(drafts, "code.symbol.indexed")
    }
    calls = {
        (names[str(r.payload["subject_id"])], names[str(r.payload["object_id"])])
        for r in by_type(drafts, "code.relation.asserted")
        if r.payload["predicate"] == "CALLS"
    }
    assert calls == {
        ("a.viaDefault", "x.helper"),
        ("a.viaNamespace", "x.foo"),
        ("a.viaAs", "x.foo"),
    }  # foo() / x() / x.foo() name nothing this file imported


def test_a_scoped_package_import_keeps_its_scope_in_the_dependency(
    tmp_path: Path, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    repo.write(
        "a.ts",
        'import whole from "@scope/pkg";\nimport { named } from "@scope/other/deep";\n'
        'import lodash from "lodash";\n\nexport function run() {}\n',
    )
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    structural = parse_structural(registry.build_adapters(_settings(tmp_path)), read_sources(scan))

    drafts = service().index(scan, None, list(structural.files), file_logical_ids=ids)

    namespace = repository_namespace(service().config.repository_id)
    expected = {
        str(uuid.uuid5(namespace, encode_components("external_module", name)))
        for name in ("@scope/pkg", "@scope/other", "lodash")
    }
    actual = {str(d.payload["dependency_id"]) for d in by_type(drafts, "code.dependency.asserted")}
    assert actual == expected


def test_a_memory_blowup_ts_file_is_degraded_alone_through_the_real_typescript_adapter(
    tmp_path: Path,
) -> None:
    sources = {
        "src/a.ts": b"export function a(): number {\n  return 1;\n}\n",
        "src/blow.ts": MEMORY_BLOWUP_INPUT,
        "src/c.ts": b"export function c(): number {\n  return 3;\n}\n",
    }

    result = parse_structural(registry.build_adapters(_settings(tmp_path)), sources)

    by_path = {item.path: item for item in result.files}
    assert _degraded(result) == ["src/blow.ts"]
    assert by_path["src/a.ts"].symbols and by_path["src/c.ts"].symbols
    assert result.diagnostics["file_degraded"] == 1


def test_a_real_go_index_resolves_siblings_imports_aliases_and_external_dependencies(
    tmp_path: Path, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    repo.write("store/store.go", "package store\n\nfunc Open() {\n\thelper()\n}\n")
    repo.write("store/helper.go", "package store\n\nfunc helper() {}\n")
    repo.write(
        "cmd/app/main.go",
        'package main\n\nimport (\n\t"fmt"\n\t"example.com/app/store"\n)\n\n'
        "func plain() {\n\tstore.Open()\n\tfmt.Println()\n}\n",
    )
    repo.write(
        "cmd/app/aliased.go",
        'package main\n\nimport s "example.com/app/store"\n\nfunc viaAlias() {\n\ts.Open()\n}\n',
    )
    repo.write(
        "cmd/app/wrong.go",
        'package main\n\nimport "example.com/app/store"\n\nfunc unbound() {\n\ts.Open()\n}\n',
    )
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    structural = parse_structural(registry.build_adapters(_settings(tmp_path)), read_sources(scan))

    drafts = service().index(scan, None, list(structural.files), file_logical_ids=ids)

    assert not structural.diagnostics
    names = {
        str(d.payload["symbol_id"]): str(d.payload["qualified_name"])
        for d in by_type(drafts, "code.symbol.indexed")
    }
    calls = {
        (names[str(r.payload["subject_id"])], names[str(r.payload["object_id"])])
        for r in by_type(drafts, "code.relation.asserted")
        if r.payload["predicate"] == "CALLS"
    }
    opened = next(n for n in names.values() if n.endswith("store.Open"))
    helper = next(n for n in names.values() if n.endswith("store.helper"))
    assert {caller.rsplit(".", 1)[-1] for caller, callee in calls if callee == opened} == {
        "plain",
        "viaAlias",  # bound by its alias; ``unbound`` calls ``s`` without importing it
    }
    assert (opened, helper) in calls  # a call across sibling files of one package
    dependencies = by_type(drafts, "code.dependency.asserted")
    assert len(dependencies) == 1  # ``fmt`` only: example.com/app/store resolved in the repository
    assert dependencies[0].payload["dependency_kind"] == "observed"
