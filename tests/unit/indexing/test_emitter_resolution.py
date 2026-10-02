"""Cross-file resolution, batching and bisection with stub adapters (no sandbox needed)."""

from __future__ import annotations

import base64
import dataclasses
import itertools
from collections.abc import Callable, Mapping
from dataclasses import replace
from types import MappingProxyType

import pytest

from agent_context_platform.indexing import registry
from agent_context_platform.indexing.emitter import MAX_BISECTION_RUNS, parse_structural
from agent_context_platform.indexing.tree_sitter.base import (
    MAX_INPUT_FILES,
    ParsedDiagnostic,
    ParsedFile,
    ParsedModule,
    ParsedReference,
    ParsedSymbol,
    ParseRequest,
    StructuralError,
    StructuralErrorCode,
)

from .conftest import RepoBuilder
from .test_emitter import by_type, lineage, scan_of, service

pytestmark = pytest.mark.unit

DIGEST = "1" * 64


def _symbol(name: str, size: int, ref: str = "0", kind: str = "module") -> ParsedSymbol:
    return ParsedSymbol(
        ref=ref,
        language="x",
        qualified_name=name,
        kind=kind,
        start_byte=0,
        end_byte=max(size, 1),
        signature="",
        signature_digest=DIGEST,
        semantic_fingerprint=DIGEST,
        evidence_kind="tree_sitter",
    )


def _reference(
    kind: str,
    name: str,
    level: int = 0,
    qualifier: str | None = None,
    alias: str | None = None,
) -> ParsedReference:
    extra: dict[str, object] = (
        {}
        if qualifier is None
        else {"qualifier": qualifier, "qualifier_start_byte": 0, "qualifier_end_byte": 1}
    )
    if alias is not None:  # a field 92ed314 does not have: pass it only when it is used
        extra.update(alias=alias, alias_start_byte=0, alias_end_byte=1)
    return ParsedReference(
        source="0",
        kind=kind,  # type: ignore[arg-type]
        target_name=name,
        relative_level=level,
        start_byte=0,
        end_byte=1,
        evidence_kind="tree_sitter",
        confidence="heuristic" if kind == "call" else "syntactic",
        **extra,  # type: ignore[arg-type]
    )


class TableAdapter:
    """Answers each file with one module symbol and the references of a per-path table."""

    def __init__(
        self,
        language: str,
        table: Mapping[str, tuple[ParsedReference, ...]],
        members: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        self.language = language
        self._table = table
        self._members = members or {}

    def parse(self, request: ParseRequest) -> ParsedModule:
        files = []
        for item in request.files:
            stem = item.path.rsplit("/", 1)[-1].split(".")[0]
            files.append(
                ParsedFile(
                    path=item.path,
                    language=item.language,
                    parser_fingerprint=DIGEST,
                    symbols=(
                        _symbol(stem, len(item.content())),
                        *(
                            _symbol(f"{stem}.{name}", len(item.content()), str(number), "function")
                            for number, name in enumerate(self._members.get(item.path, ()), 1)
                        ),
                    ),
                    references=self._table.get(item.path, ()),
                )
            )
        return ParsedModule(files=tuple(files))


@pytest.fixture
def languages(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register ``typescript`` and ``go`` for the test (what P034 and P035 will do for real)."""
    entries = {
        "typescript": (".ts", ".tsx", ".js"),
        "go": (".go",),
    }
    table = dict(registry.LANGUAGES)
    extensions = dict(registry._EXTENSIONS)
    for language, suffixes in entries.items():
        table[language] = registry.LanguageSupport(
            language,
            suffixes,
            f"{language}-stub",
            "1",
            DIGEST,
            lambda limits: None,  # type: ignore[arg-type,return-value]
            labels={".js": "javascript"} if language == "typescript" else {},
        )
        extensions.update({suffix: language for suffix in suffixes})
    monkeypatch.setattr(registry, "LANGUAGES", MappingProxyType(table))
    monkeypatch.setattr(registry, "_EXTENSIONS", MappingProxyType(extensions))


def _relations(drafts: list, predicate: str) -> list[dict]:
    return [
        dict(d.payload)
        for d in by_type(drafts, "code.relation.asserted")
        if d.payload["predicate"] == predicate
    ]


def test_typescript_and_go_imports_resolve_or_become_dependencies(
    languages: None, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    repo.write("src/app/main.ts", "x\n")
    repo.write("src/app/util.ts", "x\n")
    repo.write("src/app/legacy.js", "x\n")
    repo.write("src/lib/index.ts", "x\n")
    repo.write("go/pkg/sub/sub.go", "package sub\n")
    repo.write("go/main.go", "package main\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    imports = {
        "src/app/main.ts": (
            _reference("import", "util", level=1),
            _reference("import", "lib", level=2),
            _reference("import", "escape", level=4),
            _reference("import", "missing", level=1),
            _reference("import", "fp", qualifier="lodash"),
            _reference("import", "pkg"),
            _reference("call", "util"),
        ),
        "go/main.go": (
            _reference("import", "example.invalid/mod/go/pkg/sub"),
            _reference("import", "fmt"),
        ),
    }
    adapters = {
        "typescript": TableAdapter("typescript", imports),
        "go": TableAdapter("go", imports),
    }
    from agent_context_platform.indexing.emitter import read_sources

    parsed = list(parse_structural(adapters, read_sources(scan)).files)  # type: ignore[arg-type]

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)

    resolved = _relations(drafts, "IMPORTS")
    assert len(resolved) == 3
    assert all(r["confidence"] == 0.9 and r["evidence_kind"] == "tree_sitter" for r in resolved)
    dependencies = by_type(drafts, "code.dependency.asserted")
    assert len(dependencies) == 3  # lodash, pkg, fmt; unresolved relative ones are dropped
    assert not _relations(drafts, "CALLS")
    labels = {
        d.payload["path"]: d.payload["language"] for d in by_type(drafts, "code.file.indexed")
    }
    assert labels["src/app/legacy.js"] == "javascript"  # the extension's label, not the parser's
    assert labels["src/app/main.ts"] == "typescript"


def test_go_binds_names_per_package_and_imports_name_a_package(
    languages: None, make_repo: Callable[[str], RepoBuilder]
) -> None:
    repo = make_repo("repo")
    for path in ("go/pkg/a.go", "go/pkg/b.go", "go/pkg/b_test.go", "go/main.go"):
        repo.write(path, "package x\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    table = {
        # An unqualified call to a sibling file's function resolves; one to a name that only a
        # _test file defines does not (the package never sees its test files).
        "go/pkg/b.go": (_reference("call", "F"), _reference("call", "OnlyInTests")),
        # ``pkg.F()`` resolves through the import, to the PACKAGE (a.go, not the first file
        # only); a receiver call ``t.B()`` has an unbound qualifier and is dropped.
        "go/main.go": (
            _reference("import", "example.invalid/mod/go/pkg"),
            _reference("call", "F", qualifier="pkg"),
            _reference("call", "H", qualifier="pkg"),
            _reference("call", "B", qualifier="t"),
        ),
    }
    members = {
        "go/pkg/a.go": ("F",),
        "go/pkg/b.go": ("H",),
        "go/pkg/b_test.go": ("OnlyInTests",),
    }
    adapters = {"go": TableAdapter("go", table, members)}
    from agent_context_platform.indexing.emitter import read_sources

    parsed = list(parse_structural(adapters, read_sources(scan)).files)  # type: ignore[arg-type]

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)

    by_name = {
        d.payload["qualified_name"]: d.payload["symbol_id"]
        for d in by_type(drafts, "code.symbol.indexed")
    }
    calls = {(r["subject_id"], r["object_id"]) for r in _relations(drafts, "CALLS")}
    module = {
        d.payload["qualified_name"]: d.payload["symbol_id"]
        for d in by_type(drafts, "code.symbol.indexed")
        if d.payload["kind"] == "module"
    }
    assert (module["b"], by_name["a.F"]) in calls
    assert (module["main"], by_name["a.F"]) in calls
    assert (module["main"], by_name["b.H"]) in calls
    assert len(calls) == 3  # OnlyInTests and t.B() are dropped


def test_python_relative_levels(make_repo: Callable[[str], RepoBuilder]) -> None:
    repo = make_repo("repo")
    for path in (
        "a/__init__.py",
        "a/b/__init__.py",
        "a/b/c.py",
        "a/d.py",
        "top.py",
        "src/lib/m.py",
    ):
        repo.write(path, "x\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    table = {
        "a/b/c.py": (
            _reference("import", "d", level=2),
            _reference("import", "x", level=3, qualifier="top"),
            _reference("import", "z", level=9),
            _reference("import", "top"),
            _reference("import", "lib.m"),
        )
    }
    parsed = list(
        parse_structural(
            {"python": TableAdapter("python", table)},
            {  # type: ignore[dict-item]
                f.path: (repo.root / f.path).read_bytes() for f in scan.files
            },
        ).files
    )

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)

    assert len(_relations(drafts, "IMPORTS")) == 3
    assert not by_type(drafts, "code.dependency.asserted")


def _chunk_adapter(calls: list[int], hostile: set[str], code: StructuralErrorCode) -> object:
    class Adapter:
        language = "python"

        def parse(self, request: ParseRequest) -> ParsedModule:
            calls.append(len(request.files))
            if any(item.path in hostile for item in request.files):
                raise StructuralError(code)
            return TableAdapter("python", {}).parse(request)

    return Adapter()


def test_hostile_file_is_bisected_and_degraded_alone() -> None:
    sources = {f"m{index:03d}.py": b"x = 1\n" for index in range(130)}
    calls: list[int] = []
    adapter = _chunk_adapter(calls, {"m070.py"}, StructuralErrorCode.NONZERO_EXIT)

    result = parse_structural({"python": adapter}, sources)  # type: ignore[dict-item]

    assert max(calls) == MAX_INPUT_FILES
    assert calls[:2] == [64, 64]
    assert len(result.files) == 130
    degraded = [f for f in result.files if any(d.code == "file_degraded" for d in f.diagnostics)]
    assert [f.path for f in degraded] == ["m070.py"]
    assert degraded[0].symbols == ()
    assert result.diagnostics["file_degraded"] == 1
    assert result.diagnostics["adapter_nonzero_exit"] >= 1


def test_timeout_bisects_and_environmental_errors_propagate() -> None:
    sources = {"a.py": b"x\n", "b.py": b"y\n"}
    result = parse_structural(
        {"python": _chunk_adapter([], {"b.py"}, StructuralErrorCode.TIMEOUT)},  # type: ignore[dict-item]
        sources,
    )
    assert [f.path for f in result.files] == ["a.py", "b.py"]
    assert result.diagnostics["adapter_timeout"] >= 1

    with pytest.raises(StructuralError) as error:
        parse_structural(
            {"python": _chunk_adapter([], {"a.py"}, StructuralErrorCode.SANDBOX_UNAVAILABLE)},  # type: ignore[dict-item]
            sources,
        )
    assert error.value.code is StructuralErrorCode.SANDBOX_UNAVAILABLE


def test_unsupported_and_oversized_sources_are_counted() -> None:
    big = b"x" * (1_048_576 + 1)
    result = parse_structural(
        {"python": TableAdapter("python", {})},  # type: ignore[dict-item]
        {"a.txt": b"x", "big.py": big, "ok.py": b"x\n", "b64.rs": base64.b64encode(b"x")},
    )
    assert [f.path for f in result.files] == ["ok.py"]
    assert result.diagnostics == {"unsupported_language": 2, "source_too_large": 1}


def test_registry_replace_helper_keeps_dataclass_frozen() -> None:
    entry = registry.LANGUAGES["python"]
    assert replace(entry, language="other").language == "other"


def test_malformed_output_bisects_too() -> None:
    sources = {"a.py": b"x\n", "b.py": b"y\n", "c.py": b"z\n"}
    result = parse_structural(
        {"python": _chunk_adapter([], {"b.py"}, StructuralErrorCode.MALFORMED_OUTPUT)},  # type: ignore[dict-item]
        sources,
    )
    assert result.diagnostics["file_degraded"] == 1
    assert result.diagnostics["adapter_malformed_output"] >= 1


def test_bisection_is_bounded_and_the_rest_of_a_hostile_corpus_is_degraded() -> None:
    sources = {f"m{index:03d}.py": b"x = 1\n" for index in range(64)}
    calls: list[int] = []
    adapter = _chunk_adapter(calls, set(sources), StructuralErrorCode.TIMEOUT)

    result = parse_structural({"python": adapter}, sources)  # type: ignore[dict-item]

    assert len(calls) <= 1 + MAX_BISECTION_RUNS
    assert len(result.files) == 64
    assert result.diagnostics["file_degraded"] == 64
    assert result.diagnostics["bisection_budget_exhausted"] >= 1


def test_a_qualifier_resolves_only_through_a_name_the_file_binds(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    for path in ("util.py", "param.py", "user.py", "own.py"):
        repo.write(path, "x\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    table = {
        # ``def g(util): util.helper()``: ``util`` is a parameter, never the module util.py.
        "param.py": (_reference("call", "helper", qualifier="util"),),
        # ``import util`` binds the head.
        "user.py": (
            _reference("import", "util"),
            _reference("call", "helper", qualifier="util"),
        ),
        # ``Local.run()`` where ``Local`` is a module-level symbol of the same file.
        "own.py": (_reference("call", "run", qualifier="Local"),),
    }
    members = {"util.py": ("helper",), "own.py": ("Local", "Local.run")}
    adapter = TableAdapter("python", table, members)
    sources = {f.path: (repo.root / f.path).read_bytes() for f in scan.files}
    result = parse_structural({"python": adapter}, sources)  # type: ignore[dict-item]

    drafts = service().index(scan, None, list(result.files), file_logical_ids=ids)

    calls = _relations(drafts, "CALLS")
    assert len(calls) == 2  # user.py (imported head) and own.py (module-level head) only
    assert all(c["confidence"] < 1 and c["evidence_kind"] == "tree_sitter" for c in calls)
    symbols = {
        d.payload["qualified_name"]: d.payload["symbol_id"]
        for d in by_type(drafts, "code.symbol.indexed")
    }
    assert {c["object_id"] for c in calls} == {symbols["util.helper"], symbols["own.Local.run"]}
    assert not by_type(drafts, "code.dependency.asserted")


class _SharedBudgetAdapter:
    """Like the real child: a heavy file spends the request's budget, later files come back cut."""

    language = "python"

    def __init__(self, heavy: str) -> None:
        self.heavy = heavy
        self.requests: list[list[str]] = []

    def parse(self, request: ParseRequest) -> ParsedModule:
        self.requests.append([item.path for item in request.files])
        files = []
        spent = False
        for item in request.files:
            spent = spent or item.path == self.heavy
            cut = spent and (item.path == self.heavy or len(request.files) > 1)
            module = TableAdapter("python", {}).parse(ParseRequest(files=(item,))).files[0]
            if cut:
                module = module.model_copy(
                    update={
                        "symbols": (),
                        "diagnostics": (
                            ParsedDiagnostic(code="file_degraded", count=1),
                            ParsedDiagnostic(code="work_budget_exceeded", count=1),
                        ),
                    }
                )
            files.append(module)
        return ParsedModule(files=tuple(files))


def test_files_cut_by_a_shared_request_budget_are_retried_alone_once() -> None:
    sources = {f"m{index}.py": b"x = 1\n" for index in range(6)}
    adapter = _SharedBudgetAdapter("m0.py")

    result = parse_structural({"python": adapter}, sources)  # type: ignore[dict-item]

    degraded = [f.path for f in result.files if f.symbols == ()]
    assert degraded == ["m0.py"]  # still degraded alone; the innocent files are fully indexed
    assert result.diagnostics["file_degraded"] == 1
    assert result.diagnostics["solo_retry"] == 6  # every cut file, once
    assert adapter.requests.count(["m0.py"]) == 1
    assert len(adapter.requests) == 1 + 6  # the batch, then one solo run per file, no more


def test_a_solo_retry_that_fails_degrades_and_an_environmental_one_propagates() -> None:
    class Failing(_SharedBudgetAdapter):
        def __init__(self, code: StructuralErrorCode) -> None:
            super().__init__("a.py")
            self.code = code

        def parse(self, request: ParseRequest) -> ParsedModule:
            if len(request.files) == 1 and request.files[0].path == "b.py":
                raise StructuralError(self.code)
            return super().parse(request)

    sources = {"a.py": b"x\n", "b.py": b"y\n"}
    result = parse_structural({"python": Failing(StructuralErrorCode.TIMEOUT)}, sources)  # type: ignore[dict-item]
    assert all(f.symbols == () for f in result.files)
    assert result.diagnostics["file_degraded"] == 2
    with pytest.raises(StructuralError):
        parse_structural({"python": Failing(StructuralErrorCode.SPAWN_FAILED)}, sources)  # type: ignore[dict-item]


def test_resolution_builds_each_lookup_once_however_many_references(
    monkeypatch: pytest.MonkeyPatch, make_repo: Callable[[str], RepoBuilder]
) -> None:
    """Resolution is O(files + symbols + references): maps are built once, names tokenized once."""
    from agent_context_platform.indexing import emitter

    files, members = 120, 5
    repo = make_repo("repo")
    for index in range(files):
        repo.write(f"pkg/m{index}.py", "x\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    table = {}
    for index in range(files):
        neighbour = f"m{(index + 1) % files}"
        table[f"pkg/m{index}.py"] = (
            _reference("import", neighbour, level=1),
            _reference("import", "missing", level=1),
            *(_reference("call", f"f{n}", qualifier=neighbour) for n in range(members)),
            *(_reference("call", f"f{n}") for n in range(members)),
        )
    names = tuple(f"f{n}" for n in range(members))
    adapter = TableAdapter("python", table, {f"pkg/m{i}.py": names for i in range(files)})
    sources = {f.path: (repo.root / f.path).read_bytes() for f in scan.files}
    parsed = list(parse_structural({"python": adapter}, sources).files)  # type: ignore[dict-item]
    references = sum(len(item.references) for item in parsed)
    symbols = sum(len(item.symbols) for item in parsed)
    assert references > 1000

    calls = {"path_modules": 0, "python_modules": 0, "index": 0, "tokens": 0}

    def counted(name: str, original: Callable) -> Callable:  # type: ignore[type-arg]
        def wrapper(*args: object, **kwargs: object) -> object:
            calls[name] += 1
            return original(*args, **kwargs)

        return wrapper

    monkeypatch.setattr(
        emitter._Run, "path_modules", counted("path_modules", emitter._Run.path_modules)
    )
    monkeypatch.setattr(
        emitter._Run, "python_modules", counted("python_modules", emitter._Run.python_modules)
    )
    monkeypatch.setattr(emitter, "_tokens", counted("tokens", emitter._tokens))
    original_build = emitter._SymbolIndex.build.__func__  # type: ignore[attr-defined]
    monkeypatch.setattr(
        emitter._SymbolIndex,
        "build",
        classmethod(
            lambda cls, symbols: (
                calls.__setitem__("index", calls["index"] + 1),
                original_build(cls, symbols),
            )[1]
        ),
    )

    drafts = service().index(scan, None, parsed, file_logical_ids=ids)

    assert calls["path_modules"] == 1
    assert calls["python_modules"] == 1
    assert calls["index"] == files  # one symbol index per file, not per reference
    assert calls["tokens"] <= symbols + files  # each name tokenized once (plus module owners)
    assert len(_relations(drafts, "IMPORTS")) == files
    assert len(_relations(drafts, "CALLS")) >= files * members


def test_requests_are_bounded_by_bytes_as_well_as_count() -> None:
    from agent_context_platform.indexing.emitter import _chunks
    from agent_context_platform.indexing.registry import DEFAULT_REQUEST_SOURCE_BYTES as budget
    from agent_context_platform.indexing.tree_sitter.base import SourceFile

    def source(name: str, size: int) -> SourceFile:
        return SourceFile.from_bytes(name, "python", b"a" * size)

    # Count bound: 130 tiny files.
    tiny = [source(f"t{i:03d}.py", 3) for i in range(130)]
    assert [len(c) for c in _chunks(tiny, budget)] == [64, 64, 2]

    # Byte bound: 40 files of 100 KiB never share a request beyond the budget.
    medium = [source(f"m{i:02d}.py", 100 * 1024) for i in range(40)]
    chunks = list(_chunks(medium, budget))
    assert all(sum(len(f.content()) for f in c) <= budget for c in chunks)
    assert [len(c) for c in chunks][:2] == [10, 10] and sum(map(len, chunks)) == 40

    # A file at the budget goes alone, and its neighbours do not ride along.
    big = source("big.py", budget)
    mixed = [source("a.py", 10), big, source("z.py", 10)]
    assert [[f.path for f in c] for c in _chunks(mixed, budget)] == [["a.py"], ["big.py"], ["z.py"]]


def test_each_adapter_batches_by_its_own_registered_budget(
    languages: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = dict(registry.LANGUAGES)
    for language, budget in (("typescript", 512 * 1024), ("go", 1 << 20)):
        table[language] = dataclasses.replace(table[language], max_request_source_bytes=budget)
    monkeypatch.setattr(registry, "LANGUAGES", MappingProxyType(table))
    calls: dict[str, list[int]] = {}

    def recorder(language: str) -> object:
        class Adapter:
            def parse(self, request: ParseRequest) -> ParsedModule:
                calls.setdefault(language, []).append(sum(len(f.content()) for f in request.files))
                return TableAdapter(language, {}).parse(request)

        Adapter.language = language  # type: ignore[attr-defined]
        return Adapter()

    chunk = b"a" * (200 * 1024)
    sources = {f"f{i}.ts": chunk for i in range(6)} | {f"g{i}.go": chunk for i in range(6)}
    parse_structural(
        {"typescript": recorder("typescript"), "go": recorder("go")},  # type: ignore[dict-item]
        sources,
    )

    assert max(calls["typescript"]) <= 512 * 1024 and len(calls["typescript"]) == 3
    assert max(calls["go"]) <= 1 << 20 and len(calls["go"]) == 2


def test_go_package_names_are_indexed_once_however_many_references(
    languages: None, monkeypatch: pytest.MonkeyPatch, make_repo: Callable[[str], RepoBuilder]
) -> None:
    from agent_context_platform.indexing import emitter
    from agent_context_platform.indexing.emitter import read_sources

    files, members = 40, 4
    repo = make_repo("repo")
    for index in range(files):
        repo.write(f"go/pkg/f{index}.go", "package x\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    names = tuple(f"F{n}" for n in range(members))
    table = {
        f"go/pkg/f{i}.go": tuple(_reference("call", f"F{n}") for n in range(members))
        for i in range(files)
    }
    adapter = TableAdapter("go", table, {f"go/pkg/f{i}.go": names for i in range(files)})
    parsed = list(parse_structural({"go": adapter}, read_sources(scan)).files)  # type: ignore[arg-type]
    scans = {"count": 0}
    original = emitter._Run.scope_files

    def counted(self: object, item: object) -> object:
        scans["count"] += 1
        return original(self, item)  # type: ignore[arg-type]

    monkeypatch.setattr(emitter._Run, "scope_files", counted)

    service().index(scan, None, parsed, file_logical_ids=ids)

    assert scans["count"] <= 2  # one package table, not one scan per reference (1600 of them)


_REPOS = itertools.count()


def _called(
    make_repo: Callable[[str], RepoBuilder],
    paths: tuple[str, ...],
    table: Mapping[str, tuple[ParsedReference, ...]],
    members: Mapping[str, tuple[str, ...]],
) -> set[str]:
    """Qualified names of the symbols the table's calls resolved to."""
    repo = make_repo(f"repo{next(_REPOS)}")
    for path in paths:
        repo.write(path, "x\n")
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan)
    sources = {f.path: (repo.root / f.path).read_bytes() for f in scan.files}
    result = parse_structural(
        {"python": TableAdapter("python", table, members)},  # type: ignore[dict-item]
        sources,
    )
    drafts = service().index(scan, None, list(result.files), file_logical_ids=ids)
    names = {
        d.payload["symbol_id"]: d.payload["qualified_name"]
        for d in by_type(drafts, "code.symbol.indexed")
    }
    return {names[c["object_id"]] for c in _relations(drafts, "CALLS")}


PACKAGE = ("pkg/__init__.py", "pkg/mod.py", "user.py")
PACKAGE_MEMBERS = {"pkg/mod.py": ("f",)}


def test_a_plain_dotted_python_import_binds_its_first_component(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    def resolved(qualifier: str) -> set[str]:
        table = {
            "user.py": (
                _reference("import", "pkg.mod"),
                _reference("call", "f", qualifier=qualifier),
            )
        }
        return _called(make_repo, PACKAGE, table, PACKAGE_MEMBERS)

    assert resolved("pkg.mod") == {"mod.f"}  # ``pkg`` is bound; ``mod`` is consumed from the path
    assert resolved("mod") == set()  # ``import pkg.mod`` does NOT bind ``mod``
    assert resolved("pkg") == set()  # the dotted path must be consumed, not stopped short of it


def test_an_aliased_python_import_binds_the_alias_to_the_whole_dotted_path(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    def resolved(qualifier: str) -> set[str]:
        table = {
            "user.py": (
                _reference("import", "pkg.mod", alias="m"),
                _reference("call", "f", qualifier=qualifier),
            )
        }
        return _called(make_repo, PACKAGE, table, PACKAGE_MEMBERS)

    assert resolved("m") == {"mod.f"}
    assert resolved("pkg.mod") == set()  # an aliased import binds only the alias
    assert resolved("mod") == set()


def test_from_import_binds_the_name_as_a_module_or_as_a_symbol(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    paths = ("pkg/__init__.py", "pkg/mod.py", "pkg/core.py", "user.py")
    members = {"pkg/mod.py": ("f",), "pkg/core.py": ("A", "A.run", "B", "B.run")}

    def resolved(*references: ParsedReference) -> set[str]:
        return _called(make_repo, paths, {"user.py": references}, members)

    module = _reference("import", "mod", qualifier="pkg")
    assert resolved(module, _reference("call", "f", qualifier="mod")) == {"mod.f"}
    assert resolved(module, _reference("call", "f", qualifier="pkg.mod")) == set()
    aliased = _reference("import", "mod", qualifier="pkg", alias="m")
    assert resolved(aliased, _reference("call", "f", qualifier="m")) == {"mod.f"}
    assert resolved(aliased, _reference("call", "f", qualifier="mod")) == set()
    symbol = _reference("import", "A", qualifier="pkg.core")
    assert resolved(symbol, _reference("call", "run", qualifier="A")) == {"core.A.run"}
    renamed = _reference("import", "A", qualifier="pkg.core", alias="Z")
    assert resolved(renamed, _reference("call", "run", qualifier="Z")) == {"core.A.run"}
    assert resolved(renamed, _reference("call", "A")) == set()  # only the alias is bound
    imported_function = _reference("import", "f", qualifier="pkg.mod", alias="g")
    assert resolved(imported_function, _reference("call", "g")) == {"mod.f"}


def test_the_rest_of_a_cross_file_qualifier_constrains_the_owner_chain(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    paths = ("mod.py", "user.py")
    members = {"mod.py": ("A", "A.run", "B", "B.run", "helper")}

    def resolved(qualifier: str, name: str = "run") -> set[str]:
        table = {
            "user.py": (_reference("import", "mod"), _reference("call", name, qualifier=qualifier))
        }
        return _called(make_repo, paths, table, members)

    assert resolved("mod.A") == {"mod.A.run"}  # not B.run, and not ambiguous
    assert resolved("mod.B") == {"mod.B.run"}
    assert resolved("mod.C") == set()  # a missing owner resolves to nothing
    assert resolved("mod") == set()  # ``mod.run()``: no module-level ``run``
    assert resolved("mod", "helper") == {"mod.helper"}
    assert resolved("mod.A", "helper") == set()  # ``helper`` is not a member of ``A``


def test_only_an_import_can_carry_an_alias() -> None:
    with pytest.raises(ValueError, match="only an import binds an alias"):
        _reference("call", "f", alias="g")
