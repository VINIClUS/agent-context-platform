"""Adapter contract: models, validation and the sandboxed runner against misbehaving adapters."""

from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import resource
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from agent_context_platform.indexing.tree_sitter import base, runner
from agent_context_platform.indexing.tree_sitter.base import (
    ParsedFile,
    ParsedModule,
    ParsedSymbol,
    ParseRequest,
    SourceFile,
    StructuralAdapter,
    StructuralError,
    StructuralErrorCode,
    StructuralRelation,
    parser_fingerprint,
    validate_module,
)
from agent_context_platform.indexing.tree_sitter.runner import Limits, SandboxedAdapter

pytestmark = pytest.mark.unit

FAKE = str(Path(__file__).with_name("fake_adapter.py"))
SOURCE = b"def a():\n    return 1\n"
FINGERPRINT = parser_fingerprint("fake", "1.0", {"grammar": "x"})
DIGEST = "a" * 64


def request(content: bytes = SOURCE, path: str = "pkg/mod.py") -> ParseRequest:
    return ParseRequest(files=(SourceFile.from_bytes(path, "python", content),))


def symbol(**over: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "ref": "1",
        "language": "python",
        "qualified_name": "pkg.mod.a",
        "kind": "function",
        "disambiguator": "",
        "start_byte": 0,
        "end_byte": 8,
        "signature": "def a()",
        "signature_digest": DIGEST,
        "semantic_fingerprint": DIGEST,
        "evidence_kind": "tree_sitter",
    }
    data.update(over)
    return data


def second(**over: Any) -> dict[str, Any]:
    """A second, distinct symbol: ``return`` sits inside bytes 9..len of ``SOURCE``."""
    data = symbol(
        ref="2",
        qualified_name="pkg.mod.return",
        start_byte=9,
        end_byte=len(SOURCE),
        signature="return 1",
    )
    data.update(over)
    return data


def module(symbols: list[dict[str, Any]] | None = None, **file_over: Any) -> ParsedModule:
    parsed: dict[str, Any] = {
        "path": "pkg/mod.py",
        "language": "python",
        "parser_fingerprint": FINGERPRINT,
        "symbols": symbols if symbols is not None else [symbol()],
        "relations": [],
    }
    parsed.update(file_over)
    return ParsedModule.model_validate_json(json.dumps({"protocol_version": 1, "files": [parsed]}))


def refused(code: StructuralErrorCode, candidate: ParsedModule, req: ParseRequest | None = None):
    with pytest.raises(StructuralError) as caught:
        validate_module(req or request(), candidate, expected_fingerprint=FINGERPRINT)
    assert caught.value.code is code
    assert str(caught.value) == code.value


# --- models and validation --------------------------------------------------------------


def test_valid_module_passes_and_carries_identity_arguments() -> None:
    candidate = module()
    validated = validate_module(request(), candidate, expected_fingerprint=FINGERPRINT)
    assert validated.files[0].path == candidate.files[0].path
    item = validated.files[0].symbols[0]
    for name in (
        "language",
        "qualified_name",
        "kind",
        "disambiguator",
        "signature_digest",
        "semantic_fingerprint",
        "start_byte",
        "end_byte",
    ):
        assert hasattr(item, name)
    assert item.evidence_kind == "tree_sitter"


@pytest.mark.parametrize(
    "missing", ["start_byte", "end_byte", "qualified_name", "kind", "evidence_kind", "signature"]
)
def test_required_symbol_fields(missing: str) -> None:
    data = symbol()
    del data[missing]
    with pytest.raises(ValidationError):
        ParsedSymbol.model_validate(data)


@pytest.mark.parametrize(
    "override",
    [
        {"evidence_kind": "scip"},
        {"evidence_kind": "git"},
        {"start_byte": 5, "end_byte": 5},
        {"start_byte": -1},
        {"qualified_name": ""},
        {"qualified_name": "a\nb"},
        {"qualified_name": "x" * (base.MAX_NAME_BYTES + 1)},
        {"signature": "s" * (base.MAX_SIGNATURE_BYTES + 1)},
        {"signature": "é" * base.MAX_SIGNATURE_BYTES},
        {"disambiguator": "a b"},
        {"disambiguator": "/etc/passwd"},
        {"disambiguator": "hunter2"},
        {"disambiguator": "1234567"},
        {"kind": "macro"},
        {"ref": "abc"},
        {"ref": ""},
        {"kind": "Function"},
        {"kind": "function\n"},
        {"signature_digest": "ABC"},
        {"semantic_fingerprint": "0" * 63},
        {"ref": "has space"},
        {"extra": 1},
        {"start_byte": "0"},
    ],
)
def test_symbol_rejects(override: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        ParsedSymbol.model_validate(symbol(**override))


def test_validation_errors_do_not_echo_input() -> None:
    with pytest.raises(ValidationError) as caught:
        ParsedSymbol.model_validate(symbol(qualified_name="SECRET-\x00-TEXT"))
    assert "SECRET" not in str(caught.value)


def test_relation_requires_evidence_and_range() -> None:
    good = {
        "source_ref": "1",
        "target_ref": "2",
        "kind": "calls",
        "start_byte": 0,
        "end_byte": 1,
        "evidence_kind": "tree_sitter",
    }
    assert StructuralRelation.model_validate(good).kind == "calls"
    for bad in ({**good, "evidence_kind": "llm_inference"}, {**good, "end_byte": 0}):
        with pytest.raises(ValidationError):
            StructuralRelation.model_validate(bad)
    del good["evidence_kind"]
    with pytest.raises(ValidationError):
        StructuralRelation.model_validate(good)


def test_request_validation() -> None:
    with pytest.raises(ValidationError):
        ParseRequest(files=())
    with pytest.raises(ValidationError):
        ParseRequest(files=(SourceFile.from_bytes("a.py", "python", b"x"),) * 2)
    with pytest.raises(ValidationError):
        SourceFile(path="../etc/passwd", language="python", content_b64="")
    with pytest.raises(ValidationError):
        SourceFile(path="a.py", language="python", content_b64="***")
    with pytest.raises(ValidationError):
        SourceFile.from_bytes("a.py", "python", b"x" * (base.MAX_SOURCE_BYTES + 1))
    assert SourceFile.from_bytes("a.py", "python", b"abc").content() == b"abc"


def test_range_beyond_input_is_refused() -> None:
    refused(StructuralErrorCode.RANGE_OUT_OF_BOUNDS, module([symbol(end_byte=len(SOURCE) + 1)]))
    assert validate_module(
        request(), module([symbol(end_byte=len(SOURCE))]), expected_fingerprint=FINGERPRINT
    )


def test_relation_range_beyond_input_is_refused() -> None:
    relation = {
        "source_ref": "1",
        "target_ref": "1",
        "kind": "calls",
        "start_byte": 0,
        "end_byte": len(SOURCE) + 1,
        "evidence_kind": "tree_sitter",
    }
    refused(StructuralErrorCode.RANGE_OUT_OF_BOUNDS, module(relations=[relation]))


def test_foreign_missing_and_repeated_paths_are_refused() -> None:
    refused(StructuralErrorCode.PATH_MISMATCH, module(path="other.py"))
    refused(StructuralErrorCode.PATH_MISMATCH, ParsedModule(files=()))
    twice = module().files[0]
    refused(StructuralErrorCode.PATH_MISMATCH, ParsedModule(files=(twice, twice)))


def test_language_and_fingerprint_mismatch() -> None:
    refused(StructuralErrorCode.LANGUAGE_MISMATCH, module(language="go"))
    refused(StructuralErrorCode.LANGUAGE_MISMATCH, module([symbol(language="go")]))
    refused(StructuralErrorCode.FINGERPRINT_MISMATCH, module(parser_fingerprint="b" * 64))


def test_dangling_relation_endpoints() -> None:
    def relation(source: str, target: str) -> dict[str, Any]:
        return {
            "source_ref": source,
            "target_ref": target,
            "kind": "calls",
            "start_byte": 0,
            "end_byte": 1,
            "evidence_kind": "tree_sitter",
        }

    refused(StructuralErrorCode.DANGLING_RELATION, module(relations=[relation("1", "99")]))
    refused(StructuralErrorCode.DANGLING_RELATION, module(relations=[relation("99", "1")]))
    ok = module([symbol(), second()], relations=[relation("1", "2")])
    assert validate_module(request(), ok, expected_fingerprint=FINGERPRINT)


def test_duplicate_symbols_are_refused() -> None:
    refused(
        StructuralErrorCode.DUPLICATE_SYMBOL,
        module([symbol(), second(ref="1")]),
    )


def test_count_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(base, "MAX_SYMBOLS_PER_FILE", 1)
    refused(
        StructuralErrorCode.COUNT_EXCEEDED,
        module([symbol(), second()]),
    )
    monkeypatch.setattr(base, "MAX_RELATIONS_PER_FILE", 0)
    relation = {
        "source_ref": "1",
        "target_ref": "1",
        "kind": "calls",
        "start_byte": 0,
        "end_byte": 1,
        "evidence_kind": "tree_sitter",
    }
    refused(StructuralErrorCode.COUNT_EXCEEDED, module(relations=[relation]))


def test_parser_fingerprint_is_canonical() -> None:
    assert parser_fingerprint("p", "1", {"b": 1, "a": 2}) == parser_fingerprint(
        "p", "1", {"a": 2, "b": 1}
    )
    assert parser_fingerprint("p", "1") != parser_fingerprint("p", "2")
    assert parser_fingerprint("p", "1") != parser_fingerprint("p", "1", {"a": 1})
    assert len(parser_fingerprint("p", "1")) == 64


# --- sandbox ----------------------------------------------------------------------------


def adapter(mode: str, arg: str = "", **kwargs: Any) -> SandboxedAdapter:
    command = [sys.executable, FAKE, mode, *([arg] if arg else [])]
    limits = kwargs.pop("limits", Limits(wall_seconds=3.0, cpu_seconds=2))
    env = {"FAKE_FINGERPRINT": FINGERPRINT, **kwargs.pop("env", {})}
    return SandboxedAdapter(
        command,
        language="python",
        parser_name="fake",
        parser_version="1.0",
        parser_config={"grammar": "x"},
        limits=limits,
        env=env,
        **kwargs,
    )


def outcome(mode: str, arg: str = "", **kwargs: Any) -> StructuralErrorCode | ParsedModule:
    try:
        return adapter(mode, arg, **kwargs).parse(request())
    except StructuralError as error:
        return error.code


def test_well_behaved_adapter_round_trips() -> None:
    result = adapter("ok").parse(request())
    assert isinstance(result, ParsedModule)
    assert result.files[0].symbols[0].qualified_name == "pkg.a"
    assert isinstance(adapter("ok"), StructuralAdapter)
    assert adapter("ok").language == "python"
    assert adapter("ok").fingerprint == FINGERPRINT


@pytest.mark.parametrize(
    ("mode", "code"),
    [
        ("range", StructuralErrorCode.RANGE_OUT_OF_BOUNDS),
        ("foreign_path", StructuralErrorCode.PATH_MISMATCH),
        ("dangling", StructuralErrorCode.DANGLING_RELATION),
        ("wrong_evidence", StructuralErrorCode.SCHEMA_VIOLATION),
        ("extra_field", StructuralErrorCode.SCHEMA_VIOLATION),
        ("duplicate", StructuralErrorCode.DUPLICATE_SYMBOL),
        ("bad_language", StructuralErrorCode.LANGUAGE_MISMATCH),
        ("bad_json", StructuralErrorCode.SCHEMA_VIOLATION),
        ("crash", StructuralErrorCode.NONZERO_EXIT),
    ],
)
def test_misbehaving_output_is_refused(mode: str, code: StructuralErrorCode) -> None:
    assert outcome(mode) is code


def test_untrusted_fingerprint_is_refused() -> None:
    result = outcome("ok", env={"FAKE_FINGERPRINT": "c" * 64})
    assert result is StructuralErrorCode.FINGERPRINT_MISMATCH


def test_huge_output_is_killed_at_the_bound() -> None:
    limits = Limits(wall_seconds=5.0, max_output_bytes=100_000)
    assert outcome("huge", "5000000", limits=limits) is StructuralErrorCode.OUTPUT_TOO_LARGE


def test_stderr_flood_is_bounded_and_discarded() -> None:
    limits = Limits(wall_seconds=5.0, max_stderr_bytes=1_000)
    assert outcome("stderr_flood", limits=limits) is StructuralErrorCode.STDERR_TOO_LARGE


def test_hang_hits_the_wall_clock() -> None:
    started = time.monotonic()
    assert outcome("hang", limits=Limits(wall_seconds=1.0)) is StructuralErrorCode.TIMEOUT
    assert time.monotonic() - started < 5


def test_adapter_that_never_reads_a_large_input_does_not_deadlock() -> None:
    big = request(b"x" * base.MAX_SOURCE_BYTES)
    started = time.monotonic()
    with pytest.raises(StructuralError) as caught:
        adapter("no_read_hang", limits=Limits(wall_seconds=1.0)).parse(big)
    assert caught.value.code is StructuralErrorCode.TIMEOUT
    assert time.monotonic() - started < 5


def test_adapter_that_ignores_stdin_is_reaped() -> None:
    with pytest.raises(StructuralError) as caught:
        adapter("ignore_stdin").parse(request(b"x" * 500_000))
    assert caught.value.code is StructuralErrorCode.PATH_MISMATCH


def test_cpu_spin_is_stopped() -> None:
    limits = Limits(wall_seconds=8.0, cpu_seconds=1)
    assert outcome("spin", limits=limits) is StructuralErrorCode.NONZERO_EXIT


def test_memory_hog_is_stopped_by_the_address_space_limit() -> None:
    limits = Limits(wall_seconds=5.0, address_space_bytes=256 * 1024 * 1024, cpu_seconds=3)
    assert outcome("memhog", limits=limits) is StructuralErrorCode.NONZERO_EXIT


@pytest.mark.skipif(os.geteuid() == 0, reason="RLIMIT_NPROC is not enforced for root")
def test_fork_bomb_is_bounded_by_nproc_and_cleaned_up() -> None:
    limits = Limits(wall_seconds=5.0, processes=8, cpu_seconds=3)
    # Exit status 7 means fork() failed (EAGAIN) before 200 children were made.
    assert outcome("forkbomb", limits=limits) is StructuralErrorCode.NONZERO_EXIT


def test_write_attempt_writes_nothing(tmp_path: Path) -> None:
    target = tmp_path / "escape.bin"
    assert outcome("write", str(target)) is StructuralErrorCode.NONZERO_EXIT
    assert not target.exists() or target.stat().st_size == 0


def test_secrets_of_the_parent_do_not_reach_the_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_SECRET", "hunter2")
    # The fake exits 6 (a non-zero exit) if it can see the secret.
    assert isinstance(outcome("env"), ParsedModule)


def test_working_directory_is_a_removed_temp_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []
    real = runner.tempfile.mkdtemp

    def spy(*args: Any, **kwargs: Any) -> str:
        path = real(*args, **kwargs)
        seen.append(path)
        return path

    monkeypatch.setattr(runner.tempfile, "mkdtemp", spy)
    outcome("ok")
    assert seen
    assert not Path(seen[0]).exists()


def test_spawn_failure_is_typed(tmp_path: Path) -> None:
    missing = SandboxedAdapter(
        [str(tmp_path / "missing")],
        language="python",
        parser_name="x",
        parser_version="1",
        limits=Limits(wall_seconds=2.0),
    )
    # The trampoline starts, then execv fails: the child exits non-zero.
    with pytest.raises(StructuralError) as caught:
        missing.parse(request())
    assert caught.value.code is StructuralErrorCode.NONZERO_EXIT


def test_spawn_oserror_is_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args: Any, **kwargs: Any) -> None:
        raise OSError("no")

    monkeypatch.setattr(runner.subprocess, "Popen", boom)
    assert outcome("ok") is StructuralErrorCode.SPAWN_FAILED


def test_oversized_request_and_language_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "MAX_REQUEST_BYTES", 10)
    assert outcome("ok") is StructuralErrorCode.INPUT_TOO_LARGE
    wrong = ParseRequest(files=(SourceFile.from_bytes("a.go", "go", b"x"),))
    with pytest.raises(StructuralError) as caught:
        adapter("ok").parse(wrong)
    assert caught.value.code is StructuralErrorCode.LANGUAGE_MISMATCH


def test_constructor_guards() -> None:
    with pytest.raises(ValueError, match="absolute"):
        SandboxedAdapter(["python"], language="python", parser_name="x", parser_version="1")
    with pytest.raises(ValueError, match="absolute"):
        SandboxedAdapter([], language="python", parser_name="x", parser_version="1")
    with pytest.raises(ValueError, match="positive"):
        Limits(cpu_seconds=0)
    assert Limits().rlimits()["RLIMIT_FSIZE"] == 0
    assert Limits().rlimits()["RLIMIT_CORE"] == 0


def test_child_env_is_an_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_SECRET", "hunter2")
    monkeypatch.setenv("LANG", "C.UTF-8")
    env = runner._child_env({"EXTRA": "1"})
    assert env == {
        "LANG": "C.UTF-8",
        "EXTRA": "1",
        **({"LC_ALL": os.environ["LC_ALL"]} if "LC_ALL" in os.environ else {}),
    }


# --- adapter-side serve -----------------------------------------------------------------


def serve_with(raw: bytes, parse: Callable[[ParseRequest], ParsedModule]) -> tuple[int, bytes]:
    sink = io.BytesIO()
    return runner.serve(parse, io.BytesIO(raw), sink), sink.getvalue()


def test_serve_round_trip_and_failures() -> None:
    raw = request().model_dump_json().encode()
    status, out = serve_with(raw, lambda _req: module())
    assert status == 0
    assert ParsedModule.model_validate_json(out) == module()
    assert serve_with(b"not json", lambda _req: module())[0] == 3
    assert serve_with(b"x" * (runner.MAX_REQUEST_BYTES + 1), lambda _req: module())[0] == 2

    def crash(_req: ParseRequest) -> ParsedModule:
        raise RecursionError

    assert serve_with(raw, crash) == (3, b"")


def test_serve_defaults_to_standard_streams(monkeypatch: pytest.MonkeyPatch) -> None:
    class Text:
        buffer = io.BytesIO(request().model_dump_json().encode())

    class Out:
        buffer = io.BytesIO()

    monkeypatch.setattr(sys, "stdin", Text)
    monkeypatch.setattr(sys, "stdout", Out)
    assert runner.serve(lambda _req: module()) == 0
    assert json.loads(Out.buffer.getvalue())["files"][0]["path"] == "pkg/mod.py"


def test_source_file_round_trip_is_base64() -> None:
    encoded = base64.b64encode(SOURCE).decode()
    assert SourceFile(path="a.py", language="python", content_b64=encoded).content() == SOURCE
    assert ParsedFile.model_fields["symbols"] is not None


def test_readonly_environment_falls_back_to_root_cwd(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: Any, **kwargs: Any) -> str:
        raise OSError("read-only file system")

    monkeypatch.setattr(runner.tempfile, "mkdtemp", refuse)
    assert isinstance(outcome("ok"), ParsedModule)


# --- output text confinement ------------------------------------------------------------

PY_SOURCE = (
    b"from typing import Any\n\n"
    b"class Repo:\n"
    b"    def fetch(self, key: str, default: Any = None) -> Any:\n"
    b"        return default\n"
)
TS_SOURCE = (
    b"export class Store<T> {\n"
    b"  get(id: string,\n      fallback?: T): T | undefined {\n    return fallback;\n  }\n}\n"
    b"namespace Util { export function wrap() {} }\n"
)


def confined(content: bytes, path: str, symbols: list[dict[str, Any]]) -> None:
    candidate = module(symbols, path=path)
    validate_module(request(content, path), candidate, expected_fingerprint=FINGERPRINT)


def test_realistic_python_and_typescript_names_and_signatures_pass() -> None:
    fetch = PY_SOURCE.index(b"def fetch")
    end = PY_SOURCE.index(b"Any:\n", fetch) + 4
    confined(
        PY_SOURCE,
        "src/repo/store.py",
        [
            symbol(
                qualified_name="repo.store.Repo",
                kind="class",
                start_byte=PY_SOURCE.index(b"class"),
                end_byte=len(PY_SOURCE),
                signature="class Repo",
            ),
            symbol(
                ref="2",
                qualified_name="repo.store.Repo.fetch",
                start_byte=fetch,
                end_byte=end,
                signature="def fetch(self, key: str, default: Any = None) -> Any",
            ),
        ],
    )
    start = TS_SOURCE.index(b"get(")
    confined(
        TS_SOURCE,
        "lib/store.ts",
        [
            symbol(
                ref="3",
                qualified_name="store::Store",
                kind="class",
                start_byte=0,
                end_byte=len(TS_SOURCE) - 1,
                signature="export class Store<T>",
            ),
            symbol(
                qualified_name="store::Store#get",
                start_byte=start,
                end_byte=TS_SOURCE.index(b"{\n    return"),
                signature="get(id: string, fallback?: T): T | undefined",
            ),
            symbol(
                ref="2",
                qualified_name="Util.wrap",
                start_byte=TS_SOURCE.index(b"namespace"),
                end_byte=len(TS_SOURCE) - 1,
                signature="export function wrap()",
            ),
        ],
    )


@pytest.mark.parametrize(
    "override",
    [
        {"qualified_name": "root:x:0:0:root"},
        {"qualified_name": "pkg.mod.etc/passwd"},
        {"qualified_name": "pkg.zzz_secret_token"},
        {"signature": "DB_PASSWORD=hunter2"},
        {"signature": "def a() # smuggled"},
        {"qualified_name": "pkg.mod.zzz_secret_token"},
    ],
)
def test_text_not_in_the_parsed_file_is_refused(override: dict[str, Any]) -> None:
    refused(StructuralErrorCode.TEXT_NOT_IN_SOURCE, module([symbol(**override)]))


def test_signature_must_come_from_the_symbols_own_range() -> None:
    other = b"def a():\n    pass\n\ndef zed(secretish: int) -> None:\n    pass\n"
    zed = other.index(b"def zed")
    inside = symbol(
        qualified_name="pkg.mod.zed",
        start_byte=zed,
        end_byte=len(other),
        signature="def zed(secretish: int) -> None",
    )
    confined(other, "pkg/mod.py", [inside])
    outside = {**inside, "start_byte": 0, "end_byte": 8}
    with pytest.raises(StructuralError) as caught:
        validate_module(request(other), module([outside]), expected_fingerprint=FINGERPRINT)
    assert caught.value.code is StructuralErrorCode.TEXT_NOT_IN_SOURCE


def test_total_name_bytes_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(base, "MAX_NAME_TOTAL_BYTES", 12)
    refused(StructuralErrorCode.COUNT_EXCEEDED, module([symbol(), second()]))


@pytest.mark.parametrize(
    "name",
    [
        "h.u.n.t.e.r.2",  # one-character segments spelling out foreign data
        "pkg.mod.a.",  # empty segment
        "pkg.mod.2",  # not an identifier
        "p.mod.a",  # non-final segment that is a token nowhere it may be
        "h.u.n.t.e.r",  # spelling attack: single tokens from elsewhere in the file
        "pkg.mod.return",  # final token exists in the file, but outside the symbol's range
        "<lambda>",
    ],
)
def test_names_are_confined_to_identifier_tokens(name: str) -> None:
    refused(StructuralErrorCode.TEXT_NOT_IN_SOURCE, module([symbol(qualified_name=name)]))


def test_single_character_final_segment_and_module_names() -> None:
    assert validate_module(request(), module([symbol()]), expected_fingerprint=FINGERPRINT)
    named_module = module([symbol(qualified_name="pkg.mod", kind="module", signature="")])
    assert validate_module(request(), named_module, expected_fingerprint=FINGERPRINT)
    # A path component only qualifies a module, never another kind.
    refused(StructuralErrorCode.TEXT_NOT_IN_SOURCE, module([symbol(qualified_name="pkg.mod")]))


def test_whitespace_only_signature_and_collapsing() -> None:
    assert validate_module(
        request(), module([symbol(signature="")]), expected_fingerprint=FINGERPRINT
    )
    assert validate_module(
        request(), module([symbol(signature="def   a()")]), expected_fingerprint=FINGERPRINT
    )


# --- descendants ------------------------------------------------------------------------


def _marked(marker: str) -> list[int]:
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            argv = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if marker.encode() in argv:
            found.append(int(entry.name))
    return found


def roomy(wall: float = 4.0) -> Limits:
    """Limits whose NPROC lets a test adapter fork (the dev machine's UID has many tasks)."""
    hard = resource.getrlimit(resource.RLIMIT_NPROC)[1]
    return Limits(
        wall_seconds=wall, cpu_seconds=2, processes=1_000_000 if hard < 0 else min(hard, 1_000_000)
    )


@pytest.fixture
def marker() -> Any:
    tag = f"ac-marker-{uuid.uuid4().hex}"
    yield tag
    for pid in _marked(tag):
        with contextlib.suppress(OSError):
            os.kill(pid, signal.SIGKILL)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="subreaper is Linux-only")
@pytest.mark.parametrize("mode", ["setsid_child", "double_fork", "ignore_term"])
def test_no_descendant_survives_a_successful_run(mode: str, marker: str) -> None:
    assert isinstance(outcome(mode, marker, limits=roomy()), ParsedModule)
    assert _marked(marker) == []


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="subreaper is Linux-only")
def test_no_descendant_survives_a_timeout(marker: str) -> None:
    result = outcome("hang_with_daemon", marker, limits=roomy(1.5))
    assert result is StructuralErrorCode.TIMEOUT
    assert _marked(marker) == []


def _zombie_children() -> list[int]:
    found = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            info = runner._stat(int(entry.name))
            if info is not None and info.ppid == os.getpid() and info.state == "Z":
                found.append(int(entry.name))
    return found


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="subreaper is Linux-only")
def test_killing_the_supervisor_does_not_leave_orphans_or_zombies(marker: str) -> None:
    for _ in range(3):
        result = outcome("kill_supervisor", marker, limits=roomy())
        assert result is StructuralErrorCode.NONZERO_EXIT
        assert _marked(marker) == []
    assert _zombie_children() == []
    assert isinstance(outcome("ok"), ParsedModule)
    assert not runner._LIVE_SUPERVISORS


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="subreaper is Linux-only")
def test_many_supervisor_kills_leave_no_orphans_or_zombies(marker: str) -> None:
    for _ in range(30):
        outcome("kill_supervisor", marker, limits=roomy())
    assert isinstance(outcome("ok"), ParsedModule)
    assert _marked(marker) == []
    assert _zombie_children() == []


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="subreaper is Linux-only")
def test_a_run_sweeps_orphans_left_by_a_finished_run(marker: str) -> None:
    """An orphan with a dead token is swept by the next run's start sweep."""
    runner._ensure_subreaper()
    orphan = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)", marker],
        env={runner._RUN_KEY: "dead-token"},
        start_new_session=True,
    )
    for _ in range(200):  # environ is empty until the interpreter has finished starting
        if runner._has_run_key(orphan.pid):
            break
        time.sleep(0.01)
    assert runner._has_run_key(orphan.pid)
    assert isinstance(outcome("ok"), ParsedModule)
    assert orphan.poll() is not None or orphan.wait(5) is not None
    assert _marked(marker) == []


def test_subreaper_failure_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "_subreaper_set", False)

    class NoLibc:
        def prctl(self, *args: Any) -> int:
            return -1

    monkeypatch.setattr(runner.ctypes, "CDLL", lambda *a, **k: NoLibc())
    assert outcome("ok") is StructuralErrorCode.SPAWN_FAILED


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="subreaper is Linux-only")
def test_repeated_daemonizing_runs_leave_nothing(marker: str) -> None:
    for _ in range(5):
        assert isinstance(outcome("double_fork", marker, limits=roomy()), ParsedModule)
    assert _marked(marker) == []


def test_non_linux_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner.sys, "platform", "darwin")
    assert outcome("ok") is StructuralErrorCode.SPAWN_FAILED


# --- opaque values ----------------------------------------------------------------------


def test_every_output_field_is_classified() -> None:
    for model in (ParsedModule, ParsedFile, ParsedSymbol, StructuralRelation):
        assert set(model.model_fields) == set(base.FIELD_CONFINEMENT[model.__name__]), model
    rehashed = {
        name
        for fields in base.FIELD_CONFINEMENT.values()
        for name, how in fields.items()
        if how == "rehashed"
    }
    assert rehashed == set(base.OPAQUE_FIELDS)


def test_free_form_string_fields_are_token_confined_or_rehashed() -> None:
    """A future free-form field must be classified, and only those classes may be free text."""
    allowed = {"token", "enum", "range", "parent", "derived", "rehashed", "structure"}
    for fields in base.FIELD_CONFINEMENT.values():
        assert set(fields.values()) <= allowed


def test_opaque_digests_are_rehashed_in_the_parent() -> None:
    canary = b"SECRET-CANARY-0123456789abcdef!".hex()  # 62 hex chars of stolen data
    child = (canary + "0" * 64)[:64]
    poisoned = module(
        [symbol(signature_digest=child, semantic_fingerprint=child)],
    )
    stored = validate_module(request(), poisoned, expected_fingerprint=FINGERPRINT)
    item = stored.files[0].symbols[0]
    for name in base.OPAQUE_FIELDS:
        value = getattr(item, name)
        assert value == base.rehash_opaque(name, child)
        assert len(value) == 64
        assert canary[:16] not in value
        assert value != child
    assert item.signature_digest != item.semantic_fingerprint  # domain separated by field


def test_rehash_is_deterministic_and_injective_on_the_child_value() -> None:
    def digest_of(value: str) -> str:
        checked = validate_module(
            request(), module([symbol(signature_digest=value)]), expected_fingerprint=FINGERPRINT
        )
        return checked.files[0].symbols[0].signature_digest

    assert digest_of("1" * 64) == digest_of("1" * 64)
    assert digest_of("1" * 64) != digest_of("2" * 64)


# --- structural name binding, derived numerics, race --------------------------------------

SRC_A = b"class A:\n    def run(self):\n        pass\n\nx = 1\nmod = 2\nhunter = 3\n"


def _names(content: bytes, path: str, symbols: list[dict[str, Any]]) -> StructuralErrorCode | None:
    try:
        confined(content, path, symbols)
    except StructuralError as error:
        return error.code
    return None


def test_single_character_enclosing_segments_are_accepted() -> None:
    run = SRC_A.index(b"def run")
    cls = symbol(
        qualified_name="A", kind="class", start_byte=0, end_byte=len(SRC_A), signature="class A"
    )
    method = symbol(
        ref="2",
        qualified_name="A.run",
        start_byte=run,
        end_byte=SRC_A.index(b"pass") + 4,
        signature="def run(self)",
    )
    assert _names(SRC_A, "pkg/mod.py", [cls, method]) is None


def test_single_character_module_path_segments_are_accepted() -> None:
    f = SRC_A.index(b"class")
    assert (
        _names(
            SRC_A,
            "x/mod.py",
            [
                symbol(
                    qualified_name="x.mod.A",
                    start_byte=f,
                    end_byte=len(SRC_A),
                    signature="class A:",
                )
            ],
        )
        is None
    )


def test_go_receiver_declared_in_another_file_is_accepted() -> None:
    go = b"func (t *T) m() {}\n"
    candidate = module(
        [
            symbol(
                qualified_name="T.m",
                start_byte=0,
                end_byte=len(go),
                signature="func (t *T) m() {}",
                language="go",
            )
        ],
        path="a.go",
        language="go",
    )
    other = SourceFile(
        path="b.go", language="go", content_b64=base64.b64encode(b"type T struct{}\n").decode()
    )
    first = SourceFile(path="a.go", language="go", content_b64=base64.b64encode(go).decode())
    checked = validate_module(
        ParseRequest(files=(first, other)),
        ParsedModule.model_validate_json(
            json.dumps(
                {
                    "protocol_version": 1,
                    "files": [
                        json.loads(candidate.files[0].model_dump_json()),
                        {
                            "path": "b.go",
                            "language": "go",
                            "parser_fingerprint": FINGERPRINT,
                            "symbols": [],
                            "relations": [],
                        },
                    ],
                }
            )
        ),
        expected_fingerprint=FINGERPRINT,
    )
    assert checked.files[0].symbols[0].qualified_name == "T.m"


def test_spelling_out_a_word_from_tokens_elsewhere_is_refused() -> None:
    src = b"def h(): pass\nu = n = t = e = r = 1\n"
    bad = symbol(qualified_name="h.u.n.t.e.r", start_byte=0, end_byte=13, signature="def h(): pass")
    assert _names(src, "pkg/mod.py", [bad]) is StructuralErrorCode.TEXT_NOT_IN_SOURCE


def test_disambiguator_is_derived_by_the_parent() -> None:
    late = second(ref="9", start_byte=12, disambiguator="7", signature="return 1")
    early = second(ref="4", start_byte=9, disambiguator="999", signature="return 1")
    out = validate_module(request(), module([late, early]), expected_fingerprint=FINGERPRINT)
    got = out.files[0].symbols
    # Ordinals follow (start, end, kind), not the adapter's order or its own values.
    assert [(x.start_byte, x.disambiguator) for x in got] == [(12, "1"), (9, "0")]
    assert [x.ref for x in got] == ["0", "1"]
    lone = validate_module(
        request(), module([symbol(disambiguator="123456")]), expected_fingerprint=FINGERPRINT
    )
    assert lone.files[0].symbols[0].disambiguator == "0"


def test_relations_are_remapped_and_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    rel = {
        "source_ref": "2",
        "target_ref": "1",
        "kind": "calls",
        "start_byte": 9,
        "end_byte": 10,
        "evidence_kind": "tree_sitter",
    }
    out = validate_module(
        request(), module([symbol(), second()], relations=[rel]), expected_fingerprint=FINGERPRINT
    )
    assert (out.files[0].relations[0].source_ref, out.files[0].relations[0].target_ref) == (
        "1",
        "0",
    )
    monkeypatch.setattr(base, "MAX_RELATIONS_PER_SYMBOL", 1)
    refused(StructuralErrorCode.COUNT_EXCEEDED, module([symbol(), second()], relations=[rel, rel]))


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="subreaper is Linux-only")
def test_a_concurrent_run_does_not_sweep_a_supervisor_being_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading

    real = subprocess.Popen
    spawned = threading.Event()
    first = [True]

    def slow(*args: Any, **kwargs: Any) -> Any:
        process = real(*args, **kwargs)
        if first[0]:
            first[0] = False
            spawned.set()
            time.sleep(0.5)  # descheduled between Popen and registration
        return process

    monkeypatch.setattr(runner.subprocess, "Popen", slow)
    results: list[Any] = []
    thread = threading.Thread(target=lambda: results.append(outcome("ok")))
    thread.start()
    assert spawned.wait(5)
    other = outcome("ok")
    thread.join()
    assert isinstance(other, ParsedModule)
    assert isinstance(results[0], ParsedModule)
