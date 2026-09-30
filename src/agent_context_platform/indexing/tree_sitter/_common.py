"""Helpers shared by the syntax-only language adapters (Python, TypeScript/JavaScript, Go).

Everything here is adapter-side and untrusted-input-safe: the parent re-checks the
output with ``base.validate_module``. The rules below exist so that real (and hostile)
source never makes the *parent* refuse a whole batch of up to 64 files: an adapter skips
what it cannot express instead of emitting something the confinement rules would reject.

Reused by PLATFORM-034/035: ``frame``/``digest`` (normalized-token fingerprints),
``cut_signature``, ``identifier_bytes``, ``module_name``, ``Budget`` (name/output/work
limits, symbol and reference names), ``dotted_name_at`` (a reference name the parent will
accept), ``diagnostics``/``degraded_file`` (closed-enum degradation reports), ``safe_module``
(per-file degradation) and ``normalize_module`` (golden-test view).
"""

from __future__ import annotations

import hashlib
import json
import re
import struct
import time
from collections.abc import Callable, Iterable, Iterator
from typing import Any, Final

from pydantic import ValidationError
from tree_sitter import Parser, Tree

from agent_context_platform.indexing.tree_sitter.base import (
    DIAGNOSTIC_CODES,
    MAX_DIAGNOSTIC_COUNT,
    MAX_NAME_BYTES,
    MAX_NAME_TOTAL_BYTES,
    MAX_OUTPUT_BYTES,
    DiagnosticCode,
    ParsedDiagnostic,
    ParsedFile,
    ParsedModule,
    ParsedReference,
    ParseRequest,
    SourceFile,
    StructuralError,
    validate_module,
)

# Same token rule as base._IDENT: the parent finds names by these maximal runs.
_IDENT: Final = re.compile(rb"[A-Za-z_$\x80-\xff][A-Za-z0-9_$\x80-\xff]*")
_WHITESPACE: Final = re.compile(rb"[ \t\r\n\f\v]+")
_CONTROLS: Final = re.compile(rb"[\x00-\x08\x0e-\x1f\x7f]")
_TOKEN_BYTE: Final = re.compile(rb"[A-Za-z0-9_$\x80-\xff]")
_SEPARATOR_SPACE: Final = rb"[ \t\r\n\f\v]*"
# ``a``, ``a.b``, ``a . b``: what ``base._SourceText.path_ok`` accepts for a reference name.
_DOTTED: Final = re.compile(
    b"(?:"
    + _IDENT.pattern
    + b")(?:"
    + _SEPARATOR_SPACE
    + rb"\."
    + _SEPARATOR_SPACE
    + b"(?:"
    + _IDENT.pattern
    + b"))*"
)
# Reference cost estimate for the output bound: JSON framing of one ParsedReference.
_REFERENCE_OVERHEAD: Final = 300
READ_CHUNK: Final = 4096

# Signatures are cut well below MAX_SIGNATURE_BYTES so 10k symbols still fit the output bound.
SIGNATURE_CUT: Final = 256
# The whole answer (all files) must stay under MAX_OUTPUT_BYTES; leave room for JSON framing.
OUTPUT_BUDGET: Final = MAX_OUTPUT_BYTES - 1_048_576
_SYMBOL_OVERHEAD: Final = 420  # JSON framing + digests + fixed fields of one symbol


def frame(token: bytes) -> bytes:
    """Length-prefixed token, so concatenated tokens can never be re-split differently."""
    return struct.pack(">I", len(token)) + token


def digest(domain: bytes, tokens: Iterable[bytes]) -> str:
    """SHA-256 over already-framed tokens; the parent re-hashes it, determinism is what counts."""
    hasher = hashlib.sha256(domain)
    for token in tokens:
        hasher.update(token)
    return hasher.hexdigest()


def identifier_bytes(raw: bytes, content: bytes, start: int, end: int) -> str | None:
    """``raw`` as a name, or None when the parent would not find it as a whole source token.

    The parent looks names up among maximal ``[A-Za-z0-9_$\\x80-\\xff]`` runs, so an identifier
    glued to such a byte (``f→``) or with invalid UTF-8 must be skipped, not emitted.
    """
    if not _IDENT.fullmatch(raw):
        return None
    if start > 0 and _TOKEN_BYTE.fullmatch(content, start - 1, start):
        return None
    if _TOKEN_BYTE.fullmatch(content, end, end + 1):
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def cut_signature(raw: bytes) -> str:
    """Whitespace-collapsed, control-free, valid-UTF-8 prefix of ``raw`` (a contiguous run).

    Cut, never rewritten: the parent requires the signature to be a contiguous run of the
    whitespace-collapsed source, so replacing bytes (U+FFFD) would be refused.
    """
    collapsed = _WHITESPACE.sub(b" ", raw).strip()
    control = _CONTROLS.search(collapsed)
    if control:
        collapsed = collapsed[: control.start()]
    collapsed = collapsed[:SIGNATURE_CUT]
    while True:
        try:
            return collapsed.decode("utf-8").rstrip()
        except UnicodeDecodeError as error:
            collapsed = collapsed[: error.start]


def module_name(path: str, *, package_files: tuple[str, ...] = ("__init__",)) -> str:
    """Dotted module name from the path: the trailing run of identifier components.

    ``pkg/sub/mod.py`` -> ``pkg.sub.mod``; ``pkg/__init__.py`` -> ``pkg`` (a package);
    ``src/my-pkg/mod.py`` -> ``mod`` (components that are not identifiers, such as ``my-pkg``,
    ``2024`` or ``.github``, end the run); ``pkg/m\u00f3dulo.py`` -> ``pkg.m\u00f3dulo`` (Unicode
    identifiers are kept as written). Empty string when no name can be formed.
    """
    parts = path.split("/")
    stem = parts[-1].rsplit(".", 1)[0] if "." in parts[-1] else parts[-1]
    parts = [*parts[:-1], stem]
    if stem in package_files:
        parts.pop()
    run: list[str] = []
    for part in reversed(parts):
        if not part.isidentifier():  # Unicode names stay: they equal the path bytes
            break
        run.append(part)
    return ".".join(reversed(run))


def dotted_name_at(content: bytes, start: int, end: int) -> str | None:
    """The name spelled by ``content[start:end]`` (``a`` or ``a . b``), or None.

    None whenever the parent's reference check would refuse the range: empty (a MISSING
    node), longer than a name, anything but identifiers joined by ``.`` (a comment, a line
    continuation, a call), a token cut by either end, or bytes that are not UTF-8. The
    result is the identifier tokens joined by ``.``, exactly what ``target_name`` must be.
    """
    if not 0 <= start < end <= len(content) or end - start > MAX_NAME_BYTES:
        return None
    if _DOTTED.fullmatch(content, start, end) is None:
        return None
    if start > 0 and _TOKEN_BYTE.fullmatch(content, start - 1, start):
        return None
    if _TOKEN_BYTE.fullmatch(content, end, end + 1):
        return None
    try:
        return _WHITESPACE.sub(b"", content[start:end]).decode("utf-8")
    except UnicodeDecodeError:
        return None


def diagnostics(**counts: int) -> tuple[ParsedDiagnostic, ...]:
    """Closed-enum diagnostics in a fixed order; zero counts are omitted, counts saturate."""
    return tuple(
        ParsedDiagnostic.model_validate(
            {"code": code, "count": min(counts[code], MAX_DIAGNOSTIC_COUNT)}
        )
        for code in DIAGNOSTIC_CODES
        if counts.get(code, 0) > 0
    )


def degraded_file(
    source: SourceFile, fingerprint: str, reason: DiagnosticCode, count: int = 1
) -> ParsedFile:
    """A file the adapter could not answer: no structure, ``file_degraded`` plus its reason."""
    return ParsedFile(
        path=source.path,
        language=source.language,
        parser_fingerprint=fingerprint,
        symbols=(),
        diagnostics=diagnostics(file_degraded=1, **{reason: count}),
    )


class WorkBudgetExceeded(Exception):
    """A file needs more parsing work than the adapter budget allows (content-free)."""


class Budget:
    """What one request may cost: name bytes, output size, syntax nodes and CPU time.

    Work limits (design numbers, see ``python.py``): ``MAX_NODES_PER_FILE`` bounds one file
    deterministically; ``CPU_SOFT_LIMIT`` (5 s from the start of the request) is a backstop on
    CPU time, checked before each file's parse, inside the parse (a read callback that
    reports end of input once spent, 4 KiB chunks; measured on 3.12 with 1 MiB of hostile
    ``def (`` lines the parse ends within ~0.1 s of the deadline) and every 2048 walked nodes. Worst case under the runner's ``RLIMIT_CPU`` of
    10 s: interpreter and grammar start-up (~0.7 s, before the clock starts) + 5 s backstop +
    one check granularity (<0.1 s) + parent-side validation and JSON dump (~1.5 s for a 8 MiB
    output) = under 7.5 s. A file over budget degrades to no symbols; the rest continues.
    """

    MAX_NODES_PER_FILE: Final = 600_000
    CPU_SOFT_LIMIT: Final = 5.0

    def __init__(self, name_limit: int = MAX_NAME_TOTAL_BYTES // 2) -> None:
        self._name_limit = name_limit
        self._names = 0
        self._reference_names = 0
        self._nodes = 0
        self.max_nodes = self.MAX_NODES_PER_FILE
        self._cpu_deadline = time.process_time() + self.CPU_SOFT_LIMIT  # from this request

    def new_file(self) -> None:
        self._names = 0
        self._reference_names = 0
        self._nodes = 0

    def out_of_time(self) -> bool:
        return time.process_time() > self._cpu_deadline

    def check(self) -> None:
        """Raise when the request's CPU backstop is already spent."""
        if self.out_of_time():
            raise WorkBudgetExceeded

    def node(self) -> None:
        """Count one syntax node; raise when the file (or the process CPU) is over budget."""
        self._nodes += 1
        if self._nodes > self.max_nodes:
            raise WorkBudgetExceeded
        if self._nodes % 2048 == 0 and self.out_of_time():
            raise WorkBudgetExceeded

    @staticmethod
    def _cost(qualified_name: str, signature: str) -> tuple[int, int]:
        size = len(qualified_name.encode())
        return size, size + len(signature.encode()) + _SYMBOL_OVERHEAD

    def take(self, qualified_name: str, signature: str, output: list[int]) -> bool:
        size, cost = self._cost(qualified_name, signature)
        if size > MAX_NAME_BYTES or self._names + size > self._name_limit:
            return False
        if output[0] + cost > OUTPUT_BUDGET:
            return False
        self._names += size
        output[0] += cost
        return True

    def take_reference(self, names: int, output: list[int]) -> bool:
        """Charge one reference (``names`` = target + qualifier bytes); False when it will not fit.

        The parent charges symbol names and reference names to ONE ``MAX_NAME_TOTAL_BYTES``
        counter, so references get what the live symbols left, not a fixed half.
        """
        cost = names + _REFERENCE_OVERHEAD
        if self._names + self._reference_names + names > MAX_NAME_TOTAL_BYTES:
            return False
        if output[0] + cost > OUTPUT_BUDGET:
            return False
        self._reference_names += names
        output[0] += cost
        return True

    def release(self, qualified_name: str, signature: str, output: list[int]) -> None:
        """Give back what ``take`` charged (a symbol dropped after the fact)."""
        size, cost = self._cost(qualified_name, signature)
        self._names -= size
        output[0] -= cost


def bounded_parse(parser: Parser, content: bytes, budget: Budget, chunk: int = READ_CHUNK) -> Tree:
    """Parse under the CPU backstop: checked before the parse and on every chunk it reads.

    py-tree-sitter's ``progress_callback`` is not used: it crashes the interpreter on the
    cp312 and cp313 wheels (0.25.2 and 0.26.0). Instead the parser reads the source through a
    read callback in small chunks; once the backstop is spent the callback reports end of
    input (and keeps doing so), the parse unwinds, and ``WorkBudgetExceeded`` is raised.
    Error recovery is superlinear on hostile input, so the bound has to act inside the parse.
    """
    budget.check()
    stopped = False

    def read(offset: int, _point: object) -> bytes:
        nonlocal stopped
        if not stopped and budget.out_of_time():
            stopped = True
        return b"" if stopped else content[offset : offset + chunk]

    tree = parser.parse(read, encoding="utf8")
    if stopped:
        raise WorkBudgetExceeded
    return tree


def safe_module(
    request: ParseRequest,
    fingerprint: str,
    parse_file: Callable[[SourceFile, Budget, list[int]], ParsedFile],
) -> ParsedModule:
    """Parse every file; a file whose answer would be refused degrades to *no structure*.

    Passing ``validate_module`` per file implies passing it for the batch, so one
    pathological file cannot take down the others. Only the validation errors
    (``ValidationError``, ``StructuralError``) and ``WorkBudgetExceeded`` are caught, so an
    adapter bug (any other exception) still fails loudly. ``references`` and ``diagnostics``
    of a valid file pass through untouched. A degraded file is never silent: it carries
    ``file_degraded`` plus its reason, ``work_budget_exceeded`` (node budget or CPU backstop) or
    ``symbols_dropped`` (the answer was refused, or did not fit the output bound). Only when the
    whole answer would exceed the output bound are references shed first, never silently:
    ``references_capped`` says so, then relations, then the symbols of that file alone.
    """
    budget = Budget()
    output = [0]
    total = 0
    files: list[ParsedFile] = []
    for source in request.files:
        budget.new_file()
        mark = output[0]
        refused = degraded_file(source, fingerprint, "symbols_dropped")
        try:
            parsed = parse_file(source, budget, output)
            # A file with no structure has nothing the confinement check could refuse, and
            # re-validating a degraded 1 MB file costs ~0.7 s of the CPU budget each. The
            # parent validates the whole answer again regardless.
            if parsed.symbols or parsed.relations or parsed.references:
                validate_module(
                    ParseRequest(files=(source,)),
                    ParsedModule(files=(parsed,)),
                    expected_fingerprint=fingerprint,
                )
        except WorkBudgetExceeded:
            output[0] = mark
            parsed = degraded_file(source, fingerprint, "work_budget_exceeded")
        except (ValidationError, StructuralError):
            output[0] = mark
            parsed = refused
        # The runner refuses the whole answer above MAX_OUTPUT_BYTES: measure the real JSON,
        # then shed references (reported), then relations, then symbols, of this file only.
        for reduced in _reductions(parsed, refused):
            size = len(reduced.model_dump_json())
            if total + size <= OUTPUT_BUDGET:
                break
        total += size
        files.append(reduced)
    return ParsedModule(files=tuple(files))


def _reductions(parsed: ParsedFile, refused: ParsedFile) -> Iterator[ParsedFile]:
    """The file as is, then without references (reported), without relations, then degraded."""
    yield parsed
    shed = parsed
    if parsed.references:
        shed = _shed(parsed, "references", len(parsed.references))
        yield shed
    if shed.relations:
        # The contract has no relations code: shed relations are reported as ``references_capped``,
        # the closest one, added to whatever the adapter already reported.
        yield _shed(shed, "relations", len(shed.relations))
    yield refused


def _shed(parsed: ParsedFile, field: str, dropped: int) -> ParsedFile:
    """``parsed`` without ``field``, with the loss added to ``references_capped``."""
    counts = {item.code: item.count for item in parsed.diagnostics}
    counts["references_capped"] = counts.get("references_capped", 0) + dropped
    return parsed.model_copy(update={field: (), "diagnostics": diagnostics(**counts)})


def _reference_view(item: ParsedReference, by_ref: dict[str, str]) -> dict[str, Any]:
    view = _reference_fields(item, by_ref)
    if item.alias is not None:  # absent when unset, so aliasless goldens stay as they are
        view["alias"] = item.alias
        view["alias_range"] = [item.alias_start_byte, item.alias_end_byte]
    return view


def _reference_fields(item: ParsedReference, by_ref: dict[str, str]) -> dict[str, Any]:
    return {
        "source": None if item.source is None else by_ref[item.source],
        "kind": item.kind,
        "target": item.target_name,
        "relative_level": item.relative_level,
        "range": [item.start_byte, item.end_byte],
        "qualifier": item.qualifier,
        "qualifier_range": (
            None if item.qualifier is None else [item.qualifier_start_byte, item.qualifier_end_byte]
        ),
        "evidence_kind": item.evidence_kind,
        "confidence": item.confidence,
    }


def normalize_module(module: ParsedModule) -> dict[str, Any]:
    """Stable, human-reviewable view of a (validated) module for golden tests.

    Refs are replaced by ``qualified_name#kind#disambiguator`` keys so a diff reads as code.
    """
    files: list[dict[str, Any]] = []
    for parsed in module.files:
        keys = [
            f"{item.qualified_name}#{item.kind}#{item.disambiguator}" for item in parsed.symbols
        ]
        by_ref = {item.ref: key for item, key in zip(parsed.symbols, keys, strict=True)}
        files.append(
            {
                "path": parsed.path,
                "language": parsed.language,
                "parser_fingerprint": parsed.parser_fingerprint,
                "symbols": [
                    {
                        "key": key,
                        "range": [item.start_byte, item.end_byte],
                        "signature": item.signature,
                        "signature_digest": item.signature_digest,
                        "semantic_fingerprint": item.semantic_fingerprint,
                        "evidence_kind": item.evidence_kind,
                    }
                    for item, key in zip(parsed.symbols, keys, strict=True)
                ],
                "relations": [
                    {
                        "source": by_ref[rel.source_ref],
                        "target": by_ref[rel.target_ref],
                        "kind": rel.kind,
                        "range": [rel.start_byte, rel.end_byte],
                        "evidence_kind": rel.evidence_kind,
                    }
                    for rel in parsed.relations
                ],
                # Sorted, so the golden and parity tests catch a missing, reordered or
                # corrupted entry regardless of the order the adapter walked the tree.
                "references": sorted(
                    (_reference_view(item, by_ref) for item in parsed.references),
                    key=lambda view: json.dumps(view, sort_keys=True),
                ),
                "diagnostics": sorted(
                    ({"code": item.code, "count": item.count} for item in parsed.diagnostics),
                    key=lambda view: view["code"],
                ),
            }
        )
    return {"protocol_version": module.protocol_version, "files": files}
