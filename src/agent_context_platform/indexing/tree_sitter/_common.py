"""Helpers shared by the syntax-only language adapters (Python now; TypeScript/Go later).

Everything here is adapter-side and untrusted-input-safe: the parent re-checks the
output with ``base.validate_module``. The rules below exist so that real (and hostile)
source never makes the *parent* refuse a whole batch of up to 64 files: an adapter skips
what it cannot express instead of emitting something the confinement rules would reject.

Reused by PLATFORM-034/035: ``frame``/``digest`` (normalized-token fingerprints),
``cut_signature``, ``identifier_bytes``, ``module_name``, ``Budget`` (name/output/work
limits), ``safe_module`` (per-file degradation) and ``normalize_module`` (golden-test view).
"""

from __future__ import annotations

import hashlib
import re
import struct
import time
from collections.abc import Callable, Iterable
from typing import Any, Final

from pydantic import ValidationError

from agent_context_platform.indexing.tree_sitter.base import (
    MAX_NAME_BYTES,
    MAX_NAME_TOTAL_BYTES,
    MAX_OUTPUT_BYTES,
    ParsedFile,
    ParsedModule,
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
_IDENT_TEXT: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

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
    ``2024`` or ``.github``, end the run). Empty string when no name can be formed.
    """
    parts = path.split("/")
    stem = parts[-1].rsplit(".", 1)[0] if "." in parts[-1] else parts[-1]
    parts = [*parts[:-1], stem]
    if stem in package_files:
        parts.pop()
    run: list[str] = []
    for part in reversed(parts):
        if not _IDENT_TEXT.fullmatch(part):
            break
        run.append(part)
    return ".".join(reversed(run))


class WorkBudgetExceeded(Exception):
    """A file needs more parsing work than the adapter budget allows (content-free)."""


class Budget:
    """What one request may cost: name bytes, output size, syntax nodes and CPU time.

    Work limits (design numbers, see ``python.py``): ``MAX_NODES_PER_FILE`` bounds one file
    deterministically; ``CPU_SOFT_LIMIT`` (5 s from the start of the request) is a backstop on CPU time, kept well
    under the runner's ``RLIMIT_CPU`` (10 s) so validation and serialisation still fit. A
    file over budget degrades to no symbols; the rest of the batch continues.
    """

    MAX_NODES_PER_FILE: Final = 600_000
    CPU_SOFT_LIMIT: Final = 5.0

    def __init__(self, name_limit: int = MAX_NAME_TOTAL_BYTES // 2) -> None:
        self._name_limit = name_limit
        self._names = 0
        self._nodes = 0
        self.max_nodes = self.MAX_NODES_PER_FILE
        self._cpu_deadline = time.process_time() + self.CPU_SOFT_LIMIT  # from this request

    def new_file(self) -> None:
        self._names = 0
        self._nodes = 0

    def node(self) -> None:
        """Count one syntax node; raise when the file (or the process CPU) is over budget."""
        self._nodes += 1
        if self._nodes > self.max_nodes:
            raise WorkBudgetExceeded
        if self._nodes % 2048 == 0 and time.process_time() > self._cpu_deadline:
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

    def release(self, qualified_name: str, signature: str, output: list[int]) -> None:
        """Give back what ``take`` charged (a symbol dropped after the fact)."""
        size, cost = self._cost(qualified_name, signature)
        self._names -= size
        output[0] -= cost


def safe_module(
    request: ParseRequest,
    fingerprint: str,
    parse_file: Callable[[SourceFile, Budget, list[int]], ParsedFile],
) -> ParsedModule:
    """Parse every file; a file whose answer would be refused degrades to *no symbols*.

    Passing ``validate_module`` per file implies passing it for the batch, so one
    pathological file cannot take down the others. Only the validation errors
    (``ValidationError``, ``StructuralError``) and ``WorkBudgetExceeded`` are caught, so an
    adapter bug (any other exception) still fails loudly. The wire contract has no
    diagnostics channel: a degraded file is indistinguishable from an empty one (P032C).
    """
    budget = Budget()
    output = [0]
    total = 0
    files: list[ParsedFile] = []
    for source in request.files:
        budget.new_file()
        mark = output[0]
        empty = ParsedFile(
            path=source.path,
            language=source.language,
            parser_fingerprint=fingerprint,
            symbols=(),
        )
        try:
            parsed = parse_file(source, budget, output)
            validate_module(
                ParseRequest(files=(source,)),
                ParsedModule(files=(parsed,)),
                expected_fingerprint=fingerprint,
            )
        except (ValidationError, StructuralError, WorkBudgetExceeded):
            output[0] = mark
            parsed = empty
        # The runner refuses the whole answer above MAX_OUTPUT_BYTES: measure the real JSON,
        # then shed relations, then symbols, of this file only.
        for reduced in (parsed, parsed.model_copy(update={"relations": ()}), empty):
            size = len(reduced.model_dump_json())
            if total + size <= OUTPUT_BUDGET:
                break
        total += size
        files.append(reduced)
    return ParsedModule(files=tuple(files))


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
            }
        )
    return {"protocol_version": module.protocol_version, "files": files}
