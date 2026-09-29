"""Structural adapter protocol, normalized values and output validation.

An adapter turns source bytes into ``ParsedFile`` values. Adapters are untrusted:
they parse attacker-controlled repository content with native parsers. So the
platform never trusts their output; ``validate_module`` is the single gate every
``ParsedModule`` passes before anything downstream sees it (see ``runner.py`` for
the process sandbox).

Wire protocol: one bounded JSON document each way (``ParseRequest`` on stdin,
``ParsedModule`` on stdout). Both sides validate with pydantic models that are
frozen, strict and ``extra="forbid"``, with ``hide_input_in_errors`` so that a
validation failure never echoes source text into logs.

Trust rules enforced by ``validate_module`` (fail closed, typed content-free
errors, never the offending value):

- exactly the input files come back, once each, under the same path and language;
- byte ranges lie inside the *actual* input bytes (never a size the adapter reports);
- the parser fingerprint equals the one the host computed;
- symbol keys ``(qualified_name, kind, disambiguator)`` and refs are unique per file
  so identity derivation (``identity.py``) cannot collide;
- relation endpoints are refs of that file's symbols;
- every assertion says ``evidence_kind="tree_sitter"`` (design 9.3), no default;
- counts are bounded;
- every free-text field is confined to the parsed file itself, so an adapter that read
  some other file (``/etc/passwd``, a ``.env``) cannot exfiltrate it through the output.
  Read isolation is not something the container gives (the adapter shares the indexer's
  filesystem view); this is the confinement:

  * ``qualified_name`` is a path of identifier tokens split on ``.``, ``::``, ``/``,
    ``#``. The final segment must be an identifier token located inside the symbol's own
    ``[start_byte, end_byte]`` (for ``kind="module"`` a component of the file path also
    qualifies). Every other segment is bound structurally, with no length rule: a
    component of a path of the request, the exact final segment of a symbol emitted in
    the same module output (enclosing classes, namespaces, packages), or an identifier
    token inside this symbol's own range (Go receivers). Names that are not made of
    identifiers (``<lambda>``) are refused. All names of a file together are bounded
    (``MAX_NAME_TOTAL_BYTES``);
  * ``signature`` (whitespace runs collapsed) is a contiguous run of the
    whitespace-collapsed text inside the symbol's own byte range;
  * ``ref`` and ``disambiguator`` never come from the adapter: ``ref`` is rewritten to the
    symbol's index in the file (relations are remapped, at most
    ``MAX_RELATIONS_PER_SYMBOL`` per source), and ``disambiguator`` is derived as the
    0..n-1 ordinal among symbols with the same (qualified name, kind), ordered by
    ``(start_byte, end_byte, kind)``. The adapter's values (numeric, at most 6 digits) are
    ignored beyond uniqueness of refs. ``kind`` and the relation ``kind`` are closed
    enums; ``language`` must equal the input's.

  * opaque values (``OPAQUE_FIELDS``: ``signature_digest``, ``semantic_fingerprint``) are
    hashes the parser makes up, so nothing ties them to the file and 32 bytes per symbol
    could carry a secret. ``validate_module`` therefore never returns the child's value: it
    stores ``sha256(domain_tag || field_name || child_value)`` computed in the parent
    (``rehash_opaque``). Identity stays deterministic (the stored value changes if and only
    if the child's does) and one-way (a secret cannot be read back out of it).
    ``FIELD_CONFINEMENT`` classifies every output field; a test fails when a new field
    is not classified.

  Residual channel (inherent to structural output): an adapter can still choose *which*
  symbols, relations and ordering to emit, about one bit per candidate token or edge, at most about ``log2(tokens in file)`` bits per symbol and only ever about
  this file's own content. That leaks nothing beyond the file it was asked to parse.

Adapter output carries no source text beyond qualified names, ``kind``s,
``disambiguator``s and signatures. A signature is bounded to
``MAX_SIGNATURE_BYTES`` UTF-8 bytes and rejected, not truncated, when longer: it
can embed default-value literals, so the bound limits how much source can leak or
be smuggled through it; adapters cut it themselves and compute
``signature_digest`` over the full signature (identity keeps distinguishing
signatures that only differ after the cut).
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from array import array
from bisect import bisect_left
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, Any, Final, Literal, Protocol, Self, runtime_checkable

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from agent_context_platform.indexing.identity import canonical_path

PROTOCOL_VERSION: Final = 1
EVIDENCE_KIND: Final = "tree_sitter"

# Same default as scanner.ScanLimits.max_file_bytes: the scanner never yields larger files.
MAX_SOURCE_BYTES: Final = 1_048_576
MAX_INPUT_FILES: Final = 64
# Wire size of a request: base64 inflates 4/3, plus JSON framing.
MAX_REQUEST_BYTES: Final = 8 * 1024 * 1024
MAX_OUTPUT_BYTES: Final = 8 * 1024 * 1024
MAX_SYMBOLS_PER_FILE: Final = 10_000
MAX_RELATIONS_PER_FILE: Final = 50_000
MAX_NAME_BYTES: Final = 512
MAX_SIGNATURE_BYTES: Final = 512
MAX_NAME_TOTAL_BYTES: Final = 256 * 1024

_DIGEST = r"^[0-9a-f]{64}$"
_DISAMBIGUATOR = r"^[0-9]{0,6}$"
_REF = r"^[0-9]{1,6}$"
MAX_RELATIONS_PER_SYMBOL: Final = 64


class StructuralErrorCode(StrEnum):
    """Why an adapter run was refused. Codes only: never content, paths or values."""

    INVALID_REQUEST = "invalid_request"
    INPUT_TOO_LARGE = "input_too_large"
    SPAWN_FAILED = "spawn_failed"
    TIMEOUT = "timeout"
    OUTPUT_TOO_LARGE = "output_too_large"
    STDERR_TOO_LARGE = "stderr_too_large"
    NONZERO_EXIT = "nonzero_exit"
    MALFORMED_OUTPUT = "malformed_output"
    SCHEMA_VIOLATION = "schema_violation"
    PATH_MISMATCH = "path_mismatch"
    LANGUAGE_MISMATCH = "language_mismatch"
    FINGERPRINT_MISMATCH = "fingerprint_mismatch"
    RANGE_OUT_OF_BOUNDS = "range_out_of_bounds"
    DANGLING_RELATION = "dangling_relation"
    DUPLICATE_SYMBOL = "duplicate_symbol"
    COUNT_EXCEEDED = "count_exceeded"
    TEXT_NOT_IN_SOURCE = "text_not_in_source"


class StructuralError(Exception):
    """Typed, content-free failure; the message is the code and nothing else."""

    def __init__(self, code: StructuralErrorCode) -> None:
        super().__init__(code.value)
        self.code = code


def _no_controls(value: str) -> str:
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("control characters are not allowed")
    return value


def _bounded(value: str, limit: int) -> str:
    if len(value.encode("utf-8", errors="strict")) > limit:
        raise ValueError("text exceeds its byte bound")
    return value


def _safe_path(value: str) -> str:
    return canonical_path(value)


_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)

type Digest = Annotated[str, StringConstraints(pattern=_DIGEST)]
type SymbolKind = Literal[
    "function",
    "method",
    "class",
    "interface",
    "type",
    "variable",
    "constant",
    "module",
    "field",
    "property",
]
type RelationKind = Literal["calls", "imports", "inherits", "references", "contains"]
type Ref = Annotated[str, StringConstraints(pattern=_REF)]
type Language = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_+\-]{0,31}$")]
type RepoPath = Annotated[
    str, StringConstraints(min_length=1, max_length=4096), AfterValidator(_safe_path)
]
type Text = Annotated[str, AfterValidator(_no_controls)]


class SourceFile(BaseModel):
    """One input file: repo-relative path, language and the exact bytes to parse."""

    model_config = _MODEL

    path: RepoPath
    language: Language
    content_b64: str

    @classmethod
    def from_bytes(cls, path: str, language: str, content: bytes) -> Self:
        return cls(path=path, language=language, content_b64=base64.b64encode(content).decode())

    def content(self) -> bytes:
        return base64.b64decode(self.content_b64, validate=True)

    @model_validator(mode="after")
    def _bounded_content(self) -> Self:
        # Base64 of MAX_SOURCE_BYTES, checked before decoding anything.
        if len(self.content_b64) > (MAX_SOURCE_BYTES + 2) // 3 * 4:
            raise ValueError("source exceeds its byte bound")
        try:
            decoded = self.content()
        except ValueError:
            raise ValueError("content is not base64") from None
        if len(decoded) > MAX_SOURCE_BYTES:
            raise ValueError("source exceeds its byte bound")
        return self


class ParseRequest(BaseModel):
    """Bounded input message: the only thing an adapter receives."""

    model_config = _MODEL

    protocol_version: Literal[1] = PROTOCOL_VERSION
    files: tuple[SourceFile, ...] = Field(min_length=1, max_length=MAX_INPUT_FILES)

    @model_validator(mode="after")
    def _unique_paths(self) -> Self:
        if len({item.path for item in self.files}) != len(self.files):
            raise ValueError("duplicate path")
        return self


class ParsedSymbol(BaseModel):
    """A declaration with the arguments ``identity.symbol_*`` needs plus its byte range."""

    model_config = _MODEL

    ref: Ref
    language: Language
    qualified_name: Annotated[
        Text, StringConstraints(min_length=1), AfterValidator(lambda v: _bounded(v, MAX_NAME_BYTES))
    ]
    kind: SymbolKind
    disambiguator: Annotated[str, StringConstraints(pattern=_DISAMBIGUATOR)] = ""
    start_byte: Annotated[int, Field(ge=0)]
    end_byte: Annotated[int, Field(ge=0)]
    signature: Annotated[Text, AfterValidator(lambda v: _bounded(v, MAX_SIGNATURE_BYTES))]
    signature_digest: Digest
    semantic_fingerprint: Digest
    evidence_kind: Literal["tree_sitter"]

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.start_byte >= self.end_byte:
            raise ValueError("empty or inverted byte range")
        return self


class StructuralRelation(BaseModel):
    """A directed edge between two symbols of the same file, with the site's byte range."""

    model_config = _MODEL

    source_ref: Ref
    target_ref: Ref
    kind: RelationKind
    start_byte: Annotated[int, Field(ge=0)]
    end_byte: Annotated[int, Field(ge=0)]
    evidence_kind: Literal["tree_sitter"]

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.start_byte >= self.end_byte:
            raise ValueError("empty or inverted byte range")
        return self


class ParsedFile(BaseModel):
    """Normalized structure of one input file, tagged with the parser fingerprint."""

    model_config = _MODEL

    path: RepoPath
    language: Language
    parser_fingerprint: Digest
    symbols: tuple[ParsedSymbol, ...]
    relations: tuple[StructuralRelation, ...] = ()


class ParsedModule(BaseModel):
    """Whole adapter answer: one ``ParsedFile`` per input file."""

    model_config = _MODEL

    protocol_version: Literal[1] = PROTOCOL_VERSION
    files: tuple[ParsedFile, ...]


@runtime_checkable
class StructuralAdapter(Protocol):
    """Turns a request into a validated module; raises ``StructuralError`` on refusal."""

    @property
    def language(self) -> str: ...

    def parse(self, request: ParseRequest) -> ParsedModule: ...


# How every field of the adapter output is kept from carrying foreign data. A test requires
# every model field to be listed here, so a new field cannot slip in unclassified.
type Confinement = Literal[
    "token", "numeric", "enum", "range", "parent", "derived", "rehashed", "structure"
]
FIELD_CONFINEMENT: Final[Mapping[str, Mapping[str, Confinement]]] = {
    "ParsedModule": {"protocol_version": "enum", "files": "structure"},
    "ParsedFile": {
        "path": "parent",  # must equal an input path
        "language": "parent",  # must equal the input language
        "parser_fingerprint": "parent",  # must equal the host-computed fingerprint
        "symbols": "structure",
        "relations": "structure",
    },
    "ParsedSymbol": {
        "ref": "derived",
        "language": "parent",
        "qualified_name": "token",
        "kind": "enum",
        "disambiguator": "derived",  # parent-derived ordinal
        "start_byte": "range",
        "end_byte": "range",
        "signature": "token",  # a contiguous run of the symbol's own source text
        "signature_digest": "rehashed",
        "semantic_fingerprint": "rehashed",
        "evidence_kind": "enum",
    },
    "StructuralRelation": {
        "source_ref": "derived",  # remapped to a symbol index
        "target_ref": "derived",
        "kind": "enum",
        "start_byte": "range",
        "end_byte": "range",
        "evidence_kind": "enum",
    },
}
OPAQUE_FIELDS: Final = ("signature_digest", "semantic_fingerprint")
_REHASH_DOMAIN: Final = b"agent-context/tree-sitter/opaque/v1"


def rehash_opaque(field_name: str, child_value: str) -> str:
    """One-way, parent-side replacement of an opaque adapter value."""
    material = b"\0".join((_REHASH_DOMAIN, field_name.encode(), child_value.encode()))
    return hashlib.sha256(material).hexdigest()


def parser_fingerprint(name: str, version: str, config: Mapping[str, Any] | None = None) -> str:
    """Canonical SHA-256 of parser name, version and config (input of ``file_revision_id``)."""
    document = {"name": name, "version": version, "config": dict(config or {})}
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def validate_module(
    request: ParseRequest, module: ParsedModule, *, expected_fingerprint: str
) -> ParsedModule:
    """Check ``module`` against ``request``; return it with opaque values re-hashed, or raise."""
    sources = {item.path: item for item in request.files}
    seen: set[str] = set()
    texts: dict[str, _SourceText] = {}
    for parsed in module.files:
        source = sources.get(parsed.path)
        if source is None or parsed.path in seen:
            raise StructuralError(StructuralErrorCode.PATH_MISMATCH)
        seen.add(parsed.path)
        if parsed.language != source.language:
            raise StructuralError(StructuralErrorCode.LANGUAGE_MISMATCH)
        if parsed.parser_fingerprint != expected_fingerprint:
            raise StructuralError(StructuralErrorCode.FINGERPRINT_MISMATCH)
        texts[parsed.path] = _validate_file(parsed, source.content())
    if seen != set(sources):
        raise StructuralError(StructuralErrorCode.PATH_MISMATCH)
    # Non-final name segments may name a symbol of any file of this module output.
    finals = {
        _SEPARATORS.split(item.qualified_name)[-1].encode()
        for parsed in module.files
        for item in parsed.symbols
    }
    paths = {part for path in sources for part in _path_parts(path)}
    return module.model_copy(
        update={
            "files": tuple(
                _finish(parsed, texts[parsed.path], finals, paths) for parsed in module.files
            )
        }
    )


def _finish(
    parsed: ParsedFile, text: _SourceText, finals: set[bytes], paths: set[bytes]
) -> ParsedFile:
    """Bind non-final name segments, then rebuild with parent-derived and re-hashed values."""
    for item in parsed.symbols:
        if not text.prefix_ok(item.qualified_name, item.start_byte, item.end_byte, finals, paths):
            raise StructuralError(StructuralErrorCode.TEXT_NOT_IN_SOURCE)
    index = {item.ref: number for number, item in enumerate(parsed.symbols)}
    order = sorted(
        range(len(parsed.symbols)),
        key=lambda n: (
            parsed.symbols[n].start_byte,
            parsed.symbols[n].end_byte,
            parsed.symbols[n].kind,
            n,
        ),
    )
    ordinals: dict[tuple[str, str], int] = {}
    disambiguators = [""] * len(parsed.symbols)
    for number in order:
        name = (parsed.symbols[number].qualified_name, parsed.symbols[number].kind)
        disambiguators[number] = str(ordinals.get(name, 0))
        ordinals[name] = ordinals.get(name, 0) + 1
    symbols = tuple(
        item.model_copy(
            update={
                "ref": str(number),
                "disambiguator": disambiguators[number],
                **{name: rehash_opaque(name, getattr(item, name)) for name in OPAQUE_FIELDS},
            }
        )
        for number, item in enumerate(parsed.symbols)
    )
    relations = tuple(
        item.model_copy(
            update={
                "source_ref": str(index[item.source_ref]),
                "target_ref": str(index[item.target_ref]),
            }
        )
        for item in parsed.relations
    )
    return parsed.model_copy(update={"symbols": symbols, "relations": relations})


_SEPARATORS = re.compile(r"::|[./#]")
_IDENT = re.compile(rb"[A-Za-z_$\x80-\xff][A-Za-z0-9_$\x80-\xff]*")
_WHITESPACE = re.compile(rb"[ \t\r\n\f\v]+")


def _path_parts(path: str) -> set[bytes]:
    return {part.encode() for piece in path.split("/") for part in (piece, *piece.split("."))}


class _SourceText:
    """Confinement oracle for one file: what names and signatures may legitimately say."""

    def __init__(self, path: str, content: bytes) -> None:
        self._tokens: dict[bytes, list[int]] = {}
        for match in _IDENT.finditer(content):
            self._tokens.setdefault(match.group(), []).append(match.start())
        self._path_parts = _path_parts(path)
        self.name_bytes = 0
        # Whitespace-collapsed text, with the original offset of every collapsed byte.
        pieces: list[bytes] = []
        offsets = array("I")
        last = 0
        for match in _WHITESPACE.finditer(content):
            self._append(pieces, offsets, content[last : match.start()], last)
            self._append(pieces, offsets, b" ", match.start())
            last = match.end()
        self._append(pieces, offsets, content[last:], last)
        self._collapsed = b"".join(pieces)
        self._offsets = offsets

    @staticmethod
    def _append(pieces: list[bytes], offsets: array[int], chunk: bytes, origin: int) -> None:
        pieces.append(chunk)
        offsets.extend(range(origin, origin + len(chunk)))

    def _token_within(self, token: bytes, start: int, end: int) -> bool:
        starts = self._tokens.get(token, [])
        index = bisect_left(starts, start)
        return index < len(starts) and starts[index] + len(token) <= end

    def final_ok(self, qualified_name: str, kind: str, start: int, end: int) -> bool:
        """Every segment is an identifier and the final one is a token of the symbol's range."""
        self.name_bytes += len(qualified_name.encode())
        if self.name_bytes > MAX_NAME_TOTAL_BYTES:
            raise StructuralError(StructuralErrorCode.COUNT_EXCEEDED)
        segments = [segment.encode() for segment in _SEPARATORS.split(qualified_name)]
        if not all(_IDENT.fullmatch(raw) for raw in segments):
            return False
        last = segments[-1]
        return self._token_within(last, start, end) or (
            kind == "module" and last in self._path_parts
        )

    def prefix_ok(
        self, qualified_name: str, start: int, end: int, finals: set[bytes], paths: set[bytes]
    ) -> bool:
        """Non-final segments: a path component, another symbol's name, or a token in range."""
        segments = [segment.encode() for segment in _SEPARATORS.split(qualified_name)]
        return all(
            raw in paths or raw in finals or self._token_within(raw, start, end)
            for raw in segments[:-1]
        )

    def signature_ok(self, signature: str, start: int, end: int) -> bool:
        needle = _WHITESPACE.sub(b" ", signature.encode()).strip()
        if not needle:
            return True
        low = bisect_left(self._offsets, start)
        high = bisect_left(self._offsets, end)
        return self._collapsed.find(needle, low, high) != -1


def _validate_file(parsed: ParsedFile, content: bytes) -> _SourceText:
    size = len(content)
    if len(parsed.symbols) > MAX_SYMBOLS_PER_FILE or len(parsed.relations) > MAX_RELATIONS_PER_FILE:
        raise StructuralError(StructuralErrorCode.COUNT_EXCEEDED)
    text = _SourceText(parsed.path, content)
    refs: set[str] = set()
    for symbol in parsed.symbols:
        if symbol.language != parsed.language:
            raise StructuralError(StructuralErrorCode.LANGUAGE_MISMATCH)
        if symbol.end_byte > size:
            raise StructuralError(StructuralErrorCode.RANGE_OUT_OF_BOUNDS)
        if not text.final_ok(
            symbol.qualified_name, symbol.kind, symbol.start_byte, symbol.end_byte
        ) or not text.signature_ok(symbol.signature, symbol.start_byte, symbol.end_byte):
            raise StructuralError(StructuralErrorCode.TEXT_NOT_IN_SOURCE)
        if symbol.ref in refs:
            raise StructuralError(StructuralErrorCode.DUPLICATE_SYMBOL)
        refs.add(symbol.ref)
    per_source: dict[str, int] = {}
    for relation in parsed.relations:
        per_source[relation.source_ref] = per_source.get(relation.source_ref, 0) + 1
        if per_source[relation.source_ref] > MAX_RELATIONS_PER_SYMBOL:
            raise StructuralError(StructuralErrorCode.COUNT_EXCEEDED)
        if relation.end_byte > size:
            raise StructuralError(StructuralErrorCode.RANGE_OUT_OF_BOUNDS)
        if relation.source_ref not in refs or relation.target_ref not in refs:
            raise StructuralError(StructuralErrorCode.DANGLING_RELATION)
    return text
