"""Canonical code-index events from a scan, SCIP evidence and structural parses (design 9, 10).

``IndexingService.index(scan, semantic, structural)`` turns the evidence of one immutable target
(a commit, or a dirty workspace snapshot) into ``EventDraftV1`` values for the SDK ``code.*``
contracts. ``IndexingService.ingest`` also submits them to the in-process ``IngestionService``.

Producer and stream
-------------------
The indexer is its own producer: ``producer_id`` is ``INDEXER_PRODUCER_ID``. The in-process
ingestion path takes an already-authenticated batch and the ledger has no foreign key from
events to ``operations.registered_producers``, so no registration is needed here; the HTTP
plane binds a bearer to a producer registration, which an in-process indexer never crosses.
Events go to one stream per repository, ``code-index:<repository_id>``.

Identity and revision (MANIFEST decisions)
------------------------------------------
- The logical symbol identity prefers the global SCIP symbol string and falls back to the
  tree-sitter qualified name (``identity.symbol_logical_id``). A tree-sitter symbol adopts a SCIP
  symbol when a SCIP definition occurrence, converted from line/character to bytes with the
  document's position encoding, lies inside the symbol's byte range and spells its name.
- The ``SymbolRevision`` content identity comes from ONE source: the tree-sitter signature and
  body fingerprints. The SCIP-derived digests are used only for a file with no structural parse.
  SCIP contributes relations and evidence, never a competing revision ID.
- ``file_logical_ids`` (the introducing-commit lineage of ``identity``) is an input: nothing here
  runs git on the untrusted checkout. A tracked, clean file without a lineage is skipped and
  counted; an untracked or dirty-only file gets a provisional ID from the repository and path alone
  (``identity.uncommitted_file_logical_id``), so every checkout agrees on it.
- SCIP evidence binds a file only when the file's bytes are the SCIP commit's, decided by CONTENT
  and never by porcelain status alone: a tracked file with no change against HEAD whose observed
  blob equals the commit's (a path in ``modified_paths`` with no status, e.g. an EOL or smudge
  filter, is not at the commit). For any other file ALL its SCIP evidence is dropped and counted
  (``scip_file_not_at_commit``); tree-sitter evidence stands alone. An occurrence without a valid
  location makes no claim at all, and a column is at most the line's content length (a line
  terminator is never part of it). Every ``import_scip`` diagnostic counts as dropped evidence.
- Names bind by the LOCAL name an import introduces (an alias, a default-import name, ``* as ns``),
  not the exported one. Among equally specific bindings the latest one before the reference by
  byte position wins; bindings that cannot be ordered safely make the reference ambiguous and it
  is dropped. Module-level conditional imports are not visible to adapters: the latest wins.
- ``code.index.completed`` is ``success=false`` with ``scan_incomplete`` when the scan may have hidden
  a source file (truncated with omitted files, or a rejected path that could be one), then
  ``snapshot_incomplete`` (a dirty entry could not be hashed, see below), then ``files_skipped``,
  then ``files_degraded``. Run-level ``scan_incomplete`` and ``snapshot_incomplete`` forbid ALL
  absence inference for the target; otherwise absence is inferred per file revision (see coverage).
  Any adapter loss (dropped symbols, capped references, a degraded file) degrades the whole run:
  on large repos a single capped file makes the run ``files_degraded``. ``success`` and
  ``error_class`` stay run-level; per-file completeness is ``code.file.coverage_reported`` (below).
- Repositories with autocrlf, LFS or smudge filters have worktree bytes that differ from the
  committed blobs, so their SCIP evidence is ALWAYS dropped (``scip_file_not_at_commit``) and the
  run reports ``files_degraded``. That is conservative by design.

Idempotency and membership (what a later index, e.g. PLATFORM-038, can derive)
------------------------------------------------------------------------------
``index_id`` is deterministic: a digest of the repository, the target (commit or snapshot) and
the extractor/configuration digest. Re-indexing the same target yields the same ``index_id``.

EVERY idempotency key is the digest of the event's CLAIM: the whole envelope and payload except
an explicit per-type set of observation fields (``OBSERVATION_FIELDS``; ``context``, ``occurred_at``
and ``observed_at`` are observations of every event). ``ingest``'s conflict check compares the
very same claim view, so a key and its conflict test cannot diverge: under one key a stored event
can differ from a draft only by an observation (a replay, skipped) or by a digest collision (a
bug, raised as ``IndexingError("idempotency_conflict")`` before anything is submitted).

- ``code.file.indexed`` (no observation fields) is MEMBERSHIP per target: ``index_id``, target,
  ``file_id``, ``file_revision_id``, path and content. Every index of a target lists every file
  it covers, with the SAME ``file_revision_id`` when content did not change.
- ``code.symbol.indexed`` (observation: ``index_id``, target) claims the symbol, its revision and
  its ``file_id``/``file_revision_id``. Membership follows the file revision: an unchanged symbol
  in a new file revision (or under another file) is a new EVENT with the SAME
  ``symbol_revision_id``; "no new revision" means no new revision IDs, not no new events.
- ``code.relation.asserted`` and ``code.dependency.asserted`` (observation: ``index_id``,
  ``valid_from``) are per SOURCE FILE REVISION: the revision is part of ``assertion_id``. An
  unchanged file re-derives the same assertions (replays); a changed file yields assertions bound
  to its new revision, so a call or import that vanished from a file is exactly one whose
  assertion is absent for the file's current revision. Supersession claims are not file-bound.
- ``code.index.started`` (no observation fields) is one claim per target and configuration.
  ``code.index.completed`` (observation: ``duration_ms``) claims the OUTCOME: ``success``,
  ``error_class`` and the counts. A rerun of the same target with the same outcome is a replay; a
  different outcome (a file degraded the first time, clean on retry) is a NEW completed event
  under the same ``index_id``, never a conflict.
- ``success`` is false, with ``error_class`` ``files_skipped`` (a scanned file of a registered
  language never reached emission: missing or mismatched source, no lineage, unsupported, too
  large) or ``files_degraded`` (an adapter degraded a file). The latest completed event of the
  target's ``index_id`` is its outcome, its ``code.file.indexed`` events the membership. Absence
  inside a file revision is not read from ``success``: see the coverage rule below.
  ``files_degraded`` also covers dropped SCIP evidence (a position that names no boundary, or a
  file whose bytes are not the SCIP commit's), so a success never hides dropped evidence.
- ``code.assertion.observed`` (observation: ``index_id``, target) is ASSERTION MEMBERSHIP: one
  per (source file revision, assertion) for every relation and dependency claim (SCIP,
  syntactic and heuristic evidence alike). Its claim is ``file_id``, ``file_revision_id``,
  ``assertion_id`` and ``assertion_family``. ``index_id`` and the target are observations, so
  membership is per FILE REVISION, like ``code.symbol.indexed``: an unchanged file revision
  indexed at a new commit emits nothing new, and a changed file emits observations only for the
  assertions its new revision still produces. A consumer treats an assertion as current at T
  when an observation names it under a ``file_revision_id`` in T's ``code.file.indexed``
  membership. The assertion events themselves are unchanged (``valid_from`` and ``index_id`` stay
  observations of them). Supersession claims are not file-bound and get no observation.
- ``code.file.coverage_reported`` (no observation fields) is per FILE REVISION PER TARGET: one
  for every file of the membership, with ``index_id`` and the target in the claim because
  coverage depends on the run (a file degraded once may be clean on retry, which is a new claim
  under the same ``index_id``, never a conflict). ``complete=true`` with no losses, or the
  sorted unique ``losses`` mapped from: adapter ``symbols_dropped``, ``references_capped``,
  ``file_degraded``, ``work_budget_exceeded``; ``invalid_position``; ``scip_evidence_dropped``
  (an importer diagnostic for the path or a file not at the SCIP commit). ``syntax_recovered`` is
  no loss. ``not_indexed`` is reported (with no ``code.file.indexed``) for a scanned file that
  never reached emission yet has a ``file_id`` and a revision: a tracked file, unchanged against
  HEAD, that the scanner or parser skipped (too large, unreadable, missing or mismatched bytes),
  whose revision derives from the committed blob and lineage. A file without a lineage, an
  unsupported one, or a dirty or untracked one has no such revision: it counts at run level
  only, as does any diagnostic without a path (an unsafe SCIP path). A rerun with the same
  outcome is a replay.
- ABSENCE is inferred by a consumer (PLATFORM-038) for a file revision at T ONLY when a
  ``complete=true`` coverage event exists for it in T's index. A run-level ``scan_incomplete`` or
  ``snapshot_incomplete`` still forbids ALL absence inference for T, whatever the per-file events
  say, because the scan may have hidden files that no per-file event can mention.
- A cross-file assertion is bound to its SOURCE file revision only. If the TARGET file drops or
  renames the symbol while the source is unchanged, the source's old assertion persists: a
  consumer (PLATFORM-038) must ALSO check that the target symbol exists in the target file's
  current membership (the latest successful index's ``code.symbol.indexed`` for that
  ``file_revision_id``) before it treats the edge as live.
- A dirty snapshot is content-addressed and checkout-independent: ``snapshot_id`` is
  ``snapshot.workspace_snapshot_id(repository_id, base_commit, dirty entries)``, where an entry is
  ``(path, state, content_sha256)`` with state ``modified``, ``added``, ``deleted`` (no digest) or
  ``untracked``. The same dirty state in two checkouts is ONE snapshot, one ``index_id`` and one
  membership (``checkout_id`` is not part of it).

Joining a Codex session to the uncommitted revisions (G3)
---------------------------------------------------------
The single source of the ``snapshot_id`` is ``snapshot.workspace_snapshot_id``: a pure function of
plain strings and tuples with a versioned domain string, to be lifted verbatim into
``agent_context_sdk`` (pending, additive) so Codex capture and the indexer share it. Until Codex
emits ``git.workspace_snapshot.captured`` with that ID, the fallback join is
(``repository_id``, ``base_commit``, the set of ``modified_content_sha256`` values, the
``untracked_paths``): the indexer's dirty ``code.file.indexed`` ``content_sha256`` values and
untracked paths are exactly those fields of ``WorkspaceSnapshotCapturedV1``.

Net dirty state: ``deleted`` comes from the porcelain status (``D`` in the index or the worktree),
a rename (staged or not) is a ``deleted`` old path plus an ``added`` new one, a submodule pointer
change is ``gitlink`` (digest of the head and index OIDs), and a deleted path present again is
``modified``. Dirty files are hashed by streaming, up to ``IDENTITY_HASH_CAP`` (64 MiB) and
independent of the 1 MiB parse cap. If any dirty entry cannot be hashed (unreadable, over the
cap) or a path was rejected, NO canonical ``snapshot_id`` is derived: the run keeps a scan-local
id so the file-level events stay valid, and ``code.index.completed`` is ``success=false`` with
``error_class=snapshot_incomplete``. Such a snapshot never joins a Codex session.

Known limitation: symbols of a dirty file whose SCIP evidence was dropped get name-derived
logical IDs, which can differ from the SCIP-derived IDs at the later commit. Identity from commit
to commit stays stable.

A rename keeps the logical ID and revision; the file's symbols get new logical IDs when the
module path is part of their qualified name (or SCIP symbol), because that name is their identity.

Evidence and confidence
-----------------------
SCIP evidence is direct: ``scip`` kind, confidence 1.0, deterministic. Tree-sitter evidence is
inference: confidence 0.9 for syntactic facts (defines, imports, inherits) and 0.5 for heuristic
ones (calls, name references), always below 1. The SDK has no confidence-below-1 form of ``git``
evidence, so only certain (1.0) supersessions are emitted; weaker ones are dropped and counted.
Lower-confidence claims that a stronger one contradicts or repeats are KEPT: the assertion ID
includes the evidence kind, so a SCIP claim never overwrites a tree-sitter one. Drafts are ordered
per edge by evidence precedence: SCIP first, then tree-sitter, then git.

SDK shape notes: there is no ``INHERITS`` predicate, so a resolved inherit is ``REFERENCES``
(true, lossy); ``code.index.completed`` has counts but no diagnostics field, so adapter and
resolution diagnostics are logged as content-free counts (codes only, never paths or text).

External dependencies
---------------------
An import that resolves to nothing in the repository (stdlib or third-party) becomes one
``code.dependency.asserted`` from the importing module (its directory) to the external module's
top-level name, observed, tree-sitter evidence. Dropping it would lose the real dependency
graph, and one assertion per (module, package) keeps volume bounded. Unresolved calls and
inherits are dropped and counted, because a call target is a bare name, not a module.
"""

from __future__ import annotations

import bisect
import hashlib
import logging
import re
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any, Final

from agent_context_sdk import (
    CodeAssertionFamily,
    CodeAssertionObservedV1,
    CodeCoverageLoss,
    CodeDependencyAssertedV1,
    CodeFileCoverageReportedV1,
    CodeFileIndexedV1,
    CodeIndexCompletedV1,
    CodeIndexStartedV1,
    CodeRelationAssertedV1,
    CodeRelationPredicate,
    CodeSymbolIndexedV1,
    ContentDisposition,
    DependencyObservationKind,
    DeterministicEvidenceKind,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    InferenceEvidenceKind,
    IngestBatchRequestV1,
    ProducerV1,
    StoredEventV1,
    canonical_json_bytes,
    resolve_event_model,
    sha256_hex,
)
from agent_context_sdk.ids import new_uuid7
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.indexing import registry
from agent_context_platform.indexing.identity import (
    Supersession,
    encode_components,
    file_revision_id,
    repository_namespace,
    symbol_logical_id,
    symbol_revision_id,
    uncommitted_file_logical_id,
)
from agent_context_platform.indexing.registry import DEFAULT_REQUEST_SOURCE_BYTES
from agent_context_platform.indexing.scanner import (
    FileKind,
    RepositoryScan,
    ScanError,
    SkipReason,
    TrackedFile,
    TruncationReason,
    hash_worktree_file,
    read_worktree_file,
)
from agent_context_platform.indexing.scip import (
    PositionEncoding,
    SemanticDocument,
    SemanticIndex,
    SourceRange,
)
from agent_context_platform.indexing.snapshot import (
    DirtyEntry,
    gitlink_digest,
    workspace_snapshot_id,
)
from agent_context_platform.indexing.tree_sitter.base import (
    DIAGNOSTIC_CODES,
    MAX_INPUT_FILES,
    MAX_SOURCE_BYTES,
    ParsedDiagnostic,
    ParsedFile,
    ParsedReference,
    ParseRequest,
    SourceFile,
    StructuralAdapter,
    StructuralError,
    StructuralErrorCode,
    parser_fingerprint,
)
from agent_context_platform.ledger.repository import LedgerRepository
from agent_context_platform.ledger.service import IngestionService

_LOG = logging.getLogger(__name__)

INDEXER_PRODUCER_ID: Final = "agent-context-platform-indexer"
EMITTER_VERSION: Final = "1"
INDEXER_NAME: Final = "agent-context-platform-indexer"
SCHEMA_VERSION: Final = "1.0.0"
MAX_BATCH_EVENTS: Final = 500

SYNTACTIC_CONFIDENCE: Final = 0.9
HEURISTIC_CONFIDENCE: Final = 0.5

# Adapter refusals a hostile file can cause; anything else is environmental and propagates.
# Refusals that say nothing about a hostile file: the environment, or our own request, is at
# fault, so retrying smaller batches cannot help. Every other code (crash, signal death, timeout,
# oversized or malformed output, a lying adapter) can be caused by one file and is bisected.
_ENVIRONMENTAL_CODES: Final = frozenset(
    {
        StructuralErrorCode.INVALID_REQUEST,
        StructuralErrorCode.INPUT_TOO_LARGE,
        StructuralErrorCode.SPAWN_FAILED,
        StructuralErrorCode.SANDBOX_UNAVAILABLE,
        StructuralErrorCode.UNSAFE_READ_SET,
    }
)
# Child runs allowed for bisection retries, per ``parse_structural`` call, on top of the one
# initial run per batch. A hostile corpus cannot make the indexer spawn unbounded children.
MAX_BISECTION_RUNS: Final = 64
_EVENT_ID_DOMAIN: Final = b"agent-context-platform:code-event:v1\0"
_NAME_SEPARATOR: Final = re.compile(r"\.|::|/|#")
_SCIP_LOCAL: Final = "local "

type Diagnostics = Counter[str]


class IndexingError(Exception):
    """The evidence cannot be indexed or ingested; ``code`` is a stable, content-free reason."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class IndexingConfig:
    """Who is indexing and how time is read; both clocks are injectable for tests."""

    repository_id: str
    checkout_id: str | None = None
    workspace_id: str | None = None
    project_id: str | None = None
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    monotonic: Callable[[], float] = time.monotonic


@dataclass(frozen=True, slots=True)
class IndexingOutcome:
    """What ``ingest`` did: how many drafts were new, how many the ledger already had."""

    drafts: tuple[EventDraftV1, ...]
    submitted: int
    existing: int


@dataclass(frozen=True, slots=True)
class StructuralResult:
    """Parsed files plus the content-free counts of what degraded."""

    files: tuple[ParsedFile, ...]
    diagnostics: Mapping[str, int]


# --- structural parsing (FU-40) ------------------------------------------------------------------


def parse_structural(
    adapters: Mapping[str, StructuralAdapter], sources: Mapping[str, bytes]
) -> StructuralResult:
    """Parse ``sources`` with the registered adapters, up to 64 files per child.

    Any refusal a hostile file can cause (a crash or signal death, ``timeout``, oversized or
    malformed output, a lying adapter) is retried by bisection down to single files, so one bad
    file degrades alone: it becomes a ``file_degraded`` ``ParsedFile`` and the rest parse normally.
    Environmental refusals (sandbox unavailable, spawn failure, unsafe read set, invalid request)
    propagate. Bisection spends at most ``MAX_BISECTION_RUNS`` extra child runs per call; once
    they are gone the failing group is degraded whole (``bisection_budget_exhausted``).

    The adapter's CPU budget is per request, so one heavy file can leave every later file of a
    multi-file request degraded (``work_budget_exceeded``). Each such file is re-run ALONE in a
    fresh request, exactly once (``solo_retry``); only a file that still degrades alone stays
    degraded. Total child runs per call are therefore at most
    ``batches + MAX_BISECTION_RUNS + files`` (one initial run per batch, the bisection bound, and
    at most one solo retry per file).
    """
    diagnostics: Diagnostics = Counter()
    by_language: dict[str, list[SourceFile]] = {}
    for path in sorted(sources):
        language = registry.language_for_path(path)
        if language is None or language not in adapters:
            diagnostics["unsupported_language"] += 1
            continue
        content = sources[path]
        if len(content) > MAX_SOURCE_BYTES:
            diagnostics["source_too_large"] += 1
            continue
        by_language.setdefault(language, []).append(SourceFile.from_bytes(path, language, content))
    parsed: list[ParsedFile] = []
    retries = _Retries(MAX_BISECTION_RUNS)
    for language in sorted(by_language):
        for chunk in _chunks(by_language[language], _request_budget(language)):
            _parse_bisecting(adapters[language], chunk, parsed, diagnostics, retries)
    parsed.sort(key=lambda item: item.path)
    return StructuralResult(tuple(parsed), dict(diagnostics))


def _request_budget(language: str) -> int:
    """The adapter's per-request source budget: its walk costs CPU per byte (see the registry)."""
    support = registry.support_for(language)
    return DEFAULT_REQUEST_SOURCE_BYTES if support is None else support.max_request_source_bytes


def _chunks(files: list[SourceFile], budget: int) -> Iterable[list[SourceFile]]:
    """Requests of at most ``MAX_INPUT_FILES`` files and ``budget`` bytes of source.

    A file over the budget (up to the per-file cap) goes alone, so a normal request stays at
    about half the child's CPU backstop and inside its output bound.
    """
    chunk: list[SourceFile] = []
    size = 0
    for item in files:
        length = _source_length(item)
        if chunk and (len(chunk) >= MAX_INPUT_FILES or size + length > budget):
            yield chunk
            chunk, size = [], 0
        chunk.append(item)
        size += length
    if chunk:
        yield chunk


def _source_length(file: SourceFile) -> int:
    """Decoded size from the base64 length (no decode): exact up to its padding."""
    padding = len(file.content_b64) - len(file.content_b64.rstrip("="))
    return len(file.content_b64) // 4 * 3 - padding


class _Retries:
    """Remaining bisection child runs for one ``parse_structural`` call."""

    def __init__(self, remaining: int) -> None:
        self.remaining = remaining


def _degraded(file: SourceFile) -> ParsedFile:
    support = registry.support_for(file.language)
    assert support is not None  # parse_structural only passes registered languages
    return ParsedFile(
        path=file.path,
        language=file.language,
        parser_fingerprint=support.parser_fingerprint,
        symbols=(),
        diagnostics=(
            ParsedDiagnostic(code="file_degraded", count=1),
            ParsedDiagnostic(code="work_budget_exceeded", count=1),
        ),
    )


def _parse_bisecting(
    adapter: StructuralAdapter,
    files: list[SourceFile],
    out: list[ParsedFile],
    diagnostics: Diagnostics,
    retries: _Retries,
) -> None:
    try:
        module = adapter.parse(ParseRequest(files=tuple(files)))
    except StructuralError as error:
        if error.code in _ENVIRONMENTAL_CODES:
            raise
        diagnostics[f"adapter_{error.code.value}"] += 1
        if len(files) > 1 and retries.remaining >= 2:
            retries.remaining -= 2
            middle = len(files) // 2
            _parse_bisecting(adapter, files[:middle], out, diagnostics, retries)
            _parse_bisecting(adapter, files[middle:], out, diagnostics, retries)
            return
        if len(files) > 1:
            diagnostics["bisection_budget_exhausted"] += 1
        diagnostics["file_degraded"] += len(files)
        out.extend(_degraded(item) for item in files)
        return
    if len(files) == 1:
        out.extend(module.files)
        return
    by_path = {item.path: item for item in files}
    for parsed in module.files:
        if _budget_degraded(parsed) and parsed.path in by_path:
            diagnostics["solo_retry"] += 1
            _parse_solo(adapter, by_path[parsed.path], out, diagnostics)
        else:
            out.append(parsed)


def _budget_degraded(parsed: ParsedFile) -> bool:
    return any(item.code == "work_budget_exceeded" for item in parsed.diagnostics)


def _parse_solo(
    adapter: StructuralAdapter, file: SourceFile, out: list[ParsedFile], diagnostics: Diagnostics
) -> None:
    """The single retry of a file the shared request's budget cut short: no further retries."""
    try:
        module = adapter.parse(ParseRequest(files=(file,)))
    except StructuralError as error:
        if error.code in _ENVIRONMENTAL_CODES:
            raise
        diagnostics[f"adapter_{error.code.value}"] += 1
        diagnostics["file_degraded"] += 1
        out.append(_degraded(file))
        return
    for parsed in module.files:
        if _budget_degraded(parsed):
            diagnostics["file_degraded"] += 1
        out.append(parsed)


# --- identifiers ---------------------------------------------------------------------------------


def event_id_for(key: str) -> uuid.UUID:
    """A deterministic UUIDv7-shaped ID: the SDK types ``event_id`` as UUID7, a uuid5 would fail."""
    digest = bytearray(hashlib.sha256(_EVENT_ID_DOMAIN + key.encode("utf-8")).digest()[:16])
    digest[6] = (digest[6] & 0x0F) | 0x70
    digest[8] = (digest[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(digest))


def _digest(*components: str) -> str:
    return hashlib.sha256(encode_components(*components).encode("utf-8")).hexdigest()


def blob_oid(content: bytes, object_format: str) -> str:
    """The Git blob object ID of ``content`` (the identity input of a file revision)."""
    hasher = hashlib.sha256() if object_format == "sha256" else hashlib.sha1(usedforsecurity=False)
    hasher.update(b"blob %d\0" % len(content))
    hasher.update(content)
    return hasher.hexdigest()


def _module_dir(path: str) -> str:
    parent = str(PurePosixPath(path).parent)
    return parent


# Binding scope per language: what an unqualified name or an import target can mean besides the
# file itself. Python and TypeScript/JavaScript bind per FILE plus its imports. Go binds per
# PACKAGE: every non-test ``.go`` file of one directory shares a namespace, and an import names a
# package, not a file. Adding a language here is the only change its resolution needs.
_PACKAGE_SCOPED_LANGUAGES: Final = frozenset({"go"})


_GO_MOD_BYTES: Final = 64 * 1024
_GO_MODULE_LINE: Final = re.compile(r'^\s*module\s+"?([^\s"]+)"?\s*(?://.*)?$')


def _is_go_test_file(path: str) -> bool:
    return path.endswith("_test.go")


def _tokens(name: str) -> list[str]:
    return [token for token in _NAME_SEPARATOR.split(name) if token]


def _line_starts(source: bytes) -> list[int]:
    starts = [0]
    starts.extend(index + 1 for index, byte in enumerate(source) if byte == 0x0A)
    return starts


@dataclass(slots=True)
class _Line:
    """One source line, measured once: every position on it is then a table lookup."""

    start: int
    length: int
    text: bytes | None  # None for an ASCII line, where every encoding agrees
    valid: bool = True  # decodable as UTF-8
    code_points: list[int] | None = None  # byte offset of each code point boundary
    utf16: list[int | None] | None = None  # by UTF-16 unit; None inside a surrogate pair

    def tables(self) -> None:
        if self.code_points is not None or not self.valid or self.text is None:
            return
        try:
            decoded = self.text.decode("utf-8")
        except UnicodeDecodeError:
            self.valid = False
            return
        boundaries = [0]
        units: list[int | None] = [0]
        total = 0
        for symbol in decoded:
            code = ord(symbol)
            total += 1 if code < 0x80 else 2 if code < 0x800 else 3 if code < 0x10000 else 4
            boundaries.append(total)
            if code > 0xFFFF:
                units.append(None)  # between the two UTF-16 code units
            units.append(total)
        self.code_points, self.utf16 = boundaries, units


_Lines = dict[int, _Line]


def _line_at(source: bytes, starts: list[int], line: int, cache: _Lines) -> _Line:
    found = cache.get(line)
    if found is None:
        begin = starts[line]
        end = starts[line + 1] if line + 1 < len(starts) else len(source)
        if end > begin and source[end - 1] == 0x0A:  # the terminator is not part of the line:
            end -= 2 if end - 1 > begin and source[end - 2] == 0x0D else 1  # LF or CRLF
        text = source[begin:end]
        found = cache[line] = _Line(begin, end - begin, None if text.isascii() else text)
    return found


def _byte_offset(
    source: bytes,
    starts: list[int],
    line: int,
    character: int,
    encoding: PositionEncoding,
    cache: _Lines | None = None,
) -> int | None:
    """Byte offset of a SCIP ``line``/``character`` in ``source``, or ``None`` if it is invalid.

    Closed over ``PositionEncoding``: an unknown encoding is never guessed, and an unspecified
    one is honoured only on pure-ASCII lines, where every encoding agrees. Each line is measured
    once (``cache``), so a minified megabyte on one line costs one pass, not one per position.
    """
    if not 0 <= line < len(starts) or character < 0 or encoding is PositionEncoding.UNKNOWN:
        return None
    item = _line_at(source, starts, line, {} if cache is None else cache)
    if item.text is None:  # ASCII: a code unit is a byte in every encoding
        return item.start + character if character <= item.length else None
    if encoding is PositionEncoding.UNSPECIFIED:
        return None
    if encoding is PositionEncoding.UTF8:
        if character > item.length or (
            character < item.length and source[item.start + character] & 0xC0 == 0x80
        ):
            return None  # past the line, or inside a multi-byte character
        return item.start + character
    item.tables()
    table = item.code_points if encoding is PositionEncoding.UTF32 else item.utf16
    if table is None or character >= len(table) or table[character] is None:
        return None  # invalid UTF-8, past the line, or between the two halves of a pair
    return item.start + table[character]  # type: ignore[operator]


def _range_offsets(
    source: bytes,
    starts: list[int],
    located: SourceRange,
    encoding: PositionEncoding,
    cache: _Lines | None = None,
) -> tuple[int, int] | None:
    lines = {} if cache is None else cache
    begin = _byte_offset(
        source, starts, located.start_line, located.start_character, encoding, lines
    )
    end = _byte_offset(source, starts, located.end_line, located.end_character, encoding, lines)
    if begin is None or end is None or end < begin:
        return None
    return begin, end


class _Intervals[T]:
    """Spans that nest, answering "the smallest span containing this range" without a scan.

    Entries are given in preference order (the first of two equal spans wins). Sorted once by
    (start, -end), each entry gets the nearest earlier entry that contains it as its parent; a
    query bisects to the last span starting at or before it and walks up parents until one
    contains the whole range.
    """

    __slots__ = ("ends", "items", "parents", "starts")

    def __init__(self, entries: Iterable[tuple[int, int, T]]) -> None:
        ordered = sorted(enumerate(entries), key=lambda e: (e[1][0], -e[1][1], -e[0]))
        self.starts = [entry[0] for _, entry in ordered]
        self.ends = [entry[1] for _, entry in ordered]
        self.items = [entry[2] for _, entry in ordered]
        self.parents: list[int] = []
        stack: list[int] = []
        for position, end in enumerate(self.ends):
            while stack and self.ends[stack[-1]] < end:
                stack.pop()
            self.parents.append(stack[-1] if stack else -1)
            stack.append(position)

    def innermost(self, begin: int, end: int) -> T | None:
        position = bisect.bisect_right(self.starts, begin) - 1
        while position >= 0:
            if end <= self.ends[position]:
                return self.items[position]
            position = self.parents[position]
        return None


# --- per-run model -------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Sym:
    symbol_id: uuid.UUID
    qualified_name: str
    kind: str
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class _Binding:
    """What a local name stands for: a file, the dotted path the qualifier must still spell
    after it (``import a.b`` binds ``a`` with ``("b",)``), and the symbol it names when it is one
    (``from a import A``: ``A`` binds file ``a`` with symbol ``"A"``, the owner chain's start)."""

    file: _File
    consumed: tuple[str, ...] = ()
    symbol: str | None = None
    at: int = 0  # byte offset of the import that made the binding
    scope: str | None = None  # the symbol the import sits in (``None``: module level)


@dataclass(slots=True)
class _SymbolIndex:
    """One file's symbols, tokenized once: every reference lookup is a dict probe."""

    module: _Sym | None
    by_last: dict[str, list[_Sym]]
    module_level: frozenset[str]
    owners: dict[str, list[tuple[_Sym, list[str]]]]  # name -> (symbol, its owner tokens)
    module_tokens: list[str] | None

    @classmethod
    def build(cls, symbols: Sequence[_Sym]) -> _SymbolIndex:
        module = next((s for s in symbols if s.kind == "module"), None)
        owner = None if module is None else _tokens(module.qualified_name)
        by_last: dict[str, list[_Sym]] = {}
        owners: dict[str, list[tuple[_Sym, list[str]]]] = {}
        module_level: set[str] = set()
        for symbol in symbols:
            tokens = _tokens(symbol.qualified_name)
            if not tokens:
                continue
            by_last.setdefault(tokens[-1], []).append(symbol)
            owners.setdefault(tokens[-1], []).append((symbol, tokens[:-1]))
            if symbol is not module and owner is not None and tokens[:-1] == owner:
                module_level.add(tokens[-1])
        return cls(module, by_last, frozenset(module_level), owners, owner)


@dataclass(slots=True)
class _DefinitionOwners:
    """One file's symbols by last name token, so each SCIP definition is a bisect."""

    by_name: dict[str, _Intervals[int]]  # name -> spans (symbol index)
    module: _Intervals[int]

    @classmethod
    def build(cls, parsed: ParsedFile) -> _DefinitionOwners:
        named: dict[str, list[tuple[int, int, int]]] = {}
        module: list[tuple[int, int, int]] = []
        for index, symbol in enumerate(parsed.symbols):
            entry = (symbol.start_byte, symbol.end_byte, index)
            if symbol.kind == "module":
                module.append(entry)
                continue
            tokens = _tokens(symbol.qualified_name)
            if tokens:
                named.setdefault(tokens[-1], []).append(entry)
        return cls(
            {name: _Intervals(entries) for name, entries in named.items()}, _Intervals(module)
        )

    def owner(self, source: bytes, offsets: tuple[int, int]) -> int | None:
        begin, end = offsets
        if begin == end == 0:
            return self.module.innermost(begin, end)
        found = self.by_name.get(source[begin:end].decode("utf-8", errors="replace"))
        return None if found is None else found.innermost(begin, end)


@dataclass(slots=True)
class _File:
    path: str
    language: str
    file_id: uuid.UUID
    revision_id: uuid.UUID
    content_sha256: str
    extractor_name: str
    extractor_version: str
    module_parts: tuple[str, ...]
    symbols: list[_Sym] = field(default_factory=list)
    refs: dict[str, _Sym] = field(default_factory=dict)
    lookup: _SymbolIndex | None = None
    spans: _Intervals[_Sym] | None = None


@dataclass(frozen=True, slots=True)
class _Claim:
    """One relation claim; kept alongside every contradicting or repeated claim."""

    subject: str
    predicate: CodeRelationPredicate
    obj: str
    evidence: DeterministicEvidenceKind | InferenceEvidenceKind
    confidence: float
    extractor_name: str
    extractor_version: str
    revision: str  # file revision of the source file: claims are per revision
    file_id: (
        str  # the source file of ``revision`` (not part of the claim key: it is in the revision)
    )

    def sort_key(self) -> tuple[str, str, str, int, str, str]:
        rank = 0
        for candidate in ("scip", "tree_sitter", "git"):
            if candidate == self.evidence.value:
                break
            rank += 1
        return (
            self.subject,
            self.predicate.value,
            self.obj,
            rank,
            self.extractor_name,
            self.extractor_version + self.revision,
        )


IDENTITY_HASH_CAP = 64 * 1024 * 1024  # dirty files are hashed for the snapshot up to this size


def _dirty_entries(scan: RepositoryScan) -> list[DirtyEntry] | None:
    """The scan's NET uncommitted state as ``(path, state, digest)``; ``None`` if incomplete.

    ``deleted`` comes from the porcelain status (``D`` in the index or the worktree); a rename is a
    delete of the old path plus an add of the new one; a submodule pointer change is ``gitlink``.
    A deleted path that is present again (untracked) is ``modified``. Every other entry needs a
    digest: the scan's, else a STREAMED SHA-256 up to ``IDENTITY_HASH_CAP`` (independent of the
    parse caps). A file that cannot be hashed, or a rejected path, makes the snapshot incomplete.
    """
    workspace = scan.workspace
    tracked = {item.path: item for item in scan.files}
    untracked = {item.path: item for item in scan.untracked}
    status = {change.path: change for change in workspace.changes}
    gitlinks = {link.path: link for link in workspace.gitlinks}
    untracked_paths = set(workspace.untracked_paths)
    complete = not scan.rejections

    def digest_of(path: str, known: str | None) -> str | None:
        if known is not None:
            return known
        try:
            return hash_worktree_file(scan, path, IDENTITY_HASH_CAP)
        except ScanError:
            return None

    live: dict[str, tuple[str, str | None]] = {}
    gone: set[str] = set()
    for renamed in workspace.changes:
        if renamed.orig_path is not None and "R" in renamed.xy:
            gone.add(renamed.orig_path)
    for path in sorted({*workspace.modified_paths, *status}):
        link = gitlinks.get(path)
        if link is not None:
            live[path] = ("gitlink", gitlink_digest(link.head_oid, link.index_oid))
            continue
        change = status.get(path)
        xy = change.xy if change is not None else ""
        item = tracked.get(path)
        if "D" in xy or (item is not None and item.skipped is SkipReason.MISSING):
            gone.add(path)
            continue
        state = "added" if xy[:1] in ("A", "R", "C") else "modified"
        live[path] = (state, digest_of(path, None if item is None else item.content_sha256))
    for path in sorted(untracked_paths):
        found = untracked.get(path)
        known = None if found is None else found.content_sha256
        live[path] = ("modified" if path in gone else "untracked", digest_of(path, known))
        gone.discard(path)
    entries: list[DirtyEntry] = [(path, "deleted", None) for path in sorted(gone - live.keys())]
    for path, (state, digest) in live.items():
        if digest is None:
            complete = False
        entries.append((path, state, digest))
    return entries if complete else None


class _Run:
    """The evidence of one target, resolved into drafts. Single use."""

    def __init__(
        self,
        service: IndexingService,
        scan: RepositoryScan,
        semantic: SemanticIndex | None,
        structural: Sequence[ParsedFile],
        sources: Mapping[str, bytes],
        file_logical_ids: Mapping[str, uuid.UUID],
        supersessions: Iterable[Supersession],
    ) -> None:
        self.config = service.config
        self.repository_id = service.config.repository_id
        self.scan = scan
        self.semantic = semantic
        self.structural = {item.path: item for item in structural}
        self.sources = sources
        self.lineage = file_logical_ids
        self.supersessions = tuple(supersessions)
        self.diagnostics: Diagnostics = Counter()
        self.now = service.config.clock()
        self.namespace = repository_namespace(self.repository_id)
        self.files: dict[str, _File] = {}
        self.packages: dict[str, list[_File]] = {}
        self.go_module: str | None = None
        self.package_names: dict[str, dict[str, list[_Sym]]] = {}
        self.file_events: list[EventDraftV1] = []
        self.symbol_events: list[EventDraftV1] = []
        self.observation_events: list[EventDraftV1] = []
        self.claims: dict[tuple[str, ...], _Claim] = {}
        self.dependencies: dict[tuple[str, ...], CodeDependencyAssertedV1] = {}
        self.dependency_keys: dict[tuple[str, ...], str] = {}
        self.dependency_sources: dict[tuple[str, ...], _File] = {}
        self.file_losses: dict[str, set[CodeCoverageLoss]] = {}
        self.symbol_ids: set[str] = set()
        self.seen_keys: set[str] = set()
        head = scan.workspace.head_commit
        if head is None:
            raise IndexingError("no_commit")
        self.head = head
        if semantic is not None and (
            semantic.repository_id != self.repository_id or semantic.commit != head
        ):
            raise IndexingError("semantic_target_mismatch")
        self.snapshot_id: str | None = None
        self.snapshot_complete = True
        if scan.workspace.is_dirty:
            entries = _dirty_entries(scan)
            if entries is None:
                # No canonical (joinable) identity: a scan-local id keeps the file-level events
                # valid, and the run completes ``snapshot_incomplete``.
                self.snapshot_complete = False
                local = _digest(
                    "snapshot_incomplete", head, scan.workspace.dirty_state_sha256 or ""
                )
                self.snapshot_id = "snap_" + local[:40]
            else:
                self.snapshot_id = workspace_snapshot_id(self.repository_id, head, entries)
        self.tracked = {item.path: item for item in scan.files}
        self.untracked = {item.path: item for item in scan.untracked}
        self.modified = frozenset(scan.workspace.modified_paths)
        self.scip_documents: dict[str, SemanticDocument] = {}
        self.index_id = self._index_id()

    def lose(self, path: str, loss: CodeCoverageLoss) -> None:
        """Record that ``path`` lost information; it is reported for the file if it is indexed."""
        self.file_losses.setdefault(path, set()).add(loss)

    # --- identifiers and drafts ---

    def _index_id(self) -> str:
        return (
            "idx-"
            + _digest(
                "index",
                self.repository_id,
                self.snapshot_id or self.head,
                self.configuration_sha256(),
            )[:48]
        )

    def configuration_sha256(self) -> str:
        structural = sorted(
            {
                (item.language, s.extractor_name, s.extractor_version, s.parser_fingerprint)
                for item in self.structural.values()
                if (s := registry.support_for(item.language)) is not None
            }
        )
        scip = None
        if self.semantic is not None:
            scip = [self.semantic.run.tool_name, self.semantic.run.tool_version]
        document: dict[str, Any] = {
            "emitter": EMITTER_VERSION,
            "structural": [list(item) for item in structural],
            "semantic": scip,
        }
        return str(sha256_hex(canonical_json_bytes(document)))

    def target(self) -> dict[str, str | None]:
        return {
            "commit_id": None if self.snapshot_id else self.head,
            "snapshot_id": self.snapshot_id,
        }

    def module_id(self, path: str) -> str:
        return str(uuid.uuid5(self.namespace, encode_components("module", _module_dir(path))))

    def external_id(self, name: str) -> str:
        return str(uuid.uuid5(self.namespace, encode_components("external_module", name)))

    def draft(self, event_type: str, payload: Any) -> EventDraftV1:
        """A draft whose idempotency key is the digest of its CLAIM (see ``claim_key``)."""
        model = resolve_event_model(event_type, SCHEMA_VERSION)
        if type(payload) is not model:  # pragma: no cover - programming error guard
            raise IndexingError("payload_type_mismatch")
        pending = EventDraftV1(
            event_id=event_id_for("pending"),
            event_type=event_type,
            stream_id=f"code-index:{self.repository_id}",
            occurred_at=self.now,
            observed_at=self.now,
            producer=INDEXER_PRODUCER,
            context=EventContextV1(
                repository_id=self.repository_id,
                checkout_id=self.config.checkout_id,
                workspace_id=self.config.workspace_id,
                project_id=self.config.project_id,
            ),
            payload=payload.model_dump(mode="json"),
            redaction=EventRedactionSummaryV1(
                policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
            ),
            idempotency_key="pending",
        )
        key = claim_key(pending)
        return pending.model_copy(update={"event_id": event_id_for(key), "idempotency_key": key})

    # --- files and symbols ---

    def file_identity(self, path: str) -> uuid.UUID | None:
        committed = self.lineage.get(path)
        if committed is not None:
            return committed
        tracked = self.tracked.get(path)
        dirty_only = path in self.untracked or (tracked is not None and tracked.change is not None)
        if dirty_only:
            return uncommitted_file_logical_id(self.repository_id, path)
        return None

    def scanned_digest(self, path: str) -> str | None:
        tracked = self.tracked.get(path)
        if tracked is not None:
            return tracked.content_sha256
        untracked = self.untracked.get(path)
        return None if untracked is None else untracked.content_sha256

    def admit(self, path: str, language: str, fingerprint: str) -> _File | None:
        """Register a file that will get a revision, or count why it cannot."""
        content = self.sources.get(path)
        if content is None:
            self.diagnostics["source_missing"] += 1
            return None
        expected = self.scanned_digest(path)
        if expected is None:
            self.diagnostics["file_not_scanned"] += 1
            return None
        if hashlib.sha256(content).hexdigest() != expected:
            self.diagnostics["source_mismatch"] += 1
            return None
        file_id = self.file_identity(path)
        if file_id is None:
            self.diagnostics["file_without_lineage"] += 1
            return None
        revision = file_revision_id(
            self.repository_id,
            file_id,
            blob_oid(content, self.scan.object_format),
            language,
            fingerprint,
        )
        return _File(
            path=path,
            language=language,
            file_id=file_id,
            revision_id=revision,
            content_sha256=expected,
            extractor_name="",
            extractor_version="",
            module_parts=(),
        )

    def emit_file(self, item: _File) -> None:
        payload = CodeFileIndexedV1(
            **self.target(),
            index_id=self.index_id,
            module_id=self.module_id(item.path),
            file_id=str(item.file_id),
            file_revision_id=str(item.revision_id),
            path=item.path,
            content_sha256=item.content_sha256,
            language=item.language,
            extractor_name=item.extractor_name,
            extractor_version=item.extractor_version,
        )
        self.add(
            self.file_events,
            self.draft("code.file.indexed", payload),
        )

    def add(self, into: list[EventDraftV1], draft: EventDraftV1) -> None:
        if draft.idempotency_key in self.seen_keys:
            return
        self.seen_keys.add(draft.idempotency_key)
        into.append(draft)

    def emit_symbol(
        self,
        item: _File,
        symbol_id: uuid.UUID,
        qualified_name: str,
        kind: str,
        signature: str,
        semantic_digest: str,
        scip_symbol: str | None,
        extractor: tuple[str, str],
    ) -> None:
        revision = symbol_revision_id(self.repository_id, symbol_id, signature, semantic_digest)
        payload = CodeSymbolIndexedV1(
            **self.target(),
            index_id=self.index_id,
            file_id=str(item.file_id),
            file_revision_id=str(item.revision_id),
            symbol_id=str(symbol_id),
            symbol_revision_id=str(revision),
            qualified_name=qualified_name,
            kind=kind,
            signature_fingerprint_sha256=signature,
            semantic_fingerprint_sha256=semantic_digest,
            extractor_name=extractor[0],
            extractor_version=extractor[1],
            scip_symbol=scip_symbol,
        )
        self.add(
            self.symbol_events,
            self.draft("code.symbol.indexed", payload),
        )
        self.symbol_ids.add(str(symbol_id))

    def index_structural(self, parsed: ParsedFile, document: SemanticDocument | None) -> None:
        support = registry.support_for(parsed.language)
        if support is None:
            self.diagnostics["unsupported_language"] += 1
            return
        label = registry.label_for_path(parsed.path) or parsed.language
        item = self.admit(parsed.path, label, parsed.parser_fingerprint)
        if item is None:
            return
        item.extractor_name, item.extractor_version = (
            support.extractor_name,
            support.extractor_version,
        )
        for diagnostic in parsed.diagnostics:
            self.diagnostics[f"adapter_{diagnostic.code}"] += diagnostic.count
            loss = _ADAPTER_LOSSES.get(diagnostic.code)
            if loss is not None:
                self.lose(parsed.path, loss)
        source = self.sources[parsed.path]
        matched = self.match_scip(parsed, document, source)
        used: set[str] = set()
        for index, symbol in enumerate(parsed.symbols):
            scip_symbol = matched.get(index)
            if scip_symbol is not None and scip_symbol in used:
                scip_symbol = None  # one SCIP symbol names one identity
            if scip_symbol is not None:
                used.add(scip_symbol)
            symbol_id = symbol_logical_id(
                self.repository_id,
                scip_symbol=scip_symbol,
                language=label,
                file_logical_id=item.file_id,
                qualified_name=symbol.qualified_name,
                kind=symbol.kind,
                disambiguator=symbol.disambiguator,
            )
            sym = _Sym(
                symbol_id, symbol.qualified_name, symbol.kind, symbol.start_byte, symbol.end_byte
            )
            item.symbols.append(sym)
            item.refs[symbol.ref] = sym
            # The revision comes from the tree-sitter body fingerprint only.
            self.emit_symbol(
                item,
                symbol_id,
                symbol.qualified_name,
                symbol.kind,
                symbol.signature_digest,
                symbol.semantic_fingerprint,
                scip_symbol,
                (item.extractor_name, item.extractor_version),
            )
            self.claim(
                item,
                str(item.file_id),
                CodeRelationPredicate.DEFINES,
                str(symbol_id),
                InferenceEvidenceKind.TREE_SITTER,
                SYNTACTIC_CONFIDENCE,
                item.extractor_name,
                item.extractor_version,
            )
        by_ref = {symbol.ref: index for index, symbol in enumerate(parsed.symbols)}  # once per file
        for relation in parsed.relations:
            self.in_file_relation(item, by_ref, relation)
        self.files[parsed.path] = item
        self.emit_file(item)

    def in_file_relation(self, item: _File, by_ref: Mapping[str, int], relation: Any) -> None:
        source, target = by_ref.get(relation.source_ref), by_ref.get(relation.target_ref)
        if source is None or target is None:  # pragma: no cover - validate_module guarantees it
            return
        predicate, confidence = {
            "calls": (CodeRelationPredicate.CALLS, HEURISTIC_CONFIDENCE),
            "imports": (CodeRelationPredicate.IMPORTS, SYNTACTIC_CONFIDENCE),
            "inherits": (CodeRelationPredicate.REFERENCES, SYNTACTIC_CONFIDENCE),
            "references": (CodeRelationPredicate.REFERENCES, HEURISTIC_CONFIDENCE),
            "contains": (CodeRelationPredicate.DEFINES, SYNTACTIC_CONFIDENCE),
        }[relation.kind]
        self.claim(
            item,
            str(item.symbols[source].symbol_id),
            predicate,
            str(item.symbols[target].symbol_id),
            InferenceEvidenceKind.TREE_SITTER,
            confidence,
            item.extractor_name,
            item.extractor_version,
        )

    def index_scip_only(self, document: SemanticDocument) -> None:
        semantic = self.semantic
        if semantic is None:  # pragma: no cover - only called with semantic evidence
            return
        language = registry.label_for_path(document.path) or (
            document.language.lower() or "unknown"
        )
        run = semantic.run
        fingerprint = parser_fingerprint(run.tool_name, run.tool_version, {"evidence": "scip"})
        item = self.admit(document.path, language, fingerprint)
        if item is None:
            return
        item.extractor_name, item.extractor_version = run.tool_name, run.tool_version or "unknown"
        for symbol in document.symbols:
            item.symbols.append(
                _Sym(symbol.symbol_id, symbol.display_name or symbol.symbol, symbol.kind, 0, 0)
            )
            self.emit_symbol(
                item,
                symbol.symbol_id,
                symbol.display_name or symbol.symbol,
                symbol.kind,
                symbol.signature_digest,
                symbol.semantic_digest,
                symbol.symbol,
                (item.extractor_name, item.extractor_version),
            )
            self.claim(
                item,
                str(item.file_id),
                CodeRelationPredicate.DEFINES,
                str(symbol.symbol_id),
                DeterministicEvidenceKind.SCIP,
                1.0,
                item.extractor_name,
                item.extractor_version,
            )
        self.files[document.path] = item
        self.emit_file(item)

    def match_scip(
        self, parsed: ParsedFile, document: SemanticDocument | None, source: bytes
    ) -> dict[int, str]:
        """SCIP symbol per tree-sitter symbol index: a definition inside the symbol that names it."""
        if document is None:
            return {}
        starts = _line_starts(source)
        lines: _Lines = {}
        owners = _DefinitionOwners.build(parsed)  # tokenized once per file
        matched: dict[int, str] = {}
        taken: set[str] = set()  # SCIP symbols already named; a set, not a scan of ``matched``
        for occurrence in document.occurrences:
            if not occurrence.is_definition or occurrence.symbol.startswith(_SCIP_LOCAL):
                continue
            offsets = _range_offsets(
                source, starts, occurrence.range, document.position_encoding, lines
            )
            if offsets is None:
                self.diagnostics["invalid_position"] += 1  # a definition that names nothing
                self.lose(parsed.path, CodeCoverageLoss.INVALID_POSITION)
                continue
            index = owners.owner(source, offsets)
            if index is not None and index not in matched and occurrence.symbol not in taken:
                matched[index] = occurrence.symbol
                taken.add(occurrence.symbol)
        return matched

    # --- claims and dependencies ---

    def claim(
        self,
        item: _File,
        subject: str,
        predicate: CodeRelationPredicate,
        obj: str,
        evidence: DeterministicEvidenceKind | InferenceEvidenceKind,
        confidence: float,
        extractor_name: str,
        extractor_version: str,
    ) -> None:
        if subject == obj:
            return
        revision = str(item.revision_id)  # the source file revision that yielded the claim
        key = (
            subject,
            predicate.value,
            obj,
            evidence.value,
            extractor_name,
            extractor_version,
            revision,
        )
        self.claims.setdefault(
            key,
            _Claim(
                subject,
                predicate,
                obj,
                evidence,
                confidence,
                extractor_name,
                extractor_version,
                revision,
                str(item.file_id),
            ),
        )

    def dependency(
        self,
        item: _File,
        dependent: str,
        name: str,
        kind: DependencyObservationKind,
        evidence: DeterministicEvidenceKind | InferenceEvidenceKind,
        extractor: tuple[str, str],
        resolved_version: str | None = None,
    ) -> None:
        dependency_id = self.external_id(name)
        confidence = (
            1.0 if isinstance(evidence, DeterministicEvidenceKind) else SYNTACTIC_CONFIDENCE
        )
        key = (
            dependent,
            dependency_id,
            kind.value,
            evidence.value,
            extractor[0],
            extractor[1],
            str(item.revision_id),  # per importing-file revision
        )
        payload = CodeDependencyAssertedV1(
            assertion_id=_assertion_id("dependency", *key),
            evidence_kind=evidence,
            deterministic=isinstance(evidence, DeterministicEvidenceKind),
            extractor_name=extractor[0],
            extractor_version=extractor[1],
            confidence=confidence,
            valid_from=self.now,
            index_id=self.index_id,
            dependent_id=dependent,
            dependency_id=dependency_id,
            dependency_kind=kind,
            resolved_version=resolved_version,
        )
        if key not in self.dependencies:
            self.dependencies[key] = payload
            self.dependency_sources[key] = item

    # --- cross-file resolution ---

    def resolve_references(self) -> None:
        for item in self.files.values():  # every symbol is known: index each file exactly once
            item.lookup = _SymbolIndex.build(item.symbols)
        modules = self.python_modules()
        path_modules = self.path_modules()
        directories = self.go_directories()
        self.packages = directories
        self.go_module = self.read_go_module() if directories else None
        for path in sorted(self.files):
            item = self.files[path]
            parsed = self.structural.get(path)
            if parsed is None:
                continue
            bound: dict[str, list[_Binding]] = {}
            for reference in parsed.references:
                if reference.kind != "import":
                    continue
                target, whole = self.resolve_import(
                    item, reference, modules, path_modules, directories
                )
                self.bind_import(item, reference, target, whole, bound)
            for reference in parsed.references:
                if reference.kind != "import":
                    self.bind_symbol_reference(item, reference, bound)

    def python_modules(self) -> dict[str, _File]:
        """Dotted module name to file, for the repository root and the ``src`` layout."""
        modules: dict[str, _File] = {}
        for path, item in self.files.items():
            if item.language != "python":
                continue
            pure = PurePosixPath(path)
            parts = list(pure.with_suffix("").parts)
            if parts and parts[-1] == "__init__":
                parts.pop()
            for prefix in ((), ("src",)):
                if tuple(parts[: len(prefix)]) == prefix and len(parts) > len(prefix):
                    modules.setdefault(".".join(parts[len(prefix) :]), item)
        return modules

    def scope_files(self, item: _File) -> list[_File]:
        """The files whose top-level names ``item``'s namespace shares (the file, or its package).

        A Go test file sees its package's production files and its sibling test files; a
        production file never sees a test file (which may belong to a separate ``_test`` package).
        """
        if item.language not in _PACKAGE_SCOPED_LANGUAGES:
            return [item]
        siblings = self.packages.get(_module_dir(item.path), [])
        if _is_go_test_file(item.path):
            return list(siblings) or [item]  # production files and sibling test files
        return [f for f in siblings if not _is_go_test_file(f.path)] or [item]

    def go_directories(self) -> dict[str, list[_File]]:
        directories: dict[str, list[_File]] = {}
        for path, item in sorted(self.files.items()):
            if item.language == "go":
                directories.setdefault(_module_dir(path), []).append(item)
        return directories

    def resolve_import(
        self,
        source: _File,
        reference: ParsedReference,
        modules: Mapping[str, _File],
        path_modules: Mapping[str, _File],
        directories: Mapping[str, list[_File]],
    ) -> tuple[_File | None, bool]:
        """The repository file an import names, or ``None`` when it is external or unknown."""
        if source.language == "python":
            return self.resolve_python(source, reference, modules, path_modules)
        if source.language == "go":
            return self.resolve_go(source, reference, directories), True
        return self.resolve_script(source, reference)

    def resolve_python(
        self,
        source: _File,
        reference: ParsedReference,
        modules: Mapping[str, _File],
        path_modules: Mapping[str, _File],
    ) -> tuple[_File | None, bool]:
        """The file an import resolves to and whether it is the whole dotted name (a module).

        ``from pkg import name`` resolves to ``pkg`` when ``pkg.name`` is not a module, so
        ``name`` is then a symbol of that file, not a module.
        """
        head = f"{reference.qualifier}." if reference.qualifier else ""
        dotted = head + reference.target_name
        level = reference.relative_level
        if level:
            package = list(PurePosixPath(source.path).parent.parts)
            if level - 1 > len(package):
                return None, False
            package = package[: len(package) - (level - 1)]
            base = ".".join(package)
            candidates = [f"{base}.{dotted}" if base else dotted]
            if reference.qualifier:
                candidates.append(f"{base}.{reference.qualifier}" if base else reference.qualifier)
            elif base:
                candidates.append(base)
            for position, name in enumerate(candidates):
                if name in path_modules:
                    return path_modules[name], position == 0
            return None, False
        parts = dotted.split(".")
        for length in range(len(parts), 0, -1):
            found = modules.get(".".join(parts[:length]))
            if found is not None:
                return found, length == len(parts)
        return None, False

    def path_modules(self) -> dict[str, _File]:
        """Dotted names from repository-relative paths (no ``src`` stripping), for relative imports."""
        names: dict[str, _File] = {}
        for path, item in self.files.items():
            if item.language != "python":
                continue
            parts = list(PurePosixPath(path).with_suffix("").parts)
            if parts and parts[-1] == "__init__":
                parts.pop()
            names.setdefault(".".join(parts), item)
        return names

    def read_go_module(self) -> str | None:
        """The root ``go.mod`` module path: ``None`` when there is no go.mod, ``""`` when there is
        one whose module path is unknown (unreadable, over the read cap, no ``module`` line)."""
        try:
            data = read_worktree_file(self.scan, "go.mod", _GO_MOD_BYTES)
        except ScanError:
            return ""
        if data is None:
            return None
        if len(data) > _GO_MOD_BYTES:
            return ""
        for line in data.decode("utf-8", "replace").splitlines():
            found = _GO_MODULE_LINE.match(line)
            if found:
                return found.group(1)
        return ""

    def resolve_go(
        self,
        source: _File,
        reference: ParsedReference,
        directories: Mapping[str, list[_File]],
    ) -> _File | None:
        """The package an import path names: by the ``go.mod`` module path, else by directory.

        With a module path, an import equal to it or below it (``module/rest``) is the directory
        ``rest``. Only without one, the longest repository directory that is a ``/``-bounded suffix of
        the import path (standard-library paths, whose first element has no dot, never match).
        A directory holding only test files is not importable.
        """
        path = _go_import_path(reference, self.sources.get(source.path))
        module = self.go_module
        if module == "":
            return None  # a go.mod exists but its module path is unknown: never guess by suffix
        directory: str | None = None
        if module is not None and (path == module or path.startswith(module + "/")):
            directory = path[len(module) + 1 :] or "."
        elif module is None and "." in path.split("/", 1)[0]:
            matches = (
                candidate
                for candidate in directories
                if candidate != "." and (path == candidate or path.endswith("/" + candidate))
            )
            directory = max(matches, key=len, default=None)
        if directory is None:
            return None
        return next(
            (f for f in directories.get(directory, []) if not _is_go_test_file(f.path)), None
        )

    def resolve_script(
        self, source: _File, reference: ParsedReference
    ) -> tuple[_File | None, bool]:
        """The file a TS/JS relative import names, and whether it is the whole module.

        The adapter spells ``"./a/b"`` as dotted tokens: ``qualifier="a"`` + ``target_name="b"``
        (or ``qualifier="a.b"`` for a named import), ``relative_level`` 1 for ``./`` and one more
        per ``../``. The same shape is ``import {b} from "./a"`` or ``import b from "./a/b"``, so
        the module ``a`` wins when it has a symbol ``b``; otherwise ``a/b`` is the module. A bare
        specifier (level 0) is a package, never a repository file.
        """
        level = reference.relative_level
        if level == 0:
            return None, False
        base = _module_dir(source.path)
        for _ in range(level - 1):
            if base in ("", "."):
                return None, False
            base = str(PurePosixPath(base).parent)
        qualifier = (reference.qualifier or "").replace(".", "/")
        name = reference.target_name
        module = self.script_file(posixpath_join(base, qualifier)) if qualifier else None
        if module is not None and name in self.lookup(module).by_last:
            return module, False
        whole = self.script_file(posixpath_join(base, f"{qualifier}/{name}".lstrip("/")))
        if whole is not None:
            return whole, True
        if module is not None:
            return module, False
        if not qualifier:
            return self.script_file(base), False  # ``import {x} from "./"``: the directory index
        return None, False

    def script_file(self, joined: str | None) -> _File | None:
        """The file a relative module path names (extension optional, else a directory index)."""
        if joined is None:
            return None
        suffixes = ("", ".ts", ".tsx", ".js", ".jsx", ".mts", ".cts", ".mjs", ".cjs")
        for suffix in suffixes:
            if joined + suffix in self.files:
                return self.files[joined + suffix]
        for suffix in suffixes[1:]:
            index = f"{joined}/index{suffix}" if joined not in ("", ".") else f"index{suffix}"
            if index in self.files:
                return self.files[index]
        return None

    def subject_for(self, item: _File, source: str | None) -> str:
        if source is not None and source in item.refs:
            return str(item.refs[source].symbol_id)
        module = self.lookup(item).module
        return str(module.symbol_id) if module is not None else str(item.file_id)

    @staticmethod
    def lookup(item: _File) -> _SymbolIndex:
        if item.lookup is None:
            item.lookup = _SymbolIndex.build(item.symbols)
        return item.lookup

    def bind_import(
        self,
        item: _File,
        reference: ParsedReference,
        target: _File | None,
        whole: bool,
        bound: dict[str, list[_Binding]],
    ) -> None:
        subject = self.subject_for(item, reference.source)
        extractor = (item.extractor_name, item.extractor_version)
        if target is None:
            top = _external_name(item.language, reference, self.sources.get(item.path))
            if top is None:
                self.diagnostics["unresolved_relative_import"] += 1
                return
            self.dependency(
                item,
                self.module_id(item.path),
                top,
                DependencyObservationKind.OBSERVED,
                InferenceEvidenceKind.TREE_SITTER,
                extractor,
            )
            return
        wanted = self.bind_names(item, reference, target, whole, bound)
        if whole:
            obj = self.subject_for(target, None)
        else:
            # An imported name is a module-level symbol: never a method that shares its name.
            named = self.owned(target, wanted, [])
            if not named:
                return  # no module-level match: no internal import relation is asserted
            obj = str(named[0].symbol_id)
        self.claim(
            item,
            subject,
            CodeRelationPredicate.IMPORTS,
            obj,
            InferenceEvidenceKind.TREE_SITTER,
            SYNTACTIC_CONFIDENCE,
            *extractor,
        )

    def bind_names(
        self,
        item: _File,
        reference: ParsedReference,
        target: _File,
        whole: bool,
        bound: dict[str, list[_Binding]],
    ) -> str:
        """Bind the local name(s) an import introduces; return the imported name.

        Python: ``import a.b`` binds ``a`` (the qualifier must then consume ``b``);
        ``import a.b as c`` binds ``c`` to ``a.b``; ``from a import b`` binds ``b`` (or its alias),
        to a module when ``a.b`` is one and to a symbol of ``a`` otherwise. Go binds the
        package's last path element (or its alias); other languages bind the name as written.
        """
        place = (reference.start_byte, reference.source)
        if item.language == "go":
            # A Go import path is slash-separated (its first element is a host with dots).
            name = reference.target_name.rsplit("/", 1)[-1]
            bound.setdefault(reference.alias or name, []).append(_Binding(target, (), None, *place))
            if (
                reference.qualifier and not reference.alias
            ):  # an unaliased import binds its last token
                bound.setdefault(reference.qualifier.split(".")[-1], []).append(
                    _Binding(target, (), None, *place)
                )
            return name
        if item.language != "python":
            # TS/JS: the adapter reports the local name as ``alias``. A whole-module import binds
            # only that name (a side-effect import or ``export *`` binds nothing); a named
            # import binds its alias, or the imported name, to that symbol of the module.
            if whole:
                if reference.alias:
                    bound.setdefault(reference.alias, []).append(_Binding(target, (), None, *place))
            else:
                local = reference.alias or reference.target_name
                bound.setdefault(local, []).append(
                    _Binding(target, (), reference.target_name, *place)
                )
            return reference.target_name
        name = reference.target_name.split(".")[-1]
        if reference.qualifier or reference.relative_level:
            # ``from ... import name [as alias]``
            local = reference.alias or reference.target_name
            symbol = None if whole else reference.target_name
            bound.setdefault(local, []).append(_Binding(target, (), symbol, *place))
        elif whole:
            # ``import a.b [as c]``
            parts = reference.target_name.split(".")
            if reference.alias:
                bound.setdefault(reference.alias, []).append(_Binding(target, (), None, *place))
            else:
                bound.setdefault(parts[0], []).append(
                    _Binding(target, tuple(parts[1:]), None, *place)
                )
        return name

    def bind_symbol_reference(
        self, item: _File, reference: ParsedReference, bound: Mapping[str, list[_Binding]]
    ) -> None:
        """A call or inherit, resolved only through a name the file itself binds.

        Adapters report just the syntactic head of a qualifier and track no locals or parameters,
        so a qualifier is never taken for a module because it is written. Its FIRST segment must
        be bound in this file: by an import, or by a module-level symbol here. The rest of the
        qualifier (after any dotted import path the binding consumes) is the owner chain the
        target must sit under. Anything else is dropped and counted (``unbound_qualifier``),
        never resolved and never a dependency.
        """
        # Python spells ``a.b.f()`` as qualifier ``a.b`` + name ``f``; TS/JS as the name ``a.b.f``.
        full = (reference.qualifier.split(".") if reference.qualifier else []) + (
            reference.target_name.split(".")
        )
        wanted, parts = full[-1], full[:-1]
        target_file: _File | None
        chain: list[str]
        if not parts:
            binding, ambiguous = self.pick_binding(bound.get(wanted, []), [], reference)
            if ambiguous:
                self.diagnostics["ambiguous_reference"] += 1
                return
            if binding is None and item.language in _PACKAGE_SCOPED_LANGUAGES:
                package = self.package_candidates(item, wanted)
                if len(package) == 1:
                    self.call_or_reference(item, reference, package[0])
                else:
                    self.diagnostics[
                        "ambiguous_reference" if package else "unresolved_reference"
                    ] += 1
                return
            if binding is None:
                self.diagnostics["unresolved_reference"] += 1
                return
            target_file, chain = binding.file, []
            if binding.symbol is not None:
                wanted = binding.symbol
        else:
            binding, ambiguous = self.pick_binding(bound.get(parts[0], []), parts[1:], reference)
            if ambiguous:
                self.diagnostics["ambiguous_reference"] += 1
                return
            if binding is not None:
                target_file = binding.file
                chain = ([binding.symbol] if binding.symbol else []) + parts[
                    1 + len(binding.consumed) :
                ]
            elif parts[0] in self.lookup(item).module_level:
                target_file, chain = item, parts  # ``Local.method``: keep the owner chain
            else:
                self.diagnostics["unbound_qualifier"] += 1
                return
        if target_file.language in _PACKAGE_SCOPED_LANGUAGES and target_file is not item:
            # A package's top-level names only: a chain names a type, whose methods are not indexed.
            candidates = [] if chain else self.package_candidates(target_file, wanted)
        else:
            candidates = self.owned(target_file, wanted, chain)
        if len(candidates) != 1:
            self.diagnostics["ambiguous_reference" if candidates else "unresolved_reference"] += 1
            return
        self.call_or_reference(item, reference, candidates[0])

    @staticmethod
    def pick_binding(
        options: Sequence[_Binding], rest: list[str], reference: ParsedReference
    ) -> tuple[_Binding | None, bool]:
        """The binding a reference sees, and whether rebinding makes that ambiguous.

        Longest consumed dotted prefix first. Among equally specific bindings of one name that
        stand for different things (``from a import f`` then ``from b import f``), Python and
        JS use the LATEST binding before the use: an import inside the reference's own scope
        shadows a module-level one, and one in another function is invisible. A module-level
        import AFTER the use can still win when the use is inside a function (it runs later), so
        then a different later binding makes the reference ambiguous, as does having no binding
        before it. Conditional imports are not visible to adapters: the latest textual one wins.
        """
        fitting = [b for b in options if rest[: len(b.consumed)] == list(b.consumed)]
        scope = reference.source
        fitting = [b for b in fitting if b.scope is None or b.scope == scope]
        if not fitting:
            return None, False
        top = max(len(b.consumed) for b in fitting)
        fitting = [b for b in fitting if len(b.consumed) == top]
        if len({(id(b.file), b.symbol) for b in fitting}) == 1:
            return fitting[0], False
        at = reference.start_byte
        local = [b for b in fitting if b.scope is not None and b.at < at]
        if local:
            return max(local, key=lambda b: b.at), False
        module = [b for b in fitting if b.scope is None]
        before = [b for b in module if b.at < at]
        if not before:
            return None, True
        latest = max(before, key=lambda b: b.at)
        if scope is not None and any(
            b.at > at and (id(b.file), b.symbol) != (id(latest.file), latest.symbol) for b in module
        ):
            return None, True
        return latest, False

    def owned(self, target: _File, wanted: str, chain: list[str]) -> list[_Sym]:
        """Symbols named ``wanted`` in ``target`` whose owner chain is exactly ``chain``.

        The chain sits directly under the file's module (no module symbol: a suffix match), so
        ``mod.A.run()`` finds ``A.run`` and never ``B.run``, and ``mod.f()`` a module-level ``f``.
        """
        index = self.lookup(target)
        base = index.module_tokens
        found: list[_Sym] = []
        for symbol, owner in index.owners.get(wanted, []):
            if symbol.kind == "module":
                continue
            if base is not None:
                matches = owner == [*base, *chain]
            else:
                matches = owner[len(owner) - len(chain) :] == chain and len(owner) >= len(chain)
            if matches:
                found.append(symbol)
        return found

    def package_candidates(self, item: _File, wanted: str) -> list[_Sym]:
        """Top-level symbols named ``wanted`` anywhere in ``item``'s package (methods excluded).

        The package's names are indexed once per directory, not rescanned per reference.
        """
        # A package's non-test files share one name table; a test file gets its own (it also sees
        # the sibling test files). Either way the table is built once, on first use.
        key = ("test:" if _is_go_test_file(item.path) else "") + _module_dir(item.path)
        names = self.package_names.get(key)
        if names is None:
            names = {}
            for member in self.scope_files(item):
                index = self.lookup(member)
                for name, symbols in index.by_last.items():
                    if index.module is not None and name not in index.module_level:
                        continue
                    names.setdefault(name, []).extend(s for s in symbols if s.kind != "module")
            self.package_names[key] = names
        return names.get(wanted, [])

    def call_or_reference(self, item: _File, reference: ParsedReference, target: _Sym) -> None:
        heuristic = reference.kind == "call"
        self.claim(
            item,
            self.subject_for(item, reference.source),
            CodeRelationPredicate.CALLS if heuristic else CodeRelationPredicate.REFERENCES,
            str(target.symbol_id),
            InferenceEvidenceKind.TREE_SITTER,
            HEURISTIC_CONFIDENCE if heuristic else SYNTACTIC_CONFIDENCE,
            item.extractor_name,
            item.extractor_version,
        )

    # --- SCIP relations ---

    def scip_relations(self) -> None:
        semantic = self.semantic
        if semantic is None:
            return
        known = self.symbol_ids
        for document in self.scip_documents.values():
            item = self.files.get(document.path)
            source = self.sources.get(document.path)
            if item is None or source is None:
                continue
            starts = _line_starts(source)
            lines: _Lines = {}
            for occurrence in document.occurrences:
                if occurrence.is_definition or str(occurrence.symbol_id) not in known:
                    continue
                offsets = _range_offsets(
                    source, starts, occurrence.range, document.position_encoding, lines
                )
                if offsets is None:
                    self.diagnostics["invalid_position"] += 1
                    self.lose(item.path, CodeCoverageLoss.INVALID_POSITION)
                    continue  # no claim without a valid location
                subject = self.enclosing(item, offsets)
                predicate = (
                    CodeRelationPredicate.IMPORTS
                    if occurrence.roles & _SCIP_IMPORT_ROLE
                    else CodeRelationPredicate.REFERENCES
                )
                self.claim(
                    item,
                    subject,
                    predicate,
                    str(occurrence.symbol_id),
                    DeterministicEvidenceKind.SCIP,
                    1.0,
                    semantic.run.tool_name,
                    semantic.run.tool_version or "unknown",
                )
            for symbol in document.symbols:
                for relation in symbol.relationships:
                    if str(relation.source_id) in known and str(relation.target_id) in known:
                        self.claim(
                            item,
                            str(relation.source_id),
                            CodeRelationPredicate.REFERENCES,
                            str(relation.target_id),
                            DeterministicEvidenceKind.SCIP,
                            1.0,
                            semantic.run.tool_name,
                            semantic.run.tool_version or "unknown",
                        )

    def enclosing(self, item: _File, offsets: tuple[int, int]) -> str:
        if item.spans is None:  # built once per file, then one bisect per occurrence
            item.spans = _Intervals(
                (s.start, s.end, s) for s in item.symbols if s.kind != "module" and s.end > s.start
            )
        inside = item.spans.innermost(*offsets)
        return self.subject_for(item, None) if inside is None else str(inside.symbol_id)

    # --- assembling ---

    def relation_drafts(self) -> list[EventDraftV1]:
        drafts: list[EventDraftV1] = []
        for claim in sorted(self.claims.values(), key=_Claim.sort_key):
            deterministic = isinstance(claim.evidence, DeterministicEvidenceKind)
            assertion_id = _assertion_id(
                "relation",
                claim.subject,
                claim.predicate.value,
                claim.obj,
                claim.evidence.value,
                claim.extractor_name,
                claim.extractor_version,
                claim.revision,
            )
            self.observe(assertion_id, CodeAssertionFamily.RELATION, claim.file_id, claim.revision)
            payload = CodeRelationAssertedV1(
                assertion_id=assertion_id,
                evidence_kind=claim.evidence,
                deterministic=deterministic,
                extractor_name=claim.extractor_name,
                extractor_version=claim.extractor_version,
                confidence=claim.confidence,
                valid_from=self.now,
                index_id=self.index_id,
                subject_id=claim.subject,
                predicate=claim.predicate,
                object_id=claim.obj,
            )
            drafts.append(self.draft("code.relation.asserted", payload))
        return drafts

    def dependency_drafts(self) -> list[EventDraftV1]:
        drafts: list[EventDraftV1] = []
        for key, payload in sorted(self.dependencies.items()):
            source = self.dependency_sources[key]
            self.observe(
                payload.assertion_id,
                CodeAssertionFamily.DEPENDENCY,
                str(source.file_id),
                str(source.revision_id),
            )
            drafts.append(self.draft("code.dependency.asserted", payload))
        return drafts

    def observe(
        self, assertion_id: str, family: CodeAssertionFamily, file_id: str, revision: str
    ) -> None:
        """Membership: ``assertion_id`` was produced by source file revision ``revision``."""
        payload = CodeAssertionObservedV1(
            **self.target(),
            index_id=self.index_id,
            file_id=file_id,
            file_revision_id=revision,
            assertion_id=assertion_id,
            assertion_family=family,
        )
        self.add(self.observation_events, self.draft("code.assertion.observed", payload))

    def coverage_drafts(self) -> list[EventDraftV1]:
        """One per file of the membership: complete, or the sorted unique losses of its revision."""
        drafts: list[EventDraftV1] = []
        for path in sorted(self.files):
            item = self.files[path]
            losses = tuple(sorted(self.file_losses.get(path, ())))
            payload = CodeFileCoverageReportedV1(
                **self.target(),
                index_id=self.index_id,
                file_id=str(item.file_id),
                file_revision_id=str(item.revision_id),
                complete=not losses,
                losses=losses,
            )
            drafts.append(self.draft("code.file.coverage_reported", payload))
        drafts.extend(self.unindexed_coverage())
        return drafts

    def unindexed_coverage(self) -> list[EventDraftV1]:
        """``not_indexed`` for a clean tracked file that never reached emission and has a revision.

        Only a file unchanged against HEAD qualifies: its revision comes from the committed blob
        OID and lineage, the very inputs ``admit`` would have used, with no worktree bytes needed.
        """
        drafts: list[EventDraftV1] = []
        for path in sorted(self.tracked):
            tracked = self.tracked[path]
            language = registry.language_for_path(path)
            support = None if language is None else registry.support_for(language)
            file_id = self.lineage.get(path)
            if (
                path in self.files
                or tracked.kind != FileKind.FILE
                or support is None
                or file_id is None
                or tracked.oid is None
                or tracked.change is not None
                or path in self.modified
            ):
                continue
            revision = file_revision_id(
                self.repository_id,
                file_id,
                tracked.oid,
                registry.label_for_path(path) or support.language,
                support.parser_fingerprint,
            )
            payload = CodeFileCoverageReportedV1(
                **self.target(),
                index_id=self.index_id,
                file_id=str(file_id),
                file_revision_id=str(revision),
                complete=False,
                losses=(CodeCoverageLoss.NOT_INDEXED,),
            )
            drafts.append(self.draft("code.file.coverage_reported", payload))
        return drafts

    def supersession_drafts(self) -> list[EventDraftV1]:
        drafts: list[EventDraftV1] = []
        for item in sorted(
            self.supersessions, key=lambda s: (str(s.new_logical_id), str(s.old_logical_id))
        ):
            if item.confidence != 1.0 or item.evidence_kind != "git":
                self.diagnostics["supersession_dropped_sdk_confidence"] += 1
                continue
            payload = CodeRelationAssertedV1(
                assertion_id=_assertion_id(
                    "supersedes", str(item.new_logical_id), str(item.old_logical_id), item.basis
                ),
                evidence_kind=DeterministicEvidenceKind.GIT,
                deterministic=True,
                extractor_name=INDEXER_NAME,
                extractor_version=EMITTER_VERSION,
                confidence=1.0,
                valid_from=self.now,
                index_id=self.index_id,
                subject_id=str(item.new_logical_id),
                predicate=CodeRelationPredicate.POSSIBLY_SUPERSEDES,
                object_id=str(item.old_logical_id),
            )
            drafts.append(self.draft("code.relation.asserted", payload))
        return drafts

    def outcome_error_class(self) -> str | None:
        """Why this index is incomplete: a file was skipped before emission or degraded.

        A ``scan_incomplete``/``snapshot_incomplete`` outcome forbids all absence inference for the
        target; any other absence needs a ``complete=true`` ``code.file.coverage_reported``.
        """
        for path in [*self.tracked, *self.untracked]:  # scanned files that could not be indexed
            if path not in self.files and registry.label_for_path(path) is not None:
                self.diagnostics["file_not_indexed"] += 1
        if self.scan_hides_sources():
            return "scan_incomplete"
        if not self.snapshot_complete:
            return "snapshot_incomplete"
        if any(self.diagnostics.get(code) for code in _SKIP_DIAGNOSTICS):
            return "files_skipped"
        if any(self.diagnostics.get(code) for code in _DEGRADED_DIAGNOSTICS):
            return "files_degraded"
        return None

    def scan_hides_sources(self) -> bool:
        """Whether the scan may have left out a source file: then absence proves nothing.

        Per scanner reason: ``max_files`` omits paths outright (they are in neither ``files`` nor
        ``untracked``), so it always hides. A path skipped for size, budget or a read race is
        still listed, and a listed registered-language file that did not reach emission is
        already ``files_skipped``. A rejected path (unsafe or undecodable) is unknown unless its
        name says it is not a registered language. Symlinks, gitlinks and nested repositories are
        not this repository's source text.
        """
        scan = self.scan
        if scan.omitted_files or TruncationReason.MAX_FILES in scan.truncation_reasons:
            self.diagnostics["scan_files_omitted"] += scan.omitted_files
            return True
        hidden = [r for r in scan.rejections if r.path is None or registry.label_for_path(r.path)]
        if hidden:
            self.diagnostics["scan_rejected_path"] += len(hidden)
        return bool(hidden)

    def scip_applies(self, path: str) -> bool:
        """SCIP saw the file at the SCIP commit; it binds the target only if the bytes are those.

        A tracked path with no change against HEAD has the commit's content. A modified,
        staged or untracked path does not, so ALL its SCIP evidence is dropped and counted.
        """
        tracked = self.tracked.get(path)
        if tracked is not None and tracked.change is None and self.at_commit(path, tracked):
            return True
        self.diagnostics["scip_file_not_at_commit"] += 1
        self.lose(path, CodeCoverageLoss.SCIP_EVIDENCE_DROPPED)
        return False

    def at_commit(self, path: str, tracked: TrackedFile) -> bool:
        """Whether the observed bytes ARE the commit's, decided by content and never by status.

        Porcelain status can call a path clean while the worktree differs from the index (a
        racy edit, a clean/smudge filter, EOL conversion): the scanner lists such a path in
        ``modified_paths``. For those the observed bytes' blob must equal the index blob, which
        is HEAD's for a path with no status; unread or unreadable bytes prove nothing.
        """
        if path not in self.modified:
            return True
        content = self.sources.get(path)
        if content is None or tracked.oid is None:
            return False
        return blob_oid(content, self.scan.object_format) == tracked.oid

    def build(self, started_at: float) -> list[EventDraftV1]:
        semantic = self.semantic
        everything = {} if semantic is None else {d.path: d for d in semantic.documents}
        from_scip = {
            d.path: d.file_logical_id for d in everything.values() if d.file_logical_id is not None
        }
        if semantic is not None:
            for dropped in semantic.diagnostics:  # the importer dropped this evidence: say so
                self.diagnostics["scip_evidence_dropped"] += 1
                self.diagnostics[f"scip_import_{dropped.code}"] += 1
                if dropped.path is not None:  # an unsafe path names no file of the membership
                    self.lose(dropped.path, CodeCoverageLoss.SCIP_EVIDENCE_DROPPED)
        documents = {path: d for path, d in everything.items() if self.scip_applies(path)}
        self.scip_documents = documents
        self.lineage = {**from_scip, **self.lineage}  # built once; the caller's lineage wins
        for path in sorted(self.structural):
            self.index_structural(self.structural[path], documents.get(path))
        for path in sorted(documents):
            if path not in self.structural:
                self.index_scip_only(documents[path])
        self.resolve_references()
        self.scip_relations()
        relations = self.relation_drafts()
        dependencies = self.dependency_drafts()
        supersessions = self.supersession_drafts()
        coverage = self.coverage_drafts()
        body = [
            *self.file_events,
            *self.symbol_events,
            *relations,
            *dependencies,
            *supersessions,
            *self.observation_events,
            *coverage,
        ]
        common = {**self.target(), "index_id": self.index_id}
        started = CodeIndexStartedV1(
            **common,
            indexer_name=INDEXER_NAME,
            indexer_version=EMITTER_VERSION,
            configuration_sha256=self.configuration_sha256(),
        )
        error_class = self.outcome_error_class()
        completed = CodeIndexCompletedV1(
            **common,
            indexer_name=INDEXER_NAME,
            indexer_version=EMITTER_VERSION,
            configuration_sha256=self.configuration_sha256(),
            success=error_class is None,
            error_class=error_class,
            file_count=len(self.file_events),
            symbol_count=len(self.symbol_events),
            relation_count=len(relations) + len(supersessions),
            dependency_count=len(dependencies),
            duration_ms=max(0, int((self.config.monotonic() - started_at) * 1000)),
        )
        if self.diagnostics:
            _LOG.info("code index diagnostics %s", dict(sorted(self.diagnostics.items())))
        return [
            self.draft("code.index.started", started),
            *body,
            self.draft("code.index.completed", completed),
        ]


# Adapter diagnostics that are a per-file loss; ``syntax_recovered`` only reports recovery.
_ADAPTER_LOSSES: Final = {
    code: CodeCoverageLoss(code) for code in DIAGNOSTIC_CODES if code != "syntax_recovered"
}
_SKIP_DIAGNOSTICS: Final = (
    "source_missing",
    "source_mismatch",
    "file_not_scanned",
    "file_without_lineage",
    "unsupported_language",
    "file_not_indexed",
)
# Evidence that was dropped rather than emitted: a success must never hide it.
_DEGRADED_DIAGNOSTICS: Final = (
    # EVERY adapter loss code (``syntax_recovered`` only reports recovery, nothing was lost).
    *(f"adapter_{code}" for code in DIAGNOSTIC_CODES if code != "syntax_recovered"),
    "invalid_position",
    "scip_file_not_at_commit",
    "scip_evidence_dropped",
)

# The claim of an event is EVERYTHING it says except its observation fields: the envelope
# (event type, schema version, stream, producer, trace, redaction) and the payload minus the
# per-type fields below. ``context`` (checkout, workspace, project, session) and the envelope
# times are observations of every code event: WHERE and WHEN it was seen, not what it claims.
# The idempotency key is the digest of the claim and ``_same_claim`` compares the very same view,
# so a stored event under a key can differ from a draft only by an observation (a replay) or by
# a digest collision (a bug, surfaced as ``idempotency_conflict``).
#
# - ``code.file.indexed``: none. ``index_id`` and the target ARE the claim (membership per target).
# - ``code.assertion.observed``: ``index_id`` and the target (membership per file revision).
# - ``code.file.coverage_reported``: none. Coverage is per target (``index_id`` is in the claim).
# - ``code.symbol.indexed``, ``code.relation.asserted``, ``code.dependency.asserted``: the first
#   observation's ``index_id``, target and ``valid_from``. Their claim carries the file
#   (symbols: ``file_id`` and ``file_revision_id``; relations and dependencies: the source file
#   revision, inside ``assertion_id``), so membership follows file revisions.
# - ``code.index.started``: none. ``code.index.completed``: ``duration_ms`` only; the outcome
#   (``success``, ``error_class`` and the counts) is the claim.
OBSERVATION_FIELDS: Final[Mapping[str, frozenset[str]]] = {
    "code.file.indexed": frozenset(),
    "code.symbol.indexed": frozenset({"index_id", "commit_id", "snapshot_id"}),
    "code.relation.asserted": frozenset({"index_id", "valid_from"}),
    "code.dependency.asserted": frozenset({"index_id", "valid_from"}),
    "code.assertion.observed": frozenset({"index_id", "commit_id", "snapshot_id"}),
    "code.file.coverage_reported": frozenset(),
    "code.index.started": frozenset(),
    "code.index.completed": frozenset({"duration_ms"}),
}
_CLAIM_ENVELOPE: Final = {
    "event_type",
    "schema_version",
    "stream_id",
    "producer",
    "trace",
    "payload",
    "redaction",
}


def _claim_view(event: EventDraftV1 | StoredEventV1) -> dict[str, Any]:
    view: dict[str, Any] = event.model_dump(mode="json", include=set(_CLAIM_ENVELOPE))
    observed = OBSERVATION_FIELDS[event.event_type]
    view["payload"] = {k: v for k, v in view["payload"].items() if k not in observed}
    return view


def claim_key(event: EventDraftV1 | StoredEventV1) -> str:
    """The idempotency key of ``event``: its type and the digest of its claim."""
    return f"{event.event_type}:{_digest(canonical_json_bytes(_claim_view(event)).decode())}"


def _differing(draft: EventDraftV1, stored: StoredEventV1) -> list[str]:
    """Names of the claim fields that differ (never their values, which may be sensitive)."""
    a, b = _claim_view(draft), _claim_view(stored)
    names = [key for key in a if key != "payload" and a[key] != b[key]]
    names += [
        f"payload.{k}"
        for k in a["payload"].keys() | b["payload"].keys()
        if a["payload"].get(k) != b["payload"].get(k)
    ]
    return sorted(names)


def _same_claim(draft: EventDraftV1, stored: StoredEventV1) -> bool:
    """Whether ``stored`` is the same claim as ``draft``, ignoring only observation fields."""
    return _claim_view(draft) == _claim_view(stored)


def _assertion_id(kind: str, *parts: str) -> str:
    return f"as-{kind}-{_digest(kind, *parts)[:48]}"


INDEXER_PRODUCER: Final = ProducerV1(
    producer_id=INDEXER_PRODUCER_ID, name=INDEXER_NAME, version=EMITTER_VERSION
)


_SCIP_IMPORT_ROLE: Final = 2


def posixpath_join(base: str, relative: str) -> str | None:
    """``base`` + ``relative`` normalised, or ``None`` when it climbs out of the repository."""
    parts: list[str] = [] if base in ("", ".") else base.split("/")
    for part in relative.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return None
            parts.pop()
        else:
            parts.append(part)
    return "/".join(parts)


def _go_import_path(reference: ParsedReference, source: bytes | None) -> str:
    """The full import path: the literal's content (the qualifier's range), else the last token."""
    if source is not None and reference.qualifier_start_byte is not None:
        raw = source[reference.qualifier_start_byte : reference.qualifier_end_byte]
        try:
            text = raw.decode()
        except UnicodeDecodeError:
            return reference.target_name
        if text and not any(char.isspace() for char in text):
            return text
    return reference.target_name


def _external_name(
    language: str, reference: ParsedReference, source: bytes | None = None
) -> str | None:
    """The package an unresolved import names, or ``None`` for an unresolved relative one."""
    name = reference.target_name
    if reference.relative_level or name.startswith("."):
        return None
    if language == "go":
        return _go_import_path(reference, source)
    tokens = ((reference.qualifier + ".") if reference.qualifier else "") + name
    parts = tokens.split(".")
    if language in ("typescript", "javascript") and source is not None and len(parts) >= 2:
        # The adapter drops the ``@`` of ``@scope/pkg``: its tokens are ``scope`` and ``pkg``.
        first = reference.qualifier_start_byte if reference.qualifier else reference.start_byte
        # A whole-module token starts after the ``@``; a named import's qualifier is the whole
        # specifier literal, which starts at it.
        if first is not None and b"@" in (source[first - 1 : first], source[first : first + 1]):
            return f"@{parts[0]}/{parts[1]}"
    return parts[0]  # Python and TS/JS: dotted tokens


def read_sources(scan: RepositoryScan) -> dict[str, bytes]:
    """Bytes of scanned regular files with a registered language, read safely.

    Each file is opened by the scanner's component-wise ``O_NOFOLLOW`` walk (a directory turned
    into a symlink after the scan is refused, a FIFO leaf cannot block) and must still match the
    scan: same size and SHA-256. A swapped, unreadable or unsafe file is skipped.
    """
    out: dict[str, bytes] = {}
    candidates: list[tuple[str, FileKind, object, int | None, str | None]] = [
        *((f.path, f.kind, f.skipped, f.size, f.content_sha256) for f in scan.files),
        *((f.path, f.kind, f.skipped, f.size, f.content_sha256) for f in scan.untracked),
    ]
    candidates.sort(key=lambda entry: entry[0])
    for path, kind, skipped, size, digest in candidates:
        if kind != FileKind.FILE or skipped is not None or registry.language_for_path(path) is None:
            continue
        try:
            data = read_worktree_file(scan, path, MAX_SOURCE_BYTES)
        except ScanError:
            continue
        if data is None:
            continue
        checked = size is not None and size <= MAX_SOURCE_BYTES  # oversized: counted later
        if checked and (len(data) != size or (digest is not None and _sha256(data) != digest)):
            continue
        out[path] = data
    return out


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class IndexingService:
    """Turns scan, SCIP and structural evidence into events and submits them in process."""

    def __init__(
        self,
        config: IndexingConfig,
        ingestion: IngestionService | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self.config = config
        self._ingestion = ingestion
        self._session_factory = session_factory

    def index(
        self,
        scan: RepositoryScan,
        semantic: SemanticIndex | None,
        structural: Sequence[ParsedFile],
        *,
        file_logical_ids: Mapping[str, uuid.UUID],
        supersessions: Iterable[Supersession] = (),
        sources: Mapping[str, bytes] | None = None,
    ) -> list[EventDraftV1]:
        """Drafts for one target: started, files, symbols, relations, dependencies, completed.

        ``file_logical_ids`` is the ``identity`` lineage (path to logical ID); pass the same
        mapping to ``scip.import_scip``. ``sources`` defaults to reading the scanned files.
        """
        started = self.config.monotonic()
        loaded = read_sources(scan) if sources is None else sources
        run = _Run(self, scan, semantic, structural, loaded, file_logical_ids, supersessions)
        return run.build(started)

    async def ingest(self, drafts: Sequence[EventDraftV1]) -> IndexingOutcome:
        """Submit ``drafts`` not already in the ledger, in batches of at most 500 events."""
        if self._ingestion is None or self._session_factory is None:
            raise IndexingError("ingestion_not_configured")
        keys = [(draft.producer.producer_id, draft.idempotency_key) for draft in drafts]
        stored: dict[tuple[str, str], StoredEventV1] = {}
        async with self._session_factory() as session:
            for start in range(0, len(keys), MAX_BATCH_EVENTS):
                stored.update(
                    await LedgerRepository.get_by_idempotency_keys(
                        session, keys[start : start + MAX_BATCH_EVENTS]
                    )
                )
        fresh: list[EventDraftV1] = []
        conflicts = 0
        for draft in drafts:
            found = stored.get((draft.producer.producer_id, draft.idempotency_key))
            if found is None:
                fresh.append(draft)
            elif not _same_claim(draft, found):
                conflicts += 1
                _LOG.warning("conflicting %s: %s", draft.event_type, _differing(draft, found))
        if conflicts:
            # The same key now says something materially different: never drop it silently and
            # never submit a partial index. Nothing was written.
            _LOG.warning("code index idempotency conflicts: %d", conflicts)
            raise IndexingError("idempotency_conflict")
        for start in range(0, len(fresh), MAX_BATCH_EVENTS):
            batch = IngestBatchRequestV1(
                batch_id=new_uuid7(), events=tuple(fresh[start : start + MAX_BATCH_EVENTS])
            )
            outcome = await self._ingestion.ingest(batch)
            if outcome.http_status >= 300:
                raise IndexingError(f"ingest_rejected_{outcome.http_status}")
        return IndexingOutcome(tuple(drafts), len(fresh), len(drafts) - len(fresh))
