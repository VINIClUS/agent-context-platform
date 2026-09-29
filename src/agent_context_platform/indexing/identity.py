"""Deterministic logical identities for files and symbols.

Pure functions, no I/O. Every ID is a UUIDv5 under a per-repository namespace,
itself a UUIDv5 of the canonical platform ``repository_id`` under a fixed root
namespace. A local path, hostname or checkout location never enters an ID, so
replicas and rebuilds derive the same IDs (design 10.1). The derivation is
frozen by ``tests/fixtures/indexing/rename_cases.json``.

Components are encoded as length-prefixed tuples (``encode_components``), the
same idea as the ``commit_node_id`` and ``branch_node_id`` helpers of the git
projector, so ``("a/b", "c")`` and ``("a", "b/c")`` cannot collide.

Derivation (design 9.1 and 10.1):

- File logical ID: ``(repository, lineage)``. A lineage starts at
  ``(origin path, introducing commit)``. The introducing commit is the commit on
  the first-parent history of the indexed ref that added the lineage's origin
  path (a ``git log --first-parent --diff-filter=A`` equivalent). It is intrinsic
  to the repository history, never to where an indexer started or first looked,
  so replicas and re-indexes agree. This library is pure and cannot read
  history: the indexer (PLATFORM-037/038) MUST compute the introducing commit
  from history and pass it as ``introducing_commit_oid``; passing a "first
  observed" commit is a contract violation. Rename-preserved lineages keep the
  original origin. It survives content edits and deterministic renames, which is why
  the ID is not recomputed from the current path.
- Provisional file logical ID: ``(checkout, path)`` in a distinct sub-namespace,
  for files that are not committed yet (agent-created, untracked or dirty-only).
  It can never equal a committed ID. Its revision is keyed by the worktree blob
  OID. When the file is committed, ``resolve_committed`` links provisional to
  committed with confidence 1.0, evidence ``git`` and basis ``provisional_committed``.
- File revision ID: ``(file logical ID, content blob OID, language, parser
  fingerprint)``; the fingerprint is a canonical digest of parser name, version
  and config supplied by PLATFORM-032/033-036.
- Symbol logical ID: the SCIP symbol string when it is global, otherwise
  ``(language, file logical ID, qualified name, kind, disambiguator)``.
- Symbol revision ID: ``(symbol logical ID, signature digest, semantic
  fingerprint)``; the fingerprint is a body or AST digest from the adapters.

Delete-then-recreate at the same path starts a new lineage: design 9.1 models
``File`` as a lineage node with immutable ``FileRevision`` children, and 10.1
defines the logical ID as "repository plus lineage identity" that survives only
"when evidence is sufficient". A path alone is not lineage evidence, so a
recreated file gets a new logical ID and a low-confidence ``path_reuse``
supersession back to the deleted one, never a fabricated continuation.
"""

from __future__ import annotations

import re
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final, Literal, get_args

from agent_context_platform.indexing.scanner import is_safe_repo_path

# Frozen: changing it re-identifies every graph node. It is
# uuid5(NAMESPACE_URL, "urn:agent-context:platform:code-identity:v1").
ROOT_NAMESPACE: Final = uuid.UUID("6ff9b492-0edb-50ba-bb24-6c063fa0a74f")

# Below 1: a blob match or path reuse is not proof of lineage (design 9.3).
AMBIGUOUS_BLOB_CONFIDENCE: Final = 0.5
PATH_REUSE_CONFIDENCE: Final = 0.25
SIMILARITY_MAX_CONFIDENCE: Final = 0.9

_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_DIGEST = re.compile(r"[0-9a-f]{64}")

# Design 9.3 assertion evidence kinds; every supersession here is Git-derived.
type EvidenceKind = Literal["git", "scip", "tree_sitter", "test", "user", "agent", "llm_inference"]
EVIDENCE_KINDS: Final = frozenset(get_args(EvidenceKind.__value__))
type SupersessionBasis = Literal[
    "ambiguous_blob_match",
    "path_reuse",
    "similarity",
    "provisional_committed",
    "provisional_path_match",
]


def encode_components(*components: str) -> str:
    """Encode a tuple as ``<utf-8 byte length>:<text>`` items; injective."""
    return "".join(f"{len(part.encode('utf-8'))}:{part}" for part in components)


def _repository_id(value: str) -> str:
    if not value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("repository_id must be non-empty without control characters")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("repository_id must be valid UTF-8 text") from None
    return value


def canonical_path(path: str) -> str:
    """Validate a repo-relative POSIX path and return it unchanged.

    Path bytes are identity-preserving: no Unicode normalization is applied, so
    NFC and NFD spellings are distinct files, exactly as Git tracks them. The
    scanner already validated the UTF-8 bytes Git reported; this refuses what the
    scanner refuses.
    """
    try:
        path.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("unsafe path: not valid UTF-8 text") from None
    if not is_safe_repo_path(path):
        raise ValueError("unsafe path: must be a relative POSIX path without '..' or '.git'")
    return path


def _oid(value: str) -> str:
    if not _OID.fullmatch(value):
        raise ValueError("oid must be a lowercase 40 or 64 character hex object id")
    return value


def _digest(value: str, what: str = "digest") -> str:
    if not _DIGEST.fullmatch(value):
        raise ValueError(f"{what} must be a lowercase 64 character hex SHA-256 digest")
    return value


def repository_namespace(repository_id: str) -> uuid.UUID:
    """Namespace of one repository, derived only from its canonical platform ID."""
    return uuid.uuid5(
        ROOT_NAMESPACE, encode_components("repository", _repository_id(repository_id))
    )


def _derive(repository_id: str, *components: str) -> uuid.UUID:
    return uuid.uuid5(repository_namespace(repository_id), encode_components(*components))


def file_logical_id(repository_id: str, origin_path: str, introducing_commit_oid: str) -> uuid.UUID:
    """Logical ID of a file lineage whose origin path was added by ``introducing_commit_oid``.

    The caller derives the introducing commit from first-parent history (see the
    module docstring); it is never the first commit an indexer happened to see.
    """
    return _derive(repository_id, "file", canonical_path(origin_path), _oid(introducing_commit_oid))


def provisional_file_logical_id(repository_id: str, checkout_id: str, path: str) -> uuid.UUID:
    """Identity of an uncommitted file in one checkout; disjoint from committed IDs."""
    namespace = uuid.uuid5(
        repository_namespace(repository_id), encode_components("namespace", "provisional")
    )
    return uuid.uuid5(
        namespace, encode_components(_repository_id(checkout_id), canonical_path(path))
    )


def file_revision_id(
    repository_id: str,
    file_logical_id: uuid.UUID,
    blob_oid: str,
    language: str,
    parser_fingerprint: str,
) -> uuid.UUID:
    """Revision of a file: logical ID, content blob OID, language and parser configuration.

    ``parser_fingerprint`` is a canonical lowercase SHA-256 of the parser name,
    version and configuration, supplied by the parser adapters
    (PLATFORM-032/033-036), so a parser or config change re-revisions the file
    (design 10.1: "content digest plus language and parser configuration").
    """
    if not language:
        raise ValueError("language must be non-empty")
    return _derive(
        repository_id,
        "file_revision",
        str(file_logical_id),
        _oid(blob_oid),
        language,
        _digest(parser_fingerprint, "parser fingerprint"),
    )


def provisional_file_revision_id(
    repository_id: str,
    provisional_id: uuid.UUID,
    worktree_blob_oid: str,
    language: str,
    parser_fingerprint: str,
) -> uuid.UUID:
    """Revision of an uncommitted file, keyed by its worktree blob OID (like committed)."""
    return file_revision_id(
        repository_id, provisional_id, worktree_blob_oid, language, parser_fingerprint
    )


def symbol_fallback_id(
    repository_id: str,
    language: str,
    file_logical_id: uuid.UUID,
    qualified_name: str,
    kind: str,
    disambiguator: str = "",
) -> uuid.UUID:
    """Symbol identity when no global SCIP symbol exists."""
    return _derive(
        repository_id,
        "symbol_fallback",
        language,
        str(file_logical_id),
        qualified_name,
        kind,
        disambiguator,
    )


def symbol_logical_id(
    repository_id: str,
    *,
    scip_symbol: str | None,
    language: str,
    file_logical_id: uuid.UUID,
    qualified_name: str,
    kind: str,
    disambiguator: str = "",
) -> uuid.UUID:
    """Prefer a global SCIP symbol string; ``local`` SCIP symbols are file-scoped."""
    if scip_symbol and not scip_symbol.startswith("local "):
        return _derive(repository_id, "symbol_scip", scip_symbol)
    return symbol_fallback_id(
        repository_id, language, file_logical_id, qualified_name, kind, disambiguator
    )


def symbol_revision_id(
    repository_id: str,
    symbol_id: uuid.UUID,
    signature_digest: str,
    semantic_fingerprint: str,
) -> uuid.UUID:
    """Revision of a symbol: its signature digest plus semantic fingerprint.

    ``semantic_fingerprint`` is a lowercase SHA-256 of the body or AST supplied
    by the adapters, so a body change with an unchanged signature is a new
    revision (design 10.1: "signature and semantic fingerprint").
    """
    return _derive(
        repository_id,
        "symbol_revision",
        str(symbol_id),
        _digest(signature_digest, "signature digest"),
        _digest(semantic_fingerprint, "semantic fingerprint"),
    )


@dataclass(frozen=True, slots=True)
class PriorFile:
    """A file that disappeared: its known logical ID, last path and blob OID."""

    path: str
    logical_id: uuid.UUID
    blob_oid: str


@dataclass(frozen=True, slots=True)
class NewFile:
    """A path that appeared, with its blob OID (as reported by the scanner)."""

    path: str
    blob_oid: str


@dataclass(frozen=True, slots=True)
class SimilarRename:
    """Similarity-based rename candidate, e.g. from ``git diff -M``; a guess, not proof."""

    old_path: str
    new_path: str
    score: float


@dataclass(frozen=True, slots=True)
class Supersession:
    """Supersession claim; confidence < 1 unless the evidence is direct (`provisional_committed`)."""

    old_logical_id: uuid.UUID
    new_logical_id: uuid.UUID
    confidence: float
    evidence_kind: EvidenceKind
    basis: SupersessionBasis


@dataclass(frozen=True, slots=True)
class RenameResolution:
    """Logical ID per added path, plus the supersession assertions."""

    logical_ids: Mapping[str, uuid.UUID] = field(default_factory=dict)
    supersessions: tuple[Supersession, ...] = ()


def resolve_rename(
    repository_id: str,
    introducing_commit_oid: str,
    removed: Iterable[PriorFile],
    added: Iterable[NewFile],
    similar: Iterable[SimilarRename] = (),
) -> RenameResolution:
    """Assign logical IDs to appeared paths.

    ``removed`` are files that disappeared and ``added`` paths that appeared
    between two states; ``introducing_commit_oid`` is the commit that added ``added`` on
    first-parent history (never "first observed"); it is the lineage origin of any
    new ID. The scanner runs
    git with ``--no-renames`` and reports no renames, so the only deterministic
    evidence is one removed and one added path with the identical blob OID: the
    logical ID moves to the new path.

    Every other case gets a new logical ID plus a supersession below confidence
    1: ``ambiguous_blob_match`` when a removed file shares the blob (one source to
    many targets, many to one), ``path_reuse`` when a removed file had the same
    path (delete then recreate), and ``similarity`` for each ``similar`` candidate
    (rename or copy with edits), with confidence ``min(score, 0.9)``; duplicate
    pairs keep the maximum score, so input order never matters. When the
    same pair is linked several ways, the last of those bases wins. A candidate
    for a path that was resolved deterministically is ignored.
    """
    prior = sorted(removed, key=lambda item: (canonical_path(item.path), str(item.logical_id)))
    appeared = sorted(added, key=lambda item: canonical_path(item.path))
    paths = [canonical_path(item.path) for item in appeared]
    if len(set(paths)) != len(paths):
        raise ValueError("duplicate added path")
    _oid(introducing_commit_oid)
    removed_by_oid: dict[str, list[PriorFile]] = defaultdict(list)
    added_by_oid: dict[str, list[str]] = defaultdict(list)
    for item in prior:
        removed_by_oid[_oid(item.blob_oid)].append(item)
    for path, appeared_file in zip(paths, appeared, strict=True):
        added_by_oid[_oid(appeared_file.blob_oid)].append(path)
    prior_ids = {canonical_path(item.path): item.logical_id for item in prior}
    if len(prior_ids) != len(prior):
        raise ValueError("duplicate removed path")
    if len({item.logical_id for item in prior}) != len(prior):
        raise ValueError("duplicate removed logical id")
    guesses: dict[str, dict[uuid.UUID, float]] = defaultdict(dict)
    for guess in similar:
        if not 0 < guess.score <= 1:
            raise ValueError("similarity score must be in (0, 1]")
        old_path, new_path = canonical_path(guess.old_path), canonical_path(guess.new_path)
        if old_path not in prior_ids or new_path not in paths:
            raise ValueError("similar rename must link a removed path to an added path")
        pair = guesses[new_path]
        clamped = min(guess.score, SIMILARITY_MAX_CONFIDENCE)
        # Duplicate (old, new) pairs keep the maximum score: order-independent.
        pair[prior_ids[old_path]] = max(pair.get(prior_ids[old_path], 0.0), clamped)

    logical_ids: dict[str, uuid.UUID] = {}
    supersessions: list[Supersession] = []
    for path, appeared_file in zip(paths, appeared, strict=True):
        sources = removed_by_oid.get(appeared_file.blob_oid, [])
        siblings = added_by_oid[appeared_file.blob_oid]
        if len(sources) == 1 and len(siblings) == 1 and canonical_path(sources[0].path) != path:
            logical_ids[path] = sources[0].logical_id
            continue
        new_id = file_logical_id(repository_id, path, introducing_commit_oid)
        logical_ids[path] = new_id
        linked: dict[uuid.UUID, tuple[SupersessionBasis, float]] = {}
        for source in sources:
            linked[source.logical_id] = ("ambiguous_blob_match", AMBIGUOUS_BLOB_CONFIDENCE)
        for old_id, confidence in guesses.get(path, {}).items():
            linked[old_id] = ("similarity", confidence)
        if path in prior_ids:
            linked[prior_ids[path]] = ("path_reuse", PATH_REUSE_CONFIDENCE)
        for old_id in sorted(linked, key=str):
            basis, confidence = linked[old_id]
            supersessions.append(Supersession(old_id, new_id, confidence, "git", basis))
    return RenameResolution(MappingProxyType(logical_ids), tuple(supersessions))


@dataclass(frozen=True, slots=True)
class ProvisionalFile:
    """An uncommitted file: its provisional logical ID, path and worktree blob OID."""

    path: str
    logical_id: uuid.UUID
    blob_oid: str


def resolve_committed(
    repository_id: str,
    introducing_commit_oid: str,
    provisional: Iterable[ProvisionalFile],
    committed: Iterable[NewFile],
) -> RenameResolution:
    """Give newly committed files committed IDs and link them to provisional ones.

    A provisional file committed at the same path with the same blob OID is
    directly evidenced: supersession confidence 1.0, evidence ``git``, basis
    ``provisional_committed``.
    Same path with a different blob (edited before commit) is only a path match:
    confidence 0.5. Committed files without a provisional counterpart get a new
    ID and no assertion. Duplicate provisional or committed paths raise
    ``ValueError``.
    """
    by_path: dict[str, ProvisionalFile] = {}
    for candidate in provisional:
        path = canonical_path(candidate.path)
        if path in by_path:
            raise ValueError("duplicate provisional path")
        by_path[path] = candidate
    logical_ids: dict[str, uuid.UUID] = {}
    supersessions: list[Supersession] = []
    for item in sorted(committed, key=lambda entry: canonical_path(entry.path)):
        path = canonical_path(item.path)
        if path in logical_ids:
            raise ValueError("duplicate committed path")
        new_id = file_logical_id(repository_id, path, introducing_commit_oid)
        logical_ids[path] = new_id
        source = by_path.get(path)
        if source is None:
            continue
        if _oid(source.blob_oid) == _oid(item.blob_oid):
            supersessions.append(
                Supersession(source.logical_id, new_id, 1.0, "git", "provisional_committed")
            )
        else:
            supersessions.append(
                Supersession(source.logical_id, new_id, 0.5, "git", "provisional_path_match")
            )
    return RenameResolution(MappingProxyType(logical_ids), tuple(supersessions))
