"""File lineage derived purely from first-parent Git history (design 9.1, 10.1).

``identity`` is the contract: a file logical ID is ``(repository, origin path, introducing
commit)`` and the introducing commit is the commit on the first-parent history of the indexed ref
that added the lineage's origin path. This module computes exactly that, so an indexer never has to
pass a "first observed" commit. Nothing is persisted: the lineage of a path is a function of the
repository history alone, so replicas, re-indexes and an indexer that starts from any commit agree.

Walk
----
The first-parent chain of the target commit is walked from its root, one ``git diff-tree`` pass
per commit (against the previous commit of the chain; the root against the empty tree), so memory is
the OID list (bounded by ``max_commits``) plus the tree being tracked, never the history. A merge
commit is one step on the chain: what the merge brings in was introduced BY the merge, as in
``git log --first-parent --diff-filter=A``. The history is never read beyond the chain.

Per commit the diff records are reduced to removed and added paths with their blob OIDs, and
``identity.resolve_rename`` decides the lineage. Git's own rename pairing is NOT used as evidence: a
rename record is split into its removal and its addition, so a lineage moves only when
``resolve_rename`` finds one removed and one added path with the identical blob (an exact rename),
independent of the Git version, ``diff.renameLimit`` or similarity heuristics. Git's similarity
score is passed only as a ``SimilarRename`` guess (confidence below 1). A copy is never a rename:
copy detection is off, so a copy is a plain addition.

A delete and a later recreate of the same path never appear in one diff, so deleted lineages are
carried forward and handed to ``resolve_rename`` as ``removed`` ONLY in the commit that re-adds their
path. They carry no usable blob (the zero OID), which keeps them from ever becoming a rename source:
the recreate is a new lineage plus a ``path_reuse`` supersession.

Safety and bounds
-----------------
Git runs against a private ``GIT_DIR`` with an allowlisted config (the scanner's view), so a hostile
``.git/config`` can run nothing. Only the first-parent chain is read; a gitlink (submodule) is
skipped, as the scanner does; unsafe paths are skipped, as the scanner does. A history longer than
``max_commits`` raises ``IndexingError("history_too_large")`` and a shallow clone raises
``IndexingError("shallow_history")``: lineage is never invented from incomplete history.
"""

from __future__ import annotations

import shutil
import tempfile
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

from agent_context_platform.indexing import identity
from agent_context_platform.indexing.emitter import IndexingError
from agent_context_platform.indexing.identity import (
    NewFile,
    PriorFile,
    ProvisionalFile,
    SimilarRename,
    Supersession,
)
from agent_context_platform.indexing.scanner import (
    RepositoryScan,
    ScanError,
    ScanFailure,
    ScanLimits,
    _base_env,
    _bootstrap,
    _fd_identity,
    _is_oid,
    _read_below,
    _Repo,
    _run_git,
    _safe_config,
    _safe_path,
    _split_z,
    _validate_repository_files,
)

DEFAULT_MAX_COMMITS: Final = 100_000

_GITLINK_MODE: Final = "160000"
_NO_BLOB: Final = "0" * 40
_LIST_LINE_BYTES: Final = 66
_METADATA_CAP: Final = 65_536
# Pinned, not left to the Git version: the pairing score is only a guess, but a stable one.
_RENAME_LIMIT: Final = "2000"

__all__ = [
    "DEFAULT_MAX_COMMITS",
    "FileOrigin",
    "Lineage",
    "derive_lineage",
]


@dataclass(frozen=True, slots=True)
class FileOrigin:
    """Where a lineage began: the origin path, the commit that added it and the blob it added."""

    origin_path: str
    introducing_commit: str
    origin_blob_oid: str


@dataclass(frozen=True, slots=True)
class Lineage:
    """The lineage of every regular file or symlink in the target commit's tree.

    ``file_logical_ids`` and ``origins`` are keyed by the CURRENT path. ``supersessions`` are the
    Git-derived claims whose new logical ID is live in the target tree.
    """

    repository_id: str
    target_commit: str | None
    file_logical_ids: Mapping[str, uuid.UUID]
    origins: Mapping[str, FileOrigin]
    supersessions: tuple[Supersession, ...]
    commits_walked: int

    def link_provisional(self, provisional: Iterable[ProvisionalFile]) -> tuple[Supersession, ...]:
        """``provisional_committed`` links for uncommitted files that the history now contains.

        A provisional file (``identity.uncommitted_file_logical_id``) is linked to the lineage
        whose ORIGIN path it sits at, through ``identity.resolve_committed`` with that lineage's
        introducing commit and origin blob: the same blob is direct evidence (confidence 1.0), an
        edited one only a path match. A provisional file at no lineage origin has no link.
        """
        by_origin = {
            origin.origin_path: (path, origin) for path, origin in sorted(self.origins.items())
        }
        links: list[Supersession] = []
        for item in provisional:
            found = by_origin.get(item.path)
            if found is None:
                continue
            path, origin = found
            resolved = identity.resolve_committed(
                self.repository_id,
                origin.introducing_commit,
                [item],
                [NewFile(origin.origin_path, origin.origin_blob_oid)],
            )
            if resolved.logical_ids[origin.origin_path] != self.file_logical_ids[path]:
                raise IndexingError("lineage_inconsistent")  # pragma: no cover - invariant
            links.extend(resolved.supersessions)
        return tuple(links)


@dataclass(slots=True)
class _Live:
    logical_id: uuid.UUID
    origin: FileOrigin
    blob_oid: str


@dataclass(frozen=True, slots=True)
class _Record:
    status: str
    score: int
    old_mode: str
    new_mode: str
    old_oid: str
    new_oid: str
    path: str | None
    new_path: str | None


def derive_lineage(
    scan: RepositoryScan,
    repository_id: str,
    *,
    target_commit: str | None = None,
    max_commits: int = DEFAULT_MAX_COMMITS,
    limits: ScanLimits | None = None,
) -> Lineage:
    """The lineage of the tree of ``target_commit`` (default: the scan's HEAD commit).

    ``scan`` supplies the checkout whose object store is read (its identity is re-verified);
    an unborn HEAD has no files and no lineage. Raises ``IndexingError`` ``history_too_large``
    above ``max_commits`` first-parent commits, ``commit_diff_too_large`` when one commit's diff
    exceeds the output cap, ``shallow_history`` for an incomplete clone, and
    ``ScanError`` for anything the safe Git runner refuses.
    """
    identity.repository_namespace(repository_id)  # validates the ID before any git runs
    target = scan.workspace.head_commit if target_commit is None else target_commit
    if target is None:
        return Lineage(repository_id, None, MappingProxyType({}), MappingProxyType({}), (), 0)
    if not _is_oid(target):
        raise ValueError("target commit must be a lowercase 40 or 64 character hex object id")
    if max_commits < 1:
        raise ValueError("max_commits must be positive")
    bounds = limits or ScanLimits()
    repo = _bootstrap(scan.root, bounds)
    try:
        if _fd_identity(repo.root_fd) != scan.root_identity:
            raise ScanError(ScanFailure.ROOT_MISMATCH)
        _validate_repository_files(repo, None)
        shallow = _read_below(repo.common_fd, ("shallow",), _METADATA_CAP)
        if shallow is not None and shallow.strip():
            raise IndexingError("shallow_history")
        config_text = _safe_config(repo, bounds)
        return _walk(repo, config_text, repository_id, target, max_commits, bounds)
    finally:
        repo.close()


def _walk(
    repo: _Repo,
    config_text: str,
    repository_id: str,
    target: str,
    max_commits: int,
    limits: ScanLimits,
) -> Lineage:
    private = Path(tempfile.mkdtemp(prefix="agent-context-lineage-"))
    try:
        (private / "refs" / "heads").mkdir(parents=True)
        (private / "objects").mkdir()
        (private / "HEAD").write_text(f"{target}\n", encoding="ascii")
        (private / "config").write_text(config_text, encoding="utf-8")
        env = _base_env(repo.git, repo.root)
        env["GIT_DIR"] = str(private)
        env["GIT_OBJECT_DIRECTORY"] = str(_pinned_objects(repo))

        def run(args: list[str], *, max_bytes: int) -> bytes:
            return _run_git(
                repo.git,
                args,
                cwd=repo.cwd,
                env=env,
                limits=limits,
                max_bytes=max_bytes,
                pass_fds=repo.fds,
            )[1]

        listing = run(
            ["rev-list", "--first-parent", f"--max-count={max_commits + 1}", target, "--"],
            max_bytes=(max_commits + 2) * _LIST_LINE_BYTES,
        )
        newest_first = listing.decode("ascii", errors="replace").split()
        if not newest_first or any(not _is_oid(oid) for oid in newest_first):
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
        if len(newest_first) > max_commits:
            raise IndexingError("history_too_large")
        chain = newest_first[::-1]

        state = _State(repository_id)
        previous: str | None = None
        for commit in chain:
            diff_args = ["--root", commit] if previous is None else [previous, commit]
            try:
                raw = run(
                    [
                        "diff-tree",
                        "-r",
                        "-z",
                        "--raw",
                        "--no-abbrev",
                        "--no-commit-id",
                        "--no-ext-diff",
                        "--no-textconv",
                        "-M",
                        f"-l{_RENAME_LIMIT}",
                        *diff_args,
                    ],
                    max_bytes=limits.max_command_output_bytes,
                )
            except ScanError as error:
                if error.reason is ScanFailure.OUTPUT_TOO_LARGE:
                    raise IndexingError("commit_diff_too_large") from None
                raise
            state.apply(commit, _parse_raw(raw))
            previous = commit
        return state.result(target, len(chain))
    except OSError:
        raise ScanError(ScanFailure.GIT_FAILED) from None
    finally:
        shutil.rmtree(private, ignore_errors=True)


def _pinned_objects(repo: _Repo) -> Path:
    return Path(f"/proc/self/fd/{repo.objects_fd}")


def _parse_raw(raw: bytes) -> list[_Record]:
    """Records of ``git diff-tree --raw -z``: a ``:`` meta token, then one or two path tokens."""
    tokens = _split_z(raw)
    records: list[_Record] = []
    position = 0
    while position < len(tokens):
        meta = tokens[position]
        position += 1
        fields = meta.decode("ascii", errors="replace").split(" ")
        if not meta.startswith(b":") or len(fields) != 5:
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
        old_mode, new_mode, old_oid, new_oid, code = fields
        old_mode = old_mode.removeprefix(":")
        status = code[:1]
        if not status or status not in "ADMTRCU" or not (_is_oid(old_oid) and _is_oid(new_oid)):
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
        paths = 2 if status in "RC" else 1
        if position + paths > len(tokens):
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
        decoded = [_safe_path(tokens[position + index]) for index in range(paths)]
        position += paths
        score = int(code[1:]) if status in "RC" and code[1:].isdigit() else 0
        records.append(
            _Record(
                status,
                score,
                old_mode,
                new_mode,
                old_oid,
                new_oid,
                decoded[0],
                decoded[1] if paths == 2 else None,
            )
        )
    return records


class _State:
    """The tracked tree and the deleted lineages, advanced one first-parent commit at a time."""

    def __init__(self, repository_id: str) -> None:
        self.repository_id = repository_id
        self.live: dict[str, _Live] = {}
        self.dead: dict[str, uuid.UUID] = {}
        self.supersessions: list[Supersession] = []

    def apply(self, commit: str, records: list[_Record]) -> None:
        removed_paths: list[str] = []
        added: dict[str, str] = {}
        similar: list[tuple[str, str, int]] = []
        for record in records:
            old_path, new_path = record.path, record.new_path
            if record.status in "RC":
                # Git's pairing is only a guess; a rename is removal plus addition. A copy leaves
                # its source in place, so it is a plain addition.
                if record.status == "R" and old_path is not None:
                    removed_paths.append(old_path)
                if new_path is not None and record.new_mode != _GITLINK_MODE:
                    added[new_path] = record.new_oid
                    if record.status == "R" and old_path is not None and record.score < 100:
                        similar.append((old_path, new_path, record.score))
                continue
            if old_path is None:
                continue
            old_exists = record.old_mode != _GITLINK_MODE and record.status != "A"
            new_exists = record.new_mode != _GITLINK_MODE and record.status != "D"
            if old_exists and not new_exists:
                removed_paths.append(old_path)
            elif new_exists and not old_exists:
                added[old_path] = record.new_oid
            elif old_exists and new_exists and old_path in self.live:
                self.live[old_path].blob_oid = record.new_oid

        priors = [
            PriorFile(path, self.live[path].logical_id, self.live[path].blob_oid)
            for path in sorted(set(removed_paths))
            if path in self.live
        ]
        reused = [
            PriorFile(path, self.dead[path], _NO_BLOB)
            for path in sorted(added)
            if path in self.dead and path not in {item.path for item in priors}
        ]
        prior_by_path = {item.path: item for item in priors}
        guesses = [
            SimilarRename(old, new, score / 100)
            for old, new, score in similar
            if old in prior_by_path and score > 0
        ]
        resolution = identity.resolve_rename(
            self.repository_id,
            commit,
            [*priors, *reused],
            [NewFile(path, oid) for path, oid in sorted(added.items())],
            guesses,
        )
        self.supersessions.extend(resolution.supersessions)

        carried = {item.logical_id: self.live[item.path] for item in priors}
        for item in priors:
            del self.live[item.path]
        for path, new_oid in sorted(added.items()):
            logical_id = resolution.logical_ids[path]
            source = carried.get(logical_id)
            origin = source.origin if source is not None else FileOrigin(path, commit, new_oid)
            self.live[path] = _Live(logical_id, origin, new_oid)
            self.dead.pop(path, None)
        moved = {self.live[path].logical_id for path in added}
        for item in priors:
            if item.logical_id not in moved:
                self.dead[item.path] = item.logical_id

    def result(self, target: str, walked: int) -> Lineage:
        live_ids = {item.logical_id for item in self.live.values()}
        kept = tuple(item for item in self.supersessions if item.new_logical_id in live_ids)
        unique = {(s.old_logical_id, s.new_logical_id, s.basis): s for s in kept}
        return Lineage(
            self.repository_id,
            target,
            MappingProxyType({path: item.logical_id for path, item in sorted(self.live.items())}),
            MappingProxyType({path: item.origin for path, item in sorted(self.live.items())}),
            tuple(
                sorted(
                    unique.values(),
                    key=lambda s: (str(s.new_logical_id), str(s.old_logical_id), s.basis),
                )
            ),
            walked,
        )
