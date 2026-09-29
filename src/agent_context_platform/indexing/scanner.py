"""Safe, bounded scanner for untrusted Git checkouts.

The repositories scanned here are checkouts an agent worked in, so both the
worktree *and* the repository's own ``.git/config`` are treated as hostile.

Security model
--------------
Git runs code named by configuration and attributes: ``filter.*.clean`` (run
by ``status``/``diff``/``add`` for every path an attribute selects),
``core.fsmonitor``, ``diff.external``/``diff.*.textconv``, hooks and more.
This module therefore never lets git read repository configuration for any
command that could touch worktree content:

* One bootstrap ``rev-parse`` (plus ``symbolic-ref``/``rev-parse HEAD``)
  reads only layout and refs; it converts no content and cannot run a
  filter. Its toplevel must equal the canonical root that was passed in,
  ``GIT_CEILING_DIRECTORIES`` stops discovery from climbing out of it, and
  ``core.worktree`` or a gitfile cannot redirect the scan elsewhere.
* Every other command (``ls-files``, ``ls-tree``, ``status``) runs against a
  private ``GIT_DIR`` in a 0700 temporary directory that this module writes:
  the resolved ``HEAD``, a ``config`` rendered from an allowlist of validated
  ``core.*`` keys (none can name a program), and copies of
  ``info/exclude``/``info/attributes``. The real index and object store are
  reached through ``GIT_INDEX_FILE``/``GIT_OBJECT_DIRECTORY``. With no driver
  defined a ``filter=`` attribute is inert. The view fails closed on a split
  index, an unknown ``extensions.*`` key and any ``include`` key.
* No ``git diff`` runs at all: the dirty-state digest is computed from the
  worktree bytes this module reads itself, so ``diff.external`` and textconv
  can never fire. Every command also gets ``core.fsmonitor=false``,
  ``core.hooksPath=/dev/null`` and ``protocol.allow=never``; the environment
  is minimal (no HOME, no system or global config, no prompts, no optional
  locks) and git is an absolute path outside the repository.
* Worktree bytes are read by walking from a pinned root descriptor with
  ``O_NOFOLLOW`` one component at a time, re-checking ``(st_dev, st_ino)``.
  Symlinks are recorded as links (their target text is hashed, never
  followed); any component that is a symlink or that leaves the root is an
  ``escapes_repository`` record. Submodules are gitlinks: their OIDs are
  recorded and nothing is recursed into.

A repository owned by another user fails closed as ``not_a_repository``:
git's ``safe.directory`` is deliberately unavailable (no global config).

Bounds: each command has a timeout and a stdout cap (exceeding it is an
error, not a truncation; stderr is discarded). File count, per-file bytes and
total bytes are capped after sorting paths canonically, and any cap that bites
is reported through ``truncated``. Digests bind paths to digests and never
depend on record or set order. File content is never logged or placed in an
exception or repr; failures carry only a fixed ``ScanFailure`` code.
"""

from __future__ import annotations

import errno
import hashlib
import logging
import os
import selectors
import shutil
import stat
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Literal

from agent_context_sdk import canonical_json_bytes, sha256_hex

__all__ = [
    "FileKind",
    "Rejection",
    "RepositoryScan",
    "ScanError",
    "ScanFailure",
    "ScanLimits",
    "SkipReason",
    "TrackedFile",
    "UntrackedFile",
    "WorkspaceState",
    "scan_repository",
]

_LOG = logging.getLogger(__name__)

_CHUNK_SIZE = 65_536
_SELECT_POLL_SECONDS = 0.25
_BINARY_SNIFF_BYTES = 8_000
_METADATA_OUTPUT_CAP = 65_536
_INFO_COPY_CAP = 1_048_576
_SCAN_DOMAIN = b"agent-context-platform:scan:v1\0"
_DIRTY_DOMAIN = b"agent-context-platform:dirty:v1\0"
_GITLINK_MODE = "160000"
_SYMLINK_MODE = "120000"
_ZERO_OID_CHARS = frozenset("0")

type ObjectFormat = Literal["sha1", "sha256"]


class ScanFailure(StrEnum):
    """Fixed, content-free reason a scan could not complete."""

    UNSUPPORTED_PLATFORM = "unsupported_platform"
    GIT_UNAVAILABLE = "git_unavailable"
    NOT_A_REPOSITORY = "not_a_repository"
    ROOT_MISMATCH = "root_mismatch"
    UNSUPPORTED_REPOSITORY = "unsupported_repository"
    TIMED_OUT = "timed_out"
    OUTPUT_TOO_LARGE = "output_too_large"
    GIT_FAILED = "git_failed"
    MALFORMED_OUTPUT = "malformed_output"


class ScanError(Exception):
    """A scan failed; the message is only the fixed failure code."""

    def __init__(self, reason: ScanFailure) -> None:
        super().__init__(reason.value)
        self.reason = reason


class FileKind(StrEnum):
    FILE = "file"
    SYMLINK = "symlink"
    GITLINK = "gitlink"
    NESTED_REPOSITORY = "nested_repository"
    OTHER = "other"


class SkipReason(StrEnum):
    """Why a path has no content digest."""

    MISSING = "missing"
    TOO_LARGE = "too_large"
    BUDGET_EXHAUSTED = "budget_exhausted"
    UNREADABLE = "unreadable"
    ESCAPES_REPOSITORY = "escapes_repository"


class TruncationReason(StrEnum):
    MAX_FILES = "max_files"
    MAX_TOTAL_BYTES = "max_total_bytes"
    MAX_FILE_BYTES = "max_file_bytes"


@dataclass(frozen=True, slots=True)
class ScanLimits:
    """Bounds for one scan. Every command is limited by ``timeout_seconds``."""

    max_files: int = 50_000
    max_file_bytes: int = 1_048_576
    max_total_bytes: int = 268_435_456
    max_command_output_bytes: int = 67_108_864
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "max_files",
            "max_file_bytes",
            "max_total_bytes",
            "max_command_output_bytes",
            "timeout_seconds",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class Rejection:
    """A path git reported that the scanner refused to use.

    ``path`` is ``None`` when it could not be decoded as UTF-8.
    """

    reason: Literal["unsafe_path"]
    path: str | None


@dataclass(frozen=True, slots=True)
class TrackedFile:
    """One index entry. ``kind`` is what the worktree holds (index kind if unread)."""

    path: str
    kind: FileKind
    mode: str
    oid: str | None
    size: int | None
    content_sha256: str | None
    is_binary: bool | None
    change: str | None
    skipped: SkipReason | None
    link_target: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class UntrackedFile:
    """One untracked, non-ignored path."""

    path: str
    kind: FileKind
    size: int | None
    content_sha256: str | None
    is_binary: bool | None
    skipped: SkipReason | None
    link_target: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class WorkspaceState:
    """HEAD identity and uncommitted state.

    ``dirty_state_sha256`` is a digest over path-bound change records (status
    codes, modes, base/index OIDs and the worktree content digests) for every
    modified tracked path, staged submodule change and untracked file, plus an
    opaque count for changes whose path was rejected. It is a state digest,
    not a patch hash, and ``None`` for a clean workspace.
    """

    head_commit: str | None
    branch: str | None
    detached: bool
    is_dirty: bool
    dirty_state_sha256: str | None
    modified_paths: tuple[str, ...]
    untracked_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RepositoryScan:
    root: Path
    object_format: ObjectFormat
    workspace: WorkspaceState
    files: tuple[TrackedFile, ...]
    untracked: tuple[UntrackedFile, ...]
    rejections: tuple[Rejection, ...]
    truncated: bool
    truncation_reasons: tuple[TruncationReason, ...]
    omitted_files: int
    bytes_read: int
    scan_sha256: str


_DEFAULT_LIMITS = ScanLimits()


def scan_repository(root: Path, limits: ScanLimits = _DEFAULT_LIMITS) -> RepositoryScan:
    """Scan the Git checkout rooted at ``root``; raise ``ScanError`` on failure."""
    if os.name != "posix":
        raise ScanError(ScanFailure.UNSUPPORTED_PLATFORM)
    try:
        canonical_root = root.resolve(strict=True)
        if not canonical_root.is_dir():
            raise ScanError(ScanFailure.NOT_A_REPOSITORY)
        return _scan(canonical_root, limits)
    except OSError:
        raise ScanError(ScanFailure.NOT_A_REPOSITORY) from None


# --- subprocess plumbing -------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Repo:
    git: str
    root: Path
    git_dir: Path
    common_dir: Path
    object_format: ObjectFormat
    root_identity: tuple[int, int]


def _resolve_git(root: Path) -> str:
    found = shutil.which("git", path=os.environ.get("PATH", "/usr/bin:/bin"))
    if found is None or not os.path.isabs(found):
        raise ScanError(ScanFailure.GIT_UNAVAILABLE)
    try:
        real = Path(found).resolve(strict=True)
    except OSError:
        raise ScanError(ScanFailure.GIT_UNAVAILABLE) from None
    if real.is_relative_to(root) or Path(found).is_relative_to(root):
        raise ScanError(ScanFailure.GIT_UNAVAILABLE)
    return found


def _base_env(git: str, root: Path) -> dict[str, str]:
    # No HOME: git also reads ~/.config/git/{ignore,attributes} from it.
    return {
        "PATH": os.path.dirname(git),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_NO_LAZY_FETCH": "1",
        "GIT_CEILING_DIRECTORIES": str(root.parent),
        "GIT_PAGER": "cat",
        "LC_ALL": "C",
    }


def _run_git(
    repo_git: str,
    args: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    limits: ScanLimits,
    max_bytes: int,
    ok_codes: tuple[int, ...] = (0,),
) -> tuple[int, bytes]:
    argv = [
        repo_git,
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "protocol.allow=never",
        "--no-pager",
        *args,
    ]
    try:
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            env=dict(env),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            shell=False,
        )
    except FileNotFoundError:
        raise ScanError(ScanFailure.GIT_UNAVAILABLE) from None
    except OSError:
        raise ScanError(ScanFailure.GIT_FAILED) from None
    assert process.stdout is not None

    fd = process.stdout.fileno()
    deadline = time.monotonic() + limits.timeout_seconds
    chunks: list[bytes] = []
    total = 0
    failure: ScanFailure | None = None
    with selectors.DefaultSelector() as selector:
        selector.register(fd, selectors.EVENT_READ)
        while failure is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                failure = ScanFailure.TIMED_OUT
                break
            if not selector.select(timeout=min(remaining, _SELECT_POLL_SECONDS)):
                continue
            chunk = os.read(fd, _CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                failure = ScanFailure.OUTPUT_TOO_LARGE
    if failure is None:
        try:
            process.wait(timeout=max(deadline - time.monotonic(), 0.1))
        except subprocess.TimeoutExpired:
            failure = ScanFailure.TIMED_OUT
    if failure is not None:
        process.kill()
        process.wait()
        process.stdout.close()
        raise ScanError(failure)
    process.stdout.close()
    if process.returncode not in ok_codes:
        raise ScanError(ScanFailure.GIT_FAILED)
    return process.returncode, b"".join(chunks)


def _split_z(raw: bytes) -> list[bytes]:
    tokens = raw.split(b"\x00")
    if tokens and tokens[-1] == b"":
        tokens.pop()
    return tokens


# --- bootstrap and private view ---------------------------------------------------

_OID_CHARS = frozenset("0123456789abcdef")


def _is_oid(value: str) -> bool:
    return len(value) in (40, 64) and set(value) <= _OID_CHARS


def _real_env(repo: _Repo) -> dict[str, str]:
    env = _base_env(repo.git, repo.root)
    env["GIT_DIR"] = str(repo.git_dir)
    env["GIT_WORK_TREE"] = str(repo.root)
    return env


def _bootstrap(root: Path, limits: ScanLimits) -> _Repo:
    git = _resolve_git(root)
    try:
        dot_git = os.lstat(root / ".git")
    except FileNotFoundError:
        raise ScanError(ScanFailure.NOT_A_REPOSITORY) from None
    if not (stat.S_ISDIR(dot_git.st_mode) or stat.S_ISREG(dot_git.st_mode)):
        raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
    code, out = _run_git(
        git,
        [
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "--absolute-git-dir",
            "--git-common-dir",
            "--show-object-format",
        ],
        cwd=root,
        env=_base_env(git, root),
        limits=limits,
        max_bytes=_METADATA_OUTPUT_CAP,
        ok_codes=(0, 128),
    )
    if code != 0:
        raise ScanError(ScanFailure.NOT_A_REPOSITORY)
    try:
        lines = out.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        raise ScanError(ScanFailure.MALFORMED_OUTPUT) from None
    if len(lines) != 4 or lines[3] not in ("sha1", "sha256"):
        raise ScanError(ScanFailure.MALFORMED_OUTPUT)
    toplevel, git_dir, common_dir, object_format = lines
    # ``core.worktree`` or a gitfile must not move the scan to another tree.
    if Path(toplevel).resolve() != root:
        raise ScanError(ScanFailure.ROOT_MISMATCH)
    if not _owns_git_dir(git, root, Path(git_dir), Path(common_dir), limits):
        raise ScanError(ScanFailure.ROOT_MISMATCH)
    info = os.stat(root)
    return _Repo(
        git=git,
        root=root,
        git_dir=Path(git_dir),
        common_dir=Path(common_dir),
        object_format="sha256" if object_format == "sha256" else "sha1",
        root_identity=(info.st_dev, info.st_ino),
    )


def _dir_identity(path: Path) -> tuple[int, int] | None:
    """``(st_dev, st_ino)`` of a directory reached without following any symlink."""
    parts = path.resolve().parts
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        fd = os.open(parts[0], flags)
    except OSError:
        return None
    try:
        for name in parts[1:]:
            try:
                nxt = os.open(name, flags, dir_fd=fd)
            except OSError:
                return None
            os.close(fd)
            fd = nxt
        info = os.fstat(fd)
        return (info.st_dev, info.st_ino)
    finally:
        os.close(fd)


def _is_admin_dir_of(common_dir: Path, admin_dir: Path) -> bool:
    """Whether ``admin_dir`` is exactly ``<common_dir>/worktrees/<name>``.

    Compared by device and inode, so neither string prefixes nor symlinks can
    make a foreign repository's admin directory look like ours.
    """
    if admin_dir.parent.name != "worktrees" or not admin_dir.name:
        return False
    common = _dir_identity(common_dir)
    grandparent = _dir_identity(admin_dir.parent.parent)
    entry = _dir_identity(admin_dir.parent / admin_dir.name)
    listed = _dir_identity(common_dir / "worktrees" / admin_dir.name)
    admin = _dir_identity(admin_dir)
    return (
        common is not None
        and common == grandparent
        and admin is not None
        and admin == entry
        and admin == listed
    )


def _owns_git_dir(
    git: str, root: Path, git_dir: Path, common_dir: Path, limits: ScanLimits
) -> bool:
    """Whether ``git_dir`` really belongs to the checkout at ``root``.

    A hostile ``.git`` *file* can name any other repository's git dir, whose
    HEAD, index and exclude rules would then flow into the scan. Outside the
    ordinary ``<root>/.git`` layout the git dir must point back at ``root``:
    a linked worktree records ``<root>/.git`` in its ``gitdir`` file, and a
    submodule records the checkout in ``core.worktree``.

    The common directory (``commondir``) is checked too, since a valid
    backlink says nothing about where refs and objects come from. A linked
    worktree's admin dir must be exactly ``<common_dir>/worktrees/<name>``;
    every other layout must have its common dir equal to the git dir itself.
    """
    git_identity = _dir_identity(git_dir)
    if git_identity is None:
        return False
    own = _dir_identity(root / ".git")
    if own is not None and git_identity == own:
        return _dir_identity(common_dir) == git_identity
    backlink = git_dir / "gitdir"
    try:
        if backlink.is_file() and Path(backlink.read_text(encoding="utf-8").strip()).resolve() == (
            root / ".git"
        ):
            return _is_admin_dir_of(common_dir, git_dir)
    except (OSError, UnicodeDecodeError):
        return False
    code, out = _run_git(
        git,
        ["config", "--file", str(git_dir / "config"), "--get", "core.worktree"],
        cwd=root,
        env=_base_env(git, root),
        limits=limits,
        max_bytes=_METADATA_OUTPUT_CAP,
        ok_codes=(0, 1),
    )
    if code != 0:
        return False
    declared = out.decode("utf-8", errors="replace").rstrip("\n")
    return (git_dir / declared).resolve() == root and _dir_identity(common_dir) == git_identity


@dataclass(frozen=True, slots=True)
class _Head:
    oid: str | None
    branch: str | None
    detached: bool


def _read_head(repo: _Repo, limits: ScanLimits) -> _Head:
    """Resolve HEAD in the real repository (refs and config only, no content)."""
    env = _real_env(repo)
    code, out = _run_git(
        repo.git,
        ["symbolic-ref", "-q", "HEAD"],
        cwd=repo.root,
        env=env,
        limits=limits,
        max_bytes=_METADATA_OUTPUT_CAP,
        ok_codes=(0, 1),
    )
    ref = out.decode("utf-8", errors="replace").strip() if code == 0 else None
    if ref == "":
        raise ScanError(ScanFailure.MALFORMED_OUTPUT)
    code, out = _run_git(
        repo.git,
        ["rev-parse", "--verify", "-q", "HEAD^{commit}"],
        cwd=repo.root,
        env=env,
        limits=limits,
        max_bytes=_METADATA_OUTPUT_CAP,
        ok_codes=(0, 1),
    )
    oid: str | None = None
    if code == 0:
        oid = out.decode("ascii", errors="replace").strip()
        if not _is_oid(oid):
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
    elif ref is None:
        # Only an unborn *branch* may lack a commit.
        raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
    if ref is None:
        return _Head(oid=oid, branch=None, detached=True)
    return _Head(oid=oid, branch=ref.removeprefix("refs/heads/"), detached=False)


# Keys copied from repository config into the private view. None can name a
# program or a file: no filter, fsmonitor, hook, include, ssh, textconv or
# pager setting is on the list.
_BOOL_KEYS = (
    "core.filemode",
    "core.symlinks",
    "core.ignorecase",
    "core.precomposeunicode",
    "core.quotepath",
)
_ALLOWED_EXTENSIONS = frozenset({"objectformat", "worktreeconfig", "refstorage"})
_BOOL_SPELLINGS = {
    "true": "true",
    "yes": "true",
    "on": "true",
    "1": "true",
    "false": "false",
    "no": "false",
    "off": "false",
    "0": "false",
    "": "false",
}


def _safe_config(repo: _Repo, limits: ScanLimits) -> str:
    """Render the private config from an allowlist of validated values."""
    _, out = _run_git(
        repo.git,
        ["config", "--local", "--null", "--list"],
        cwd=repo.root,
        env=_real_env(repo),
        limits=limits,
        max_bytes=_METADATA_OUTPUT_CAP,
    )
    values: dict[str, str] = {}
    for token in _split_z(out):
        try:
            text = token.decode("utf-8")
        except UnicodeDecodeError:
            raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY) from None
        key, separator, value = text.partition("\n")
        values[key] = value if separator else "true"
    if any(key.startswith(("include.", "includeif.")) for key in values):
        raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
    if values.get("core.repositoryformatversion", "0").strip() not in ("0", "1"):
        raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
    for key in values:
        if key.startswith("extensions.") and key[len("extensions.") :] not in _ALLOWED_EXTENSIONS:
            raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)

    sha256 = repo.object_format == "sha256"
    lines = ["[core]", f"\trepositoryformatversion = {1 if sha256 else 0}"]
    for key in _BOOL_KEYS:
        if key in values:
            spelled = _BOOL_SPELLINGS.get(values[key].strip().lower())
            if spelled is None:
                raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
            lines.append(f"\t{key.split('.', 1)[1]} = {spelled}")
    if "core.autocrlf" in values:
        autocrlf = values["core.autocrlf"].strip().lower()
        if autocrlf == "input":
            lines.append("\tautocrlf = input")
        elif autocrlf in _BOOL_SPELLINGS:
            lines.append(f"\tautocrlf = {_BOOL_SPELLINGS[autocrlf]}")
        else:
            raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
    if "core.eol" in values:
        eol = values["core.eol"].strip().lower()
        if eol not in ("lf", "crlf", "native"):
            raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
        lines.append(f"\teol = {eol}")
    if sha256:
        lines.extend(["[extensions]", "\tobjectformat = sha256"])
    return "\n".join(lines) + "\n"


def _copy_info_file(source: Path, destination: Path) -> None:
    try:
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except FileNotFoundError:
        return
    except OSError:
        raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY) from None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
        data = b""
        while len(data) <= _INFO_COPY_CAP:
            chunk = os.read(fd, _CHUNK_SIZE)
            if not chunk:
                break
            data += chunk
    finally:
        os.close(fd)
    if len(data) > _INFO_COPY_CAP:
        raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
    destination.parent.mkdir(exist_ok=True)
    destination.write_bytes(data)


@dataclass(frozen=True, slots=True)
class _GitOutput:
    index: bytes
    head_tree: bytes
    status: bytes


def _run_in_private_view(repo: _Repo, head: _Head, limits: ScanLimits) -> _GitOutput:
    """Run ``ls-files``, ``ls-tree`` and ``status`` without reading repo config."""
    if any(repo.git_dir.glob("sharedindex.*")):
        raise ScanError(ScanFailure.UNSUPPORTED_REPOSITORY)
    config_text = _safe_config(repo, limits)
    private = Path(tempfile.mkdtemp(prefix="agent-context-scan-"))
    try:
        (private / "refs" / "heads").mkdir(parents=True)
        (private / "objects").mkdir()
        head_text = f"{head.oid}\n" if head.oid is not None else "ref: refs/heads/unborn\n"
        (private / "HEAD").write_text(head_text, encoding="ascii")
        (private / "config").write_text(config_text, encoding="utf-8")
        for name in ("exclude", "attributes"):
            _copy_info_file(repo.common_dir / "info" / name, private / "info" / name)

        env = _base_env(repo.git, repo.root)
        env["GIT_DIR"] = str(private)
        env["GIT_WORK_TREE"] = str(repo.root)
        env["GIT_INDEX_FILE"] = str(repo.git_dir / "index")
        env["GIT_OBJECT_DIRECTORY"] = str(repo.common_dir / "objects")

        def run(args: Sequence[str]) -> bytes:
            return _run_git(
                repo.git,
                args,
                cwd=repo.root,
                env=env,
                limits=limits,
                max_bytes=limits.max_command_output_bytes,
            )[1]

        index = run(["ls-files", "--stage", "-z"])
        head_tree = run(["ls-tree", "-r", "-z", head.oid]) if head.oid is not None else b""
        status = run(
            [
                "--no-optional-locks",
                "status",
                "--porcelain=v2",
                "-z",
                "--no-renames",
                "--untracked-files=all",
                "--ignore-submodules=all",
            ]
        )
        return _GitOutput(index=index, head_tree=head_tree, status=status)
    except OSError:
        raise ScanError(ScanFailure.GIT_FAILED) from None
    finally:
        shutil.rmtree(private, ignore_errors=True)


# --- git output parsing -----------------------------------------------------------


def _safe_path(raw: bytes) -> str | None:
    """Decode a git-reported path, or ``None`` if it may not be used."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return text if _is_safe_path(text) else None


def _is_safe_path(text: str) -> bool:
    if not text or text.startswith("/") or "\\" in text:
        return False
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        return False
    return all(part not in ("", ".", "..") and part.lower() != ".git" for part in text.split("/"))


@dataclass(frozen=True, slots=True)
class _IndexEntry:
    mode: str
    oid: str | None
    conflicted: bool


@dataclass(frozen=True, slots=True)
class _Change:
    xy: str
    modes: tuple[str, ...]
    base_oid: str | None
    index_oid: str | None


def _sort_key(path: str) -> bytes:
    return path.encode("utf-8")


def _parse_index(raw: bytes, rejections: list[Rejection]) -> dict[str, _IndexEntry]:
    entries: dict[str, _IndexEntry] = {}
    for token in _split_z(raw):
        meta, separator, path_raw = token.partition(b"\t")
        fields = meta.decode("ascii", errors="replace").split(" ")
        if not separator or len(fields) != 3 or not _is_oid(fields[1]):
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
        mode, oid, stage = fields
        path = _safe_path(path_raw)
        if path is None:
            rejections.append(_reject(path_raw))
            continue
        if stage != "0" or (path in entries and entries[path].conflicted):
            entries[path] = _IndexEntry(mode=mode, oid=None, conflicted=True)
        else:
            entries[path] = _IndexEntry(mode=mode, oid=oid, conflicted=False)
    return entries


def _parse_head_gitlinks(raw: bytes) -> dict[str, str]:
    links: dict[str, str] = {}
    for token in _split_z(raw):
        meta, separator, path_raw = token.partition(b"\t")
        fields = meta.decode("ascii", errors="replace").split(" ")
        if not separator or len(fields) != 3:
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
        if fields[0] == _GITLINK_MODE:
            path = _safe_path(path_raw)
            if path is not None:
                links[path] = fields[2]
    return links


def _reject(raw: bytes) -> Rejection:
    try:
        return Rejection("unsafe_path", ascii(raw.decode("utf-8")).strip("'"))
    except UnicodeDecodeError:
        return Rejection("unsafe_path", None)


def _clean_oid(value: str) -> str | None:
    return None if not _is_oid(value) or set(value) == _ZERO_OID_CHARS else value


def _parse_status(
    raw: bytes, rejections: list[Rejection], rejected_changes: list[str] | None = None
) -> tuple[dict[str, _Change], list[tuple[str, bool]]]:
    """Return tracked changes by path and untracked ``(path, is_directory)`` pairs."""
    changes: dict[str, _Change] = {}
    untracked: list[tuple[str, bool]] = []
    tokens = _split_z(raw)
    position = 0
    while position < len(tokens):
        token = tokens[position]
        position += 1
        marker = token[:1]
        if marker == b"?":
            path_raw = token[2:]
            is_directory = path_raw.endswith(b"/")
            path = _safe_path(path_raw.rstrip(b"/") if is_directory else path_raw)
            if path is None:
                rejections.append(_reject(path_raw))
                if rejected_changes is not None:
                    rejected_changes.append("?")
            else:
                untracked.append((path, is_directory))
        elif marker == b"!":
            continue
        elif marker in (b"1", b"2", b"u"):
            text = token.decode("utf-8", errors="surrogateescape")
            modes: tuple[int, ...]
            if marker == b"1":
                fields, path_index, modes, base, index = text.split(" ", 8), 8, (3, 4, 5), 6, 7
            elif marker == b"2":
                fields, path_index, modes, base, index = text.split(" ", 9), 9, (3, 4, 5), 6, 7
                position += 1  # the original path follows as its own token
            else:
                fields, path_index, modes, base, index = text.split(" ", 10), 10, (3, 4, 5, 6), 7, 8
            if len(fields) != path_index + 1:
                raise ScanError(ScanFailure.MALFORMED_OUTPUT)
            path = _safe_path(fields[path_index].encode("utf-8", errors="surrogateescape"))
            if path is None:
                rejections.append(_reject(fields[path_index].encode("utf-8", "surrogateescape")))
                if rejected_changes is not None:
                    rejected_changes.append(f"{marker.decode()}{fields[1]}")
                continue
            changes[path] = _Change(
                xy=fields[1],
                modes=tuple(fields[i] for i in modes),
                base_oid=None if marker == b"u" else _clean_oid(fields[base]),
                index_oid=None if marker == b"u" else _clean_oid(fields[index]),
            )
        elif marker == b"#":
            continue
        else:
            raise ScanError(ScanFailure.MALFORMED_OUTPUT)
    return changes, untracked


# --- worktree reads ---------------------------------------------------------------


def _open_pinned_root(root: Path, identity: tuple[int, int]) -> int:
    """Open ``root`` from ``/`` without following any symlink, then pin it."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for name in root.parts[1:]:
            child = os.open(name, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        info = os.fstat(fd)
        if (info.st_dev, info.st_ino) != identity:
            raise ScanError(ScanFailure.ROOT_MISMATCH)
    except OSError:
        os.close(fd)
        raise ScanError(ScanFailure.ROOT_MISMATCH) from None
    except BaseException:
        os.close(fd)
        raise
    return fd


@dataclass(frozen=True, slots=True)
class _Observed:
    kind: FileKind
    size: int | None
    sha256: str | None
    is_binary: bool | None
    link_target: str | None
    skipped: SkipReason | None
    # Bytes actually read from the file, even when the digest was discarded.
    consumed: int = 0


def _skipped(
    reason: SkipReason, kind: FileKind = FileKind.OTHER, size: int | None = None
) -> _Observed:
    return _Observed(kind, size, None, None, None, reason)


def _open_component(parent: int, name: str) -> int | SkipReason:
    """Open one directory component below ``parent``, never following a link."""
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError:
        return SkipReason.MISSING
    except OSError:
        return SkipReason.UNREADABLE
    if stat.S_ISLNK(before.st_mode):
        return SkipReason.ESCAPES_REPOSITORY
    if not stat.S_ISDIR(before.st_mode):
        return SkipReason.MISSING
    try:
        fd = os.open(
            name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent
        )
    except OSError as error:
        if error.errno in (errno.ELOOP, errno.ENOTDIR):
            return SkipReason.ESCAPES_REPOSITORY
        return SkipReason.UNREADABLE
    after = os.fstat(fd)
    if (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
        os.close(fd)
        return SkipReason.ESCAPES_REPOSITORY
    return fd


def _observe(root_fd: int, path: str, *, file_cap: int, budget: int) -> _Observed:
    """Read one worktree path by a component-wise ``O_NOFOLLOW`` walk from the root."""
    parts = path.split("/")
    held: list[int] = []
    try:
        parent = root_fd
        for name in parts[:-1]:
            opened = _open_component(parent, name)
            if isinstance(opened, SkipReason):
                return _skipped(opened)
            held.append(opened)
            parent = opened
        return _observe_leaf(parent, parts[-1], file_cap=file_cap, budget=budget)
    finally:
        for fd in held:
            os.close(fd)


def _over_cap(size: int, file_cap: int, budget: int, kind: FileKind) -> _Observed | None:
    if size <= min(file_cap, budget):
        return None
    reason = SkipReason.TOO_LARGE if size > file_cap else SkipReason.BUDGET_EXHAUSTED
    return _skipped(reason, kind, size)


def _observe_leaf(parent: int, name: str, *, file_cap: int, budget: int) -> _Observed:
    try:
        info = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError:
        return _skipped(SkipReason.MISSING)
    except OSError:
        return _skipped(SkipReason.UNREADABLE)
    if stat.S_ISLNK(info.st_mode):
        try:
            target = os.readlink(os.fsencode(name), dir_fd=parent)
        except OSError:
            return _skipped(SkipReason.UNREADABLE, FileKind.SYMLINK)
        over = _over_cap(len(target), file_cap, budget, FileKind.SYMLINK)
        if over is not None:
            return over
        return _Observed(
            FileKind.SYMLINK, len(target), sha256_hex(target), False, os.fsdecode(target), None
        )
    if not stat.S_ISREG(info.st_mode):
        return _skipped(SkipReason.UNREADABLE, FileKind.OTHER)
    over = _over_cap(info.st_size, file_cap, budget, FileKind.FILE)
    if over is not None:
        return over
    try:
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent
        )
    except OSError:
        return _skipped(SkipReason.UNREADABLE, FileKind.FILE)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
            info.st_dev,
            info.st_ino,
        ):
            return _skipped(SkipReason.ESCAPES_REPOSITORY, FileKind.FILE)
        digest = hashlib.sha256()
        total = 0
        sniff = b""
        cap = min(file_cap, budget)
        while True:
            try:
                # Never ask for more than the allowance plus one byte, which
                # is what detects a file that grew past it.
                chunk = os.read(fd, min(_CHUNK_SIZE, cap - total + 1))
            except OSError:
                return replace(_skipped(SkipReason.UNREADABLE, FileKind.FILE), consumed=total)
            if not chunk:
                break
            total += len(chunk)
            if total > cap:
                # Grew while being read: report it as over the cap, never truncate.
                over = _over_cap(total, file_cap, budget, FileKind.FILE) or _skipped(
                    SkipReason.TOO_LARGE, FileKind.FILE
                )
                # The detection byte is not charged: the ceiling stays hard.
                return replace(over, consumed=min(total, budget))
            if len(sniff) < _BINARY_SNIFF_BYTES:
                sniff += chunk[: _BINARY_SNIFF_BYTES - len(sniff)]
            digest.update(chunk)
    finally:
        os.close(fd)
    return _Observed(FileKind.FILE, total, digest.hexdigest(), b"\x00" in sniff, None, None, total)


# --- orchestration ------------------------------------------------------------------


def _kind_for_mode(mode: str) -> FileKind:
    if mode == _GITLINK_MODE:
        return FileKind.GITLINK
    if mode == _SYMLINK_MODE:
        return FileKind.SYMLINK
    return FileKind.FILE


def _scan(root: Path, limits: ScanLimits) -> RepositoryScan:
    repo = _bootstrap(root, limits)
    root_fd = _open_pinned_root(root, repo.root_identity)
    try:
        head = _read_head(repo, limits)
        output = _run_in_private_view(repo, head, limits)
        rejections: list[Rejection] = []
        index = _parse_index(output.index, rejections)
        head_links = _parse_head_gitlinks(output.head_tree)
        rejected_changes: list[str] = []
        changes, untracked_raw = _parse_status(output.status, rejections, rejected_changes)

        bytes_read = 0
        processed = 0
        reasons: set[TruncationReason] = set()

        def observe(path: str) -> _Observed:
            nonlocal bytes_read
            seen = _observe(
                root_fd,
                path,
                file_cap=limits.max_file_bytes,
                budget=max(limits.max_total_bytes - bytes_read, 0),
            )
            # Charge what was actually read, even if the digest was discarded.
            bytes_read += (seen.size or 0) if seen.skipped is None else seen.consumed
            if seen.skipped is SkipReason.TOO_LARGE:
                reasons.add(TruncationReason.MAX_FILE_BYTES)
            elif seen.skipped is SkipReason.BUDGET_EXHAUSTED:
                reasons.add(TruncationReason.MAX_TOTAL_BYTES)
            return seen

        # Canonical order first: the caps below must never depend on git's order.
        tracked_paths = sorted(index, key=_sort_key)
        files: list[TrackedFile] = []
        omitted_tracked: list[str] = []
        for path in tracked_paths:
            entry = index[path]
            change = changes.get(path)
            if processed >= limits.max_files:
                omitted_tracked.append(path)
                continue
            processed += 1
            if entry.mode == _GITLINK_MODE:
                files.append(
                    TrackedFile(
                        path,
                        FileKind.GITLINK,
                        entry.mode,
                        entry.oid,
                        None,
                        None,
                        None,
                        change.xy if change else None,
                        None,
                    )
                )
                continue
            seen = observe(path)
            kind = seen.kind if seen.skipped is None else _kind_for_mode(entry.mode)
            files.append(
                TrackedFile(
                    path,
                    kind,
                    entry.mode,
                    entry.oid,
                    seen.size,
                    seen.sha256,
                    seen.is_binary,
                    change.xy if change else None,
                    seen.skipped,
                    seen.link_target,
                )
            )

        untracked_sorted = sorted(set(untracked_raw), key=lambda item: _sort_key(item[0]))
        untracked: list[UntrackedFile] = []
        omitted_untracked: list[str] = []
        for path, is_directory in untracked_sorted:
            if processed >= limits.max_files:
                omitted_untracked.append(path)
                continue
            processed += 1
            if is_directory:
                untracked.append(
                    UntrackedFile(path, FileKind.NESTED_REPOSITORY, None, None, None, None)
                )
                continue
            seen = observe(path)
            untracked.append(
                UntrackedFile(
                    path,
                    seen.kind,
                    seen.size,
                    seen.sha256,
                    seen.is_binary,
                    seen.skipped,
                    seen.link_target,
                )
            )
    finally:
        os.close(root_fd)

    omitted = len(omitted_tracked) + len(omitted_untracked)
    if omitted:
        reasons.add(TruncationReason.MAX_FILES)
    truncated = bool(reasons)
    ordered_reasons = tuple(sorted(reasons))

    by_path = {item.path: item for item in files}
    gitlink_changes = sorted(
        (
            path,
            head_links.get(path),
            index[path].oid if path in index and index[path].mode == _GITLINK_MODE else None,
            path in index and index[path].conflicted,
        )
        for path in head_links.keys() | {p for p, e in index.items() if e.mode == _GITLINK_MODE}
        if head_links.get(path)
        != (index[path].oid if path in index and index[path].mode == _GITLINK_MODE else None)
    )
    change_records: list[list[object]] = []
    for path in sorted(changes, key=_sort_key):
        change = changes[path]
        tracked_file = by_path.get(path)
        change_records.append(
            [
                path,
                change.xy,
                list(change.modes),
                change.base_oid,
                change.index_oid,
                tracked_file.content_sha256 if tracked_file else None,
                (tracked_file.skipped.value if tracked_file and tracked_file.skipped else None)
                if tracked_file is not None or path not in index
                else "omitted",
            ]
        )
    untracked_records: list[list[object]] = []
    untracked_records.extend(
        [
            item.path,
            item.kind.value,
            item.content_sha256,
            item.skipped.value if item.skipped else None,
        ]
        for item in untracked
    )
    untracked_records.extend([path, None, None, "omitted"] for path in omitted_untracked)
    modified_paths = tuple(
        sorted({*changes, *(record[0] for record in gitlink_changes)}, key=_sort_key)
    )
    # Changes at rejected paths stay dirty; only marker, status code and a
    # count are recorded, never the raw path.
    rejected_records = sorted(
        [code, rejected_changes.count(code)] for code in set(rejected_changes)
    )
    is_dirty = bool(change_records or gitlink_changes or untracked_records or rejected_records)
    dirty_digest: str | None = None
    if is_dirty:
        payload: bytes = canonical_json_bytes(
            {
                "head": head.oid,
                "changes": change_records,
                "gitlinks": [list(item) for item in gitlink_changes],
                "untracked": untracked_records,
                "truncated": truncated,
                "omitted_files": omitted,
                "rejected_changes": rejected_records,
            }
        )
        dirty_digest = sha256_hex(_DIRTY_DOMAIN + payload)

    workspace = WorkspaceState(
        head_commit=head.oid,
        branch=head.branch,
        detached=head.detached,
        is_dirty=is_dirty,
        dirty_state_sha256=dirty_digest,
        modified_paths=modified_paths,
        untracked_paths=tuple(item.path for item in untracked) + tuple(omitted_untracked),
    )
    scan_payload: bytes = canonical_json_bytes(
        {
            "object_format": repo.object_format,
            "head": head.oid,
            "files": [
                [
                    f.path,
                    f.kind.value,
                    f.mode,
                    f.oid,
                    f.size,
                    f.content_sha256,
                    f.change,
                    f.skipped.value if f.skipped else None,
                ]
                for f in files
            ],
            "untracked": untracked_records,
            "dirty": dirty_digest,
            "rejected": len(rejections),
            "truncated": truncated,
            "truncation_reasons": [reason.value for reason in ordered_reasons],
            "omitted_files": omitted,
            # Index-bound (path, OID) pairs for tracked entries past max_files.
            "omitted_tracked_sha256": sha256_hex(
                canonical_json_bytes(
                    [[path, index[path].oid] for path in sorted(omitted_tracked, key=_sort_key)]
                )
            ),
        }
    )
    result = RepositoryScan(
        root=root,
        object_format=repo.object_format,
        workspace=workspace,
        files=tuple(files),
        untracked=tuple(untracked),
        rejections=tuple(sorted(rejections, key=lambda item: item.path or "")),
        truncated=truncated,
        truncation_reasons=ordered_reasons,
        omitted_files=omitted,
        bytes_read=bytes_read,
        scan_sha256=sha256_hex(_SCAN_DOMAIN + scan_payload),
    )
    _LOG.debug(
        "repository scan complete: files=%d untracked=%d rejected=%d truncated=%s",
        len(files),
        len(untracked),
        len(rejections),
        truncated,
    )
    return result
