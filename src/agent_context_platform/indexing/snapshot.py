"""Content-addressed identity of uncommitted repository state (design 10.1), platform-free.

``workspace_snapshot_id`` is a PURE function of plain strings and tuples: no platform type, no
checkout, no clock. It is meant to be lifted VERBATIM into ``agent_context_sdk`` so that Codex
capture (``git.workspace_snapshot.captured``) and the indexer derive the SAME ``snapshot_id`` for
the same dirty state. The same dirty state in two checkouts IS the same snapshot.

Inputs: the repository id, the base commit (a 40- or 64-hex OID, or ``None``) and the dirty
entries ``(path, state, digest)``, describing the NET state of each path against the base:

- ``modified``, ``added``, ``untracked``: ``digest`` is the SHA-256 (64 lowercase hex) of the raw
  worktree bytes. A renamed path is a ``deleted`` old path plus an ``added`` new one.
- ``deleted``: no digest (``None``).
- ``gitlink`` (a submodule pointer change): ``digest`` is ``gitlink_digest(head_oid, index_oid)``.

Every input is validated here, so the function is canonical on its own: a path must be a
repo-relative POSIX path (no ``./``, ``..``, ``//``, absolute, backslash, control or NUL), a path
appears once, states come from the closed set above, and the entries sort by the UTF-8 bytes of
the path. Anything else raises ``ValueError``. The domain string is versioned: a change to the
derivation is a new domain.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable

SNAPSHOT_DOMAIN = "agent-context.workspace-snapshot.v1"
GITLINK_DOMAIN = "agent-context.workspace-snapshot.gitlink.v1"
DIRTY_STATES = frozenset({"modified", "added", "deleted", "untracked", "gitlink"})

DirtyEntry = tuple[str, str, str | None]

_DIGEST = re.compile(r"[0-9a-f]{64}")
_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


def _safe_path(text: str) -> bool:
    """A self-contained copy of the repository path rules (kept free of platform imports)."""
    if not text or text.startswith("/") or "\\" in text:
        return False
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        return False
    return all(part not in ("", ".", "..") and part.lower() != ".git" for part in text.split("/"))


def gitlink_digest(head_oid: str | None, index_oid: str | None) -> str:
    """Digest of a submodule pointer change: the HEAD and index OIDs (``None`` when absent)."""
    for oid in (head_oid, index_oid):
        if oid is not None and _OID.fullmatch(oid) is None:
            raise ValueError("a gitlink OID is 40 or 64 lowercase hex digits")
    document = [GITLINK_DOMAIN, head_oid, index_oid]
    encoded = json.dumps(document, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def workspace_snapshot_id(
    repository_id: str, base_commit: str | None, entries: Iterable[DirtyEntry]
) -> str:
    """``snap_`` plus 40 hex digits over (domain, repository, base commit, sorted entries)."""
    if not repository_id:
        raise ValueError("repository_id must not be empty")
    if base_commit is not None and _OID.fullmatch(base_commit) is None:
        raise ValueError("base_commit is a 40- or 64-hex OID or None")
    keyed: list[tuple[bytes, str, str, str | None]] = []
    for path, state, digest in entries:
        if state not in DIRTY_STATES:
            raise ValueError(f"unknown dirty state {state!r}")
        if not _safe_path(path):
            raise ValueError(f"not a canonical repository path: {path!r}")
        if state == "deleted":
            if digest is not None:
                raise ValueError("a deleted entry has no content digest")
        elif digest is None or _DIGEST.fullmatch(digest) is None:
            raise ValueError("a non-deleted entry carries a 64-hex SHA-256 digest")
        try:
            key = path.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("a path must be encodable as UTF-8") from None
        keyed.append((key, path, state, digest))
    keyed.sort(key=lambda item: item[0])
    if len({item[0] for item in keyed}) != len(keyed):
        raise ValueError("duplicate dirty path")
    records = [[path, state, digest] for _, path, state, digest in keyed]
    document = [SNAPSHOT_DOMAIN, repository_id, base_commit, records]
    encoded = json.dumps(document, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    return "snap_" + hashlib.sha256(encoded).hexdigest()[:40]
