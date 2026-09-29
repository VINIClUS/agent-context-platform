"""Project Git state (checkout, workspace snapshot, commit) into the graph.

A working tree is projected as the `Checkout` node: design 10.1 treats checkout
and worktree as one ephemeral identity and no SDK event carries a separate
worktree ID, so no `Worktree` identities are invented here.

A checkout's current head and branch are forward-only pointers ordered by
`(occurred_at, event_id)`. Every observation is also kept as an immutable,
provenance-carrying relationship (`OBSERVED_AT`, `OBSERVED_ON`), so history
survives when the pointer moves. Commits and snapshots are immutable: repeated
observations converge on the same node.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Final

from agent_context_sdk import (
    CheckoutObservedV1,
    CommitObservedV1,
    StoredEventV1,
    WorkspaceSnapshotCapturedV1,
)

from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors import (
    assert_link,
    event_order,
    fill_once,
    min_non_null,
    newest_wins,
    node_statement,
    relationship_statement,
)


def commit_node_id(repository_id: str, oid: str) -> str:
    """Graph identity of a commit: the same OID may exist in two repositories."""
    return f"commit:{repository_id}:{oid}"


def branch_node_id(repository_id: str, name: str) -> str:
    """Graph identity of a branch; the length prefix keeps `(repository, name)` unambiguous."""
    return f"branch:{len(repository_id)}:{repository_id}:{name}"


_CHECKOUT_OBSERVED: Final = (
    node_statement("Checkout", "checkout_id")
    + min_non_null("repository_id", "object_format")
    + " "
    + newest_wins("head", "head_commit", "branch", "detached")
)
_COMMIT_STUB: Final = node_statement("Commit", "commit_id") + min_non_null("repository_id", "oid")
_BRANCH: Final = node_statement("Branch", "branch_id") + min_non_null("repository_id", "name")
_COMMIT_OBSERVED: Final = node_statement("Commit", "commit_id") + min_non_null(
    "repository_id", "oid", "tree_id", "authored_at", "committed_at", "message_content_id"
)
_SNAPSHOT: Final = (
    node_statement("WorkspaceSnapshot", "snapshot_id")
    + min_non_null("repository_id", "checkout_id", "base_commit", "dirty_patch_sha256")
    + " "
    + fill_once("modified_content_sha256", "untracked_paths")
)

_CHECKOUT_IN_REPOSITORY: Final = relationship_statement(
    "Checkout", "checkout_id", "IN_REPOSITORY", "Repository", "repository_id"
)
_BRANCH_IN_REPOSITORY: Final = relationship_statement(
    "Branch", "branch_id", "IN_REPOSITORY", "Repository", "repository_id"
)
_COMMIT_IN_REPOSITORY: Final = relationship_statement(
    "Commit", "commit_id", "IN_REPOSITORY", "Repository", "repository_id"
)
_OBSERVED_ON: Final = relationship_statement(
    "Checkout", "checkout_id", "OBSERVED_ON", "Branch", "branch_id"
)
_OBSERVED_AT: Final = relationship_statement(
    "Checkout", "checkout_id", "OBSERVED_AT", "Commit", "commit_id"
)
_HAS_PARENT: Final = relationship_statement(
    "Commit", "commit_id", "HAS_PARENT", "Commit", "commit_id"
)
_HAS_SNAPSHOT: Final = relationship_statement(
    "Checkout", "checkout_id", "HAS_SNAPSHOT", "WorkspaceSnapshot", "snapshot_id"
)
_BASED_ON: Final = relationship_statement(
    "WorkspaceSnapshot", "snapshot_id", "BASED_ON", "Commit", "commit_id"
)
_SESSION_PRODUCED_SNAPSHOT: Final = relationship_statement(
    "Session", "session_id", "PRODUCED", "WorkspaceSnapshot", "snapshot_id"
)
_SESSION_PRODUCED_COMMIT: Final = relationship_statement(
    "Session", "session_id", "PRODUCED", "Commit", "commit_id"
)


async def _checkout_observed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = CheckoutObservedV1.model_validate(dict(event.payload))
    checkout = payload.checkout_id
    await tx.run(
        _CHECKOUT_OBSERVED,
        parameters={
            "node_id": checkout,
            "order": event_order(event),
            "repository_id": payload.repository_id,
            "object_format": payload.object_format,
            "head_commit": payload.head_commit,
            "branch": payload.branch,
            "detached": payload.detached,
        },
    )
    await assert_link(tx, _CHECKOUT_IN_REPOSITORY, event, checkout, payload.repository_id)
    if payload.branch is not None:
        branch = branch_node_id(payload.repository_id, payload.branch)
        await tx.run(
            _BRANCH,
            parameters={
                "node_id": branch,
                "repository_id": payload.repository_id,
                "name": payload.branch,
            },
        )
        await assert_link(tx, _BRANCH_IN_REPOSITORY, event, branch, payload.repository_id)
        await assert_link(tx, _OBSERVED_ON, event, checkout, branch)
    if payload.head_commit is not None:
        commit = await _stub_commit(tx, event, payload.repository_id, payload.head_commit)
        await assert_link(tx, _OBSERVED_AT, event, checkout, commit)


async def _stub_commit(
    tx: Neo4jTransaction, event: StoredEventV1, repository_id: str, oid: str
) -> str:
    """Ensure the commit node exists (identity only) and is tied to its repository."""
    node_id = commit_node_id(repository_id, oid)
    await tx.run(
        _COMMIT_STUB,
        parameters={"node_id": node_id, "repository_id": repository_id, "oid": oid},
    )
    await assert_link(tx, _COMMIT_IN_REPOSITORY, event, node_id, repository_id)
    return node_id


async def _commit_observed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = CommitObservedV1.model_validate(dict(event.payload))
    commit = commit_node_id(payload.repository_id, payload.commit_id)
    await tx.run(
        _COMMIT_OBSERVED,
        parameters={
            "node_id": commit,
            "repository_id": payload.repository_id,
            "oid": payload.commit_id,
            "tree_id": payload.tree_id,
            "authored_at": payload.authored_at,
            "committed_at": payload.committed_at,
            "message_content_id": payload.message_content_id,
        },
    )
    await assert_link(tx, _COMMIT_IN_REPOSITORY, event, commit, payload.repository_id)
    for parent_oid in payload.parent_commit_ids:
        parent = await _stub_commit(tx, event, payload.repository_id, parent_oid)
        await assert_link(tx, _HAS_PARENT, event, commit, parent)
    if event.context.session_id is not None:
        await assert_link(tx, _SESSION_PRODUCED_COMMIT, event, event.context.session_id, commit)


async def _snapshot_captured(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = WorkspaceSnapshotCapturedV1.model_validate(dict(event.payload))
    snapshot = payload.snapshot_id
    await tx.run(
        _SNAPSHOT,
        parameters={
            "node_id": snapshot,
            "repository_id": payload.repository_id,
            "checkout_id": payload.checkout_id,
            "base_commit": payload.base_commit,
            "dirty_patch_sha256": payload.dirty_patch_sha256,
            "modified_content_sha256": list(payload.modified_content_sha256),
            "untracked_paths": list(payload.untracked_paths),
        },
    )
    await assert_link(tx, _HAS_SNAPSHOT, event, payload.checkout_id, snapshot)
    if payload.base_commit is not None:
        base = await _stub_commit(tx, event, payload.repository_id, payload.base_commit)
        await assert_link(tx, _BASED_ON, event, snapshot, base)
    if event.context.session_id is not None:
        await assert_link(tx, _SESSION_PRODUCED_SNAPSHOT, event, event.context.session_id, snapshot)


_HANDLERS: Final[dict[str, Callable[[Neo4jTransaction, StoredEventV1], Awaitable[None]]]] = {
    "git.checkout.observed": _checkout_observed,
    "git.commit.observed": _commit_observed,
    "git.workspace_snapshot.captured": _snapshot_captured,
}

GIT_EVENT_TYPES: Final = frozenset(_HANDLERS)


class GitProjector:
    """Projects checkout, workspace snapshot and commit events."""

    name = "git"
    version = "1"

    def handles(self, event_type: str) -> bool:
        return event_type in _HANDLERS

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        await _HANDLERS[event.event_type](tx, event)
