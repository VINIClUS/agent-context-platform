"""Project the portfolio (workspace, project, repository) into the graph.

The SDK has no portfolio events. Workspaces, projects and repositories are
derived from the envelope `context` of every projected event, plus the
repository payload of `git.repository.observed`. Their IDs are the platform
catalog IDs, used as-is.
"""

from __future__ import annotations

from typing import Final

from agent_context_sdk import RepositoryObservedV1, StoredEventV1

from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors import (
    assert_link,
    event_order,
    lock_event_nodes,
    min_non_null,
    newest_wins,
    node_statement,
    relationship_statement,
)
from agent_context_platform.projection.projectors.agent import AGENT_EVENT_TYPES
from agent_context_platform.projection.projectors.git import GIT_EVENT_TYPES

_REPOSITORY_OBSERVED_TYPE: Final = "git.repository.observed"
_HANDLED: Final = AGENT_EVENT_TYPES | GIT_EVENT_TYPES | {_REPOSITORY_OBSERVED_TYPE}

# Nodes are merged for every event, which is idempotent. Relationships are only
# asserted by these events, so their provenance lists stay bounded per session
# or checkout instead of growing with every tool call.
_LINKING_TYPES: Final = frozenset(
    {
        _REPOSITORY_OBSERVED_TYPE,
        "git.checkout.observed",
        "agent.session.started",
        "agent.session.ended",
    }
)

_REPOSITORY_OBSERVED: Final = (
    node_statement("Repository", "repository_id")
    + min_non_null("object_format")
    + " "
    + newest_wins("remotes", "remote_identities")
)
# Identity-only merges: no properties, no relationships, no provenance growth.
_WORKSPACE: Final = "MERGE (n:Workspace {workspace_id: $node_id})"
_PROJECT: Final = "MERGE (n:Project {project_id: $node_id})"
_REPOSITORY: Final = "MERGE (n:Repository {repository_id: $node_id})"
_HAS_PROJECT: Final = relationship_statement(
    "Workspace", "workspace_id", "HAS_PROJECT", "Project", "project_id"
)
_USES_REPOSITORY: Final = relationship_statement(
    "Project", "project_id", "USES_REPOSITORY", "Repository", "repository_id"
)


def _repositories(event: StoredEventV1) -> list[str]:
    ids = {event.context.repository_id}
    payload_repository = event.payload.get("repository_id")
    if isinstance(payload_repository, str):
        ids.add(payload_repository)
    return sorted(id_ for id_ in ids if id_ is not None)


def lock_keys(event: StoredEventV1) -> list[tuple[str, str]]:
    """Identity nodes `PortfolioProjector` merges for `event`, and no others."""
    if event.event_type not in _HANDLED:
        return []
    keys = [("Repository", repository_id) for repository_id in _repositories(event)]
    if event.context.workspace_id is not None:
        keys.append(("Workspace", event.context.workspace_id))
    if event.context.project_id is not None:
        keys.append(("Project", event.context.project_id))
    return sorted(keys)


class PortfolioProjector:
    """Projects workspace, project and repository from event context."""

    name = "portfolio"
    version = "1"

    def handles(self, event_type: str) -> bool:
        return event_type in _HANDLED

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        await lock_event_nodes(tx, event)
        workspace_id = event.context.workspace_id
        project_id = event.context.project_id
        repositories = _repositories(event)

        if workspace_id is not None:
            await tx.run(_WORKSPACE, parameters={"node_id": workspace_id})
        if project_id is not None:
            await tx.run(_PROJECT, parameters={"node_id": project_id})
        for repository_id in repositories:
            await tx.run(_REPOSITORY, parameters={"node_id": repository_id})

        if event.event_type == _REPOSITORY_OBSERVED_TYPE:
            observed = RepositoryObservedV1.model_validate(dict(event.payload))
            await tx.run(
                _REPOSITORY_OBSERVED,
                parameters={
                    "node_id": observed.repository_id,
                    "order": event_order(event),
                    "object_format": observed.object_format,
                    "remote_identities": list(observed.remote_identities),
                },
            )
        links = event.event_type in _LINKING_TYPES
        if links and workspace_id is not None and project_id is not None:
            await assert_link(tx, _HAS_PROJECT, event, workspace_id, project_id)
        if links and project_id is not None:
            for repository_id in repositories:
                await assert_link(tx, _USES_REPOSITORY, event, project_id, repository_id)
