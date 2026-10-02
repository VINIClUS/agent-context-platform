"""The single, ordered registry of the platform's graph projectors.

`ProjectionRunner` and the `agent-context projection` CLI both take their projectors from here, so
the live projection, a rebuild and a verification can never disagree about which projectors exist
or in which order they run.

Order: portfolio, agent, git, code, search. Delivery is at-least-once and unordered, so no projector may
depend on another having run first (every write commutes, see `projectors/__init__.py`); the order
therefore only fixes the sequence the matching projectors run in inside one Neo4j transaction. It
follows the data's dependency direction, so a transaction touches roots before leaves and a replay
reads like the system grew: the portfolio owns the workspace, project and repository roots every
other event is scoped to; the agent projector hangs sessions, turns and tool calls on those
roots; the git projector adds branches, commits, checkouts and snapshots the agent and code
projections point at; the code projector's file, symbol and assertion revisions
hang on the repository, commit and snapshot nodes; the search projector is last because its embedding
nodes hang on the session, turn and tool-call nodes the agent projector writes. Every projector takes the same global node-lock
order first (`event_lock_keys`), so this sequence can never cause a deadlock.

Registration rejects a duplicate projector name (checkpoints and dead letters are attributed by
name) and a duplicate (name, version) pair, each with its own typed error.
"""

from __future__ import annotations

from collections.abc import Iterable

from agent_context_platform.projection.projectors.agent import AgentProjector
from agent_context_platform.projection.projectors.code import CodeProjector
from agent_context_platform.projection.projectors.git import GitProjector
from agent_context_platform.projection.projectors.portfolio import PortfolioProjector
from agent_context_platform.projection.projectors.search import (
    PurgeAccess,
    SearchBackend,
    SearchProjector,
)
from agent_context_platform.projection.runtime import Projector


class ProjectorRegistryError(ValueError):
    """A set of projectors cannot be registered."""


class DuplicateProjectorVersionError(ProjectorRegistryError):
    """Two projectors share the same (name, version) pair."""

    def __init__(self, name: str, version: str) -> None:
        super().__init__(f"projector registered twice: {name} version {version}")
        self.name = name
        self.version = version


class DuplicateProjectorNameError(ProjectorRegistryError):
    """Two projectors share a name under different versions."""

    def __init__(self, name: str) -> None:
        super().__init__(f"projector name registered twice: {name}")
        self.name = name


def register_projectors(projectors: Iterable[Projector]) -> tuple[Projector, ...]:
    """Validate `projectors` and freeze them, in the given order, as a tuple."""
    registered: list[Projector] = []
    identities: set[tuple[str, str]] = set()
    names: set[str] = set()
    for projector in projectors:
        identity = (projector.name, projector.version)
        if identity in identities:
            raise DuplicateProjectorVersionError(*identity)
        if projector.name in names:
            raise DuplicateProjectorNameError(projector.name)
        identities.add(identity)
        names.add(projector.name)
        registered.append(projector)
    return tuple(registered)


PROJECTORS: tuple[Projector, ...] = register_projectors(
    (PortfolioProjector(), AgentProjector(), GitProjector(), CodeProjector(), SearchProjector())
)


def registered_projectors() -> tuple[Projector, ...]:
    """The platform's projectors in their deterministic run order."""
    return PROJECTORS


def projectors_with_search(backend: SearchBackend) -> tuple[Projector, ...]:
    """The registered projectors with the search projector bound to a configured `backend`.

    `PROJECTORS` holds an unconfigured `SearchProjector`: it handles its events but fails closed
    (typed error, so the runner retries then dead-letters) when asked to index. A process that
    can embed (a worker, `agent-context projection rebuild` with a model directory) uses this.
    """
    return register_projectors(
        projector.with_backend(backend) if isinstance(projector, SearchProjector) else projector
        for projector in PROJECTORS
    )


def projectors_with_purge(access: PurgeAccess) -> tuple[Projector, ...]:
    """The registered projectors with a search projector that can purge but not index.

    For a deployment with no embedding model: `content.purged` still removes search rows and
    embeddings (it needs no model or content reader); events that must be indexed still fail
    closed.
    """
    return register_projectors(
        projector.with_purge(access) if isinstance(projector, SearchProjector) else projector
        for projector in PROJECTORS
    )
