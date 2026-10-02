"""The single, ordered registry of the platform's graph projectors.

`ProjectionRunner` and the `agent-context projection` CLI both take their projectors from here, so
the live projection, a rebuild and a verification can never disagree about which projectors exist
or in which order they run.

Order: portfolio, agent, git, code. Delivery is at-least-once and unordered, so no projector may
depend on another having run first (every write commutes, see `projectors/__init__.py`); the order
therefore only fixes the sequence the matching projectors run in inside one Neo4j transaction. It
follows the data's dependency direction, so a transaction touches roots before leaves and a replay
reads like the system grew: the portfolio owns the workspace, project and repository roots every
other event is scoped to; the agent projector hangs sessions, turns and tool calls on those
roots; the git projector adds branches, commits, checkouts and snapshots the agent and code
projections point at; the code projector is last because its file, symbol and assertion revisions
hang on the repository, commit and snapshot nodes. Every projector takes the same global node-lock
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
    (PortfolioProjector(), AgentProjector(), GitProjector(), CodeProjector())
)


def registered_projectors() -> tuple[Projector, ...]:
    """The platform's projectors in their deterministic run order."""
    return PROJECTORS
