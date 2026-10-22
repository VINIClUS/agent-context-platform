"""How fresh a context package is: `as_of_event_id`, `projection_checkpoint`, `staleness_ms`.

- `as_of_event_id` is the projection watermark: the event of the newest DELIVERED outbox row below
  the oldest row still waiting (`pending` or `leased`). Every event at or before it has been
  delivered to every projector or dead-lettered, so the graph and the search material reflect the
  ledger up to it. A dead-lettered row does not hold the watermark back (it would never move
  again); it is reported as a warning instead. With nothing delivered yet there is no snapshot to
  answer from and `ProjectionNotReady` is raised.
- `projection_checkpoint` is opaque: `pc1-` and 40 hex digits of SHA-256 over the canonical JSON of
  the `(name, version, last_outbox_id, state)` checkpoints of the projectors the answer depends on
  (in the given order). It changes exactly when one of those checkpoints moves, and a graph replay
  (which never writes checkpoints) leaves it unchanged.
- `staleness_ms` is the age, at `now`, of the oldest outbox row still waiting; 0 when none waits,
  and never negative.
- Two instants let the answer depend on the snapshot only, never on the wall clock, and compare
  like with like (both are producer `observed_at` values, the clock temporal reads use):
  `known_until` is just before the earliest `observed_at` of any knowledge or quality event
  BEYOND the watermark (a later outbox row, delivered out of order or still waiting), or `None`
  when there is none. Reading temporal facts recorded up to it hides every such event (a
  supersession, a fix, a new decision is not known yet) while a producer whose clock runs ahead
  is still seen as soon as its event is delivered. `valid_instant` is the latest `observed_at`
  among the newest `RECENT_EVENTS` delivered events at or below the watermark: the "now" of an
  open revision window.
- Warnings, never silent: `projection_dead_letters:<projector>` for open dead letters of a
  projector the answer depends on, `projection_<state>:<projector>` for a paused or rebuilding one,
  and `projection_not_started:<projector>` when it has no checkpoint at all.

Reading checkpoints and dead letters needs SELECT on `projection.projection_checkpoints` and
`projection.dead_letters`. The reader runs as the API role (never the projector role, which may
write projection state), which reads them through migration 20261022_0001 (`API_READ_TABLES` in
`operations/readiness.py`); a missing grant is a driver error, never a guessed answer.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.retrieval.graph import RetrievalError

_WAITING: Final[str] = (
    "SELECT min(outbox_id) AS low, min(created_at) AS oldest FROM projection.outbox "
    "WHERE status IN ('pending', 'leased')"
)
_WATERMARK: Final[str] = (
    "SELECT outbox_id, event_id FROM projection.outbox WHERE status = 'delivered' "
    "AND (CAST(:low AS bigint) IS NULL OR outbox_id < CAST(:low AS bigint)) "
    "ORDER BY outbox_id DESC LIMIT 1"
)
# Bounded by the outbox rows after the watermark (the backlog), never the whole ledger.
_BEYOND: Final[str] = (
    "SELECT min(e.observed_at) FROM projection.outbox o "
    "JOIN ledger.events e ON e.event_id = o.event_id "
    "WHERE o.outbox_id > :as_of AND (e.event_type LIKE 'knowledge.%' "
    "OR e.event_type LIKE 'quality.%')"
)
_RECENT: Final[str] = (
    "SELECT max(e.observed_at) FROM (SELECT event_id FROM projection.outbox "
    "WHERE outbox_id <= :as_of AND status = 'delivered' ORDER BY outbox_id DESC "
    "LIMIT :recent) o JOIN ledger.events e ON e.event_id = o.event_id"
)
RECENT_EVENTS: Final = 1000
_TIMEOUT: Final[str] = "SELECT set_config('statement_timeout', :millis, true)"
_CHECKPOINTS: Final[str] = (
    "SELECT projector_name, projector_version, last_outbox_id, state "
    "FROM projection.projection_checkpoints"
)
_DEAD_LETTERS: Final[str] = (
    "SELECT projector_name, projector_version, count(*) AS open FROM projection.dead_letters "
    "WHERE status = 'open' GROUP BY projector_name, projector_version"
)


class ProjectionNotReady(RetrievalError):
    """Nothing has been projected yet, so no snapshot exists to answer from."""


@dataclass(frozen=True, slots=True)
class Freshness:
    as_of_event_id: str
    as_of_outbox_id: int
    valid_instant: datetime  # the "now" of an open window (see the module docstring)
    known_until: datetime | None  # temporal facts recorded after it are beyond the snapshot
    projection_checkpoint: str
    staleness_ms: int
    warnings: tuple[str, ...]


class FreshnessReader:
    """Reads the projection state. Needs SELECT on the outbox, `ledger.events`, checkpoints and
    dead letters: give it sessions of the API role (see the module docstring)."""

    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], *, statement_timeout_seconds: float = 5.0
    ) -> None:
        if not statement_timeout_seconds > 0:
            raise ValueError("statement_timeout_seconds must be positive")
        self._sessions = sessions
        self._timeout = str(max(1, int(statement_timeout_seconds * 1000)))

    async def read(self, projectors: Sequence[tuple[str, str]], now: datetime) -> Freshness:
        async with self._sessions() as session, session.begin():
            await session.execute(text(_TIMEOUT), {"millis": self._timeout})
            waiting = (await session.execute(text(_WAITING))).one()
            mark = (await session.execute(text(_WATERMARK), {"low": waiting.low})).first()
            recent = beyond = None
            if mark is not None:
                bound = {"as_of": mark.outbox_id, "recent": RECENT_EVENTS}
                recent = (await session.execute(text(_RECENT), bound)).scalar_one()
                beyond = (await session.execute(text(_BEYOND), bound)).scalar_one()
            checkpoints = {
                (str(row.projector_name), str(row.projector_version)): row
                for row in await session.execute(text(_CHECKPOINTS))
            }
            dead = {
                (str(row.projector_name), str(row.projector_version)): int(row.open)
                for row in await session.execute(text(_DEAD_LETTERS))
            }
        if mark is None or recent is None:
            raise ProjectionNotReady("no event has been projected yet")
        material: list[list[object]] = []
        warnings: list[str] = []
        for name, version in projectors:
            row = checkpoints.get((name, version))
            if row is None:
                material.append([name, version, 0, "none"])
                warnings.append(f"projection_not_started:{name}")
                continue
            state = str(row.state)
            material.append([name, version, int(row.last_outbox_id or 0), state])
            if state != "active":
                warnings.append(f"projection_{state}:{name}")
            if dead.get((name, version)):
                warnings.append(f"projection_dead_letters:{name}")
        digest = hashlib.sha256(
            json.dumps(material, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
        staleness = 0
        if waiting.oldest is not None:
            staleness = max(0, int((now - waiting.oldest).total_seconds() * 1000))
        return Freshness(
            as_of_event_id=str(mark.event_id),
            as_of_outbox_id=int(mark.outbox_id),
            valid_instant=recent,
            known_until=None if beyond is None else beyond - timedelta(microseconds=1),
            projection_checkpoint=f"pc1-{digest[:40]}",
            staleness_ms=staleness,
            warnings=tuple(dict.fromkeys(warnings)),
        )
