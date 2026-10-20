"""The projection worker's gauge sampler: one bounded query per interval, never per scrape.

`ProjectionSampler.refresh` reads the outstanding outbox rows (count and oldest creation time,
served by the status index, so the cost follows the backlog like `ProjectionRunner.backlog`) and
the open dead letters per projector, then replaces the snapshot the observable gauges report.
A failed refresh clears the snapshot: the gauges stop reporting rather than report stale values.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Final

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.observability import contract
from agent_context_platform.observability.core import ProjectionSnapshot, set_projection_snapshot
from agent_context_platform.projection.models import (
    DeadLetterRow,
    DeadLetterStatus,
    OutboxRow,
    OutboxStatus,
)

# The projection runtime's name for a failure no projector is attributable to.
_UNATTRIBUTED_DEAD_LETTER: Final = "<projection.runtime>"

Clock = Callable[[], datetime]


class ProjectionSampler:
    """Refresh the projection gauges for the projectors one worker runs."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        projector_names: Sequence[str],
        *,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self._session_factory = session_factory
        self._projector_names = tuple(dict.fromkeys(projector_names))
        self._clock = clock

    async def sample(self) -> ProjectionSnapshot:
        async with self._session_factory() as session:
            outstanding, oldest = (
                await session.execute(
                    select(func.count(), func.min(OutboxRow.created_at)).where(
                        OutboxRow.status.in_((OutboxStatus.PENDING, OutboxStatus.LEASED))
                    )
                )
            ).one()
            dead_letters = (
                await session.execute(
                    select(DeadLetterRow.projector_name, func.count())
                    .where(DeadLetterRow.status == DeadLetterStatus.OPEN)
                    .group_by(DeadLetterRow.projector_name)
                )
            ).all()
            await session.rollback()
        lag = 0.0 if not outstanding or oldest is None else (self._clock() - oldest).total_seconds()
        return ProjectionSnapshot(
            outbox_pending=int(outstanding),
            lag_seconds=dict.fromkeys(self._projector_names, max(lag, 0.0)),
            dead_letters={
                contract.PROJECTOR_UNATTRIBUTED if name == _UNATTRIBUTED_DEAD_LETTER else name: int(
                    count
                )
                for name, count in dead_letters
            },
        )

    async def refresh(self) -> bool:
        """Replace the gauge snapshot; ``False`` (and no gauges) when the query failed."""
        try:
            snapshot = await self.sample()
        except Exception:
            set_projection_snapshot(None)
            return False
        set_projection_snapshot(snapshot)
        return True

    async def run(self, stop: asyncio.Event, interval_seconds: float) -> None:
        """Refresh every ``interval_seconds`` until ``stop`` is set (or the task is cancelled)."""
        try:
            while not stop.is_set():
                await self.refresh()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        finally:
            set_projection_snapshot(None)
