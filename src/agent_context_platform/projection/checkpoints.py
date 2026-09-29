"""Monotonic per-projector checkpoint persistence."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import case, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from agent_context_platform.projection.models import ProjectionCheckpointRow, ProjectionState


class CheckpointRepository:
    """Transaction-neutral, monotonic per-projector checkpoint operations.

    A checkpoint tracks how far one (`projector_name`, `projector_version`)
    pair has progressed through the outbox. Competing workers can finalize
    claimed rows out of order -- the outbox makes no per-stream ordering
    guarantee -- so `advance` must never let `last_outbox_id` regress: it
    always keeps the greatest outbox position seen so far, while
    `processed_count` still increments on every call because each call
    represents one genuinely processed event.
    """

    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        projector_name: str,
        projector_version: str,
    ) -> ProjectionCheckpointRow | None:
        return await session.get(ProjectionCheckpointRow, (projector_name, projector_version))

    @staticmethod
    async def advance(
        session: AsyncSession,
        *,
        projector_name: str,
        projector_version: str,
        outbox_id: int,
        event_id: UUID,
        now: datetime,
    ) -> ProjectionCheckpointRow:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")

        insert_statement = insert(ProjectionCheckpointRow).values(
            projector_name=projector_name,
            projector_version=projector_version,
            last_outbox_id=outbox_id,
            last_event_id=event_id,
            processed_count=1,
            state=ProjectionState.ACTIVE,
            updated_at=now,
        )
        statement = insert_statement.on_conflict_do_update(
            index_elements=[
                ProjectionCheckpointRow.projector_name,
                ProjectionCheckpointRow.projector_version,
            ],
            set_={
                "last_outbox_id": func.greatest(
                    ProjectionCheckpointRow.last_outbox_id,
                    insert_statement.excluded.last_outbox_id,
                ),
                "last_event_id": case(
                    (
                        # A row can be pre-registered with no progress yet
                        # (`processed_count=0`, `last_outbox_id IS NULL`).
                        # SQL three-valued logic makes `excluded.last_outbox_id
                        # >= NULL` evaluate to NULL rather than true, so
                        # without this branch the comparison below would fall
                        # through to `else_` and leave `last_event_id` stuck
                        # at NULL forever even as `last_outbox_id` correctly
                        # advances via `GREATEST` (which does ignore NULL
                        # operands) -- violating the schema's paired
                        # null/non-null CHECK constraint on first advance.
                        ProjectionCheckpointRow.last_outbox_id.is_(None),
                        insert_statement.excluded.last_event_id,
                    ),
                    (
                        insert_statement.excluded.last_outbox_id
                        >= ProjectionCheckpointRow.last_outbox_id,
                        insert_statement.excluded.last_event_id,
                    ),
                    else_=ProjectionCheckpointRow.last_event_id,
                ),
                "processed_count": ProjectionCheckpointRow.processed_count + 1,
                "updated_at": now,
            },
        ).returning(ProjectionCheckpointRow)
        result = await session.scalars(statement.execution_options(populate_existing=True))
        return result.one()
