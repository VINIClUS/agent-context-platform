from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.dialects.postgresql import Insert

from agent_context_platform.projection.checkpoints import CheckpointRepository
from agent_context_platform.projection.models import ProjectionCheckpointRow

from ._doubles import FakeScalarResult, FakeSession

pytestmark = pytest.mark.unit

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_get_returns_none_when_no_checkpoint_exists() -> None:
    session = FakeSession(get_results=[None])

    result = asyncio.run(
        CheckpointRepository.get(session, projector_name="graph.test", projector_version="1")  # type: ignore[arg-type]
    )

    assert result is None


def test_get_returns_the_stored_checkpoint_row() -> None:
    stored = ProjectionCheckpointRow(
        projector_name="graph.test",
        projector_version="1",
        last_outbox_id=7,
        last_event_id=uuid4(),
        processed_count=3,
        updated_at=NOW,
    )
    session = FakeSession(get_results=[stored])

    result = asyncio.run(
        CheckpointRepository.get(session, projector_name="graph.test", projector_version="1")  # type: ignore[arg-type]
    )

    assert result is stored


def test_advance_rejects_a_naive_now() -> None:
    session = FakeSession()

    async def call() -> None:
        await CheckpointRepository.advance(
            session,  # type: ignore[arg-type]
            projector_name="graph.test",
            projector_version="1",
            outbox_id=1,
            event_id=uuid4(),
            now=datetime(2026, 1, 1),
        )

    with pytest.raises(ValueError, match="timezone-aware"):
        asyncio.run(call())


def test_advance_upserts_and_returns_the_resulting_row() -> None:
    event_id = uuid4()
    resulting_row = ProjectionCheckpointRow(
        projector_name="graph.test",
        projector_version="1",
        last_outbox_id=5,
        last_event_id=event_id,
        processed_count=1,
        updated_at=NOW,
    )
    session = FakeSession(scalars_results=[FakeScalarResult([resulting_row])])

    result = asyncio.run(
        CheckpointRepository.advance(
            session,  # type: ignore[arg-type]
            projector_name="graph.test",
            projector_version="1",
            outbox_id=5,
            event_id=event_id,
            now=NOW,
        )
    )

    assert result is resulting_row
    [statement] = session.executed_statements
    assert isinstance(statement, Insert)
    compiled = str(statement)
    assert "ON CONFLICT" in compiled
    assert "greatest" in compiled.lower()
