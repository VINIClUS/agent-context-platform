"""Hand-written test doubles for the projection runtime's unit tests.

Mirrors the style established by `test_neo4j.py`: minimal fakes that
implement just the protocol surface the code under test actually calls,
rather than mocking SQLAlchemy or the Neo4j driver internals. Not a test
module itself (no `test_` prefix), so pytest does not collect it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from agent_context_sdk import (
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content import ContentDisposition


def build_stored_event(
    *,
    stream_id: str = "test-stream",
    event_type: str = "test.event.happened",
    sequence: int = 1,
    previous_hash: str | None = None,
    payload: dict[str, object] | None = None,
    producer_id: str = "test-producer",
    idempotency_key: str | None = None,
    occurred_at: datetime | None = None,
) -> StoredEventV1:
    """Build a real, SDK-sealed `StoredEventV1` for test fixtures.

    Uses the SDK's own `seal_event` rather than a hand-rolled dataclass, so
    tests exercise the runtime against the same contract production code
    produces.
    """
    moment = occurred_at or datetime.now(UTC)
    draft = EventDraftV1(
        event_type=event_type,
        stream_id=stream_id,
        occurred_at=moment,
        observed_at=moment,
        producer=ProducerV1(producer_id=producer_id, name="test-harness", version="1.0.0"),
        payload=payload or {},
        redaction=EventRedactionSummaryV1(
            policy_version="test-policy-v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key=idempotency_key or f"{stream_id}-{sequence}-{uuid4().hex}",
    )
    return seal_event(draft, [], sequence, previous_hash)


class FakeScalarResult:
    """Stand-in for the object returned by `AsyncSession.scalars(...)`."""

    def __init__(self, rows: Sequence[object] = ()) -> None:
        self._rows = list(rows)

    def all(self) -> list[object]:
        return list(self._rows)

    def one(self) -> object:
        return self._rows[0]


class FakeExecuteResult:
    """Stand-in for the object returned by `AsyncSession.execute(...)`."""

    def __init__(self, scalar: object = None) -> None:
        self._scalar = scalar

    def scalar_one_or_none(self) -> object:
        return self._scalar


class FakeSession:
    """Scripted async-session double.

    Each call to `execute`, `scalars`, or `get` pops the next canned result
    off the matching queue, in call order. Tests script exactly the results
    needed for the branch under test; SQL correctness itself is proven by
    the integration suite against real PostgreSQL.
    """

    def __init__(
        self,
        *,
        execute_results: Sequence[FakeExecuteResult] = (),
        scalars_results: Sequence[FakeScalarResult] = (),
        get_results: Sequence[object] = (),
    ) -> None:
        self._execute_results = list(execute_results)
        self._scalars_results = list(scalars_results)
        self._get_results = list(get_results)
        self.executed_statements: list[object] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.added: list[object] = []

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def execute(self, statement: object) -> FakeExecuteResult:
        self.executed_statements.append(statement)
        return self._execute_results.pop(0)

    async def scalars(self, statement: object) -> FakeScalarResult:
        self.executed_statements.append(statement)
        return self._scalars_results.pop(0)

    async def get(self, _model: object, _primary_key: object) -> object:
        return self._get_results.pop(0)

    async def commit(self) -> None:
        self.commit_calls += 1

    async def rollback(self) -> None:
        self.rollback_calls += 1

    def add(self, instance: object) -> None:
        self.added.append(instance)


class FakeSessionFactory:
    """Hands out pre-scripted `FakeSession` instances in call order."""

    def __init__(self, sessions: Sequence[FakeSession]) -> None:
        self._sessions = list(sessions)
        self.calls = 0

    def __call__(self) -> FakeSession:
        self.calls += 1
        return self._sessions.pop(0)


class FakeNeo4jStore:
    """Minimal Neo4j store double: runs the callback against a stub transaction."""

    def __init__(self) -> None:
        self.write_calls = 0

    async def execute_write(self, callback: Callable[[Any], Awaitable[Any]]) -> Any:
        self.write_calls += 1
        return await callback(object())


class RecordingProjector:
    """Test projector that records every event it is asked to project."""

    def __init__(
        self,
        name: str,
        version: str,
        *,
        handled_types: Sequence[str],
        fail_with: Exception | None = None,
    ) -> None:
        self.name = name
        self.version = version
        self._handled_types = set(handled_types)
        self._fail_with = fail_with
        self.project_calls: list[StoredEventV1] = []

    def handles(self, event_type: str) -> bool:
        return event_type in self._handled_types

    async def project(self, _tx: object, event: StoredEventV1) -> None:
        self.project_calls.append(event)
        if self._fail_with is not None:
            raise self._fail_with
