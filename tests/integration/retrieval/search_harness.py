"""A small world for the search tests: real ingestion, real runner, real PostgreSQL and Neo4j.

Events go through `IngestionService` as `agent_context_api` (so content is stored by the real
content service and the outbox row exists); the `ProjectionRunner` and the search projector run as
`agent_context_projector`, so a missing grant fails the tests. Only the embedder is fake.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    ProducerV1,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
    StoredEventV1,
)
from agent_context_sdk.ids import new_uuid7
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from agent_context_platform.content.blob_store import BlobStoreError, StoredBlob
from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.ledger.repository import LedgerRepository
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.projectors.search import SearchBackend, SearchProjector
from agent_context_platform.projection.registry import projectors_with_search
from agent_context_platform.projection.runtime import ProjectionRunner, ProjectionRunReport

from ..projection.conftest import neo4j_integration_settings
from .conftest import wipe
from .fake_embeddings import FakeEmbeddingProvider

BASE_TIME = datetime(2026, 9, 1, 9, 0, 0, tzinfo=UTC)
_PRODUCER = ProducerV1(producer_id="search-test-producer", name="Codex", version="1.0.0")


class InlineOnlyBlobStore:
    """Test content is small, so it is always inline; any object-storage use is a bug."""

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        raise BlobStoreError("test content is inline")

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        raise BlobStoreError("test content is inline")

    async def delete(self, object_key: str) -> None:
        raise BlobStoreError("test content is inline")


def role_engine(dsn: str, role: str) -> AsyncEngine:
    return create_async_engine(dsn, poolclass=NullPool, connect_args={"options": f"-c role={role}"})


class SearchWorld:
    """Ingest content-bearing events, project them and query the result."""

    def __init__(self, dsn: str, owner: AsyncEngine, store: Neo4jStore) -> None:
        self.owner = owner
        self.store = store
        self._engines = [
            role_engine(dsn, "agent_context_api"),
            role_engine(dsn, "agent_context_projector"),
        ]
        self.api: async_sessionmaker[AsyncSession] = session_factory(self._engines[0])
        self.projector_sessions: async_sessionmaker[AsyncSession] = session_factory(
            self._engines[1]
        )
        self.content = ContentService(InlineOnlyBlobStore(), RedactionPolicyV1())
        self.ingestion = IngestionService(self.content, self.api)
        self.embeddings = FakeEmbeddingProvider()
        self._clock = BASE_TIME

    async def close(self) -> None:
        for engine in self._engines:
            await engine.dispose()

    def backend(
        self, embeddings: FakeEmbeddingProvider | None = None, *, write_documents: bool = True
    ) -> SearchBackend:
        return SearchBackend(
            self.projector_sessions, self.content, embeddings or self.embeddings, write_documents
        )

    def _tick(self) -> datetime:
        self._clock += timedelta(seconds=1)
        return self._clock

    async def ingest(
        self,
        body: str,
        content_id: str,
        *,
        project: str | None = "project-1",
        repository: str | None = "repo-1",
        session: str = "session-1",
        turn: str = "turn-1",
        media_type: str = "text/plain",
        event_type: str = "agent.turn.started",
        at: datetime | None = None,
        disposition: ContentDisposition = ContentDisposition.SANITIZED,
    ) -> UUID:
        data = body.encode()
        claim = ContentClaimV1(
            content_id=content_id,
            content_sha256=hashlib.sha256(data).hexdigest(),
            media_type=media_type,
            uncompressed_bytes=len(data),
        )
        item = SanitizedContentItemV1(
            claim=claim,
            sanitized_bytes_base64=base64.b64encode(data).decode(),
            redaction_report=RedactionReportV1(policy_version="1.0.0", disposition=disposition),
        )
        payload: dict[str, Any] = {
            "source": "codex",
            "session_id": session,
            "turn_id": turn,
            "user_message_content_id": content_id,
        }
        return await self._ingest(
            self._draft(
                event_type,
                payload,
                at=at,
                context=EventContextV1(
                    project_id=project, repository_id=repository, session_id=session, turn_id=turn
                ),
                claims=(claim,),
            ),
            (item,),
        )

    async def purge(self, *content_ids: str) -> UUID:
        payload = {
            "purge_request_id": f"purge-{uuid.uuid4().hex[:12]}",
            "purged_contents": [
                {"content_id": cid, "content_sha256": hashlib.sha256(cid.encode()).hexdigest()}
                for cid in content_ids
            ],
            "reason": "operator_request",
            "purged_at": self._tick().isoformat(),
            "affected_event_count": len(content_ids),
            "storage_kinds": ["inline"],
        }
        return await self._ingest(self._draft("content.purged", payload), ())

    def _draft(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        at: datetime | None = None,
        context: EventContextV1 | None = None,
        claims: Sequence[ContentClaimV1] = (),
    ) -> EventDraftV1:
        moment = at or self._tick()
        return EventDraftV1(
            event_type=event_type,
            stream_id=f"search-test-{uuid.uuid4().hex}",
            occurred_at=moment,
            observed_at=moment,
            producer=_PRODUCER,
            payload=payload,
            context=context or EventContextV1(),
            redaction=EventRedactionSummaryV1(
                policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
            ),
            idempotency_key=f"key-{uuid.uuid4().hex}",
            content_claims=tuple(claims),
        )

    async def _ingest(self, draft: EventDraftV1, items: Sequence[SanitizedContentItemV1]) -> UUID:
        outcome = await self.ingestion.ingest(
            IngestBatchRequestV1(batch_id=new_uuid7(), events=(draft,), content_items=tuple(items))
        )
        assert outcome.http_status < 300, outcome.response
        return draft.event_id

    async def event(self, event_id: UUID) -> StoredEventV1:
        async with self.projector_sessions() as session:
            stored = await LedgerRepository.get_event(session, event_id)
        assert stored is not None
        return stored

    async def project(
        self,
        embeddings: FakeEmbeddingProvider | None = None,
        extra_projectors: Sequence[Any] = (),
        projectors: Sequence[Any] | None = None,
    ) -> ProjectionRunReport:
        """Run every pending outbox row through the registered projectors, search configured."""
        runner = ProjectionRunner(
            self.projector_sessions,
            self.store,
            projectors
            if projectors is not None
            else (*projectors_with_search(self.backend(embeddings)), *extra_projectors),
            worker_id="search-world",
            max_attempts=1,
        )
        total = ProjectionRunReport()
        while True:
            report = await runner.run_once(50)
            if not report.claimed:
                return total
            total = ProjectionRunReport(
                claimed=total.claimed + report.claimed,
                delivered=total.delivered + report.delivered,
                retried=total.retried + report.retried,
                dead_lettered=total.dead_lettered + report.dead_lettered,
                lost_leases=total.lost_leases + report.lost_leases,
            )

    async def redeliver(
        self, event_id: UUID, embeddings: FakeEmbeddingProvider | None = None
    ) -> None:
        """Project one stored event again, as an at-least-once redelivery would."""
        stored = await self.event(event_id)
        projector = SearchProjector(self.backend(embeddings))

        async def apply(tx: Neo4jTransaction) -> None:
            await projector.project(tx, stored)

        await self.store.execute_write(apply)

    async def reset(self) -> None:
        """Empty the ledger, projection state and search tables (the database is test-owned)."""
        async with self.owner.begin() as connection:
            await connection.execute(
                text(
                    "TRUNCATE ledger.event_streams, ledger.events, retrieval.search_documents, "
                    "retrieval.content_tombstones CASCADE"
                )
            )

    async def documents(self) -> list[str]:
        async with self.owner.connect() as connection:
            rows = await connection.execute(
                text("SELECT content_id FROM retrieval.search_documents ORDER BY content_id")
            )
            return list(rows.scalars())

    async def embedding_ids(self) -> list[str]:
        async def read(tx: Any) -> list[str]:
            result = await tx.run(
                "MATCH (n:ContentEmbedding) RETURN n.content_id AS id ORDER BY id", parameters={}
            )
            return [str(record["id"]) for record in result.records]

        return await self.store.execute_read(read)


def with_world(
    body: Callable[[SearchWorld], Awaitable[None]], *, dsn: str, owner: AsyncEngine
) -> None:
    """Run `body` against an empty ledger, search tables and graph; leave them empty again."""

    async def run() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)
            world = SearchWorld(dsn, owner, store)
            try:
                await world.reset()
                await body(world)
            finally:
                await world.reset()
                await wipe(store)
                await world.close()

    asyncio.run(run())
