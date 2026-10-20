"""`ContextComposer` contract over real services (PLATFORM-043): scope, cursors, budgets, freshness.

The world is the evaluation world (`tests/evaluation/world.py`), seeded once per module through the
real ingestion service and projection runner. The quality gates over the 30 golden questions live
in `tests/evaluation/test_context_quality.py`; this module checks the typed failure modes and the
freshness, purge and deadline rules a question set cannot express.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from agent_context_sdk import ContentRefV1, ContextPackageV1
from agent_context_sdk.contracts.mcp import (
    ContextForTaskInputV1,
    DecisionHistoryInputV1,
    FailureHistoryInputV1,
    RepositoryOverviewInputV1,
    SymbolContextInputV1,
)
from evaluation.world import NOW, PROJECT, REPOSITORY, EvalWorld, Revisions, at
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from agent_context_platform.content.service import ContentService
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.retrieval.composer import (
    ContextBudgetTooSmall,
    ContextRequestError,
    RequestContext,
    ScopeNotFound,
)
from agent_context_platform.retrieval.cursor import (
    CursorExpired,
    CursorInvalid,
    CursorMismatch,
    CursorStale,
)
from agent_context_platform.retrieval.evidence import EvidenceLedger, EvidenceScope
from agent_context_platform.retrieval.graph import RetrievalDeadlineExceeded
from agent_context_platform.retrieval.revision import RevisionNotFound
from agent_context_platform.retrieval.temporal import TemporalProjectionOutdated

from ..projection.conftest import neo4j_integration_settings
from .conftest import wipe

pytestmark = pytest.mark.integration

PRINCIPAL = RequestContext("contract-principal")
WIDE = {"max_items": 30, "max_bytes": 60_000, "max_tokens": 15_000}


@dataclass
class Seeded:
    keys: dict[str, str]
    revisions: Revisions


@pytest.fixture(scope="module")
def seeded(
    postgres_dsn: str, projection_engine: AsyncEngine, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Seeded]:
    async def seed() -> Seeded:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)
            world = EvalWorld(postgres_dsn, projection_engine, store)
            try:
                await world.reset()
                await world.seed(tmp_path_factory.mktemp("composer"))
                assert (await world.project()).dead_lettered == 0
                return Seeded(dict(world.keys), world.revisions)
            finally:
                await world.close()

    async def clean() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)

    yield asyncio.run(seed())
    asyncio.run(clean())


def run_in_world[ResultT](
    seeded: Seeded,
    dsn: str,
    owner: AsyncEngine,
    body: Callable[[EvalWorld], Awaitable[ResultT]],
) -> ResultT:
    async def run() -> ResultT:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            world = EvalWorld(dsn, owner, store, keys=dict(seeded.keys), revisions=seeded.revisions)
            try:
                return await body(world)
            finally:
                await world.close()

    return asyncio.run(run())


def task(revision: dict[str, str], text_: str = "rounding drift in add", **extra: Any) -> Any:
    return ContextForTaskInputV1.model_validate(
        {"repository_id": REPOSITORY, "revision": revision, "task": text_, "budget": WIDE} | extra
    )


def commit(oid: str) -> dict[str, str]:
    return {"kind": "commit", "commit_id": oid}


def overview(oid: str, budget: dict[str, int] | None = None, **extra: Any) -> Any:
    return RepositoryOverviewInputV1.model_validate(
        {"repository_id": REPOSITORY, "commit_id": oid, "budget": budget or WIDE} | extra
    )


def stripped(page: ContextPackageV1) -> dict[str, Any]:
    return page.model_dump(mode="json", exclude={"query_id", "generated_at"})


def test_unknown_scope_ids_fail_with_typed_errors_and_never_widen(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> None:
        composer = world.composer()
        c1 = seeded.revisions.c1
        with pytest.raises(ScopeNotFound):
            await composer.context_for_task(
                task(commit(c1)).model_copy(update={"repository_id": "repo_other"}), PRINCIPAL
            )
        with pytest.raises(RevisionNotFound):
            await composer.context_for_task(
                task({"kind": "workspace_snapshot", "snapshot_id": "snap_unknown"}), PRINCIPAL
            )
        with pytest.raises(RevisionNotFound):
            await composer.repository_overview(overview("0" * 40), PRINCIPAL)
        with pytest.raises(ScopeNotFound):
            await composer.symbol_context(
                SymbolContextInputV1.model_validate(
                    {
                        "repository_id": REPOSITORY,
                        "commit_id": c1,
                        "symbol_id": "no-such-symbol",
                        "budget": WIDE,
                    }
                ),
                PRINCIPAL,
            )
        with pytest.raises(ScopeNotFound):
            await composer.decision_history(
                DecisionHistoryInputV1.model_validate(
                    {
                        "scope": {"kind": "project", "project_id": "prj_unknown"},
                        "query": "rounding",
                        "valid_at": at(4).isoformat(),
                        "budget": WIDE,
                    }
                ),
                PRINCIPAL,
            )
        with pytest.raises(ScopeNotFound):
            await composer.failure_history(
                FailureHistoryInputV1.model_validate(
                    {
                        "repository_id": "repo_other",
                        "query": "rounding",
                        "valid_from": at(0).isoformat(),
                        "valid_to": at(4).isoformat(),
                        "budget": WIDE,
                    }
                ),
                PRINCIPAL,
            )
        with pytest.raises(ContextRequestError):
            await composer.context_for_task(task(commit(c1), "x" * 1001), PRINCIPAL)

    run_in_world(seeded, postgres_dsn, projection_engine, body)


def test_pages_are_contiguous_and_cursors_are_bound_to_query_principal_and_snapshot(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    c2 = seeded.revisions.c2
    small = {"max_items": 2, "max_bytes": 60_000, "max_tokens": 15_000}

    async def body(world: EvalWorld) -> None:
        composer = world.composer()
        everything = await composer.repository_overview(overview(c2), PRINCIPAL)
        first = await composer.repository_overview(overview(c2, small), PRINCIPAL)
        assert first.truncated and first.next_cursor is not None
        second = await composer.repository_overview(
            overview(c2, small, cursor=first.next_cursor), PRINCIPAL
        )
        assert [i.item_id for i in (*first.items, *second.items)] == [
            i.item_id for i in everything.items[:4]
        ]
        assert first.as_of_event_id == second.as_of_event_id == everything.as_of_event_id

        cursor = first.next_cursor
        tampered = cursor[:-2] + ("AA" if not cursor.endswith("AA") else "BB")
        with pytest.raises(CursorInvalid):
            await composer.repository_overview(overview(c2, small, cursor=tampered), PRINCIPAL)
        with pytest.raises(CursorMismatch):  # another budget is another query
            await composer.repository_overview(overview(c2, cursor=cursor), PRINCIPAL)
        with pytest.raises(CursorMismatch):  # another principal
            await composer.repository_overview(
                overview(c2, small, cursor=cursor), RequestContext("someone-else")
            )
        with pytest.raises(CursorMismatch):  # another tool
            await composer.context_for_task(task(commit(c2), cursor=cursor), PRINCIPAL)
        later = world.composer(clock=lambda: NOW + timedelta(days=1))
        with pytest.raises(CursorExpired):
            await later.repository_overview(overview(c2, small, cursor=cursor), PRINCIPAL)

        # One more delivered event moves the snapshot: the old cursor is stale.
        await world._decision(9, "dec_late", 7.5, "A late decision about logging.")
        assert (await world.project()).delivered == 1
        with pytest.raises(CursorStale):
            await composer.repository_overview(overview(c2, small, cursor=cursor), PRINCIPAL)

    run_in_world(seeded, postgres_dsn, projection_engine, body)


def test_budgets_are_exact_and_a_budget_below_one_item_is_a_typed_error(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    c1 = seeded.revisions.c1

    async def body(world: EvalWorld) -> None:
        composer = world.composer()
        full = await composer.repository_overview(overview(c1), PRINCIPAL)
        sizes = [len(item.model_dump_json().encode()) for item in full.items]
        assert len(sizes) >= 3
        # A byte budget that fits exactly the first two items stops before the third.
        exact = {"max_items": 30, "max_bytes": sizes[0] + sizes[1], "max_tokens": 15_000}
        page = await composer.repository_overview(overview(c1, exact), PRINCIPAL)
        assert [i.item_id for i in page.items] == [i.item_id for i in full.items[:2]]
        assert page.truncated and page.next_cursor
        # The token estimate is ceil(bytes / 4): one token short drops the second item.
        tokens = {
            "max_items": 30,
            "max_bytes": 60_000,
            "max_tokens": -(-(sizes[0] + sizes[1]) // 4) - 1,
        }
        page = await composer.repository_overview(overview(c1, tokens), PRINCIPAL)
        assert [i.item_id for i in page.items] == [full.items[0].item_id]
        with pytest.raises(ContextBudgetTooSmall):
            await composer.repository_overview(
                overview(c1, {"max_items": 5, "max_bytes": 50, "max_tokens": 50}), PRINCIPAL
            )

    run_in_world(seeded, postgres_dsn, projection_engine, body)


def test_a_snapshot_query_sees_uncommitted_code_and_a_commit_query_does_not(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> tuple[ContextPackageV1, ContextPackageV1]:
        composer = world.composer()
        dirty = await composer.context_for_task(
            task(
                {"kind": "workspace_snapshot", "snapshot_id": seeded.revisions.snapshot},
                "average helper draft",
            ),
            PRINCIPAL,
        )
        clean = await composer.context_for_task(
            task(commit(seeded.revisions.c2), "average helper draft"), PRINCIPAL
        )
        return dirty, clean

    dirty, clean = run_in_world(seeded, postgres_dsn, projection_engine, body)
    assert {"symbol pkg.report.average", "file pkg/draft.py"} <= {i.title for i in dirty.items}
    assert dirty.revision is not None and dirty.revision.kind == "workspace_snapshot"
    text_ = "\n".join(f"{i.title}\n{i.content}" for i in clean.items)
    assert "average" not in text_.replace("average helper", "") and "pkg/draft.py" not in text_
    window_end = at(5)
    cited = {str(ref.source_event_id) for item in clean.items for ref in item.evidence}
    assert seeded.keys["snapshot:symbol:pkg.report.average"] not in cited
    assert window_end > at(3)


def test_freshness_reports_the_watermark_lag_and_dead_letters(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> list[ContextPackageV1]:
        now = datetime.now(UTC)
        composer = world.composer(clock=lambda: now)
        request = overview(seeded.revisions.c2)
        settled = await composer.repository_overview(request, PRINCIPAL)
        async with world.owner.begin() as connection:
            newest = (
                await connection.execute(
                    text(
                        "SELECT outbox_id, event_id FROM projection.outbox ORDER BY outbox_id DESC LIMIT 1"
                    )
                )
            ).one()
            await connection.execute(
                text(
                    "UPDATE projection.outbox SET status = 'pending', delivered_at = NULL, "
                    "created_at = :created, updated_at = :created, available_at = :created "
                    "WHERE outbox_id = :id"
                ),
                {"id": newest.outbox_id, "created": now - timedelta(seconds=90)},
            )
            await connection.execute(
                text(
                    "INSERT INTO projection.dead_letters (outbox_id, event_id, projector_name, "
                    "projector_version, attempts, error_class, status, first_failed_at, "
                    "updated_at) SELECT outbox_id, event_id, 'code', '1', 5, 'ValueError', "
                    "'open', :now, :now FROM projection.outbox WHERE outbox_id < :id "
                    "ORDER BY outbox_id LIMIT 1"
                ),
                {"id": newest.outbox_id, "now": now},
            )
            await connection.execute(
                text(
                    "UPDATE projection.projection_checkpoints SET state = 'paused' WHERE projector_name = 'quality'"
                )
            )
        try:
            lagging = await composer.repository_overview(request, PRINCIPAL)
        finally:
            async with world.owner.begin() as connection:
                await connection.execute(text("DELETE FROM projection.dead_letters"))
                await connection.execute(
                    text(
                        "UPDATE projection.outbox SET status = 'delivered', delivered_at = now(), "
                        "updated_at = now() WHERE outbox_id = :id"
                    ),
                    {"id": newest.outbox_id},
                )
                await connection.execute(
                    text("UPDATE projection.projection_checkpoints SET state = 'active'")
                )
        return [settled, lagging, newest.event_id]  # type: ignore[list-item]

    settled, lagging, newest = run_in_world(seeded, postgres_dsn, projection_engine, body)
    assert settled.staleness_ms == 0 and str(settled.as_of_event_id) == str(newest)
    assert 90_000 <= lagging.staleness_ms < 600_000
    assert lagging.as_of_event_id != settled.as_of_event_id
    assert {"projection_dead_letters:code", "projection_paused:quality"} <= set(lagging.warnings)
    assert lagging.projection_checkpoint.startswith("pc1-")
    assert settled.projection_checkpoint != lagging.projection_checkpoint  # a state moved


def test_purged_or_foreign_content_is_never_read(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    reads: list[str] = []

    class Spy(ContentService):
        async def read(self, session: AsyncSession, ref: ContentRefV1) -> bytes:
            reads.append(ref.content_id)
            return await super().read(session, ref)

    async def body(world: EvalWorld) -> None:
        spy = Spy(world.content._blob_store, world.content._policy)  # type: ignore[attr-defined]
        ledger = EvidenceLedger(world.api, spy)
        scope = EvidenceScope(repository_id=REPOSITORY)
        purged_event = seeded.keys["content:purged-transcript"]
        events = await ledger.events([purged_event, seeded.keys["decision:dec_parser"]], scope)
        assert await ledger.snippet(events[purged_event], "purged-transcript") is None
        assert await ledger.snippet(events[purged_event], None) is None
        assert events[purged_event].purged == {"purged-transcript"}
        assert await ledger.snippet(events[seeded.keys["decision:dec_parser"]], None)
        foreign = await ledger.events(
            [seeded.keys["decision:dec_parser"]], EvidenceScope(repository_id="repo_other")
        )
        assert foreign == {}
        later = await ledger.events(
            [seeded.keys["decision:dec_average"]], EvidenceScope(project_id=PROJECT, before=at(6))
        )
        assert later == {}
        assert await ledger.events(["not-a-uuid", str(UUID(int=7))], scope) == {}
        decision = seeded.keys["decision:dec_parser"]
        async with world.owner.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE projection.outbox SET status = 'dead_lettered', delivered_at = NULL, "
                    "dead_lettered_at = now() WHERE event_id = :id"
                ),
                {"id": decision},
            )
        try:
            assert await ledger.events([decision], scope) == {}  # never cite an undelivered event
        finally:
            async with world.owner.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE projection.outbox SET status = 'delivered', delivered_at = now(), "
                        "dead_lettered_at = NULL WHERE event_id = :id"
                    ),
                    {"id": decision},
                )

        composer = world.composer(content=spy)
        page = await composer.context_for_task(
            task(commit(seeded.revisions.c1), "scratch transcript purged rounding"), PRINCIPAL
        )
        assert all("operator purged" not in item.content for item in page.items)

    run_in_world(seeded, postgres_dsn, projection_engine, body)
    assert reads and "purged-transcript" not in reads


def test_an_outdated_temporal_projection_is_a_warning_or_an_error_never_silent(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> ContextPackageV1:
        async def stale(tx: Neo4jTransaction) -> None:
            await tx.run(
                "CREATE (:Decision {decision_id: 'dec_unversioned', state_order: 1})",
                parameters={},
            )

        async def heal(tx: Neo4jTransaction) -> None:
            await tx.run(
                "MATCH (d:Decision {decision_id: 'dec_unversioned'}) DETACH DELETE d",
                parameters={},
            )

        await world.store.execute_write(stale)
        try:
            with pytest.raises(TemporalProjectionOutdated):
                await world.composer().decision_history(
                    DecisionHistoryInputV1.model_validate(
                        {
                            "scope": {"kind": "project", "project_id": PROJECT},
                            "query": "rounding",
                            "valid_at": at(4).isoformat(),
                            "budget": WIDE,
                        }
                    ),
                    PRINCIPAL,
                )
            return await world.composer().context_for_task(
                task(commit(seeded.revisions.c1)), PRINCIPAL
            )
        finally:
            await world.store.execute_write(heal)

    page = run_in_world(seeded, postgres_dsn, projection_engine, body)
    assert "temporal_projection_outdated" in page.warnings
    assert not any(item.kind == "decision" for item in page.items)


def test_the_request_deadline_is_enforced(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> None:
        with pytest.raises(RetrievalDeadlineExceeded):
            await world.composer().repository_overview(
                overview(seeded.revisions.c1), RequestContext("p", deadline_seconds=1e-6)
            )

    run_in_world(seeded, postgres_dsn, projection_engine, body)


def test_the_same_request_twice_gives_the_same_package(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> list[dict[str, Any]]:
        composer = world.composer()
        request = task(commit(seeded.revisions.c2), "flaky lint timeout")
        return [stripped(await composer.context_for_task(request, PRINCIPAL)) for _ in range(2)]

    first, second = run_in_world(seeded, postgres_dsn, projection_engine, body)
    assert first == second
    assert first["items"]
