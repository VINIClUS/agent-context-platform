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
from evaluation.world import NOW, PROJECT, REPOSITORY, EvalWorld, Revisions, _content, at
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from agent_context_platform.content.service import ContentService
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.registry import projectors_with_search
from agent_context_platform.projection.schema import history_projectors, record_projection_history
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
STATEMENT_CEILING = 40  # PostgreSQL statements of one context_for_task page (API role)
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
                task(commit(c1)).model_copy(update={"repository_id": "repo_absent"}), PRINCIPAL
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
                        "repository_id": "repo_absent",
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
        # The byte budget is the serialized items array: "[a,b]" is sizes[0] + sizes[1] + 3.
        pair = sizes[0] + sizes[1] + 3
        exact = {"max_items": 30, "max_bytes": pair, "max_tokens": 15_000}
        page = await composer.repository_overview(overview(c1, exact), PRINCIPAL)
        assert [i.item_id for i in page.items] == [i.item_id for i in full.items[:2]]
        assert page.truncated and page.next_cursor
        short = {"max_items": 30, "max_bytes": pair - 1, "max_tokens": 15_000}
        page = await composer.repository_overview(overview(c1, short), PRINCIPAL)
        assert [i.item_id for i in page.items] == [full.items[0].item_id]
        # The token estimate is ceil(bytes / 4): one token short drops the second item.
        tokens = {
            "max_items": 30,
            "max_bytes": 60_000,
            "max_tokens": -(-pair // 4) - 1,
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
            [seeded.keys["decision:dec_parser"]], EvidenceScope(repository_id="repo_absent")
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
        # A graph without the history marker (never rebuilt, or wiped by an unfinished rebuild)
        # cannot prove the temporal history complete.
        async def stale(tx: Neo4jTransaction) -> None:
            await tx.run("MATCH (m:ProjectionHistory) DETACH DELETE m", parameters={})

        async def heal() -> None:
            projectors = projectors_with_search(world.backend())
            await record_projection_history(world.store, history_projectors(projectors))

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
            await heal()

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


def test_dirty_ci_output_and_late_knowledge_back_only_the_snapshot_answer(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    probes = {seeded.keys["ci:ci_run_dirty_output"], seeded.keys["content:late-observed-note"]}

    async def body(world: EvalWorld) -> tuple[ContextPackageV1, ContextPackageV1]:
        composer = world.composer()
        ask = "average helper in report output"
        dirty = await composer.context_for_task(
            task({"kind": "workspace_snapshot", "snapshot_id": seeded.revisions.snapshot}, ask),
            PRINCIPAL,
        )
        clean = await composer.context_for_task(task(commit(seeded.revisions.c2), ask), PRINCIPAL)
        return dirty, clean

    # The dirty output never even becomes a candidate (it would be dropped as uncitable).

    dirty, clean = run_in_world(seeded, postgres_dsn, projection_engine, body)

    def cited(page: ContextPackageV1) -> set[str]:
        return {str(ref.source_event_id) for item in page.items for ref in item.evidence}

    # The snapshot's own CI output and the note (occurred at 4:00, observed at 7:54) are evidence
    # for the open snapshot window, so the probes are real candidates ...
    assert probes <= cited(dirty)
    assert any(item.kind == "ci_output" for item in dirty.items)
    # ... but at C2 (window [3:00, 5:00)) one is dirty and the other later knowledge.
    assert not probes & cited(clean)
    assert "items_without_evidence" not in clean.warnings
    text_ = "\n".join(item.content for item in clean.items)
    assert "dirty workspace test output" not in text_ and "late observed note" not in text_


def test_a_delivery_past_a_still_pending_row_stales_the_cursor_and_is_never_cited(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    def history(cursor: str | None = None) -> Any:
        return DecisionHistoryInputV1.model_validate(
            {
                "scope": {"kind": "project", "project_id": PROJECT},
                "query": "rounding",
                "valid_at": at(9).isoformat(),
                "budget": {"max_items": 1, "max_bytes": 60_000, "max_tokens": 15_000},
            }
            | ({"cursor": cursor} if cursor else {})
        )

    async def body(world: EvalWorld) -> None:
        composer = world.composer()
        first = await composer.decision_history(history(), PRINCIPAL)
        assert first.next_cursor is not None
        await world._decision(97, "dec_pending", 8.5, "A rounding decision held back.")
        await world._decision(
            98, "dec_rounding_v3", 8.6, "Round half even in add.", supersedes="dec_rounding_v2"
        )
        pending = seeded_event = world.keys["decision:dec_pending"]
        async with world.owner.begin() as connection:  # postpone the earlier row
            await connection.execute(
                text(
                    "UPDATE projection.outbox SET available_at = now() + interval '1 day' "
                    "WHERE event_id = :id"
                ),
                {"id": pending},
            )
        try:
            assert (await world.project()).delivered == 1  # only dec_rounding_v3
            later = await composer.decision_history(history(), PRINCIPAL)
            assert later.as_of_event_id == first.as_of_event_id  # the watermark did not move
            assert later.projection_checkpoint != first.projection_checkpoint
            with pytest.raises(CursorStale):
                await composer.decision_history(history(first.next_cursor), PRINCIPAL)
            everything = await composer.decision_history(
                history().model_copy(
                    update={"budget": history().budget.model_copy(update={"max_items": 30})}
                ),
                PRINCIPAL,
            )
            cited = {str(r.source_event_id) for i in everything.items for r in i.evidence}
            assert world.keys["decision:dec_rounding_v3"] not in cited  # delivered after as_of
            # Nothing v3 changed is known at the snapshot: v2 is still active, not superseded.
            by_id = {item.item_id: item for item in everything.items}
            assert "decision:dec_rounding_v3" not in by_id
            v2 = by_id["decision:dec_rounding_v2"].content
            assert "ACTIVE" in v2 and "dec_rounding_v3" not in v2
            snapshot = await composer.context_for_task(
                task(
                    {"kind": "workspace_snapshot", "snapshot_id": seeded.revisions.snapshot},
                    "rounding decision for add",
                ),
                PRINCIPAL,
            )
            active = [i for i in snapshot.items if i.item_id == "decision:dec_rounding_v2"]
            assert active and "ACTIVE" in active[0].content
        finally:
            async with world.owner.begin() as connection:
                await connection.execute(
                    text("UPDATE projection.outbox SET available_at = now() WHERE event_id = :id"),
                    {"id": seeded_event},
                )
            await world.project()

    run_in_world(seeded, postgres_dsn, projection_engine, body)


def test_answers_follow_the_snapshot_not_the_wall_clock_nor_a_producer_clock(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    """A decision whose producer clock runs ahead is known as soon as it is delivered; one that
    starts in two hours is not active in the open snapshot window, whatever the reader's clock."""

    async def body(world: EvalWorld) -> None:
        real = datetime.now(UTC)

        async def decision(
            number: int, decision_id: str, observed: datetime, start: datetime
        ) -> None:
            item, claim = _content(f"content_{decision_id}", f"{decision_id} about rounding in add")
            await world._event(
                number,
                "knowledge.decision.recorded",
                observed,
                {
                    "decision_id": decision_id,
                    "status": "accepted",
                    "subjects": ["pkg.calc.add"],
                    "content_id": claim.content_id,
                    "valid_from": start.isoformat(),
                    "recorded_at": observed.isoformat(),
                },
                claims=(claim,),
                items=(item,),
            )

        await decision(95, "dec_skewed", real + timedelta(seconds=3), real + timedelta(seconds=3))
        await decision(96, "dec_scheduled", real, real + timedelta(hours=2))
        assert (await world.project()).delivered == 2
        now, tomorrow = (
            world.composer(clock=lambda: real),
            world.composer(clock=lambda: real + timedelta(days=1)),
        )
        ask = DecisionHistoryInputV1.model_validate(
            {
                "scope": {"kind": "project", "project_id": PROJECT},
                "query": "skewed",
                "valid_at": (real + timedelta(minutes=1)).isoformat(),
                "budget": WIDE,
            }
        )
        found = await now.decision_history(ask, PRINCIPAL)
        assert "decision:dec_skewed" in {item.item_id for item in found.items}
        snapshot = task(
            {"kind": "workspace_snapshot", "snapshot_id": seeded.revisions.snapshot},
            "scheduled decision for add",
        )
        today = await now.context_for_task(snapshot, PRINCIPAL)
        later = await tomorrow.context_for_task(snapshot, PRINCIPAL)
        assert "decision:dec_scheduled" not in {item.item_id for item in later.items}
        volatile = {"query_id", "generated_at", "staleness_ms", "next_cursor"}
        assert today.model_dump(exclude=volatile) == later.model_dump(exclude=volatile)

    run_in_world(seeded, postgres_dsn, projection_engine, body)


def test_a_task_answer_needs_a_bounded_number_of_statements(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    statements: list[str] = []

    async def body(world: EvalWorld) -> ContextPackageV1:
        engine = world._engines[0].sync_engine  # the API role

        def count(*_: Any) -> None:
            statements.append("x")

        event.listen(engine, "before_cursor_execute", count)
        try:
            return await world.composer().context_for_task(
                task(commit(seeded.revisions.c2), "rounding drift in add for report totals"),
                PRINCIPAL,
            )
        finally:
            event.remove(engine, "before_cursor_execute", count)

    page = run_in_world(seeded, postgres_dsn, projection_engine, body)
    assert page.items
    print("api statements", len(statements))
    assert len(statements) <= STATEMENT_CEILING
