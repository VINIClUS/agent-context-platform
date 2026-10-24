"""Snapshot isolation of context answers under producer clocks and projection lag (PLATFORM-043
review round 6).

Its own world (the evaluation world), because every test here ingests and projects more events:
backdated and fast-clocked ones, rows postponed or failed past the watermark, a fact projected in
the middle of a request. Each answer must depend on the projection snapshot only:

- `valid_instant` is the newest delivered events' time, clamped by the server's `recorded_at`, so
  one fast producer clock cannot make a future decision current;
- temporal reads never record past it (a fact projected after the freshness read is not known);
- the backlog bound (`known_until`) only counts rows of the request's scope that may be in the
  graph, and says so when it hides facts;
- an event delivered past a still-waiting row is never cited, whatever its type.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from agent_context_sdk import ContextPackageV1
from agent_context_sdk.contracts.mcp import (
    ContextForTaskInputV1,
    DecisionHistoryInputV1,
    FailureHistoryInputV1,
)
from evaluation.world import PROJECT, REPOSITORY, EvalWorld, Revisions, _content, at
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.retrieval.composer import BACKLOG_WARNING, RequestContext
from agent_context_platform.retrieval.freshness import Freshness

from ..projection.conftest import neo4j_integration_settings
from .conftest import wipe

pytestmark = pytest.mark.integration

PRINCIPAL = RequestContext("snapshot-principal")
WIDE = {"max_items": 40, "max_bytes": 80_000, "max_tokens": 20_000}
VOLATILE = {"query_id", "generated_at", "staleness_ms", "next_cursor"}


@dataclass
class Seeded:
    keys: dict[str, str]
    revisions: Revisions
    api_login: str  # see `api_login` in tests/integration/retrieval/conftest.py


@pytest.fixture(scope="module")
def seeded(
    postgres_dsn: str,
    projection_engine: AsyncEngine,
    api_login: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[Seeded]:
    async def seed() -> Seeded:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)
            world = EvalWorld(postgres_dsn, projection_engine, store, api_login)
            try:
                await world.reset()
                await world.seed(tmp_path_factory.mktemp("snapshot"))
                assert (await world.project()).dead_lettered == 0
                return Seeded(dict(world.keys), world.revisions, api_login)
            finally:
                await world.close()

    async def clean() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)

    yield asyncio.run(seed())
    asyncio.run(clean())


def run[ResultT](
    seeded: Seeded, dsn: str, owner: AsyncEngine, body: Callable[[EvalWorld], Awaitable[ResultT]]
) -> ResultT:
    async def go() -> ResultT:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            world = EvalWorld(
                dsn,
                owner,
                store,
                seeded.api_login,
                keys=dict(seeded.keys),
                revisions=seeded.revisions,
            )
            try:
                return await body(world)
            finally:
                await world.close()

    return asyncio.run(go())


def snapshot_task(seeded: Seeded, ask: str) -> ContextForTaskInputV1:
    return ContextForTaskInputV1.model_validate(
        {
            "repository_id": REPOSITORY,
            "revision": {"kind": "workspace_snapshot", "snapshot_id": seeded.revisions.snapshot},
            "task": ask,
            "budget": WIDE,
        }
    )


def cited(page: ContextPackageV1) -> set[str]:
    return {str(ref.source_event_id) for item in page.items for ref in item.evidence}


def items(page: ContextPackageV1) -> dict[str, str]:
    return {item.item_id: item.content for item in page.items}


async def hold(world: EvalWorld, event_id: str, *, attempted: bool = False) -> None:
    """Keep a row waiting: never claimed, or (`attempted`) failed once and due again later."""
    statement = "UPDATE projection.outbox SET available_at = now() + interval '1 day'"
    if attempted:
        statement += ", retry_count = 1, updated_at = created_at + interval '1 millisecond'"
    async with world.owner.begin() as connection:
        await connection.execute(text(statement + " WHERE event_id = :id"), {"id": event_id})


async def release(world: EvalWorld, *event_ids: str) -> None:
    async with world.owner.begin() as connection:
        await connection.execute(
            text("UPDATE projection.outbox SET available_at = now() WHERE event_id = ANY(:ids)"),
            {"ids": list(event_ids)},
        )
    assert (await world.project()).dead_lettered == 0


async def other_note(world: EvalWorld, number: int, name: str, hours: float, **extra: Any) -> str:
    """A session note of the OTHER project and repository (it never answers this scope)."""
    world._other = True
    try:
        await world._content_event(
            number, name, hours, f"sess_{name}", f"turn_{name}", f"{name} note", **extra
        )
    finally:
        world._other = False
    return world.keys[f"content:{name}"]


async def decision(
    world: EvalWorld,
    number: int,
    decision_id: str,
    observed: datetime,
    valid_from: datetime,
    *,
    supersedes: str | None = None,
) -> str:
    item, claim = _content(f"content_{decision_id}", f"{decision_id} about rounding in add")
    return await world._event(
        number,
        "knowledge.decision.recorded",
        observed,
        {
            "decision_id": decision_id,
            "status": "accepted",
            "supersedes_id": supersedes,
            "subjects": ["pkg.calc.add"],
            "content_id": claim.content_id,
            "valid_from": valid_from.isoformat(),
            "recorded_at": observed.isoformat(),
        },
        claims=(claim,),
        items=(item,),
    )


def test_the_open_window_is_the_newest_deliveries_time_not_the_last_event(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> tuple[str, ContextPackageV1]:
        # An offline upload delivered last: the watermark event is backdated to 1:00.
        backdated = await other_note(world, 300, "offline-upload", 1.0)
        assert (await world.project()).delivered == 1
        page = await world.composer().context_for_task(
            snapshot_task(seeded, "average helper for empty lists"), PRINCIPAL
        )
        return backdated, page

    backdated, page = run(seeded, postgres_dsn, projection_engine, body)
    assert str(page.as_of_event_id) == backdated
    # dec_average (valid from 6:00) is active in the open window S [5:00, open).
    assert "ACTIVE" in items(page)["decision:dec_average"]


def test_a_fast_producer_clock_never_makes_a_future_decision_current(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> tuple[ContextPackageV1, ContextPackageV1]:
        real = datetime.now(UTC)
        # An honest decision, recorded now, that only takes effect in two hours ...
        await decision(world, 301, "dec_future", real, real + timedelta(hours=2))
        # ... and another project's agent whose clock runs a day ahead.
        await other_note(world, 302, "fast-clock", 1.0, observed=real + timedelta(days=1))
        assert (await world.project()).delivered == 2
        composer = world.composer()
        snapshot = await composer.context_for_task(
            snapshot_task(seeded, "future rounding decision for add"), PRINCIPAL
        )
        later = await composer.decision_history(
            DecisionHistoryInputV1.model_validate(
                {
                    "scope": {"kind": "project", "project_id": PROJECT},
                    "query": "dec_future",
                    "valid_at": (real + timedelta(hours=3)).isoformat(),
                    "budget": WIDE,
                }
            ),
            PRINCIPAL,
        )
        return snapshot, later

    snapshot, later = run(seeded, postgres_dsn, projection_engine, body)
    assert "decision:dec_future" not in items(snapshot)
    # It is known, and active once its own validity starts.
    assert "ACTIVE" in items(later)["decision:dec_future"]


class _Interleaving:
    """A freshness reader that lets `act` run right after the snapshot is read: the forced
    interleaving of a projection between the PostgreSQL and the graph reads of one request."""

    def __init__(self, inner: Any, act: Callable[[], Awaitable[None]]) -> None:
        self._inner = inner
        self._act: Callable[[], Awaitable[None]] | None = act

    async def read(self, *args: Any, **kwargs: Any) -> Freshness:
        fresh: Freshness = await self._inner.read(*args, **kwargs)
        if self._act is not None:
            act, self._act = self._act, None
            await act()
        return fresh


def test_a_fact_projected_between_the_snapshot_and_the_graph_read_is_not_known(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    valid_at = datetime.now(UTC) + timedelta(hours=1)

    def history() -> DecisionHistoryInputV1:
        return DecisionHistoryInputV1.model_validate(
            {
                "scope": {"kind": "project", "project_id": PROJECT},
                "query": "rounding",
                "valid_at": valid_at.isoformat(),
                "budget": WIDE,
            }
        )

    async def body(world: EvalWorld) -> tuple[ContextPackageV1, ContextPackageV1]:
        async def supersede() -> None:
            real = datetime.now(UTC)
            await decision(world, 310, "dec_rounding_v4", real, real, supersedes="dec_rounding_v2")
            assert (await world.project()).delivered == 1

        composer = world.composer()
        composer._freshness = _Interleaving(composer._freshness, supersede)  # type: ignore[assignment]
        raced = await composer.decision_history(history(), PRINCIPAL)
        return raced, await world.composer().decision_history(history(), PRINCIPAL)

    raced, after = run(seeded, postgres_dsn, projection_engine, body)
    found = items(raced)
    assert "decision:dec_rounding_v4" not in found
    assert "ACTIVE" in found["decision:dec_rounding_v2"]
    assert "dec_rounding_v4" not in found["decision:dec_rounding_v2"]
    assert "items_without_evidence" not in raced.warnings
    # The interleaving did happen: the next request's snapshot has v4.
    assert "decision:dec_rounding_v4" in items(after)
    assert "dec_rounding_v4" in items(after)["decision:dec_rounding_v2"]


def test_a_turn_delivered_past_a_waiting_row_is_never_cited(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    ask = "zephyr quartz migration"

    async def body(world: EvalWorld) -> tuple[str, ContextPackageV1, ContextPackageV1]:
        holder = await other_note(world, 320, "turn-holder", 6.0)
        await hold(world, holder)
        await world._content_event(
            321, "zephyr-note", 7.0, "sess_zephyr", "turn_zephyr", "zephyr quartz migration note"
        )
        turn = world.keys["content:zephyr-note"]
        assert (await world.project()).delivered == 1  # the turn, past the waiting row
        held = await world.composer().context_for_task(snapshot_task(seeded, ask), PRINCIPAL)
        await release(world, holder)
        released = await world.composer().context_for_task(snapshot_task(seeded, ask), PRINCIPAL)
        return turn, held, released

    turn, held, released = run(seeded, postgres_dsn, projection_engine, body)
    assert turn not in cited(held)
    assert "zephyr" not in "\n".join(item.content for item in held.items)
    assert turn in cited(released)  # the same question finds it once the snapshot has it


def test_a_ci_run_delivered_out_of_order_is_beyond_the_snapshot(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    ask = "ZeroDivisionError in test_average"

    async def body(world: EvalWorld) -> tuple[ContextPackageV1, ContextPackageV1]:
        holder = await other_note(world, 330, "ci-holder", 5.4)
        await hold(world, holder)
        # A passing re-run on the snapshot at 5:30 (before the snapshot's own time): the fix of
        # fail_dirty, delivered past the waiting row.
        snapshot = seeded.revisions.snapshot
        await world._ci_run(331, "ci_run_dirty_pass", 5.5, "test", "success", snapshot=snapshot)
        assert (await world.project()).delivered == 1
        held = await world.composer().context_for_task(snapshot_task(seeded, ask), PRINCIPAL)
        await release(world, holder)
        released = await world.composer().context_for_task(snapshot_task(seeded, ask), PRINCIPAL)
        return held, released

    held, released = run(seeded, postgres_dsn, projection_engine, body)
    assert "resolution: unresolved" in items(held)["failure:fail_dirty"]
    assert "fix:fail_dirty" not in items(held)
    assert BACKLOG_WARNING in held.warnings
    assert "items_without_evidence" not in held.warnings
    assert "resolution: resolved" in items(released)["failure:fail_dirty"]
    assert BACKLOG_WARNING not in released.warnings


def test_the_backlog_bound_is_scoped_and_rows_never_claimed_hide_nothing(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    """Runs last: it leaves two backdated decisions delivered."""

    async def answers(world: EvalWorld) -> list[ContextPackageV1]:
        composer = world.composer()
        return [
            await composer.decision_history(
                DecisionHistoryInputV1.model_validate(
                    {
                        "scope": {"kind": "project", "project_id": PROJECT},
                        "query": "rounding",
                        "valid_at": at(4).isoformat(),
                        "budget": WIDE,
                    }
                ),
                PRINCIPAL,
            ),
            await composer.failure_history(
                FailureHistoryInputV1.model_validate(
                    {
                        "repository_id": REPOSITORY,
                        "query": "rounding",
                        "valid_from": at(0).isoformat(),
                        "valid_to": at(6).isoformat(),
                        "budget": WIDE,
                    }
                ),
                PRINCIPAL,
            ),
            await composer.context_for_task(
                ContextForTaskInputV1.model_validate(
                    {
                        "repository_id": REPOSITORY,
                        "revision": {"kind": "commit", "commit_id": seeded.revisions.c2},
                        "task": "rounding drift in add",
                        "budget": WIDE,
                    }
                ),
                PRINCIPAL,
            ),
        ]

    def stable(pages: list[ContextPackageV1]) -> list[dict[str, Any]]:
        return [page.model_dump(mode="json", exclude=VOLATILE) for page in pages]

    async def body(world: EvalWorld) -> None:
        baseline = stable(await answers(world))
        assert all(page["items"] for page in baseline)
        # Another project's decision observed a day early (an offline upload), not projected.
        world._other = True
        try:
            await world._decision(340, "dec_offline_other", -24, "An offline rounding decision.")
        finally:
            world._other = False
        other = world.keys["other:decision:dec_offline_other"]
        assert stable(await answers(world)) == baseline
        await hold(world, other, attempted=True)  # it may be in the graph now, but elsewhere
        assert stable(await answers(world)) == baseline
        # The same in this project: never claimed, it has never touched the graph ...
        await world._decision(341, "dec_offline", -24, "An offline rounding decision here.")
        own = world.keys["decision:dec_offline"]
        assert stable(await answers(world)) == baseline
        # ... claimed once, it may have: what was recorded after it is not known yet, and the
        # answers say so.
        await hold(world, own, attempted=True)
        lagging = await answers(world)
        for page in lagging:
            assert BACKLOG_WARNING in page.warnings
        assert not any(item.kind == "decision" for item in lagging[0].items)
        await release(world, other, own)

    run(seeded, postgres_dsn, projection_engine, body)
