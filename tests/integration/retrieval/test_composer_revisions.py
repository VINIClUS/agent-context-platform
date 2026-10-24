"""Revision evidence after a dirty snapshot is committed (PLATFORM-043 review round 3).

Its own world (the evaluation world plus a commit C3 of the snapshot's working tree, unchanged,
a failure seen in both a clean C2 run and a dirty run, failures observed in C2's window in dirty
runs that complete after it or never, and a failure whose fix path starts at a dirty run),
because C3 closes the snapshot's window and would change the other modules' answers.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from agent_context_sdk import ContextPackageV1
from agent_context_sdk.contracts.mcp import (
    ContextForTaskInputV1,
    FailureHistoryInputV1,
    RepositoryOverviewInputV1,
    SymbolContextInputV1,
)
from evaluation.world import REPOSITORY, EvalWorld, Revisions, at
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.retrieval.composer import RequestContext

from ..projection.conftest import neo4j_integration_settings
from .conftest import wipe

pytestmark = pytest.mark.integration

PRINCIPAL = RequestContext("revision-principal")
WIDE = {"max_items": 40, "max_bytes": 80_000, "max_tokens": 20_000}


@dataclass
class Seeded:
    keys: dict[str, str]
    revisions: Revisions
    c3: str
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
                await world.seed(tmp_path_factory.mktemp("revisions"))
                # A failure seen in a clean C2 run and in a dirty run, both inside C2's window.
                c2, snapshot = world.revisions.c2, world.revisions.snapshot
                await world._ci_run(100, "ci_mixed_clean", 3.6, "mixed", "failure", commit=c2)
                await world._failure(
                    101,
                    "fail_mixed",
                    3.61,
                    component="pytest",
                    operation="test_mixed",
                    error_class="AssertionError",
                    fingerprint="1" * 64,
                    ci_run="ci_mixed_clean",
                    details="clean mixed failure details",
                    content_id="details_fail_mixed_clean",
                )
                world.keys["failure:fail_mixed:clean"] = world.keys.pop("failure:fail_mixed")
                await world._ci_run(
                    102, "ci_mixed_dirty", 4.6, "mixed", "failure", snapshot=snapshot
                )
                await world._failure(
                    103,
                    "fail_mixed",
                    4.61,
                    component="pytest",
                    operation="test_mixed",
                    error_class="AssertionError",
                    fingerprint="1" * 64,
                    ci_run="ci_mixed_dirty",
                    details="DIRTYMARK mixed failure details from the dirty workspace",
                    content_id="details_fail_mixed_dirty",
                )
                # Observed at 4:48 in a dirty run that only completes at 5:12, after C2's window
                # [3:00, 5:00) ends: nothing at the cut says the run was dirty (review round 6).
                await world._failure(
                    104,
                    "fail_late",
                    4.8,
                    component="pytest",
                    operation="test_late",
                    error_class="TimeoutError",
                    fingerprint="2" * 64,
                    ci_run="ci_late_dirty",
                    details="DIRTYMARK2 late failure details from the dirty workspace",
                )
                await world._ci_run(105, "ci_late_dirty", 5.2, "late", "failure", snapshot=snapshot)
                # Observed at 4:50 in a run that never reports completion at all.
                await world._failure(
                    106,
                    "fail_hung",
                    4.83,
                    component="pytest",
                    operation="test_hung",
                    error_class="TimeoutError",
                    fingerprint="3" * 64,
                    ci_run="ci_never_completes",
                    details="DIRTYMARK3 hung failure details from a run that never completed",
                )
                # Failing on C2, failing again on the dirty snapshot, then passing on C2: the fix
                # path's failing run is the dirty one, so C2 cannot claim the fix.
                await world._ci_run(107, "ci_fixd_clean", 3.7, "fixd", "failure", commit=c2)
                await world._failure(
                    108,
                    "fail_fixd",
                    3.71,
                    component="pytest",
                    operation="test_fixd",
                    error_class="AssertionError",
                    fingerprint="4" * 64,
                    ci_run="ci_fixd_clean",
                    details="clean fixd failure details",
                    content_id="details_fail_fixd_clean",
                )
                world.keys["failure:fail_fixd:clean"] = world.keys.pop("failure:fail_fixd")
                await world._ci_run(109, "ci_fixd_dirty", 4.7, "fixd", "failure", snapshot=snapshot)
                await world._failure(
                    110,
                    "fail_fixd",
                    4.71,
                    component="pytest",
                    operation="test_fixd",
                    error_class="AssertionError",
                    fingerprint="4" * 64,
                    ci_run="ci_fixd_dirty",
                    details="DIRTYMARK4 fixd failure details from the dirty workspace",
                    content_id="details_fail_fixd_dirty",
                )
                await world._ci_run(111, "ci_fixd_pass", 4.9, "fixd", "success", commit=c2)
                c3 = await world.commit_worktree(6.5, "c3")
                assert (await world.project()).dead_lettered == 0
                return Seeded(dict(world.keys), world.revisions, c3, api_login)
            finally:
                await world.close()

    async def clean() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)

    yield asyncio.run(seed())
    asyncio.run(clean())


def run(seeded: Seeded, dsn: str, owner: AsyncEngine, body: object) -> object:
    async def go() -> object:
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
                return await body(world)  # type: ignore[operator]
            finally:
                await world.close()

    return asyncio.run(go())


def cited(page: ContextPackageV1) -> set[str]:
    return {str(ref.source_event_id) for item in page.items for ref in item.evidence}


def test_code_first_indexed_in_a_dirty_snapshot_backs_the_commit_that_adopts_it(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    claim = seeded.keys["snapshot:symbol:pkg.report.average"]
    # The commit made the same claim as the snapshot: its only event names the snapshot.
    assert seeded.keys["c3:symbol:pkg.report.average"] == claim

    async def body(world: EvalWorld) -> tuple[ContextPackageV1, ContextPackageV1]:
        composer = world.composer()
        symbol = await composer.symbol_context(
            SymbolContextInputV1.model_validate(
                {
                    "repository_id": REPOSITORY,
                    "commit_id": seeded.c3,
                    "symbol_name": "pkg.report.average",
                    "budget": WIDE,
                }
            ),
            PRINCIPAL,
        )
        task = await composer.context_for_task(
            ContextForTaskInputV1.model_validate(
                {
                    "repository_id": REPOSITORY,
                    "revision": {"kind": "commit", "commit_id": seeded.c3},
                    "task": "average helper in report",
                    "budget": WIDE,
                }
            ),
            PRINCIPAL,
        )
        return symbol, task

    symbol, task = run(seeded, postgres_dsn, projection_engine, body)  # type: ignore[misc]
    kinds = {item.item_id.split(":")[0] for item in symbol.items}
    assert {"symbol", "symbol_history"} <= kinds
    assert claim in cited(symbol) and "items_without_evidence" not in symbol.warnings
    assert any(item.title == "symbol pkg.report.average" for item in task.items)
    assert claim in cited(task)


def test_a_failure_observed_in_a_dirty_run_is_not_cited_or_quoted_at_a_commit(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> ContextPackageV1:
        return await world.composer().context_for_task(
            ContextForTaskInputV1.model_validate(
                {
                    "repository_id": REPOSITORY,
                    "revision": {"kind": "commit", "commit_id": seeded.revisions.c2},
                    "task": "mixed failure in test_mixed",
                    "budget": WIDE,
                }
            ),
            PRINCIPAL,
        )

    page = run(seeded, postgres_dsn, projection_engine, body)
    assert isinstance(page, ContextPackageV1)
    failure = next(item for item in page.items if item.item_id == "failure:fail_mixed")
    events = {str(ref.source_event_id) for ref in failure.evidence}
    assert seeded.keys["failure:fail_mixed:clean"] in events
    assert seeded.keys["failure:fail_mixed"] not in events  # the dirty observation
    assert seeded.keys["ci:ci_mixed_dirty"] not in cited(page)
    text = "\n".join(f"{item.title}\n{item.content}" for item in page.items)
    assert "DIRTYMARK" not in text
    assert "ci_mixed_dirty" not in text
    assert "resolution: unknown (dirty_runs_excluded)" in failure.content


def _c2_answers(seeded: Seeded, dsn: str, owner: AsyncEngine, ask: str) -> list[ContextPackageV1]:
    async def body(world: EvalWorld) -> list[ContextPackageV1]:
        composer = world.composer()
        task = await composer.context_for_task(
            ContextForTaskInputV1.model_validate(
                {
                    "repository_id": REPOSITORY,
                    "revision": {"kind": "commit", "commit_id": seeded.revisions.c2},
                    "task": ask,
                    "budget": WIDE,
                }
            ),
            PRINCIPAL,
        )
        overview = await composer.repository_overview(
            RepositoryOverviewInputV1.model_validate(
                {"repository_id": REPOSITORY, "commit_id": seeded.revisions.c2, "budget": WIDE}
            ),
            PRINCIPAL,
        )
        return [task, overview]

    answers = run(seeded, dsn, owner, body)
    assert isinstance(answers, list)
    return answers


def test_an_observation_in_a_run_not_kept_at_the_cut_never_backs_a_commit(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    """An allow-list, not a deny-list: a dirty run completing after the window (or never) is not
    visible at the cut, and its observations must not back the commit answer either."""
    observations = {seeded.keys["failure:fail_late"], seeded.keys["failure:fail_hung"]}
    for page in _c2_answers(
        seeded, postgres_dsn, projection_engine, "late timeout in test_late and hung test_hung"
    ):
        assert not observations & cited(page)
        text = "\n".join(f"{item.title}\n{item.content}" for item in page.items)
        assert "DIRTYMARK" not in text


def test_a_fix_path_that_starts_at_a_dirty_run_is_not_claimed_at_the_commit(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def history(world: EvalWorld) -> ContextPackageV1:
        return await world.composer().failure_history(
            FailureHistoryInputV1.model_validate(
                {
                    "repository_id": REPOSITORY,
                    "fingerprint": "4" * 64,
                    "valid_from": at(0).isoformat(),
                    "valid_to": at(6).isoformat(),
                    "budget": WIDE,
                }
            ),
            PRINCIPAL,
        )

    # Outside a revision every run counts: the fix path really starts at the dirty run.
    everything = run(seeded, postgres_dsn, projection_engine, history)
    assert isinstance(everything, ContextPackageV1)
    fix = next(item for item in everything.items if item.item_id == "fix:fail_fixd")
    assert "failed in run ci_fixd_dirty" in fix.content
    for page in _c2_answers(seeded, postgres_dsn, projection_engine, "fixd failure in test_fixd"):
        by_id = {item.item_id: item for item in page.items}
        assert "fix:fail_fixd" not in by_id
        failure = by_id["failure:fail_fixd"]
        assert "resolution: unknown (dirty_runs_excluded)" in failure.content
        events = {str(ref.source_event_id) for ref in failure.evidence}
        assert seeded.keys["failure:fail_fixd:clean"] in events
        assert seeded.keys["failure:fail_fixd"] not in events
        text = "\n".join(f"{item.title}\n{item.content}" for item in page.items)
        assert "DIRTYMARK" not in text and "ci_fixd_dirty" not in text
