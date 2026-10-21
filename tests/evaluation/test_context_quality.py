"""Context quality over the 30 golden questions (PLATFORM-043, design section 13).

The world (`world.py`) is seeded once per module through the real ingestion service and projection
runner; every question in `questions.json` is then answered by the real `ContextComposer` over
PostgreSQL, Neo4j and the content store. The gates:

- provenance: every item cites at least one `EvidenceRefV1` whose event exists in the ledger,
  passes integrity verification, belongs to the request scope, and whose `content_ref` is either
  the event's own sanitized, unpurged content reference or the documented metadata-only form
  (digest and length of the event's canonical preimage);
- budgets: item count, serialized bytes and the `ceil(bytes / 4)` token estimate stay within the
  question's budget on every page, and `truncated` matches `next_cursor`;
- zero unsupported claims: no forbidden claim appears in any title or content, purged content is
  never quoted, and every relation with confidence below 1 is rendered as heuristic;
- zero temporal leaks: no cited event occurred at or after the question's `not_after` bound;
- recall of the required evidence: at least 90% overall and 80% per tool;
- determinism: a second run, and a run after the graph is emptied and replayed from the ledger,
  give byte-identical packages except `query_id` and `generated_at`.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from agent_context_sdk import ContentDisposition, ContextPackageV1, verify_event
from agent_context_sdk.canonical import canonical_json_bytes
from agent_context_sdk.contracts.mcp import (
    ContextForTaskInputV1,
    DecisionHistoryInputV1,
    FailureHistoryInputV1,
    RepositoryOverviewInputV1,
    SymbolContextInputV1,
)
from integration.projection.conftest import neo4j_integration_settings
from integration.retrieval.conftest import wipe
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.ledger.repository import LedgerRepository
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.registry import projectors_with_search
from agent_context_platform.projection.verify import replay_ledger, wipe_graph
from agent_context_platform.retrieval.composer import ContextComposer, RequestContext
from agent_context_platform.retrieval.evidence import EVENT_METADATA_MEDIA_TYPE

from .world import (
    FINGERPRINT_FLAKY,
    FINGERPRINT_ROUNDING,
    PROJECT,
    REPOSITORY,
    EvalWorld,
    Revisions,
    at,
)

pytestmark = pytest.mark.integration

QUESTIONS: list[dict[str, Any]] = json.loads(
    (Path(__file__).with_name("questions.json")).read_text()
)["questions"]
PRINCIPAL = RequestContext("evaluation-principal")
_INPUTS: Mapping[str, type[BaseModel]] = {
    "context_for_task": ContextForTaskInputV1,
    "repository_overview": RepositoryOverviewInputV1,
    "symbol_context": SymbolContextInputV1,
    "decision_history": DecisionHistoryInputV1,
    "failure_history": FailureHistoryInputV1,
}


@dataclass
class Seeded:
    keys: dict[str, str]
    symbols: dict[str, str]
    other_symbols: dict[str, str]
    revisions: Revisions


@dataclass
class Answer:
    """What one question produced: its pages, or the error type it raised."""

    pages: list[ContextPackageV1] = field(default_factory=list)
    error: str | None = None

    def fingerprint(self) -> str:
        if self.error is not None:
            return f"error:{self.error}"
        return json.dumps(
            [
                page.model_dump(mode="json", exclude={"query_id", "generated_at"})
                for page in self.pages
            ],
            sort_keys=True,
        )


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
                await world.seed(tmp_path_factory.mktemp("evaluation"))
                report = await world.project()
                assert report.dead_lettered == 0 and report.retried == 0, report
                return Seeded(
                    dict(world.keys),
                    dict(world.symbols),
                    dict(world.other_symbols),
                    world.revisions,
                )
            finally:
                await world.close()

    async def clean() -> None:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            await wipe(store)

    yield asyncio.run(seed())
    asyncio.run(clean())


def _value(raw: Any, seeded: Seeded) -> Any:
    if isinstance(raw, dict):
        return {key: _value(item, seeded) for key, item in raw.items()}
    if isinstance(raw, list):
        return [_value(item, seeded) for item in raw]
    if not isinstance(raw, str):
        return raw
    if raw.startswith("@"):
        return at(float(raw[1:])).isoformat()
    if raw.startswith("{SYMBOL:") and raw.endswith("}"):
        return seeded.symbols[raw[len("{SYMBOL:") : -1]]
    if raw.startswith("{OTHER_SYMBOL:") and raw.endswith("}"):
        return seeded.other_symbols[raw[len("{OTHER_SYMBOL:") : -1]]
    named = {
        "{C1}": seeded.revisions.c1,
        "{C2}": seeded.revisions.c2,
        "{SNAPSHOT}": seeded.revisions.snapshot,
        "{OTHER_C1}": seeded.revisions.other_c1,
        "{OTHER_SNAPSHOT}": seeded.revisions.other_snapshot,
        "{REPOSITORY}": REPOSITORY,
        "{PROJECT}": PROJECT,
        "{FP_ROUNDING}": FINGERPRINT_ROUNDING,
        "{FP_FLAKY}": FINGERPRINT_FLAKY,
    }
    return named.get(raw, raw)


async def _ask(composer: ContextComposer, question: dict[str, Any], seeded: Seeded) -> Answer:
    model = _INPUTS[question["tool"]]
    payload = _value(question["input"], seeded)
    method = getattr(composer, question["tool"])
    answer = Answer()
    cursor: str | None = None
    for _ in range(question["pages"]):
        request = model.model_validate(payload | ({"cursor": cursor} if cursor else {}))
        try:
            page = await method(request, PRINCIPAL)
        except Exception as error:
            answer.error = type(error).__name__
            return answer
        answer.pages.append(page)
        cursor = page.next_cursor
        if cursor is None:
            break
    return answer


async def _answer_all(world: EvalWorld, seeded: Seeded) -> dict[str, Answer]:
    composer = world.composer()
    return {q["id"]: await _ask(composer, q, seeded) for q in QUESTIONS}


def _with_world(seeded: Seeded, dsn: str, owner: AsyncEngine, body: Any) -> Any:
    async def run() -> Any:
        async with Neo4jStore(neo4j_integration_settings()) as store:
            world = EvalWorld(dsn, owner, store, keys=dict(seeded.keys), revisions=seeded.revisions)
            try:
                return await body(world)
            finally:
                await world.close()

    return asyncio.run(run())


# --------------------------------------------------------------------------------------------
# Gates
# --------------------------------------------------------------------------------------------


@dataclass
class Ledger:
    """The stored events cited by the answers, with their scope and integrity."""

    events: dict[str, Any]


async def _ledger(world: EvalWorld, answers: Mapping[str, Answer]) -> Ledger:
    cited = {
        str(ref.source_event_id)
        for answer in answers.values()
        for page in answer.pages
        for item in page.items
        for ref in item.evidence
    }
    found: dict[str, Any] = {}
    async with world.api() as session:
        for event_id in sorted(cited):
            event = await LedgerRepository.get_event(session, UUID(event_id))
            if event is not None:
                found[event_id] = event
    return Ledger(found)


def _provenance_problems(
    page: ContextPackageV1, ledger: Ledger, question: Mapping[str, Any]
) -> list[str]:
    problems = []
    for item in page.items:
        if not item.evidence:
            problems.append(f"{item.item_id}: no evidence")
        for ref in item.evidence:
            event = ledger.events.get(str(ref.source_event_id))
            if event is None or not verify_event(event):
                problems.append(f"{item.item_id}: cites a missing or unverifiable event")
                continue
            scope = question["input"].get("scope", {"kind": "repository"})
            if scope["kind"] == "project":
                inside = event.context.project_id == PROJECT
            else:
                inside = event.context.repository_id == REPOSITORY
            if not inside:
                problems.append(f"{item.item_id}: cites an event outside the question's scope")
            content = ref.content_ref
            if content.disposition is ContentDisposition.METADATA_ONLY:
                preimage = canonical_json_bytes(
                    event.model_dump(mode="json", exclude={"integrity": {"event_sha256"}})
                )
                if (
                    content.content_id != f"event:{event.event_id}"
                    or content.media_type != EVENT_METADATA_MEDIA_TYPE
                    or content.content_sha256 != event.integrity.event_sha256
                    or content.uncompressed_bytes != len(preimage)
                    or content.inline_id != str(event.event_id)
                ):
                    problems.append(f"{item.item_id}: malformed metadata-only reference")
            elif content not in event.content_refs or content.disposition is not (
                ContentDisposition.SANITIZED
            ):
                problems.append(f"{item.item_id}: content reference is not the event's own")
            if not (ref.resource_uri or "").startswith("agent-context://events/"):
                problems.append(f"{item.item_id}: resource URI is not opaque")
    return problems


def _budget_problems(page: ContextPackageV1, budget: Mapping[str, int]) -> list[str]:
    # Exactly the `items` array as a client serializes the package.
    array = "[" + ",".join(item.model_dump_json() for item in page.items) + "]"
    assert f'"items":{array}' in page.model_dump_json()  # exactly what a client receives
    size = len(array.encode())
    problems = []
    if len(page.items) > budget["max_items"]:
        problems.append("items over budget")
    if size > budget["max_bytes"]:
        problems.append("bytes over budget")
    if math.ceil(size / 4) > budget["max_tokens"]:
        problems.append("tokens over budget")
    if page.truncated != (page.next_cursor is not None):
        problems.append("truncated does not match next_cursor")
    return problems


def _claim_problems(page: ContextPackageV1, forbidden: list[str]) -> list[str]:
    problems = []
    for item in page.items:
        text = f"{item.title}\n{item.content}"
        problems += [f"{item.item_id}: says {claim!r}" for claim in forbidden if claim in text]
        if item.content_trust != "untrusted_evidence":
            problems.append(f"{item.item_id}: content is not marked as untrusted evidence")
        uncertain = any(ref.confidence < 1.0 for ref in item.evidence)
        if item.kind == "relation" and uncertain and "heuristic" not in item.content:
            problems.append(f"{item.item_id}: inference rendered as fact")
    return problems


def _leak_problems(page: ContextPackageV1, bound: str | None, ledger: Ledger) -> list[str]:
    if bound is None:
        return []
    limit = datetime.fromisoformat(at(float(bound[1:])).isoformat())
    return [
        f"{item.item_id}: cites an event of {event.occurred_at.isoformat()}"
        for item in page.items
        for ref in item.evidence
        if (event := ledger.events.get(str(ref.source_event_id))) is not None
        and event.occurred_at >= limit
    ]


def test_the_question_set_is_complete_and_well_formed() -> None:
    assert len(QUESTIONS) == 30
    tools = [q["tool"] for q in QUESTIONS]
    assert {tool: tools.count(tool) for tool in _INPUTS} == dict.fromkeys(_INPUTS, 6)
    for question in QUESTIONS:
        assert question["forbidden_claims"], question["id"]
        assert question["required_evidence"] or question["expect_error"] or question["pages"]
        assert question["input"]["budget"]["max_items"] >= 1


def test_context_quality_gates(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def body(world: EvalWorld) -> tuple[dict[str, Answer], Ledger]:
        answers = await _answer_all(world, seeded)
        return answers, await _ledger(world, answers)

    answers, ledger = _with_world(seeded, postgres_dsn, projection_engine, body)
    problems: list[str] = []
    recall: dict[str, list[int]] = {tool: [0, 0] for tool in _INPUTS}
    for question in QUESTIONS:
        answer = answers[question["id"]]
        qid = question["id"]
        if question["expect_error"] is not None:
            if answer.error != question["expect_error"]:
                problems.append(f"{qid}: expected {question['expect_error']}, got {answer.error}")
            continue
        if answer.error is not None:
            problems.append(f"{qid}: raised {answer.error}")
            continue
        cited: set[str] = set()
        for page in answer.pages:
            problems += [f"{qid}: {p}" for p in _provenance_problems(page, ledger, question)]
            problems += [f"{qid}: {p}" for p in _budget_problems(page, question["input"]["budget"])]
            problems += [f"{qid}: {p}" for p in _claim_problems(page, question["forbidden_claims"])]
            problems += [f"{qid}: {p}" for p in _leak_problems(page, question["not_after"], ledger)]
            cited |= {str(ref.source_event_id) for item in page.items for ref in item.evidence}
        required = [seeded.keys[key] for key in question["required_evidence"]]
        missing = [k for k in question["required_evidence"] if seeded.keys[k] not in cited]
        recall[question["tool"]][0] += len(required) - len(missing)
        recall[question["tool"]][1] += len(required)
        if missing:
            print(f"{qid}: missing required evidence {missing}")

    assert problems == [], "\n".join(problems)
    found = sum(hit for hit, _ in recall.values())
    total = sum(count for _, count in recall.values())
    print("recall", {tool: f"{hit}/{count}" for tool, (hit, count) in recall.items()})
    assert found / total >= 0.9
    for tool, (hit, count) in recall.items():
        assert count == 0 or hit / count >= 0.8, tool


def test_answers_are_byte_identical_on_a_second_run_and_after_an_empty_graph_replay(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    async def twice(world: EvalWorld) -> tuple[dict[str, str], dict[str, str]]:
        first = await _answer_all(world, seeded)
        second = await _answer_all(world, seeded)
        return (
            {qid: answer.fingerprint() for qid, answer in first.items()},
            {qid: answer.fingerprint() for qid, answer in second.items()},
        )

    async def replayed(world: EvalWorld) -> dict[str, str]:
        assert await wipe_graph(world.store) > 0
        await replay_ledger(
            world.projector,
            world.store,
            projectors_with_search(world.backend()),
            delivered_only=True,
        )
        answers = await _answer_all(world, seeded)
        return {qid: answer.fingerprint() for qid, answer in answers.items()}

    first, second = _with_world(seeded, postgres_dsn, projection_engine, twice)
    assert second == first
    assert all(not value.startswith("error:") or "Not" in value for value in first.values())
    assert _with_world(seeded, postgres_dsn, projection_engine, replayed) == first


def test_answers_do_not_depend_on_the_wall_clock(
    seeded: Seeded, postgres_dsn: str, projection_engine: AsyncEngine
) -> None:
    """The same snapshot read with the fixed clock and with the real one, a second apart, gives
    the same packages except the documented time-dependent fields (open windows included)."""

    async def body(world: EvalWorld) -> tuple[dict[str, str], dict[str, str]]:
        fixed = await _answer_all(world, seeded)
        await asyncio.sleep(1.1)
        real = world.composer(clock=lambda: datetime.now(UTC))
        live = {q["id"]: await _ask(real, q, seeded) for q in QUESTIONS}
        return (
            {qid: _timeless(answer) for qid, answer in fixed.items()},
            {qid: _timeless(answer) for qid, answer in live.items()},
        )

    fixed, live = _with_world(seeded, postgres_dsn, projection_engine, body)
    assert live == fixed


def _timeless(answer: Answer) -> str:
    if answer.error is not None:
        return f"error:{answer.error}"
    volatile = {"query_id", "generated_at", "staleness_ms", "next_cursor"}
    return json.dumps(
        [page.model_dump(mode="json", exclude=volatile) for page in answer.pages], sort_keys=True
    )
