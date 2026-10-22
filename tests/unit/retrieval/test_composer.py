"""Composer internals without services (PLATFORM-043): ranking, paging, wording, evidence, freshness."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from agent_context_sdk import (
    ContentDisposition,
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    QueryBudgetV1,
    seal_event,
)

from agent_context_platform.retrieval import composer as composer_module
from agent_context_platform.retrieval.composer import (
    AmbiguousSymbol,
    ContextBudgetTooSmall,
    ContextComposer,
    ContextRequestError,
    RequestContext,
    SymbolNotFound,
    _Candidate,
    _CodeIndex,
    _content_candidate,
    _decision_candidate,
    _failure_candidates,
    _Gathered,
    _in_window,
    _overlap,
    _rank,
    _tokens,
    _without_dirty,
    tokens_for,
)
from agent_context_platform.retrieval.cursor import CursorCodec
from agent_context_platform.retrieval.evidence import (
    Citation,
    EvidenceScope,
    LedgerEvent,
    LoadedEvents,
    clean_text,
    evidence_ref,
    metadata_ref,
    resource_uri,
)
from agent_context_platform.retrieval.freshness import FreshnessReader, ProjectionNotReady
from agent_context_platform.retrieval.revision import (
    FileFact,
    RevisionView,
    Run,
    SymbolFact,
    window_bound,
)
from agent_context_platform.retrieval.search import SearchHit
from agent_context_platform.retrieval.temporal import (
    DecisionRecord,
    DecisionStanding,
    FailureRecord,
    FixPath,
    FixResolution,
    FixStatus,
    HistoricalReason,
    ObservedRun,
    RunKind,
)

pytestmark = pytest.mark.unit

T = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
EVENT = uuid.UUID("0198a4b1-98c0-7c28-ae3f-00000000f001")


def stored(event_id: uuid.UUID = EVENT, *, repository: str = "repo-1") -> Any:
    draft = EventDraftV1(
        event_id=event_id,
        event_type="quality.ci_run.completed",
        stream_id="unit-stream",
        occurred_at=T,
        observed_at=T,
        producer=ProducerV1(producer_id="unit", name="unit", version="1"),
        context=EventContextV1(project_id="prj-1", repository_id=repository),
        payload={
            "ci_run_id": "ci-1",
            "provider": "github",
            "workflow": "ci",
            "job": "test",
            "external_id": "1",
            "status": "success",
            "duration_ms": 1,
            "commit_id": "a" * 40,
        },
        redaction=EventRedactionSummaryV1(
            policy_version="1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key="unit-1",
    )
    return seal_event(draft, [], 1, None)


def ledger_event(event_id: uuid.UUID = EVENT) -> LedgerEvent:
    return LedgerEvent(stored(event_id), 100, frozenset())


def candidate(item_id: str, **fields: Any) -> _Candidate:
    base: dict[str, Any] = {
        "kind": "file",
        "title": f"title {item_id}",
        "body": f"body of {item_id}",
        "citations": (Citation(str(EVENT), "git", 1.0),),
        "tier": 5,
    }
    return _Candidate(item_id=item_id, **(base | fields))


class FakeLedger:
    def __init__(self, events: dict[str, LedgerEvent], snippet: str | None = None) -> None:
        self._events = events
        self._snippet = snippet
        self.loads: list[set[str]] = []

    async def events(self, event_ids: Any, scope: EvidenceScope) -> LoadedEvents:
        self.loads.append(set(event_ids))
        found = LoadedEvents({e: self._events[e] for e in event_ids if e in self._events})
        found.capped = len(set(event_ids)) > 3
        return found

    async def snippets(
        self, requests: list[tuple[LedgerEvent, str | None]]
    ) -> dict[tuple[str, str | None], str | None]:
        return {(item.event_id, content_id): self._snippet for item, content_id in requests}


def composer(ledger: FakeLedger) -> ContextComposer:
    fake: Any = object()
    return ContextComposer(
        graph=fake,
        graph_reader=fake,
        temporal=fake,
        search=fake,
        ledger=ledger,  # type: ignore[arg-type]
        freshness=fake,
        cursor=CursorCodec.ephemeral(),
    )


def page(
    ledger: FakeLedger, ranked: list[_Candidate], budget: QueryBudgetV1, start: int = 0
) -> Any:
    scope = EvidenceScope(repository_id="repo-1")
    return asyncio.run(composer(ledger)._page(ranked, start, budget, scope))


def test_request_context_and_composer_validate_their_bounds() -> None:
    with pytest.raises(ContextRequestError):
        RequestContext(" ")
    for bad in (0.0, -1.0, float("inf"), float("nan")):
        with pytest.raises(ContextRequestError):
            RequestContext("p", deadline_seconds=bad)
    with pytest.raises(ContextRequestError):
        ContextComposer(
            graph=None,  # type: ignore[arg-type]
            graph_reader=None,  # type: ignore[arg-type]
            temporal=None,  # type: ignore[arg-type]
            search=None,  # type: ignore[arg-type]
            ledger=None,  # type: ignore[arg-type]
            freshness=None,  # type: ignore[arg-type]
            cursor=CursorCodec.ephemeral(),
            default_deadline_seconds=0,
        )


def test_ranking_is_deterministic_and_ties_break_on_the_item_id() -> None:
    a = candidate("b:2", tier=3, match=0.5)
    b = candidate("a:1", tier=3, match=0.5)
    near = candidate("c:3", tier=2, distance=0)
    first = candidate("z:9", tier=1, priority=1)
    assert [c.item_id for c in _rank([a, near, b, first])] == ["z:9", "a:1", "b:2", "c:3"]
    assert _rank([a, b, near, first]) == _rank([first, near, b, a])


def test_merging_keeps_the_strongest_signal_and_every_citation() -> None:
    gathered = _Gathered()
    gathered.add(candidate("x", distance=2, match=0.1, citations=(Citation("e1", "git", 1.0),)))
    gathered.add(
        candidate(
            "x",
            distance=1,
            match=0.4,
            priority=2,
            exact_scope=True,
            citations=(Citation("e2", "git", 1.0),),
            excerpt=("e2", None),
        )
    )
    merged = gathered.candidates["x"]
    assert (merged.distance, merged.match, merged.priority) == (1, 0.4, 2)
    assert merged.exact_scope and merged.excerpt == ("e2", None)
    assert [c.event_id for c in merged.citations] == ["e1", "e2"]
    gathered.add(candidate("x", citations=(Citation("e3", "git", 1.0),)))
    assert gathered.candidates["x"].distance == 1


def test_a_page_stops_at_the_first_item_that_does_not_fit_and_drops_uncited_items() -> None:
    ledger = FakeLedger({str(EVENT): ledger_event()})
    uncited = candidate("orphan", citations=(Citation("missing", "git", 1.0),))
    ranked = [candidate("a"), uncited, candidate("b"), candidate("c")]
    items, position, dropped = page(
        ledger, ranked, QueryBudgetV1(max_items=2, max_bytes=10**6, max_tokens=10**6)
    )
    assert [i.item_id for i in items] == ["a", "b"] and position == 3 and dropped == 1
    # max_bytes is the exact size of the serialized items array, brackets and commas included.
    pair = "[" + ",".join(item.model_dump_json() for item in items) + "]"
    one = len(items[0].model_dump_json().encode()) + 2
    items, position, _ = page(
        ledger, ranked, QueryBudgetV1(max_items=9, max_bytes=len(pair.encode()), max_tokens=10**6)
    )
    assert [i.item_id for i in items] == ["a", "b"] and position == 3
    items, position, _ = page(
        ledger,
        ranked,
        QueryBudgetV1(max_items=9, max_bytes=len(pair.encode()) - 1, max_tokens=10**6),
    )
    assert [i.item_id for i in items] == ["a"] and position == 2  # past the dropped orphan
    items, _, _ = page(
        ledger, ranked, QueryBudgetV1(max_items=9, max_bytes=one, max_tokens=-(-one // 4))
    )
    assert [i.item_id for i in items] == ["a"]
    items, position, _ = page(
        ledger, ranked, QueryBudgetV1(max_items=9, max_bytes=10**6, max_tokens=10**6), start=2
    )
    assert [i.item_id for i in items] == ["b", "c"] and position == 4


def test_a_page_is_never_followed_by_an_empty_one() -> None:
    ledger = FakeLedger({str(EVENT): ledger_event()})
    orphans = [candidate(f"z{n}", citations=(Citation("missing", "git", 1.0),)) for n in range(3)]
    items, position, dropped = page(
        ledger,
        [candidate("a"), *orphans],
        QueryBudgetV1(max_items=1, max_bytes=10**6, max_tokens=10**6),
    )
    assert [i.item_id for i in items] == ["a"] and position == 4 and dropped == 3


def test_a_first_item_too_large_with_its_excerpt_is_sent_without_it_or_refused() -> None:
    ledger = FakeLedger({str(EVENT): ledger_event()}, snippet="x" * 400)
    quoted = candidate("q", excerpt=(str(EVENT), None))
    full = page(ledger, [quoted], QueryBudgetV1(max_items=1, max_bytes=10**6, max_tokens=10**6))[0]
    assert '"xxxx' in full[0].content
    bare = len(full[0].model_dump_json().encode()) - 300
    items, position, _ = page(
        ledger, [quoted], QueryBudgetV1(max_items=1, max_bytes=bare, max_tokens=10**6)
    )
    assert "xxxx" not in items[0].content and position == 1
    with pytest.raises(ContextBudgetTooSmall):
        page(ledger, [quoted], QueryBudgetV1(max_items=1, max_bytes=40, max_tokens=10))


def test_the_token_estimate_is_a_ceiling_of_bytes_over_four() -> None:
    assert [tokens_for(n) for n in (0, 1, 4, 5, 8)] == [0, 1, 1, 2, 2]


def test_tokens_skip_stop_words_split_identifiers_and_overlap_is_a_share() -> None:
    assert _tokens("Why does the parseLegacy rounding_drift fail? Por que a migração falha") == [
        "does",
        "parse",
        "legacy",
        "rounding",
        "drift",
        "fail",
        "migração",
        "falha",
    ]
    assert _overlap(["rounding", "add"], ("pkg.calc.add",)) == 0.5
    assert _overlap([], ("anything",)) == 0.0
    assert len(_tokens(" ".join(f"word{n}" for n in range(50)))) == 16
    assert _tokens("x" * 49 + " ok") == ["ok"]
    assert len(" or ".join(_tokens(" ".join("w" * 48 + str(n) for n in range(40))))) <= 1000


def _symbol(symbol_id: str, name: str) -> SymbolFact:
    return SymbolFact(
        symbol_id, f"rev-{symbol_id}", name, "function", "f1", "fr1", None, None, None, "", ()
    )


def _view(*symbols: SymbolFact) -> RevisionView:
    run = Run(
        "idx", "Commit", "commit:6:repo-1:" + "a" * 40, "2026-09-01T09:00:00.000000Z", (), True
    )
    files = (FileFact("f1", "fr1", "pkg/a.py", "m1", "python", None, None, ()),)
    return RevisionView("repo-1", run, None, None, (run,), files, symbols, (), False)


def test_symbol_names_resolve_exactly_then_by_short_name_and_never_ambiguously() -> None:
    code = _CodeIndex(_view(_symbol("s1", "pkg.a.load"), _symbol("s2", "pkg.b.load")))
    assert code.resolve_symbol("s1", None).qualified_name == "pkg.a.load"
    assert code.resolve_symbol(None, "pkg.b.load").symbol_id == "s2"
    with pytest.raises(AmbiguousSymbol):
        code.resolve_symbol(None, "load")
    with pytest.raises(SymbolNotFound):
        code.resolve_symbol(None, "save")
    with pytest.raises(SymbolNotFound):
        code.resolve_symbol("s9", None)
    assert code.module_name("m1") == "pkg" and code.name("m1") == "module pkg"
    assert code.name("unknown") == "unknown" and code.file_path("nope") == ""


def _decision(standing: DecisionStanding, reason: HistoricalReason | None) -> DecisionRecord:
    return DecisionRecord(
        decision_id="dec-1",
        standing=standing,
        reason=reason,
        superseded_by="dec-2" if reason is HistoricalReason.SUPERSEDED else None,
        status="accepted",
        subjects=("pkg.a",),
        valid_from=T,
        valid_to=None,
        effective_valid_to=T + timedelta(days=1) if reason else None,
        supersedes_id=None,
        recorded_at=T,
        source_event_ids=(str(EVENT),),
    )


def test_decisions_say_whether_they_are_active_and_why_not() -> None:
    at = T + timedelta(days=2)
    texts = [
        _decision_candidate(_decision(standing, reason), at).body
        for standing, reason in (
            (DecisionStanding.ACTIVE, None),
            (DecisionStanding.HISTORICAL, HistoricalReason.SUPERSEDED),
            (DecisionStanding.HISTORICAL, HistoricalReason.EXPIRED),
            (DecisionStanding.HISTORICAL, HistoricalReason.SUPERSEDED_UNDATED),
        )
    ]
    assert "ACTIVE at" in texts[0]
    assert "superseded by dec-2" in texts[1]
    assert all("2026-09-03" not in text for text in texts)  # no reading time in the content
    assert "expired at" in texts[2]
    assert "end unknown" in texts[3]


def _run(run_id: str, *, status: str, snapshot: str | None = None) -> ObservedRun:
    return ObservedRun(
        RunKind.CI_RUN,
        run_id,
        status,
        "ci",
        "test",
        T,
        None if snapshot else "a" * 40,
        snapshot,
        (),
    )


def _failure(changed: bool | None, *, snapshot: str | None = None) -> FailureRecord:
    path = FixPath(
        "fail-1", "ci-1", "ci-2", "ci", "test", T, T + timedelta(hours=1),
        None if snapshot else "b" * 40, snapshot, changed, (str(EVENT),),
    )  # fmt: skip
    return FailureRecord(
        failure_id="fail-1",
        component="pytest",
        operation="run",
        error_class="AssertionError",
        fingerprint_sha256=None,
        valid_from=T,
        recorded_at=T,
        observed_in=(_run("ci-1", status="failure", snapshot=snapshot),),
        resolution=FixResolution(FixStatus.RESOLVED, None, path),
        source_event_ids=(str(EVENT),),
    )


def test_fix_paths_never_claim_more_than_the_runs_show() -> None:
    same, other, unknown = (
        _failure_candidates(_failure(value))[1].body for value in (False, True, None)
    )
    assert "SAME commit" in same and "no code change is evidenced" in same
    assert "different revision" in other and "not attributed" in other
    assert "unknown" in unknown


def test_a_commit_query_drops_runs_and_fixes_of_dirty_snapshots() -> None:
    assert _without_dirty(_failure(True, snapshot="snap-1"), None) is None
    assert _without_dirty(_failure(True, snapshot="snap-2"), "snap-1") is None
    own = _failure(True, snapshot="snap-1")
    assert _without_dirty(own, "snap-1") == own  # the requested snapshot's own runs count
    clean = _failure(True)
    assert _without_dirty(clean, None) == clean
    mixed = replace(
        clean,
        observed_in=(
            _run("ci-1", status="failure"),
            _run("ci-3", status="failure", snapshot="s"),
        ),
        resolution=_failure(True, snapshot="s").resolution,
    )
    kept = _without_dirty(mixed, None)
    assert kept is not None and [r.run_id for r in kept.observed_in] == ["ci-1"]
    assert kept.resolution.status is FixStatus.UNKNOWN and kept.resolution.path is None
    assert kept.resolution.reason == "dirty_runs_excluded"
    # A fix path between kept runs that validated a commit survives a dropped dirty run ...
    clean_fix = replace(mixed, resolution=clean.resolution)
    survived = _without_dirty(clean_fix, None)
    assert survived is not None and survived.resolution == clean.resolution
    # ... but one that validated a dirty snapshot does not, even when every run is clean.
    dirty_fix = replace(clean, resolution=_failure(True, snapshot="s").resolution)
    unknown = _without_dirty(dirty_fix, None)
    assert unknown is not None and unknown.resolution.status is FixStatus.UNKNOWN


def test_failure_windows_count_the_first_observation_or_any_run_completion() -> None:
    failure = _failure(True)
    assert _in_window(failure, T - timedelta(hours=1), T + timedelta(hours=1))
    assert not _in_window(failure, T + timedelta(hours=2), T + timedelta(hours=3))


def test_content_candidates_are_typed_by_event_and_quote_their_own_content() -> None:
    hit = SearchHit(
        "c-1",
        0.5,
        "prj-1",
        "repo-1",
        None,
        str(EVENT),
        "quality.ci_run.completed",
        T,
        (str(EVENT),),
        "sanitized",
    )
    found = _content_candidate(hit, 0.5)
    assert (found.kind, found.tier, found.excerpt) == ("ci_output", 5, (str(EVENT), "c-1"))
    assert "the repository" in found.title and found.item_id.startswith("content:")
    other = _content_candidate(
        SearchHit(
            "c-2", 1.0, "p", "r", "s-1", str(EVENT), "agent.unknown", T, (str(EVENT),), "sanitized"
        ),
        1.0,
    )
    assert (other.kind, other.tier) == ("session_content", 1)


def test_evidence_refs_use_the_metadata_form_and_never_coerce_an_invalid_one() -> None:
    item = ledger_event()
    ref = evidence_ref(item, Citation(str(EVENT), "test", 1.0, valid_to=T - timedelta(days=1)))
    assert ref is not None and ref.valid_to is None and ref.valid_from == T
    assert ref.content_ref == metadata_ref(item)
    assert ref.content_ref.content_id == f"event:{EVENT}"
    assert ref.content_ref.content_sha256 == item.event.integrity.event_sha256
    assert ref.resource_uri == resource_uri(str(EVENT))
    assert evidence_ref(item, Citation(str(EVENT), " ", 1.0)) is None
    assert evidence_ref(item, Citation(str(EVENT), "test", 1.5)) is None
    uri = resource_uri(str(EVENT), "/home/user/secret path")
    assert "home" not in uri and uri.startswith("agent-context://events/")


def test_evidence_scopes_match_the_envelope_and_the_window() -> None:
    event = stored()
    assert EvidenceScope(repository_id="repo-1").admits(event)
    assert EvidenceScope(project_id="prj-1", repository_id="repo-1").admits(event)
    assert not EvidenceScope(repository_id="repo-2").admits(event)
    assert not EvidenceScope(project_id="prj-2").admits(event)
    assert not EvidenceScope(repository_id="repo-1", before=T).admits(event)
    assert EvidenceScope(repository_id="repo-1", before=T + timedelta(seconds=1)).admits(event)
    with pytest.raises(ValueError):
        EvidenceScope()
    late = stored().model_copy(update={"observed_at": T + timedelta(hours=1)})
    assert not EvidenceScope(
        repository_id="repo-1", recorded_before=T + timedelta(minutes=1)
    ).admits(late)
    dirty = stored().model_copy(update={"payload": {**stored().payload, "snapshot_id": "snap-1"}})
    assert EvidenceScope(repository_id="repo-1").admits(dirty)  # history tools: any run
    assert not EvidenceScope(repository_id="repo-1", revision_mode=True).admits(dirty)
    assert EvidenceScope(repository_id="repo-1", revision_mode=True, snapshot_id="snap-1").admits(
        dirty
    )
    assert not EvidenceScope(
        repository_id="repo-1", revision_mode=True, snapshot_id="snap-2"
    ).admits(dirty)


def test_clean_text_removes_control_and_format_characters_and_bounds_length() -> None:
    assert clean_text("a\x00b‮c\n\td  e", 50) == "a b c d e"
    assert clean_text("x" * 20, 10) == "x" * 9 + "…"
    assert window_bound(None) is None
    assert window_bound("2026-09-01T09:00:00.000000Z") == datetime(2026, 9, 1, 9, tzinfo=UTC)


class _Rows(list[Any]):
    def one(self) -> Any:
        return self[0]

    def first(self) -> Any:
        return self[0] if self else None


class _Outbox:
    def __init__(self, waiting: Any, mark: Any, checkpoints: list[Any], dead: list[Any]) -> None:
        self._answers = [
            _Rows([]),  # statement timeout
            _Rows([waiting]),
            _Rows([mark] if mark else []),
            *([_Scalar(T), _Scalar(None)] if mark else []),
            _Rows(checkpoints),
            _Rows(dead),
        ]

    def __call__(self) -> _Outbox:
        return self

    async def __aenter__(self) -> _Outbox:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def begin(self) -> _Outbox:
        return self

    async def execute(self, *_: object) -> Any:
        return self._answers.pop(0)


class _Scalar:
    def __init__(self, value: datetime | None) -> None:
        self.value = value

    def scalar_one(self) -> datetime | None:
        return self.value


def _row(**fields: Any) -> Any:
    return type("Row", (), fields)()


def test_freshness_needs_a_projected_event_and_reports_missing_checkpoints() -> None:
    empty = _Outbox(_row(low=None, oldest=None), None, [], [])
    with pytest.raises(ProjectionNotReady):
        asyncio.run(FreshnessReader(empty).read([("code", "1")], T))  # type: ignore[arg-type]
    future = _Outbox(
        _row(low=5, oldest=T + timedelta(seconds=5)),
        _row(outbox_id=4, event_id=EVENT),
        [_row(projector_name="code", projector_version="1", last_outbox_id=4, state="active")],
        [_row(projector_name="code", projector_version="1", open=2)],
    )
    fresh = asyncio.run(
        FreshnessReader(future).read([("code", "1"), ("search", "1")], T)  # type: ignore[arg-type]
    )
    assert fresh.staleness_ms == 0  # a clock behind the outbox never yields a negative lag
    assert fresh.as_of_event_id == str(EVENT)
    assert (fresh.as_of_outbox_id, fresh.valid_instant, fresh.known_until) == (4, T, None)
    with pytest.raises(ValueError):
        FreshnessReader(empty, statement_timeout_seconds=0)  # type: ignore[arg-type]
    assert fresh.warnings == ("projection_dead_letters:code", "projection_not_started:search")


def test_the_dependency_table_names_registered_projectors() -> None:
    from agent_context_platform.projection.registry import registered_projectors

    registered = {(p.name, p.version) for p in registered_projectors()}
    for tool in composer_module.TOOLS:
        assert set(composer_module.DEPENDENCIES[tool]) <= registered


def test_a_request_can_shorten_the_deadline_but_never_extend_it() -> None:
    from agent_context_sdk.contracts.mcp import DecisionHistoryInputV1

    from agent_context_platform.retrieval.graph import RetrievalDeadlineExceeded

    class SlowFreshness:
        async def read(self, *_: object) -> Any:
            await asyncio.sleep(1)
            raise AssertionError("the deadline should have fired first")

    fake: Any = object()
    slow = ContextComposer(
        graph=fake,
        graph_reader=fake,
        temporal=fake,
        search=fake,
        ledger=fake,
        freshness=SlowFreshness(),  # type: ignore[arg-type]
        cursor=CursorCodec.ephemeral(),
        default_deadline_seconds=0.05,
    )
    request = DecisionHistoryInputV1.model_validate(
        {
            "scope": {"kind": "project", "project_id": "p"},
            "query": "q",
            "valid_at": T.isoformat(),
            "budget": {"max_items": 1, "max_bytes": 1000, "max_tokens": 1000},
        }
    )
    with pytest.raises(RetrievalDeadlineExceeded):
        asyncio.run(slow.decision_history(request, RequestContext("p", deadline_seconds=60)))


def test_only_execution_events_of_another_snapshot_are_dirty_and_dropped_runs_take_their_observations() -> (
    None
):
    def event_of(event_type: str, payload: dict[str, Any]) -> Any:
        return stored().model_copy(update={"event_type": event_type, "payload": payload})

    commit_scope = EvidenceScope(repository_id="repo-1", revision_mode=True)
    # A code claim names the run that first observed it: committed unchanged, it stays citable.
    replayed = event_of("code.symbol.indexed", {"snapshot_id": "snap-1", "symbol_id": "s"})
    assert commit_scope.admits(replayed)
    dirty_run = event_of("quality.ci_run.completed", {"snapshot_id": "snap-1"})
    assert not commit_scope.admits(dirty_run)
    observation = event_of("knowledge.failure.observed", {"ci_run_id": "ci-dirty"})
    assert commit_scope.admits(observation)
    assert not replace(commit_scope, excluded_runs=frozenset({"ci-dirty"})).admits(observation)
    clean = event_of("knowledge.failure.observed", {"ci_run_id": "ci-clean"})
    assert replace(commit_scope, excluded_runs=frozenset({"ci-dirty"})).admits(clean)


def test_dirty_runs_are_the_runs_of_another_snapshot() -> None:
    from agent_context_platform.retrieval.composer import _dirty_runs

    mixed = replace(
        _failure(True),
        observed_in=(
            _run("ci-clean", status="failure"),
            _run("ci-own", status="failure", snapshot="snap-1"),
            _run("ci-other", status="failure", snapshot="snap-2"),
        ),
    )
    assert _dirty_runs([mixed], None) == {"ci-own", "ci-other"}
    assert _dirty_runs([mixed], "snap-1") == {"ci-other"}


def test_record_time_never_passes_the_snapshot() -> None:
    from agent_context_platform.retrieval.composer import FAR_FUTURE, _known
    from agent_context_platform.retrieval.freshness import Freshness

    def fresh(known: datetime | None) -> Freshness:
        return Freshness("e", 1, T, known, "pc1-x", 0, ())

    assert _known(fresh(None)) == FAR_FUTURE
    assert _known(fresh(T)) == T
    assert _known(fresh(T), T - timedelta(hours=1)) == T - timedelta(hours=1)
    assert _known(fresh(None), T) == T


def test_executed_output_names_the_revision_it_ran_on() -> None:
    hit = SearchHit(
        "c-1",
        1.0,
        "p",
        "repo-1",
        None,
        str(EVENT),
        "quality.ci_run.completed",
        T,
        (str(EVENT),),
        "sanitized",
    )
    body = _content_candidate(hit, 1.0, {str(EVENT): ledger_event()}).body
    assert "run on commit " + "a" * 40 in body
    snap = ledger_event().event.model_copy(
        update={
            "payload": {**ledger_event().event.payload, "commit_id": None, "snapshot_id": "s-1"}
        }
    )
    body = _content_candidate(hit, 1.0, {str(EVENT): LedgerEvent(snap, 1, frozenset())}).body
    assert "run on workspace snapshot s-1" in body
    assert "run on" not in _content_candidate(hit, 1.0, {}).body


def test_too_many_cited_events_are_reported_as_capped() -> None:
    ledger = FakeLedger({str(EVENT): ledger_event()})
    many = candidate(
        "m",
        citations=(
            *(Citation(f"e{n}", "git", 1.0) for n in range(4)),
            Citation(str(EVENT), "git", 1.0),
        ),
    )
    warnings: set[str] = set()
    scope = EvidenceScope(repository_id="repo-1")
    asyncio.run(
        composer(ledger)._page(
            [many],
            0,
            QueryBudgetV1(max_items=1, max_bytes=10**6, max_tokens=10**6),
            scope,
            {},
            warnings,
        )
    )
    assert warnings == {"evidence_capped"}


@pytest.mark.parametrize(
    ("sqlstate", "expected"), [("57014", "deadline"), ("08006", "unavailable")]
)
def test_driver_errors_become_typed_errors_without_the_statement_or_its_parameters(
    sqlstate: str, expected: str
) -> None:
    from agent_context_sdk.contracts.mcp import DecisionHistoryInputV1
    from sqlalchemy.exc import OperationalError

    from agent_context_platform.retrieval.composer import ContextUnavailable
    from agent_context_platform.retrieval.graph import RetrievalDeadlineExceeded

    class Driver(Exception):
        def __init__(self) -> None:
            super().__init__("canceling statement due to statement timeout")
            self.sqlstate = sqlstate

    class FailingFreshness:
        async def read(self, *_: object) -> Any:
            raise OperationalError(
                "SELECT secret_column FROM ledger.events WHERE q = %(query)s",
                {"query": "private task words"},
                Driver(),
            )

    fake: Any = object()
    failing = ContextComposer(
        graph=fake,
        graph_reader=fake,
        temporal=fake,
        search=fake,
        ledger=fake,
        freshness=FailingFreshness(),  # type: ignore[arg-type]
        cursor=CursorCodec.ephemeral(),
    )
    request = DecisionHistoryInputV1.model_validate(
        {
            "scope": {"kind": "project", "project_id": "p"},
            "query": "private task words",
            "valid_at": T.isoformat(),
            "budget": {"max_items": 1, "max_bytes": 1000, "max_tokens": 1000},
        }
    )
    error_type = RetrievalDeadlineExceeded if expected == "deadline" else ContextUnavailable
    with pytest.raises(error_type) as raised:
        asyncio.run(failing.decision_history(request, RequestContext("p")))
    message = str(raised.value)
    assert "SELECT" not in message and "private" not in message and "secret" not in message
    assert raised.value.__cause__ is None and raised.value.__suppress_context__
