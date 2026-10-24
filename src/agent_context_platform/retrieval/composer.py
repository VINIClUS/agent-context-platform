"""Budgeted, provenance-rich context packages for the five memory tools (PLATFORM-043).

`ContextComposer` answers `context_for_task`, `repository_overview`, `symbol_context`,
`decision_history` and `failure_history`. Each takes the SDK `*InputV1` plus a `RequestContext`
(the principal, an optional deadline) and returns a valid SDK `ContextPackageV1`. It is
transport-neutral: the MCP tools (PLATFORM-046) bind to these methods directly.

Scope first. The explicit scope is resolved before any candidate is read, and every unknown ID
fails with a typed error (`ScopeNotFound`, `SymbolNotFound`), never by widening: a repository must
exist in the graph, a revision must have been indexed for it (`revision.py`), a project must
exist. Code is read AS INDEXED AT the revision, from the per-run facts (`RevisionView`), never from
the current pointers, so a commit query cannot see a later or dirty snapshot's code, and a
snapshot query sees its uncommitted revisions. The revision's window `[run start, next run)`
bounds everything else: decisions and failures are read at `valid_at` just before the end of the
window (an open window: the snapshot's `valid_instant`, never the wall clock) and at a record time
that never passes the snapshot (`Freshness.valid_instant` and `known_until`: a supersession, fix
or decision delivered beyond `as_of_event_id` is not known yet), and every cited event, session content included, must
have occurred AND been observed inside the window. CI and test output of a workspace snapshot
other than the requested one (dirty state, for a commit: any snapshot) is never evidence: such
execution events are not citable, such runs are dropped from failures, a failure observation
made in a run is cited only when the answer kept that run (an allow-list: a dirty run completing
after the window, or never, is not seen at the cut), and a resolution that rested on dropped runs
becomes `unknown (dirty_runs_excluded)`. Code facts are not filtered this way: a commit that adopts code
first indexed in a dirty snapshot cites that replayed claim; revision membership decides.

Candidates. Graph proximity comes from PLATFORM-040 (`neighborhood`, depth 2, around the code the
task names) and from the revision's own relations; every node 040 returns is admitted only if the
revision has it, and its evidence is the revision's. Temporal candidates come from PLATFORM-041
(active decisions, failures and fix paths); lexical and vector hits from PLATFORM-042 (and, for the
history tools, `EvidenceLedger.rank_documents` over the same lexical material, restricted to the
events the scoped temporal read returned).

Fusion is deterministic: an integer score of

    100 * priority (the tool's section order, e.g. a symbol's definition first)
  +   4 * evidence tier (SCIP, git and executed test/CI evidence 5; an accepted decision 4;
          Tree-sitter syntactic and other agent records 3; Tree-sitter heuristic 2; session text 1)
  +   6 * exact scope match (the fact belongs to the requested revision target itself)
  +  20 * graph proximity (1 / (1 + distance))
  +  40 * query match (lexical rank normalized per list, vector cosine, or name-token overlap)
  +   3 * recency (rank of the fact's time among the candidates, normalized to [0, 1])

in micro-units, ties broken by the stable `item_id`. Same input and same ledger state give a
byte-identical package except the time-dependent fields: `query_id`, `generated_at`,
`staleness_ms` (lag at the wall clock) and `next_cursor` (it carries an expiry). Item content
holds no wall-clock time.

Evidence and trust. Every item cites at least one `EvidenceRefV1` built from a real stored event
in scope (`evidence.py`); a candidate with none is dropped and reported (`items_without_evidence`).
Item `content` is `untrusted_evidence`. Control fields (`item_id`, `kind`, evidence) are built
only from IDs and structured facts. Titles name what an item is about and so carry
producer-supplied names (paths, qualified names, session IDs): they are untrusted too, cleaned of
control and format characters and bounded. Stored text (decision bodies, messages, tool output)
appears only as a quoted, bounded excerpt inside `content`. A relation whose confidence is below 1 is rendered
as heuristic with its evidence kind, confidence and extractor; it is never stated as fact.

Budgets. `max_items`, `max_bytes` (the exact UTF-8 size of the serialized `items` JSON array,
brackets and commas included, as `model_dump_json` writes it) and `max_tokens` (estimated as
`ceil(bytes / 4)` over the same bytes) are enforced exactly, together
with internal ceilings (`MAX_PAGE_ITEMS` items, `MAX_CANDIDATES` ranked candidates, the
retrieval modules' own node and row caps, and the request deadline). Items are taken in rank order
and a page stops at the first item that does not fit, so pages are contiguous. `truncated` is true
exactly when a ranked item with citable evidence remains, and `next_cursor` is then set (the SDK
invariant), so a page is never followed by an empty one. A first item that cannot fit even without
its excerpt raises `ContextBudgetTooSmall`.

Cursor. Opaque and HMAC-signed (`cursor.py`): it binds the tool, the normalized input (all fields
but the cursor), the principal, the position and the projection snapshot, with an expiry. The
snapshot is the `as_of_event_id` AND the `projection_checkpoint`, because a later row can be
delivered while an earlier one is still pending (the watermark does not move, the answer does).
Ranking is recomputed on every page, so a cursor is only valid at the same snapshot. No package
cites an event delivered after its `as_of_event_id` (outbox order).

Freshness. `as_of_event_id`, `projection_checkpoint` and `staleness_ms` come from
`freshness.py`, over the projectors each tool depends on. An outdated temporal projection
(`TemporalProjectionOutdated`) fails `decision_history` and `failure_history`, and is a warning
(`temporal_projection_outdated`) where temporal facts are only one source. Temporal record time
never passes the snapshot's own time (`valid_instant`) nor its scope's projection backlog
(`known_until`); when the backlog hides facts the snapshot's time would show, the package carries
`projection_backlog_hides_recent_facts`.

Bounds and safety. Inputs are data: no SQL, Cypher, path or URL is ever taken from them (all
statements are static and parameterized, all graph reads go through the read-only facades). The
whole request runs under one deadline (`RetrievalDeadlineExceeded`); a request may shorten the
composer's deadline, never extend it, and every PostgreSQL statement has its own timeout. A
cancelled statement is the deadline and any other driver error is `ContextUnavailable`, both
raised `from None` and content-free (a driver error carries the statement and its parameters, the
user's query words among them). Nothing is logged here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Final
from uuid import UUID

from agent_context_sdk import (
    CommitRevisionV1,
    ContextItemV1,
    ContextPackageV1,
    EvidenceRefV1,
    ProjectScopeV1,
    QueryBudgetV1,
    RepositoryScopeV1,
    WorkspaceSnapshotRevisionV1,
)
from agent_context_sdk.contracts.mcp import (
    ContextForTaskInputV1,
    DecisionHistoryInputV1,
    FailureHistoryInputV1,
    RepositoryOverviewInputV1,
    SymbolContextInputV1,
)
from agent_context_sdk.ids import new_uuid7
from pydantic import BaseModel
from sqlalchemy.exc import DBAPIError

from agent_context_platform.observability import retrieval_span
from agent_context_platform.projection.neo4j import Neo4jReadFacade
from agent_context_platform.retrieval.cursor import CursorCodec, CursorError, query_digest
from agent_context_platform.retrieval.evidence import (
    Citation,
    EvidenceLedger,
    EvidenceScope,
    LedgerEvent,
    clean_text,
    evidence_ref,
)
from agent_context_platform.retrieval.freshness import Freshness, FreshnessReader
from agent_context_platform.retrieval.graph import (
    GraphAnchorNotFound,
    GraphRequestError,
    GraphScope,
    GraphTraversalService,
    RetrievalDeadlineExceeded,
    RetrievalError,
)
from agent_context_platform.retrieval.revision import (
    AssertionFact,
    FileFact,
    RevisionReader,
    RevisionView,
    Run,
    ScopeNotFound,
    SymbolFact,
    SymbolRevisionFact,
    window_bound,
)
from agent_context_platform.retrieval.search import (
    SearchHit,
    SearchRequestError,
    SearchScope,
    SearchService,
    SearchTimeoutError,
)
from agent_context_platform.retrieval.temporal import (
    DecisionRecord,
    DecisionStanding,
    FailureRecord,
    FixResolution,
    FixStatus,
    HistoricalReason,
    ObservedRun,
    RunKind,
    TemporalDecisionNotFound,
    TemporalProjectionOutdated,
    TemporalRequestError,
    TemporalScope,
    TemporalService,
)

DEFAULT_DEADLINE_SECONDS: Final = 10.0
MAX_PAGE_ITEMS: Final = 100
LOAD_WINDOW: Final = 50  # candidates whose cited events are loaded together
MAX_CANDIDATES: Final = 500
MAX_EVIDENCE_PER_ITEM: Final = 8
MAX_QUERY_CHARS: Final = 1000
MAX_QUERY_TOKENS: Final = 16
MAX_TOKEN_CHARS: Final = 48  # 16 tokens joined by " or " stay under the search query limit
MAX_ANCHORS: Final = 5
MAX_SEARCH_HITS: Final = 50
MAX_SEARCH_PROJECTS: Final = 5
MIN_VECTOR_SCORE: Final = 0.3  # cosine; unrelated MiniLM texts score below it
VECTOR_WEIGHT: Final = 0.8  # a vector match counts a little less than a lexical one
MAX_TITLE_CHARS: Final = 160
MAX_LISTED: Final = 10
NEIGHBORHOOD_DEPTH: Final = 2
BYTES_PER_TOKEN: Final = 4
EXCERPT_LABEL: Final = "evidence excerpt (untrusted, quoted)"

TOOLS: Final = (
    "context_for_task",
    "repository_overview",
    "symbol_context",
    "decision_history",
    "failure_history",
)
# The projectors whose state each answer depends on (name, version), in registry order.
_CODE: Final = ("code", "1")
_KNOWLEDGE: Final = ("knowledge", "2")
_QUALITY: Final = ("quality", "2")
_SEARCH: Final = ("search", "1")
_PORTFOLIO: Final = ("portfolio", "1")
DEPENDENCIES: Final[Mapping[str, tuple[tuple[str, str], ...]]] = {
    "context_for_task": (_PORTFOLIO, _CODE, _KNOWLEDGE, _QUALITY, _SEARCH),
    "repository_overview": (_CODE, _KNOWLEDGE, _QUALITY),
    "symbol_context": (_CODE, _KNOWLEDGE),
    "decision_history": (_PORTFOLIO, _KNOWLEDGE, _SEARCH),
    "failure_history": (_PORTFOLIO, _KNOWLEDGE, _QUALITY, _SEARCH),
}

_TOKEN: Final = re.compile(r"[0-9a-zà-öø-ÿ]+")
_CAMEL: Final = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
# English and Portuguese function words (the content is multilingual, see `retrieval.models`).
_STOPWORD_TEXT: Final = (
    "a an and are as at be by do for from how in into is it of on or the this that to was "
    "were what when where which why with o os de da dos das em no na um uma para por que com"
)
_STOPWORDS: Final = frozenset(_STOPWORD_TEXT.split())
_ONE_MICROSECOND: Final = timedelta(microseconds=1)
QUERY_CANCELED: Final = "57014"  # PostgreSQL SQLSTATE of a statement cancelled by its timeout
# Temporal facts recorded after an earlier, still-undelivered event of the scope are hidden.
BACKLOG_WARNING: Final = "projection_backlog_hides_recent_facts"


class ContextRequestError(RetrievalError, ValueError):
    """The request is malformed or cannot be answered as asked."""


class ContextUnavailable(RetrievalError):
    """A backing store failed; the message never carries a statement or its parameters."""


class SymbolNotFound(ScopeNotFound):
    """No symbol with that ID or name exists at the revision."""


class AmbiguousSymbol(ContextRequestError):
    """The symbol name matches several symbols at the revision; pass `symbol_id`."""


class ContextBudgetTooSmall(ContextRequestError):
    """The budget cannot hold even the first ranked item."""


# Telemetry (docs/operations/telemetry.md): a typed request error is the client's, the deadline is
# the deadline, anything else (a backend failure, `ContextUnavailable`) is a server error.
CLIENT_ERRORS: Final = (ContextRequestError, ScopeNotFound, CursorError)
DEADLINE_ERRORS: Final = (RetrievalDeadlineExceeded,)


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Who asks, and how long the request may take."""

    principal_id: str
    deadline_seconds: float | None = None

    def __post_init__(self) -> None:
        if not self.principal_id.strip():
            raise ContextRequestError("principal_id is required")
        if self.deadline_seconds is not None and not (
            math.isfinite(self.deadline_seconds) and self.deadline_seconds > 0
        ):
            raise ContextRequestError("deadline must be positive")


@dataclass(frozen=True, slots=True)
class _Candidate:
    item_id: str
    kind: str
    title: str
    body: str
    citations: tuple[Citation, ...]
    tier: int
    exact_scope: bool = False
    distance: int | None = None
    match: float = 0.0
    recency: str = ""
    priority: int = 0
    # (event ID, content ID) of gated content to quote; `None` as content ID: the event's own
    excerpt: tuple[str, str | None] | None = None


@dataclass(slots=True)
class _Gathered:
    candidates: dict[str, _Candidate] = field(default_factory=dict)
    warnings: set[str] = field(default_factory=set)
    evidence: EvidenceScope | None = None  # the scope cited events must satisfy, if narrower
    loaded: dict[str, LedgerEvent] = field(default_factory=dict)  # events already loaded

    def add(self, candidate: _Candidate) -> None:
        held = self.candidates.get(candidate.item_id)
        if held is None:
            self.candidates[candidate.item_id] = candidate
            return
        distances = [d for d in (held.distance, candidate.distance) if d is not None]
        self.candidates[candidate.item_id] = replace(
            held,
            citations=tuple(dict.fromkeys((*held.citations, *candidate.citations))),
            exact_scope=held.exact_scope or candidate.exact_scope,
            distance=min(distances) if distances else None,
            match=max(held.match, candidate.match),
            priority=max(held.priority, candidate.priority),
            excerpt=held.excerpt or candidate.excerpt,
        )


@dataclass(frozen=True, slots=True)
class _Window:
    """A revision's time window and the instants temporal facts are read at."""

    start: datetime
    end: datetime | None
    at: datetime  # valid time
    known: datetime  # record time: what the platform knew, never beyond the snapshot


class _Renderer:
    """Renders ranked candidates for one page, loading cited events and excerpts a window at a
    time (events already loaded for this request, e.g. by the search filter, are reused after
    the page's scope admits them again)."""

    def __init__(
        self,
        composer: ContextComposer,
        ranked: Sequence[_Candidate],
        scope: EvidenceScope,
        loaded: Mapping[str, LedgerEvent],
        warnings: set[str],
    ) -> None:
        self._composer = composer
        self._ranked = ranked
        self._scope = scope
        self._events = {k: v for k, v in loaded.items() if scope.admits(v.event)}
        self._seen = set(loaded)  # loaded before: not citable when the scope refused it
        self._snippets: dict[tuple[str, str | None], str | None] = {}
        self._warnings = warnings
        self._loaded_to = 0

    async def _load(self, position: int) -> None:
        if position < self._loaded_to:
            return
        window = self._ranked[position : position + LOAD_WINDOW]
        wanted = {c.event_id for cand in window for c in cand.citations} - self._seen
        if wanted:
            found = await self._composer._ledger.events(wanted, self._scope)
            if found.capped:
                self._warnings.add("evidence_capped")
            self._events.update(found)
            self._seen |= wanted
        requests = {
            (owner.event_id, cand.excerpt[1]): (owner, cand.excerpt[1])
            for cand in window
            if cand.excerpt is not None
            and (owner := _excerpt_owner(cand, self._events)) is not None
            and (owner.event_id, cand.excerpt[1]) not in self._snippets
        }
        if requests:
            self._snippets.update(await self._composer._ledger.snippets(list(requests.values())))
        self._loaded_to = position + len(window)

    async def citable(self, position: int) -> bool:
        await self._load(position)
        return any(
            c.event_id in self._events and evidence_ref(self._events[c.event_id], c) is not None
            for c in self._ranked[position].citations
        )

    async def render(self, position: int, *, quote: bool) -> ContextItemV1 | None:
        await self._load(position)
        candidate = self._ranked[position]
        excerpt = None
        if quote and candidate.excerpt is not None:
            owner = _excerpt_owner(candidate, self._events)
            if owner is not None:
                excerpt = self._snippets.get((owner.event_id, candidate.excerpt[1]))
        return _render(candidate, self._events, excerpt)


def _excerpt_owner(candidate: _Candidate, events: Mapping[str, LedgerEvent]) -> LedgerEvent | None:
    """The citable event whose content an item quotes: the named one, else a cited event that
    carries the same content (or, with no content ID, any cited event)."""
    if candidate.excerpt is None:
        return None
    event_id, content_id = candidate.excerpt
    if event_id in events:
        return events[event_id]
    for citation in candidate.citations:
        if citation.event_id in events and (
            content_id is None or citation.content_id == content_id
        ):
            return events[citation.event_id]
    return None


def _render(
    candidate: _Candidate, events: Mapping[str, LedgerEvent], excerpt: str | None
) -> ContextItemV1 | None:
    refs: dict[str, EvidenceRefV1] = {}
    for citation in candidate.citations:
        found = events.get(citation.event_id)
        ref = None if found is None else evidence_ref(found, citation)
        if ref is not None:
            refs.setdefault(ref.model_dump_json(), ref)
    if not refs:
        return None
    evidence = tuple(refs[key] for key in sorted(refs)[:MAX_EVIDENCE_PER_ITEM])
    content = candidate.body
    if excerpt is not None:
        content += f"\n{EXCERPT_LABEL}: " + json.dumps(excerpt, ensure_ascii=False)
    return ContextItemV1(
        item_id=candidate.item_id,
        kind=candidate.kind,
        title=clean_text(candidate.title, MAX_TITLE_CHARS) or candidate.kind,
        content=content,
        evidence=evidence,
    )


class ContextComposer:
    """Composes `ContextPackageV1` answers from the retrieval primitives."""

    def __init__(
        self,
        *,
        graph: GraphTraversalService,
        graph_reader: Neo4jReadFacade,
        temporal: TemporalService,
        search: SearchService,
        ledger: EvidenceLedger,
        freshness: FreshnessReader,
        cursor: CursorCodec,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        default_deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
        vector_search: bool = True,
    ) -> None:
        """`vector_search=False` for a lexical-only deployment (no embedding provider): the
        vector leg is skipped, not attempted and reported as a failed search."""
        if not (math.isfinite(default_deadline_seconds) and default_deadline_seconds > 0):
            raise ContextRequestError("deadline must be positive")
        self._graph = graph
        self._revisions = RevisionReader(graph_reader)
        self._temporal = temporal
        self._search = search
        self._ledger = ledger
        self._freshness = freshness
        self._cursor = cursor
        self._clock = clock
        self._deadline = default_deadline_seconds
        self._vector = vector_search

    # --- the five tools ---

    async def context_for_task(
        self, request: ContextForTaskInputV1, context: RequestContext
    ) -> ContextPackageV1:
        _check_text(request.task)
        return await self._compose(
            "context_for_task",
            request,
            context,
            RepositoryScopeV1(kind="repository", repository_id=request.repository_id),
            request.revision,
            EvidenceScope(repository_id=request.repository_id),
            lambda fresh: self._task_candidates(request, fresh),
        )

    async def repository_overview(
        self, request: RepositoryOverviewInputV1, context: RequestContext
    ) -> ContextPackageV1:
        revision = CommitRevisionV1(kind="commit", commit_id=request.commit_id)
        return await self._compose(
            "repository_overview",
            request,
            context,
            RepositoryScopeV1(kind="repository", repository_id=request.repository_id),
            revision,
            EvidenceScope(repository_id=request.repository_id),
            lambda fresh: self._overview_candidates(request.repository_id, revision, fresh),
        )

    async def symbol_context(
        self, request: SymbolContextInputV1, context: RequestContext
    ) -> ContextPackageV1:
        if request.symbol_name is not None:
            _check_text(request.symbol_name)
        revision = CommitRevisionV1(kind="commit", commit_id=request.commit_id)
        return await self._compose(
            "symbol_context",
            request,
            context,
            RepositoryScopeV1(kind="repository", repository_id=request.repository_id),
            revision,
            EvidenceScope(repository_id=request.repository_id),
            lambda fresh: self._symbol_candidates(request, revision, fresh),
        )

    async def decision_history(
        self, request: DecisionHistoryInputV1, context: RequestContext
    ) -> ContextPackageV1:
        _check_text(request.query)
        if isinstance(request.scope, ProjectScopeV1):
            scope = EvidenceScope(project_id=request.scope.project_id)
        else:
            scope = EvidenceScope(repository_id=request.scope.repository_id)
        return await self._compose(
            "decision_history",
            request,
            context,
            request.scope,
            None,
            scope,
            lambda fresh: self._decision_candidates(request, fresh),
        )

    async def failure_history(
        self, request: FailureHistoryInputV1, context: RequestContext
    ) -> ContextPackageV1:
        if request.query is not None:
            _check_text(request.query)
        return await self._compose(
            "failure_history",
            request,
            context,
            RepositoryScopeV1(kind="repository", repository_id=request.repository_id),
            None,
            EvidenceScope(repository_id=request.repository_id),
            lambda fresh: self._failure_candidates(request, fresh),
        )

    # --- the shared pipeline ---

    async def _compose(
        self,
        tool: str,
        request: BaseModel,
        context: RequestContext,
        scope: ProjectScopeV1 | RepositoryScopeV1,
        revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1 | None,
        evidence_scope: EvidenceScope,
        gather: Callable[[Freshness], Awaitable[_Gathered]],
    ) -> ContextPackageV1:
        """One `operation=<tool>` retrieval sample and span around the whole request."""
        with retrieval_span(tool, client_errors=CLIENT_ERRORS, deadline_errors=DEADLINE_ERRORS):
            return await self._answer(
                tool, request, context, scope, revision, evidence_scope, gather
            )

    async def _answer(
        self,
        tool: str,
        request: BaseModel,
        context: RequestContext,
        scope: ProjectScopeV1 | RepositoryScopeV1,
        revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1 | None,
        evidence_scope: EvidenceScope,
        gather: Callable[[Freshness], Awaitable[_Gathered]],
    ) -> ContextPackageV1:
        deadline = min(context.deadline_seconds or self._deadline, self._deadline)
        budget: QueryBudgetV1 = getattr(request, "budget")  # noqa: B009 (every tool input has one)
        cursor: str | None = getattr(request, "cursor")  # noqa: B009
        now = self._clock()
        try:
            async with asyncio.timeout(deadline):
                freshness = await self._freshness.read(
                    DEPENDENCIES[tool],
                    now,
                    project_id=getattr(scope, "project_id", None),
                    repository_id=getattr(scope, "repository_id", None),
                )
                digest = query_digest(tool, request, context.principal_id)
                start = 0
                if cursor is not None:
                    start = self._cursor.decode(
                        cursor,
                        tool=tool,
                        digest=digest,
                        as_of_event_id=_snapshot_token(freshness),
                        now=now,
                    ).position
                gathered = await gather(freshness)
                ranked = _rank(gathered.candidates.values())
                if len(ranked) > MAX_CANDIDATES:
                    ranked = ranked[:MAX_CANDIDATES]
                    gathered.warnings.add("candidates_capped")
                cited = replace(
                    gathered.evidence or evidence_scope, max_outbox_id=freshness.as_of_outbox_id
                )
                items, next_position, dropped = await self._page(
                    ranked, start, budget, cited, gathered.loaded, gathered.warnings
                )
        except TimeoutError:
            raise RetrievalDeadlineExceeded("context request exceeded its deadline") from None
        except DBAPIError as error:
            # A driver error carries the statement and its parameters (the user's query words
            # among them): never let it travel. A cancelled statement is the deadline.
            if getattr(error.orig, "sqlstate", None) == QUERY_CANCELED:
                raise RetrievalDeadlineExceeded("context request exceeded its deadline") from None
            raise ContextUnavailable("the ledger could not be read") from None
        if dropped:
            gathered.warnings.add("items_without_evidence")
        next_cursor = None
        if next_position < len(ranked):
            next_cursor = self._cursor.encode(
                tool=tool,
                digest=digest,
                position=next_position,
                as_of_event_id=_snapshot_token(freshness),
                now=now,
            )
        return _package(
            scope, revision, budget, freshness, items, next_cursor, gathered.warnings, now
        )

    async def _page(
        self,
        ranked: Sequence[_Candidate],
        start: int,
        budget: QueryBudgetV1,
        scope: EvidenceScope,
        loaded: Mapping[str, LedgerEvent] | None = None,
        warnings: set[str] | None = None,
    ) -> tuple[list[ContextItemV1], int, int]:
        """Items from `start` in rank order, and the position after the last one taken.

        `max_bytes` is the exact UTF-8 size of the serialized `items` array (`[`, `,`, `]`
        included, as `ContextPackageV1.model_dump_json` writes it); `max_tokens` bounds the same
        bytes with the documented `ceil(bytes / 4)` estimate. The returned position skips every
        following candidate that has no citable evidence, so a page is never followed by an
        empty one.
        """
        max_items = min(budget.max_items, MAX_PAGE_ITEMS)
        max_bytes = min(budget.max_bytes, budget.max_tokens * BYTES_PER_TOKEN)
        renderer = _Renderer(
            self, ranked, scope, loaded or {}, set() if warnings is None else warnings
        )
        items: list[ContextItemV1] = []
        used = 2  # the array's brackets
        dropped = 0
        position = start
        while position < len(ranked) and len(items) < max_items:
            item = await renderer.render(position, quote=True)
            if item is None:
                dropped += 1
                position += 1
                continue
            size = _size(item) + (1 if items else 0)  # a separating comma after the first
            if used + size > max_bytes:
                if items:
                    return items, position, dropped
                item = await renderer.render(position, quote=False)
                assert item is not None  # the same citations resolved a moment ago
                size = _size(item)
                if used + size > max_bytes:
                    raise ContextBudgetTooSmall("the budget cannot hold the first item")
            items.append(item)
            used += size
            position += 1
        while position < len(ranked) and not await renderer.citable(position):
            dropped += 1
            position += 1
        return items, position, dropped

    # --- scope resolution ---

    async def _view(
        self, repository_id: str, revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1
    ) -> RevisionView:
        if not await self._revisions.repository_exists(repository_id):
            raise ScopeNotFound("the repository is unknown")
        return await self._revisions.view(repository_id, revision)

    def _window(self, view: RevisionView, freshness: Freshness, gathered: _Gathered) -> _Window:
        """The revision's window: a closed one is read just before its end; an open one at the
        snapshot's `valid_instant`. Record time is never beyond the snapshot (`_known`)."""
        start = window_bound(view.run.occurred_at)
        end = window_bound(view.window_end)
        assert start is not None
        at = max(start, freshness.valid_instant if end is None else end - _ONE_MICROSECOND)
        return _Window(start, end, at, _known(freshness, gathered, at))

    @staticmethod
    def _revision_scope(
        repository_id: str,
        revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1,
        window: _Window,
        freshness: Freshness,
    ) -> EvidenceScope:
        snapshot = None if isinstance(revision, CommitRevisionV1) else revision.snapshot_id
        return EvidenceScope(
            repository_id=repository_id,
            before=window.end,
            recorded_before=window.end,
            revision_mode=True,
            snapshot_id=snapshot,
            max_outbox_id=freshness.as_of_outbox_id,
        )

    # --- context_for_task ---

    async def _task_candidates(
        self, request: ContextForTaskInputV1, freshness: Freshness
    ) -> _Gathered:
        repository_id = request.repository_id
        view = await self._view(repository_id, request.revision)
        gathered = _Gathered()
        window = self._window(view, freshness, gathered)
        scope = self._revision_scope(repository_id, request.revision, window, freshness)
        gathered.evidence = scope
        _view_warnings(view, gathered)
        tokens = _tokens(request.task)
        snapshot = None if isinstance(request.revision, CommitRevisionV1) else scope.snapshot_id
        code = _CodeIndex(view)
        matches = code.matches(tokens)
        anchors = [node for node, _ in matches[:MAX_ANCHORS]]
        distances = {node: 0 for node in anchors}
        if view.window_end is None:
            # The current graph IS this revision only when no later run exists; otherwise its
            # edges could pull nodes in through later (or dirty) relations.
            await self._current_proximity(repository_id, anchors, code, distances, gathered)
        for node_id, distance in code.bfs(anchors, NEIGHBORHOOD_DEPTH).items():
            distances[node_id] = min(distances.get(node_id, distance), distance)
        match_of = dict(matches)
        for node_id, distance in sorted(distances.items()):
            gathered.add(
                code.candidate(node_id, distance=distance, match=match_of.get(node_id, 0.0))
            )
        for relation in view.relations():
            ends = [distances.get(relation.subject_id), distances.get(relation.object_id or "")]
            near = [d for d in ends if d is not None]
            if len(near) == 2:
                gathered.add(code.relation(relation, distance=min(near)))

        temporal = TemporalScope(repository_id=repository_id)
        decisions = await self._decisions(temporal, window.at, window.known, False, gathered)
        failures = await self._failures(temporal, window.at, window.known, gathered)
        scope = replace(scope, kept_runs=_kept_runs(failures, snapshot))
        gathered.evidence = scope
        failures = [clean for f in failures if (clean := _without_dirty(f, snapshot)) is not None]
        hits = await self._search_hits(repository_id, request.task, tokens, scope, gathered)
        hit_match = {event: score for hit, score in hits for event in hit.source_event_ids}
        decision_events = {e for d in decisions for e in d.source_event_ids}
        failure_events = {e for f in failures for e in f.source_event_ids}
        for record in decisions:
            match = max(
                [_overlap(tokens, (record.decision_id, *record.subjects))]
                + [hit_match.get(e, 0.0) for e in record.source_event_ids]
            )
            gathered.add(_decision_candidate(record, window.at, match=match))
        for failure in failures:
            match = max(
                [_overlap(tokens, _failure_words(failure))]
                + [hit_match.get(e, 0.0) for e in failure.source_event_ids]
            )
            if failure.resolution.status is FixStatus.RESOLVED and match == 0.0:
                continue
            for candidate in _failure_candidates(failure, match=match):
                gathered.add(candidate)
        for hit, score in hits:
            if decision_events.intersection(hit.source_event_ids) or failure_events.intersection(
                hit.source_event_ids
            ):
                continue  # the decision or failure item already carries this content
            if hit.event_type.startswith(("knowledge.decision", "knowledge.failure")):
                continue  # not active or not visible at the revision: never a leak
            gathered.add(_content_candidate(hit, score, gathered.loaded))
        return gathered

    async def _current_proximity(
        self,
        repository_id: str,
        anchors: Sequence[str],
        code: _CodeIndex,
        distances: dict[str, int],
        gathered: _Gathered,
    ) -> None:
        """Distances from PLATFORM-040's neighborhood, kept only for nodes of the revision."""
        for anchor in anchors:
            try:
                reached = await self._graph.neighborhood(
                    GraphScope(repository_id), anchor, NEIGHBORHOOD_DEPTH
                )
            except (GraphAnchorNotFound, GraphRequestError):
                gathered.warnings.add("graph_anchor_not_current")
                continue
            if reached.truncated:
                gathered.warnings.add("graph_neighborhood_capped")
            for node in reached.nodes:
                if code.has(node.node_id):
                    held = distances.get(node.node_id, node.distance)
                    distances[node.node_id] = min(held, node.distance)

    async def _search_hits(
        self,
        repository_id: str,
        task: str,
        tokens: Sequence[str],
        scope: EvidenceScope,
        gathered: _Gathered,
    ) -> list[tuple[SearchHit, float]]:
        """Lexical and vector hits whose content some citable event of the revision carries.

        A search row only knows when its content occurred; whether an event that carries it was
        recorded inside the window, is delivered by the snapshot, and is not output of another
        (dirty) workspace is the ledger's to say (`EvidenceScope`), so every hit is checked there.
        """
        projects = await self._revisions.projects_of(repository_id, MAX_SEARCH_PROJECTS + 1)
        if not projects:
            gathered.warnings.add("search_scope_unresolved")
            return []
        if len(projects) > MAX_SEARCH_PROJECTS:
            projects = projects[:MAX_SEARCH_PROJECTS]
            gathered.warnings.add("search_projects_capped")
        lexical: list[SearchHit] = []
        vector: list[SearchHit] = []
        for project in projects:
            search_scope = SearchScope(project, repository_id)
            if tokens:
                query = " or ".join(tokens)
                with _search_span():
                    lexical += await self._search.lexical(query, search_scope, MAX_SEARCH_HITS)
            if not self._vector:
                continue
            try:
                with _search_span():
                    vector += await self._search.vector(task, search_scope, MAX_SEARCH_HITS)
            except SearchRequestError:
                gathered.warnings.add("vector_search_unavailable")
        citable = await self._ledger.events(
            {e for hit in (*lexical, *vector) for e in (hit.event_id, *hit.source_event_ids)},
            scope,
        )
        gathered.loaded.update(citable)
        if citable.capped:
            gathered.warnings.add("evidence_capped")

        def admitted(hit: SearchHit) -> SearchHit | None:
            events = tuple(sorted(e for e in {hit.event_id, *hit.source_event_ids} if e in citable))
            return replace(hit, source_event_ids=events) if events else None

        found: dict[str, tuple[SearchHit, float]] = {}

        def keep(hit: SearchHit, score: float) -> None:
            held = found.get(hit.content_id)
            if held is None or score > held[1]:
                found[hit.content_id] = (hit, score)

        kept = [hit for hit in map(admitted, lexical) if hit is not None]
        top = max((hit.score for hit in kept), default=0.0)
        for hit in kept:
            keep(hit, _quantize(hit.score / top) if top > 0 else 0.0)
        for raw in vector:
            near = admitted(raw)
            if near is not None and near.score >= MIN_VECTOR_SCORE:  # below: noise, not relevance
                keep(near, _quantize(min(1.0, near.score) * VECTOR_WEIGHT))
        return [found[key] for key in sorted(found)]

    # --- repository_overview ---

    async def _overview_candidates(
        self, repository_id: str, revision: CommitRevisionV1, freshness: Freshness
    ) -> _Gathered:
        view = await self._view(repository_id, revision)
        gathered = _Gathered()
        window = self._window(view, freshness, gathered)
        gathered.evidence = self._revision_scope(repository_id, revision, window, freshness)
        _view_warnings(view, gathered)
        code = _CodeIndex(view)
        gathered.add(code.summary(revision.commit_id))
        for candidate in code.modules():
            gathered.add(candidate)
        for dependency in view.dependencies():
            gathered.add(code.dependency(dependency))
        if view.previous is not None:
            before = await self._revisions.run_files(repository_id, view.previous)
            for candidate in code.changes(view.previous, before):
                gathered.add(candidate)
        temporal = TemporalScope(repository_id=repository_id)
        for record in await self._decisions(temporal, window.at, window.known, False, gathered):
            gathered.add(_decision_candidate(record, window.at, priority=3))
        failures = await self._failures(temporal, window.at, window.known, gathered)
        if gathered.evidence is not None:
            gathered.evidence = replace(gathered.evidence, kept_runs=_kept_runs(failures, None))
        for failure in failures:
            clean = _without_dirty(failure, None)
            if clean is None or clean.resolution.status is FixStatus.RESOLVED:
                continue
            for candidate in _failure_candidates(clean, priority=3):
                gathered.add(candidate)
        return gathered

    # --- symbol_context ---

    async def _symbol_candidates(
        self, request: SymbolContextInputV1, revision: CommitRevisionV1, freshness: Freshness
    ) -> _Gathered:
        view = await self._view(request.repository_id, revision)
        gathered = _Gathered()
        window = self._window(view, freshness, gathered)
        gathered.evidence = self._revision_scope(request.repository_id, revision, window, freshness)
        _view_warnings(view, gathered)
        code = _CodeIndex(view)
        symbol = code.resolve_symbol(request.symbol_id, request.symbol_name)
        gathered.add(code.candidate(symbol.symbol_id, distance=0, match=1.0, priority=5))
        distances = code.bfs([symbol.symbol_id], NEIGHBORHOOD_DEPTH)
        for relation in view.relations():
            ends = [distances.get(relation.subject_id), distances.get(relation.object_id or "")]
            if None not in ends:
                near = min(d for d in ends if d is not None)
                priority = 4 if symbol.symbol_id in (relation.subject_id, relation.object_id) else 2
                gathered.add(code.relation(relation, distance=near, priority=priority))
        history = await self._revisions.symbol_history(view, symbol.symbol_id)
        if history:
            gathered.add(code.history(symbol, history))
        names = {
            symbol.symbol_id,
            symbol.qualified_name,
            symbol.file_id,
            code.file_path(symbol.file_id),
            code.module_of(symbol.file_id),
        }
        temporal = TemporalScope(repository_id=request.repository_id)
        for record in await self._decisions(temporal, window.at, window.known, True, gathered):
            if names.intersection(record.subjects):
                gathered.add(_decision_candidate(record, window.at, priority=3, match=1.0))
        return gathered

    # --- decision_history ---

    async def _decision_candidates(
        self, request: DecisionHistoryInputV1, freshness: Freshness
    ) -> _Gathered:
        if isinstance(request.scope, ProjectScopeV1):
            temporal = TemporalScope(project_id=request.scope.project_id)
            exists = await self._revisions.project_exists(request.scope.project_id)
        else:
            temporal = TemporalScope(repository_id=request.scope.repository_id)
            exists = await self._revisions.repository_exists(request.scope.repository_id)
        if not exists:
            raise ScopeNotFound("the project or repository is unknown")
        gathered = _Gathered()
        # Known as of the projection snapshot, not the wall clock: the same snapshot, the same answer.
        known = _known(freshness, gathered)
        with _temporal_span():
            result = await self._temporal.decisions(temporal, request.valid_at, known, True)
        if result.truncated:
            gathered.warnings.add("decisions_capped")
        records = list(result.decisions)
        tokens = _tokens(request.query)
        lexical = await self._ledger.rank_documents(
            [e for r in records for e in r.source_event_ids], " or ".join(tokens), limit=200
        )
        top = max(lexical.values(), default=0.0)
        match: dict[str, float] = {}
        for record in records:
            scores = [_overlap(tokens, (record.decision_id, *record.subjects))]
            scores += [_quantize(lexical[e] / top) for e in record.source_event_ids if e in lexical]
            match[record.decision_id] = max(scores)
        chosen = {d for d, score in match.items() if score > 0}
        if not chosen:
            gathered.warnings.add("query_matched_nothing")
            chosen = set(match)
        by_id = {r.decision_id: r for r in records}
        frontier = set(chosen)
        while frontier:  # close over supersession links, both ways, among the returned decisions
            reached = {
                link
                for decision_id in frontier
                for link in (by_id[decision_id].superseded_by, by_id[decision_id].supersedes_id)
                if link in by_id
            } | {r.decision_id for r in records if {r.superseded_by, r.supersedes_id} & frontier}
            frontier = reached - chosen
            chosen |= frontier
        for decision_id in sorted(chosen):
            record = by_id[decision_id]
            priority = 1 if record.standing is DecisionStanding.ACTIVE else 0
            gathered.add(
                _decision_candidate(
                    record, request.valid_at, match=match[decision_id], priority=priority
                )
            )
        return gathered

    # --- failure_history ---

    async def _failure_candidates(
        self, request: FailureHistoryInputV1, freshness: Freshness
    ) -> _Gathered:
        if not await self._revisions.repository_exists(request.repository_id):
            raise ScopeNotFound("the repository is unknown")
        gathered = _Gathered(
            evidence=EvidenceScope(repository_id=request.repository_id, before=request.valid_to)
        )
        temporal = TemporalScope(repository_id=request.repository_id)
        at = request.valid_to - _ONE_MICROSECOND
        with _temporal_span():
            result = await self._temporal.failures(temporal, at, _known(freshness, gathered), True)
        if result.truncated:
            gathered.warnings.add("failures_capped")
        failures = [f for f in result.failures if _in_window(f, request.valid_from, at)]
        if request.fingerprint is not None:
            chosen = {
                f.failure_id: 1.0 for f in failures if f.fingerprint_sha256 == request.fingerprint
            }
        else:
            tokens = _tokens(request.query or "")
            lexical = await self._ledger.rank_documents(
                [e for f in failures for e in f.source_event_ids], " or ".join(tokens), limit=200
            )
            top = max(lexical.values(), default=0.0)
            scored = {
                f.failure_id: max(
                    [_overlap(tokens, _failure_words(f))]
                    + [_quantize(lexical[e] / top) for e in f.source_event_ids if e in lexical]
                )
                for f in failures
            }
            chosen = {k: v for k, v in scored.items() if v > 0}
            if not chosen:
                gathered.warnings.add("query_matched_nothing")
                chosen = scored
        for failure in failures:
            if failure.failure_id in chosen:
                for candidate in _failure_candidates(failure, match=chosen[failure.failure_id]):
                    gathered.add(candidate)
        return gathered

    # --- temporal reads shared by the tools ---

    async def _decisions(
        self,
        scope: TemporalScope,
        valid_at: datetime,
        recorded_at: datetime,
        history: bool,
        gathered: _Gathered,
    ) -> list[DecisionRecord]:
        try:
            with _temporal_span():
                result = await self._temporal.decisions(scope, valid_at, recorded_at, history)
        except TemporalProjectionOutdated:
            gathered.warnings.add("temporal_projection_outdated")
            return []
        if result.truncated:
            gathered.warnings.add("decisions_capped")
        return list(result.decisions)

    async def _failures(
        self, scope: TemporalScope, valid_at: datetime, recorded_at: datetime, gathered: _Gathered
    ) -> list[FailureRecord]:
        try:
            with _temporal_span():
                result = await self._temporal.failures(scope, valid_at, recorded_at, True)
        except TemporalProjectionOutdated:
            gathered.warnings.add("temporal_projection_outdated")
            return []
        if result.truncated:
            gathered.warnings.add("failures_capped")
        return list(result.failures)


# --------------------------------------------------------------------------------------------
# Code candidates (all from one `RevisionView`)
# --------------------------------------------------------------------------------------------


class _CodeIndex:
    """Lookups and candidates over one revision's files, symbols and relations."""

    def __init__(self, view: RevisionView) -> None:
        self.view = view
        self.files = {item.file_id: item for item in view.files}
        self.symbols = {item.symbol_id: item for item in view.symbols}
        self.modules_by_id: dict[str, list[FileFact]] = {}
        for item in view.files:
            self.modules_by_id.setdefault(item.module_id, []).append(item)
        self.target = _short_target(view.run)

    def has(self, node_id: str) -> bool:
        return node_id in self.files or node_id in self.symbols

    def file_path(self, file_id: str) -> str:
        found = self.files.get(file_id)
        return "" if found is None else found.path

    def module_of(self, file_id: str) -> str:
        found = self.files.get(file_id)
        return "" if found is None else found.module_id

    def name(self, node_id: str) -> str:
        if node_id in self.symbols:
            return self.symbols[node_id].qualified_name
        if node_id in self.files:
            return self.files[node_id].path
        if node_id in self.modules_by_id:
            return f"module {self.module_name(node_id)}"
        return node_id

    def module_name(self, module_id: str) -> str:
        """The directory a module's files share at this revision (`.` for the root)."""
        directories = {
            item.path.rpartition("/")[0] or "." for item in self.modules_by_id.get(module_id, ())
        }
        return min(directories) if len(directories) == 1 else module_id

    def matches(self, tokens: Sequence[str]) -> list[tuple[str, float]]:
        scored = [
            (node, _overlap(tokens, (name,)))
            for node, name in [
                *((s.symbol_id, s.qualified_name) for s in self.view.symbols),
                *((f.file_id, f.path) for f in self.view.files),
            ]
        ]
        return sorted(
            ((node, score) for node, score in scored if score > 0), key=lambda x: (-x[1], x[0])
        )

    def bfs(self, anchors: Iterable[str], depth: int) -> dict[str, int]:
        neighbours: dict[str, set[str]] = {}
        for relation in self.view.relations():
            assert relation.object_id is not None
            neighbours.setdefault(relation.subject_id, set()).add(relation.object_id)
            neighbours.setdefault(relation.object_id, set()).add(relation.subject_id)
        for symbol in self.view.symbols:
            neighbours.setdefault(symbol.file_id, set()).add(symbol.symbol_id)
            neighbours.setdefault(symbol.symbol_id, set()).add(symbol.file_id)
        distances = {anchor: 0 for anchor in anchors if self.has(anchor)}
        frontier = sorted(distances)
        for level in range(1, depth + 1):
            reached = sorted(
                {n for node in frontier for n in neighbours.get(node, ()) if n not in distances}
            )
            for node in reached:
                if self.has(node):
                    distances[node] = level
            frontier = [n for n in reached if self.has(n)]
        return distances

    def resolve_symbol(self, symbol_id: str | None, name: str | None) -> SymbolFact:
        if symbol_id is not None:
            found = self.symbols.get(symbol_id)
            if found is None:
                raise SymbolNotFound("no symbol with that ID at the revision")
            return found
        assert name is not None
        exact = [s for s in self.view.symbols if s.qualified_name == name]
        short = [s for s in self.view.symbols if s.qualified_name.rsplit(".", 1)[-1] == name]
        found_all = exact or short
        unique = {s.symbol_id: s for s in found_all}
        if not unique:
            raise SymbolNotFound("no symbol with that name at the revision")
        if len(unique) > 1:
            raise AmbiguousSymbol(f"{len(unique)} symbols have that name; pass symbol_id")
        return next(iter(unique.values()))

    def candidate(
        self, node_id: str, *, distance: int, match: float, priority: int = 0
    ) -> _Candidate:
        if node_id in self.symbols:
            return self._symbol(self.symbols[node_id], distance, match, priority)
        return self._file(self.files[node_id], distance, match, priority)

    def _file(self, item: FileFact, distance: int, match: float, priority: int) -> _Candidate:
        body = (
            f"file {item.path} at {self.target}\n"
            f"module: {item.module_id}; language: {item.language or 'unknown'}; "
            f"file revision: {item.file_revision_id}\n"
            f"membership: deterministic (repository scan of this revision), confidence 1.0"
        )
        return _Candidate(
            item_id=f"file:{item.file_id}",
            kind="file",
            title=f"file {item.path}",
            body=body,
            citations=self._run_citations(item.source_event_ids, "git", 1.0, item.file_revision_id),
            tier=5,
            exact_scope=True,
            distance=distance,
            match=match,
            recency=self.view.run.occurred_at,
            priority=priority,
        )

    def _symbol(self, item: SymbolFact, distance: int, match: float, priority: int) -> _Candidate:
        kind, confidence, how = _definition_evidence(item)
        body = (
            f"symbol {item.qualified_name} ({item.kind or 'unknown kind'}) "
            f"defined in {self.file_path(item.file_id)} at {self.target}\n"
            f"symbol revision: {item.symbol_revision_id}\n"
            f"definition evidence: {how}, confidence {confidence}, extractor "
            f"{item.extractor_name or 'unknown'} {item.extractor_version or ''}".rstrip()
        )
        return _Candidate(
            item_id=f"symbol:{item.symbol_id}",
            kind="symbol",
            title=f"symbol {item.qualified_name}",
            body=body,
            citations=self._run_citations(
                item.source_event_ids,
                kind,
                confidence,
                item.symbol_revision_id,
                valid_from=item.valid_from,
            ),
            tier=5 if kind == "scip" else 3,
            exact_scope=True,
            distance=distance,
            match=match,
            recency=self.view.run.occurred_at,
            priority=priority,
        )

    def relation(self, fact: AssertionFact, *, distance: int, priority: int = 0) -> _Candidate:
        assert fact.object_id is not None and fact.predicate is not None
        subject, target = self.name(fact.subject_id), self.name(fact.object_id)
        confidence = float(fact.confidence or 0.0)
        extractor = f"{fact.extractor_name or 'unknown'} {fact.extractor_version or ''}".rstrip()
        if confidence >= 1.0 and fact.evidence_kind in {"scip", "git", "test"}:
            standing = f"deterministic {fact.evidence_kind} evidence, confidence 1.0"
            tier = 5
        else:
            standing = (
                f"heuristic (inferred, not verified): {fact.evidence_kind or 'unknown'} evidence, "
                f"confidence {confidence}"
            )
            tier = 3 if confidence >= 0.9 else 2
        review = f"; review: {fact.review_status}" if fact.review_status else ""
        body = (
            f"{subject} {fact.predicate} {target} at {self.target}\n"
            f"evidence: {standing}; extractor {extractor}{review}"
        )
        return _Candidate(
            item_id=f"relation:{fact.assertion_id}",
            kind="relation",
            title=f"{subject} {fact.predicate} {target}",
            body=body,
            citations=self._run_citations(
                fact.source_event_ids,
                fact.evidence_kind or "tree_sitter",
                confidence,
                fact.file_revision_id,
                valid_from=fact.valid_from,
            ),
            tier=tier + (1 if fact.review_status == "accepted" else 0),
            exact_scope=True,
            distance=distance,
            recency=self.view.run.occurred_at,
            priority=priority,
        )

    def dependency(self, fact: AssertionFact) -> _Candidate:
        version = fact.resolved_version or fact.requirement or "unversioned"
        confidence = float(fact.confidence or 0.0)
        heuristic = "" if confidence >= 1.0 else f" (heuristic, confidence {confidence})"
        body = (
            f"external dependency {fact.dependency_id} ({fact.dependency_kind or 'unknown kind'})"
            f", {version}, declared in {self.name(fact.subject_id)} at {self.target}"
            f"{heuristic}; extractor {fact.extractor_name or 'unknown'}"
        )
        return _Candidate(
            item_id=f"dependency:{fact.dependency_id}",
            kind="dependency",
            title=f"dependency {fact.dependency_id}",
            body=body,
            citations=self._run_citations(
                fact.source_event_ids,
                fact.evidence_kind or "tree_sitter",
                confidence,
                fact.file_revision_id,
                valid_from=fact.valid_from,
            ),
            tier=5 if confidence >= 1.0 else 3,
            exact_scope=True,
            recency=self.view.run.occurred_at,
            priority=1,
        )

    def summary(self, commit_id: str) -> _Candidate:
        relations = self.view.relations()
        certain = sum(1 for r in relations if (r.confidence or 0.0) >= 1.0)
        modules = {item.module_id for item in self.view.files}
        languages = sorted({item.language for item in self.view.files if item.language})
        state = "complete" if self.view.run.complete else "incomplete (absence not inferred)"
        body = (
            f"repository {self.view.repository_id} at commit {commit_id}: "
            f"{len(self.view.files)} files in {len(modules)} modules, "
            f"{len(self.view.symbols)} symbols, {len(relations)} relations "
            f"({certain} deterministic, {len(relations) - certain} heuristic), "
            f"{len(self.view.dependencies())} external dependencies\n"
            f"languages: {', '.join(languages) or 'none'}; index run: {state}"
        )
        return _Candidate(
            item_id=f"repository:{self.view.repository_id}",
            kind="repository",
            title=f"repository {self.view.repository_id} at {commit_id[:12]}",
            body=body,
            citations=self._run_citations(self.view.run.source_event_ids, "git", 1.0, None),
            tier=5,
            exact_scope=True,
            recency=self.view.run.occurred_at,
            priority=5,
        )

    def modules(self) -> list[_Candidate]:
        grouped: dict[str, list[FileFact]] = {}
        for item in self.view.files:
            grouped.setdefault(item.module_id, []).append(item)
        found = []
        for module_id, files in sorted(grouped.items()):
            paths = sorted(item.path for item in files)
            where = self.module_name(module_id)
            listed = ", ".join(paths[:MAX_LISTED]) + (", ..." if len(paths) > MAX_LISTED else "")
            symbols = sum(1 for s in self.view.symbols if s.file_id in {f.file_id for f in files})
            found.append(
                _Candidate(
                    item_id=f"module:{module_id}",
                    kind="module",
                    title=f"module {where}",
                    body=(
                        f"module {where} (module ID {module_id}) at {self.target}: "
                        f"{len(files)} files, "
                        f"{symbols} symbols\nfiles: {listed}"
                    ),
                    citations=tuple(
                        citation
                        for item in sorted(files, key=lambda f: f.path)[:3]
                        for citation in self._run_citations(
                            item.source_event_ids, "git", 1.0, item.file_revision_id
                        )
                    ),
                    tier=5,
                    exact_scope=True,
                    recency=self.view.run.occurred_at,
                    priority=4,
                )
            )
        return found

    def changes(self, previous: Run, before: Sequence[FileFact]) -> list[_Candidate]:
        earlier = {item.file_id: item for item in before}
        found = []
        for file_id in sorted(set(earlier) | set(self.files)):
            old, new = earlier.get(file_id), self.files.get(file_id)
            if new is None:
                assert old is not None
                what, cited, at = f"removed {old.path}", old, previous
            elif old is None:
                what, cited, at = f"added {new.path}", new, self.view.run
            elif old.path != new.path:
                what, cited, at = f"renamed {old.path} to {new.path}", new, self.view.run
                if old.file_revision_id != new.file_revision_id:
                    what += " (content changed)"
            elif old.file_revision_id != new.file_revision_id:
                what, cited, at = f"modified {new.path}", new, self.view.run
            else:
                continue
            found.append(
                _Candidate(
                    item_id=f"change:{file_id}",
                    kind="change",
                    title=what,
                    body=(
                        f"{what}: between {_short_target(previous)} and {self.target} "
                        f"(deterministic, from the two index runs' file membership)"
                    ),
                    citations=self._run_citations(
                        cited.source_event_ids, "git", 1.0, cited.file_revision_id
                    )
                    + (
                        ()
                        if old is None or cited is old
                        else self._run_citations(
                            old.source_event_ids, "git", 1.0, old.file_revision_id
                        )
                    ),
                    tier=5,
                    exact_scope=cited is new,
                    recency=at.occurred_at,
                    priority=2,
                )
            )
        return found

    def history(self, symbol: SymbolFact, revisions: Sequence[SymbolRevisionFact]) -> _Candidate:
        kind, confidence, _ = _definition_evidence(symbol)
        lines = []
        citations: list[Citation] = []
        for fact in revisions:
            until = f" until {fact.until}" if fact.until else f", still held at {self.target}"
            lines.append(
                f"- revision {fact.symbol_revision_id} as {fact.qualified_name}: from "
                f"{fact.first_seen} ({_short(fact.run_label, fact.run_target_id)}){until}"
            )
            citations += self._run_citations(
                fact.source_event_ids,
                kind,
                confidence,
                fact.symbol_revision_id,
                valid_from=fact.first_seen,
            )
        return _Candidate(
            item_id=f"symbol_history:{symbol.symbol_id}",
            kind="symbol_history",
            title=f"history of {symbol.qualified_name}",
            body=f"revisions of {symbol.qualified_name} up to {self.target}:\n" + "\n".join(lines),
            citations=tuple(citations),
            tier=5 if kind == "scip" else 3,
            exact_scope=False,
            recency=self.view.run.occurred_at,
            priority=2,
        )

    def _run_citations(
        self,
        events: Iterable[str],
        kind: str,
        confidence: float,
        revision_id: str | None,
        *,
        valid_from: str | None = None,
    ) -> tuple[Citation, ...]:
        start = window_bound(valid_from) or window_bound(self.view.run.occurred_at)
        return tuple(
            Citation(
                event_id=event,
                evidence_kind=kind,
                confidence=confidence,
                valid_from=start,
                revision_id=revision_id,
            )
            for event in sorted(events)
        )


def _definition_evidence(symbol: SymbolFact) -> tuple[str, float, str]:
    """How sure a symbol definition is: SCIP is deterministic, Tree-sitter only syntactic."""
    if symbol.scip_symbol is not None:
        return "scip", 1.0, "deterministic (SCIP)"
    return "tree_sitter", 0.9, "syntactic (Tree-sitter), not semantic"


# --------------------------------------------------------------------------------------------
# Temporal and content candidates
# --------------------------------------------------------------------------------------------


def _decision_candidate(
    record: DecisionRecord, at: datetime, *, match: float = 0.0, priority: int = 0
) -> _Candidate:
    if record.standing is DecisionStanding.ACTIVE:
        standing = "ACTIVE at the requested time"
    elif record.reason is HistoricalReason.SUPERSEDED:
        standing = f"HISTORICAL at the requested time: superseded by {record.superseded_by}"
    elif record.reason is HistoricalReason.EXPIRED:
        standing = f"HISTORICAL at the requested time: expired at {_iso(record.effective_valid_to)}"
    else:
        standing = "HISTORICAL at the requested time: recorded as superseded, end unknown"
    body = (
        f"decision {record.decision_id}: {standing}\n"
        f"recorded status: {record.status}; valid from {_iso(record.valid_from)} to "
        f"{_iso(record.effective_valid_to) if record.effective_valid_to else 'open'}; "
        f"recorded at {_iso(record.recorded_at)}\n"
        f"supersedes: {record.supersedes_id or 'none'}; subjects: "
        f"{', '.join(record.subjects) or 'none'}"
    )
    accepted = record.status == "accepted"
    return _Candidate(
        item_id=f"decision:{record.decision_id}",
        kind="decision",
        title=f"decision {record.decision_id} ({record.standing.value})",
        body=body,
        citations=tuple(
            Citation(
                event_id=event,
                evidence_kind="agent",
                confidence=1.0,
                valid_from=record.valid_from,
                valid_to=record.effective_valid_to,
            )
            for event in record.source_event_ids
        ),
        tier=4 if accepted else 3,
        exact_scope=True,
        match=match,
        recency=_iso(record.valid_from),
        priority=priority,
        excerpt=(record.source_event_ids[-1], None) if record.source_event_ids else None,
    )


def _failure_candidates(
    failure: FailureRecord, *, match: float = 0.0, priority: int = 0
) -> list[_Candidate]:
    runs = [_run_line(run) for run in failure.observed_in]
    resolution = failure.resolution
    status = resolution.status.value
    if resolution.reason:
        status += f" ({resolution.reason})"
    body = (
        f"failure {failure.failure_id}: {failure.error_class or 'unknown error'} in "
        f"{failure.component or 'unknown component'}/{failure.operation or 'unknown operation'}\n"
        f"first observed {_iso(failure.valid_from)}; recorded {_iso(failure.recorded_at)}; "
        f"fingerprint {failure.fingerprint_sha256 or 'unknown'}\n"
        f"resolution: {status}\nobserved in:\n" + ("\n".join(runs) or "- no run in scope")
    )
    found = [
        _Candidate(
            item_id=f"failure:{failure.failure_id}",
            kind="failure",
            title=f"failure {failure.failure_id} ({resolution.status.value})",
            body=body,
            citations=tuple(
                Citation(event, "agent", 1.0, valid_from=failure.valid_from)
                for event in failure.source_event_ids
            )
            + tuple(
                Citation(event, "test", 1.0, valid_from=run.completed_at)
                for run in failure.observed_in
                if run.kind is not RunKind.SESSION
                for event in run.source_event_ids
            ),
            tier=4,
            exact_scope=True,
            match=match,
            recency=_iso(failure.valid_from),
            priority=priority,
            excerpt=(failure.source_event_ids[0], None) if failure.source_event_ids else None,
        )
    ]
    path = resolution.path
    if resolution.status is FixStatus.RESOLVED and path is not None:
        validated = path.validated_commit_id or path.validated_snapshot_id or "unknown"
        if path.commit_changed is False:
            meaning = (
                "a re-run passed on the SAME commit as the failing run: no code change is "
                "evidenced (the failure may be flaky)"
            )
        elif path.commit_changed is True:
            meaning = (
                f"the passing run validated {validated}, a different revision than the failing "
                "run; the change between them is not attributed to any specific edit"
            )
        else:
            meaning = "whether the validated revision differs from the failing run is unknown"
        found.append(
            _Candidate(
                item_id=f"fix:{failure.failure_id}",
                kind="fix_path",
                title=f"CI {path.workflow}/{path.job} passed after failure {failure.failure_id}",
                body=(
                    f"failure {failure.failure_id}: CI {path.workflow}/{path.job} failed in run "
                    f"{path.failing_run_id} ({_iso(path.failing_completed_at)}) and passed in "
                    f"run {path.passing_run_id} ({_iso(path.passing_completed_at)})\n"
                    f"validated revision: {validated}\n{meaning}"
                ),
                citations=tuple(
                    Citation(event, "test", 1.0, valid_from=path.passing_completed_at)
                    for event in path.source_event_ids
                ),
                tier=5,
                exact_scope=True,
                match=match,
                recency=_iso(path.passing_completed_at),
                priority=priority,
            )
        )
    return found


def _content_candidate(
    hit: SearchHit, score: float, events: Mapping[str, LedgerEvent] | None = None
) -> _Candidate:
    kind = _CONTENT_KINDS.get(hit.event_type, "session_content")
    where = f"session {hit.session_id}" if hit.session_id else "the repository"
    body = (
        f"{kind.replace('_', ' ')} from {where} ({hit.event_type}) at {_iso(hit.occurred_at)}"
        f"{_ran_on(hit, events or {})}\n"
        f"content: sanitized ({hit.redaction}); match score {score}"
    )
    digest = hashlib.sha256(hit.content_id.encode()).hexdigest()[:32]
    executed = kind == "ci_output"
    return _Candidate(
        item_id=f"content:{digest}",
        kind=kind,
        title=f"{kind.replace('_', ' ')} in {where}",
        body=body,
        # Every event that referenced the content; the ledger keeps only the delivered ones (a
        # row's metadata can come from a later event that was dead-lettered).
        citations=tuple(
            Citation(
                event_id=event_id,
                evidence_kind="test" if executed else "agent",
                confidence=1.0,
                valid_from=hit.occurred_at,
                content_id=hit.content_id,
            )
            for event_id in hit.source_event_ids  # the citable ones (`_search_hits`)
        ),
        tier=5 if executed else 1,
        exact_scope=False,
        match=score,
        recency=_iso(hit.occurred_at),
        excerpt=(hit.source_event_ids[0], hit.content_id),
    )


_CONTENT_KINDS: Final = {
    "agent.turn.started": "user_message",
    "agent.turn.completed": "assistant_message",
    "agent.turn.stopped": "assistant_message",
    "agent.subagent.stopped": "assistant_message",
    "agent.tool_call.started": "tool_call",
    "agent.tool_call.completed": "tool_call",
    "agent.tool_call.output_observed": "tool_call",
    "agent.permission.decision": "permission_decision",
    "git.commit.observed": "commit_message",
    "quality.ci_run.completed": "ci_output",
    "quality.test_run.completed": "ci_output",
    "quality.finding.observed": "finding",
    "knowledge.summary.recorded": "summary",
    "knowledge.constraint.recorded": "constraint",
}


def _ran_on(hit: SearchHit, events: Mapping[str, LedgerEvent]) -> str:
    """Which revision executed output ran on, from its citable run event (a CI or test run of
    another commit is in the window but is not the requested revision's)."""
    for event_id in hit.source_event_ids:
        found = events.get(event_id)
        if found is None or not found.event.event_type.startswith("quality."):
            continue
        payload = found.event.payload
        if payload.get("commit_id"):
            return f", run on commit {payload['commit_id']}"
        if payload.get("snapshot_id"):
            return f", run on workspace snapshot {payload['snapshot_id']}"
    return ""


def _kept_runs(failures: Iterable[FailureRecord], snapshot: str | None) -> frozenset[str]:
    """The CI and test runs a revision answer keeps: those visible in its window (the temporal
    read returned them) that ran on the revision's own state, never another workspace snapshot.
    An observation made in any other run (dirty, or not visible at the cut) is not evidence."""
    return frozenset(
        run.run_id
        for failure in failures
        for run in failure.observed_in
        if run.kind is not RunKind.SESSION and run.snapshot_id in (None, snapshot)
    )


def _run_line(run: ObservedRun) -> str:
    parts = [f"- {run.kind.value} {run.run_id}"]
    if run.status:
        parts.append(f"status {run.status}")
    if run.workflow or run.job:
        parts.append(f"{run.workflow or '?'}/{run.job or '?'}")
    if run.completed_at:
        parts.append(f"completed {_iso(run.completed_at)}")
    if run.commit_id:
        parts.append(f"commit {run.commit_id}")
    if run.snapshot_id:
        parts.append(f"workspace snapshot {run.snapshot_id}")
    return ", ".join(parts)


def _without_dirty(failure: FailureRecord, snapshot: str | None) -> FailureRecord | None:
    """A failure as seen from a revision: runs on any other workspace snapshot are dirty, not
    evidence (`snapshot` is the requested one, `None` for a commit).

    The resolution was derived over every run, dirty ones included, so it is kept only when it
    rests on the remaining runs alone: a fix path between two kept runs that validated an allowed
    revision. Anything else is `unknown` with the reason `dirty_runs_excluded` (never a reason
    text that names a dirty run).
    """
    kept = tuple(run for run in failure.observed_in if run.snapshot_id in (None, snapshot))
    if failure.observed_in and not kept:
        return None
    ids = {run.run_id for run in kept}
    path = failure.resolution.path
    path_kept = path is None or (
        path.failing_run_id in ids and path.validated_snapshot_id in (None, snapshot)
    )
    if len(kept) == len(failure.observed_in) and path_kept:
        return failure
    resolution = failure.resolution
    if path is None or not path_kept:
        resolution = FixResolution(FixStatus.UNKNOWN, "dirty_runs_excluded")
    return replace(failure, observed_in=kept, resolution=resolution)


def _in_window(failure: FailureRecord, start: datetime, end: datetime) -> bool:
    if start <= failure.valid_from <= end:
        return True
    return any(
        run.completed_at is not None and start <= run.completed_at <= end
        for run in failure.observed_in
    )


def _failure_words(failure: FailureRecord) -> tuple[str, ...]:
    return tuple(
        word
        for word in (
            failure.failure_id,
            failure.component,
            failure.operation,
            failure.error_class,
            *(run.workflow for run in failure.observed_in),
            *(run.job for run in failure.observed_in),
        )
        if word
    )


# --------------------------------------------------------------------------------------------
# Ranking, budgets and helpers
# --------------------------------------------------------------------------------------------


def _search_span() -> AbstractContextManager[object]:
    """One `operation=search` sample per lexical or vector call (`retrieval/search.py` has none)."""
    return retrieval_span(
        "search", client_errors=(SearchRequestError,), deadline_errors=(SearchTimeoutError,)
    )


def _temporal_span() -> AbstractContextManager[object]:
    """One `operation=temporal` sample per temporal read (`retrieval/temporal.py` has none)."""
    return retrieval_span(
        "temporal",
        client_errors=(TemporalRequestError, TemporalDecisionNotFound),
        deadline_errors=DEADLINE_ERRORS,
    )


def _known(freshness: Freshness, gathered: _Gathered, ceiling: datetime | None = None) -> datetime:
    """The record time temporal facts are read at: never past the snapshot's own time
    (`valid_instant`: a fact projected after the freshness read is not known yet), nor past the
    projection backlog (`known_until`), at most `ceiling` (a revision window's instant). When
    the backlog is the binding bound, facts the snapshot's time would otherwise show are hidden,
    and the answer says so (`BACKLOG_WARNING`)."""
    limit = freshness.valid_instant if ceiling is None else min(ceiling, freshness.valid_instant)
    if freshness.known_until is None or freshness.known_until >= limit:
        return limit
    gathered.warnings.add(BACKLOG_WARNING)
    return freshness.known_until


def _snapshot_token(freshness: Freshness) -> str:
    """What a cursor binds as its snapshot: the watermark AND the dependent checkpoints, so any
    delivery that changes an answer (even one past a still-pending earlier row) makes it stale."""
    return f"{freshness.as_of_event_id}|{freshness.projection_checkpoint}"


def _rank(candidates: Iterable[_Candidate]) -> list[_Candidate]:
    pool = sorted(candidates, key=lambda c: c.item_id)
    times = sorted({c.recency for c in pool if c.recency})
    rank_of = {t: (i / (len(times) - 1) if len(times) > 1 else 1.0) for i, t in enumerate(times)}

    def score(c: _Candidate) -> int:
        proximity = 0.0 if c.distance is None else 1.0 / (1 + c.distance)
        total = (
            100 * c.priority
            + 4 * c.tier
            + 6 * (1 if c.exact_scope else 0)
            + 20 * proximity
            + 40 * c.match
            + 3 * rank_of.get(c.recency, 0.0)
        )
        return round(total * 1_000_000)

    return sorted(pool, key=lambda c: (-score(c), c.item_id))


def _size(item: ContextItemV1) -> int:
    return len(item.model_dump_json().encode())


def tokens_for(byte_count: int) -> int:
    """The documented token estimate: one token per started group of four UTF-8 bytes."""
    return math.ceil(byte_count / BYTES_PER_TOKEN)


def _package(
    scope: ProjectScopeV1 | RepositoryScopeV1,
    revision: CommitRevisionV1 | WorkspaceSnapshotRevisionV1 | None,
    budget: QueryBudgetV1,
    freshness: Freshness,
    items: Sequence[ContextItemV1],
    next_cursor: str | None,
    warnings: Iterable[str],
    now: datetime,
) -> ContextPackageV1:
    return ContextPackageV1(
        query_id=f"query_{new_uuid7().hex}",
        generated_at=now,
        scope=scope,
        revision=revision,
        budget=budget,
        as_of_event_id=UUID(freshness.as_of_event_id),
        projection_checkpoint=freshness.projection_checkpoint,
        items=tuple(items),
        truncated=next_cursor is not None,
        next_cursor=next_cursor,
        warnings=tuple(sorted({*warnings, *freshness.warnings})),
        staleness_ms=freshness.staleness_ms,
    )


def _view_warnings(view: RevisionView, gathered: _Gathered) -> None:
    if not view.run.complete:
        gathered.warnings.add("revision_index_incomplete")
    if view.truncated:
        gathered.warnings.add("revision_view_capped")


def _check_text(value: str) -> None:
    if len(value) > MAX_QUERY_CHARS:
        raise ContextRequestError(f"query text must be at most {MAX_QUERY_CHARS} characters")


def _words(value: str) -> list[str]:
    split = _CAMEL.sub(" ", value).lower()
    return _TOKEN.findall(split)


def _tokens(value: str) -> list[str]:
    """Distinct lowercase words of length >= 2 that are not stop words, in first-seen order."""
    found = [w for w in _words(value) if 2 <= len(w) <= MAX_TOKEN_CHARS and w not in _STOPWORDS]
    return list(dict.fromkeys(found))[:MAX_QUERY_TOKENS]


def _overlap(tokens: Sequence[str], names: Iterable[str | None]) -> float:
    """The share of query tokens that are words of `names` (0 when the query has none)."""
    if not tokens:
        return 0.0
    words = {w for name in names if name for w in _words(name)}
    return _quantize(sum(1 for t in tokens if t in words) / len(tokens))


def _quantize(value: float) -> float:
    return round(value, 6)


def _iso(moment: datetime | None) -> str:
    if moment is None:
        return "unknown"
    return f"{moment.astimezone(UTC):%Y-%m-%dT%H:%M:%S.%fZ}"


def _short(label: str, target: str) -> str:
    if label == "Commit":
        return f"commit {target.rsplit(':', 1)[-1][:12]}"
    return f"workspace snapshot {target[:16]}"


def _short_target(run: Run) -> str:
    return _short(run.label, run.target_id)


__all__ = [
    "DEPENDENCIES",
    "TOOLS",
    "AmbiguousSymbol",
    "ContextBudgetTooSmall",
    "ContextComposer",
    "ContextRequestError",
    "RequestContext",
    "ScopeNotFound",
    "SymbolNotFound",
    "tokens_for",
]
