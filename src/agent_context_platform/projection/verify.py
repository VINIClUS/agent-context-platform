"""Graph digest, projection verification, replay and rebuild.

Everything the `agent-context projection` CLI does lives here, so it can be tested without the CLI.

* `compute_graph_digest` is the production digest: a canonical SHA-256 over every node (labels and
  non-volatile properties) and relationship (type, endpoint identities, properties) of ONE Neo4j
  database. It streams in keyset-ordered batches and holds only 32 bytes per graph item.
* `verify_projections` checks, read-only, checkpoint continuity, event coverage, orphan source IDs
  and stream heads against the ledger, and can compare the live graph with a scratch replay.
* `replay_ledger` projects the ledger, in ledger (outbox) order, through the registered projectors
  into a target store with its own loop: it never touches the live checkpoints or the outbox.
* `rebuild_projections` is the operator workflow built on those pieces (see its docstring for the
  Neo4j Community constraint that shapes it).

Both workflows report a `projection.rebuilt` system event (SDK `ProjectionRebuiltV1`) per projector
through the in-process `IngestionService`. System events are handled by no projector, so coverage
never counts them. A report is a ledger fact, so a re-run that would say the same thing appends
nothing: the idempotency key derives from the projector, mode, outcome, ledger range and digest.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
from collections import Counter
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Final, LiteralString
from uuid import UUID

from agent_context_sdk import (
    EventDraftV1,
    EventRedactionSummaryV1,
    IngestBatchRequestV1,
    ProducerV1,
    RedactionPolicyV1,
    new_uuid7,
    verify_event,
)
from agent_context_sdk.content import ContentDisposition
from agent_context_sdk.events.system import (
    ProjectionMode,
    ProjectionOutcome,
    ProjectionRebuiltV1,
)
from sqlalchemy import String, and_, func, literal, select, text
from sqlalchemy import cast as sql_cast
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.blob_store import BlobStoreError, StoredBlob
from agent_context_platform.content.service import ContentService
from agent_context_platform.ledger.models import EventRow, EventStreamRow
from agent_context_platform.ledger.repository import LedgerRepository
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.projection.models import (
    DeadLetterRow,
    OutboxRow,
    OutboxStatus,
    ProjectionCheckpointRow,
    ProjectionState,
)
from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.projection.runtime import (
    EventIntegrityError,
    OrphanedOutboxRowError,
    Projector,
)
from agent_context_platform.projection.schema import CONSTRAINT_STATEMENTS, ensure_schema

Clock = Callable[[], datetime]

# --------------------------------------------------------------------------------------------
# Graph digest
# --------------------------------------------------------------------------------------------

# Properties a projector may write transiently and that must never reach the digest. The projectors
# write no wall clock or run identity, so the only entry is the lock marker `lock_nodes` sets and
# removes inside one statement. Extend this list, never the hashing code, when a projector gains a
# volatile property.
VOLATILE_PROPERTIES: Final[frozenset[str]] = frozenset({"_lock"})

# Graph properties that point back at the ledger; every value must be an existing event ID.
SOURCE_ID_PROPERTIES: Final[tuple[str, ...]] = ("source_event_id", "source_event_ids")

DEFAULT_BATCH_SIZE: Final = 1000
_SOURCE_ID_FLUSH: Final = 5000
_DIGEST_DOMAIN: Final = b"agent-context.graph-digest.v1\n"

# Every projected label has exactly one uniqueness constraint on its identity property; that
# property is the node's identity in the digest and the keyset cursor of the scan.
NODE_KEYS: Final[Mapping[str, str]] = {
    statement.label: statement.property_name for statement in CONSTRAINT_STATEMENTS
}
_IDENTIFIER: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SourceIdSink = Callable[[set[str]], Awaitable[None]]


class GraphScanError(RuntimeError):
    """The graph could not be read into a digest."""


def _literal(query: str) -> LiteralString:
    """Treat an assembled query as a literal.

    Only ever called with templates in this module filled with labels and properties from the
    static schema manifest, which `_identifier` has just checked; no external value reaches it.
    """
    return query


def _identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise GraphScanError("schema manifest holds a non-identifier name")
    return name


@dataclass(frozen=True, slots=True)
class GraphDigest:
    """The digest of one graph with its node and relationship counts."""

    digest: str
    node_count: int
    relationship_count: int
    nodes: Mapping[str, int]
    relationships: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "digest": self.digest,
            "node_count": self.node_count,
            "relationship_count": self.relationship_count,
            "nodes": dict(sorted(self.nodes.items())),
            "relationships": dict(sorted(self.relationships.items())),
        }


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _item_hash(*parts: object) -> bytes:
    return hashlib.sha256(_canonical(parts).encode()).digest()


def _stable_props(props: Mapping[str, Any]) -> dict[str, Any]:
    return {name: value for name, value in props.items() if name not in VOLATILE_PROPERTIES}


def primary_label(labels: Iterable[str], props: Mapping[str, Any]) -> str | None:
    """The first (sorted) label of a node that has a registered identity property set."""
    for label in sorted(labels):
        key = NODE_KEYS.get(label)
        if key is not None and key in props:
            return label
    return None


def node_identity(labels: Iterable[str], props: Mapping[str, Any]) -> str:
    """A node's identity: its primary label and identity value, else a hash of its content."""
    label = primary_label(labels, props)
    if label is not None:
        return f"{label}:{props[NODE_KEYS[label]]}"
    ordered = sorted(labels)
    content = hashlib.sha256(_canonical([ordered, _stable_props(props)]).encode()).hexdigest()
    return f"~{':'.join(ordered)}:{content}"


def source_ids(props: Mapping[str, Any]) -> list[str]:
    """Every ledger event ID a graph property set points back at."""
    found: list[str] = []
    for name in SOURCE_ID_PROPERTIES:
        value = props.get(name)
        if isinstance(value, str):
            found.append(value)
        elif isinstance(value, (list, tuple)):
            found.extend(item for item in value if isinstance(item, str))
    return found


_REL_RETURN: Final[LiteralString] = (
    "RETURN labels(a) AS a_labels, properties(a) AS a_props, type(r) AS type, "
    "properties(r) AS props, labels(b) AS b_labels, properties(b) AS b_props"
)


@dataclass(frozen=True, slots=True)
class _Pass:
    """One scan pass: the nodes of one keyed label, or every node without a keyed label."""

    label: str | None

    def _nodes(self) -> tuple[LiteralString, LiteralString]:
        """(page query, relationship query), each taking `$cursors`/`$after`/`$limit`."""
        if self.label is None:
            return (
                "MATCH (n) WHERE NOT any(l IN labels(n) WHERE l IN $keyed) "
                "AND ($after IS NULL OR elementId(n) > $after) "
                "WITH n ORDER BY elementId(n) LIMIT $limit "
                "RETURN elementId(n) AS cursor, labels(n) AS labels, properties(n) AS props",
                "MATCH (a) WHERE elementId(a) IN $cursors MATCH (a)-[r]->(b) " + _REL_RETURN,
            )
        label, key = _identifier(self.label), _identifier(NODE_KEYS[self.label])
        return (
            _literal(
                f"MATCH (n:`{label}`) WHERE $after IS NULL OR n.`{key}` > $after "
                f"WITH n ORDER BY n.`{key}` LIMIT $limit "
                f"RETURN n.`{key}` AS cursor, labels(n) AS labels, properties(n) AS props"
            ),
            _literal(
                f"MATCH (a:`{label}`) WHERE a.`{key}` IN $cursors MATCH (a)-[r]->(b) " + _REL_RETURN
            ),
        )

    async def page(
        self, store: Neo4jStore, after: str | None, limit: int
    ) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        node_query, relationship_query = self._nodes()

        async def read(
            tx: Neo4jTransaction,
        ) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
            nodes = (
                await tx.run(
                    node_query,
                    parameters={"after": after, "limit": limit, "keyed": sorted(NODE_KEYS)},
                )
            ).records
            cursors = [record["cursor"] for record in nodes]
            if not cursors:
                return [], []
            rels = (await tx.run(relationship_query, parameters={"cursors": cursors})).records
            return [record.data() for record in nodes], [record.data() for record in rels]

        return await store.execute_read(read)


async def compute_graph_digest(
    store: Neo4jStore,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    on_source_ids: SourceIdSink | None = None,
) -> GraphDigest:
    """Digest every node and relationship of the store's database.

    The scan is keyset-paged per label on the identity property (backed by its uniqueness index),
    so it never loads the graph: only a 32-byte hash per item is kept, then sorted, which makes
    the result independent of batch size and of Neo4j's internal order. Nodes with no registered
    identity are scanned last, by element ID, and identified by a hash of their content.
    `on_source_ids` receives the ledger event IDs found in properties, in sets of about
    `_SOURCE_ID_FLUSH`, so a caller can check them without holding them all.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    hashes: list[bytes] = []
    node_counts: Counter[str] = Counter()
    relationship_counts: Counter[str] = Counter()
    pending: set[str] = set()

    async def flush() -> None:
        if pending and on_source_ids is not None:
            batch = set(pending)
            pending.clear()
            await on_source_ids(batch)
        else:
            pending.clear()

    for scan in (*(_Pass(label) for label in sorted(NODE_KEYS)), _Pass(None)):
        after: str | None = None
        while True:
            nodes, rels = await scan.page(store, after, batch_size)
            if not nodes:
                break
            for record in nodes:
                after = record["cursor"]
                labels, props = record["labels"], record["props"]
                if scan.label is not None and primary_label(labels, props) != scan.label:
                    continue  # a multi-label node is counted under its primary label only
                identity = node_identity(labels, props)
                hashes.append(
                    b"n" + _item_hash("node", identity, sorted(labels), _stable_props(props))
                )
                node_counts[":".join(sorted(labels))] += 1
                pending.update(source_ids(props))
            for record in rels:
                a_labels, a_props = record["a_labels"], record["a_props"]
                if scan.label is not None and primary_label(a_labels, a_props) != scan.label:
                    continue
                props = record["props"]
                hashes.append(
                    b"r"
                    + _item_hash(
                        "rel",
                        record["type"],
                        node_identity(a_labels, a_props),
                        node_identity(record["b_labels"], record["b_props"]),
                        _stable_props(props),
                    )
                )
                relationship_counts[record["type"]] += 1
                pending.update(source_ids(props))
            if len(pending) >= _SOURCE_ID_FLUSH:
                await flush()
            if len(nodes) < batch_size:
                break
    await flush()

    digest = hashlib.sha256(_DIGEST_DOMAIN + b"".join(sorted(hashes))).hexdigest()
    return GraphDigest(
        digest=digest,
        node_count=sum(node_counts.values()),
        relationship_count=sum(relationship_counts.values()),
        nodes=dict(node_counts),
        relationships=dict(relationship_counts),
    )


# --------------------------------------------------------------------------------------------
# Orphan source IDs
# --------------------------------------------------------------------------------------------


class OrphanCollector:
    """Checks, in batches, that every graph source event ID exists in `ledger.events`."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.checked = 0
        self.orphans: set[str] = set()

    async def add(self, ids: set[str]) -> None:
        self.checked += len(ids)
        parsed: dict[UUID, str] = {}
        for raw in ids:
            try:
                parsed[UUID(raw)] = raw
            except ValueError:
                self.orphans.add(raw)
        if not parsed:
            return
        async with self._session_factory() as session:
            found = set(
                await session.scalars(
                    select(EventRow.event_id).where(EventRow.event_id.in_(parsed))
                )
            )
        self.orphans.update(raw for event_id, raw in parsed.items() if event_id not in found)


# --------------------------------------------------------------------------------------------
# Ledger aggregates and pure classification
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EventTypeStats:
    """Counts of the ledger events of one type relative to one checkpoint position."""

    event_type: str
    total: int
    unqueued: int
    covered: int
    delivered_covered: int
    dead_covered: int
    delivered_beyond: int
    first_event_id: str | None
    last_event_id: str | None


@dataclass(frozen=True, slots=True)
class CheckpointView:
    """A projector checkpoint plus the event the outbox row at its position carries."""

    outbox_id: int | None
    event_id: UUID | None
    processed_count: int
    state: str
    updated_at: datetime
    pointer_event_id: UUID | None


@dataclass(frozen=True, slots=True)
class ProjectorVerification:
    """What verification found for one projector."""

    name: str
    version: str
    checkpoint_outbox_id: int | None
    processed_count: int
    handled_events: int
    covered_events: int
    lag: int
    in_flight: int
    dead_lettered: int
    unqueued: int
    from_event_id: UUID | None
    through_event_id: UUID | None
    issues: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "ok": self.ok,
            "checkpoint_outbox_id": self.checkpoint_outbox_id,
            "processed_count": self.processed_count,
            "handled_events": self.handled_events,
            "covered_events": self.covered_events,
            "lag": self.lag,
            "in_flight": self.in_flight,
            "dead_lettered": self.dead_lettered,
            "unqueued": self.unqueued,
            "from_event_id": None if self.from_event_id is None else str(self.from_event_id),
            "through_event_id": None
            if self.through_event_id is None
            else str(self.through_event_id),
            "issues": list(self.issues),
        }


def _event_range(handled: Sequence[EventTypeStats]) -> tuple[UUID | None, UUID | None, int]:
    """Smallest and largest handled event ID (UUIDv7 order) and the handled count."""
    firsts = [stats.first_event_id for stats in handled if stats.first_event_id is not None]
    lasts = [stats.last_event_id for stats in handled if stats.last_event_id is not None]
    count = sum(stats.total for stats in handled)
    if not firsts or not lasts:
        return None, None, count
    return UUID(min(firsts)), UUID(max(lasts)), count


def classify_projector(
    projector: Projector,
    checkpoint: CheckpointView | None,
    stats: Sequence[EventTypeStats],
    *,
    require_caught_up: bool = False,
) -> ProjectorVerification:
    """Judge one projector's checkpoint against the ledger aggregates (pure).

    Failures: a checkpoint behind an event the outbox already delivered (regression), a
    checkpoint whose pointer disagrees with the outbox, a dead letter below the checkpoint (gap),
    a processed count that is not the number of delivered handled events up to the checkpoint, and
    an event that was never queued. Lag and in-flight rows are reported and only fail under
    `require_caught_up`.
    """
    handled = [item for item in stats if projector.handles(item.event_type)]
    total = sum(item.total for item in handled)
    unqueued = sum(item.unqueued for item in handled)
    covered = sum(item.covered for item in handled)
    delivered_covered = sum(item.delivered_covered for item in handled)
    dead = sum(item.dead_covered for item in handled)
    beyond = sum(item.delivered_beyond for item in handled)
    in_flight = covered - delivered_covered - dead
    lag = total - unqueued - covered - beyond
    from_id, through_id, count = _event_range(handled)

    issues: list[str] = []
    position = None if checkpoint is None else checkpoint.outbox_id
    if (
        checkpoint is not None
        and position is not None
        and checkpoint.pointer_event_id != checkpoint.event_id
    ):
        issues.append("checkpoint_pointer_mismatch")
    if beyond:
        issues.append("checkpoint_regressed")
    if dead:
        issues.append("dead_lettered_below_checkpoint")
    processed = 0 if checkpoint is None else checkpoint.processed_count
    if processed != delivered_covered:
        issues.append("processed_count_mismatch")
    if unqueued:
        issues.append("unqueued_events")
    if require_caught_up and lag:
        issues.append("lagging")
    if require_caught_up and in_flight:
        issues.append("in_flight_below_checkpoint")
    return ProjectorVerification(
        name=projector.name,
        version=projector.version,
        checkpoint_outbox_id=position,
        processed_count=processed,
        handled_events=count,
        covered_events=covered,
        lag=lag,
        in_flight=in_flight,
        dead_lettered=dead,
        unqueued=unqueued,
        from_event_id=from_id,
        through_event_id=through_id,
        issues=tuple(issues),
    )


@dataclass(slots=True)
class ReplayProgress:
    """How far one projector got through a replay (the transient checkpoint)."""

    count: int = 0
    last_outbox_id: int | None = None
    last_event_id: UUID | None = None


def classify_replay(
    projector: Projector, progress: ReplayProgress | None, stats: Sequence[EventTypeStats]
) -> ProjectorVerification:
    """Judge a full replay against the ledger as of the replay's head (pure).

    `stats` are aggregated at the position the replay reached. Every handled event up to it must
    have been projected (the replay takes every status). Events appended afterwards and still
    pending are only lag: the runner delivers them to whichever graph is live after cutover. An
    event past the head that the outbox already DELIVERED is a failure: the live runner put it in
    the old graph and this target would never receive it.
    """
    handled = [item for item in stats if projector.handles(item.event_type)]
    covered = sum(item.covered for item in handled)
    beyond = sum(item.delivered_beyond for item in handled)
    unqueued = sum(item.unqueued for item in handled)
    pending_after = sum(
        item.total - item.unqueued - item.covered - item.delivered_beyond for item in handled
    )
    replayed = 0 if progress is None else progress.count
    from_id, through_id, count = _event_range(handled)
    issues: list[str] = []
    if replayed != covered:
        issues.append("replay_incomplete")
    if unqueued:
        issues.append("unqueued_events")
    if beyond:
        issues.append("delivered_after_replay")
    return ProjectorVerification(
        name=projector.name,
        version=projector.version,
        checkpoint_outbox_id=None if progress is None else progress.last_outbox_id,
        processed_count=replayed,
        handled_events=count,
        covered_events=replayed,
        lag=pending_after,
        in_flight=0,
        dead_lettered=0,
        unqueued=unqueued,
        from_event_id=from_id,
        through_event_id=through_id,
        issues=tuple(issues),
    )


async def event_type_stats(session: AsyncSession, position: int | None) -> list[EventTypeStats]:
    """Aggregate ledger events by type against an outbox `position` (None means nothing covered)."""
    cutoff = literal(0 if position is None else position)
    statement = (
        select(
            EventRow.event_type,
            func.count().label("total"),
            func.count().filter(OutboxRow.outbox_id.is_(None)).label("unqueued"),
            func.count().filter(OutboxRow.outbox_id <= cutoff).label("covered"),
            func.count()
            .filter(and_(OutboxRow.outbox_id <= cutoff, OutboxRow.status == OutboxStatus.DELIVERED))
            .label("delivered_covered"),
            func.count()
            .filter(
                and_(OutboxRow.outbox_id <= cutoff, OutboxRow.status == OutboxStatus.DEAD_LETTERED)
            )
            .label("dead_covered"),
            func.count()
            .filter(and_(OutboxRow.outbox_id > cutoff, OutboxRow.status == OutboxStatus.DELIVERED))
            .label("delivered_beyond"),
            func.min(sql_cast(EventRow.event_id, String)).label("first_event_id"),
            func.max(sql_cast(EventRow.event_id, String)).label("last_event_id"),
        )
        .select_from(EventRow)
        .outerjoin(OutboxRow, OutboxRow.event_id == EventRow.event_id)
        .group_by(EventRow.event_type)
        .order_by(EventRow.event_type)
    )
    return [EventTypeStats(*row) for row in (await session.execute(statement)).all()]


async def _checkpoint_view(session: AsyncSession, projector: Projector) -> CheckpointView | None:
    row = await session.get(ProjectionCheckpointRow, (projector.name, projector.version))
    if row is None:
        return None
    pointer = None
    if row.last_outbox_id is not None:
        pointer = await session.scalar(
            select(OutboxRow.event_id).where(OutboxRow.outbox_id == row.last_outbox_id)
        )
    return CheckpointView(
        outbox_id=row.last_outbox_id,
        event_id=row.last_event_id,
        processed_count=row.processed_count,
        state=row.state.value,
        updated_at=row.updated_at,
        pointer_event_id=pointer,
    )


@dataclass(frozen=True, slots=True)
class StreamHeadCheck:
    """Ledger stream heads against the events actually stored, and the projection's lag."""

    streams: int
    mismatched: int
    mismatched_sample: tuple[str, ...]
    behind: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "streams": self.streams,
            "mismatched": self.mismatched,
            "mismatched_sample": list(self.mismatched_sample),
            "behind": self.behind,
        }


_SAMPLE_SIZE: Final = 20


async def check_stream_heads(
    session: AsyncSession, handled_types: Collection[str]
) -> StreamHeadCheck:
    """Compare each stream's recorded head with its newest stored event, and report lag.

    The graph holds no stream heads, so the projection's view is what the outbox and ledger can
    prove: the newest stored event of a stream must be the head the stream row records. A stream
    is `behind` while it holds an event of a `handled_types` type (one some registered projector
    handles) that the outbox has not delivered: lag, a failure only when the caller requires a
    caught-up projection. System events are handled by no projector, so a stream of them is never
    behind.
    """
    newest = (
        select(
            EventRow.stream_id.label("stream_id"),
            func.max(EventRow.stream_sequence).label("sequence"),
        )
        .group_by(EventRow.stream_id)
        .subquery()
    )
    head_event = (
        select(EventRow.stream_id, EventRow.stream_sequence, EventRow.event_id)
        .join(
            newest,
            and_(
                EventRow.stream_id == newest.c.stream_id,
                EventRow.stream_sequence == newest.c.sequence,
            ),
        )
        .subquery()
    )
    mismatched = await session.scalars(
        select(EventStreamRow.stream_id)
        .outerjoin(head_event, head_event.c.stream_id == EventStreamRow.stream_id)
        .where(
            (EventStreamRow.last_sequence != func.coalesce(head_event.c.stream_sequence, 0))
            | EventStreamRow.last_event_id.is_distinct_from(head_event.c.event_id)
        )
        .order_by(EventStreamRow.stream_id)
    )
    mismatched_ids = list(mismatched)
    streams = await session.scalar(select(func.count()).select_from(EventStreamRow)) or 0
    behind = 0
    if handled_types:
        behind = (
            await session.scalar(
                select(func.count(func.distinct(EventRow.stream_id)))
                .select_from(EventRow)
                .outerjoin(OutboxRow, OutboxRow.event_id == EventRow.event_id)
                .where(
                    EventRow.event_type.in_(sorted(handled_types)),
                    OutboxRow.outbox_id.is_(None) | (OutboxRow.status != OutboxStatus.DELIVERED),
                )
            )
            or 0
        )
    return StreamHeadCheck(
        streams=streams,
        mismatched=len(mismatched_ids),
        mismatched_sample=tuple(mismatched_ids[:_SAMPLE_SIZE]),
        behind=behind,
    )


async def ledger_head(session: AsyncSession) -> int | None:
    """The newest outbox position, the ledger's global order."""
    return await session.scalar(select(func.max(OutboxRow.outbox_id)))


# --------------------------------------------------------------------------------------------
# Reporting `projection.rebuilt`
# --------------------------------------------------------------------------------------------

# The public error class of a report the ledger could not take; it never carries the cause.
RECORD_FAILED: Final = "record_failed"
PLATFORM_PRODUCER_ID: Final = "agent-context-platform"
SYSTEM_STREAM_ID: Final = "platform:projection"
SYSTEM_EVENT_TYPE: Final = "projection.rebuilt"
_PRODUCER: Final = ProducerV1(
    producer_id=PLATFORM_PRODUCER_ID, name="agent-context-platform", version="1"
)
_EVENT_ID_DOMAIN: Final = b"agent-context-platform.projection-rebuilt\n"
_PUBLIC_ERROR_CLASS: Final = re.compile(r"^[A-Za-z][A-Za-z0-9_.]{0,127}$")


def public_error_class(error: BaseException) -> str:
    """The error's class name when it is a valid public token, else a fixed fallback."""
    name = type(error).__name__
    return name if _PUBLIC_ERROR_CLASS.fullmatch(name) else "ProjectionError"


@dataclass(frozen=True, slots=True)
class RunRecord:
    """One projector's verify or rebuild outcome, ready to become a `projection.rebuilt` event."""

    projector_name: str
    projector_version: str
    mode: ProjectionMode
    outcome: ProjectionOutcome
    started_at: datetime
    completed_at: datetime
    from_event_id: UUID | None
    through_event_id: UUID | None
    event_count: int
    graph_digest: str | None
    error_class: str | None = None


def system_event_key(run: RunRecord) -> str:
    """The idempotency key: the same finding about the same ledger range and graph is one fact.

    Derived from the projector, mode, outcome (so a later mismatch is recorded even when the graph
    digest has not changed), ledger range and digest. Times are not part of it.
    """
    material = _canonical(
        [
            run.projector_name,
            run.projector_version,
            run.mode.value,
            run.outcome.value,
            None if run.from_event_id is None else str(run.from_event_id),
            None if run.through_event_id is None else str(run.through_event_id),
            run.event_count,
            run.graph_digest,
            run.error_class,
        ]
    )
    return f"{SYSTEM_EVENT_TYPE}:{hashlib.sha256(material.encode()).hexdigest()}"


def _event_id_for(key: str) -> UUID:
    """A deterministic UUIDv7-shaped event ID (the SDK types `event_id` as UUID7)."""
    digest = bytearray(hashlib.sha256(_EVENT_ID_DOMAIN + key.encode()).digest()[:16])
    digest[6] = (digest[6] & 0x0F) | 0x70
    digest[8] = (digest[8] & 0x3F) | 0x80
    return UUID(bytes=bytes(digest))


def build_system_draft(run: RunRecord, *, observed_at: datetime) -> EventDraftV1:
    payload = ProjectionRebuiltV1(
        projector_name=run.projector_name,
        projector_version=run.projector_version,
        mode=run.mode,
        started_at=run.started_at,
        completed_at=run.completed_at,
        from_event_id=run.from_event_id,
        through_event_id=run.through_event_id,
        event_count=run.event_count,
        graph_digest=run.graph_digest,
        outcome=run.outcome,
        error_class=run.error_class,
    )
    key = system_event_key(run)
    return EventDraftV1(
        event_id=_event_id_for(key),
        event_type=SYSTEM_EVENT_TYPE,
        stream_id=SYSTEM_STREAM_ID,
        occurred_at=run.completed_at,
        observed_at=max(observed_at, run.completed_at),
        producer=_PRODUCER,
        payload=payload.model_dump(mode="json"),
        redaction=EventRedactionSummaryV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED, finding_counts={}
        ),
        idempotency_key=key,
    )


class NoContentBlobStore:
    """A blob store for in-process producers that never carry content: any use is a bug."""

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        raise BlobStoreError("system events carry no content")

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        raise BlobStoreError("system events carry no content")

    async def delete(self, object_key: str) -> None:
        raise BlobStoreError("system events carry no content")


class RecordingError(RuntimeError):
    """The ledger refused a `projection.rebuilt` event."""


@dataclass(frozen=True, slots=True)
class RecordResult:
    appended: int
    existing: int

    def to_dict(self) -> dict[str, int]:
        return {"appended": self.appended, "existing": self.existing}


class ProjectionEventRecorder:
    """Appends `projection.rebuilt` events in process, as the platform's own producer."""

    def __init__(
        self,
        ingestion: IngestionService,
        session_factory: async_sessionmaker[AsyncSession],
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self._ingestion = ingestion
        self._session_factory = session_factory
        self._clock = clock

    @classmethod
    def in_process(
        cls,
        session_factory: async_sessionmaker[AsyncSession],
        clock: Clock = lambda: datetime.now(UTC),
    ) -> ProjectionEventRecorder:
        content = ContentService(NoContentBlobStore(), RedactionPolicyV1())
        return cls(IngestionService(content, session_factory), session_factory, clock)

    async def record(self, runs: Sequence[RunRecord]) -> RecordResult:
        """Append the events not already in the ledger; an identical earlier report is a no-op."""
        drafts = [build_system_draft(run, observed_at=self._clock()) for run in runs]
        keys = [(PLATFORM_PRODUCER_ID, draft.idempotency_key) for draft in drafts]
        async with self._session_factory() as session:
            stored = await LedgerRepository.get_by_idempotency_keys(session, keys)
        fresh = [
            draft for draft in drafts if (PLATFORM_PRODUCER_ID, draft.idempotency_key) not in stored
        ]
        if not fresh:
            return RecordResult(appended=0, existing=len(drafts))
        outcome = await self._ingestion.ingest(
            IngestBatchRequestV1(batch_id=new_uuid7(), events=tuple(fresh))
        )
        if outcome.http_status >= 300:
            codes = {item.error_code for item in outcome.response.rejected}
            if outcome.http_status == 409 and codes <= {"idempotency_conflict", "batch_rejected"}:
                # A concurrent identical report won the race: it is recorded.
                return RecordResult(appended=0, existing=len(drafts))
            raise RecordingError(f"ledger rejected the report ({','.join(sorted(codes))})")
        return RecordResult(appended=len(fresh), existing=len(drafts) - len(fresh))


# --------------------------------------------------------------------------------------------
# Verification
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class VerificationReport:
    """The outcome of one verification or of the verification step of a rebuild."""

    projectors: tuple[ProjectorVerification, ...]
    streams: StreamHeadCheck
    graph: GraphDigest
    orphan_count: int
    orphan_sample: tuple[str, ...]
    source_ids_checked: int
    ledger_head_outbox_id: int | None
    replay_digest: str | None = None
    recorded: RecordResult | None = None
    record_error: str | None = None

    @property
    def replay_matches(self) -> bool | None:
        return None if self.replay_digest is None else self.replay_digest == self.graph.digest

    def problems(self, *, require_caught_up: bool = False) -> list[str]:
        found = [f"{item.name}: {issue}" for item in self.projectors for issue in item.issues]
        if self.streams.mismatched:
            found.append("streams: head_mismatch")
        if require_caught_up and self.streams.behind:
            found.append("streams: behind")
        if self.orphan_count:
            found.append("graph: orphan_source_ids")
        if self.replay_matches is False:
            found.append("graph: replay_digest_mismatch")
        return found

    def ok(self, *, require_caught_up: bool = False) -> bool:
        return not self.problems(require_caught_up=require_caught_up)

    def to_dict(self, *, require_caught_up: bool = False) -> dict[str, Any]:
        problems = self.problems(require_caught_up=require_caught_up)
        return {
            "ok": not problems,
            "problems": problems,
            "ledger_head_outbox_id": self.ledger_head_outbox_id,
            "projectors": [item.to_dict() for item in self.projectors],
            "streams": self.streams.to_dict(),
            "graph": self.graph.to_dict(),
            "orphans": {
                "checked": self.source_ids_checked,
                "count": self.orphan_count,
                "sample": list(self.orphan_sample),
            },
            "replay_digest": self.replay_digest,
            "replay_matches": self.replay_matches,
            "recorded": None if self.recorded is None else self.recorded.to_dict(),
            "record_error": self.record_error,
        }


def _runs(
    verifications: Sequence[ProjectorVerification],
    *,
    mode: ProjectionMode,
    outcome: ProjectionOutcome,
    started_at: datetime,
    completed_at: datetime,
    digest: str | None,
    error_class: str | None = None,
) -> list[RunRecord]:
    return [
        RunRecord(
            projector_name=item.name,
            projector_version=item.version,
            mode=mode,
            outcome=outcome,
            started_at=started_at,
            completed_at=completed_at,
            from_event_id=item.from_event_id,
            through_event_id=item.through_event_id,
            event_count=item.handled_events,
            graph_digest=digest,
            error_class=error_class,
        )
        for item in verifications
    ]


async def _scan_with_orphans(
    store: Neo4jStore,
    session_factory: async_sessionmaker[AsyncSession],
    batch_size: int,
) -> tuple[GraphDigest, OrphanCollector]:
    collector = OrphanCollector(session_factory)
    digest = await compute_graph_digest(store, batch_size=batch_size, on_source_ids=collector.add)
    return digest, collector


async def _collect_at(
    session_factory: async_sessionmaker[AsyncSession],
    projectors: Sequence[Projector],
    position: int | None,
) -> tuple[list[tuple[Projector, CheckpointView | None, list[EventTypeStats]]], int | None]:
    """Ledger aggregates for every projector as of one outbox `position`, and the ledger head."""
    async with session_factory() as session:
        head = await ledger_head(session)
        stats = await event_type_stats(session, position)
    return [(projector, None, stats) for projector in projectors], head


async def _collect_ledger_state(
    session_factory: async_sessionmaker[AsyncSession], projectors: Sequence[Projector]
) -> tuple[list[tuple[Projector, CheckpointView | None, list[EventTypeStats]]], int | None]:
    async with session_factory() as session:
        head = await ledger_head(session)
        collected = []
        for projector in projectors:
            checkpoint = await _checkpoint_view(session, projector)
            position = None if checkpoint is None else checkpoint.outbox_id
            collected.append((projector, checkpoint, await event_type_stats(session, position)))
    return collected, head


def handled_event_types(
    collected: Sequence[tuple[Projector, CheckpointView | None, Sequence[EventTypeStats]]],
) -> set[str]:
    """Ledger event types at least one of the collected projectors handles."""
    return {
        item.event_type
        for projector, _checkpoint, stats in collected
        for item in stats
        if projector.handles(item.event_type)
    }


async def verify_projections(
    session_factory: async_sessionmaker[AsyncSession],
    store: Neo4jStore,
    projectors: Sequence[Projector],
    *,
    require_caught_up: bool = False,
    replay_target: Neo4jStore | None = None,
    wipe_replay_target: bool = False,
    recorder: ProjectionEventRecorder | None = None,
    clock: Clock = lambda: datetime.now(UTC),
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> VerificationReport:
    """Verify the live graph and checkpoints against the ledger; read-only except the report.

    The checks need a quiescent projection: a runner mid-batch legitimately has rows in flight.
    With `replay_target`, the delivered events are also replayed into that scratch store and its
    digest compared with the live graph. The outcome is recorded as a `projection.rebuilt` event
    (mode verify) per projector when a `recorder` is given; a failure while reading the graph is
    recorded as `failed` and re-raised.
    """
    started = clock()
    collected, head = await _collect_ledger_state(session_factory, projectors)
    verifications = tuple(
        classify_projector(projector, checkpoint, stats, require_caught_up=require_caught_up)
        for projector, checkpoint, stats in collected
    )
    try:
        async with session_factory() as session:
            streams = await check_stream_heads(session, handled_event_types(collected))
        graph, collector = await _scan_with_orphans(store, session_factory, batch_size)
        replay_digest = None
        if replay_target is not None:
            replay_digest = await _replay_digest(
                session_factory,
                replay_target,
                projectors,
                wipe=wipe_replay_target,
                batch_size=batch_size,
            )
    except Exception as error:
        if recorder is not None:
            with contextlib.suppress(Exception):  # the original failure is the one to surface
                await recorder.record(
                    _runs(
                        verifications,
                        mode=ProjectionMode.VERIFY,
                        outcome=ProjectionOutcome.FAILED,
                        started_at=started,
                        completed_at=clock(),
                        digest=None,
                        error_class=public_error_class(error),
                    )
                )
        raise
    report = VerificationReport(
        projectors=verifications,
        streams=streams,
        graph=graph,
        orphan_count=len(collector.orphans),
        orphan_sample=tuple(sorted(collector.orphans)[:_SAMPLE_SIZE]),
        source_ids_checked=collector.checked,
        ledger_head_outbox_id=head,
        replay_digest=replay_digest,
    )
    if recorder is None:
        return report
    outcome = (
        ProjectionOutcome.MATCHED
        if report.ok(require_caught_up=require_caught_up)
        else ProjectionOutcome.MISMATCHED
    )
    try:
        recorded = await recorder.record(
            _runs(
                verifications,
                mode=ProjectionMode.VERIFY,
                outcome=outcome,
                started_at=started,
                completed_at=max(clock(), started),
                digest=graph.digest,
            )
        )
    except Exception:
        # Recording never masks the verification result.
        return replace(report, record_error=RECORD_FAILED)
    return replace(report, recorded=recorded)


# --------------------------------------------------------------------------------------------
# Replay and rebuild
# --------------------------------------------------------------------------------------------

DEFAULT_PAGE_SIZE: Final = 200
_WIPE_BATCH: Final = 5000


class ProjectionOperationError(RuntimeError):
    """A rebuild or verification cannot proceed; the message is safe to show."""


class RunnerActiveError(ProjectionOperationError):
    """The projection runner holds outbox leases, so an in-place rebuild must not start."""


class TargetNotEmptyError(ProjectionOperationError):
    """The rebuild target already holds graph data and a wipe was not confirmed."""


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """What a replay projected."""

    rows: int
    projected_events: int
    head_outbox_id: int | None
    progress: Mapping[tuple[str, str], ReplayProgress]


async def count_nodes(store: Neo4jStore) -> int:
    async def read(tx: Neo4jTransaction) -> int:
        result = await tx.run("MATCH (n) RETURN count(n) AS nodes", parameters={})
        return int(result.records[0]["nodes"])

    return await store.execute_read(read)


async def wipe_graph(store: Neo4jStore, *, batch: int = _WIPE_BATCH) -> int:
    """Delete every node and relationship in bounded transactions; return the nodes deleted."""
    deleted = 0

    async def delete_batch(tx: Neo4jTransaction) -> int:
        result = await tx.run(
            "MATCH (n) WITH n LIMIT $limit DETACH DELETE n RETURN count(*) AS deleted",
            parameters={"limit": batch},
        )
        return int(result.records[0]["deleted"])

    while True:
        step = await store.execute_write(delete_batch)
        if step == 0:
            return deleted
        deleted += step


async def replay_ledger(
    session_factory: async_sessionmaker[AsyncSession],
    target: Neo4jStore,
    projectors: Sequence[Projector],
    *,
    delivered_only: bool,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> ReplayResult:
    """Project the ledger into `target` in ledger (outbox) order, with its own loop.

    Reads only: nothing in PostgreSQL changes, so the live checkpoints and outbox are untouched.
    Every event is integrity-checked as the runner does and applied by the matching projectors, in
    registry order, in one write transaction. `delivered_only` limits the replay to events the
    runner has already delivered, which is exactly the set behind the live graph and checkpoints.
    The per-projector `progress` is the transient checkpoint state of this replay.
    """
    progress = {(item.name, item.version): ReplayProgress() for item in projectors}
    after = 0
    rows = projected = 0
    head: int | None = None
    while True:
        statement = select(OutboxRow.outbox_id, OutboxRow.event_id).where(
            OutboxRow.outbox_id > after
        )
        if delivered_only:
            statement = statement.where(OutboxRow.status == OutboxStatus.DELIVERED)
        async with session_factory() as session:
            page = (
                await session.execute(statement.order_by(OutboxRow.outbox_id).limit(page_size))
            ).all()
            if not page:
                return ReplayResult(rows, projected, head, progress)
            for outbox_id, event_id in page:
                event = await LedgerRepository.get_event(session, event_id)
                if event is None:
                    raise OrphanedOutboxRowError("ledger event missing for an outbox row")
                if not verify_event(event):
                    raise EventIntegrityError("a stored event failed integrity verification")
                matching = [item for item in projectors if item.handles(event.event_type)]
                if matching:

                    async def apply(
                        tx: Neo4jTransaction,
                        _event: Any = event,
                        _matching: list[Projector] = matching,
                    ) -> None:
                        for projector in _matching:
                            await projector.project(tx, _event)

                    await target.execute_write(apply)
                    projected += 1
                    for item in matching:
                        step = progress[(item.name, item.version)]
                        step.count += 1
                        step.last_outbox_id = outbox_id
                        step.last_event_id = event.event_id
                rows += 1
                after = head = outbox_id


async def _replay_digest(
    session_factory: async_sessionmaker[AsyncSession],
    target: Neo4jStore,
    projectors: Sequence[Projector],
    *,
    wipe: bool,
    batch_size: int,
) -> str:
    """Digest of a scratch replay of the delivered events (the verify `--replay-check`)."""
    existing = await count_nodes(target)
    if existing and not wipe:
        raise TargetNotEmptyError("the replay target holds graph data")
    if existing:
        await wipe_graph(target)
    await ensure_schema(target)
    await replay_ledger(session_factory, target, projectors, delivered_only=True)
    return (await compute_graph_digest(target, batch_size=batch_size)).digest


async def active_lease_count(session: AsyncSession) -> int:
    """Outbox rows leased and not yet expired: the only runner signal the runtime keeps."""
    return (
        await session.scalar(
            select(func.count())
            .select_from(OutboxRow)
            .where(OutboxRow.status == OutboxStatus.LEASED, OutboxRow.lease_expires_at > func.now())
        )
        or 0
    )


async def _write_checkpoints(
    session_factory: async_sessionmaker[AsyncSession],
    projectors: Sequence[Projector],
    progress: Mapping[tuple[str, str], ReplayProgress],
    *,
    state: ProjectionState,
    now: datetime,
) -> None:
    async with session_factory() as session, session.begin():
        for projector in projectors:
            step = progress.get((projector.name, projector.version), ReplayProgress())
            values = {
                "last_outbox_id": step.last_outbox_id,
                "last_event_id": step.last_event_id,
                "processed_count": step.count,
                "state": state,
                "updated_at": now,
            }
            await session.execute(
                insert(ProjectionCheckpointRow)
                .values(
                    projector_name=projector.name, projector_version=projector.version, **values
                )
                .on_conflict_do_update(
                    index_elements=[
                        ProjectionCheckpointRow.projector_name,
                        ProjectionCheckpointRow.projector_version,
                    ],
                    set_=values,
                )
            )


@dataclass(frozen=True, slots=True)
class RebuildReport:
    """The outcome of a rebuild: the verified target, its digest and what was recorded."""

    mode: str
    target: str
    projected_events: int
    verification: VerificationReport
    wiped_nodes: int
    outcome: ProjectionOutcome
    recorded: RecordResult | None
    record_error: str | None = None

    @property
    def ok(self) -> bool:
        return self.outcome is ProjectionOutcome.COMPLETED

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "outcome": self.outcome.value,
            "mode": self.mode,
            "target": self.target,
            "verified_target": self.target if self.ok else None,
            "graph_digest": self.verification.graph.digest,
            "projected_events": self.projected_events,
            "wiped_nodes": self.wiped_nodes,
            "verification": self.verification.to_dict(),
            "recorded": None if self.recorded is None else self.recorded.to_dict(),
            "record_error": self.record_error,
        }


async def _verify_replayed_target(
    session_factory: async_sessionmaker[AsyncSession],
    target: Neo4jStore,
    projectors: Sequence[Projector],
    replay: ReplayResult,
    batch_size: int,
) -> VerificationReport:
    """Coverage complete, no orphans, digest taken: the checks before a target may be adopted."""
    collected, head = await _collect_at(session_factory, projectors, replay.head_outbox_id)
    verifications = tuple(
        classify_replay(projector, replay.progress[(projector.name, projector.version)], stats)
        for projector, _checkpoint, stats in collected
    )
    async with session_factory() as session:
        streams = await check_stream_heads(session, handled_event_types(collected))
    graph, collector = await _scan_with_orphans(target, session_factory, batch_size)
    return VerificationReport(
        projectors=verifications,
        streams=streams,
        graph=graph,
        orphan_count=len(collector.orphans),
        orphan_sample=tuple(sorted(collector.orphans)[:_SAMPLE_SIZE]),
        source_ids_checked=collector.checked,
        ledger_head_outbox_id=head,
    )


async def rebuild_projections(
    session_factory: async_sessionmaker[AsyncSession],
    target: Neo4jStore,
    projectors: Sequence[Projector],
    *,
    target_description: str,
    in_place: bool,
    wipe_target: bool = False,
    recorder: ProjectionEventRecorder | None = None,
    clock: Clock = lambda: datetime.now(UTC),
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> RebuildReport:
    """Rebuild the graph projection from the ledger into `target`, then verify it.

    Neo4j Community has a single user database per instance, so a rebuild cannot sit beside the
    live graph in a second database. Its target is therefore an explicit CONNECTION (a standby or
    scratch instance). `in_place=False` (normal mode) builds that target from scratch with its own
    replay loop and never touches the live checkpoints or the outbox; the caller has already
    refused a target equal to the live projection. It refuses a non-empty target unless
    `wipe_target`, creates the schema, replays every event, and verifies coverage, orphans and the
    digest. Adopting the target (pointing the API and projector at it) is an operator step.

    `in_place=True` rebuilds the live graph (`target` is the live store). It refuses while the
    projection runner holds an unexpired outbox lease. Order, so a crash never leaves a checkpoint
    claiming progress the graph lacks: mark checkpoints rebuilding at zero, wipe the graph, create
    the schema, replay the delivered events, write the checkpoints the replay reached, and verify
    the live projection like `verify_projections`. Pending events stay pending for the runner.
    """
    started = clock()
    wiped = 0
    if in_place:
        async with session_factory() as session:
            if await active_lease_count(session):
                raise RunnerActiveError("the projection runner holds outbox leases; stop it first")
        await _write_checkpoints(
            session_factory, projectors, {}, state=ProjectionState.REBUILDING, now=started
        )
    else:
        existing = await count_nodes(target)
        if existing and not wipe_target:
            raise TargetNotEmptyError("the rebuild target holds graph data")
        if existing:
            wiped = await wipe_graph(target)
    projected = 0
    verification: VerificationReport | None = None
    try:
        if in_place:
            wiped = await wipe_graph(target)
        await ensure_schema(target)
        replay = await replay_ledger(session_factory, target, projectors, delivered_only=in_place)
        projected = replay.projected_events
        if in_place:
            await _write_checkpoints(
                session_factory,
                projectors,
                replay.progress,
                state=ProjectionState.ACTIVE,
                now=clock(),
            )
            verification = await verify_projections(
                session_factory, target, projectors, clock=clock, batch_size=batch_size
            )
        else:
            verification = await _verify_replayed_target(
                session_factory, target, projectors, replay, batch_size
            )
    except Exception as error:
        if recorder is not None:
            with contextlib.suppress(Exception):  # the original failure is the one to surface
                collected, _head = await _collect_ledger_state(session_factory, projectors)
                await recorder.record(
                    _runs(
                        tuple(classify_replay(item, None, stats) for item, _cp, stats in collected),
                        mode=ProjectionMode.REBUILD,
                        outcome=ProjectionOutcome.FAILED,
                        started_at=started,
                        completed_at=max(clock(), started),
                        digest=None,
                        error_class=public_error_class(error),
                    )
                )
        raise
    ok = verification.ok()
    outcome = ProjectionOutcome.COMPLETED if ok else ProjectionOutcome.FAILED
    recorded = None
    record_error = None
    if recorder is not None:
        try:
            recorded = await recorder.record(
                _runs(
                    verification.projectors,
                    mode=ProjectionMode.REBUILD,
                    outcome=outcome,
                    started_at=started,
                    completed_at=max(clock(), started),
                    digest=verification.graph.digest,
                    error_class=None if ok else "VerificationFailed",
                )
            )
        except Exception:
            # The rebuild and its verification stand; only the ledger report is missing.
            record_error = RECORD_FAILED
    return RebuildReport(
        mode="in_place" if in_place else "standby",
        target=target_description,
        projected_events=projected,
        verification=verification,
        wiped_nodes=wiped,
        outcome=outcome,
        recorded=recorded,
        record_error=record_error,
    )


# --------------------------------------------------------------------------------------------
# Grant preflight
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GrantRequirement:
    """One privilege a connection must hold, checked with `has_*_privilege` (inherited roles count)."""

    kind: str  # "table", "column" or "sequence"
    obj: str
    privilege: str
    column: str | None = None

    @property
    def label(self) -> str:
        where = self.obj if self.column is None else f"{self.obj}.{self.column}"
        return f"{self.privilege} on {where}"


_READ_TABLES: Final = (
    "ledger.events",
    "ledger.event_streams",
    "ledger.event_content_refs",
    "projection.outbox",
    "projection.projection_checkpoints",
    "projection.dead_letters",
)
_CHECKPOINT_COLUMNS: Final = (
    "last_outbox_id",
    "last_event_id",
    "processed_count",
    "state",
    "updated_at",
)
# Checkpoints, dead letters, stream heads and in-place resets: the projector role.
PROJECTOR_READ_GRANTS: Final = tuple(GrantRequirement("table", t, "SELECT") for t in _READ_TABLES)
PROJECTOR_CHECKPOINT_GRANTS: Final = (
    GrantRequirement("table", "projection.projection_checkpoints", "INSERT"),
    *(
        GrantRequirement("column", "projection.projection_checkpoints", "UPDATE", column)
        for column in _CHECKPOINT_COLUMNS
    ),
)
# Recording `projection.rebuilt` through `IngestionService`: the API role.
RECORDER_GRANTS: Final = (
    GrantRequirement("table", "ledger.events", "SELECT"),
    GrantRequirement("table", "ledger.events", "INSERT"),
    GrantRequirement("table", "ledger.event_streams", "SELECT"),
    GrantRequirement("table", "ledger.event_streams", "INSERT"),
    GrantRequirement("column", "ledger.event_streams", "UPDATE", "last_sequence"),
    GrantRequirement("table", "ledger.event_content_refs", "SELECT"),
    GrantRequirement("table", "projection.outbox", "INSERT"),
    GrantRequirement("sequence", "projection.outbox_outbox_id_seq", "USAGE"),
)


class MissingGrantError(ProjectionOperationError):
    """A connection lacks privileges the operation needs; raised before anything is mutated."""

    def __init__(self, role: str, missing: Sequence[str]) -> None:
        super().__init__(f"the {role} connection lacks: {'; '.join(missing)}")
        self.role = role
        self.missing = tuple(missing)


async def check_grants(
    session_factory: async_sessionmaker[AsyncSession],
    requirements: Sequence[GrantRequirement],
    *,
    role: str,
) -> None:
    """Fail with `MissingGrantError` unless the connection holds every required privilege."""
    missing: list[str] = []
    async with session_factory() as session:
        for item in requirements:
            if item.kind == "table":
                query = text("SELECT has_table_privilege(:obj, :priv)")
                params: dict[str, Any] = {"obj": item.obj, "priv": item.privilege}
            elif item.kind == "column":
                query = text("SELECT has_column_privilege(:obj, :col, :priv)")
                params = {"obj": item.obj, "col": item.column, "priv": item.privilege}
            else:
                query = text("SELECT has_sequence_privilege(:obj, :priv)")
                params = {"obj": item.obj, "priv": item.privilege}
            if not await session.scalar(query, params):
                missing.append(item.label)
    if missing:
        raise MissingGrantError(role, missing)


async def preflight(
    projector_sessions: async_sessionmaker[AsyncSession],
    recorder_sessions: async_sessionmaker[AsyncSession] | None,
    *,
    write_checkpoints: bool,
) -> None:
    """Check both connections' grants; call before any target or checkpoint is mutated.

    `recorder_sessions` is None when the report is not recorded. `write_checkpoints` is the
    in-place rebuild, which rewrites the live checkpoints.
    """
    needed = PROJECTOR_READ_GRANTS + (PROJECTOR_CHECKPOINT_GRANTS if write_checkpoints else ())
    await check_grants(projector_sessions, needed, role="projector")
    if recorder_sessions is not None:
        await check_grants(recorder_sessions, RECORDER_GRANTS, role="api")


# --------------------------------------------------------------------------------------------
# Status
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ProjectorStatus:
    """A read-only snapshot of one projector."""

    name: str
    version: str
    state: str | None
    checkpoint_outbox_id: int | None
    processed_count: int
    ledger_head_outbox_id: int | None
    lag: int
    last_error_class: str | None
    last_run_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "state": self.state,
            "checkpoint_outbox_id": self.checkpoint_outbox_id,
            "processed_count": self.processed_count,
            "ledger_head_outbox_id": self.ledger_head_outbox_id,
            "lag": self.lag,
            "last_error_class": self.last_error_class,
            "last_run_at": None if self.last_run_at is None else self.last_run_at.isoformat(),
        }


async def projection_status(
    session_factory: async_sessionmaker[AsyncSession], projectors: Sequence[Projector]
) -> list[ProjectorStatus]:
    """Per projector: checkpoint, lag behind the ledger head, last error class and last run."""
    collected, head = await _collect_ledger_state(session_factory, projectors)
    statuses: list[ProjectorStatus] = []
    async with session_factory() as session:
        for projector, checkpoint, stats in collected:
            verification = classify_projector(projector, checkpoint, stats)
            error_class = await session.scalar(
                select(DeadLetterRow.error_class)
                .where(
                    DeadLetterRow.projector_name == projector.name,
                    DeadLetterRow.projector_version == projector.version,
                )
                .order_by(DeadLetterRow.updated_at.desc(), DeadLetterRow.outbox_id.desc())
                .limit(1)
            )
            statuses.append(
                ProjectorStatus(
                    name=projector.name,
                    version=projector.version,
                    state=None if checkpoint is None else checkpoint.state,
                    checkpoint_outbox_id=verification.checkpoint_outbox_id,
                    processed_count=verification.processed_count,
                    ledger_head_outbox_id=head,
                    lag=verification.lag,
                    last_error_class=error_class,
                    last_run_at=None if checkpoint is None else checkpoint.updated_at,
                )
            )
    return statuses


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "NODE_KEYS",
    "PLATFORM_PRODUCER_ID",
    "RECORD_FAILED",
    "SOURCE_ID_PROPERTIES",
    "VOLATILE_PROPERTIES",
    "CheckpointView",
    "EventTypeStats",
    "GrantRequirement",
    "GraphDigest",
    "GraphScanError",
    "MissingGrantError",
    "NoContentBlobStore",
    "OrphanCollector",
    "ProjectionEventRecorder",
    "ProjectionOperationError",
    "ProjectorStatus",
    "ProjectorVerification",
    "RebuildReport",
    "RecordResult",
    "RecordingError",
    "ReplayProgress",
    "ReplayResult",
    "RunRecord",
    "RunnerActiveError",
    "StreamHeadCheck",
    "TargetNotEmptyError",
    "VerificationReport",
    "check_grants",
    "classify_projector",
    "classify_replay",
    "compute_graph_digest",
    "count_nodes",
    "node_identity",
    "preflight",
    "primary_label",
    "projection_status",
    "rebuild_projections",
    "replay_ledger",
    "source_ids",
    "system_event_key",
    "verify_projections",
    "wipe_graph",
]
