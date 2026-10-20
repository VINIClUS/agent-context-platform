"""Project quality observations (test run, CI run, finding) into the graph.

Each run or finding is one node keyed by its native ID. Re-observing it moves its result forward
only, on the total order `(occurred_at, event_id)`, so any delivery order converges.

`(:TestRun)-[:VALIDATES]->(:Commit|WorkspaceSnapshot)` is the only quality relation §9.2 defines.
It follows the run's CURRENT observation: when a newer observation of the same `test_run_id`
changes the target, the old edge is deleted, and a commit, snapshot or repository that only exists
as the identity-only stub that edge created is deleted with it, so the node and its edges agree and
any delivery order converges (an older observation arriving late changes nothing).
A payload names its target as a bare commit OID or snapshot ID; a commit node is keyed by
`(repository, oid)`, and the repository only travels in the envelope `context`. Without
`context.repository_id` the commit relation is skipped rather than guessed. CI runs and findings
keep their target as `commit_id`/`snapshot_id` properties, because §9.2 has no relation for them.
The commit and snapshot nodes are identity-only stubs until the git projector fills them, the way
that projector stubs a parent commit. Only content IDs enter the graph, never output text.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Final, LiteralString

from agent_context_sdk import (
    CIRunCompletedV1,
    FindingObservedV1,
    StoredEventV1,
    TestRunCompletedV1,
)

from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.projectors import (
    assert_link,
    event_order,
    lock_event_nodes,
    lock_nodes,
    min_non_null,
    newest_wins,
    node_statement,
    relationship_statement,
)
from agent_context_platform.projection.projectors.code import timestamp
from agent_context_platform.projection.projectors.git import commit_node_id

_TEST_RUN: Final = node_statement("TestRun", "test_run_id") + newest_wins(
    "result",
    "framework",
    "status",
    "total_count",
    "passed_count",
    "failed_count",
    "skipped_count",
    "error_count",
    "duration_ms",
    "output_content_id",
    "commit_id",
    "snapshot_id",
    "completed_at",
)
_CI_RUN: Final = node_statement("CIRun", "ci_run_id") + newest_wins(
    "result",
    "provider",
    "workflow",
    "job",
    "external_id",
    "status",
    "duration_ms",
    "output_content_id",
    "error_class",
    "commit_id",
    "snapshot_id",
    "completed_at",
)
_FINDING: Final = node_statement("Finding", "finding_id") + newest_wins(
    "result",
    "scanner",
    "rule_id",
    "severity",
    "status",
    "fingerprint_sha256",
    "path",
    "symbol_id",
    "details_content_id",
    "commit_id",
    "snapshot_id",
    "observed_at",
)

# The same identity-only commit stub the git projector writes for a parent commit.
_COMMIT_STUB: Final = node_statement("Commit", "commit_id") + min_non_null("repository_id", "oid")
_COMMIT_IN_REPOSITORY: Final = relationship_statement(
    "Commit", "commit_id", "IN_REPOSITORY", "Repository", "repository_id"
)
_VALIDATES_COMMIT: Final = relationship_statement(
    "TestRun", "test_run_id", "VALIDATES", "Commit", "commit_id"
)
_VALIDATES_SNAPSHOT: Final = relationship_statement(
    "TestRun", "test_run_id", "VALIDATES", "WorkspaceSnapshot", "snapshot_id"
)


_STORED_ORDER: Final[LiteralString] = (
    "MATCH (n:TestRun {test_run_id: $node_id}) RETURN n.result_order AS stored"
)
_OLD_TARGETS: Final[LiteralString] = (
    "MATCH (:TestRun {test_run_id: $node_id})-[r:VALIDATES]->(t) "
    "RETURN head(labels(t)) AS label, coalesce(t.commit_id, t.snapshot_id) AS node_id, "
    "t.repository_id AS repository_id, r.source_event_ids AS source_event_ids"
)
_DROP_EDGES: Final[LiteralString] = (
    "MATCH (:TestRun {test_run_id: $node_id})-[r:VALIDATES]->() DELETE r"
)
# Take the dropped observation's event IDs off the commit's repository edge, then delete the
# edge, the commit and its repository when nothing but that stub is left of them.
_STRIP_COMMIT: Final[LiteralString] = (
    "MATCH (:Commit {commit_id: $node_id})-[r:IN_REPOSITORY]->(:Repository) "
    "SET r.source_event_ids = [x IN r.source_event_ids WHERE NOT x IN $source_event_ids] "
    "WITH r WHERE size(r.source_event_ids) = 0 DELETE r"
)
_DROP_COMMIT_STUB: Final[LiteralString] = (
    "MATCH (n:Commit {commit_id: $node_id}) "
    "WHERE NOT (n)--() AND all(k IN keys(n) WHERE k IN ['commit_id', 'repository_id', 'oid']) "
    "DELETE n"
)
_DROP_REPOSITORY_STUB: Final[LiteralString] = (
    "MATCH (n:Repository {repository_id: $node_id}) "
    "WHERE NOT (n)--() AND all(k IN keys(n) WHERE k = 'repository_id') DELETE n"
)
_DROP_SNAPSHOT_STUB: Final[LiteralString] = (
    "MATCH (n:WorkspaceSnapshot {snapshot_id: $node_id}) "
    "WHERE NOT (n)--() AND all(k IN keys(n) WHERE k = 'snapshot_id') DELETE n"
)


async def _drop_stubs(
    tx: Neo4jTransaction, commit_id: str | None, snapshot_id: str | None, repository: str | None
) -> None:
    """Delete the identity-only commit, snapshot and repository nodes nothing else uses."""
    if snapshot_id is not None:
        await tx.run(_DROP_SNAPSHOT_STUB, parameters={"node_id": snapshot_id})
    if commit_id is not None and repository is not None:
        node_id = commit_node_id(repository, commit_id)
        await tx.run(_DROP_COMMIT_STUB, parameters={"node_id": node_id})
        await tx.run(_DROP_REPOSITORY_STUB, parameters={"node_id": repository})


async def _replace_validated_targets(tx: Neo4jTransaction, test_run_id: str) -> None:
    """Delete the run's `VALIDATES` edges, and the stubs only they kept alive."""
    old = (await tx.run(_OLD_TARGETS, parameters={"node_id": test_run_id})).records
    if not old:
        return
    keys: list[tuple[str, str]] = []
    for record in old:
        keys.append((str(record["label"]), str(record["node_id"])))
        if record["repository_id"] is not None:
            keys.append(("Repository", str(record["repository_id"])))
    # The old targets are only known from the graph; lock them before changing them.
    await lock_nodes(tx, keys)
    await tx.run(_DROP_EDGES, parameters={"node_id": test_run_id})
    for record in old:
        target = str(record["node_id"])
        if record["label"] == "Commit":
            sources = [str(item) for item in record["source_event_ids"]]
            await tx.run(_STRIP_COMMIT, parameters={"node_id": target, "source_event_ids": sources})
            await tx.run(_DROP_COMMIT_STUB, parameters={"node_id": target})
        else:
            await tx.run(_DROP_SNAPSHOT_STUB, parameters={"node_id": target})
        if record["repository_id"] is not None:
            repository = str(record["repository_id"])
            await tx.run(_DROP_REPOSITORY_STUB, parameters={"node_id": repository})


async def _test_run_completed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = TestRunCompletedV1.model_validate(dict(event.payload))
    order = event_order(event)
    stored = (await tx.run(_STORED_ORDER, parameters={"node_id": payload.test_run_id})).records
    newer = not stored or stored[0]["stored"] is None or order > stored[0]["stored"]
    await tx.run(
        _TEST_RUN,
        parameters={
            "node_id": payload.test_run_id,
            "order": order,
            "framework": payload.framework,
            "status": payload.status,
            "total_count": payload.total_count,
            "passed_count": payload.passed_count,
            "failed_count": payload.failed_count,
            "skipped_count": payload.skipped_count,
            "error_count": payload.error_count,
            "duration_ms": payload.duration_ms,
            "output_content_id": payload.output_content_id,
            "commit_id": payload.commit_id,
            "snapshot_id": payload.snapshot_id,
            "completed_at": timestamp(event.occurred_at),
        },
    )
    if not newer:
        # Locking merged identity-only nodes for the targets this older observation names; an
        # older observation changes no edge, so leave nothing of them behind.
        await _drop_stubs(tx, payload.commit_id, payload.snapshot_id, event.context.repository_id)
        return
    await _replace_validated_targets(tx, payload.test_run_id)
    if payload.snapshot_id is not None:
        await assert_link(tx, _VALIDATES_SNAPSHOT, event, payload.test_run_id, payload.snapshot_id)
    repository = event.context.repository_id
    if payload.commit_id is not None and repository is not None:
        commit = commit_node_id(repository, payload.commit_id)
        await tx.run(
            _COMMIT_STUB,
            parameters={"node_id": commit, "repository_id": repository, "oid": payload.commit_id},
        )
        await assert_link(tx, _COMMIT_IN_REPOSITORY, event, commit, repository)
        await assert_link(tx, _VALIDATES_COMMIT, event, payload.test_run_id, commit)


async def _ci_run_completed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = CIRunCompletedV1.model_validate(dict(event.payload))
    await tx.run(
        _CI_RUN,
        parameters={
            "node_id": payload.ci_run_id,
            "order": event_order(event),
            "provider": payload.provider,
            "workflow": payload.workflow,
            "job": payload.job,
            "external_id": payload.external_id,
            "status": payload.status,
            "duration_ms": payload.duration_ms,
            "output_content_id": payload.output_content_id,
            "error_class": payload.error_class,
            "commit_id": payload.commit_id,
            "snapshot_id": payload.snapshot_id,
            "completed_at": timestamp(event.occurred_at),
        },
    )


async def _finding_observed(tx: Neo4jTransaction, event: StoredEventV1) -> None:
    payload = FindingObservedV1.model_validate(dict(event.payload))
    await tx.run(
        _FINDING,
        parameters={
            "node_id": payload.finding_id,
            "order": event_order(event),
            "scanner": payload.scanner,
            "rule_id": payload.rule_id,
            "severity": payload.severity,
            "status": payload.status,
            "fingerprint_sha256": payload.fingerprint_sha256,
            "path": payload.path,
            "symbol_id": payload.symbol_id,
            "details_content_id": payload.details_content_id,
            "commit_id": payload.commit_id,
            "snapshot_id": payload.snapshot_id,
            "observed_at": timestamp(event.occurred_at),
        },
    )


_HANDLERS: Final[dict[str, Callable[[Neo4jTransaction, StoredEventV1], Awaitable[None]]]] = {
    "quality.test_run.completed": _test_run_completed,
    "quality.ci_run.completed": _ci_run_completed,
    "quality.finding.observed": _finding_observed,
}

QUALITY_EVENT_TYPES: Final = frozenset(_HANDLERS)


def lock_keys(event: StoredEventV1) -> list[tuple[str, str]]:
    """Nodes `QualityProjector` writes for `event`, from the IDs it writes with."""
    kind = event.event_type
    if kind == "quality.test_run.completed":
        run = TestRunCompletedV1.model_validate(dict(event.payload))
        keys = [("TestRun", run.test_run_id)]
        if run.snapshot_id is not None:
            keys.append(("WorkspaceSnapshot", run.snapshot_id))
        repository = event.context.repository_id
        if run.commit_id is not None and repository is not None:
            keys += [
                ("Commit", commit_node_id(repository, run.commit_id)),
                ("Repository", repository),
            ]
        return keys
    if kind == "quality.ci_run.completed":
        return [("CIRun", CIRunCompletedV1.model_validate(dict(event.payload)).ci_run_id)]
    if kind == "quality.finding.observed":
        return [("Finding", FindingObservedV1.model_validate(dict(event.payload)).finding_id)]
    return []


class QualityProjector:
    """Projects test run, CI run and finding events."""

    name = "quality"
    version = "1"

    def handles(self, event_type: str) -> bool:
        return event_type in _HANDLERS

    async def project(self, tx: Neo4jTransaction, event: StoredEventV1) -> None:
        await lock_event_nodes(tx, event)
        await _HANDLERS[event.event_type](tx, event)
