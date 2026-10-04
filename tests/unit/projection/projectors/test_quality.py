"""Unit tests for the quality projector: routing, locking and statement parameters."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from agent_context_sdk import (
    EventContextV1,
    EventDraftV1,
    EventRedactionSummaryV1,
    ProducerV1,
    StoredEventV1,
    seal_event,
)
from agent_context_sdk.content import ContentDisposition

from agent_context_platform.projection.projectors import event_lock_keys
from agent_context_platform.projection.projectors.git import commit_node_id
from agent_context_platform.projection.projectors.quality import (
    QUALITY_EVENT_TYPES,
    QualityProjector,
)

OID = "a" * 40
MOMENT = datetime(2026, 8, 13, 13, 0, 0, tzinfo=UTC)
COUNTS = {
    "total_count": 3,
    "passed_count": 3,
    "failed_count": 0,
    "skipped_count": 0,
    "error_count": 0,
}


def _event(
    event_type: str, payload: dict[str, object], context: dict[str, str] | None = None
) -> StoredEventV1:
    draft = EventDraftV1(
        event_id=UUID("0198a4b1-98c0-7c28-ae3f-000000000001"),
        event_type=event_type,
        stream_id="stream",
        occurred_at=MOMENT,
        observed_at=MOMENT,
        producer=ProducerV1(producer_id="p", name="p", version="1"),
        context=EventContextV1(**(context or {})),
        payload=payload,
        redaction=EventRedactionSummaryV1(
            policy_version="v1", disposition=ContentDisposition.SANITIZED
        ),
        idempotency_key="key-1",
    )
    return seal_event(draft, [], 1, None)


def _test_run(target: dict[str, str], context: dict[str, str] | None) -> StoredEventV1:
    payload = {
        "test_run_id": "tr_1",
        "framework": "pytest",
        "status": "passed",
        "duration_ms": 10,
        "output_content_id": "out_1",
        **COUNTS,
        **target,
    }
    return _event("quality.test_run.completed", payload, context)


class Recorder:
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, *, parameters: dict[str, Any]) -> SimpleNamespace:
        self.statements.append((query, parameters))
        return SimpleNamespace(records=[])


def test_projector_handles_exactly_the_quality_event_types() -> None:
    projector = QualityProjector()
    assert {
        "quality.test_run.completed",
        "quality.ci_run.completed",
        "quality.finding.observed",
    } == QUALITY_EVENT_TYPES
    assert all(projector.handles(item) for item in QUALITY_EVENT_TYPES)
    assert not projector.handles("knowledge.failure.observed")


def test_a_commit_test_run_locks_the_commit_and_its_repository() -> None:
    event = _test_run({"commit_id": OID}, {"repository_id": "repo_1"})
    assert event_lock_keys(event) == sorted(
        [
            ("TestRun", "tr_1"),
            ("Commit", commit_node_id("repo_1", OID)),
            ("Repository", "repo_1"),
        ]
    )


def test_a_snapshot_test_run_locks_the_snapshot() -> None:
    event = _test_run({"snapshot_id": "snap_1"}, None)
    assert event_lock_keys(event) == [("TestRun", "tr_1"), ("WorkspaceSnapshot", "snap_1")]


def test_a_commit_test_run_without_a_repository_validates_nothing() -> None:
    event = _test_run({"commit_id": OID}, None)
    assert event_lock_keys(event) == [("TestRun", "tr_1")]
    tx = Recorder()
    asyncio.run(QualityProjector().project(tx, event))  # type: ignore[arg-type]
    assert not any("MERGE (a)-[r:VALIDATES]" in query for query, _ in tx.statements)
    assert any("MERGE (n:TestRun" in query for query, _ in tx.statements)


def test_a_finding_and_a_ci_run_lock_only_themselves() -> None:
    finding = _event(
        "quality.finding.observed",
        {
            "finding_id": "find_1",
            "scanner": "bandit",
            "rule_id": "B101",
            "severity": "low",
            "status": "open",
            "fingerprint_sha256": "f" * 64,
            "commit_id": OID,
        },
        {"repository_id": "repo_1"},
    )
    ci = _event(
        "quality.ci_run.completed",
        {
            "ci_run_id": "ci_1",
            "provider": "github",
            "workflow": "ci",
            "job": "test",
            "external_id": "42",
            "status": "success",
            "duration_ms": 5,
            "commit_id": OID,
        },
        {"repository_id": "repo_1"},
    )
    assert event_lock_keys(finding) == [("Finding", "find_1")]
    assert event_lock_keys(ci) == [("CIRun", "ci_1")]


def _run_projection(event: StoredEventV1) -> list[tuple[str, dict[str, Any]]]:
    tx = Recorder()
    asyncio.run(QualityProjector().project(tx, event))  # type: ignore[arg-type]
    return tx.statements


def test_test_and_ci_runs_are_written_with_the_envelope_scope() -> None:
    scope = {"project_id": "prj_1", "repository_id": "repo_1"}
    run = _run_projection(_test_run({"commit_id": OID}, scope))
    written = next(
        item for _, item in run if item.get("test_run_id") is None and "framework" in item
    )
    assert (written["project_id"], written["repository_id"]) == ("prj_1", "repo_1")
    ci = _run_projection(
        _event(
            "quality.ci_run.completed",
            {
                "ci_run_id": "ci_1",
                "provider": "github",
                "workflow": "ci",
                "job": "test",
                "external_id": "1",
                "status": "success",
                "duration_ms": 1,
                "commit_id": OID,
            },
            scope,
        )
    )
    query, params = next((q, p) for q, p in ci if "workflow" in p)
    assert params["recorded_at"] == "2026-08-13T13:00:00.000000Z"
    assert (params["project_id"], params["repository_id"]) == ("prj_1", "repo_1")
    assert "n.recorded_at" in query and "n.project_id" in query
    # without a context the scope is null, so it never overwrites a stored one
    bare = _run_projection(_test_run({"commit_id": OID}, None))
    unscoped = next(item for _, item in bare if "framework" in item)
    assert (unscoped["project_id"], unscoped["repository_id"]) == (None, None)


class _Replacing(Recorder):
    """A graph where the run already validates another commit and snapshot."""

    async def run(self, query: str, *, parameters: dict[str, Any]) -> SimpleNamespace:
        await super().run(query, parameters=parameters)
        if "RETURN n.result_order" in query:
            return SimpleNamespace(records=[{"stored": "0000"}])
        if "RETURN head(labels(t))" in query:
            return SimpleNamespace(
                records=[
                    {
                        "label": "Commit",
                        "node_id": "old_commit",
                        "repository_id": "repo_1",
                        "source_event_ids": ["ev_old"],
                    },
                    {
                        "label": "WorkspaceSnapshot",
                        "node_id": "snap_old",
                        "repository_id": None,
                        "source_event_ids": ["ev_old"],
                    },
                ]
            )
        return SimpleNamespace(records=[])


def test_a_newer_observation_replaces_the_old_validated_targets() -> None:
    tx = _Replacing()
    event = _test_run({"commit_id": OID}, {"repository_id": "repo_1"})
    asyncio.run(QualityProjector().project(tx, event))  # type: ignore[arg-type]
    queries = [query for query, _ in tx.statements]
    assert any("DELETE r" in query and "VALIDATES" in query for query in queries)
    assert any("IN_REPOSITORY" in query and "source_event_ids" in query for query in queries)
    stripped = [p for q, p in tx.statements if "x IN $source_event_ids" in q]
    assert stripped == [{"node_id": "old_commit", "source_event_ids": ["ev_old"]}]
    assert any("DETACH" not in q and "WorkspaceSnapshot" in q and "DELETE n" in q for q in queries)
