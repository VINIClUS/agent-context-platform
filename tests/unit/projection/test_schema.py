from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from agent_context_platform import projection
from agent_context_platform.projection.neo4j import Neo4jTransaction
from agent_context_platform.projection.schema import (
    CONSTRAINT_STATEMENTS,
    INDEX_STATEMENTS,
    SCHEMA_STATEMENTS,
    ensure_schema,
)

pytestmark = pytest.mark.unit

EXPECTED_CONSTRAINTS = {
    "uq_workspace_workspace_id": ("Workspace", "workspace_id"),
    "uq_project_project_id": ("Project", "project_id"),
    "uq_repository_repository_id": ("Repository", "repository_id"),
    "uq_branch_branch_id": ("Branch", "branch_id"),
    "uq_commit_commit_id": ("Commit", "commit_id"),
    "uq_checkout_checkout_id": ("Checkout", "checkout_id"),
    "uq_worktree_worktree_id": ("Worktree", "worktree_id"),
    "uq_workspace_snapshot_snapshot_id": ("WorkspaceSnapshot", "snapshot_id"),
    "uq_change_set_change_set_id": ("ChangeSet", "change_set_id"),
    "uq_module_module_id": ("Module", "module_id"),
    "uq_file_file_id": ("File", "file_id"),
    "uq_file_revision_file_revision_id": ("FileRevision", "file_revision_id"),
    "uq_symbol_symbol_id": ("Symbol", "symbol_id"),
    "uq_symbol_revision_symbol_revision_id": ("SymbolRevision", "symbol_revision_id"),
    "uq_dependency_dependency_id": ("Dependency", "dependency_id"),
    "uq_agent_runtime_agent_runtime_id": ("AgentRuntime", "agent_runtime_id"),
    "uq_session_session_id": ("Session", "session_id"),
    "uq_turn_turn_id": ("Turn", "turn_id"),
    "uq_tool_call_tool_call_id": ("ToolCall", "tool_call_id"),
    "uq_test_run_test_run_id": ("TestRun", "test_run_id"),
    "uq_ci_run_ci_run_id": ("CIRun", "ci_run_id"),
    "uq_artifact_artifact_id": ("Artifact", "artifact_id"),
    "uq_finding_finding_id": ("Finding", "finding_id"),
    "uq_decision_decision_id": ("Decision", "decision_id"),
    "uq_constraint_constraint_id": ("Constraint", "constraint_id"),
    "uq_failure_failure_id": ("Failure", "failure_id"),
    "uq_summary_summary_id": ("Summary", "summary_id"),
    "uq_assertion_assertion_id": ("Assertion", "assertion_id"),
    "uq_content_embedding_content_id": ("ContentEmbedding", "content_id"),
}

EXPECTED_INDEXES = {
    "ix_assertion_source_event_id",
    "ix_assertion_valid_from",
    "ix_assertion_valid_to",
    "ix_assertion_recorded_from",
    "ix_assertion_recorded_to",
    "vx_content_embedding_embedding",
}


def test_projection_package_exports_store_schema_and_read_capability() -> None:
    assert projection.Neo4jStore is not None
    assert projection.Neo4jReadFacade is not None
    assert projection.ensure_schema is ensure_schema


def test_schema_manifest_enumerates_named_literal_community_ddl() -> None:
    constraints = {
        statement.name: (statement.label, statement.property_name)
        for statement in CONSTRAINT_STATEMENTS
    }

    assert constraints == EXPECTED_CONSTRAINTS
    assert {statement.name for statement in INDEX_STATEMENTS} == EXPECTED_INDEXES
    assert len(SCHEMA_STATEMENTS) == len(EXPECTED_CONSTRAINTS) + len(EXPECTED_INDEXES)
    assert all(" IF NOT EXISTS " in statement.query for statement in SCHEMA_STATEMENTS)
    assert all(statement.name in statement.query for statement in SCHEMA_STATEMENTS)

    vector = next(
        statement
        for statement in INDEX_STATEMENTS
        if statement.name == "vx_content_embedding_embedding"
    )
    assert vector.parameters == {"dimensions": 384, "similarity": "cosine"}
    assert "$dimensions" in vector.query
    assert "$similarity" in vector.query


class RecordedResult:
    def __init__(self, records: list[dict[str, object]]) -> None:
        self.records = records


class RecordingTransaction:
    def __init__(self, index_names: set[str]) -> None:
        self.index_names = index_names
        self.runs: list[tuple[str, dict[str, object]]] = []

    async def run(
        self,
        query: str,
        *,
        parameters: dict[str, object],
    ) -> RecordedResult:
        self.runs.append((query, parameters))
        if query.startswith("SHOW INDEXES"):
            return RecordedResult(
                [{"name": name, "state": "ONLINE"} for name in sorted(self.index_names)]
            )
        return RecordedResult([])


class RecordingStore:
    def __init__(self, *, schema_timeout: float = 30.0) -> None:
        self.schema_timeout = schema_timeout
        self.transaction = RecordingTransaction({statement.name for statement in SCHEMA_STATEMENTS})
        self.write_calls = 0
        self.read_calls = 0

    async def execute_write(self, callback: Callable[[Neo4jTransaction], Awaitable[Any]]) -> Any:
        self.write_calls += 1
        return await callback(self.transaction)  # type: ignore[arg-type]

    async def execute_read(self, callback: Callable[[Neo4jTransaction], Awaitable[Any]]) -> Any:
        self.read_calls += 1
        return await callback(self.transaction)  # type: ignore[arg-type]


def test_ensure_schema_executes_every_statement_and_waits_for_named_indexes() -> None:
    store = RecordingStore()

    asyncio.run(ensure_schema(store))  # type: ignore[arg-type]

    assert store.write_calls == len(SCHEMA_STATEMENTS)
    assert store.read_calls == 1
    assert store.transaction.runs[: len(SCHEMA_STATEMENTS)] == [
        (statement.query, dict(statement.parameters)) for statement in SCHEMA_STATEMENTS
    ]
    wait_query, wait_parameters = store.transaction.runs[-1]
    assert wait_query.startswith("SHOW INDEXES")
    assert wait_parameters == {"names": sorted(statement.name for statement in SCHEMA_STATEMENTS)}


def test_ensure_schema_times_out_when_an_index_never_becomes_online(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RecordingStore(schema_timeout=0.01)
    store.transaction.index_names.remove("vx_content_embedding_embedding")
    clock = iter([0.0, 0.02])
    monkeypatch.setattr("agent_context_platform.projection.schema.monotonic", lambda: next(clock))

    with pytest.raises(TimeoutError, match="Neo4j indexes did not become ONLINE"):
        asyncio.run(ensure_schema(store))  # type: ignore[arg-type]
