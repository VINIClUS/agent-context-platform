from __future__ import annotations

import asyncio
import os

import pytest

from agent_context_platform.projection import Neo4jStore, Neo4jTransaction, ensure_schema
from agent_context_platform.settings import Neo4jSettings

pytestmark = pytest.mark.integration

SchemaObject = tuple[str, str, str, tuple[str, ...], tuple[str, ...]]

EXPECTED_CONSTRAINTS: set[SchemaObject] = {
    ("uq_workspace_workspace_id", "UNIQUENESS", "NODE", ("Workspace",), ("workspace_id",)),
    ("uq_project_project_id", "UNIQUENESS", "NODE", ("Project",), ("project_id",)),
    ("uq_repository_repository_id", "UNIQUENESS", "NODE", ("Repository",), ("repository_id",)),
    ("uq_branch_branch_id", "UNIQUENESS", "NODE", ("Branch",), ("branch_id",)),
    ("uq_commit_commit_id", "UNIQUENESS", "NODE", ("Commit",), ("commit_id",)),
    ("uq_checkout_checkout_id", "UNIQUENESS", "NODE", ("Checkout",), ("checkout_id",)),
    ("uq_worktree_worktree_id", "UNIQUENESS", "NODE", ("Worktree",), ("worktree_id",)),
    (
        "uq_workspace_snapshot_snapshot_id",
        "UNIQUENESS",
        "NODE",
        ("WorkspaceSnapshot",),
        ("snapshot_id",),
    ),
    ("uq_change_set_change_set_id", "UNIQUENESS", "NODE", ("ChangeSet",), ("change_set_id",)),
    ("uq_module_module_id", "UNIQUENESS", "NODE", ("Module",), ("module_id",)),
    ("uq_file_file_id", "UNIQUENESS", "NODE", ("File",), ("file_id",)),
    (
        "uq_file_revision_file_revision_id",
        "UNIQUENESS",
        "NODE",
        ("FileRevision",),
        ("file_revision_id",),
    ),
    ("uq_symbol_symbol_id", "UNIQUENESS", "NODE", ("Symbol",), ("symbol_id",)),
    (
        "uq_symbol_revision_symbol_revision_id",
        "UNIQUENESS",
        "NODE",
        ("SymbolRevision",),
        ("symbol_revision_id",),
    ),
    ("uq_dependency_dependency_id", "UNIQUENESS", "NODE", ("Dependency",), ("dependency_id",)),
    (
        "uq_agent_runtime_agent_runtime_id",
        "UNIQUENESS",
        "NODE",
        ("AgentRuntime",),
        ("agent_runtime_id",),
    ),
    ("uq_session_session_id", "UNIQUENESS", "NODE", ("Session",), ("session_id",)),
    ("uq_turn_turn_id", "UNIQUENESS", "NODE", ("Turn",), ("turn_id",)),
    ("uq_tool_call_tool_call_id", "UNIQUENESS", "NODE", ("ToolCall",), ("tool_call_id",)),
    ("uq_test_run_test_run_id", "UNIQUENESS", "NODE", ("TestRun",), ("test_run_id",)),
    ("uq_ci_run_ci_run_id", "UNIQUENESS", "NODE", ("CIRun",), ("ci_run_id",)),
    ("uq_artifact_artifact_id", "UNIQUENESS", "NODE", ("Artifact",), ("artifact_id",)),
    ("uq_finding_finding_id", "UNIQUENESS", "NODE", ("Finding",), ("finding_id",)),
    ("uq_decision_decision_id", "UNIQUENESS", "NODE", ("Decision",), ("decision_id",)),
    ("uq_constraint_constraint_id", "UNIQUENESS", "NODE", ("Constraint",), ("constraint_id",)),
    ("uq_failure_failure_id", "UNIQUENESS", "NODE", ("Failure",), ("failure_id",)),
    ("uq_summary_summary_id", "UNIQUENESS", "NODE", ("Summary",), ("summary_id",)),
    ("uq_assertion_assertion_id", "UNIQUENESS", "NODE", ("Assertion",), ("assertion_id",)),
    (
        "uq_content_embedding_content_id",
        "UNIQUENESS",
        "NODE",
        ("ContentEmbedding",),
        ("content_id",),
    ),
}

EXPECTED_INDEXES: set[SchemaObject] = {
    ("uq_workspace_workspace_id", "RANGE", "NODE", ("Workspace",), ("workspace_id",)),
    ("uq_project_project_id", "RANGE", "NODE", ("Project",), ("project_id",)),
    ("uq_repository_repository_id", "RANGE", "NODE", ("Repository",), ("repository_id",)),
    ("uq_branch_branch_id", "RANGE", "NODE", ("Branch",), ("branch_id",)),
    ("uq_commit_commit_id", "RANGE", "NODE", ("Commit",), ("commit_id",)),
    ("uq_checkout_checkout_id", "RANGE", "NODE", ("Checkout",), ("checkout_id",)),
    ("uq_worktree_worktree_id", "RANGE", "NODE", ("Worktree",), ("worktree_id",)),
    (
        "uq_workspace_snapshot_snapshot_id",
        "RANGE",
        "NODE",
        ("WorkspaceSnapshot",),
        ("snapshot_id",),
    ),
    ("uq_change_set_change_set_id", "RANGE", "NODE", ("ChangeSet",), ("change_set_id",)),
    ("uq_module_module_id", "RANGE", "NODE", ("Module",), ("module_id",)),
    ("uq_file_file_id", "RANGE", "NODE", ("File",), ("file_id",)),
    (
        "uq_file_revision_file_revision_id",
        "RANGE",
        "NODE",
        ("FileRevision",),
        ("file_revision_id",),
    ),
    ("uq_symbol_symbol_id", "RANGE", "NODE", ("Symbol",), ("symbol_id",)),
    (
        "uq_symbol_revision_symbol_revision_id",
        "RANGE",
        "NODE",
        ("SymbolRevision",),
        ("symbol_revision_id",),
    ),
    ("uq_dependency_dependency_id", "RANGE", "NODE", ("Dependency",), ("dependency_id",)),
    (
        "uq_agent_runtime_agent_runtime_id",
        "RANGE",
        "NODE",
        ("AgentRuntime",),
        ("agent_runtime_id",),
    ),
    ("uq_session_session_id", "RANGE", "NODE", ("Session",), ("session_id",)),
    ("uq_turn_turn_id", "RANGE", "NODE", ("Turn",), ("turn_id",)),
    ("uq_tool_call_tool_call_id", "RANGE", "NODE", ("ToolCall",), ("tool_call_id",)),
    ("uq_test_run_test_run_id", "RANGE", "NODE", ("TestRun",), ("test_run_id",)),
    ("uq_ci_run_ci_run_id", "RANGE", "NODE", ("CIRun",), ("ci_run_id",)),
    ("uq_artifact_artifact_id", "RANGE", "NODE", ("Artifact",), ("artifact_id",)),
    ("uq_finding_finding_id", "RANGE", "NODE", ("Finding",), ("finding_id",)),
    ("uq_decision_decision_id", "RANGE", "NODE", ("Decision",), ("decision_id",)),
    ("uq_constraint_constraint_id", "RANGE", "NODE", ("Constraint",), ("constraint_id",)),
    ("uq_failure_failure_id", "RANGE", "NODE", ("Failure",), ("failure_id",)),
    ("uq_summary_summary_id", "RANGE", "NODE", ("Summary",), ("summary_id",)),
    ("uq_assertion_assertion_id", "RANGE", "NODE", ("Assertion",), ("assertion_id",)),
    ("uq_content_embedding_content_id", "RANGE", "NODE", ("ContentEmbedding",), ("content_id",)),
    ("ix_assertion_source_event_id", "RANGE", "NODE", ("Assertion",), ("source_event_id",)),
    ("ix_assertion_valid_from", "RANGE", "NODE", ("Assertion",), ("valid_from",)),
    ("ix_assertion_valid_to", "RANGE", "NODE", ("Assertion",), ("valid_to",)),
    ("ix_assertion_recorded_from", "RANGE", "NODE", ("Assertion",), ("recorded_from",)),
    ("ix_assertion_recorded_to", "RANGE", "NODE", ("Assertion",), ("recorded_to",)),
    ("vx_content_embedding_embedding", "VECTOR", "NODE", ("ContentEmbedding",), ("embedding",)),
}


def integration_settings() -> Neo4jSettings:
    variable_names = (
        "AGENT_CONTEXT_TEST_NEO4J_URI",
        "AGENT_CONTEXT_TEST_NEO4J_USERNAME",
        "AGENT_CONTEXT_TEST_NEO4J_PASSWORD",
    )
    values = {name: os.environ.get(name) for name in variable_names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.skip(f"{', '.join(missing)} required for Neo4j integration tests")

    return Neo4jSettings.model_validate(
        {
            "uri": values["AGENT_CONTEXT_TEST_NEO4J_URI"],
            "username": values["AGENT_CONTEXT_TEST_NEO4J_USERNAME"],
            "password": values["AGENT_CONTEXT_TEST_NEO4J_PASSWORD"],
            "database": os.environ.get("AGENT_CONTEXT_TEST_NEO4J_DATABASE", "neo4j"),
        }
    )


def test_schema_is_idempotent_and_matches_the_complete_manifest() -> None:
    async def exercise() -> None:
        async with Neo4jStore(integration_settings()) as store:
            health = await store.verify_connectivity()
            assert health.status == "ok", health.reason

            async def read_constraints(transaction: Neo4jTransaction) -> set[SchemaObject]:
                result = await transaction.run(
                    "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties RETURN name, type, entityType, labelsOrTypes, properties",
                    parameters={},
                )
                return {
                    (
                        str(record["name"]),
                        str(record["type"]),
                        str(record["entityType"]),
                        tuple(str(value) for value in record["labelsOrTypes"]),
                        tuple(str(value) for value in record["properties"]),
                    )
                    for record in result.records
                }

            async def read_indexes(transaction: Neo4jTransaction) -> set[SchemaObject]:
                result = await transaction.run(
                    "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties WHERE type <> 'LOOKUP' RETURN name, type, entityType, labelsOrTypes, properties",
                    parameters={},
                )
                return {
                    (
                        str(record["name"]),
                        str(record["type"]),
                        str(record["entityType"]),
                        tuple(str(value) for value in record["labelsOrTypes"]),
                        tuple(str(value) for value in record["properties"]),
                    )
                    for record in result.records
                }

            async def read_vector_config(transaction: Neo4jTransaction) -> dict[str, object]:
                result = await transaction.run(
                    "SHOW INDEXES YIELD name, options WHERE name = $name RETURN options",
                    parameters={"name": "vx_content_embedding_embedding"},
                )
                return dict(result.records[0]["options"]["indexConfig"])

            if await store.execute_read(read_constraints) or await store.execute_read(read_indexes):
                raise RuntimeError(
                    "Neo4j integration variables must reference a clean disposable database"
                )

            await ensure_schema(store)
            await ensure_schema(store)

            assert await store.execute_read(read_constraints) == EXPECTED_CONSTRAINTS
            assert await store.execute_read(read_indexes) == EXPECTED_INDEXES
            vector_config = await store.execute_read(read_vector_config)
            assert vector_config["vector.dimensions"] == 384
            assert str(vector_config["vector.similarity_function"]).lower() == "cosine"

    asyncio.run(exercise())
