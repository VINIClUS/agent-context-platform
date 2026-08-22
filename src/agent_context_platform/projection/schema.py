from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from time import monotonic
from typing import LiteralString

from neo4j import Record

from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction


@dataclass(frozen=True, slots=True)
class SchemaStatement:
    """A named, statically authored Cypher schema statement."""

    name: str
    query: LiteralString
    label: str
    property_name: str
    parameters: Mapping[str, object] = field(default_factory=dict)


class Neo4jSchemaMismatchError(RuntimeError):
    """The named database schema does not match the projection manifest."""


CONSTRAINT_STATEMENTS: tuple[SchemaStatement, ...] = (
    SchemaStatement(
        "uq_workspace_workspace_id",
        "CREATE CONSTRAINT uq_workspace_workspace_id IF NOT EXISTS FOR (n:Workspace) REQUIRE n.workspace_id IS UNIQUE",
        "Workspace",
        "workspace_id",
    ),
    SchemaStatement(
        "uq_project_project_id",
        "CREATE CONSTRAINT uq_project_project_id IF NOT EXISTS FOR (n:Project) REQUIRE n.project_id IS UNIQUE",
        "Project",
        "project_id",
    ),
    SchemaStatement(
        "uq_repository_repository_id",
        "CREATE CONSTRAINT uq_repository_repository_id IF NOT EXISTS FOR (n:Repository) REQUIRE n.repository_id IS UNIQUE",
        "Repository",
        "repository_id",
    ),
    SchemaStatement(
        "uq_branch_branch_id",
        "CREATE CONSTRAINT uq_branch_branch_id IF NOT EXISTS FOR (n:Branch) REQUIRE n.branch_id IS UNIQUE",
        "Branch",
        "branch_id",
    ),
    SchemaStatement(
        "uq_commit_commit_id",
        "CREATE CONSTRAINT uq_commit_commit_id IF NOT EXISTS FOR (n:Commit) REQUIRE n.commit_id IS UNIQUE",
        "Commit",
        "commit_id",
    ),
    SchemaStatement(
        "uq_checkout_checkout_id",
        "CREATE CONSTRAINT uq_checkout_checkout_id IF NOT EXISTS FOR (n:Checkout) REQUIRE n.checkout_id IS UNIQUE",
        "Checkout",
        "checkout_id",
    ),
    SchemaStatement(
        "uq_worktree_worktree_id",
        "CREATE CONSTRAINT uq_worktree_worktree_id IF NOT EXISTS FOR (n:Worktree) REQUIRE n.worktree_id IS UNIQUE",
        "Worktree",
        "worktree_id",
    ),
    SchemaStatement(
        "uq_workspace_snapshot_snapshot_id",
        "CREATE CONSTRAINT uq_workspace_snapshot_snapshot_id IF NOT EXISTS FOR (n:WorkspaceSnapshot) REQUIRE n.snapshot_id IS UNIQUE",
        "WorkspaceSnapshot",
        "snapshot_id",
    ),
    SchemaStatement(
        "uq_change_set_change_set_id",
        "CREATE CONSTRAINT uq_change_set_change_set_id IF NOT EXISTS FOR (n:ChangeSet) REQUIRE n.change_set_id IS UNIQUE",
        "ChangeSet",
        "change_set_id",
    ),
    SchemaStatement(
        "uq_module_module_id",
        "CREATE CONSTRAINT uq_module_module_id IF NOT EXISTS FOR (n:Module) REQUIRE n.module_id IS UNIQUE",
        "Module",
        "module_id",
    ),
    SchemaStatement(
        "uq_file_file_id",
        "CREATE CONSTRAINT uq_file_file_id IF NOT EXISTS FOR (n:File) REQUIRE n.file_id IS UNIQUE",
        "File",
        "file_id",
    ),
    SchemaStatement(
        "uq_file_revision_file_revision_id",
        "CREATE CONSTRAINT uq_file_revision_file_revision_id IF NOT EXISTS FOR (n:FileRevision) REQUIRE n.file_revision_id IS UNIQUE",
        "FileRevision",
        "file_revision_id",
    ),
    SchemaStatement(
        "uq_symbol_symbol_id",
        "CREATE CONSTRAINT uq_symbol_symbol_id IF NOT EXISTS FOR (n:Symbol) REQUIRE n.symbol_id IS UNIQUE",
        "Symbol",
        "symbol_id",
    ),
    SchemaStatement(
        "uq_symbol_revision_symbol_revision_id",
        "CREATE CONSTRAINT uq_symbol_revision_symbol_revision_id IF NOT EXISTS FOR (n:SymbolRevision) REQUIRE n.symbol_revision_id IS UNIQUE",
        "SymbolRevision",
        "symbol_revision_id",
    ),
    SchemaStatement(
        "uq_dependency_dependency_id",
        "CREATE CONSTRAINT uq_dependency_dependency_id IF NOT EXISTS FOR (n:Dependency) REQUIRE n.dependency_id IS UNIQUE",
        "Dependency",
        "dependency_id",
    ),
    SchemaStatement(
        "uq_agent_runtime_agent_runtime_id",
        "CREATE CONSTRAINT uq_agent_runtime_agent_runtime_id IF NOT EXISTS FOR (n:AgentRuntime) REQUIRE n.agent_runtime_id IS UNIQUE",
        "AgentRuntime",
        "agent_runtime_id",
    ),
    SchemaStatement(
        "uq_session_session_id",
        "CREATE CONSTRAINT uq_session_session_id IF NOT EXISTS FOR (n:Session) REQUIRE n.session_id IS UNIQUE",
        "Session",
        "session_id",
    ),
    SchemaStatement(
        "uq_turn_turn_id",
        "CREATE CONSTRAINT uq_turn_turn_id IF NOT EXISTS FOR (n:Turn) REQUIRE n.turn_id IS UNIQUE",
        "Turn",
        "turn_id",
    ),
    SchemaStatement(
        "uq_tool_call_tool_call_id",
        "CREATE CONSTRAINT uq_tool_call_tool_call_id IF NOT EXISTS FOR (n:ToolCall) REQUIRE n.tool_call_id IS UNIQUE",
        "ToolCall",
        "tool_call_id",
    ),
    SchemaStatement(
        "uq_test_run_test_run_id",
        "CREATE CONSTRAINT uq_test_run_test_run_id IF NOT EXISTS FOR (n:TestRun) REQUIRE n.test_run_id IS UNIQUE",
        "TestRun",
        "test_run_id",
    ),
    SchemaStatement(
        "uq_ci_run_ci_run_id",
        "CREATE CONSTRAINT uq_ci_run_ci_run_id IF NOT EXISTS FOR (n:CIRun) REQUIRE n.ci_run_id IS UNIQUE",
        "CIRun",
        "ci_run_id",
    ),
    SchemaStatement(
        "uq_artifact_artifact_id",
        "CREATE CONSTRAINT uq_artifact_artifact_id IF NOT EXISTS FOR (n:Artifact) REQUIRE n.artifact_id IS UNIQUE",
        "Artifact",
        "artifact_id",
    ),
    SchemaStatement(
        "uq_finding_finding_id",
        "CREATE CONSTRAINT uq_finding_finding_id IF NOT EXISTS FOR (n:Finding) REQUIRE n.finding_id IS UNIQUE",
        "Finding",
        "finding_id",
    ),
    SchemaStatement(
        "uq_decision_decision_id",
        "CREATE CONSTRAINT uq_decision_decision_id IF NOT EXISTS FOR (n:Decision) REQUIRE n.decision_id IS UNIQUE",
        "Decision",
        "decision_id",
    ),
    SchemaStatement(
        "uq_constraint_constraint_id",
        "CREATE CONSTRAINT uq_constraint_constraint_id IF NOT EXISTS FOR (n:Constraint) REQUIRE n.constraint_id IS UNIQUE",
        "Constraint",
        "constraint_id",
    ),
    SchemaStatement(
        "uq_failure_failure_id",
        "CREATE CONSTRAINT uq_failure_failure_id IF NOT EXISTS FOR (n:Failure) REQUIRE n.failure_id IS UNIQUE",
        "Failure",
        "failure_id",
    ),
    SchemaStatement(
        "uq_summary_summary_id",
        "CREATE CONSTRAINT uq_summary_summary_id IF NOT EXISTS FOR (n:Summary) REQUIRE n.summary_id IS UNIQUE",
        "Summary",
        "summary_id",
    ),
    SchemaStatement(
        "uq_assertion_assertion_id",
        "CREATE CONSTRAINT uq_assertion_assertion_id IF NOT EXISTS FOR (n:Assertion) REQUIRE n.assertion_id IS UNIQUE",
        "Assertion",
        "assertion_id",
    ),
    SchemaStatement(
        "uq_content_embedding_content_id",
        "CREATE CONSTRAINT uq_content_embedding_content_id IF NOT EXISTS FOR (n:ContentEmbedding) REQUIRE n.content_id IS UNIQUE",
        "ContentEmbedding",
        "content_id",
    ),
)

INDEX_STATEMENTS: tuple[SchemaStatement, ...] = (
    SchemaStatement(
        "ix_assertion_source_event_id",
        "CREATE RANGE INDEX ix_assertion_source_event_id IF NOT EXISTS FOR (n:Assertion) ON (n.source_event_id)",
        "Assertion",
        "source_event_id",
    ),
    SchemaStatement(
        "ix_assertion_valid_from",
        "CREATE RANGE INDEX ix_assertion_valid_from IF NOT EXISTS FOR (n:Assertion) ON (n.valid_from)",
        "Assertion",
        "valid_from",
    ),
    SchemaStatement(
        "ix_assertion_valid_to",
        "CREATE RANGE INDEX ix_assertion_valid_to IF NOT EXISTS FOR (n:Assertion) ON (n.valid_to)",
        "Assertion",
        "valid_to",
    ),
    SchemaStatement(
        "ix_assertion_recorded_from",
        "CREATE RANGE INDEX ix_assertion_recorded_from IF NOT EXISTS FOR (n:Assertion) ON (n.recorded_from)",
        "Assertion",
        "recorded_from",
    ),
    SchemaStatement(
        "ix_assertion_recorded_to",
        "CREATE RANGE INDEX ix_assertion_recorded_to IF NOT EXISTS FOR (n:Assertion) ON (n.recorded_to)",
        "Assertion",
        "recorded_to",
    ),
    SchemaStatement(
        "vx_content_embedding_embedding",
        "CREATE VECTOR INDEX vx_content_embedding_embedding IF NOT EXISTS FOR (n:ContentEmbedding) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: $dimensions, `vector.similarity_function`: $similarity}}",
        "ContentEmbedding",
        "embedding",
        {"dimensions": 384, "similarity": "cosine"},
    ),
)

SCHEMA_STATEMENTS = CONSTRAINT_STATEMENTS + INDEX_STATEMENTS
_SCHEMA_INDEX_NAMES = frozenset(statement.name for statement in SCHEMA_STATEMENTS)
_SHOW_SCHEMA_INDEXES: LiteralString = (
    "SHOW INDEXES YIELD name, state, type, entityType, labelsOrTypes, properties, options "
    "WHERE name IN $names "
    "RETURN name, state, type, entityType, labelsOrTypes, properties, options"
)
_SHOW_SCHEMA_CONSTRAINTS: LiteralString = (
    "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties "
    "WHERE name IN $names "
    "RETURN name, type, entityType, labelsOrTypes, properties"
)


async def ensure_schema(store: Neo4jStore) -> None:
    """Create the Community schema idempotently and wait for every index."""

    for statement in SCHEMA_STATEMENTS:
        await _execute_statement(store, statement)
    await _wait_for_indexes(store)


async def _execute_statement(store: Neo4jStore, statement: SchemaStatement) -> None:
    async def execute(transaction: Neo4jTransaction) -> None:
        await transaction.run(statement.query, parameters=dict(statement.parameters))

    await store.execute_write(execute)


async def _wait_for_indexes(store: Neo4jStore) -> None:
    deadline = monotonic() + store.schema_timeout
    names = sorted(_SCHEMA_INDEX_NAMES)

    async def read_indexes(transaction: Neo4jTransaction) -> list[Record]:
        result = await transaction.run(_SHOW_SCHEMA_INDEXES, parameters={"names": names})
        return result.records

    while True:
        records = await store.execute_read(read_indexes)
        states = {str(record["name"]): str(record["state"]) for record in records}
        if states.keys() == _SCHEMA_INDEX_NAMES and all(
            state == "ONLINE" for state in states.values()
        ):
            _validate_indexes(records)
            await _validate_constraints(store)
            return
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("Neo4j indexes did not become ONLINE within schema timeout")
        await asyncio.sleep(min(0.1, remaining))


def _validate_indexes(records: list[Record]) -> None:
    records_by_name = {str(record["name"]): record for record in records}
    for statement in SCHEMA_STATEMENTS:
        record = records_by_name[statement.name]
        expected_type = "VECTOR" if statement.name == "vx_content_embedding_embedding" else "RANGE"
        _validate_schema_object(record, statement, expected_type=expected_type)
        if expected_type == "VECTOR":
            _validate_vector_config(record, statement)


async def _validate_constraints(store: Neo4jStore) -> None:
    names = sorted(statement.name for statement in CONSTRAINT_STATEMENTS)

    async def read_constraints(transaction: Neo4jTransaction) -> list[Record]:
        result = await transaction.run(_SHOW_SCHEMA_CONSTRAINTS, parameters={"names": names})
        return result.records

    records = await store.execute_read(read_constraints)
    records_by_name = {str(record["name"]): record for record in records}
    missing = set(names) - records_by_name.keys()
    if missing:
        raise Neo4jSchemaMismatchError(
            f"Neo4j constraints missing from schema: {', '.join(sorted(missing))}"
        )
    for statement in CONSTRAINT_STATEMENTS:
        _validate_schema_object(
            records_by_name[statement.name],
            statement,
            expected_type="UNIQUENESS",
        )


def _validate_schema_object(
    record: Record,
    statement: SchemaStatement,
    *,
    expected_type: str,
) -> None:
    actual = (
        str(record["type"]),
        str(record["entityType"]),
        tuple(str(value) for value in record["labelsOrTypes"]),
        tuple(str(value) for value in record["properties"]),
    )
    expected = (expected_type, "NODE", (statement.label,), (statement.property_name,))
    if actual != expected:
        raise Neo4jSchemaMismatchError(
            f"Neo4j schema object {statement.name} does not match the projection manifest"
        )


def _validate_vector_config(record: Record, statement: SchemaStatement) -> None:
    options = record["options"]
    config = options.get("indexConfig") if isinstance(options, Mapping) else None
    dimensions = config.get("vector.dimensions") if isinstance(config, Mapping) else None
    similarity = (
        str(config.get("vector.similarity_function")).lower()
        if isinstance(config, Mapping)
        else None
    )
    if (
        dimensions != statement.parameters["dimensions"]
        or similarity != statement.parameters["similarity"]
    ):
        raise Neo4jSchemaMismatchError(
            f"Neo4j schema object {statement.name} does not match the projection manifest"
        )
