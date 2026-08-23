from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from alembic import command

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).parents[2]
MANAGED_SCHEMAS = {"catalog", "ledger", "projection", "operations"}
MANAGED_ROLES = {"agent_context_api", "agent_context_projector"}
EXPECTED_TABLES = {
    "catalog": {
        "workspaces",
        "projects",
        "repositories",
        "project_repositories",
        "checkouts",
        "content_objects",
    },
    "ledger": {
        "event_streams",
        "events",
        "event_content_refs",
        "redaction_reports",
        "integrity_checkpoints",
    },
    "projection": {
        "outbox",
        "projection_checkpoints",
        "ingestion_batches",
        "dead_letters",
    },
    "operations": {
        "registered_producers",
        "schema_versions",
        "retention_policies",
    },
}
EXPECTED_COLUMNS = {
    "catalog.workspaces": {"name", "id", "created_at", "observed_at"},
    "catalog.projects": {"workspace_id", "name", "id", "created_at", "observed_at"},
    "catalog.repositories": {
        "remote_host",
        "remote_owner",
        "remote_path",
        "original_remote_url",
        "id",
        "created_at",
        "observed_at",
    },
    "catalog.project_repositories": {
        "project_id",
        "repository_id",
        "id",
        "created_at",
        "observed_at",
    },
    "catalog.checkouts": {
        "repository_id",
        "filesystem_root",
        "id",
        "created_at",
        "observed_at",
    },
    "catalog.content_objects": {
        "content_sha256",
        "object_key",
        "media_type",
        "compressed_bytes",
        "uncompressed_bytes",
        "id",
        "created_at",
        "observed_at",
    },
    "ledger.event_streams": {
        "stream_id",
        "last_sequence",
        "last_event_id",
        "last_event_sha256",
        "status",
        "quarantined_at",
        "created_at",
        "updated_at",
    },
    "ledger.events": {
        "event_id",
        "event_type",
        "schema_version",
        "stream_id",
        "stream_sequence",
        "producer_id",
        "idempotency_key",
        "occurred_at",
        "observed_at",
        "recorded_at",
        "producer",
        "context",
        "trace",
        "payload",
        "redaction",
        "payload_sha256",
        "previous_event_sha256",
        "event_sha256",
    },
    "ledger.event_content_refs": {
        "event_id",
        "content_id",
        "content_sha256",
        "media_type",
        "uncompressed_bytes",
        "disposition",
        "storage",
        "inline_id",
        "object_key",
        "encoding",
    },
    "ledger.redaction_reports": {
        "event_id",
        "content_id",
        "policy_version",
        "disposition",
        "findings",
        "metadata_only_reason",
        "error_class",
        "failed_detector",
        "failed_detector_version",
    },
    "ledger.integrity_checkpoints": {
        "checkpoint_id",
        "stream_heads",
        "heads_sha256",
        "signature_algorithm",
        "signing_key_id",
        "signature",
        "stream_count",
        "created_at",
    },
    "projection.outbox": {
        "outbox_id",
        "event_id",
        "status",
        "retry_count",
        "available_at",
        "lease_owner",
        "lease_expires_at",
        "last_error_class",
        "delivered_at",
        "dead_lettered_at",
        "created_at",
        "updated_at",
    },
    "projection.projection_checkpoints": {
        "projector_name",
        "projector_version",
        "last_outbox_id",
        "last_event_id",
        "processed_count",
        "state",
        "updated_at",
    },
    "projection.ingestion_batches": {
        "batch_id",
        "producer_id",
        "request_sha256",
        "event_count",
        "accepted_count",
        "rejected_count",
        "status",
        "error_code",
        "started_at",
        "finished_at",
    },
    "projection.dead_letters": {
        "outbox_id",
        "event_id",
        "projector_name",
        "projector_version",
        "attempts",
        "error_class",
        "status",
        "first_failed_at",
        "updated_at",
        "resolved_at",
    },
    "operations.registered_producers": {
        "producer_id",
        "token_prefix",
        "token_verifier",
        "scope",
        "expires_at",
        "revoked_at",
        "created_at",
        "updated_at",
        "last_used_at",
    },
    "operations.schema_versions": {
        "schema_name",
        "schema_version",
        "schema_document",
        "schema_sha256",
        "upcaster",
        "registered_at",
    },
    "operations.retention_policies": {
        "retention_policy_id",
        "resource_class",
        "retention_days",
        "preserve_metadata",
        "effective_from",
        "effective_to",
        "created_at",
    },
}


def test_alembic_configuration_exposes_one_initial_head() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "heads", "--verbose"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.count("(head)") == 1
    assert "initial ledger" in result.stdout.lower()


def test_initial_migration_round_trip_and_privileges() -> None:
    dsn = os.environ.get("AGENT_CONTEXT_TEST_POSTGRES_DSN")
    if dsn is None:
        pytest.skip("AGENT_CONTEXT_TEST_POSTGRES_DSN is required for PostgreSQL tests")

    asyncio.run(_exercise_migration(dsn))


async def _exercise_migration(dsn: str) -> None:
    engine = create_async_engine(dsn, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await _assert_prerequisites(connection)
            await _run_alembic(connection, command.upgrade, "head")
            await connection.commit()
            await _assert_head(connection)

            await _run_alembic(connection, command.downgrade, "base")
            await connection.commit()
            await _assert_base(connection)

            await _run_alembic(connection, command.upgrade, "head")
            await connection.commit()
            await _assert_head(connection)
            await _run_alembic_check(connection)
            _assert_direct_async_connection_is_rejected(connection)
    finally:
        await engine.dispose()


async def _run_alembic(
    connection: AsyncConnection,
    operation: Callable[[Config, str], None],
    revision: str,
) -> None:
    def run(sync_connection: Connection) -> None:
        config = Config(PROJECT_ROOT / "alembic.ini")
        config.attributes["connection"] = sync_connection
        operation(config, revision)

    await connection.run_sync(run)


async def _run_alembic_check(connection: AsyncConnection) -> None:
    def run(sync_connection: Connection) -> None:
        config = Config(PROJECT_ROOT / "alembic.ini")
        config.attributes["connection"] = sync_connection
        command.check(config)

    await connection.run_sync(run)


def _assert_direct_async_connection_is_rejected(connection: AsyncConnection) -> None:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.attributes["connection"] = connection

    with pytest.raises(TypeError, match="synchronous SQLAlchemy Connection"):
        command.current(config)


async def _assert_prerequisites(connection: AsyncConnection) -> None:
    version = await connection.scalar(text("SHOW server_version_num"))
    assert int(version or 0) >= 180000
    assert await connection.scalar(text("SELECT to_regprocedure('uuidv7()') IS NOT NULL"))
    assert await _names(connection, "SELECT nspname FROM pg_namespace") >= {"public"}
    assert not MANAGED_SCHEMAS & await _names(
        connection,
        "SELECT nspname FROM pg_namespace WHERE nspname = ANY(:names)",
        {"names": sorted(MANAGED_SCHEMAS)},
    )
    assert not MANAGED_ROLES & await _names(
        connection,
        "SELECT rolname FROM pg_roles WHERE rolname = ANY(:names)",
        {"names": sorted(MANAGED_ROLES)},
    )
    version_table = await connection.scalar(text("SELECT to_regclass('public.alembic_version')"))
    if version_table is not None:
        assert await connection.scalar(text("SELECT count(*) FROM public.alembic_version")) == 0


async def _assert_head(connection: AsyncConnection) -> None:
    assert (
        await _names(
            connection,
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY(:names)",
            {"names": sorted(MANAGED_SCHEMAS)},
        )
        == MANAGED_SCHEMAS
    )

    table_rows = await connection.execute(
        text(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema = ANY(:schemas) AND table_type = 'BASE TABLE'"
        ),
        {"schemas": sorted(MANAGED_SCHEMAS)},
    )
    actual_tables: dict[str, set[str]] = {schema: set() for schema in MANAGED_SCHEMAS}
    for schema, table in table_rows:
        actual_tables[schema].add(table)
    assert actual_tables == EXPECTED_TABLES

    column_rows = await connection.execute(
        text(
            "SELECT table_schema || '.' || table_name, column_name "
            "FROM information_schema.columns WHERE table_schema = ANY(:schemas)"
        ),
        {"schemas": sorted(MANAGED_SCHEMAS)},
    )
    actual_columns: dict[str, set[str]] = {table: set() for table in EXPECTED_COLUMNS}
    for table, column in column_rows:
        actual_columns[table].add(column)
    assert actual_columns == EXPECTED_COLUMNS

    await _assert_defaults_identity_and_index(connection)
    await _assert_constraint_contract(connection)
    await _assert_role_contract(connection)


async def _assert_defaults_identity_and_index(connection: AsyncConnection) -> None:
    catalog_defaults = await connection.execute(
        text(
            "SELECT table_name, column_name, column_default "
            "FROM information_schema.columns WHERE table_schema = 'catalog' "
            "AND column_name IN ('id', 'created_at', 'observed_at')"
        )
    )
    for _table, column, default in catalog_defaults:
        assert default == ("uuidv7()" if column == "id" else "now()")

    identity = (
        await connection.execute(
            text(
                "SELECT is_identity, identity_generation FROM information_schema.columns "
                "WHERE table_schema = 'projection' AND table_name = 'outbox' "
                "AND column_name = 'outbox_id'"
            )
        )
    ).one()
    assert identity == ("YES", "BY DEFAULT")
    assert (
        await connection.scalar(
            text("SELECT pg_get_serial_sequence('projection.outbox', 'outbox_id')")
        )
        == "projection.outbox_outbox_id_seq"
    )

    indexes = await _names(
        connection,
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'projection' AND tablename = 'outbox'",
    )
    assert indexes == {"pk_outbox", "uq_outbox_event_id", "ix_outbox_status"}
    index_definition = await connection.scalar(
        text(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'projection' "
            "AND indexname = 'ix_outbox_status'"
        )
    )
    assert str(index_definition).endswith("(status, available_at, outbox_id)")


async def _assert_constraint_contract(connection: AsyncConnection) -> None:
    expected_foreign_keys = {
        "fk_projects_workspace_id_workspaces": "r",
        "fk_project_repositories_project_id_projects": "r",
        "fk_project_repositories_repository_id_repositories": "r",
        "fk_checkouts_repository_id_repositories": "r",
        "fk_event_streams_last_event_id_events": "a",
        "fk_events_stream_id_event_streams": "a",
        "fk_event_content_refs_event_id_events": "a",
        "fk_redaction_reports_event_id_event_content_refs": "a",
        "fk_outbox_event_id_events": "a",
        "fk_projection_checkpoints_last_outbox_id_outbox": "a",
        "fk_projection_checkpoints_last_event_id_events": "a",
        "fk_dead_letters_outbox_id_outbox": "a",
        "fk_dead_letters_event_id_events": "a",
    }
    foreign_keys = await connection.execute(
        text(
            "SELECT conname, confdeltype FROM pg_constraint c "
            "JOIN pg_namespace n ON n.oid = c.connamespace "
            "WHERE n.nspname = ANY(:schemas) AND c.contype = 'f'"
        ),
        {"schemas": sorted(MANAGED_SCHEMAS)},
    )
    assert dict(tuple(row) for row in foreign_keys) == expected_foreign_keys

    constraint_names = await _names(
        connection,
        "SELECT conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
        "WHERE n.nspname = ANY(:schemas) AND c.contype IN ('p', 'u', 'c', 'f')",
        {"schemas": sorted(MANAGED_SCHEMAS)},
    )
    assert constraint_names == EXPECTED_CONSTRAINT_NAMES

    enum_columns = await connection.execute(
        text(
            "SELECT table_schema || '.' || table_name, column_name, data_type "
            "FROM information_schema.columns WHERE table_schema = ANY(:schemas) "
            "AND column_name = ANY(:columns)"
        ),
        {
            "schemas": ["ledger", "projection"],
            "columns": ["status", "disposition", "storage", "metadata_only_reason", "state"],
        },
    )
    assert {data_type for _table, _column, data_type in enum_columns} == {"character varying"}

    enum_checks = await connection.execute(
        text(
            "SELECT conname, pg_get_constraintdef(c.oid) FROM pg_constraint c "
            "JOIN pg_namespace n ON n.oid = c.connamespace "
            "WHERE n.nspname = ANY(:schemas) AND conname = ANY(:names)"
        ),
        {
            "schemas": ["ledger", "projection"],
            "names": sorted(EXPECTED_ENUM_VALUES),
        },
    )
    actual_enum_values = {
        name: tuple(re.findall(r"'([^']+)'::character varying", definition))
        for name, definition in enum_checks
    }
    assert actual_enum_values == EXPECTED_ENUM_VALUES


async def _assert_role_contract(connection: AsyncConnection) -> None:
    role_rows = await connection.execute(
        text(
            "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, "
            "rolreplication, rolbypassrls FROM pg_roles WHERE rolname = ANY(:roles)"
        ),
        {"roles": sorted(MANAGED_ROLES)},
    )
    assert {tuple(row) for row in role_rows} == {
        (role, False, False, False, False, False, False) for role in MANAGED_ROLES
    }

    for schema in MANAGED_SCHEMAS:
        assert await connection.scalar(
            text("SELECT has_schema_privilege('agent_context_api', :schema, 'USAGE')"),
            {"schema": schema},
        )
        assert not await connection.scalar(
            text("SELECT has_schema_privilege('agent_context_api', :schema, 'CREATE')"),
            {"schema": schema},
        )
    for schema in {"ledger", "projection"}:
        assert await connection.scalar(
            text("SELECT has_schema_privilege('agent_context_projector', :schema, 'USAGE')"),
            {"schema": schema},
        )
    for schema in {"catalog", "operations"}:
        assert not await connection.scalar(
            text("SELECT has_schema_privilege('agent_context_projector', :schema, 'USAGE')"),
            {"schema": schema},
        )

    table_grants = await connection.execute(
        text(
            "SELECT grantee, table_schema || '.' || table_name, privilege_type "
            "FROM information_schema.role_table_grants WHERE grantee = ANY(:roles)"
        ),
        {"roles": sorted(MANAGED_ROLES)},
    )
    assert {tuple(row) for row in table_grants} == EXPECTED_TABLE_GRANTS

    update_grants = await connection.execute(
        text(
            "SELECT grantee, table_schema || '.' || table_name, column_name "
            "FROM information_schema.column_privileges WHERE grantee = ANY(:roles) "
            "AND privilege_type = 'UPDATE'"
        ),
        {"roles": sorted(MANAGED_ROLES)},
    )
    assert {tuple(row) for row in update_grants} == EXPECTED_UPDATE_GRANTS

    for privilege in ("UPDATE", "DELETE", "TRUNCATE"):
        assert not await connection.scalar(
            text("SELECT has_table_privilege('agent_context_api', 'ledger.events', :privilege)"),
            {"privilege": privilege},
        )

    sequence = "projection.outbox_outbox_id_seq"
    assert await connection.scalar(
        text("SELECT has_sequence_privilege('agent_context_api', :sequence, 'USAGE')"),
        {"sequence": sequence},
    )
    for role, privilege in (
        ("agent_context_api", "SELECT"),
        ("agent_context_api", "UPDATE"),
        ("agent_context_projector", "USAGE"),
    ):
        assert not await connection.scalar(
            text("SELECT has_sequence_privilege(:role, :sequence, :privilege)"),
            {"role": role, "sequence": sequence, "privilege": privilege},
        )

    owners = await _names(
        connection,
        "SELECT owner_name FROM ("
        "SELECT pg_get_userbyid(nspowner) AS owner_name FROM pg_namespace "
        "WHERE nspname = ANY(:schemas) UNION ALL "
        "SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n "
        "ON n.oid = c.relnamespace WHERE n.nspname = ANY(:schemas)) owned",
        {"schemas": sorted(MANAGED_SCHEMAS)},
    )
    assert not owners & MANAGED_ROLES
    assert not await connection.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM pg_default_acl d, aclexplode(d.defaclacl) a "
            "JOIN pg_roles r ON r.oid = a.grantee WHERE r.rolname = ANY(:roles))"
        ),
        {"roles": sorted(MANAGED_ROLES)},
    )


async def _assert_base(connection: AsyncConnection) -> None:
    assert not MANAGED_SCHEMAS & await _names(
        connection,
        "SELECT nspname FROM pg_namespace WHERE nspname = ANY(:names)",
        {"names": sorted(MANAGED_SCHEMAS)},
    )
    assert not MANAGED_ROLES & await _names(
        connection,
        "SELECT rolname FROM pg_roles WHERE rolname = ANY(:names)",
        {"names": sorted(MANAGED_ROLES)},
    )
    assert await connection.scalar(text("SELECT count(*) FROM public.alembic_version")) == 0


async def _names(
    connection: AsyncConnection,
    statement: str,
    parameters: dict[str, Any] | None = None,
) -> set[str]:
    rows = await connection.execute(text(statement), parameters or {})
    return set(rows.scalars())


EXPECTED_CONSTRAINT_NAMES = {
    "pk_workspaces",
    "pk_projects",
    "uq_projects_workspace_id",
    "fk_projects_workspace_id_workspaces",
    "pk_repositories",
    "uq_repositories_remote_host",
    "pk_project_repositories",
    "uq_project_repositories_project_id",
    "fk_project_repositories_project_id_projects",
    "fk_project_repositories_repository_id_repositories",
    "pk_checkouts",
    "uq_checkouts_repository_id",
    "fk_checkouts_repository_id_repositories",
    "pk_content_objects",
    "uq_content_objects_content_sha256",
    "uq_content_objects_object_key",
    "ck_content_objects_content_sha256_format",
    "ck_content_objects_object_key_not_empty",
    "ck_content_objects_compressed_bytes_nonnegative",
    "ck_content_objects_uncompressed_bytes_nonnegative",
    "pk_event_streams",
    "fk_event_streams_last_event_id_events",
    "ck_event_streams_non_negative_last_sequence",
    "ck_event_streams_head_matches_sequence",
    "ck_event_streams_quarantine_matches_status",
    "ck_event_streams_timestamp_order",
    "ck_event_streams_last_event_sha256_lower_hex",
    "ck_event_streams_stream_status",
    "pk_events",
    "uq_events_producer_id",
    "uq_events_stream_id",
    "fk_events_stream_id_event_streams",
    "ck_events_positive_stream_sequence",
    "ck_events_payload_sha256_lower_hex",
    "ck_events_event_sha256_lower_hex",
    "ck_events_previous_hash_matches_sequence",
    "pk_event_content_refs",
    "fk_event_content_refs_event_id_events",
    "ck_event_content_refs_content_sha256_lower_hex",
    "ck_event_content_refs_non_negative_uncompressed_bytes",
    "ck_event_content_refs_storage_form",
    "ck_event_content_refs_object_key_matches_digest",
    "ck_event_content_refs_content_disposition",
    "ck_event_content_refs_content_storage",
    "pk_redaction_reports",
    "fk_redaction_reports_event_id_event_content_refs",
    "ck_redaction_reports_disposition_details",
    "ck_redaction_reports_failure_detector_pair",
    "ck_redaction_reports_redaction_disposition",
    "ck_redaction_reports_metadata_only_reason",
    "pk_integrity_checkpoints",
    "ck_integrity_checkpoints_heads_sha256_lower_hex",
    "ck_integrity_checkpoints_non_negative_stream_count",
    "ck_integrity_checkpoints_signature_algorithm",
    "pk_outbox",
    "uq_outbox_event_id",
    "fk_outbox_event_id_events",
    "ck_outbox_non_negative_retry_count",
    "ck_outbox_lease_matches_status",
    "ck_outbox_terminal_timestamp_matches_status",
    "ck_outbox_timestamp_order",
    "ck_outbox_public_error_class",
    "ck_outbox_outbox_status",
    "pk_projection_checkpoints",
    "fk_projection_checkpoints_last_outbox_id_outbox",
    "fk_projection_checkpoints_last_event_id_events",
    "ck_projection_checkpoints_non_negative_processed_count",
    "ck_projection_checkpoints_progress_is_complete_pair",
    "ck_projection_checkpoints_projection_state",
    "pk_ingestion_batches",
    "ck_ingestion_batches_request_sha256_lower_hex",
    "ck_ingestion_batches_event_count_bounds",
    "ck_ingestion_batches_result_counts",
    "ck_ingestion_batches_status_cycle",
    "ck_ingestion_batches_timestamp_order",
    "ck_ingestion_batches_public_error_code",
    "ck_ingestion_batches_ingestion_batch_status",
    "pk_dead_letters",
    "fk_dead_letters_outbox_id_outbox",
    "fk_dead_letters_event_id_events",
    "ck_dead_letters_positive_attempts",
    "ck_dead_letters_resolution_matches_status",
    "ck_dead_letters_timestamp_order",
    "ck_dead_letters_public_error_class",
    "ck_dead_letters_dead_letter_status",
    "pk_registered_producers",
    "uq_registered_producers_token_prefix",
    "ck_registered_producers_argon2id_verifier",
    "ck_registered_producers_ingest_scope",
    "ck_registered_producers_expiry_after_creation",
    "ck_registered_producers_revocation_after_creation",
    "ck_registered_producers_timestamp_order",
    "pk_schema_versions",
    "ck_schema_versions_schema_sha256_lower_hex",
    "pk_retention_policies",
    "uq_retention_policies_resource_class",
    "ck_retention_policies_positive_retention_days",
    "ck_retention_policies_effective_interval",
}
EXPECTED_ENUM_VALUES = {
    "ck_event_streams_stream_status": ("active", "quarantined"),
    "ck_event_content_refs_content_disposition": (
        "sanitized",
        "metadata_only",
        "dropped_redaction_failure",
        "purged",
    ),
    "ck_event_content_refs_content_storage": ("inline", "object"),
    "ck_redaction_reports_redaction_disposition": (
        "sanitized",
        "metadata_only",
        "dropped_redaction_failure",
        "purged",
    ),
    "ck_redaction_reports_metadata_only_reason": (
        "source_unmapped",
        "source_invalid",
        "spool_pressure",
    ),
    "ck_outbox_outbox_status": ("pending", "leased", "delivered", "dead_lettered"),
    "ck_projection_checkpoints_projection_state": ("active", "paused", "rebuilding"),
    "ck_ingestion_batches_ingestion_batch_status": (
        "processing",
        "accepted",
        "rejected",
    ),
    "ck_dead_letters_dead_letter_status": ("open", "requeued", "resolved"),
}


def _table_grants(role: str, tables: set[str], privileges: set[str]) -> set[tuple[str, str, str]]:
    return {(role, table, privilege) for table in tables for privilege in privileges}


CATALOG_TABLES = {f"catalog.{table}" for table in EXPECTED_TABLES["catalog"]}
LEDGER_TABLES = {f"ledger.{table}" for table in EXPECTED_TABLES["ledger"]}
OPERATIONS_TABLES = {f"operations.{table}" for table in EXPECTED_TABLES["operations"]}
EXPECTED_TABLE_GRANTS = (
    _table_grants("agent_context_api", CATALOG_TABLES, {"SELECT", "INSERT"})
    | _table_grants("agent_context_api", LEDGER_TABLES, {"SELECT", "INSERT"})
    | _table_grants("agent_context_api", OPERATIONS_TABLES, {"SELECT"})
    | _table_grants(
        "agent_context_api",
        {"projection.outbox", "projection.ingestion_batches"},
        {"SELECT", "INSERT"},
    )
    | _table_grants("agent_context_projector", LEDGER_TABLES, {"SELECT"})
    | _table_grants("agent_context_projector", {"projection.outbox"}, {"SELECT"})
    | _table_grants(
        "agent_context_projector",
        {"projection.projection_checkpoints", "projection.dead_letters"},
        {"SELECT", "INSERT"},
    )
)
EXPECTED_UPDATE_GRANTS = {
    ("agent_context_api", "catalog.repositories", "observed_at"),
    ("agent_context_api", "catalog.checkouts", "observed_at"),
    *{
        ("agent_context_api", "ledger.event_streams", column)
        for column in {
            "last_sequence",
            "last_event_id",
            "last_event_sha256",
            "status",
            "quarantined_at",
            "updated_at",
        }
    },
    *{
        ("agent_context_api", "projection.ingestion_batches", column)
        for column in {
            "accepted_count",
            "rejected_count",
            "status",
            "error_code",
            "finished_at",
        }
    },
    ("agent_context_api", "operations.registered_producers", "last_used_at"),
    ("agent_context_api", "operations.registered_producers", "updated_at"),
    *{
        ("agent_context_projector", "projection.outbox", column)
        for column in {
            "status",
            "retry_count",
            "available_at",
            "lease_owner",
            "lease_expires_at",
            "last_error_class",
            "delivered_at",
            "dead_lettered_at",
            "updated_at",
        }
    },
    *{
        ("agent_context_projector", "projection.projection_checkpoints", column)
        for column in EXPECTED_COLUMNS["projection.projection_checkpoints"]
        - {"projector_name", "projector_version"}
    },
    *{
        ("agent_context_projector", "projection.dead_letters", column)
        for column in EXPECTED_COLUMNS["projection.dead_letters"] - {"outbox_id"}
    },
}
