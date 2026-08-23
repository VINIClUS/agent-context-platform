"""Initial ledger, catalog, projection, and operations schemas.

Revision ID: 20260823_0001
Revises:
Create Date: 2026-08-23
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260823_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for schema in ("catalog", "ledger", "projection", "operations"):
        op.execute(sa.schema.CreateSchema(schema))

    _create_catalog_tables()
    _create_ledger_tables()
    _create_projection_tables()
    _create_operations_tables()
    _create_roles_and_grants()


def downgrade() -> None:
    op.drop_constraint(
        "fk_event_streams_last_event_id_events",
        "event_streams",
        schema="ledger",
        type_="foreignkey",
    )

    for schema, table in (
        ("operations", "retention_policies"),
        ("operations", "schema_versions"),
        ("operations", "registered_producers"),
        ("projection", "dead_letters"),
        ("projection", "ingestion_batches"),
        ("projection", "projection_checkpoints"),
        ("projection", "outbox"),
        ("ledger", "integrity_checkpoints"),
        ("ledger", "redaction_reports"),
        ("ledger", "event_content_refs"),
        ("ledger", "events"),
        ("ledger", "event_streams"),
        ("catalog", "content_objects"),
        ("catalog", "checkouts"),
        ("catalog", "project_repositories"),
        ("catalog", "projects"),
        ("catalog", "repositories"),
        ("catalog", "workspaces"),
    ):
        op.drop_table(table, schema=schema)

    for schema in ("operations", "projection", "ledger", "catalog"):
        op.execute(sa.schema.DropSchema(schema))

    op.execute("DROP ROLE agent_context_projector")
    op.execute("DROP ROLE agent_context_api")


def _create_catalog_tables() -> None:
    uuid = postgresql.UUID(as_uuid=True)

    def observed_columns() -> tuple[sa.Column[object], ...]:
        return (
            sa.Column("id", uuid, server_default=sa.text("uuidv7()"), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.Column(
                "observed_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
        )

    op.create_table(
        "workspaces",
        sa.Column("name", sa.String(255), nullable=False),
        *observed_columns(),
        sa.PrimaryKeyConstraint("id", name="pk_workspaces"),
        schema="catalog",
    )
    op.create_table(
        "repositories",
        sa.Column("remote_host", sa.String(255), nullable=False),
        sa.Column("remote_owner", sa.String(255), nullable=False),
        sa.Column("remote_path", sa.String(1024), nullable=False),
        sa.Column("original_remote_url", sa.Text(), nullable=False),
        *observed_columns(),
        sa.PrimaryKeyConstraint("id", name="pk_repositories"),
        sa.UniqueConstraint(
            "remote_host", "remote_owner", "remote_path", name="uq_repositories_remote_host"
        ),
        schema="catalog",
    )
    op.create_table(
        "projects",
        sa.Column("workspace_id", uuid, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        *observed_columns(),
        sa.ForeignKeyConstraint(
            ["workspace_id"],
            ["catalog.workspaces.id"],
            name="fk_projects_workspace_id_workspaces",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_projects"),
        sa.UniqueConstraint("workspace_id", "name", name="uq_projects_workspace_id"),
        schema="catalog",
    )
    op.create_table(
        "project_repositories",
        sa.Column("project_id", uuid, nullable=False),
        sa.Column("repository_id", uuid, nullable=False),
        *observed_columns(),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["catalog.projects.id"],
            name="fk_project_repositories_project_id_projects",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["repository_id"],
            ["catalog.repositories.id"],
            name="fk_project_repositories_repository_id_repositories",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_project_repositories"),
        sa.UniqueConstraint(
            "project_id", "repository_id", name="uq_project_repositories_project_id"
        ),
        schema="catalog",
    )
    op.create_table(
        "checkouts",
        sa.Column("repository_id", uuid, nullable=False),
        sa.Column("filesystem_root", sa.String(4096), nullable=False),
        *observed_columns(),
        sa.ForeignKeyConstraint(
            ["repository_id"],
            ["catalog.repositories.id"],
            name="fk_checkouts_repository_id_repositories",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_checkouts"),
        sa.UniqueConstraint("repository_id", "filesystem_root", name="uq_checkouts_repository_id"),
        schema="catalog",
    )
    op.create_table(
        "content_objects",
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("object_key", sa.String(1024), nullable=False),
        sa.Column("media_type", sa.String(255), nullable=False),
        sa.Column("compressed_bytes", sa.BigInteger(), nullable=False),
        sa.Column("uncompressed_bytes", sa.BigInteger(), nullable=False),
        *observed_columns(),
        sa.CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_content_objects_content_sha256_format",
        ),
        sa.CheckConstraint(
            "length(object_key) > 0", name="ck_content_objects_object_key_not_empty"
        ),
        sa.CheckConstraint(
            "compressed_bytes >= 0", name="ck_content_objects_compressed_bytes_nonnegative"
        ),
        sa.CheckConstraint(
            "uncompressed_bytes >= 0",
            name="ck_content_objects_uncompressed_bytes_nonnegative",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_content_objects"),
        sa.UniqueConstraint("content_sha256", name="uq_content_objects_content_sha256"),
        sa.UniqueConstraint("object_key", name="uq_content_objects_object_key"),
        schema="catalog",
    )


def _create_ledger_tables() -> None:
    uuid = postgresql.UUID(as_uuid=True)
    timestamp = sa.DateTime(timezone=True)
    op.create_table(
        "event_streams",
        sa.Column("stream_id", sa.String(512), nullable=False),
        sa.Column("last_sequence", sa.BigInteger(), nullable=False),
        sa.Column("last_event_id", uuid, nullable=True),
        sa.Column("last_event_sha256", sa.String(64), nullable=True),
        sa.Column("status", sa.String(11), nullable=False),
        sa.Column("quarantined_at", timestamp, nullable=True),
        sa.Column("created_at", timestamp, nullable=False),
        sa.Column("updated_at", timestamp, nullable=False),
        sa.CheckConstraint(
            "last_sequence >= 0", name="ck_event_streams_non_negative_last_sequence"
        ),
        sa.CheckConstraint(
            "(last_sequence = 0 AND last_event_id IS NULL AND last_event_sha256 IS NULL) OR "
            "(last_sequence > 0 AND last_event_id IS NOT NULL AND last_event_sha256 IS NOT NULL)",
            name="ck_event_streams_head_matches_sequence",
        ),
        sa.CheckConstraint(
            "(status = 'active' AND quarantined_at IS NULL) OR "
            "(status = 'quarantined' AND quarantined_at IS NOT NULL)",
            name="ck_event_streams_quarantine_matches_status",
        ),
        sa.CheckConstraint("created_at <= updated_at", name="ck_event_streams_timestamp_order"),
        sa.CheckConstraint(
            "last_event_sha256 IS NULL OR last_event_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_event_streams_last_event_sha256_lower_hex",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'quarantined')", name="ck_event_streams_stream_status"
        ),
        sa.PrimaryKeyConstraint("stream_id", name="pk_event_streams"),
        schema="ledger",
    )
    op.create_table(
        "events",
        sa.Column("event_id", uuid, nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("schema_version", sa.String(64), nullable=False),
        sa.Column("stream_id", sa.String(512), nullable=False),
        sa.Column("stream_sequence", sa.BigInteger(), nullable=False),
        sa.Column("producer_id", sa.String(255), nullable=False),
        sa.Column("idempotency_key", sa.String(512), nullable=False),
        sa.Column("occurred_at", timestamp, nullable=False),
        sa.Column("observed_at", timestamp, nullable=False),
        sa.Column("recorded_at", timestamp, nullable=False),
        sa.Column("producer", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("context", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trace", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("redaction", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("previous_event_sha256", sa.String(64), nullable=True),
        sa.Column("event_sha256", sa.String(64), nullable=False),
        sa.CheckConstraint("stream_sequence > 0", name="ck_events_positive_stream_sequence"),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'", name="ck_events_payload_sha256_lower_hex"
        ),
        sa.CheckConstraint(
            "event_sha256 ~ '^[0-9a-f]{64}$'", name="ck_events_event_sha256_lower_hex"
        ),
        sa.CheckConstraint(
            "(stream_sequence = 1 AND previous_event_sha256 IS NULL) OR "
            "(stream_sequence > 1 AND previous_event_sha256 ~ '^[0-9a-f]{64}$')",
            name="ck_events_previous_hash_matches_sequence",
        ),
        sa.ForeignKeyConstraint(
            ["stream_id"],
            ["ledger.event_streams.stream_id"],
            name="fk_events_stream_id_event_streams",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_events"),
        sa.UniqueConstraint("producer_id", "idempotency_key", name="uq_events_producer_id"),
        sa.UniqueConstraint("stream_id", "stream_sequence", name="uq_events_stream_id"),
        schema="ledger",
    )
    op.create_foreign_key(
        "fk_event_streams_last_event_id_events",
        "event_streams",
        "events",
        ["last_event_id"],
        ["event_id"],
        source_schema="ledger",
        referent_schema="ledger",
    )
    op.create_table(
        "event_content_refs",
        sa.Column("event_id", uuid, nullable=False),
        sa.Column("content_id", sa.String(512), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("media_type", sa.String(255), nullable=False),
        sa.Column("uncompressed_bytes", sa.BigInteger(), nullable=False),
        sa.Column("disposition", sa.String(25), nullable=False),
        sa.Column("storage", sa.String(6), nullable=False),
        sa.Column("inline_id", sa.String(512), nullable=True),
        sa.Column("object_key", sa.String(512), nullable=True),
        sa.Column("encoding", sa.String(16), nullable=True),
        sa.CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_event_content_refs_content_sha256_lower_hex",
        ),
        sa.CheckConstraint(
            "uncompressed_bytes >= 0",
            name="ck_event_content_refs_non_negative_uncompressed_bytes",
        ),
        sa.CheckConstraint(
            "(storage = 'inline' AND inline_id IS NOT NULL AND object_key IS NULL "
            "AND encoding IS NULL) OR (storage = 'object' AND inline_id IS NULL "
            "AND object_key IS NOT NULL AND encoding = 'zstd')",
            name="ck_event_content_refs_storage_form",
        ),
        sa.CheckConstraint(
            "storage <> 'object' OR object_key = "
            "'sha256/' || substr(content_sha256, 1, 2) || '/' || "
            "substr(content_sha256, 3, 2) || '/' || content_sha256 || '.zst'",
            name="ck_event_content_refs_object_key_matches_digest",
        ),
        sa.CheckConstraint(
            "disposition IN ('sanitized', 'metadata_only', 'dropped_redaction_failure', 'purged')",
            name="ck_event_content_refs_content_disposition",
        ),
        sa.CheckConstraint(
            "storage IN ('inline', 'object')", name="ck_event_content_refs_content_storage"
        ),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["ledger.events.event_id"],
            name="fk_event_content_refs_event_id_events",
        ),
        sa.PrimaryKeyConstraint("event_id", "content_id", name="pk_event_content_refs"),
        schema="ledger",
    )
    op.create_table(
        "redaction_reports",
        sa.Column("event_id", uuid, nullable=False),
        sa.Column("content_id", sa.String(512), nullable=False),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("disposition", sa.String(25), nullable=False),
        sa.Column("findings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metadata_only_reason", sa.String(15), nullable=True),
        sa.Column("error_class", sa.String(128), nullable=True),
        sa.Column("failed_detector", sa.String(255), nullable=True),
        sa.Column("failed_detector_version", sa.String(64), nullable=True),
        sa.CheckConstraint(
            "(disposition = 'metadata_only' AND metadata_only_reason IS NOT NULL "
            "AND error_class IS NULL AND failed_detector IS NULL "
            "AND failed_detector_version IS NULL) OR "
            "(disposition = 'dropped_redaction_failure' AND metadata_only_reason IS NULL "
            "AND error_class IS NOT NULL AND failed_detector IS NOT NULL "
            "AND failed_detector_version IS NOT NULL) OR "
            "(disposition IN ('sanitized', 'purged') AND metadata_only_reason IS NULL "
            "AND error_class IS NULL AND failed_detector IS NULL "
            "AND failed_detector_version IS NULL)",
            name="ck_redaction_reports_disposition_details",
        ),
        sa.CheckConstraint(
            "(failed_detector IS NULL AND failed_detector_version IS NULL) OR "
            "(failed_detector IS NOT NULL AND failed_detector_version IS NOT NULL)",
            name="ck_redaction_reports_failure_detector_pair",
        ),
        sa.CheckConstraint(
            "disposition IN ('sanitized', 'metadata_only', 'dropped_redaction_failure', 'purged')",
            name="ck_redaction_reports_redaction_disposition",
        ),
        sa.CheckConstraint(
            "metadata_only_reason IN ('source_unmapped', 'source_invalid', 'spool_pressure')",
            name="ck_redaction_reports_metadata_only_reason",
        ),
        sa.ForeignKeyConstraint(
            ["event_id", "content_id"],
            ["ledger.event_content_refs.event_id", "ledger.event_content_refs.content_id"],
            name="fk_redaction_reports_event_id_event_content_refs",
        ),
        sa.PrimaryKeyConstraint("event_id", "content_id", name="pk_redaction_reports"),
        schema="ledger",
    )
    op.create_table(
        "integrity_checkpoints",
        sa.Column("checkpoint_id", uuid, nullable=False),
        sa.Column("stream_heads", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("heads_sha256", sa.String(64), nullable=False),
        sa.Column("signature_algorithm", sa.String(32), nullable=False),
        sa.Column("signing_key_id", sa.String(255), nullable=False),
        sa.Column("signature", sa.LargeBinary(), nullable=False),
        sa.Column("stream_count", sa.Integer(), nullable=False),
        sa.Column("created_at", timestamp, nullable=False),
        sa.CheckConstraint(
            "heads_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_integrity_checkpoints_heads_sha256_lower_hex",
        ),
        sa.CheckConstraint(
            "stream_count >= 0", name="ck_integrity_checkpoints_non_negative_stream_count"
        ),
        sa.CheckConstraint(
            "signature_algorithm = 'ed25519'",
            name="ck_integrity_checkpoints_signature_algorithm",
        ),
        sa.PrimaryKeyConstraint("checkpoint_id", name="pk_integrity_checkpoints"),
        schema="ledger",
    )


def _create_projection_tables() -> None:
    uuid = postgresql.UUID(as_uuid=True)
    timestamp = sa.DateTime(timezone=True)
    op.create_table(
        "outbox",
        sa.Column("outbox_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("event_id", uuid, nullable=False),
        sa.Column("status", sa.String(13), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("available_at", timestamp, nullable=False),
        sa.Column("lease_owner", sa.String(255), nullable=True),
        sa.Column("lease_expires_at", timestamp, nullable=True),
        sa.Column("last_error_class", sa.String(128), nullable=True),
        sa.Column("delivered_at", timestamp, nullable=True),
        sa.Column("dead_lettered_at", timestamp, nullable=True),
        sa.Column("created_at", timestamp, nullable=False),
        sa.Column("updated_at", timestamp, nullable=False),
        sa.CheckConstraint("retry_count >= 0", name="ck_outbox_non_negative_retry_count"),
        sa.CheckConstraint(
            "(status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="ck_outbox_lease_matches_status",
        ),
        sa.CheckConstraint(
            "(status = 'delivered' AND delivered_at IS NOT NULL AND dead_lettered_at IS NULL) "
            "OR (status = 'dead_lettered' AND delivered_at IS NULL "
            "AND dead_lettered_at IS NOT NULL) OR (status IN ('pending', 'leased') "
            "AND delivered_at IS NULL AND dead_lettered_at IS NULL)",
            name="ck_outbox_terminal_timestamp_matches_status",
        ),
        sa.CheckConstraint(
            "created_at <= updated_at AND created_at <= available_at",
            name="ck_outbox_timestamp_order",
        ),
        sa.CheckConstraint(
            "last_error_class IS NULL OR last_error_class ~ '^[A-Za-z][A-Za-z0-9_.]{0,127}$'",
            name="ck_outbox_public_error_class",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'leased', 'delivered', 'dead_lettered')",
            name="ck_outbox_outbox_status",
        ),
        sa.ForeignKeyConstraint(
            ["event_id"], ["ledger.events.event_id"], name="fk_outbox_event_id_events"
        ),
        sa.PrimaryKeyConstraint("outbox_id", name="pk_outbox"),
        sa.UniqueConstraint("event_id", name="uq_outbox_event_id"),
        schema="projection",
    )
    op.create_index(
        "ix_outbox_status",
        "outbox",
        ["status", "available_at", "outbox_id"],
        schema="projection",
    )
    op.create_table(
        "projection_checkpoints",
        sa.Column("projector_name", sa.String(255), nullable=False),
        sa.Column("projector_version", sa.String(64), nullable=False),
        sa.Column("last_outbox_id", sa.BigInteger(), nullable=True),
        sa.Column("last_event_id", uuid, nullable=True),
        sa.Column("processed_count", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(10), nullable=False),
        sa.Column("updated_at", timestamp, nullable=False),
        sa.CheckConstraint(
            "processed_count >= 0",
            name="ck_projection_checkpoints_non_negative_processed_count",
        ),
        sa.CheckConstraint(
            "(processed_count = 0 AND last_outbox_id IS NULL AND last_event_id IS NULL) OR "
            "(processed_count > 0 AND last_outbox_id IS NOT NULL AND last_event_id IS NOT NULL)",
            name="ck_projection_checkpoints_progress_is_complete_pair",
        ),
        sa.CheckConstraint(
            "state IN ('active', 'paused', 'rebuilding')",
            name="ck_projection_checkpoints_projection_state",
        ),
        sa.ForeignKeyConstraint(
            ["last_outbox_id"],
            ["projection.outbox.outbox_id"],
            name="fk_projection_checkpoints_last_outbox_id_outbox",
        ),
        sa.ForeignKeyConstraint(
            ["last_event_id"],
            ["ledger.events.event_id"],
            name="fk_projection_checkpoints_last_event_id_events",
        ),
        sa.PrimaryKeyConstraint(
            "projector_name", "projector_version", name="pk_projection_checkpoints"
        ),
        schema="projection",
    )
    op.create_table(
        "ingestion_batches",
        sa.Column("batch_id", uuid, nullable=False),
        sa.Column("producer_id", sa.String(255), nullable=False),
        sa.Column("request_sha256", sa.String(64), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("accepted_count", sa.Integer(), nullable=False),
        sa.Column("rejected_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(10), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("started_at", timestamp, nullable=False),
        sa.Column("finished_at", timestamp, nullable=True),
        sa.CheckConstraint(
            "request_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_ingestion_batches_request_sha256_lower_hex",
        ),
        sa.CheckConstraint(
            "event_count BETWEEN 1 AND 500", name="ck_ingestion_batches_event_count_bounds"
        ),
        sa.CheckConstraint(
            "accepted_count >= 0 AND rejected_count >= 0 "
            "AND accepted_count + rejected_count <= event_count",
            name="ck_ingestion_batches_result_counts",
        ),
        sa.CheckConstraint(
            "(status = 'processing' AND finished_at IS NULL AND error_code IS NULL "
            "AND accepted_count = 0 AND rejected_count = 0) OR "
            "(status = 'accepted' AND finished_at IS NOT NULL AND error_code IS NULL "
            "AND accepted_count = event_count AND rejected_count = 0) OR "
            "(status = 'rejected' AND finished_at IS NOT NULL AND error_code IS NOT NULL "
            "AND accepted_count + rejected_count = event_count AND rejected_count > 0)",
            name="ck_ingestion_batches_status_cycle",
        ),
        sa.CheckConstraint(
            "finished_at IS NULL OR started_at <= finished_at",
            name="ck_ingestion_batches_timestamp_order",
        ),
        sa.CheckConstraint(
            "error_code IS NULL OR error_code ~ '^[a-z][a-z0-9_]{0,127}$'",
            name="ck_ingestion_batches_public_error_code",
        ),
        sa.CheckConstraint(
            "status IN ('processing', 'accepted', 'rejected')",
            name="ck_ingestion_batches_ingestion_batch_status",
        ),
        sa.PrimaryKeyConstraint("batch_id", name="pk_ingestion_batches"),
        schema="projection",
    )
    op.create_table(
        "dead_letters",
        sa.Column("outbox_id", sa.BigInteger(), nullable=False),
        sa.Column("event_id", uuid, nullable=False),
        sa.Column("projector_name", sa.String(255), nullable=False),
        sa.Column("projector_version", sa.String(64), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("error_class", sa.String(128), nullable=False),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("first_failed_at", timestamp, nullable=False),
        sa.Column("updated_at", timestamp, nullable=False),
        sa.Column("resolved_at", timestamp, nullable=True),
        sa.CheckConstraint("attempts > 0", name="ck_dead_letters_positive_attempts"),
        sa.CheckConstraint(
            "(status = 'open' AND resolved_at IS NULL) OR "
            "(status IN ('requeued', 'resolved') AND resolved_at IS NOT NULL)",
            name="ck_dead_letters_resolution_matches_status",
        ),
        sa.CheckConstraint(
            "first_failed_at <= updated_at AND "
            "(resolved_at IS NULL OR first_failed_at <= resolved_at)",
            name="ck_dead_letters_timestamp_order",
        ),
        sa.CheckConstraint(
            "error_class ~ '^[A-Za-z][A-Za-z0-9_.]{0,127}$'",
            name="ck_dead_letters_public_error_class",
        ),
        sa.CheckConstraint(
            "status IN ('open', 'requeued', 'resolved')",
            name="ck_dead_letters_dead_letter_status",
        ),
        sa.ForeignKeyConstraint(
            ["outbox_id"],
            ["projection.outbox.outbox_id"],
            name="fk_dead_letters_outbox_id_outbox",
        ),
        sa.ForeignKeyConstraint(
            ["event_id"], ["ledger.events.event_id"], name="fk_dead_letters_event_id_events"
        ),
        sa.PrimaryKeyConstraint("outbox_id", name="pk_dead_letters"),
        schema="projection",
    )


def _create_operations_tables() -> None:
    uuid = postgresql.UUID(as_uuid=True)
    timestamp = sa.DateTime(timezone=True)
    op.create_table(
        "registered_producers",
        sa.Column("producer_id", sa.String(255), nullable=False),
        sa.Column("token_prefix", sa.String(64), nullable=False),
        sa.Column("token_verifier", sa.String(512), nullable=False),
        sa.Column("scope", sa.String(64), nullable=False),
        sa.Column("expires_at", timestamp, nullable=False),
        sa.Column("revoked_at", timestamp, nullable=True),
        sa.Column("created_at", timestamp, nullable=False),
        sa.Column("updated_at", timestamp, nullable=False),
        sa.Column("last_used_at", timestamp, nullable=True),
        sa.CheckConstraint(
            "token_verifier LIKE '$argon2id$v=19$%'",
            name="ck_registered_producers_argon2id_verifier",
        ),
        sa.CheckConstraint("scope = 'events:ingest'", name="ck_registered_producers_ingest_scope"),
        sa.CheckConstraint(
            "expires_at > created_at", name="ck_registered_producers_expiry_after_creation"
        ),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="ck_registered_producers_revocation_after_creation",
        ),
        sa.CheckConstraint(
            "created_at <= updated_at", name="ck_registered_producers_timestamp_order"
        ),
        sa.PrimaryKeyConstraint("producer_id", name="pk_registered_producers"),
        sa.UniqueConstraint("token_prefix", name="uq_registered_producers_token_prefix"),
        schema="operations",
    )
    op.create_table(
        "schema_versions",
        sa.Column("schema_name", sa.String(255), nullable=False),
        sa.Column("schema_version", sa.String(64), nullable=False),
        sa.Column("schema_document", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("schema_sha256", sa.String(64), nullable=False),
        sa.Column("upcaster", sa.String(512), nullable=True),
        sa.Column("registered_at", timestamp, nullable=False),
        sa.CheckConstraint(
            "schema_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_schema_versions_schema_sha256_lower_hex",
        ),
        sa.PrimaryKeyConstraint("schema_name", "schema_version", name="pk_schema_versions"),
        schema="operations",
    )
    op.create_table(
        "retention_policies",
        sa.Column("retention_policy_id", uuid, nullable=False),
        sa.Column("resource_class", sa.String(255), nullable=False),
        sa.Column("retention_days", sa.Integer(), nullable=True),
        sa.Column("preserve_metadata", sa.Boolean(), nullable=False),
        sa.Column("effective_from", timestamp, nullable=False),
        sa.Column("effective_to", timestamp, nullable=True),
        sa.Column("created_at", timestamp, nullable=False),
        sa.CheckConstraint(
            "retention_days IS NULL OR retention_days > 0",
            name="ck_retention_policies_positive_retention_days",
        ),
        sa.CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_retention_policies_effective_interval",
        ),
        sa.PrimaryKeyConstraint("retention_policy_id", name="pk_retention_policies"),
        sa.UniqueConstraint(
            "resource_class", "effective_from", name="uq_retention_policies_resource_class"
        ),
        schema="operations",
    )


def _create_roles_and_grants() -> None:
    for role in ("agent_context_api", "agent_context_projector"):
        op.execute(
            f"CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS"
        )

    op.execute("GRANT USAGE ON SCHEMA catalog, ledger, projection, operations TO agent_context_api")
    op.execute("GRANT USAGE ON SCHEMA ledger, projection TO agent_context_projector")

    catalog_tables = ", ".join(
        f"catalog.{table}"
        for table in (
            "workspaces",
            "projects",
            "repositories",
            "project_repositories",
            "checkouts",
            "content_objects",
        )
    )
    ledger_tables = ", ".join(
        f"ledger.{table}"
        for table in (
            "event_streams",
            "events",
            "event_content_refs",
            "redaction_reports",
            "integrity_checkpoints",
        )
    )
    op.execute(f"GRANT SELECT, INSERT ON {catalog_tables} TO agent_context_api")
    op.execute(f"GRANT SELECT, INSERT ON {ledger_tables} TO agent_context_api")
    op.execute(f"GRANT SELECT ON {ledger_tables} TO agent_context_projector")
    op.execute(
        "GRANT SELECT, INSERT ON projection.outbox, projection.ingestion_batches "
        "TO agent_context_api"
    )
    op.execute(
        "GRANT SELECT ON operations.registered_producers, operations.schema_versions, "
        "operations.retention_policies TO agent_context_api"
    )
    op.execute("GRANT SELECT ON projection.outbox TO agent_context_projector")
    op.execute(
        "GRANT SELECT, INSERT ON projection.projection_checkpoints, "
        "projection.dead_letters TO agent_context_projector"
    )

    op.execute("GRANT UPDATE (observed_at) ON catalog.repositories TO agent_context_api")
    op.execute("GRANT UPDATE (observed_at) ON catalog.checkouts TO agent_context_api")
    op.execute(
        "GRANT UPDATE (last_sequence, last_event_id, last_event_sha256, status, "
        "quarantined_at, updated_at) ON ledger.event_streams TO agent_context_api"
    )
    op.execute(
        "GRANT UPDATE (accepted_count, rejected_count, status, error_code, finished_at) "
        "ON projection.ingestion_batches TO agent_context_api"
    )
    op.execute(
        "GRANT UPDATE (last_used_at, updated_at) ON operations.registered_producers "
        "TO agent_context_api"
    )
    op.execute(
        "GRANT UPDATE (status, retry_count, available_at, lease_owner, lease_expires_at, "
        "last_error_class, delivered_at, dead_lettered_at, updated_at) "
        "ON projection.outbox TO agent_context_projector"
    )
    op.execute(
        "GRANT UPDATE (last_outbox_id, last_event_id, processed_count, state, updated_at) "
        "ON projection.projection_checkpoints TO agent_context_projector"
    )
    op.execute(
        "GRANT UPDATE (event_id, projector_name, projector_version, attempts, error_class, "
        "status, first_failed_at, updated_at, resolved_at) ON projection.dead_letters "
        "TO agent_context_projector"
    )
    op.execute("GRANT USAGE ON SEQUENCE projection.outbox_outbox_id_seq TO agent_context_api")
