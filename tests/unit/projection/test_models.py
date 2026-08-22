from __future__ import annotations

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from agent_context_platform.db import Base
from agent_context_platform.projection.models import (
    DeadLetterRow,
    IngestionBatchRow,
    OutboxRow,
    ProjectionCheckpointRow,
)


def _check_names(model: type[object]) -> set[str]:
    return {
        str(constraint.name)
        for constraint in model.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }


def test_projection_models_are_registered_in_explicit_schema() -> None:
    assert {
        "projection.outbox",
        "projection.projection_checkpoints",
        "projection.dead_letters",
        "projection.ingestion_batches",
    } <= set(Base.metadata.tables)
    assert {
        model.__table__.schema
        for model in (OutboxRow, ProjectionCheckpointRow, DeadLetterRow, IngestionBatchRow)
    } == {"projection"}


def test_outbox_has_monotonic_identity_unique_event_and_ordered_claim_index() -> None:
    table = OutboxRow.__table__

    assert tuple(column.name for column in table.primary_key.columns) == ("outbox_id",)
    assert table.c.outbox_id.identity is not None
    assert any(
        tuple(column.name for column in constraint.columns) == ("event_id",)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    )
    assert any(
        tuple(column.name for column in index.columns) == ("status", "available_at", "outbox_id")
        for index in table.indexes
        if isinstance(index, Index)
    )
    assert _check_names(OutboxRow) >= {
        "ck_outbox_non_negative_retry_count",
        "ck_outbox_lease_matches_status",
        "ck_outbox_terminal_timestamp_matches_status",
        "ck_outbox_timestamp_order",
    }


def test_projection_checkpoint_has_composite_identity_and_ordered_progress() -> None:
    table = ProjectionCheckpointRow.__table__

    assert tuple(column.name for column in table.primary_key.columns) == (
        "projector_name",
        "projector_version",
    )
    assert _check_names(ProjectionCheckpointRow) >= {
        "ck_projection_checkpoints_non_negative_processed_count",
        "ck_projection_checkpoints_progress_is_complete_pair",
    }


def test_dead_letter_is_one_per_outbox_and_secret_safe() -> None:
    table = DeadLetterRow.__table__
    columns = set(table.c.keys())

    assert tuple(column.name for column in table.primary_key.columns) == ("outbox_id",)
    assert isinstance(table.c.event_id.type, UUID)
    assert {"payload", "error_message", "message", "exception", "traceback"}.isdisjoint(columns)
    assert {"error_class", "attempts", "status"} <= columns
    assert _check_names(DeadLetterRow) >= {
        "ck_dead_letters_positive_attempts",
        "ck_dead_letters_resolution_matches_status",
        "ck_dead_letters_timestamp_order",
    }


def test_ingestion_batch_enforces_atomic_counts_hash_and_status_cycle() -> None:
    table = IngestionBatchRow.__table__

    assert isinstance(table.c.batch_id.type, UUID)
    assert _check_names(IngestionBatchRow) >= {
        "ck_ingestion_batches_request_sha256_lower_hex",
        "ck_ingestion_batches_event_count_bounds",
        "ck_ingestion_batches_result_counts",
        "ck_ingestion_batches_status_cycle",
        "ck_ingestion_batches_timestamp_order",
    }


def test_projection_foreign_keys_never_cascade() -> None:
    foreign_keys = [
        constraint
        for model in (OutboxRow, DeadLetterRow)
        for constraint in model.__table__.constraints
        if isinstance(constraint, ForeignKeyConstraint)
    ]

    assert foreign_keys
    assert all(constraint.ondelete is None for constraint in foreign_keys)
    checkpoint_targets = {
        element.target_fullname
        for constraint in ProjectionCheckpointRow.__table__.constraints
        if isinstance(constraint, ForeignKeyConstraint)
        for element in constraint.elements
    }
    assert checkpoint_targets == {
        "ledger.events.event_id",
        "projection.outbox.outbox_id",
    }


def test_projection_timestamps_are_timezone_aware_and_application_supplied() -> None:
    required = {
        OutboxRow: ("available_at", "created_at", "updated_at"),
        ProjectionCheckpointRow: ("updated_at",),
        DeadLetterRow: ("first_failed_at", "updated_at"),
        IngestionBatchRow: ("started_at",),
    }

    for model, names in required.items():
        for name in names:
            column = model.__table__.c[name]
            assert column.type.timezone is True
            assert column.nullable is False
            assert column.default is None
            assert column.server_default is None
            assert column.onupdate is None
