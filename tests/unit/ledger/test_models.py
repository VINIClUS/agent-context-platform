from __future__ import annotations

from sqlalchemy import CheckConstraint, Enum, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB, UUID

from agent_context_platform.db import Base
from agent_context_platform.ledger.models import (
    EventContentRefRow,
    EventRow,
    EventStreamRow,
    IntegrityCheckpointRow,
    RedactionReportRow,
)


def _ddl(model: type[object]) -> str:
    return str(
        __import__("sqlalchemy").schema.CreateTable(model.__table__).compile(
            dialect=postgresql.dialect()
        )
    )


def _check_names(model: type[object]) -> set[str]:
    return {
        str(constraint.name)
        for constraint in model.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }


def test_ledger_models_are_registered_in_explicit_schema() -> None:
    expected = {
        "ledger.event_streams",
        "ledger.events",
        "ledger.event_content_refs",
        "ledger.redaction_reports",
        "ledger.integrity_checkpoints",
    }

    assert expected <= set(Base.metadata.tables)
    assert {model.__table__.schema for model in (
        EventStreamRow,
        EventRow,
        EventContentRefRow,
        RedactionReportRow,
        IntegrityCheckpointRow,
    )} == {"ledger"}


def test_events_preserve_typed_identity_json_and_ordering() -> None:
    table = EventRow.__table__

    assert tuple(column.name for column in table.primary_key.columns) == ("event_id",)
    assert isinstance(table.c.event_id.type, UUID)
    assert all(isinstance(table.c[name].type, JSONB) for name in (
        "producer", "context", "trace", "payload", "redaction"
    ))
    assert {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    } >= {("producer_id", "idempotency_key"), ("stream_id", "stream_sequence")}
    assert _check_names(EventRow) >= {
        "ck_events_positive_stream_sequence",
        "ck_events_payload_sha256_lower_hex",
        "ck_events_event_sha256_lower_hex",
        "ck_events_previous_hash_matches_sequence",
    }


def test_ledger_foreign_keys_never_cascade() -> None:
    models = (EventRow, EventContentRefRow, RedactionReportRow)
    foreign_keys = [
        constraint
        for model in models
        for constraint in model.__table__.constraints
        if isinstance(constraint, ForeignKeyConstraint)
    ]

    assert foreign_keys
    assert all(constraint.ondelete is None for constraint in foreign_keys)


def test_content_reference_mirrors_sdk_storage_contract() -> None:
    table = EventContentRefRow.__table__

    assert tuple(column.name for column in table.primary_key.columns) == (
        "event_id", "content_id"
    )
    assert _check_names(EventContentRefRow) >= {
        "ck_event_content_refs_content_sha256_lower_hex",
        "ck_event_content_refs_non_negative_uncompressed_bytes",
        "ck_event_content_refs_storage_form",
        "ck_event_content_refs_object_key_matches_digest",
    }
    assert isinstance(table.c.disposition.type, Enum)
    assert table.c.disposition.type.enums == [
        "sanitized", "metadata_only", "dropped_redaction_failure", "purged"
    ]
    assert table.c.storage.type.enums == ["inline", "object"]
    ddl = _ddl(EventContentRefRow)
    assert "VARCHAR" in ddl and "CHECK" in ddl


def test_redaction_report_is_one_to_one_and_disposition_coherent() -> None:
    table = RedactionReportRow.__table__

    assert tuple(column.name for column in table.primary_key.columns) == (
        "event_id", "content_id"
    )
    assert isinstance(table.c.findings.type, JSONB)
    assert _check_names(RedactionReportRow) >= {
        "ck_redaction_reports_disposition_details",
        "ck_redaction_reports_failure_detector_pair",
    }


def test_stream_heads_and_integrity_checkpoints_enforce_consistency() -> None:
    assert _check_names(EventStreamRow) >= {
        "ck_event_streams_non_negative_last_sequence",
        "ck_event_streams_head_matches_sequence",
        "ck_event_streams_quarantine_matches_status",
        "ck_event_streams_timestamp_order",
        "ck_event_streams_last_event_sha256_lower_hex",
    }
    table = IntegrityCheckpointRow.__table__
    assert isinstance(table.c.checkpoint_id.type, UUID)
    assert isinstance(table.c.stream_heads.type, JSONB)
    assert _check_names(IntegrityCheckpointRow) >= {
        "ck_integrity_checkpoints_heads_sha256_lower_hex",
        "ck_integrity_checkpoints_non_negative_stream_count",
        "ck_integrity_checkpoints_signature_algorithm",
    }


def test_ledger_timestamps_are_required_and_application_supplied() -> None:
    timestamps = {
        EventStreamRow: ("created_at", "updated_at"),
        EventRow: ("occurred_at", "observed_at", "recorded_at"),
        IntegrityCheckpointRow: ("created_at",),
    }

    for model, names in timestamps.items():
        for name in names:
            column = model.__table__.c[name]
            assert column.type.timezone is True
            assert column.nullable is False
            assert column.default is None
            assert column.server_default is None
            assert column.onupdate is None
