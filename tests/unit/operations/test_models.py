from __future__ import annotations

from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID

from agent_context_platform.db import Base
from agent_context_platform.operations.models import (
    RegisteredProducerRow,
    RetentionPolicyRow,
    SchemaVersionRow,
)


def _check_names(model: type[object]) -> set[str]:
    return {
        str(constraint.name)
        for constraint in model.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }


def test_all_twelve_models_share_the_base_metadata() -> None:
    assert {
        "ledger.event_streams",
        "ledger.events",
        "ledger.event_content_refs",
        "ledger.redaction_reports",
        "ledger.integrity_checkpoints",
        "projection.outbox",
        "projection.projection_checkpoints",
        "projection.dead_letters",
        "projection.ingestion_batches",
        "operations.registered_producers",
        "operations.schema_versions",
        "operations.retention_policies",
    } <= set(Base.metadata.tables)


def test_registered_producer_stores_only_non_recoverable_credential_material() -> None:
    table = RegisteredProducerRow.__table__
    columns = set(table.c.keys())

    assert tuple(column.name for column in table.primary_key.columns) == ("producer_id",)
    assert any(
        tuple(column.name for column in constraint.columns) == ("token_prefix",)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    )
    assert {"bearer", "bearer_token", "token", "secret", "raw_token"}.isdisjoint(columns)
    assert {"token_prefix", "token_verifier", "scope"} <= columns
    assert _check_names(RegisteredProducerRow) >= {
        "ck_registered_producers_argon2id_verifier",
        "ck_registered_producers_ingest_scope",
        "ck_registered_producers_expiry_after_creation",
        "ck_registered_producers_revocation_after_creation",
    }


def test_schema_versions_are_identified_by_name_and_version_with_typed_document() -> None:
    table = SchemaVersionRow.__table__

    assert tuple(column.name for column in table.primary_key.columns) == (
        "schema_name", "schema_version"
    )
    assert isinstance(table.c.schema_document.type, JSONB)
    assert _check_names(SchemaVersionRow) == {
        "ck_schema_versions_schema_sha256_lower_hex"
    }


def test_retention_policy_has_effective_interval_and_unique_revision() -> None:
    table = RetentionPolicyRow.__table__

    assert isinstance(table.c.retention_policy_id.type, UUID)
    assert any(
        tuple(column.name for column in constraint.columns)
        == ("resource_class", "effective_from")
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    )
    assert _check_names(RetentionPolicyRow) >= {
        "ck_retention_policies_positive_retention_days",
        "ck_retention_policies_effective_interval",
    }


def test_operations_timestamps_are_timezone_aware_and_application_supplied() -> None:
    required = {
        RegisteredProducerRow: ("expires_at", "created_at", "updated_at"),
        SchemaVersionRow: ("registered_at",),
        RetentionPolicyRow: ("effective_from", "created_at"),
    }

    for model, names in required.items():
        for name in names:
            column = model.__table__.c[name]
            assert column.type.timezone is True
            assert column.nullable is False
            assert column.default is None
            assert column.server_default is None
            assert column.onupdate is None
