from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID as PythonUUID

from sqlalchemy import Boolean, CheckConstraint, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from agent_context_platform.db import Base


class RegisteredProducerRow(Base):
    __tablename__ = "registered_producers"
    __table_args__ = (
        UniqueConstraint("token_prefix"),
        CheckConstraint(
            "token_verifier LIKE '$argon2id$v=19$%'",
            name="argon2id_verifier",
        ),
        CheckConstraint("scope = 'events:ingest'", name="ingest_scope"),
        CheckConstraint("expires_at > created_at", name="expiry_after_creation"),
        CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="revocation_after_creation",
        ),
        CheckConstraint("created_at <= updated_at", name="timestamp_order"),
        {"schema": "operations"},
    )

    producer_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    token_prefix: Mapped[str] = mapped_column(String(64), nullable=False)
    token_verifier: Mapped[str] = mapped_column(String(512), nullable=False)
    scope: Mapped[str] = mapped_column(String(64), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SchemaVersionRow(Base):
    __tablename__ = "schema_versions"
    __table_args__ = (
        CheckConstraint("schema_sha256 ~ '^[0-9a-f]{64}$'", name="schema_sha256_lower_hex"),
        {"schema": "operations"},
    )

    schema_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(64), primary_key=True)
    schema_document: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    schema_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    upcaster: Mapped[str | None] = mapped_column(String(512), nullable=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RetentionPolicyRow(Base):
    __tablename__ = "retention_policies"
    __table_args__ = (
        UniqueConstraint("resource_class", "effective_from"),
        CheckConstraint(
            "retention_days IS NULL OR retention_days > 0",
            name="positive_retention_days",
        ),
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="effective_interval",
        ),
        {"schema": "operations"},
    )

    retention_policy_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True
    )
    resource_class: Mapped[str] = mapped_column(String(255), nullable=False)
    retention_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    preserve_metadata: Mapped[bool] = mapped_column(Boolean, nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
