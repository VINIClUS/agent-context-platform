from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID as PythonUUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from agent_context_platform.db import Base


def _enum_values(enum_type: type[StrEnum]) -> list[str]:
    return [member.value for member in enum_type]


class OutboxStatus(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    DELIVERED = "delivered"
    DEAD_LETTERED = "dead_lettered"


class ProjectionState(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    REBUILDING = "rebuilding"


class DeadLetterStatus(StrEnum):
    OPEN = "open"
    REQUEUED = "requeued"
    RESOLVED = "resolved"


class IngestionBatchStatus(StrEnum):
    PROCESSING = "processing"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class OutboxRow(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint("event_id"),
        Index(None, "status", "available_at", "outbox_id"),
        CheckConstraint("retry_count >= 0", name="non_negative_retry_count"),
        CheckConstraint(
            "(status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="lease_matches_status",
        ),
        CheckConstraint(
            "(status = 'delivered' AND delivered_at IS NOT NULL AND dead_lettered_at IS NULL) "
            "OR (status = 'dead_lettered' AND delivered_at IS NULL "
            "AND dead_lettered_at IS NOT NULL) OR (status IN ('pending', 'leased') "
            "AND delivered_at IS NULL AND dead_lettered_at IS NULL)",
            name="terminal_timestamp_matches_status",
        ),
        CheckConstraint(
            "created_at <= updated_at AND created_at <= available_at",
            name="timestamp_order",
        ),
        CheckConstraint(
            "last_error_class IS NULL OR last_error_class ~ '^[A-Za-z][A-Za-z0-9_.]{0,127}$'",
            name="public_error_class",
        ),
        {"schema": "projection"},
    )

    outbox_id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ledger.events.event_id"), nullable=False
    )
    status: Mapped[OutboxStatus] = mapped_column(
        Enum(
            OutboxStatus,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="outbox_status",
        ),
        nullable=False,
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error_class: Mapped[str | None] = mapped_column(String(128), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dead_lettered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ProjectionCheckpointRow(Base):
    __tablename__ = "projection_checkpoints"
    __table_args__ = (
        CheckConstraint("processed_count >= 0", name="non_negative_processed_count"),
        CheckConstraint(
            "(processed_count = 0 AND last_outbox_id IS NULL AND last_event_id IS NULL) OR "
            "(processed_count > 0 AND last_outbox_id IS NOT NULL AND last_event_id IS NOT NULL)",
            name="progress_is_complete_pair",
        ),
        {"schema": "projection"},
    )

    projector_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    projector_version: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_outbox_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("projection.outbox.outbox_id"), nullable=True
    )
    last_event_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ledger.events.event_id"), nullable=True
    )
    processed_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[ProjectionState] = mapped_column(
        Enum(
            ProjectionState,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="projection_state",
        ),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DeadLetterRow(Base):
    __tablename__ = "dead_letters"
    __table_args__ = (
        CheckConstraint("attempts > 0", name="positive_attempts"),
        CheckConstraint(
            "(status = 'open' AND resolved_at IS NULL) OR "
            "(status IN ('requeued', 'resolved') AND resolved_at IS NOT NULL)",
            name="resolution_matches_status",
        ),
        CheckConstraint(
            "first_failed_at <= updated_at AND "
            "(resolved_at IS NULL OR first_failed_at <= resolved_at)",
            name="timestamp_order",
        ),
        CheckConstraint(
            "error_class ~ '^[A-Za-z][A-Za-z0-9_.]{0,127}$'",
            name="public_error_class",
        ),
        {"schema": "projection"},
    )

    outbox_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("projection.outbox.outbox_id"), primary_key=True
    )
    event_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ledger.events.event_id"), nullable=False
    )
    projector_name: Mapped[str] = mapped_column(String(255), nullable=False)
    projector_version: Mapped[str] = mapped_column(String(64), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    error_class: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[DeadLetterStatus] = mapped_column(
        Enum(
            DeadLetterStatus,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="dead_letter_status",
        ),
        nullable=False,
    )
    first_failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class IngestionBatchRow(Base):
    __tablename__ = "ingestion_batches"
    __table_args__ = (
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$'", name="request_sha256_lower_hex"),
        CheckConstraint("event_count BETWEEN 1 AND 500", name="event_count_bounds"),
        CheckConstraint(
            "accepted_count >= 0 AND rejected_count >= 0 "
            "AND accepted_count + rejected_count <= event_count",
            name="result_counts",
        ),
        CheckConstraint(
            "(status = 'processing' AND finished_at IS NULL AND error_code IS NULL "
            "AND accepted_count = 0 AND rejected_count = 0) OR "
            "(status = 'accepted' AND finished_at IS NOT NULL AND error_code IS NULL "
            "AND accepted_count = event_count AND rejected_count = 0) OR "
            "(status = 'rejected' AND finished_at IS NOT NULL AND error_code IS NOT NULL "
            "AND accepted_count + rejected_count = event_count AND rejected_count > 0)",
            name="status_cycle",
        ),
        CheckConstraint(
            "finished_at IS NULL OR started_at <= finished_at",
            name="timestamp_order",
        ),
        CheckConstraint(
            "error_code IS NULL OR error_code ~ '^[a-z][a-z0-9_]{0,127}$'",
            name="public_error_code",
        ),
        {"schema": "projection"},
    )

    batch_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    producer_id: Mapped[str] = mapped_column(String(255), nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[IngestionBatchStatus] = mapped_column(
        Enum(
            IngestionBatchStatus,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="ingestion_batch_status",
        ),
        nullable=False,
    )
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
