from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID as PythonUUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from agent_context_platform.db import Base


def _enum_values(enum_type: type[StrEnum]) -> list[str]:
    return [member.value for member in enum_type]


class StreamStatus(StrEnum):
    ACTIVE = "active"
    QUARANTINED = "quarantined"


class ContentDisposition(StrEnum):
    SANITIZED = "sanitized"
    METADATA_ONLY = "metadata_only"
    DROPPED_REDACTION_FAILURE = "dropped_redaction_failure"
    PURGED = "purged"


class ContentStorage(StrEnum):
    INLINE = "inline"
    OBJECT = "object"


class MetadataOnlyReason(StrEnum):
    SOURCE_UNMAPPED = "source_unmapped"
    SOURCE_INVALID = "source_invalid"
    SPOOL_PRESSURE = "spool_pressure"


class EventStreamRow(Base):
    __tablename__ = "event_streams"
    __table_args__ = (
        CheckConstraint("last_sequence >= 0", name="non_negative_last_sequence"),
        CheckConstraint(
            "(last_sequence = 0 AND last_event_id IS NULL AND last_event_sha256 IS NULL) OR "
            "(last_sequence > 0 AND last_event_id IS NOT NULL AND last_event_sha256 IS NOT NULL)",
            name="head_matches_sequence",
        ),
        CheckConstraint(
            "(status = 'active' AND quarantined_at IS NULL) OR "
            "(status = 'quarantined' AND quarantined_at IS NOT NULL)",
            name="quarantine_matches_status",
        ),
        CheckConstraint("created_at <= updated_at", name="timestamp_order"),
        CheckConstraint(
            "last_event_sha256 IS NULL OR last_event_sha256 ~ '^[0-9a-f]{64}$'",
            name="last_event_sha256_lower_hex",
        ),
        {"schema": "ledger"},
    )

    stream_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_event_id: Mapped[PythonUUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ledger.events.event_id"), nullable=True
    )
    last_event_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[StreamStatus] = mapped_column(
        Enum(
            StreamStatus,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="stream_status",
        ),
        nullable=False,
    )
    quarantined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class EventRow(Base):
    __tablename__ = "events"
    __table_args__ = (
        UniqueConstraint("producer_id", "idempotency_key"),
        UniqueConstraint("stream_id", "stream_sequence"),
        CheckConstraint("stream_sequence > 0", name="positive_stream_sequence"),
        CheckConstraint("payload_sha256 ~ '^[0-9a-f]{64}$'", name="payload_sha256_lower_hex"),
        CheckConstraint("event_sha256 ~ '^[0-9a-f]{64}$'", name="event_sha256_lower_hex"),
        CheckConstraint(
            "(stream_sequence = 1 AND previous_event_sha256 IS NULL) OR "
            "(stream_sequence > 1 AND previous_event_sha256 ~ '^[0-9a-f]{64}$')",
            name="previous_hash_matches_sequence",
        ),
        {"schema": "ledger"},
    )

    event_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    stream_id: Mapped[str] = mapped_column(
        String(512), ForeignKey("ledger.event_streams.stream_id"), nullable=False
    )
    stream_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    producer_id: Mapped[str] = mapped_column(String(255), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(512), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    producer: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    context: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    trace: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    redaction: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_event_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_sha256: Mapped[str] = mapped_column(String(64), nullable=False)


class EventContentRefRow(Base):
    __tablename__ = "event_content_refs"
    __table_args__ = (
        CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'", name="content_sha256_lower_hex"),
        CheckConstraint("uncompressed_bytes >= 0", name="non_negative_uncompressed_bytes"),
        CheckConstraint(
            "(storage = 'inline' AND inline_id IS NOT NULL AND object_key IS NULL "
            "AND encoding IS NULL) OR (storage = 'object' AND inline_id IS NULL "
            "AND object_key IS NOT NULL AND encoding = 'zstd')",
            name="storage_form",
        ),
        CheckConstraint(
            "storage <> 'object' OR object_key = "
            "'sha256/' || substr(content_sha256, 1, 2) || '/' || "
            "substr(content_sha256, 3, 2) || '/' || content_sha256 || '.zst'",
            name="object_key_matches_digest",
        ),
        {"schema": "ledger"},
    )

    event_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ledger.events.event_id"), primary_key=True
    )
    content_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    media_type: Mapped[str] = mapped_column(String(255), nullable=False)
    uncompressed_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    disposition: Mapped[ContentDisposition] = mapped_column(
        Enum(
            ContentDisposition,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="content_disposition",
        ),
        nullable=False,
    )
    storage: Mapped[ContentStorage] = mapped_column(
        Enum(
            ContentStorage,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="content_storage",
        ),
        nullable=False,
    )
    inline_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    object_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    encoding: Mapped[str | None] = mapped_column(String(16), nullable=True)


class RedactionReportRow(Base):
    __tablename__ = "redaction_reports"
    __table_args__ = (
        ForeignKeyConstraint(
            ("event_id", "content_id"),
            ("ledger.event_content_refs.event_id", "ledger.event_content_refs.content_id"),
        ),
        CheckConstraint(
            "(disposition = 'metadata_only' AND metadata_only_reason IS NOT NULL "
            "AND error_class IS NULL AND failed_detector IS NULL "
            "AND failed_detector_version IS NULL) OR "
            "(disposition = 'dropped_redaction_failure' AND metadata_only_reason IS NULL "
            "AND error_class IS NOT NULL AND failed_detector IS NOT NULL "
            "AND failed_detector_version IS NOT NULL) OR "
            "(disposition IN ('sanitized', 'purged') AND metadata_only_reason IS NULL "
            "AND error_class IS NULL AND failed_detector IS NULL "
            "AND failed_detector_version IS NULL)",
            name="disposition_details",
        ),
        CheckConstraint(
            "(failed_detector IS NULL AND failed_detector_version IS NULL) OR "
            "(failed_detector IS NOT NULL AND failed_detector_version IS NOT NULL)",
            name="failure_detector_pair",
        ),
        {"schema": "ledger"},
    )

    event_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    content_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    disposition: Mapped[ContentDisposition] = mapped_column(
        Enum(
            ContentDisposition,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="redaction_disposition",
        ),
        nullable=False,
    )
    findings: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    metadata_only_reason: Mapped[MetadataOnlyReason | None] = mapped_column(
        Enum(
            MetadataOnlyReason,
            values_callable=_enum_values,
            native_enum=False,
            create_constraint=True,
            name="metadata_only_reason",
        ),
        nullable=True,
    )
    error_class: Mapped[str | None] = mapped_column(String(128), nullable=True)
    failed_detector: Mapped[str | None] = mapped_column(String(255), nullable=True)
    failed_detector_version: Mapped[str | None] = mapped_column(String(64), nullable=True)


class IntegrityCheckpointRow(Base):
    __tablename__ = "integrity_checkpoints"
    __table_args__ = (
        CheckConstraint("heads_sha256 ~ '^[0-9a-f]{64}$'", name="heads_sha256_lower_hex"),
        CheckConstraint("stream_count >= 0", name="non_negative_stream_count"),
        CheckConstraint("signature_algorithm = 'ed25519'", name="signature_algorithm"),
        {"schema": "ledger"},
    )

    checkpoint_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    stream_heads: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    heads_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    signature_algorithm: Mapped[str] = mapped_column(String(32), nullable=False)
    signing_key_id: Mapped[str] = mapped_column(String(255), nullable=False)
    signature: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    stream_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
