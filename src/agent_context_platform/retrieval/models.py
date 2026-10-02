"""PostgreSQL lexical search material (PLATFORM-042): the `retrieval` schema.

`search_documents` holds one row per sanitized content object: its scope, provenance and a
`tsvector`. The text itself is NOT stored here (the content service owns it; a snippet is fetched
through it later). The rows are a projection: rebuildable from the ledger, written only by the
search projector. `content_tombstones` remembers purged content IDs so a redelivered original
event can never index purged content again.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID as PythonUUID

from sqlalchemy import CheckConstraint, DateTime, Index, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, TSVECTOR, UUID
from sqlalchemy.orm import Mapped, mapped_column

from agent_context_platform.db import Base

#: Text-search configuration. `simple` (no stemming, no stop words) because the content is
#: multilingual (Portuguese and English); `unaccent` is deliberately not used.
TEXT_SEARCH_CONFIG = "simple"


class SearchDocumentRow(Base):
    __tablename__ = "search_documents"
    __table_args__ = (
        CheckConstraint("cardinality(source_event_ids) > 0", name="source_event_ids_not_empty"),
        CheckConstraint("event_id = ANY(source_event_ids)", name="event_id_in_sources"),
        Index("ix_search_documents_tsv", "tsv", postgresql_using="gin"),
        Index("ix_search_documents_scope", "project_id", "repository_id"),
        {"schema": "retrieval"},
    )

    content_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    # The earliest `(occurred_at, event_id)` event that referenced the content: its metadata wins,
    # so the row does not depend on delivery order. `source_event_ids` is every referencing event.
    event_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_event_ids: Mapped[list[PythonUUID]] = mapped_column(
        ARRAY(UUID(as_uuid=True)), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    project_id: Mapped[str] = mapped_column(Text, nullable=False)
    repository_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    redaction: Mapped[str] = mapped_column(String(25), nullable=False)
    tsv: Mapped[str] = mapped_column(TSVECTOR, nullable=False)
    indexed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ContentTombstoneRow(Base):
    __tablename__ = "content_tombstones"
    __table_args__ = ({"schema": "retrieval"},)

    content_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    purged_event_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    purged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
