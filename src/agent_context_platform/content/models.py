"""SQLAlchemy ORM model for sanitized content stored inline in Postgres."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    LargeBinary,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from agent_context_platform.catalog.models import CatalogBase

#: Inline storage holds sanitized bytes no larger than this boundary.
#: Anything larger is persisted through the object path
#: (``catalog.content_objects`` / ``BlobStore``) instead.
INLINE_MAX_BYTES = 65_536


class InlineContentRow(CatalogBase):
    """Sanitized content bytes stored inline in Postgres.

    ``inline_id`` is always the same value as ``content_sha256``. The two
    columns are kept distinct (rather than collapsing to a single column)
    only so that ``ledger.event_content_refs.inline_id`` can carry a foreign
    key shaped like its ``object_key`` sibling, which references
    ``catalog.content_objects.object_key``. The ``inline_id_matches_digest``
    check constraint enforces the equality at the database level.

    ``media_type`` on this row is first-writer metadata only: it is whatever
    media type was attached the first time this digest was persisted inline.
    It is not authoritative for any individual reference. Each
    ``ledger.event_content_refs`` row records its own ``media_type``, and a
    second reference to the same digest under a different media type is not
    an error -- see ``ContentService.attach``.

    This table is append-only. Rows are inserted with
    ``ON CONFLICT (content_sha256) DO NOTHING`` and are never updated. There
    is intentionally no UPDATE or DELETE grant for either platform role.
    """

    __tablename__ = "inline_contents"
    __table_args__ = (
        UniqueConstraint("content_sha256"),
        CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'", name="content_sha256_format"),
        CheckConstraint("inline_id = content_sha256", name="inline_id_matches_digest"),
        CheckConstraint(
            f"uncompressed_bytes >= 0 AND uncompressed_bytes <= {INLINE_MAX_BYTES}",
            name="uncompressed_bytes_bound",
        ),
        CheckConstraint("octet_length(data) = uncompressed_bytes", name="data_length_matches"),
    )

    inline_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    media_type: Mapped[str] = mapped_column(String(255), nullable=False)
    uncompressed_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
