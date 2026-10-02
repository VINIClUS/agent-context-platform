"""Add the retrieval schema: lexical search documents and purge tombstones.

``retrieval.search_documents`` holds, per sanitized content object, its scope, provenance and a
``simple``-configuration ``tsvector`` with a GIN index. It never stores the text. The projector
role writes it (and deletes rows on purge); the API role only reads it. ``retrieval.content_tombstones``
records purged content IDs (insert-only for the projector) so a redelivered original event cannot
re-index purged content.

Revision ID: 20261002_0001
Revises: 20260929_0001
Create Date: 2026-10-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20261002_0001"
down_revision: str | None = "20260929_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    timestamp = sa.DateTime(timezone=True)
    uuid = postgresql.UUID(as_uuid=True)
    op.execute("CREATE SCHEMA retrieval")
    op.create_table(
        "search_documents",
        sa.Column("content_id", sa.String(512), nullable=False),
        sa.Column("event_id", uuid, nullable=False),
        sa.Column("source_event_ids", postgresql.ARRAY(uuid), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("project_id", sa.Text(), nullable=False),
        sa.Column("repository_id", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column("occurred_at", timestamp, nullable=False),
        sa.Column("redaction", sa.String(25), nullable=False),
        sa.Column("tsv", postgresql.TSVECTOR(), nullable=False),
        sa.Column("indexed_at", timestamp, server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "cardinality(source_event_ids) > 0",
            name="ck_search_documents_source_event_ids_not_empty",
        ),
        sa.CheckConstraint(
            "event_id = ANY(source_event_ids)", name="ck_search_documents_event_id_in_sources"
        ),
        sa.PrimaryKeyConstraint("content_id", name="pk_search_documents"),
        schema="retrieval",
    )
    op.create_index(
        "ix_search_documents_tsv",
        "search_documents",
        ["tsv"],
        schema="retrieval",
        postgresql_using="gin",
    )
    op.create_index(
        "ix_search_documents_scope",
        "search_documents",
        ["project_id", "repository_id"],
        schema="retrieval",
    )
    op.create_table(
        "content_tombstones",
        sa.Column("content_id", sa.String(512), nullable=False),
        sa.Column("purged_event_id", uuid, nullable=False),
        sa.Column("purged_at", timestamp, nullable=False),
        sa.PrimaryKeyConstraint("content_id", name="pk_content_tombstones"),
        schema="retrieval",
    )
    op.execute("GRANT USAGE ON SCHEMA retrieval TO agent_context_api, agent_context_projector")
    op.execute("GRANT SELECT ON retrieval.search_documents TO agent_context_api")
    op.execute(
        "GRANT SELECT, INSERT, DELETE, TRUNCATE ON retrieval.search_documents "
        "TO agent_context_projector"
    )
    op.execute(
        "GRANT UPDATE (event_id, source_event_ids, event_type, session_id, occurred_at, "
        "redaction, tsv, indexed_at) ON retrieval.search_documents TO agent_context_projector"
    )
    # TRUNCATE: an in-place rebuild resets both projection tables before replaying the ledger.
    op.execute(
        "GRANT SELECT, INSERT, TRUNCATE ON retrieval.content_tombstones TO agent_context_projector"
    )


def downgrade() -> None:
    # Dropping the schema drops its tables and their grants; the roles belong to the initial
    # revision. Rebuildable projection data, so nothing is archived.
    op.execute("DROP SCHEMA retrieval CASCADE")
