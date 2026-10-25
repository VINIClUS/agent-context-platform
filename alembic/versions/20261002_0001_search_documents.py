"""Add the retrieval schema: lexical search documents and purge tombstones.

``retrieval.search_documents`` holds, per sanitized content object, its scope, provenance and a
``simple``-configuration ``tsvector`` with a GIN index, plus the sanitized content's digest and media
type, so a producer-chosen ``content_id`` reused for different bytes is refused instead of
overwritten. It never stores the text. The projector
role writes it (and deletes rows on purge); the API role only reads it. ``retrieval.content_tombstones``
records purged content IDs (insert-only for the projector) so a redelivered original event cannot
re-index purged content. ``retrieval.reset_search_projection()`` (SECURITY DEFINER) is the only reset:
it empties the documents and reseeds the tombstones from the ledger in one call.

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


# The ONLY way to empty the search projection (an in-place rebuild, before it replays the ledger).
# SECURITY DEFINER so the projector role needs no TRUNCATE or tombstone DELETE: the tombstones are
# reseeded from every `content.purged` the ledger records (whatever its outbox status) in the same
# statement that drops the stale ones, so purge knowledge can never be emptied without the reseed.
# Locks follow the projector's purge order (tombstones, then documents) under a lock timeout; the
# ledger scans run before the documents table is locked.
RESET_FUNCTION = """
CREATE FUNCTION retrieval.reset_search_projection() RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '10s'
AS $$
DECLARE
    remaining bigint;
BEGIN
    INSERT INTO retrieval.content_tombstones (content_id, purged_event_id, purged_at)
    SELECT DISTINCT ON (item ->> 'content_id')
        item ->> 'content_id', e.event_id, CAST(e.payload ->> 'purged_at' AS timestamptz)
    FROM ledger.events e, jsonb_array_elements(e.payload -> 'purged_contents') AS item
    WHERE e.event_type = 'content.purged'
    ORDER BY item ->> 'content_id', e.recorded_at, e.event_id
    ON CONFLICT (content_id) DO NOTHING;
    DELETE FROM retrieval.content_tombstones t
    WHERE t.content_id NOT IN (
        SELECT item ->> 'content_id'
        FROM ledger.events e, jsonb_array_elements(e.payload -> 'purged_contents') AS item
        WHERE e.event_type = 'content.purged'
    );
    TRUNCATE retrieval.search_documents;
    SELECT count(*) INTO remaining FROM retrieval.content_tombstones;
    RETURN remaining;
END
$$
"""


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
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("media_type", sa.String(255), nullable=False),
        sa.Column("tsv", postgresql.TSVECTOR(), nullable=False),
        sa.Column("indexed_at", timestamp, server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "cardinality(source_event_ids) > 0",
            name="ck_search_documents_source_event_ids_not_empty",
        ),
        sa.CheckConstraint(
            "event_id = ANY(source_event_ids)", name="ck_search_documents_event_id_in_sources"
        ),
        sa.CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$'", name="ck_search_documents_content_sha256_hex"
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
        "GRANT SELECT, INSERT, DELETE ON retrieval.search_documents TO agent_context_projector"
    )
    op.execute(
        "GRANT UPDATE (event_id, source_event_ids, event_type, session_id, occurred_at, "
        "redaction, tsv, indexed_at) ON retrieval.search_documents TO agent_context_projector"
    )
    # Insert-only for the projector role: it can never remove a tombstone itself.
    op.execute("GRANT SELECT, INSERT ON retrieval.content_tombstones TO agent_context_projector")
    op.execute(RESET_FUNCTION)
    op.execute("REVOKE ALL ON FUNCTION retrieval.reset_search_projection() FROM PUBLIC")
    op.execute(
        "GRANT EXECUTE ON FUNCTION retrieval.reset_search_projection() TO agent_context_projector"
    )


def downgrade() -> None:
    # Dropping the schema drops its tables and their grants; the roles belong to the initial
    # revision. Rebuildable projection data, so nothing is archived.
    op.execute("DROP SCHEMA retrieval CASCADE")
