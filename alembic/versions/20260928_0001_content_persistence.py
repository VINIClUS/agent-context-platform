"""Add catalog.inline_contents and bind event_content_refs storage FKs.

Revision ID: 20260928_0001
Revises: 20260823_0001
Create Date: 2026-09-28
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260928_0001"
down_revision: str | None = "20260823_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INLINE_MAX_BYTES = 65_536


def upgrade() -> None:
    op.create_table(
        "inline_contents",
        sa.Column("inline_id", sa.String(512), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("media_type", sa.String(255), nullable=False),
        sa.Column("uncompressed_bytes", sa.BigInteger(), nullable=False),
        sa.Column("data", sa.LargeBinary(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_inline_contents_content_sha256_format",
        ),
        sa.CheckConstraint(
            "inline_id = content_sha256",
            name="ck_inline_contents_inline_id_matches_digest",
        ),
        sa.CheckConstraint(
            f"uncompressed_bytes >= 0 AND uncompressed_bytes <= {INLINE_MAX_BYTES}",
            name="ck_inline_contents_uncompressed_bytes_bound",
        ),
        sa.CheckConstraint(
            "octet_length(data) = uncompressed_bytes",
            name="ck_inline_contents_data_length_matches",
        ),
        sa.PrimaryKeyConstraint("inline_id", name="pk_inline_contents"),
        sa.UniqueConstraint("content_sha256", name="uq_inline_contents_content_sha256"),
        schema="catalog",
    )

    op.create_foreign_key(
        "fk_event_content_refs_inline_id_inline_contents",
        "event_content_refs",
        "inline_contents",
        ["inline_id"],
        ["inline_id"],
        source_schema="ledger",
        referent_schema="catalog",
    )
    op.create_foreign_key(
        "fk_event_content_refs_object_key_content_objects",
        "event_content_refs",
        "content_objects",
        ["object_key"],
        ["object_key"],
        source_schema="ledger",
        referent_schema="catalog",
    )

    # catalog schema USAGE was granted only to agent_context_api by the
    # initial migration. The projector now needs to read inline_contents
    # (to hydrate sanitized inline bytes when projecting), so it needs
    # USAGE on the schema too -- schema USAGE is a prerequisite for any
    # object-level grant inside it to have effect.
    op.execute("GRANT USAGE ON SCHEMA catalog TO agent_context_projector")
    op.execute("GRANT SELECT, INSERT ON catalog.inline_contents TO agent_context_api")
    op.execute("GRANT SELECT ON catalog.inline_contents TO agent_context_projector")


def downgrade() -> None:
    op.execute("REVOKE SELECT ON catalog.inline_contents FROM agent_context_projector")
    op.execute("REVOKE SELECT, INSERT ON catalog.inline_contents FROM agent_context_api")
    op.execute("REVOKE USAGE ON SCHEMA catalog FROM agent_context_projector")

    op.drop_constraint(
        "fk_event_content_refs_object_key_content_objects",
        "event_content_refs",
        schema="ledger",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_event_content_refs_inline_id_inline_contents",
        "event_content_refs",
        schema="ledger",
        type_="foreignkey",
    )

    op.drop_table("inline_contents", schema="catalog")
