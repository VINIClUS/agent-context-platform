"""Add operations.mcp_tokens for read-only MCP bearer authentication.

MCP ``memory:read`` credentials live in their own table, apart from
``operations.registered_producers``, and their non-secret prefix must start
with ``mcp_``. Together these make an ingestion token unusable on ``/mcp``
and an MCP token unusable on the ingestion plane. Tokens are inserted by an
operator (the mint command); ``agent_context_api`` may only read them, so a
compromised API process cannot mint or un-revoke a credential. Later scopes
widen the ``scopes`` CHECK in their own migration.

Revision ID: 20260929_0001
Revises: 20260928_0001
Create Date: 2026-09-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260929_0001"
down_revision: str | None = "20260928_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    timestamp = sa.DateTime(timezone=True)
    op.create_table(
        "mcp_tokens",
        sa.Column("token_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("token_prefix", sa.String(64), nullable=False),
        sa.Column("token_verifier", sa.String(512), nullable=False),
        sa.Column("principal", sa.String(255), nullable=False),
        sa.Column("scopes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("created_at", timestamp, nullable=False),
        sa.Column("expires_at", timestamp, nullable=False),
        sa.Column("revoked_at", timestamp, nullable=True),
        sa.CheckConstraint("token_prefix LIKE 'mcp\\_%'", name="ck_mcp_tokens_mcp_prefix"),
        sa.CheckConstraint(
            "token_verifier LIKE '$argon2id$v=19$%'", name="ck_mcp_tokens_argon2id_verifier"
        ),
        sa.CheckConstraint(
            "cardinality(scopes) > 0 AND scopes <@ ARRAY['memory:read']::text[]",
            name="ck_mcp_tokens_memory_read_scopes",
        ),
        sa.CheckConstraint("expires_at > created_at", name="ck_mcp_tokens_expiry_after_creation"),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="ck_mcp_tokens_revocation_after_creation",
        ),
        sa.PrimaryKeyConstraint("token_id", name="pk_mcp_tokens"),
        sa.UniqueConstraint("token_prefix", name="uq_mcp_tokens_token_prefix"),
        schema="operations",
    )
    op.execute("GRANT SELECT ON operations.mcp_tokens TO agent_context_api")


def downgrade() -> None:
    op.execute("REVOKE SELECT ON operations.mcp_tokens FROM agent_context_api")
    op.drop_table("mcp_tokens", schema="operations")
