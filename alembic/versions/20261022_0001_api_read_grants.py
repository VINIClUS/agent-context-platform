"""Let the API role read the schema head and projection progress.

Readiness (``GET /health/ready``) compares ``public.alembic_version`` with the image's head, and
retrieval freshness reads ``projection.projection_checkpoints`` and ``projection.dead_letters``,
both as ``agent_context_api``. Until now only the projector role could read the two projection
tables and nobody but the owner could read the version table, so a least-privileged API replica
could never become ready. The grant is ``SELECT`` only; writes stay with the projector role.

Revision ID: 20261022_0001
Revises: 20261002_0001
Create Date: 2026-10-22
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "20261022_0001"
down_revision: str | None = "20261002_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

API_READ_TABLES = (
    "public.alembic_version",
    "projection.projection_checkpoints",
    "projection.dead_letters",
)


def upgrade() -> None:
    op.execute(f"GRANT SELECT ON {', '.join(API_READ_TABLES)} TO agent_context_api")


def downgrade() -> None:
    op.execute(f"REVOKE SELECT ON {', '.join(API_READ_TABLES)} FROM agent_context_api")
