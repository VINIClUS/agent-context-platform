"""Add catalog.inline_contents and bind event_content_refs storage FKs.

Upgrade strategy for pre-existing data: refuse, do not backfill.

Before this revision nothing guaranteed that ``ledger.event_content_refs``
pointed at stored content: ``catalog.inline_contents`` did not exist, so a
legacy inline ref's bytes were never persisted anywhere and cannot be
reconstructed; likewise a legacy object ref may have no
``catalog.content_objects`` row. Adding the new foreign keys over such rows
would fail mid-migration with an opaque constraint error, and adding them
``NOT VALID`` would ship a schema that claims "no reference may dangle" while
known dangling references exist. Instead ``upgrade()`` first takes a
``SHARE ROW EXCLUSIVE`` lock on the refs table (so no legacy-shaped row can
appear between the check and the constraints), counts refs that have no stored
content, and, if there are any, raises a content-free error reporting only the
counts. The transaction rolls back and the database is left untouched. Operator
remediation: resolve those events first (purge or quarantine them, or re-ingest
their content through ``ContentService`` so the rows exist), then re-run.

Refs to objects must also agree on the digest, not just the key. A legacy
``content_objects`` row may carry the referenced ``object_key`` under a different
digest; ``OrphanSweeper`` looks rows up by the digest embedded in the key, would
find none, and could delete an object a committed ref still needs. The
precondition therefore also counts refs whose digest differs from their object
row's, and ``content_objects`` rows whose key is not the canonical key for their
own digest. Enforcement afterwards is a CHECK on ``catalog.content_objects``
(``ck_content_objects_object_key_matches_digest``, the same key format as
``object_key_matches_digest`` on the refs) rather than a composite FK: the refs'
CHECK ties ref key to ref digest, this CHECK ties object key to object digest,
and the existing key FK joins the two, so ref digest = object digest follows
transitively. A composite ``(object_key, content_sha256)`` FK would add a
redundant unique index for no extra guarantee.

The inline digest binding (``ck_event_content_refs_inline_id_matches_digest``)
is added with the same revision: ``inline_contents`` already enforces
``inline_id = content_sha256``, so requiring the same equality on the ref
makes the inline FK imply that the referenced row holds the ref's digest.

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
OBJECT_KEY_MATCHES = (
    "object_key = 'sha256/' || substr(content_sha256, 1, 2) || '/' || "
    "substr(content_sha256, 3, 2) || '/' || content_sha256 || '.zst'"
)


def _refuse_legacy_refs_without_stored_content() -> None:
    op.execute("LOCK TABLE ledger.event_content_refs IN SHARE ROW EXCLUSIVE MODE")
    op.execute("LOCK TABLE catalog.content_objects IN SHARE ROW EXCLUSIVE MODE")
    connection = op.get_bind()
    inline_refs = connection.scalar(
        sa.text("SELECT count(*) FROM ledger.event_content_refs WHERE storage = 'inline'")
    )
    unbacked_object_refs = connection.scalar(
        sa.text(
            "SELECT count(*) FROM ledger.event_content_refs r WHERE r.storage = 'object' "
            "AND NOT EXISTS (SELECT 1 FROM catalog.content_objects o "
            "WHERE o.object_key = r.object_key)"
        )
    )
    mismatched_object_refs = connection.scalar(
        sa.text(
            "SELECT count(*) FROM ledger.event_content_refs r "
            "JOIN catalog.content_objects o ON o.object_key = r.object_key "
            "WHERE r.storage = 'object' AND r.content_sha256 <> o.content_sha256"
        )
    )
    noncanonical_objects = connection.scalar(
        sa.text(f"SELECT count(*) FROM catalog.content_objects WHERE NOT ({OBJECT_KEY_MATCHES})")
    )
    if inline_refs or unbacked_object_refs or mismatched_object_refs or noncanonical_objects:
        raise RuntimeError(
            "cannot add content foreign keys: ledger.event_content_refs holds legacy refs "
            f"whose content was never stored (inline={inline_refs}, "
            f"object={unbacked_object_refs}, object_digest={mismatched_object_refs}, "
            f"content_objects_key={noncanonical_objects}). Their bytes cannot be backfilled "
            "and object rows must carry the digest their key encodes. "
            "Remediation: purge or quarantine those events, or re-ingest their content "
            "through the content service, then re-run this migration."
        )


def upgrade() -> None:
    _refuse_legacy_refs_without_stored_content()
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

    op.create_check_constraint(
        "ck_content_objects_object_key_matches_digest",
        "content_objects",
        OBJECT_KEY_MATCHES,
        schema="catalog",
    )
    op.create_check_constraint(
        "ck_event_content_refs_inline_id_matches_digest",
        "event_content_refs",
        "storage <> 'inline' OR inline_id = content_sha256",
        schema="ledger",
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
        "ck_content_objects_object_key_matches_digest",
        "content_objects",
        schema="catalog",
        type_="check",
    )
    op.drop_constraint(
        "ck_event_content_refs_inline_id_matches_digest",
        "event_content_refs",
        schema="ledger",
        type_="check",
    )
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
