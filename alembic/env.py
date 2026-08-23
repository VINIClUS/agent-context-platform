from __future__ import annotations

import asyncio

from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.schema import SchemaItem

from agent_context_platform.catalog.models import CatalogBase
from agent_context_platform.db import Base, create_engine

# Importing the modules registers every table on Base.metadata.
from agent_context_platform.ledger import models as ledger_models  # noqa: F401
from agent_context_platform.operations import models as operations_models  # noqa: F401
from agent_context_platform.projection import models as projection_models  # noqa: F401
from agent_context_platform.settings import Settings
from alembic import context

MANAGED_TABLES = {
    "catalog.workspaces",
    "catalog.projects",
    "catalog.repositories",
    "catalog.project_repositories",
    "catalog.checkouts",
    "catalog.content_objects",
    "ledger.event_streams",
    "ledger.events",
    "ledger.event_content_refs",
    "ledger.redaction_reports",
    "ledger.integrity_checkpoints",
    "projection.outbox",
    "projection.projection_checkpoints",
    "projection.ingestion_batches",
    "projection.dead_letters",
    "operations.registered_producers",
    "operations.schema_versions",
    "operations.retention_policies",
}
MANAGED_SCHEMAS = {table.partition(".")[0] for table in MANAGED_TABLES}
TYPE_BOUND_CHECK_CONSTRAINTS = {
    "ck_event_streams_stream_status",
    "ck_event_content_refs_content_disposition",
    "ck_event_content_refs_content_storage",
    "ck_redaction_reports_redaction_disposition",
    "ck_redaction_reports_metadata_only_reason",
    "ck_outbox_outbox_status",
    "ck_projection_checkpoints_projection_state",
    "ck_ingestion_batches_ingestion_batch_status",
    "ck_dead_letters_dead_letter_status",
}
target_metadata = [CatalogBase.metadata, Base.metadata]

# The ORM relationship is intentionally circular; mark its head pointer as an ALTER
# constraint so Alembic can sort the tables the same way as the initial revision.
for foreign_key in Base.metadata.tables["ledger.event_streams"].foreign_key_constraints:
    if foreign_key.name == "fk_event_streams_last_event_id_events":
        foreign_key.use_alter = True


def include_name(
    name: str | None,
    type_: str,
    parent_names: dict[str, str | None],
) -> bool:
    """Limit reflection to the schemas and tables owned by this service."""
    if type_ == "schema":
        return name in MANAGED_SCHEMAS
    if type_ == "table":
        return parent_names.get("schema_qualified_table_name") in MANAGED_TABLES
    return True


def include_object(
    object_: SchemaItem,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: SchemaItem | None,
) -> bool:
    """Ignore reflected counterparts of SQLAlchemy's type-bound enum checks."""
    return not (type_ == "check_constraint" and name in TYPE_BOUND_CHECK_CONSTRAINTS)


def _configure(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_name=include_name,
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_offline() -> None:
    settings = Settings()
    dsn = settings.postgresql.dsn
    if dsn is None:
        raise ValueError("postgresql.dsn is required to run migrations")
    context.configure(
        url=dsn.get_secret_value().unicode_string(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_name=include_name,
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def _run_async(connection: AsyncConnection | None = None) -> None:
    if connection is not None:
        await connection.run_sync(_configure)
        return

    engine = create_engine(Settings())
    try:
        async with engine.connect() as owned_connection:
            await owned_connection.run_sync(_configure)
    finally:
        await engine.dispose()


def run_migrations_online() -> None:
    supplied_connection = context.config.attributes.get("connection")
    if isinstance(supplied_connection, Connection):
        _configure(supplied_connection)
    elif isinstance(supplied_connection, AsyncConnection):
        asyncio.run(_run_async(supplied_connection))
    elif supplied_connection is None:
        asyncio.run(_run_async())
    else:
        raise TypeError("Alembic connection must be a SQLAlchemy Connection")


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
