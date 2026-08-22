from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from agent_context_platform.catalog.models import (
    CatalogBase,
    CheckoutRow,
    ContentObjectRow,
    ProjectRepositoryRow,
    ProjectRow,
    RepositoryRow,
    WorkspaceRow,
)
from agent_context_platform.catalog.repository import (
    CatalogRepository,
    RemoteIdentityError,
    normalize_remote,
)

pytestmark = pytest.mark.unit


def test_catalog_metadata_defines_the_six_schema_qualified_tables() -> None:
    assert set(CatalogBase.metadata.tables) == {
        "catalog.workspaces",
        "catalog.projects",
        "catalog.repositories",
        "catalog.project_repositories",
        "catalog.checkouts",
        "catalog.content_objects",
    }


@pytest.mark.parametrize(
    "row_type",
    [
        WorkspaceRow,
        ProjectRow,
        RepositoryRow,
        ProjectRepositoryRow,
        CheckoutRow,
        ContentObjectRow,
    ],
)
def test_catalog_rows_have_native_uuid_identity_and_utc_timestamps(
    row_type: type[CatalogBase],
) -> None:
    table = row_type.__table__

    assert table.c.id.type.python_type is UUID
    assert str(table.c.id.server_default.arg) == "uuidv7()"
    assert table.c.created_at.type.python_type is datetime
    assert table.c.created_at.type.timezone is True
    assert table.c.observed_at.type.python_type is datetime
    assert table.c.observed_at.type.timezone is True


def test_catalog_constraints_encode_domain_identity_and_content_invariants() -> None:
    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for table in CatalogBase.metadata.tables.values()
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    checks = {
        constraint.name
        for table in CatalogBase.metadata.tables.values()
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert ("workspace_id", "name") in unique_columns
    assert ("remote_host", "remote_owner", "remote_path") in unique_columns
    assert ("project_id", "repository_id") in unique_columns
    assert ("repository_id", "filesystem_root") in unique_columns
    assert ("content_sha256",) in unique_columns
    assert ("object_key",) in unique_columns
    assert checks == {
        "ck_content_objects_content_sha256_format",
        "ck_content_objects_object_key_not_empty",
        "ck_content_objects_compressed_bytes_nonnegative",
        "ck_content_objects_uncompressed_bytes_nonnegative",
    }


def test_foreign_keys_are_restrictive_and_relationships_do_not_delete_dependents() -> None:
    foreign_keys = [
        constraint
        for table in CatalogBase.metadata.tables.values()
        for constraint in table.constraints
        if isinstance(constraint, ForeignKeyConstraint)
    ]

    assert foreign_keys
    assert {constraint.ondelete for constraint in foreign_keys} == {"RESTRICT"}

    relationships = [
        relationship
        for mapper in CatalogBase.registry.mappers
        for relationship in mapper.relationships
    ]
    assert relationships
    assert all(relationship.passive_deletes == "all" for relationship in relationships)
    assert all("delete" not in relationship.cascade for relationship in relationships)
    assert all(relationship.secondary is None for relationship in relationships)


@pytest.mark.parametrize(
    ("remote_url", "expected_host", "expected_owner", "expected_path"),
    [
        ("https://GitHub.com/OpenAI/codex.git", "github.com", "OpenAI", "codex"),
        ("https://github.com:443/OpenAI/codex/", "github.com", "OpenAI", "codex"),
        ("ssh://git@GitHub.com/OpenAI/codex.git", "github.com", "OpenAI", "codex"),
        ("ssh://git@github.com:22/OpenAI/codex", "github.com", "OpenAI", "codex"),
        ("git@GitHub.com:OpenAI/codex.git", "github.com", "OpenAI", "codex"),
        ("https://github.com/OpenAI/%63odex.git", "github.com", "OpenAI", "codex"),
        (
            "ssh://git@example.com:2222/team/tools/repo.git",
            "example.com:2222",
            "team",
            "tools/repo",
        ),
        ("ssh://git@[2001:db8::1]:2222/team/repo.git", "[2001:db8::1]:2222", "team", "repo"),
    ],
)
def test_normalize_remote_accepts_network_git_remote_forms(
    remote_url: str,
    expected_host: str,
    expected_owner: str,
    expected_path: str,
) -> None:
    normalized = normalize_remote(remote_url)

    assert normalized.remote_host == expected_host
    assert normalized.remote_owner == expected_owner
    assert normalized.remote_path == expected_path


@pytest.mark.parametrize(
    "remote_url",
    [
        "",
        "/srv/repository.git",
        "../repository",
        "file:///srv/repository.git",
        "git://github.com/OpenAI/codex.git",
        "https://user:secret@github.com/OpenAI/codex.git",
        "https://github.com/OpenAI/codex.git?ref=main",
        "ssh://git@github.com/OpenAI/codex.git#main",
        "git@github.com:OpenAI/codex.git?ref=main",
        "git@github.com:OpenAI/codex.git#main",
        "ssh://git:secret@github.com/OpenAI/codex.git",
        "https://github.com:invalid/OpenAI/codex.git",
        "https:///OpenAI/codex.git",
        "https://./OpenAI/codex.git",
        "https://[::1/OpenAI/codex.git",
        "https://\udcff/OpenAI/codex.git",
        "https://bad_host/OpenAI/codex.git",
        "https://-bad.example/OpenAI/codex.git",
        "https://github.com/repository.git",
        "git@github.com:repository.git",
        "git@.:OpenAI/codex.git",
        "git@github.com:OpenAI/../codex.git",
        "git@github.com:OpenAI//codex.git",
        r"git@github.com:OpenAI\codex.git",
        "git@github.com:OpenAI/%2e%2e/codex.git",
        "git@github.com:OpenAI%2Fteam/codex.git",
        "git@github.com:OpenAI/%zzcodex.git",
        "git@2001:db8::1:OpenAI/codex.git",
    ],
)
def test_normalize_remote_rejects_noncanonical_or_unsafe_identity(remote_url: str) -> None:
    with pytest.raises(RemoteIdentityError):
        normalize_remote(remote_url)


def test_repository_upsert_builds_postgresql_conflict_update_without_committing() -> None:
    observed_at = datetime(2026, 8, 22, 12, tzinfo=UTC)
    expected = RepositoryRow(
        remote_host="github.com",
        remote_owner="OpenAI",
        remote_path="codex",
        original_remote_url="https://github.com/OpenAI/codex.git",
        observed_at=observed_at,
    )
    scalar_result = MagicMock()
    scalar_result.one.return_value = expected
    session = AsyncMock(spec=AsyncSession)
    session.scalars.return_value = scalar_result

    actual = asyncio.run(
        CatalogRepository.upsert_repository(
            session,
            "https://github.com/OpenAI/codex.git",
            observed_at=observed_at,
        )
    )

    statement = session.scalars.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert actual is expected
    assert "ON CONFLICT (remote_host, remote_owner, remote_path) DO UPDATE" in sql
    assert "greatest(catalog.repositories.observed_at, excluded.observed_at)" in sql
    assert "RETURNING" in sql
    session.commit.assert_not_awaited()


def test_checkout_upsert_canonicalizes_root_and_uses_repository_scoped_identity(
    tmp_path: Path,
) -> None:
    repository_id = uuid4()
    observed_at = datetime(2026, 8, 22, 12, tzinfo=UTC)
    expected = CheckoutRow(
        repository_id=repository_id,
        filesystem_root=str(tmp_path),
        observed_at=observed_at,
    )
    scalar_result = MagicMock()
    scalar_result.one.return_value = expected
    session = AsyncMock(spec=AsyncSession)
    session.scalars.return_value = scalar_result

    actual = asyncio.run(
        CatalogRepository.resolve_checkout(
            session,
            repository_id,
            tmp_path / ".",
            observed_at=observed_at,
        )
    )

    statement = session.scalars.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert actual is expected
    assert "ON CONFLICT (repository_id, filesystem_root) DO UPDATE" in sql
    assert statement.compile().params["filesystem_root"] == str(tmp_path.resolve())
    session.commit.assert_not_awaited()


def test_scope_requires_exactly_one_identifier_before_querying() -> None:
    session = AsyncMock(spec=AsyncSession)

    with pytest.raises(ValueError, match="exactly one"):
        asyncio.run(CatalogRepository.get_scope(session))
    with pytest.raises(ValueError, match="exactly one"):
        asyncio.run(CatalogRepository.get_scope(session, project_id=uuid4(), repository_id=uuid4()))

    session.scalar.assert_not_awaited()
    session.scalars.assert_not_awaited()
