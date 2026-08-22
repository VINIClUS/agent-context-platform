from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateSchema, DropSchema

from agent_context_platform.catalog.models import (
    CatalogBase,
    ProjectRepositoryRow,
    ProjectRow,
    RepositoryRow,
    WorkspaceRow,
)
from agent_context_platform.catalog.repository import (
    CatalogRepository,
    CatalogScopeNotFoundError,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def catalog_engine() -> Iterator[AsyncEngine]:
    dsn = os.environ.get("AGENT_CONTEXT_TEST_POSTGRES_DSN")
    if dsn is None:
        pytest.skip("AGENT_CONTEXT_TEST_POSTGRES_DSN is required for PostgreSQL tests")

    engine = create_async_engine(dsn, poolclass=NullPool)

    async def create_catalog() -> None:
        async with engine.begin() as connection:
            catalog_exists = await connection.scalar(
                text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'catalog')")
            )
            if catalog_exists:
                raise RuntimeError(
                    "AGENT_CONTEXT_TEST_POSTGRES_DSN must reference a clean disposable database"
                )
            await connection.execute(CreateSchema("catalog"))
            await connection.run_sync(CatalogBase.metadata.create_all)

    async def drop_catalog() -> None:
        async with engine.begin() as connection:
            await connection.execute(DropSchema("catalog", cascade=True, if_exists=True))
        await engine.dispose()

    asyncio.run(create_catalog())
    yield engine
    asyncio.run(drop_catalog())


def test_remote_forms_upsert_one_repository_without_committing(catalog_engine: AsyncEngine) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(catalog_engine, expire_on_commit=False)
        first_observation = datetime(2026, 8, 22, 12, tzinfo=UTC)
        latest_observation = first_observation + timedelta(minutes=10)

        async with factory() as session:
            https = await CatalogRepository.upsert_repository(
                session,
                "https://github.com/OpenAI/codex.git",
                observed_at=first_observation,
            )
            ssh = await CatalogRepository.upsert_repository(
                session,
                "ssh://git@github.com/OpenAI/codex",
                observed_at=latest_observation,
            )
            scp = await CatalogRepository.upsert_repository(
                session,
                "git@github.com:OpenAI/codex.git",
                observed_at=first_observation + timedelta(minutes=5),
            )

            assert https.id == ssh.id == scp.id
            assert scp.id.version == 7
            assert scp.original_remote_url == "https://github.com/OpenAI/codex.git"
            assert scp.observed_at == latest_observation
            assert session.in_transaction()
            await session.rollback()

        async with factory() as session:
            count = await session.scalar(select(func.count()).select_from(RepositoryRow))
            assert count == 0

    asyncio.run(exercise())


def test_projects_worktrees_and_scope_resolution(
    catalog_engine: AsyncEngine,
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(catalog_engine, expire_on_commit=False)
        observed_at = datetime(2026, 8, 22, 13, tzinfo=UTC)
        worktree_a = tmp_path / "primary"
        worktree_b = tmp_path / "feature"
        shared_path = tmp_path / "shared"
        for directory in (worktree_a, worktree_b, shared_path):
            directory.mkdir()

        async with factory.begin() as session:
            workspace = WorkspaceRow(name="engineering", observed_at=observed_at)
            project_a = ProjectRow(
                name="agent-context", workspace=workspace, observed_at=observed_at
            )
            project_b = ProjectRow(name="tooling", workspace=workspace, observed_at=observed_at)
            session.add_all([workspace, project_a, project_b])
            repository_z = await CatalogRepository.upsert_repository(
                session,
                "https://example.com/team/zeta.git",
                observed_at=observed_at,
            )
            repository_a = await CatalogRepository.upsert_repository(
                session,
                "git@example.com:team/alpha.git",
                observed_at=observed_at,
            )
            session.add_all(
                [
                    ProjectRepositoryRow(
                        project=project_a,
                        repository=repository_z,
                        observed_at=observed_at,
                    ),
                    ProjectRepositoryRow(
                        project=project_a,
                        repository=repository_a,
                        observed_at=observed_at,
                    ),
                    ProjectRepositoryRow(
                        project=project_b,
                        repository=repository_a,
                        observed_at=observed_at,
                    ),
                ]
            )
            await session.flush()

            checkout_a = await CatalogRepository.resolve_checkout(
                session, repository_a.id, worktree_a, observed_at=observed_at
            )
            checkout_b = await CatalogRepository.resolve_checkout(
                session, repository_a.id, worktree_b, observed_at=observed_at
            )
            checkout_a_again = await CatalogRepository.resolve_checkout(
                session,
                repository_a.id,
                worktree_a / ".." / "primary",
                observed_at=observed_at + timedelta(minutes=1),
            )
            same_path_other_repository = await CatalogRepository.resolve_checkout(
                session, repository_z.id, shared_path, observed_at=observed_at
            )
            same_path_repository_a = await CatalogRepository.resolve_checkout(
                session, repository_a.id, shared_path, observed_at=observed_at
            )

            assert checkout_a.id != checkout_b.id
            assert checkout_a_again.id == checkout_a.id
            assert checkout_a_again.observed_at == observed_at + timedelta(minutes=1)
            assert same_path_other_repository.id != same_path_repository_a.id

            project_scope = await CatalogRepository.get_scope(session, project_id=project_a.id)
            repository_scope = await CatalogRepository.get_scope(
                session, repository_id=repository_z.id
            )

            assert [repository.remote_path for repository in project_scope.repositories] == [
                "alpha",
                "zeta",
            ]
            assert repository_scope.repositories == (repository_z,)
            assert project_scope.project_id == project_a.id
            assert repository_scope.repository_id == repository_z.id

            with pytest.raises(ValueError, match="exactly one"):
                await CatalogRepository.get_scope(session)
            with pytest.raises(ValueError, match="exactly one"):
                await CatalogRepository.get_scope(
                    session, project_id=project_a.id, repository_id=repository_a.id
                )
            with pytest.raises(CatalogScopeNotFoundError, match="project"):
                await CatalogRepository.get_scope(session, project_id=uuid4())
            with pytest.raises(CatalogScopeNotFoundError, match="repository"):
                await CatalogRepository.get_scope(session, repository_id=uuid4())

    asyncio.run(exercise())


def test_checkout_requires_an_existing_directory(
    catalog_engine: AsyncEngine,
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        factory = async_sessionmaker(catalog_engine)
        async with factory() as session:
            with pytest.raises(ValueError, match="existing directory"):
                await CatalogRepository.resolve_checkout(
                    session,
                    UUID("00000000-0000-0000-0000-000000000001"),
                    tmp_path / "missing",
                    observed_at=datetime.now(UTC),
                )

            file_path = tmp_path / "file"
            file_path.touch()
            with pytest.raises(ValueError, match="existing directory"):
                await CatalogRepository.resolve_checkout(
                    session,
                    UUID("00000000-0000-0000-0000-000000000001"),
                    file_path,
                    observed_at=datetime.now(UTC),
                )

            with pytest.raises(ValueError, match="timezone-aware"):
                await CatalogRepository.upsert_repository(
                    session,
                    "https://github.com/OpenAI/codex.git",
                    observed_at=datetime(2026, 8, 22, 15),
                )

    asyncio.run(exercise())
