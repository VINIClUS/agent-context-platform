"""SQLAlchemy mappings for the authoritative catalog."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

CATALOG_SCHEMA = "catalog"

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class CatalogBase(DeclarativeBase):
    """Declarative base isolated to the catalog PostgreSQL schema."""

    metadata = MetaData(schema=CATALOG_SCHEMA, naming_convention=NAMING_CONVENTION)


class CatalogRowMixin:
    """Identity and observation metadata shared by catalog records."""

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), primary_key=True, server_default=text("uuidv7()")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WorkspaceRow(CatalogRowMixin, CatalogBase):
    """A top-level grouping for projects."""

    __tablename__ = "workspaces"

    name: Mapped[str] = mapped_column(String(255), nullable=False)

    projects: Mapped[list[ProjectRow]] = relationship(
        back_populates="workspace", passive_deletes="all"
    )


class ProjectRow(CatalogRowMixin, CatalogBase):
    """A named project within a workspace."""

    __tablename__ = "projects"
    __table_args__ = (UniqueConstraint("workspace_id", "name"),)

    workspace_id: Mapped[UUID] = mapped_column(
        ForeignKey("catalog.workspaces.id", ondelete="RESTRICT"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    workspace: Mapped[WorkspaceRow] = relationship(back_populates="projects", passive_deletes="all")
    repository_links: Mapped[list[ProjectRepositoryRow]] = relationship(
        back_populates="project", passive_deletes="all"
    )


class RepositoryRow(CatalogRowMixin, CatalogBase):
    """Canonical network identity for a Git repository."""

    __tablename__ = "repositories"
    __table_args__ = (UniqueConstraint("remote_host", "remote_owner", "remote_path"),)

    remote_host: Mapped[str] = mapped_column(String(255), nullable=False)
    remote_owner: Mapped[str] = mapped_column(String(255), nullable=False)
    remote_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    original_remote_url: Mapped[str] = mapped_column(Text, nullable=False)

    project_links: Mapped[list[ProjectRepositoryRow]] = relationship(
        back_populates="repository", passive_deletes="all"
    )
    checkouts: Mapped[list[CheckoutRow]] = relationship(
        back_populates="repository", passive_deletes="all"
    )


class ProjectRepositoryRow(CatalogRowMixin, CatalogBase):
    """Association object connecting projects to repositories."""

    __tablename__ = "project_repositories"
    __table_args__ = (UniqueConstraint("project_id", "repository_id"),)

    project_id: Mapped[UUID] = mapped_column(
        ForeignKey("catalog.projects.id", ondelete="RESTRICT"), nullable=False
    )
    repository_id: Mapped[UUID] = mapped_column(
        ForeignKey("catalog.repositories.id", ondelete="RESTRICT"), nullable=False
    )

    project: Mapped[ProjectRow] = relationship(
        back_populates="repository_links", passive_deletes="all"
    )
    repository: Mapped[RepositoryRow] = relationship(
        back_populates="project_links", passive_deletes="all"
    )


class CheckoutRow(CatalogRowMixin, CatalogBase):
    """A canonical filesystem checkout, including independent worktrees."""

    __tablename__ = "checkouts"
    __table_args__ = (UniqueConstraint("repository_id", "filesystem_root"),)

    repository_id: Mapped[UUID] = mapped_column(
        ForeignKey("catalog.repositories.id", ondelete="RESTRICT"), nullable=False
    )
    filesystem_root: Mapped[str] = mapped_column(String(4096), nullable=False)

    repository: Mapped[RepositoryRow] = relationship(
        back_populates="checkouts", passive_deletes="all"
    )


class ContentObjectRow(CatalogRowMixin, CatalogBase):
    """Metadata for sanitized content persisted in object storage."""

    __tablename__ = "content_objects"
    __table_args__ = (
        UniqueConstraint("content_sha256"),
        UniqueConstraint("object_key"),
        CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'", name="content_sha256_format"),
        CheckConstraint("length(object_key) > 0", name="object_key_not_empty"),
        CheckConstraint("compressed_bytes >= 0", name="compressed_bytes_nonnegative"),
        CheckConstraint("uncompressed_bytes >= 0", name="uncompressed_bytes_nonnegative"),
    )

    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    object_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    media_type: Mapped[str] = mapped_column(String(255), nullable=False)
    compressed_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    uncompressed_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
