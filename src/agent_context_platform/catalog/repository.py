"""Persistence operations and canonical identity rules for the catalog."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from ipaddress import ip_address
from pathlib import Path
from urllib.parse import SplitResult, urlsplit
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from agent_context_platform.catalog.models import (
    CheckoutRow,
    ProjectRepositoryRow,
    ProjectRow,
    RepositoryRow,
)

_SCP_REMOTE = re.compile(r"^(?:(?P<user>[^@/:\s]+)@)?(?P<host>\[[^\]]+\]|[^/:\s]+):(?P<path>.+)$")
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")


class RemoteIdentityError(ValueError):
    """Raised when a remote cannot provide a canonical network identity."""


class CatalogScopeNotFoundError(LookupError):
    """Raised when a requested project or repository scope does not exist."""


@dataclass(frozen=True, slots=True)
class NormalizedRemote:
    """Stable repository identity extracted from a Git remote."""

    remote_host: str
    remote_owner: str
    remote_path: str


@dataclass(frozen=True, slots=True)
class CatalogScope:
    """The ordered repository membership of one catalog scope."""

    project_id: UUID | None
    repository_id: UUID | None
    repositories: tuple[RepositoryRow, ...]


def _canonical_hostname(host: str) -> str:
    candidate = host.strip("[]").rstrip(".")
    try:
        normalized = candidate.encode("idna").decode("ascii").lower()
    except UnicodeError as error:
        raise RemoteIdentityError("remote contains an invalid host") from error
    if not normalized:
        raise RemoteIdentityError("remote must include a host")
    try:
        return ip_address(normalized).compressed
    except ValueError:
        pass
    labels = normalized.split(".")
    if len(normalized) > 253 or any(_DNS_LABEL.fullmatch(label) is None for label in labels):
        raise RemoteIdentityError("remote contains an invalid host")
    return normalized


def _normalized_host(parsed: SplitResult, *, default_port: int) -> str:
    try:
        host = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise RemoteIdentityError("remote contains an invalid port") from error
    if host is None:
        raise RemoteIdentityError("remote must include a host")

    normalized = _canonical_hostname(host)
    if port is None or port == default_port:
        return normalized
    if ":" in normalized:
        normalized = f"[{normalized}]"
    return f"{normalized}:{port}"


def _normalized_owner_path(raw_path: str) -> tuple[str, str]:
    normalized_parts: list[str] = []
    position = 0
    while position < len(raw_path):
        character = raw_path[position]
        if character != "%":
            normalized_parts.append(character)
            position += 1
            continue
        encoded = raw_path[position + 1 : position + 3]
        if len(encoded) != 2 or re.fullmatch(r"[0-9A-Fa-f]{2}", encoded) is None:
            raise RemoteIdentityError("remote path contains invalid percent encoding")
        decoded = chr(int(encoded, 16))
        if decoded in {"/", "\\"}:
            raise RemoteIdentityError("remote path contains an encoded separator")
        normalized_parts.append(decoded if decoded in _UNRESERVED else f"%{encoded.upper()}")
        position += 3

    candidate = "".join(normalized_parts).strip("/")
    if candidate.lower().endswith(".git"):
        candidate = candidate[:-4]
    candidate = candidate.rstrip("/")
    parts = candidate.split("/")
    if (
        len(parts) < 2
        or any(not part or part in {".", ".."} for part in parts)
        or "\\" in candidate
    ):
        raise RemoteIdentityError("remote path must contain an owner and repository path")
    return parts[0], "/".join(parts[1:])


def normalize_remote(remote_url: str) -> NormalizedRemote:
    """Normalize HTTPS, SSH URI, or SCP-style Git remotes into stable identity parts."""

    candidate = remote_url.strip()
    if not candidate:
        raise RemoteIdentityError("remote URL is required")
    if "?" in candidate or "#" in candidate:
        raise RemoteIdentityError("remote query strings and fragments are not identities")

    if "://" in candidate:
        try:
            parsed = urlsplit(candidate)
        except ValueError as error:
            raise RemoteIdentityError("remote URL is malformed") from error
        if parsed.scheme not in {"https", "ssh"}:
            raise RemoteIdentityError(f"unsupported remote scheme: {parsed.scheme or '<none>'}")
        if parsed.scheme == "https" and (
            parsed.username is not None or parsed.password is not None
        ):
            raise RemoteIdentityError("HTTPS credentials are not allowed in remote identity")
        if parsed.scheme == "ssh" and parsed.password is not None:
            raise RemoteIdentityError("SSH passwords are not allowed in remote identity")
        host = _normalized_host(parsed, default_port=443 if parsed.scheme == "https" else 22)
        owner, path = _normalized_owner_path(parsed.path)
        return NormalizedRemote(host, owner, path)

    match = _SCP_REMOTE.fullmatch(candidate)
    remote_part = candidate.rsplit("@", maxsplit=1)[-1]
    if not remote_part.startswith("[") and remote_part.count(":") != 1:
        raise RemoteIdentityError("SCP remotes with IPv6 hosts must use brackets")
    if match is None:
        raise RemoteIdentityError("remote must use HTTPS, SSH, or SCP syntax")
    host = _canonical_hostname(match.group("host"))
    owner, path = _normalized_owner_path(match.group("path"))
    return NormalizedRemote(host, owner, path)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(UTC)


class CatalogRepository:
    """Transaction-neutral async catalog operations."""

    @staticmethod
    async def upsert_repository(
        session: AsyncSession,
        remote_url: str,
        *,
        observed_at: datetime,
    ) -> RepositoryRow:
        normalized = normalize_remote(remote_url)
        observation = _as_utc(observed_at)
        insert_statement = insert(RepositoryRow).values(
            remote_host=normalized.remote_host,
            remote_owner=normalized.remote_owner,
            remote_path=normalized.remote_path,
            original_remote_url=remote_url,
            observed_at=observation,
        )
        statement = insert_statement.on_conflict_do_update(
            index_elements=[
                RepositoryRow.remote_host,
                RepositoryRow.remote_owner,
                RepositoryRow.remote_path,
            ],
            set_={
                "observed_at": func.greatest(
                    RepositoryRow.observed_at, insert_statement.excluded.observed_at
                )
            },
        ).returning(RepositoryRow)
        result = await session.scalars(statement.execution_options(populate_existing=True))
        return result.one()

    @staticmethod
    async def resolve_checkout(
        session: AsyncSession,
        repository_id: UUID,
        filesystem_root: Path,
        *,
        observed_at: datetime,
    ) -> CheckoutRow:
        try:
            canonical_root = filesystem_root.resolve(strict=True)
        except OSError as error:
            raise ValueError("filesystem_root must be an existing directory") from error
        if not canonical_root.is_dir():
            raise ValueError("filesystem_root must be an existing directory")

        observation = _as_utc(observed_at)
        insert_statement = insert(CheckoutRow).values(
            repository_id=repository_id,
            filesystem_root=str(canonical_root),
            observed_at=observation,
        )
        statement = insert_statement.on_conflict_do_update(
            index_elements=[CheckoutRow.repository_id, CheckoutRow.filesystem_root],
            set_={
                "observed_at": func.greatest(
                    CheckoutRow.observed_at, insert_statement.excluded.observed_at
                )
            },
        ).returning(CheckoutRow)
        result = await session.scalars(statement.execution_options(populate_existing=True))
        return result.one()

    @staticmethod
    async def get_scope(
        session: AsyncSession,
        *,
        project_id: UUID | None = None,
        repository_id: UUID | None = None,
    ) -> CatalogScope:
        if (project_id is None) == (repository_id is None):
            raise ValueError("exactly one of project_id or repository_id is required")

        ordering = (
            RepositoryRow.remote_host,
            RepositoryRow.remote_owner,
            RepositoryRow.remote_path,
            RepositoryRow.id,
        )
        if project_id is not None:
            project_exists = await session.scalar(
                select(ProjectRow.id).where(ProjectRow.id == project_id)
            )
            if project_exists is None:
                raise CatalogScopeNotFoundError(f"project scope not found: {project_id}")
            statement = (
                select(RepositoryRow)
                .join(ProjectRepositoryRow)
                .where(ProjectRepositoryRow.project_id == project_id)
                .order_by(*ordering)
            )
            repositories = tuple((await session.scalars(statement)).all())
            return CatalogScope(project_id, None, repositories)

        statement = (
            select(RepositoryRow).where(RepositoryRow.id == repository_id).order_by(*ordering)
        )
        repositories = tuple((await session.scalars(statement)).all())
        if not repositories:
            raise CatalogScopeNotFoundError(f"repository scope not found: {repository_id}")
        return CatalogScope(None, repository_id, repositories)
