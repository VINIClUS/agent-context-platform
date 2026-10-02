"""Operator provisioning of ingestion producers and MCP bearer tokens (PLATFORM-039B, FU-36).

A credential is ``<prefix>.<secret>``: the non-secret prefix selects one ``operations.*`` row and
only an Argon2id verifier of the whole token is stored, so the plaintext exists once, in the
:class:`IssuedCredential` this module returns. Nothing here logs or prints it.

Namespaces: MCP tokens must start with ``mcp_`` (a CHECK on ``operations.mcp_tokens``, and the
MCP gate refuses any other prefix); producer tokens use ``prd_`` and are never allowed to start
with ``mcp_``. ``operations.registered_producers`` has no prefix CHECK, so that separation is
enforced here.

Rows are written by an operator connection (``postgresql.admin_dsn``): the migrations grant the
API role only ``SELECT`` on both tables (plus a column ``UPDATE`` of ``last_used_at`` and
``updated_at`` on producers) and the projector role nothing, so `check_grants` verifies the
connection before anything is written.
"""

from __future__ import annotations

import asyncio
import secrets
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final

from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.ledger.auth import INGEST_SCOPE
from agent_context_platform.mcp.auth import MCP_TOKEN_PREFIX, MEMORY_READ_SCOPE
from agent_context_platform.operations.models import McpTokenRow, RegisteredProducerRow
from agent_context_platform.security.bearer import hash_verifier

PRODUCER_TOKEN_PREFIX: Final = "prd_"
MCP_SCOPES: Final = (MEMORY_READ_SCOPE,)
_PREFIX_RANDOM_BYTES: Final = 9  # 12 URL-safe characters
_SECRET_RANDOM_BYTES: Final = 32  # 43 URL-safe characters (the bearer pattern wants 32-256)
_MAX_NAME_LENGTH: Final = 255
_MAX_EXPIRES_IN_DAYS: Final = 3650


class ProvisioningError(Exception):
    """A refused provisioning request; ``code`` is a stable, content-free reason."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class MissingProvisioningGrantError(ProvisioningError):
    """The operator connection lacks privileges on ``operations.*``."""

    def __init__(self, missing: Sequence[str]) -> None:
        super().__init__("missing_grant")
        self.missing = tuple(missing)

    def __str__(self) -> str:
        return "the admin connection lacks: " + "; ".join(self.missing)


@dataclass(frozen=True, slots=True)
class HashCost:
    """The Argon2id parameters a new verifier is hashed with (the app's configured cost)."""

    time_cost: int
    memory_cost_kib: int
    parallelism: int


@dataclass(frozen=True, slots=True)
class IssuedCredential:
    """A freshly issued credential: ``token`` is the only copy of the plaintext."""

    kind: str
    identifier: str
    prefix: str
    token: str = field(repr=False)
    scope: str
    expires_at: datetime
    rotated: bool = False


@dataclass(frozen=True, slots=True)
class ProducerSummary:
    """A registration without its verifier."""

    producer_id: str
    token_prefix: str
    scope: str
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None
    last_used_at: datetime | None


@dataclass(frozen=True, slots=True)
class McpTokenSummary:
    """An MCP token record without its verifier."""

    token_id: uuid.UUID
    token_prefix: str
    principal: str
    scopes: tuple[str, ...]
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None


def _name(value: str, what: str) -> str:
    if not value.strip() or len(value) > _MAX_NAME_LENGTH or not value.isprintable():
        raise ProvisioningError(f"invalid_{what}")
    return value


def _expiry(now: datetime, days: int) -> datetime:
    if not 1 <= days <= _MAX_EXPIRES_IN_DAYS:
        raise ProvisioningError("invalid_expiry")
    return now + timedelta(days=days)


def _new_token(namespace: str) -> tuple[str, str]:
    """A ``(prefix, token)`` pair; the producer namespace can never collide with ``mcp_``."""
    prefix = namespace + secrets.token_urlsafe(_PREFIX_RANDOM_BYTES)
    return prefix, f"{prefix}.{secrets.token_urlsafe(_SECRET_RANDOM_BYTES)}"


async def _hash(token: str, cost: HashCost) -> str:
    return await asyncio.to_thread(
        hash_verifier,
        token,
        time_cost=cost.time_cost,
        memory_cost_kib=cost.memory_cost_kib,
        parallelism=cost.parallelism,
    )


_TABLES: Final = {
    "operations.registered_producers": ("SELECT", "INSERT", "UPDATE"),
    "operations.mcp_tokens": ("SELECT", "INSERT", "UPDATE"),
}


async def check_grants(
    sessions: async_sessionmaker[AsyncSession], *, tables: Sequence[str], write: bool
) -> None:
    """Fail before any write when the connection cannot read (and write) ``tables``."""
    missing: list[str] = []
    async with sessions() as session:
        # Without schema USAGE, probing a table's privileges raises instead of answering.
        if not await session.scalar(
            text("SELECT has_schema_privilege(current_user, 'operations', 'USAGE')")
        ):
            raise MissingProvisioningGrantError(["USAGE on schema operations"])
        for table in tables:
            for privilege in _TABLES[table] if write else ("SELECT",):
                granted = await session.scalar(
                    text("SELECT has_table_privilege(current_user, :table, :privilege)"),
                    {"table": table, "privilege": privilege},
                )
                if not granted:
                    missing.append(f"{privilege} on {table}")
    if missing:
        raise MissingProvisioningGrantError(missing)


def _is_active(revoked_at: datetime | None, expires_at: datetime, now: datetime) -> bool:
    return (revoked_at is None or revoked_at > now) and expires_at > now


async def register_producer(
    sessions: async_sessionmaker[AsyncSession],
    *,
    producer_id: str,
    expires_in_days: int,
    cost: HashCost,
    rotate: bool = False,
    now: datetime | None = None,
) -> IssuedCredential:
    """Register ``producer_id`` with a new ``events:ingest`` token.

    An active registration is refused unless ``rotate``; ``rotate`` keeps the id and
    ``created_at`` and replaces the prefix, verifier and expiry (clearing any revocation). An
    expired or revoked registration is replaced the same way, since there is no live credential
    to protect.
    """
    producer_id = _name(producer_id, "producer_id")
    moment = now or datetime.now(UTC)
    expires_at = _expiry(moment, expires_in_days)
    prefix, token = _new_token(PRODUCER_TOKEN_PREFIX)
    verifier = await _hash(token, cost)
    try:
        async with sessions.begin() as session:
            row = await session.scalar(
                select(RegisteredProducerRow)
                .where(RegisteredProducerRow.producer_id == producer_id)
                .with_for_update()
            )
            rotated = row is not None
            if row is None:
                session.add(
                    RegisteredProducerRow(
                        producer_id=producer_id,
                        token_prefix=prefix,
                        token_verifier=verifier,
                        scope=INGEST_SCOPE,
                        expires_at=expires_at,
                        revoked_at=None,
                        created_at=moment,
                        updated_at=moment,
                    )
                )
            else:
                if not rotate and _is_active(row.revoked_at, row.expires_at, moment):
                    raise ProvisioningError("producer_exists")
                row.token_prefix = prefix
                row.token_verifier = verifier
                row.expires_at = expires_at
                row.revoked_at = None
                row.updated_at = max(moment, row.created_at)
    except IntegrityError:
        # A concurrent registration of the same id (or, vanishingly, the same prefix).
        raise ProvisioningError("producer_conflict") from None
    return IssuedCredential(
        "producer", producer_id, prefix, token, INGEST_SCOPE, expires_at, rotated=rotated
    )


async def revoke_producer(
    sessions: async_sessionmaker[AsyncSession], producer_id: str, *, now: datetime | None = None
) -> bool:
    """Revoke a registration; ``False`` when it was already revoked, error when unknown."""
    moment = now or datetime.now(UTC)
    async with sessions.begin() as session:
        row = await session.scalar(
            select(RegisteredProducerRow)
            .where(RegisteredProducerRow.producer_id == producer_id)
            .with_for_update()
        )
        if row is None:
            raise ProvisioningError("producer_not_found")
        if row.revoked_at is not None and row.revoked_at <= moment:
            return False
        row.revoked_at = max(moment, row.created_at)
        row.updated_at = max(moment, row.created_at)
        return True


async def list_producers(sessions: async_sessionmaker[AsyncSession]) -> list[ProducerSummary]:
    async with sessions() as session:
        rows = await session.scalars(
            select(RegisteredProducerRow).order_by(RegisteredProducerRow.producer_id)
        )
        return [
            ProducerSummary(
                row.producer_id,
                row.token_prefix,
                row.scope,
                row.created_at,
                row.expires_at,
                row.revoked_at,
                row.last_used_at,
            )
            for row in rows
        ]


async def create_mcp_token(
    sessions: async_sessionmaker[AsyncSession],
    *,
    principal: str,
    scope: str,
    expires_in_days: int,
    cost: HashCost,
    now: datetime | None = None,
) -> IssuedCredential:
    """Create a ``memory:read`` MCP token for ``principal``."""
    principal = _name(principal, "principal")
    if scope not in MCP_SCOPES:
        raise ProvisioningError("invalid_scope")
    moment = now or datetime.now(UTC)
    expires_at = _expiry(moment, expires_in_days)
    prefix, token = _new_token(MCP_TOKEN_PREFIX)
    verifier = await _hash(token, cost)
    token_id = uuid.uuid4()
    try:
        async with sessions.begin() as session:
            session.add(
                McpTokenRow(
                    token_id=token_id,
                    token_prefix=prefix,
                    token_verifier=verifier,
                    principal=principal,
                    scopes=[scope],
                    created_at=moment,
                    expires_at=expires_at,
                    revoked_at=None,
                )
            )
    except IntegrityError:
        raise ProvisioningError("token_conflict") from None
    return IssuedCredential("mcp-token", str(token_id), prefix, token, scope, expires_at)


async def revoke_mcp_token(
    sessions: async_sessionmaker[AsyncSession], prefix: str, *, now: datetime | None = None
) -> bool:
    """Revoke the token with ``prefix``; ``False`` when it was already revoked."""
    moment = now or datetime.now(UTC)
    async with sessions.begin() as session:
        row = await session.scalar(
            select(McpTokenRow).where(McpTokenRow.token_prefix == prefix).with_for_update()
        )
        if row is None:
            raise ProvisioningError("token_not_found")
        if row.revoked_at is not None and row.revoked_at <= moment:
            return False
        await session.execute(
            update(McpTokenRow)
            .where(McpTokenRow.token_id == row.token_id)
            .values(revoked_at=max(moment, row.created_at))
        )
        return True


async def list_mcp_tokens(sessions: async_sessionmaker[AsyncSession]) -> list[McpTokenSummary]:
    async with sessions() as session:
        rows = await session.scalars(
            select(McpTokenRow).order_by(McpTokenRow.principal, McpTokenRow.created_at)
        )
        return [
            McpTokenSummary(
                row.token_id,
                row.token_prefix,
                row.principal,
                tuple(row.scopes),
                row.created_at,
                row.expires_at,
                row.revoked_at,
            )
            for row in rows
        ]
