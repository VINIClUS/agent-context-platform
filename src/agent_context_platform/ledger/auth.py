"""Producer bearer authentication for the ingestion plane.

A producer credential is a pre-provisioned, high-entropy bearer of the form
``<prefix>.<secret>``. The non-secret ``prefix`` selects one row of
``operations.registered_producers``; the full token is then verified against
that row's Argon2id verifier. A registration binds the token to exactly one
``producer_id`` and to the ``events:ingest`` scope. MCP ``memory:read`` tokens
are a different credential class that never has a registration here, so they
fail as an unknown prefix.

Bearer parsing and the Argon2id verifier live in
:mod:`agent_context_platform.security.bearer`, shared with the MCP plane and
re-exported here. This module keeps the producer-specific parts:
:class:`SqlProducerLookup` (prefix lookup) and :class:`ProducerAuthenticator`
(the policy that combines lookup and verification).

Nothing here logs or echoes token material; failures carry a fixed code only.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.operations.models import RegisteredProducerRow
from agent_context_platform.security.bearer import (
    Argon2Verifier,
    AuthError,
    AuthOverloadedError,
    BearerToken,
    InsufficientScopeError,
    InvalidCredentialError,
    MissingCredentialError,
    parse_bearer,
)

__all__ = [
    "INGEST_SCOPE",
    "Argon2Verifier",
    "AuthError",
    "AuthOverloadedError",
    "BearerToken",
    "InsufficientScopeError",
    "InvalidCredentialError",
    "MissingCredentialError",
    "ProducerAuthenticator",
    "ProducerLookup",
    "ProducerMismatchError",
    "ProducerPrincipal",
    "ProducerRegistration",
    "SqlProducerLookup",
    "parse_bearer",
]

INGEST_SCOPE: Final = "events:ingest"


class ProducerMismatchError(AuthError):
    status_code = 403
    code = "producer_mismatch"


@dataclass(frozen=True, slots=True)
class ProducerPrincipal:
    """The authenticated producer this request is bound to."""

    producer_id: str


@dataclass(frozen=True, slots=True)
class ProducerRegistration:
    """The registration columns authentication needs; carries the verifier."""

    producer_id: str
    token_verifier: str = field(repr=False)
    scope: str
    expires_at: datetime
    revoked_at: datetime | None


ProducerLookup = Callable[[str], Awaitable[ProducerRegistration | None]]


class SqlProducerLookup:
    """Select one registration by its unique non-secret token prefix."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __call__(self, token_prefix: str) -> ProducerRegistration | None:
        # A short read-only session: it is closed before the slow Argon2 verify runs.
        async with self._session_factory() as session:
            row = await session.scalar(
                select(RegisteredProducerRow).where(
                    RegisteredProducerRow.token_prefix == token_prefix
                )
            )
        if row is None:
            return None
        return ProducerRegistration(
            producer_id=row.producer_id,
            token_verifier=row.token_verifier,
            scope=row.scope,
            expires_at=row.expires_at,
            revoked_at=row.revoked_at,
        )


class ProducerAuthenticator:
    """Authenticate an ``events:ingest`` bearer and bind it to one ``producer_id``."""

    def __init__(
        self,
        lookup: ProducerLookup,
        verifier: Argon2Verifier,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._lookup = lookup
        self._verifier = verifier
        self._clock = clock

    async def authenticate(self, authorization: str | None) -> ProducerPrincipal:
        if authorization is None:
            raise MissingCredentialError
        bearer = parse_bearer(authorization)
        if bearer is None:
            raise InvalidCredentialError

        # One gate covers lookup and verify, so a flood cannot exhaust the DB pool.
        async with self._verifier.admission():
            registration = await self._lookup(bearer.prefix)
            # Always run one Argon2 verification, so an unknown prefix is not faster.
            verified = await self._verifier.verify(
                None if registration is None else registration.token_verifier, bearer.token
            )
        if registration is None or not verified:
            raise InvalidCredentialError

        # Lifecycle and scope are checked only after the secret proved possession.
        now = self._clock()
        if registration.revoked_at is not None and registration.revoked_at <= now:
            raise InvalidCredentialError
        if registration.expires_at <= now:
            raise InvalidCredentialError
        if registration.scope != INGEST_SCOPE:
            raise InsufficientScopeError
        return ProducerPrincipal(producer_id=registration.producer_id)
