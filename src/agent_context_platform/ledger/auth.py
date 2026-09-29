"""Producer bearer authentication for the ingestion plane.

A producer credential is a pre-provisioned, high-entropy bearer of the form
``<prefix>.<secret>``. The non-secret ``prefix`` selects one row of
``operations.registered_producers``; the full token is then verified against
that row's Argon2id verifier. A registration binds the token to exactly one
``producer_id`` and to the ``events:ingest`` scope. MCP ``memory:read`` tokens
are a different credential class that never has a registration here, so they
fail as an unknown prefix.

The pieces are deliberately small and separate so PLATFORM-045 can lift them
into a shared module: :func:`parse_bearer`, :class:`SqlProducerLookup` (prefix
lookup), :class:`Argon2Verifier` (bounded worker plus dummy-hash path) and
:class:`ProducerAuthenticator` (the policy that combines them).

Nothing here logs or echoes token material; failures carry a fixed code only.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar, Final

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.operations.models import RegisteredProducerRow

INGEST_SCOPE: Final = "events:ingest"

_TOKEN_MAX_LENGTH: Final = 512
_TOKEN_PATTERN: Final = re.compile(
    r"(?P<prefix>[A-Za-z0-9_-]{8,64})\.(?P<secret>[A-Za-z0-9_-]{32,256})", re.ASCII
)
_BEARER_SCHEME: Final = "bearer "
# Any well-formed token verifies against this; only its cost matters.
_DUMMY_PLAINTEXT: Final = "agent-context-dummy-credential"


class AuthError(Exception):
    """Base for authentication failures; ``code`` is a fixed, content-free string."""

    status_code: ClassVar[int]
    code: ClassVar[str]


class MissingCredentialError(AuthError):
    status_code = 401
    code = "missing_credential"


class InvalidCredentialError(AuthError):
    """Malformed, unknown, wrong, revoked or expired: one code, so no state oracle."""

    status_code = 401
    code = "invalid_credential"


class InsufficientScopeError(AuthError):
    status_code = 403
    code = "insufficient_scope"


class AuthOverloadedError(AuthError):
    """The verification queue is full: shed load instead of delaying legitimate callers."""

    status_code = 503
    code = "auth_overloaded"


class ProducerMismatchError(AuthError):
    status_code = 403
    code = "producer_mismatch"


@dataclass(frozen=True, slots=True)
class BearerToken:
    """A parsed bearer: the non-secret lookup ``prefix`` and the full token."""

    prefix: str
    token: str = field(repr=False)


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


def parse_bearer(authorization: str | None) -> BearerToken | None:
    """Parse ``Authorization: Bearer <prefix>.<secret>`` once; ``None`` when malformed."""
    if authorization is None or len(authorization) > _TOKEN_MAX_LENGTH + len(_BEARER_SCHEME):
        return None
    if authorization[: len(_BEARER_SCHEME)].lower() != _BEARER_SCHEME:
        return None
    token = authorization[len(_BEARER_SCHEME) :].strip()
    match = _TOKEN_PATTERN.fullmatch(token)
    if match is None:
        return None
    return BearerToken(prefix=match["prefix"], token=token)


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


class Argon2Verifier:
    """Argon2id verification in worker threads, bounded by ``max_concurrency``.

    At most ``max_queue_depth`` further verifications may wait; more raise
    ``AuthOverloadedError`` at once, so a flood of bad tokens cannot build an
    unbounded backlog in front of legitimate callers.

    Stored verifiers carry their own parameters, so ``verify`` honours whatever
    cost each registration was provisioned with. The cost arguments only shape
    the dummy verifier used for unknown prefixes, so those requests cost as much
    as a real one.
    """

    def __init__(
        self,
        *,
        time_cost: int,
        memory_cost_kib: int,
        parallelism: int,
        max_concurrency: int,
        max_queue_depth: int,
    ) -> None:
        self._hasher = PasswordHasher(
            time_cost=time_cost, memory_cost=memory_cost_kib, parallelism=parallelism
        )
        self._dummy_verifier = self._hasher.hash(_DUMMY_PLAINTEXT)
        self._slots = asyncio.Semaphore(max_concurrency)
        self._capacity = max_concurrency + max_queue_depth
        self._pending = 0

    @contextlib.asynccontextmanager
    async def admission(self) -> AsyncIterator[None]:
        """Admit one whole authentication (lookup and verify) or fail fast.

        Holds one of ``max_concurrency`` slots; at most ``max_queue_depth`` more
        callers wait, and any beyond that raise ``AuthOverloadedError`` before
        touching the database.
        """
        if self._pending >= self._capacity:
            raise AuthOverloadedError
        self._pending += 1
        try:
            async with self._slots:
                yield
        finally:
            self._pending -= 1

    async def verify(self, verifier: str | None, token: str) -> bool:
        """Verify ``token``; ``verifier=None`` runs the dummy-hash path and is always False."""
        expected = self._dummy_verifier if verifier is None else verifier
        matched = await asyncio.to_thread(self._verify_blocking, expected, token)
        return matched and verifier is not None

    def _verify_blocking(self, verifier: str, token: str) -> bool:
        try:
            return self._hasher.verify(verifier, token)
        except (VerificationError, InvalidHashError):
            return False


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
