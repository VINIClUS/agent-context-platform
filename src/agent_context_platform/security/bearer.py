"""Bearer parsing and Argon2id verification shared by the ingestion and MCP planes.

A credential is a pre-provisioned, high-entropy bearer ``<prefix>.<secret>``: the
non-secret ``prefix`` selects one stored record and the full token is verified
against that record's Argon2id verifier. Each plane owns its own table and
prefix namespace, and its own :class:`Argon2Verifier`, so a credential of one
plane is never usable on the other and a flood on one plane cannot shed load on
the other.

Nothing here logs or echoes token material; failures carry a fixed code only.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import ClassVar, Final

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

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


@dataclass(frozen=True, slots=True)
class BearerToken:
    """A parsed bearer: the non-secret lookup ``prefix`` and the full token."""

    prefix: str
    token: str = field(repr=False)


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
