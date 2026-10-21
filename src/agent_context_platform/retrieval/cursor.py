"""Opaque, signed, stateless pagination cursors for context packages (PLATFORM-043).

A cursor is `ctx1.<payload>.<signature>`, both parts unpadded base64url. The payload is canonical
JSON (sorted keys, no spaces) of:

- `v`: format version (1);
- `t`: the tool the cursor was issued for;
- `q`: SHA-256 of the normalized query: the tool's input without its `cursor`, plus the principal
  the page was composed for (`query_digest`);
- `p`: the position of the next item in the ranked candidate list;
- `a`: the projection snapshot the ranking was computed at (the composer binds the
  `as_of_event_id` and the `projection_checkpoint`);
- `e`: expiry, whole seconds since the epoch (UTC).

The signature is HMAC-SHA256 over a domain-separated payload with the server key, checked in
constant time BEFORE the payload is parsed, so a forged or truncated cursor is never decoded. The
key comes from settings (`mcp.cursor_hmac_key`, required in production); every replica uses the
same key, so any replica can continue a page another one issued and no server state exists.

A cursor is accepted only for the same tool, the same normalized query, the same principal and the
same snapshot: a cursor signed with another key, or edited, is `CursorInvalid`; one past
its expiry is `CursorExpired`; one issued for another query, tool or principal is
`CursorMismatch`; one issued at another projection snapshot is `CursorStale` (the ranking cannot be
recomputed at an older snapshot, so the client starts again from the first page). Messages are
content-free: they never echo the cursor or the query.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Self

from pydantic import BaseModel, SecretStr

from agent_context_platform.retrieval.graph import RetrievalError

PREFIX: Final = "ctx1"
MAX_CURSOR_CHARS: Final = 1024
MIN_KEY_CHARS: Final = 32
DEFAULT_TTL: Final = timedelta(minutes=15)
_DOMAIN: Final = b"agent-context.context-cursor.v1\0"
_FIELDS: Final = frozenset({"v", "t", "q", "p", "a", "e"})


class CursorError(RetrievalError, ValueError):
    """A cursor cannot be used for this request."""


class CursorInvalid(CursorError):
    """The cursor is malformed, was edited, or was signed with another key."""


class CursorExpired(CursorError):
    """The cursor is older than its time to live."""


class CursorMismatch(CursorError):
    """The cursor was issued for another tool, query or principal."""


class CursorStale(CursorError):
    """The projection snapshot changed since the cursor was issued; start from the first page."""


def query_digest(tool: str, request: BaseModel, principal_id: str) -> str:
    """SHA-256 of the normalized query: every input except `cursor`, plus tool and principal."""
    normalized = request.model_dump(mode="json", exclude={"cursor"})
    material = {"tool": tool, "principal": principal_id, "input": normalized}
    return hashlib.sha256(_canonical(material)).hexdigest()


@dataclass(frozen=True, slots=True)
class CursorPosition:
    position: int
    as_of_event_id: str


class CursorCodec:
    """Issues and checks cursors with one server key."""

    __slots__ = ("_key", "_ttl")

    def __init__(self, key: SecretStr, *, ttl: timedelta = DEFAULT_TTL) -> None:
        secret = key.get_secret_value()
        if len(secret.strip()) < MIN_KEY_CHARS:
            raise ValueError(f"cursor key must be at least {MIN_KEY_CHARS} characters")
        if ttl <= timedelta(0):
            raise ValueError("cursor ttl must be positive")
        self._key = secret.encode()
        self._ttl = ttl

    @classmethod
    def ephemeral(cls, *, ttl: timedelta = DEFAULT_TTL) -> Self:
        """A random per-process key for development and tests (cursors die with the process)."""
        return cls(SecretStr(secrets.token_urlsafe(48)), ttl=ttl)

    @classmethod
    def from_key(cls, key: SecretStr | None, *, ttl: timedelta = DEFAULT_TTL) -> Self:
        """The configured key, or an ephemeral one when none is set (settings enforce production)."""
        return cls.ephemeral(ttl=ttl) if key is None else cls(key, ttl=ttl)

    def encode(
        self, *, tool: str, digest: str, position: int, as_of_event_id: str, now: datetime
    ) -> str:
        if position < 0:
            raise ValueError("cursor position must not be negative")
        expires = int((now.astimezone(UTC) + self._ttl).timestamp())
        payload = _canonical(
            {"v": 1, "t": tool, "q": digest, "p": position, "a": as_of_event_id, "e": expires}
        )
        return f"{PREFIX}.{_b64(payload)}.{_b64(self._sign(payload))}"

    def decode(
        self, cursor: str, *, tool: str, digest: str, as_of_event_id: str, now: datetime
    ) -> CursorPosition:
        if len(cursor) > MAX_CURSOR_CHARS:
            raise CursorInvalid("cursor is malformed")
        prefix, _, rest = cursor.partition(".")
        encoded, _, signature = rest.partition(".")
        if prefix != PREFIX or not encoded or not signature:
            raise CursorInvalid("cursor is malformed")
        payload = _unb64(encoded)
        if not hmac.compare_digest(_unb64(signature), self._sign(payload)):
            raise CursorInvalid("cursor signature does not verify")
        fields = _fields(payload)
        if fields["t"] != tool or not hmac.compare_digest(str(fields["q"]), digest):
            raise CursorMismatch("cursor was issued for another query")
        if int(now.astimezone(UTC).timestamp()) >= fields["e"]:
            raise CursorExpired("cursor has expired")
        if fields["a"] != as_of_event_id:
            raise CursorStale("the projection changed since the cursor was issued")
        return CursorPosition(fields["p"], fields["a"])

    def _sign(self, payload: bytes) -> bytes:
        return hmac.new(self._key, _DOMAIN + payload, hashlib.sha256).digest()


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError):
        raise CursorInvalid("cursor is malformed") from None


def _fields(payload: bytes) -> dict[str, Any]:
    """The verified payload's fields; a signed but ill-formed payload is still rejected."""
    try:
        fields = json.loads(payload)
    except (UnicodeDecodeError, ValueError):
        raise CursorInvalid("cursor is malformed") from None
    if (
        not isinstance(fields, dict)
        or set(fields) != _FIELDS
        or fields["v"] != 1
        or not all(isinstance(fields[k], str) for k in ("t", "q", "a"))
        or not all(type(fields[k]) is int for k in ("p", "e"))
        or fields["p"] < 0
    ):
        raise CursorInvalid("cursor is malformed")
    return fields
