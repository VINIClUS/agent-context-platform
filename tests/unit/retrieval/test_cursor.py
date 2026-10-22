"""Signed, stateless context cursors (PLATFORM-043): tampering, expiry, foreign keys, mismatches."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
from agent_context_sdk.contracts.mcp import ContextForTaskInputV1, DecisionHistoryInputV1
from pydantic import SecretStr, ValidationError

from agent_context_platform.retrieval.cursor import (
    MAX_CURSOR_CHARS,
    CursorCodec,
    CursorExpired,
    CursorInvalid,
    CursorMismatch,
    CursorPosition,
    CursorStale,
    query_digest,
)
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit

KEY = SecretStr("cursor-test-key-0123456789abcdef-0123")
OTHER_KEY = SecretStr("another-cursor-key-0123456789abcdef-99")
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
AS_OF = "0198a4b1-98c0-7c28-ae3f-000000000001"
BUDGET = {"max_items": 5, "max_bytes": 10_000, "max_tokens": 2_500}


def task(text: str = "fix the flaky test", cursor: str | None = None) -> ContextForTaskInputV1:
    return ContextForTaskInputV1.model_validate(
        {
            "repository_id": "repo-1",
            "revision": {"kind": "commit", "commit_id": "a" * 40},
            "task": text,
            "budget": BUDGET,
            "cursor": cursor,
        }
    )


def issue(codec: CursorCodec, digest: str, *, position: int = 3, now: datetime = NOW) -> str:
    return codec.encode(
        tool="context_for_task", digest=digest, position=position, as_of_event_id=AS_OF, now=now
    )


def decode(codec: CursorCodec, cursor: str, digest: str, **overrides: object) -> int:
    arguments: dict[str, object] = {
        "tool": "context_for_task",
        "digest": digest,
        "as_of_event_id": AS_OF,
        "now": NOW,
    }
    arguments.update(overrides)
    return codec.decode(cursor, **arguments).position  # type: ignore[arg-type]


def test_a_cursor_round_trips_and_is_opaque() -> None:
    codec = CursorCodec(KEY)
    digest = query_digest("context_for_task", task(), "principal-1")

    cursor = issue(codec, digest)

    assert decode(codec, cursor, digest) == 3
    assert cursor.startswith("ctx1.")
    assert "repo-1" not in cursor and "flaky" not in cursor
    assert len(cursor) < MAX_CURSOR_CHARS


def test_the_digest_binds_every_input_but_the_cursor_plus_tool_and_principal() -> None:
    base = query_digest("context_for_task", task(), "principal-1")

    assert query_digest("context_for_task", task(cursor="ctx1.x.y"), "principal-1") == base
    assert query_digest("context_for_task", task("other task"), "principal-1") != base
    assert query_digest("context_for_task", task(), "principal-2") != base
    assert query_digest("symbol_context", task(), "principal-1") != base


def test_a_replica_with_the_same_key_accepts_the_cursor() -> None:
    digest = query_digest("context_for_task", task(), "principal-1")

    cursor = issue(CursorCodec(KEY), digest)

    assert decode(CursorCodec(KEY), cursor, digest) == 3


def test_a_cursor_signed_with_another_key_is_invalid() -> None:
    digest = query_digest("context_for_task", task(), "principal-1")
    cursor = issue(CursorCodec(OTHER_KEY), digest)

    with pytest.raises(CursorInvalid):
        decode(CursorCodec(KEY), cursor, digest)


def _reencode(cursor: str, **changes: object) -> str:
    prefix, payload, signature = cursor.split(".")
    fields = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    fields.update(changes)
    raw = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()
    return ".".join((prefix, base64.urlsafe_b64encode(raw).rstrip(b"=").decode(), signature))


@pytest.mark.parametrize("change", [{"p": 0}, {"p": 999}, {"a": "other"}, {"e": 4_000_000_000}])
def test_an_edited_payload_fails_the_signature(change: dict[str, object]) -> None:
    codec = CursorCodec(KEY)
    digest = query_digest("context_for_task", task(), "principal-1")

    with pytest.raises(CursorInvalid):
        decode(codec, _reencode(issue(codec, digest), **change), digest)


@pytest.mark.parametrize(
    "cursor",
    [
        "",
        "ctx1",
        "ctx1.",
        "ctx1..sig",
        "ctx2.e30.e30",
        "ctx1.!!!.???",
        "ctx1.e30.%%%",
        "x" * (MAX_CURSOR_CHARS + 1),
    ],
)
def test_malformed_cursors_are_invalid(cursor: str) -> None:
    with pytest.raises(CursorInvalid):
        decode(CursorCodec(KEY), cursor, "0" * 64)


def _signed(codec: CursorCodec, raw: bytes) -> str:
    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    return f"ctx1.{b64(raw)}.{b64(codec._sign(raw))}"


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"\xff\xfe",
        b"[]",
        b'{"v":1}',
        b'{"a":"x","e":1,"p":1,"q":"x","t":"x","v":2}',
        b'{"a":"x","e":1,"p":-1,"q":"x","t":"x","v":1}',
        b'{"a":"x","e":1,"p":true,"q":"x","t":"x","v":1}',
        b'{"a":1,"e":1,"p":1,"q":"x","t":"x","v":1}',
        b'{"a":"x","e":1,"p":1,"q":"x","t":"x","v":1,"z":0}',
    ],
)
def test_a_validly_signed_but_ill_formed_payload_is_still_invalid(raw: bytes) -> None:
    codec = CursorCodec(KEY)

    with pytest.raises(CursorInvalid):
        decode(codec, _signed(codec, raw), "0" * 64)


def test_an_expired_cursor_is_rejected() -> None:
    codec = CursorCodec(KEY, ttl=timedelta(minutes=5))
    digest = query_digest("context_for_task", task(), "principal-1")
    cursor = issue(codec, digest)

    assert decode(codec, cursor, digest, now=NOW + timedelta(minutes=4)) == 3
    with pytest.raises(CursorExpired):
        decode(codec, cursor, digest, now=NOW + timedelta(minutes=5))


def test_a_cursor_for_another_query_tool_or_principal_is_a_mismatch() -> None:
    codec = CursorCodec(KEY)
    digest = query_digest("context_for_task", task(), "principal-1")
    cursor = issue(codec, digest)

    for other in (
        query_digest("context_for_task", task("another task"), "principal-1"),
        query_digest("context_for_task", task(), "principal-2"),
    ):
        with pytest.raises(CursorMismatch):
            decode(codec, cursor, other)
    with pytest.raises(CursorMismatch):
        decode(codec, cursor, digest, tool="symbol_context")


def test_a_cursor_from_another_projection_snapshot_is_stale() -> None:
    codec = CursorCodec(KEY)
    digest = query_digest("context_for_task", task(), "principal-1")

    with pytest.raises(CursorStale):
        decode(codec, issue(codec, digest), digest, as_of_event_id="another-snapshot")


def test_errors_never_echo_the_cursor_or_the_query() -> None:
    codec = CursorCodec(KEY)
    digest = query_digest("context_for_task", task("secret-ish task words"), "principal-1")
    cursor = issue(codec, digest)
    cases: tuple[tuple[type[Exception], str, dict[str, object]], ...] = (
        (CursorMismatch, "f" * 64, {}),
        (CursorStale, digest, {"as_of_event_id": "x"}),
        (CursorExpired, digest, {"now": NOW + timedelta(days=1)}),
    )
    for error_type, expected, overrides in cases:
        with pytest.raises(error_type) as raised:
            decode(codec, cursor, expected, **overrides)
        assert cursor not in str(raised.value) and "secret-ish" not in str(raised.value)


def test_key_and_ttl_are_validated_and_a_missing_key_is_ephemeral() -> None:
    with pytest.raises(ValueError, match="at least 32"):
        CursorCodec(SecretStr("short"))
    with pytest.raises(ValueError, match="ttl"):
        CursorCodec(KEY, ttl=timedelta(0))
    with pytest.raises(ValueError, match="position"):
        CursorCodec(KEY).encode(tool="t", digest="d", position=-1, as_of_event_id="a", now=NOW)

    digest = query_digest("decision_history", _decisions(), "p")
    first, second = CursorCodec.from_key(None), CursorCodec.from_key(None)
    cursor = first.encode(tool="t", digest=digest, position=1, as_of_event_id="a", now=NOW)
    assert first.decode(cursor, tool="t", digest=digest, as_of_event_id="a", now=NOW).position == 1
    with pytest.raises(CursorInvalid):  # per-process keys never agree
        second.decode(cursor, tool="t", digest=digest, as_of_event_id="a", now=NOW)
    configured = CursorCodec.from_key(KEY)
    issued = CursorCodec(KEY).encode(
        tool="t", digest=digest, position=2, as_of_event_id="a", now=NOW
    )
    assert configured.decode(issued, tool="t", digest=digest, as_of_event_id="a", now=NOW) == (
        CursorPosition(2, "a")
    )


def _decisions() -> DecisionHistoryInputV1:
    return DecisionHistoryInputV1.model_validate(
        {
            "scope": {"kind": "project", "project_id": "p"},
            "query": "q",
            "valid_at": NOW.isoformat(),
            "budget": BUDGET,
        }
    )


def test_the_settings_key_is_secret_long_and_required_in_production() -> None:
    settings = Settings(mcp={"cursor_hmac_key": "cursor-settings-key-0123456789abcdef"})
    assert "cursor-settings-key" not in repr(settings)
    assert settings.mcp.cursor_hmac_key is not None

    with pytest.raises(ValidationError) as short:
        Settings(mcp={"cursor_hmac_key": "short-cursor-key"})
    assert "at least 32 characters" in str(short.value)
    assert "short-cursor-key" not in str(short.value)

    with pytest.raises(ValidationError) as missing:
        Settings(environment="production")
    assert "mcp.cursor_hmac_key" in str(missing.value)
