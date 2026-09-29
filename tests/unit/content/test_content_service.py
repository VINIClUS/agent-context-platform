"""Unit tests for ``ContentService``: ``prepare()``, ``attach()``, ``PreparedContent``.

These tests fake ``BlobStore`` and ``AsyncSession`` -- no Postgres or S3
required -- so they run in the coverage-gated CI job that has no external
services attached. ``tests/integration/content/test_service.py`` covers the
same surface against real Postgres and S3 (byte-boundary storage routing
read back from real backends, foreign-key enforcement, and the advisory-lock
races that only a real database can prove).

The REQUIRED canaries test (see the task card) lives here too: canary
payloads are always inline-sized, so `prepare()` never touches the blob
store, and this is the only test job that always runs.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
from typing import Any

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    ContentRefV1,
    ContentStorage,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
    canonical_json_bytes,
    redact_json,
)
from sqlalchemy import TextClause

from agent_context_platform.content.blob_store import BlobNotFoundError, StoredBlob
from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.service import (
    _MAX_JSON_DEPTH,
    _OBJECT_STORAGE_MEDIA_TYPE,
    ContentRequiresRedactionError,
    ContentResolutionError,
    ContentService,
    DuplicateContentIdError,
    InvalidContentEncodingError,
    PreparedContent,
    UnsupportedMediaTypeError,
    _canonicalize_or_reject,
    _decode_utf8_strict,
    _parse_json_strict,
    _PreparedItem,
    content_digest_lock_key,
)

pytestmark = pytest.mark.unit

_POLICY = RedactionPolicyV1()

#: Canary values from agent-context-sdk's tests/fixtures/redaction/canaries.json,
#: embedded literally: the SDK ships as a wheel (see pyproject.toml's
#: [tool.uv.sources]), so its tests/ directory is not available at CI time.
#: The AWS-key-shaped and high-entropy canaries below are built with `+`
#: (never adjacent string literals, which ruff's formatter would rejoin) so
#: the byte-identical values never appear as one literal in the source --
#: GitHub push protection's secret scanner otherwise flags these fake keys
#: as real AWS credentials.
_CANARIES: tuple[str, ...] = (
    "AGENT_CONTEXT_CANARY_AWS_" + "AKIA" + "ABCDEFGHIJKLMNOP",
    "AGENT_CONTEXT_CANARY_AWS_" + "ASIA" + "QRSTUVWXYZABCDEF",
    "AGENT_CONTEXT_CANARY_GITHUB_ghp_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd",
    "AGENT_CONTEXT_CANARY_GITHUB_gho_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd",
    "AGENT_CONTEXT_CANARY_GITHUB_ghu_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd",
    "AGENT_CONTEXT_CANARY_GITHUB_ghs_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd",
    "AGENT_CONTEXT_CANARY_GITHUB_ghr_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd",
    "AGENT_CONTEXT_CANARY_GITHUB_github_pat_11AA22BB33CC44DD55EE66FF77GG88HH99II00JJ",
    "AGENT_CONTEXT_CANARY_OPENAI_sk-proj-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdef",
    "AGENT_CONTEXT_CANARY_JWT_eyJhbGciOiJIUzI1NiJ9"
    ".eyJzdWIiOiJhZ2VudC1jb250ZXh0LWNhbmFyeSJ9.c2lnbmF0dXJlLWNhbmFyeS12YWx1ZQ",
    "AGENT_CONTEXT_CANARY_PEM_\n"
    "-----BEGIN PRIVATE KEY-----\n"
    "QUdFTlRfQ09OVEVYVF9DQU5BUllfTk9UX0FfUkVBTF9LRVk=\n"
    "-----END PRIVATE KEY-----",
    "AGENT_CONTEXT_CANARY_URL_https://agent:canary-password@example.invalid/private",
    "AGENT_CONTEXT_CANARY_DSN_Server=db.invalid;User=agent;Password=canary-dsn-password;"
    "Database=context",
    "AGENT_CONTEXT_CANARY_ENTROPY_:" + "aB3dE5fG7hJ9kL2mN4pQ" + "6rS8tU0vW1xY3zA5cD7f",
)


# ---------------------------------------------------------------------------
# Helpers and fakes
# ---------------------------------------------------------------------------


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _claim(content_id: str, data: bytes, media_type: str = "application/json") -> ContentClaimV1:
    return ContentClaimV1(
        content_id=content_id,
        content_sha256=_sha256_hex(data),
        media_type=media_type,
        uncompressed_bytes=len(data),
    )


def _sanitized_report() -> RedactionReportV1:
    return RedactionReportV1(policy_version="1.0.0", disposition=ContentDisposition.SANITIZED)


def _item(
    content_id: str,
    data: bytes,
    *,
    media_type: str = "application/json",
    report: RedactionReportV1 | None = None,
) -> SanitizedContentItemV1:
    return SanitizedContentItemV1(
        claim=_claim(content_id, data, media_type),
        sanitized_bytes_base64=base64.b64encode(data).decode("ascii"),
        redaction_report=report if report is not None else _sanitized_report(),
    )


def _json_item(
    content_id: str, value: Any, *, report: RedactionReportV1 | None = None
) -> SanitizedContentItemV1:
    return _item(
        content_id, canonical_json_bytes(value), media_type="application/json", report=report
    )


def _nested_json_text(depth: int) -> str:
    """Raw JSON text for a value whose deepest node sits at ``depth`` (root = 0).

    Mirrors ``_exceeds_max_json_depth``'s own depth convention exactly: an
    empty list is depth 0, and each extra wrapping ``[...]`` adds one.
    """
    brackets = depth + 1
    return "[" * brackets + "]" * brackets


def _nested_value(depth: int) -> Any:
    """A Python structure whose deepest node sits at ``depth`` (root = 0).

    Same depth convention as ``_nested_json_text``, but as a Python object
    ready for ``_json_item``/``canonical_json_bytes`` rather than raw text.
    """
    value: Any = []
    for _ in range(depth):
        value = [value]
    return value


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


class FakeBlobStore:
    """Records every call; content-addressed just like the real store."""

    def __init__(self, call_log: list[str] | None = None) -> None:
        self.put_calls: list[tuple[bytes, str]] = []
        self.get_calls: list[tuple[str, str]] = []
        self.fail_get_for: set[str] = set()
        self._call_log = call_log if call_log is not None else []

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        self.put_calls.append((content, media_type))
        digest = hashlib.sha256(content).hexdigest()
        self._call_log.append(f"put_verified:{digest}")
        return StoredBlob(
            sha256=digest,
            object_key=f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst",
            compressed_bytes=max(len(content) // 2, 1),
            uncompressed_bytes=len(content),
            media_type=media_type,
        )

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        self._call_log.append(f"get_verified:{expected_sha256}")
        self.get_calls.append((object_key, expected_sha256))
        if expected_sha256 in self.fail_get_for:
            raise BlobNotFoundError("swept before attach could re-verify it")
        return b""

    async def delete(self, object_key: str) -> None:
        raise NotImplementedError


class FakeAsyncSession:
    """Records every ``execute()`` call; introspects bound params offline."""

    def __init__(self, call_log: list[str] | None = None) -> None:
        self.executed: list[tuple[Any, Any]] = []
        self._call_log = call_log if call_log is not None else []

    async def execute(self, clause: Any, params: Any = None) -> None:
        self.executed.append((clause, params))
        if isinstance(clause, TextClause):
            self._call_log.append(f"lock:{params['digest_key']}")
            return
        compiled_params = clause.compile().params
        table_name = clause.table.name
        if table_name == "content_objects":
            self._call_log.append(f"insert_object:{compiled_params['content_sha256']}")
        else:
            self._call_log.append(f"insert_inline:{compiled_params['inline_id']}")


# ---------------------------------------------------------------------------
# content_digest_lock_key
# ---------------------------------------------------------------------------


def test_content_digest_lock_key_known_values() -> None:
    namespace, digest_key = content_digest_lock_key("00000000" + "0" * 56)
    assert namespace == 0x434F_4E54
    assert digest_key == 0

    _, negative_key = content_digest_lock_key("ffffffff" + "0" * 56)
    assert negative_key == -1


def test_content_digest_lock_key_is_deterministic_and_digest_sensitive() -> None:
    digest_a = _sha256_hex(b"alpha")
    digest_b = _sha256_hex(b"beta")
    assert content_digest_lock_key(digest_a) == content_digest_lock_key(digest_a)
    assert content_digest_lock_key(digest_a) != content_digest_lock_key(digest_b)


# ---------------------------------------------------------------------------
# Decoding / parsing / canonicalization helpers
# ---------------------------------------------------------------------------


def test_decode_utf8_strict_accepts_valid_utf8() -> None:
    assert _decode_utf8_strict(b"hello") == "hello"


def test_decode_utf8_strict_rejects_invalid_utf8() -> None:
    with pytest.raises(InvalidContentEncodingError):
        _decode_utf8_strict(b"\xff\xfe\x00\x01")


def test_parse_json_strict_accepts_valid_json() -> None:
    assert _parse_json_strict('{"a": 1}') == {"a": 1}


def test_parse_json_strict_rejects_duplicate_keys() -> None:
    with pytest.raises(InvalidContentEncodingError):
        _parse_json_strict('{"a": 1, "a": 2}')


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity", '{"x": NaN}'])
def test_parse_json_strict_rejects_non_finite_constants(literal: str) -> None:
    with pytest.raises(InvalidContentEncodingError):
        _parse_json_strict(literal)


def test_parse_json_strict_rejects_deeply_nested_json() -> None:
    nested = "[" * 50_000 + "]" * 50_000
    with pytest.raises(InvalidContentEncodingError):
        _parse_json_strict(nested)


def test_parse_json_strict_accepts_json_at_the_max_depth() -> None:
    """The boundary itself is not rejected -- only content deeper than it.

    Regression test for a CI-only failure: ``json.loads``'s C scanner
    recurses in C, guarded against runaway C recursion by a check whose trip
    point was observed to vary by available C stack, not just by
    interpreter version. 50,000-deep input (the fixture above) raised
    ``RecursionError`` during parsing on every environment this suite ran on
    except one -- a GitHub Actions Python 3.14 runner, running the identical
    3.14.6 patch release as a local run that rejected the same input.
    ``_exceeds_max_json_depth`` is what makes rejection deterministic
    regardless: it runs whether or not ``json.loads`` itself raised.
    """
    parsed = _parse_json_strict(_nested_json_text(_MAX_JSON_DEPTH))
    assert parsed == _nested_value(_MAX_JSON_DEPTH)


def test_parse_json_strict_rejects_json_one_level_past_the_max_depth() -> None:
    with pytest.raises(InvalidContentEncodingError):
        _parse_json_strict(_nested_json_text(_MAX_JSON_DEPTH + 1))


def test_parse_json_strict_rejects_invalid_json() -> None:
    with pytest.raises(InvalidContentEncodingError):
        _parse_json_strict("{not json")


def test_canonicalize_or_reject_success() -> None:
    assert _canonicalize_or_reject({"a": 1}) == canonical_json_bytes({"a": 1})


def test_canonicalize_or_reject_wraps_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(value: Any) -> bytes:
        raise RuntimeError("simulated canonicalization failure")

    monkeypatch.setattr("agent_context_platform.content.service.canonical_json_bytes", _boom)
    with pytest.raises(InvalidContentEncodingError):
        _canonicalize_or_reject({"a": 1})


# ---------------------------------------------------------------------------
# ContentService.__init__
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_threshold", [0, -1, INLINE_MAX_BYTES + 1])
def test_content_service_rejects_invalid_inline_threshold(bad_threshold: int) -> None:
    with pytest.raises(ValueError, match="inline_threshold"):
        ContentService(FakeBlobStore(), _POLICY, inline_threshold=bad_threshold)


def test_content_service_accepts_boundary_and_small_inline_threshold() -> None:
    ContentService(FakeBlobStore(), _POLICY, inline_threshold=INLINE_MAX_BYTES)
    ContentService(FakeBlobStore(), _POLICY, inline_threshold=1)


# ---------------------------------------------------------------------------
# prepare(): validation branches
# ---------------------------------------------------------------------------


def test_prepare_rejects_duplicate_content_id() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item_a = _json_item("dup", "hello")
        item_b = _json_item("dup", "world")
        with pytest.raises(DuplicateContentIdError):
            await service.prepare([item_a, item_b])

    _run(exercise())


def test_prepare_rejects_invalid_base64() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        claim = _claim("bad-b64", b"placeholder")
        item = SanitizedContentItemV1.model_construct(
            claim=claim,
            sanitized_bytes_base64="!!!not-valid-base64!!!",
            redaction_report=_sanitized_report(),
        )
        with pytest.raises(InvalidContentEncodingError):
            await service.prepare([item])

    _run(exercise())


def test_prepare_rejects_unsupported_media_type() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item = _item("image", b"\x89PNG", media_type="image/png")
        with pytest.raises(UnsupportedMediaTypeError):
            await service.prepare([item])

    _run(exercise())


def test_prepare_rejects_invalid_utf8_content() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item = _item("bad-utf8", b"\xff\xfe\x00\x01", media_type="text/plain")
        with pytest.raises(InvalidContentEncodingError):
            await service.prepare([item])

    _run(exercise())


def test_prepare_rejects_content_requiring_redaction() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        # A raw, never-redacted secret under a sensitive field name: redact_json
        # will rewrite it, so this must never be accepted as "already sanitized".
        item = _json_item("raw-secret", {"password": "this-is-not-actually-redacted"})
        with pytest.raises(ContentRequiresRedactionError):
            await service.prepare([item])

    _run(exercise())


@pytest.mark.parametrize("mismatch", ["digest", "length"])
def test_prepare_rejects_bytes_that_do_not_match_the_claim(mismatch: str) -> None:
    """``model_construct`` skips validation, so prepare() must re-check the claim itself."""

    async def exercise() -> None:
        data = canonical_json_bytes({"real": "content-bytes"})
        other = canonical_json_bytes({"claimed": "some-other-bytes"})
        claim = _claim("forged", data if mismatch == "length" else other)
        if mismatch == "length":
            claim = claim.model_copy(update={"uncompressed_bytes": len(data) + 1})
        item = SanitizedContentItemV1.model_construct(
            claim=claim,
            sanitized_bytes_base64=base64.b64encode(data).decode("ascii"),
            redaction_report=_sanitized_report(),
        )
        store = FakeBlobStore()
        service = ContentService(store, _POLICY)

        with pytest.raises(ContentResolutionError) as failure:
            await service.prepare([item])

        assert "real" not in str(failure.value)
        assert "content-bytes" not in str(failure.value)
        assert store.put_calls == []

    _run(exercise())


def test_prepare_rejects_content_when_policy_raises_redaction_error() -> None:
    async def exercise() -> None:
        strict_policy = RedactionPolicyV1(max_depth=1)
        service = ContentService(FakeBlobStore(), strict_policy)
        item = _json_item("too-deep", {"a": {"b": "c"}})
        with pytest.raises(InvalidContentEncodingError):
            await service.prepare([item])

    _run(exercise())


def test_prepare_accepts_json_at_the_max_depth_end_to_end() -> None:
    """``_MAX_JSON_DEPTH`` never rejects content a valid policy would accept.

    ``RedactionPolicyV1.max_depth`` is bounded at 256 for every caller
    (``agent_context_sdk``'s own field constraint), so pairing the service
    with a policy configured at that same ceiling proves the parse-level
    gate and the redaction-level gate agree at the boundary, not just each
    in isolation.
    """

    async def exercise() -> None:
        policy = RedactionPolicyV1(max_depth=_MAX_JSON_DEPTH)
        service = ContentService(FakeBlobStore(), policy)
        item = _json_item("at-the-limit", _nested_value(_MAX_JSON_DEPTH))
        prepared = await service.prepare([item])
        assert prepared.content_ids == ("at-the-limit",)

    _run(exercise())


def test_prepare_rejects_json_one_level_past_the_max_depth_end_to_end() -> None:
    """Content past ``_MAX_JSON_DEPTH`` never reaches ``redact_json`` at all.

    Uses the default policy deliberately: rejection here comes from
    ``_parse_json_strict`` itself, before ``self._policy.max_depth`` is ever
    consulted, so the policy in effect cannot change this outcome.
    """

    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item = _json_item("past-the-limit", _nested_value(_MAX_JSON_DEPTH + 1))
        with pytest.raises(InvalidContentEncodingError):
            await service.prepare([item])

    _run(exercise())


def test_prepare_wraps_recursion_error_from_redact_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Defense-in-depth backstop: a bare ``RecursionError`` from ``redact_json``
    is still reported as ``InvalidContentEncodingError``, not left to escape
    ``prepare()`` uncaught. Not reachable through real input today -- content
    is already depth-bounded by ``_parse_json_strict`` before this call -- so
    exercised directly via monkeypatch, mirroring
    ``test_canonicalize_or_reject_wraps_unexpected_errors``.
    """

    def _boom(value: Any, policy: Any) -> Any:
        raise RecursionError("simulated redact_json stack exhaustion")

    monkeypatch.setattr("agent_context_platform.content.service.redact_json", _boom)

    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item = _json_item("triggers-backstop", {"a": 1})
        with pytest.raises(InvalidContentEncodingError):
            await service.prepare([item])

    _run(exercise())


def test_prepare_accepts_stable_json_content() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item = _json_item("stable", {"greeting": "hello world", "count": 3})
        prepared = await service.prepare([item])
        assert prepared.content_ids == ("stable",)

    _run(exercise())


def test_prepare_accepts_stable_text_plain_content() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item = _item("stable-text", b"just some boring text", media_type="text/plain")
        prepared = await service.prepare([item])
        assert prepared.content_ids == ("stable-text",)

    _run(exercise())


# ---------------------------------------------------------------------------
# prepare(): storage routing, dedupe, atomicity
# ---------------------------------------------------------------------------


def test_prepare_routes_boundary_bytes_inline_vs_object() -> None:
    async def exercise() -> None:
        blob_store = FakeBlobStore()
        service = ContentService(blob_store, _POLICY)
        inline_data = b"a" * INLINE_MAX_BYTES
        object_data = b"a" * (INLINE_MAX_BYTES + 1)
        inline_item = _item("inline", inline_data, media_type="text/plain")
        object_item = _item("object", object_data, media_type="text/plain")

        prepared = await service.prepare([inline_item, object_item])
        [inline_claim, object_claim] = [inline_item.claim, object_item.claim]
        [inline_ref, object_ref] = prepared.resolve([inline_claim, object_claim])

        assert inline_ref.storage is ContentStorage.INLINE
        assert inline_ref.inline_id == inline_claim.content_sha256
        assert inline_ref.content_sha256 == inline_claim.content_sha256

        assert object_ref.storage is ContentStorage.OBJECT
        assert object_ref.content_sha256 == object_claim.content_sha256
        assert blob_store.put_calls == [(object_data, _OBJECT_STORAGE_MEDIA_TYPE)]

    _run(exercise())


def test_prepare_dedupes_object_uploads_by_digest_across_media_types() -> None:
    async def exercise() -> None:
        blob_store = FakeBlobStore()
        service = ContentService(blob_store, _POLICY)
        # Same bytes, above the inline threshold, claimed under two different
        # media types by two different content_ids -- the regression proof for
        # the object-storage dedupe-by-digest fix.
        data = b'"' + b"x" * (INLINE_MAX_BYTES + 10) + b'"'
        item_json = _item("as-json", data, media_type="application/json")
        item_text = _item("as-text", data, media_type="text/plain")

        prepared = await service.prepare([item_json, item_text])
        [ref_json, ref_text] = prepared.resolve([item_json.claim, item_text.claim])

        assert len(blob_store.put_calls) == 1
        assert blob_store.put_calls[0] == (data, _OBJECT_STORAGE_MEDIA_TYPE)
        assert ref_json.media_type == "application/json"
        assert ref_text.media_type == "text/plain"
        assert ref_json.object_key == ref_text.object_key

    _run(exercise())


def test_prepare_is_atomic_when_a_later_item_fails_recheck() -> None:
    async def exercise() -> None:
        blob_store = FakeBlobStore()
        service = ContentService(blob_store, _POLICY)
        object_item = _item("would-upload", b"a" * (INLINE_MAX_BYTES + 1), media_type="text/plain")
        failing_item = _json_item("fails", {"password": "not-actually-redacted"})

        with pytest.raises(ContentRequiresRedactionError):
            await service.prepare([object_item, failing_item])

        assert blob_store.put_calls == []

    _run(exercise())


# ---------------------------------------------------------------------------
# REQUIRED: canaries sanitized by redact_json must pass prepare()
# ---------------------------------------------------------------------------


def test_prepare_accepts_sdk_sanitized_canary_payload() -> None:
    """Card requirement: canaries sanitized by the SDK must then pass prepare().

    Simulates the adapter's own sanitize step with a plain ``redact_json(value,
    policy)`` call (no ``known_secrets`` / ``correlation_key``): grepping
    agent-context-codex's src/ for ``redact_json`` finds no call site yet, so
    there is no real adapter call to match -- this uses the SDK's default
    signature.
    """

    async def exercise() -> None:
        payload = {
            "notes": [{"sample_value": canary, "index": i} for i, canary in enumerate(_CANARIES)]
        }
        result = redact_json(payload, _POLICY)
        # Guard against a vacuous pass: the platform recheck is only meaningful
        # if pass 1 actually found and rewrote something.
        assert len(result.findings) == len(_CANARIES)
        assert result.value != payload
        sanitized_bytes = canonical_json_bytes(result.value)
        for canary in _CANARIES:
            assert canary.encode() not in sanitized_bytes

        item = _json_item("canaries", result.value, report=result.report)
        service = ContentService(FakeBlobStore(), _POLICY)
        prepared = await service.prepare([item])
        assert prepared.content_ids == ("canaries",)

    _run(exercise())


def test_prepare_rejects_the_raw_canary_payload_directly() -> None:
    async def exercise() -> None:
        payload = {"notes": [{"sample_value": canary} for canary in _CANARIES]}
        item = _json_item("raw-canaries", payload)
        service = ContentService(FakeBlobStore(), _POLICY)
        with pytest.raises(ContentRequiresRedactionError):
            await service.prepare([item])

    _run(exercise())


def test_prepare_accepts_sdk_sanitized_canary_payload_as_text_plain() -> None:
    async def exercise() -> None:
        pem_canary = _CANARIES[10]  # multi-line PEM canary
        result = redact_json(pem_canary, _POLICY)
        assert result.findings
        assert result.value != pem_canary
        assert isinstance(result.value, str)

        item = _item(
            "pem-text", result.value.encode("utf-8"), media_type="text/plain", report=result.report
        )
        service = ContentService(FakeBlobStore(), _POLICY)
        prepared = await service.prepare([item])
        assert prepared.content_ids == ("pem-text",)

    _run(exercise())


def test_prepare_accepts_value_equal_to_its_own_redaction_placeholder() -> None:
    """A value that is exactly the engine's own placeholder is stable.

    Since SDK v0.3.3 the engine exempts text matching its own placeholder
    grammar, so re-redacting ``{"password": "<redacted:sensitive_field:1>"}``
    yields no finding and identical output; the value-equality recheck
    accepts it.
    """

    async def exercise() -> None:
        value = {"password": "<redacted:sensitive_field:1>"}
        result = redact_json(value, _POLICY)
        assert result.findings == ()
        assert result.value == value

        item = _json_item("self-stable-placeholder", value, report=result.report)
        service = ContentService(FakeBlobStore(), _POLICY)
        prepared = await service.prepare([item])
        assert prepared.content_ids == ("self-stable-placeholder",)

    _run(exercise())


# ---------------------------------------------------------------------------
# Formerly known SDK gaps, closed by SDK v0.3.3 (redaction is idempotent on
# its own output): already-sanitized shapes are stable across a second pass.
# ---------------------------------------------------------------------------


def test_prepare_accepts_pem_under_sensitive_field_name() -> None:
    async def exercise() -> None:
        pem_canary = _CANARIES[10]
        value = {"private_key": pem_canary}
        result = redact_json(value, _POLICY)
        item = _json_item("pem-under-private-key", result.value, report=result.report)
        service = ContentService(FakeBlobStore(), _POLICY)
        await service.prepare([item])

    _run(exercise())


def test_prepare_accepts_known_secret_under_sensitive_field_name() -> None:
    async def exercise() -> None:
        secret = "s3cr3t-known-value-1234567890"
        value = {"password": secret}
        result = redact_json(value, _POLICY, known_secrets=[secret])
        item = _json_item("known-secret-under-password", result.value, report=result.report)
        service = ContentService(FakeBlobStore(), _POLICY)
        await service.prepare([item])

    _run(exercise())


# ---------------------------------------------------------------------------
# PreparedContent.resolve()
# ---------------------------------------------------------------------------


def test_resolve_returns_refs_for_a_subset_of_claims_in_claim_order() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item_a = _json_item("a", "alpha")
        item_b = _json_item("b", "beta")
        prepared = await service.prepare([item_a, item_b])

        [ref_a] = prepared.resolve([item_a.claim])
        assert ref_a.content_id == "a"

        [ref_b, ref_a_again] = prepared.resolve([item_b.claim, item_a.claim])
        assert (ref_b.content_id, ref_a_again.content_id) == ("b", "a")

    _run(exercise())


def test_resolve_rejects_duplicate_content_id_in_claims() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item_a = _json_item("a", "alpha")
        prepared = await service.prepare([item_a])
        with pytest.raises(ContentResolutionError):
            prepared.resolve([item_a.claim, item_a.claim])

    _run(exercise())


def test_resolve_rejects_unknown_content_id() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item_a = _json_item("a", "alpha")
        prepared = await service.prepare([item_a])
        unknown_claim = _claim("never-prepared", b"data")
        with pytest.raises(ContentResolutionError):
            prepared.resolve([unknown_claim])

    _run(exercise())


def test_resolve_rejects_mismatched_claim_metadata() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item_a = _json_item("a", "alpha")
        prepared = await service.prepare([item_a])
        mismatched_claim = item_a.claim.model_copy(update={"uncompressed_bytes": 999})
        with pytest.raises(ContentResolutionError):
            prepared.resolve([mismatched_claim])

    _run(exercise())


def test_report_for_returns_the_items_report() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        report = _sanitized_report()
        item_a = _json_item("a", "alpha", report=report)
        prepared = await service.prepare([item_a])
        assert prepared.report_for("a") == report
        assert prepared.redaction_reports == {"a": report}

    _run(exercise())


def test_report_for_unknown_content_id_raises() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        item_a = _json_item("a", "alpha")
        prepared = await service.prepare([item_a])
        with pytest.raises(ContentResolutionError):
            prepared.report_for("nope")

    _run(exercise())


# ---------------------------------------------------------------------------
# ContentService.attach()
# ---------------------------------------------------------------------------


def test_attach_inserts_inline_items() -> None:
    async def exercise() -> None:
        call_log: list[str] = []
        blob_store = FakeBlobStore(call_log)
        service = ContentService(blob_store, _POLICY)
        item = _json_item("inline-item", "small value")
        prepared = await service.prepare([item])

        session = FakeAsyncSession(call_log)
        await service.attach(session, prepared)

        digest = item.claim.content_sha256
        assert call_log == [f"insert_inline:{digest}"]
        [(clause, _)] = session.executed
        params = clause.compile().params
        assert params["inline_id"] == digest
        assert params["content_sha256"] == digest
        assert params["media_type"] == "application/json"

    _run(exercise())


def test_attach_locks_reverifies_and_inserts_object_items() -> None:
    async def exercise() -> None:
        call_log: list[str] = []
        blob_store = FakeBlobStore(call_log)
        service = ContentService(blob_store, _POLICY)
        item = _item("object-item", b"a" * (INLINE_MAX_BYTES + 1), media_type="text/plain")
        prepared = await service.prepare([item])
        # `prepare()` already called `put_verified` for this object-sized item;
        # clear the shared log so the assertion below covers only `attach()`.
        call_log.clear()

        session = FakeAsyncSession(call_log)
        await service.attach(session, prepared)

        digest = item.claim.content_sha256
        _, digest_key = content_digest_lock_key(digest)
        assert call_log == [
            f"lock:{digest_key}",
            f"get_verified:{digest}",
            f"insert_object:{digest}",
        ]
        insert_calls = [
            (clause, params)
            for clause, params in session.executed
            if not isinstance(clause, TextClause)
        ]
        [(clause, _)] = insert_calls
        compiled = clause.compile().params
        assert compiled["content_sha256"] == digest
        # The stored media type is the fixed constant, not the claimed one --
        # see the module docstring for why.
        assert compiled["media_type"] == _OBJECT_STORAGE_MEDIA_TYPE

    _run(exercise())


def test_attach_locks_and_inserts_multiple_digests_in_sorted_order() -> None:
    async def exercise() -> None:
        call_log: list[str] = []
        blob_store = FakeBlobStore(call_log)
        service = ContentService(blob_store, _POLICY)
        # Choose payloads whose digests sort in a *different* order than the
        # items were prepared in, so a naive "prepared order" implementation
        # would fail this assertion.
        items = [
            _item(
                f"item-{i}",
                f"payload-{i}-".encode() + b"a" * INLINE_MAX_BYTES,
                media_type="text/plain",
            )
            for i in range(4)
        ]
        prepared = await service.prepare(items)
        digests = sorted(item.claim.content_sha256 for item in items)

        session = FakeAsyncSession(call_log)
        await service.attach(session, prepared)

        lock_order = [entry for entry in call_log if entry.startswith("lock:")]
        expected_lock_order = [f"lock:{content_digest_lock_key(digest)[1]}" for digest in digests]
        assert lock_order == expected_lock_order

    _run(exercise())


def test_attach_handles_mixed_inline_and_object_items() -> None:
    async def exercise() -> None:
        call_log: list[str] = []
        blob_store = FakeBlobStore(call_log)
        service = ContentService(blob_store, _POLICY)
        inline_item = _json_item("inline", "small")
        object_item = _item("object", b"a" * (INLINE_MAX_BYTES + 1), media_type="text/plain")
        prepared = await service.prepare([inline_item, object_item])

        session = FakeAsyncSession(call_log)
        await service.attach(session, prepared)

        assert len(session.executed) == 3  # lock + object insert + inline insert
        table_names = {
            clause.table.name
            for clause, _ in session.executed
            if not isinstance(clause, TextClause)
        }
        assert table_names == {"content_objects", "inline_contents"}

    _run(exercise())


def test_attach_raises_assertion_error_for_a_malformed_prepared_item() -> None:
    async def exercise() -> None:
        service = ContentService(FakeBlobStore(), _POLICY)
        claim = _claim("broken", b"data")
        ref = ContentRefV1(
            content_id=claim.content_id,
            content_sha256=claim.content_sha256,
            media_type=claim.media_type,
            uncompressed_bytes=claim.uncompressed_bytes,
            disposition=ContentDisposition.SANITIZED,
            storage=ContentStorage.INLINE,
            inline_id=claim.content_sha256,
        )
        broken_item = _PreparedItem(
            claim=claim, ref=ref, redaction_report=_sanitized_report(), data=None, stored_blob=None
        )
        prepared = PreparedContent((broken_item,))

        with pytest.raises(AssertionError):
            await service.attach(FakeAsyncSession(), prepared)

    _run(exercise())


def test_attach_propagates_blob_not_found_when_reverify_fails() -> None:
    async def exercise() -> None:
        blob_store = FakeBlobStore()
        service = ContentService(blob_store, _POLICY)
        item = _item("swept-away", b"a" * (INLINE_MAX_BYTES + 1), media_type="text/plain")
        prepared = await service.prepare([item])
        blob_store.fail_get_for.add(item.claim.content_sha256)

        with pytest.raises(BlobNotFoundError):
            await service.attach(FakeAsyncSession(), prepared)

    _run(exercise())
