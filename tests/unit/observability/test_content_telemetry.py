"""Content objects and attach-time integrity failures are counted, never their bytes."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from typing import Any

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
)

from agent_context_platform.content.blob_store import (
    BlobIntegrityError,
    BlobNotFoundError,
    StoredBlob,
)
from agent_context_platform.content.service import (
    ContentRequiresRedactionError,
    ContentService,
    InvalidContentEncodingError,
)
from agent_context_platform.observability import contract, record_content_object

from ._capture import Canaries, Capture

pytestmark = pytest.mark.unit

_INLINE_THRESHOLD = 64


class MemoryBlobStore:
    def __init__(self) -> None:
        self.failure: Exception | None = None

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        digest = hashlib.sha256(content).hexdigest()
        key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"
        return StoredBlob(digest, key, len(content), len(content), media_type)

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        if self.failure is not None:
            raise self.failure
        return b""

    async def delete(self, object_key: str) -> None:
        raise NotImplementedError


class RecordingSession:
    async def execute(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def _item(content_id: str, data: bytes, media_type: str = "text/plain") -> SanitizedContentItemV1:
    return SanitizedContentItemV1(
        claim=ContentClaimV1(
            content_id=content_id,
            content_sha256=hashlib.sha256(data).hexdigest(),
            media_type=media_type,
            uncompressed_bytes=len(data),
        ),
        sanitized_bytes_base64=base64.b64encode(data).decode(),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )


def _service(store: MemoryBlobStore) -> ContentService:
    return ContentService(store, RedactionPolicyV1(), inline_threshold=_INLINE_THRESHOLD)


def test_items_are_counted_by_storage_and_result(telemetry: Capture, canaries: Canaries) -> None:
    large = (canaries.content * 8).encode()
    assert len(large) > _INLINE_THRESHOLD
    items = [
        _item("inline", canaries.prompt.encode()),
        _item("object-1", large),
        _item("object-2", large),  # same digest: the batch reuses the first upload
    ]

    prepared = asyncio.run(_service(MemoryBlobStore()).prepare(items))

    # Counted by the ingestion service once its transaction commits, never during prepare.
    assert telemetry.points(contract.CONTENT_OBJECTS) == {}
    assert prepared.storage_outcomes() == (
        ("inline", "stored"),
        ("object", "stored"),
        ("object", "reused"),
    )
    for storage, result in prepared.storage_outcomes():
        record_content_object(storage, result)
    assert telemetry.value(contract.CONTENT_OBJECTS, storage="inline", result="stored") == 1
    assert telemetry.value(contract.CONTENT_OBJECTS, storage="object", result="stored") == 1
    assert telemetry.value(contract.CONTENT_OBJECTS, storage="object", result="reused") == 1
    span = telemetry.span("agent_context.content.prepare")
    assert dict(span.attributes or {}) == {"agent_context.item_count": 3}
    canaries.assert_absent(telemetry.dump())


@pytest.mark.parametrize(
    ("data", "media_type", "storage", "error"),
    [
        (b"\xff\xfe not utf-8", "text/plain", "inline", InvalidContentEncodingError),
        (b"x" * 100 + b"\xff", "text/plain", "object", InvalidContentEncodingError),
        (
            b'{"api_key": "sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"}',
            "application/json",
            "inline",
            ContentRequiresRedactionError,
        ),
    ],
)
def test_a_rejected_item_is_counted_once_with_its_storage(
    telemetry: Capture,
    canaries: Canaries,
    data: bytes,
    media_type: str,
    storage: str,
    error: type[Exception],
) -> None:
    items = [_item("ok", canaries.prompt.encode()), _item("bad", data, media_type)]

    with pytest.raises(error):
        asyncio.run(_service(MemoryBlobStore()).prepare(items))

    assert telemetry.points(contract.CONTENT_OBJECTS) == {
        (("result", "rejected"), ("storage", storage)): 1
    }
    span = telemetry.span("agent_context.content.prepare")
    assert (span.attributes or {})["error.type"] == error.__name__
    canaries.assert_absent(telemetry.dump())


@pytest.mark.parametrize(
    ("failure", "check"),
    [
        (BlobNotFoundError("gone"), "missing_object"),
        (BlobIntegrityError("digest differs"), "digest_mismatch"),
    ],
)
def test_attach_counts_integrity_failures(
    telemetry: Capture, canaries: Canaries, failure: Exception, check: str
) -> None:
    store = MemoryBlobStore()
    service = _service(store)
    prepared = asyncio.run(service.prepare([_item("o", (canaries.content * 8).encode())]))
    store.failure = failure

    with pytest.raises(type(failure)):
        asyncio.run(service.attach(RecordingSession(), prepared))  # type: ignore[arg-type]

    assert telemetry.points(contract.LEDGER_INTEGRITY_FAILURES) == {(("check", check),): 1}
