"""`ContentService.read`: the sanitized-content read path the search projector uses."""

from __future__ import annotations

import asyncio
import hashlib
from types import SimpleNamespace
from typing import Any

import pytest
from agent_context_sdk import ContentDisposition, ContentRefV1, ContentStorage, RedactionPolicyV1

from agent_context_platform.content.blob_store import BlobNotFoundError, StoredBlob
from agent_context_platform.content.models import InlineContentRow
from agent_context_platform.content.service import ContentService, ContentUnavailableError

pytestmark = pytest.mark.unit

DATA = b"sanitized text"
DIGEST = hashlib.sha256(DATA).hexdigest()


class Session:
    def __init__(self, stored: bytes | None) -> None:
        self.stored = stored
        self.requested: list[tuple[Any, Any]] = []

    async def get(self, model: Any, key: Any) -> Any:
        self.requested.append((model, key))
        return None if self.stored is None else SimpleNamespace(data=self.stored)


class Blobs:
    def __init__(self, result: bytes | Exception) -> None:
        self.result = result
        self.requested: list[tuple[str, str]] = []

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        raise AssertionError("read never writes")

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        self.requested.append((object_key, expected_sha256))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    async def delete(self, object_key: str) -> None:
        raise AssertionError("read never deletes")


def _service(blobs: Blobs | None = None) -> ContentService:
    return ContentService(blobs or Blobs(b""), RedactionPolicyV1())  # type: ignore[arg-type]


def _ref(storage: ContentStorage, **changes: Any) -> ContentRefV1:
    values: dict[str, Any] = {
        "content_id": "c-1",
        "content_sha256": DIGEST,
        "media_type": "text/plain",
        "uncompressed_bytes": len(DATA),
        "disposition": ContentDisposition.SANITIZED,
        "storage": storage,
    }
    if storage is ContentStorage.INLINE:
        values["inline_id"] = DIGEST
    else:
        values.update(object_key=f"sha256/{DIGEST[:2]}/{DIGEST[2:4]}/{DIGEST}.zst", encoding="zstd")
    values.update(changes)
    return ContentRefV1(**values)


def test_inline_content_is_read_through_the_callers_session_and_verified() -> None:
    session = Session(DATA)

    data = asyncio.run(_service().read(session, _ref(ContentStorage.INLINE)))  # type: ignore[arg-type]

    assert data == DATA
    assert session.requested == [(InlineContentRow, DIGEST)]


@pytest.mark.parametrize("stored", [None, b"tampered text", b"short"])
def test_missing_or_mismatching_inline_content_is_unavailable(stored: bytes | None) -> None:
    with pytest.raises(ContentUnavailableError) as raised:
        asyncio.run(_service().read(Session(stored), _ref(ContentStorage.INLINE)))  # type: ignore[arg-type]

    assert DATA.decode() not in str(raised.value)  # content-free messages


def test_object_content_goes_through_the_verifying_blob_store() -> None:
    blobs = Blobs(DATA)
    ref = _ref(ContentStorage.OBJECT)

    assert asyncio.run(_service(blobs).read(Session(None), ref)) == DATA  # type: ignore[arg-type]
    assert blobs.requested == [(ref.object_key, DIGEST)]


def test_object_content_that_is_gone_or_does_not_match_is_unavailable() -> None:
    ref = _ref(ContentStorage.OBJECT)

    with pytest.raises(ContentUnavailableError, match="could not be read"):
        asyncio.run(_service(Blobs(BlobNotFoundError("gone"))).read(Session(None), ref))  # type: ignore[arg-type]
    with pytest.raises(ContentUnavailableError, match="does not match"):
        asyncio.run(_service(Blobs(b"other bytes!!!")).read(Session(None), ref))  # type: ignore[arg-type]
    no_key = ref.model_copy(update={"object_key": None})
    with pytest.raises(ContentUnavailableError, match="no object key"):
        asyncio.run(_service(Blobs(DATA)).read(Session(None), no_key))  # type: ignore[arg-type]
