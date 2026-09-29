"""Integration tests for ``ContentService`` against real Postgres and S3.

Complements ``tests/unit/content/test_content_service.py`` (fakes, no
external services): this module proves the same byte-boundary storage
routing and dedupe-by-digest design actually round-trip through the real
backends, and that the ``ledger.event_content_refs`` storage foreign keys
enforce "no reference may dangle" at the database level -- not just in the
unit tests' fakes.

Every payload embeds a ``time.time_ns()`` decimal nonce (never a
``uuid4().hex`` string, which risks being misidentified as a high-entropy
secret by the redaction engine) so repeated runs against the same
long-lived Garage bucket never collide on content-addressed keys.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time
from typing import Any

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    RedactionReportV1,
    SanitizedContentItemV1,
    canonical_json_bytes,
)
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.blob_store import BlobNotFoundError, S3BlobStore
from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.service import ContentRequiresRedactionError, ContentService

pytestmark = pytest.mark.integration


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _sized_payload(*, size: int, nonce: int) -> bytes:
    """`size` bytes of low-entropy ASCII, unique per `nonce`."""
    prefix = f"nonce={nonce} size={size} ".encode("ascii")
    assert len(prefix) <= size
    return prefix + b"a" * (size - len(prefix))


def _json_string_payload(nonce: int, filler_len: int) -> bytes:
    """Bytes that are simultaneously valid ``text/plain`` and strict JSON.

    A JSON-encoded string literal is valid UTF-8 text on its own (so it
    passes the ``text/plain`` recheck) and also parses as strict JSON (so it
    passes the ``application/json`` recheck) -- letting the same bytes be
    claimed under either media type, which is exactly what the dedupe test
    below needs.
    """
    value = f"nonce-{nonce}-" + "a" * filler_len
    return json.dumps(value).encode("ascii")


def _item(
    content_id: str, data: bytes, *, media_type: str = "text/plain"
) -> SanitizedContentItemV1:
    return SanitizedContentItemV1(
        claim=ContentClaimV1(
            content_id=content_id,
            content_sha256=hashlib.sha256(data).hexdigest(),
            media_type=media_type,
            uncompressed_bytes=len(data),
        ),
        sanitized_bytes_base64=base64.b64encode(data).decode("ascii"),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )


def _json_item(content_id: str, value: Any) -> SanitizedContentItemV1:
    return _item(content_id, canonical_json_bytes(value), media_type="application/json")


def _key(digest: str) -> str:
    return f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"


# ---------------------------------------------------------------------------
# Byte-boundary storage routing, read back from real Postgres / S3
# ---------------------------------------------------------------------------


def test_inline_boundary_round_trips_through_postgres(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES, nonce=time.time_ns())
        item = _item("boundary-inline", data)

        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])
        assert ref.storage.value == "inline"
        assert ref.content_sha256 == hashlib.sha256(data).hexdigest()

        async with content_session_factory() as session, session.begin():
            await content_service.attach(session, prepared)
            stored = await session.scalar(
                text("SELECT data FROM catalog.inline_contents WHERE inline_id = :digest"),
                {"digest": ref.content_sha256},
            )

        assert stored is not None
        assert bytes(stored) == data

    _run(exercise())


def test_object_boundary_round_trips_through_s3(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        item = _item("boundary-object", data)

        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])
        assert ref.storage.value == "object"
        assert ref.object_key is not None
        assert ref.content_sha256 == hashlib.sha256(data).hexdigest()

        async with content_session_factory() as session, session.begin():
            await content_service.attach(session, prepared)

        got = await blob_store.get_verified(ref.object_key, ref.content_sha256)
        assert got == data

    _run(exercise())


# ---------------------------------------------------------------------------
# Dedupe by digest: two independent attach() calls, one storage row
# ---------------------------------------------------------------------------


def test_dedupe_by_digest_keeps_distinct_media_type_per_reference(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    insert_minimal_event: Any,
    insert_content_ref: Any,
) -> None:
    async def exercise() -> None:
        nonce = time.time_ns()
        data = _json_string_payload(nonce, INLINE_MAX_BYTES + 200)
        digest = hashlib.sha256(data).hexdigest()

        item_text = _item("dedupe-text", data, media_type="text/plain")
        item_json = _item("dedupe-json", data, media_type="application/json")

        async with content_session_factory() as session:
            event = await insert_minimal_event(session)

            prepared_text = await content_service.prepare([item_text])
            [ref_text] = prepared_text.resolve([item_text.claim])
            async with session.begin():
                await content_service.attach(session, prepared_text)
                await insert_content_ref(session, event.event_id, ref_text)

            # A second, independent prepare()+attach() call for the same
            # digest: this must re-verify and no-op the storage insert
            # (ON CONFLICT DO NOTHING) rather than erroring or duplicating.
            prepared_json = await content_service.prepare([item_json])
            [ref_json] = prepared_json.resolve([item_json.claim])
            async with session.begin():
                await content_service.attach(session, prepared_json)
                await insert_content_ref(session, event.event_id, ref_json)

            object_count = await session.scalar(
                text("SELECT count(*) FROM catalog.content_objects WHERE content_sha256 = :digest"),
                {"digest": digest},
            )
            media_types = (
                await session.scalars(
                    text(
                        "SELECT media_type FROM ledger.event_content_refs "
                        "WHERE content_sha256 = :digest ORDER BY media_type"
                    ),
                    {"digest": digest},
                )
            ).all()

        assert object_count == 1
        assert list(media_types) == ["application/json", "text/plain"]

    _run(exercise())


# ---------------------------------------------------------------------------
# FK ordering: a ref can never point at a digest that was never attach()-ed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("size", "constraint_name"),
    [
        pytest.param(256, "fk_event_content_refs_inline_id_inline_contents", id="inline"),
        pytest.param(
            INLINE_MAX_BYTES + 256,
            "fk_event_content_refs_object_key_content_objects",
            id="object",
        ),
    ],
)
def test_ref_insert_without_attach_violates_the_storage_fk(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    insert_minimal_event: Any,
    insert_content_ref: Any,
    size: int,
    constraint_name: str,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=size, nonce=time.time_ns())
        item = _item(f"fk-neg-{size}", data)

        # prepare() uploads/validates but never durably records the
        # digest -- only attach() does that, and this test deliberately
        # never calls it. A minimal *real* event is required first so the
        # failure below can only be the storage FK, not the unrelated
        # event_id FK on a fabricated event_id.
        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])

        async with content_session_factory() as session:
            event = await insert_minimal_event(session)
            with pytest.raises(IntegrityError) as exc_info:
                async with session.begin():
                    await insert_content_ref(session, event.event_id, ref)
            assert exc_info.value.orig.diag.constraint_name == constraint_name

    _run(exercise())


@pytest.mark.parametrize(
    "size",
    [pytest.param(256, id="inline"), pytest.param(INLINE_MAX_BYTES + 256, id="object")],
)
def test_attach_then_ref_insert_commits_successfully(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    insert_minimal_event: Any,
    insert_content_ref: Any,
    size: int,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=size, nonce=time.time_ns())
        item = _item(f"fk-pos-{size}", data)

        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])

        async with content_session_factory() as session:
            event = await insert_minimal_event(session)
            async with session.begin():
                await content_service.attach(session, prepared)
                await insert_content_ref(session, event.event_id, ref)

            count = await session.scalar(
                text(
                    "SELECT count(*) FROM ledger.event_content_refs "
                    "WHERE event_id = :event_id AND content_id = :content_id"
                ),
                {"event_id": event.event_id, "content_id": item.claim.content_id},
            )

        assert count == 1

    _run(exercise())


# ---------------------------------------------------------------------------
# Defense in depth: a batch that fails the redaction recheck is atomic
# ---------------------------------------------------------------------------


def test_prepare_is_atomic_when_a_later_item_fails_recheck(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
) -> None:
    """A rejected batch never reaches S3 or Postgres, against real backends.

    Mirrors ``tests/unit/content/test_content_service.py``'s
    ``test_prepare_is_atomic_when_a_later_item_fails_recheck`` (fakes only)
    with real Postgres and S3 underneath. The object-sized item is listed
    *before* the failing item precisely so this also proves storage never
    starts until the whole batch has passed the recheck -- not merely that a
    failure stops a partially-completed upload from continuing.
    """

    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        digest = hashlib.sha256(data).hexdigest()
        object_item = _item("would-upload", data)
        failing_item = _json_item("fails-recheck", {"password": "this-is-not-actually-redacted"})

        with pytest.raises(ContentRequiresRedactionError):
            await content_service.prepare([object_item, failing_item])

        with pytest.raises(BlobNotFoundError):
            await blob_store.get_verified(_key(digest), digest)

        async with content_session_factory() as session:
            object_count = await session.scalar(
                text("SELECT count(*) FROM catalog.content_objects WHERE content_sha256 = :d"),
                {"d": digest},
            )
        assert object_count == 0

    _run(exercise())
