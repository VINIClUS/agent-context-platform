"""Crash-injection and advisory-lock race tests against real Postgres and S3.

Proves the security boundary the module docstring in
``agent_context_platform.content.service`` states directly: no committed
``ledger.event_content_refs`` row may ever reference content that was never
durably written, no matter *where* in the ``prepare()`` -> ``attach()`` ->
append-refs sequence a crash lands, and no matter which side of the
``attach()`` / ``OrphanSweeper`` race wins.

"No dangling reference" is checked as an S3-side property throughout: after
a crash, the object may legitimately still exist in S3 as an orphan (that
is exactly what ``OrphanSweeper`` exists to clean up on a delay), but no DB
row may ever reference it, and once swept it must actually be gone
(``BlobNotFoundError`` from a fresh ``get_verified``).

The two race tests call ``OrphanSweeper._sweep_one`` directly rather than
the public ``run()``: ``run()`` first lists the *entire* shared bucket,
which -- being long-lived across the whole test session -- may contain
old objects left behind by other test modules, making the wall-clock
timing of "the sweeper is now blocked on our digest's lock" nondeterministic.
Calling the private single-digest method directly exercises exactly the
critical section the module docstring describes, deterministically.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.blob_store import (
    BlobNotFoundError,
    BlobStore,
    S3BlobStore,
    StoredBlob,
)
from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.orphans import OrphanSweeper
from agent_context_platform.content.service import ContentService

pytestmark = pytest.mark.integration


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _sized_payload(*, size: int, nonce: int) -> bytes:
    prefix = f"nonce={nonce} size={size} ".encode("ascii")
    assert len(prefix) <= size
    return prefix + b"a" * (size - len(prefix))


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


def _key(digest: str) -> str:
    return f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"


async def _wait_until_true(
    check: Callable[[], bool], *, timeout: float = 30.0, interval: float = 0.02
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not check():
        if loop.time() > deadline:
            raise AssertionError("condition was never met within the timeout")
        await asyncio.sleep(interval)


async def _wait_for_blocked_advisory_lock(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    timeout: float = 30.0,
    interval: float = 0.02,
) -> None:
    """Poll ``pg_locks`` until some advisory lock in this database is waiting."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    async with session_factory() as session:
        while True:
            waiting = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted "
                    "AND database = "
                    "(SELECT oid FROM pg_database WHERE datname = current_database())"
                )
            )
            await session.rollback()
            if waiting:
                return
            if loop.time() > deadline:
                raise AssertionError("no blocked advisory lock observed within the timeout")
            await asyncio.sleep(interval)


async def _assert_orphan_swept(
    sweeper: OrphanSweeper, blob_store: S3BlobStore, object_key: str, digest: str
) -> None:
    # min_age=0 makes the effective cutoff "now"; Garage's LastModified may
    # have coarser-than-instant granularity, so without this margin a
    # freshly written object can read back as still >= cutoff and be
    # skipped as "recent" instead of examined for deletion.
    await asyncio.sleep(1.1)
    await sweeper.run(datetime.now(UTC))
    with pytest.raises(BlobNotFoundError):
        await blob_store.get_verified(object_key, digest)


class _CrashInjectionError(Exception):
    """Raised deliberately by these tests to simulate a mid-flight crash."""


@dataclass
class _CrashingBlobStore:
    """Delegates to a real ``BlobStore``, raising at one configurable point."""

    inner: BlobStore
    fail_before_put: bool = False
    fail_after_put: bool = False
    fail_after_get_verified: bool = False

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        if self.fail_before_put:
            raise _CrashInjectionError("before_put")
        stored = await self.inner.put_verified(content, media_type)
        if self.fail_after_put:
            raise _CrashInjectionError("after_put")
        return stored

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        data = await self.inner.get_verified(object_key, expected_sha256)
        if self.fail_after_get_verified:
            raise _CrashInjectionError("after_get_verified")
        return data

    async def delete(self, object_key: str) -> None:
        await self.inner.delete(object_key)


class _GatedDeleteClient:
    """Wraps a real boto3 client, blocking ``delete_object`` for one key.

    ``delete_object`` runs inside ``OrphanSweeper``'s ``asyncio.to_thread``,
    so blocking the calling thread in ``release.wait()`` is safe -- it never
    blocks the event loop.
    """

    def __init__(self, inner: Any, *, blocked_key: str) -> None:
        self._inner = inner
        self._blocked_key = blocked_key
        self.entered = threading.Event()
        self.release = threading.Event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def delete_object(self, **kwargs: Any) -> Any:
        if kwargs.get("Key") == self._blocked_key:
            self.entered.set()
            if not self.release.wait(timeout=30):
                raise AssertionError("release was never set; see the test's finally block")
        return self._inner.delete_object(**kwargs)


# ---------------------------------------------------------------------------
# Crash matrix: before put / after put / after attach's re-verify / before commit
# ---------------------------------------------------------------------------


def test_crash_before_put_leaves_nothing_anywhere(
    content_session_factory: async_sessionmaker[AsyncSession],
    blob_store: S3BlobStore,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        digest = hashlib.sha256(data).hexdigest()
        item = _item("crash-before-put", data)
        service = ContentService(
            _CrashingBlobStore(inner=blob_store, fail_before_put=True), RedactionPolicyV1()
        )

        with pytest.raises(_CrashInjectionError):
            await service.prepare([item])

        with pytest.raises(BlobNotFoundError):
            await blob_store.get_verified(_key(digest), digest)

        async with content_session_factory() as session:
            count = await session.scalar(
                text("SELECT count(*) FROM catalog.content_objects WHERE content_sha256 = :d"),
                {"d": digest},
            )
        assert count == 0

    _run(exercise())


def test_crash_after_put_leaves_a_sweepable_orphan(
    content_session_factory: async_sessionmaker[AsyncSession],
    blob_store: S3BlobStore,
    s3_client: Any,
    s3_settings: Any,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        digest = hashlib.sha256(data).hexdigest()
        object_key = _key(digest)
        item = _item("crash-after-put", data)
        service = ContentService(
            _CrashingBlobStore(inner=blob_store, fail_after_put=True), RedactionPolicyV1()
        )

        with pytest.raises(_CrashInjectionError):
            await service.prepare([item])

        # The real upload already completed -- fail_after_put only raises
        # once the inner put_verified() returns -- so a genuine orphan now
        # sits in S3 with no referencing row anywhere.
        got = await blob_store.get_verified(object_key, digest)
        assert got == data

        async with content_session_factory() as session:
            count = await session.scalar(
                text("SELECT count(*) FROM catalog.content_objects WHERE content_sha256 = :d"),
                {"d": digest},
            )
        assert count == 0

        sweeper = OrphanSweeper(
            s3_client, s3_settings.bucket_name, content_session_factory, min_age=timedelta(0)
        )
        await _assert_orphan_swept(sweeper, blob_store, object_key, digest)

    _run(exercise())


def test_crash_after_attach_reverify_rolls_back_the_reference(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
    s3_client: Any,
    s3_settings: Any,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        item = _item("crash-after-reverify", data)

        # prepare() with the real, non-crashing store: the upload genuinely
        # completes.
        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])
        assert ref.object_key is not None

        crashing_service = ContentService(
            _CrashingBlobStore(inner=blob_store, fail_after_get_verified=True), RedactionPolicyV1()
        )

        async with content_session_factory() as session:
            with pytest.raises(_CrashInjectionError):
                async with session.begin():
                    await crashing_service.attach(session, prepared)

            count = await session.scalar(
                text("SELECT count(*) FROM catalog.content_objects WHERE content_sha256 = :d"),
                {"d": ref.content_sha256},
            )
        assert count == 0

        sweeper = OrphanSweeper(
            s3_client, s3_settings.bucket_name, content_session_factory, min_age=timedelta(0)
        )
        await _assert_orphan_swept(sweeper, blob_store, ref.object_key, ref.content_sha256)

    _run(exercise())


def test_crash_before_commit_rolls_back_the_whole_transaction(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
    insert_minimal_event: Any,
    insert_content_ref: Any,
    s3_client: Any,
    s3_settings: Any,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        item = _item("crash-before-commit", data)

        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])
        assert ref.object_key is not None

        async with content_session_factory() as session:
            event = await insert_minimal_event(session)
            with pytest.raises(_CrashInjectionError):
                async with session.begin():
                    await content_service.attach(session, prepared)
                    await insert_content_ref(session, event.event_id, ref)
                    raise _CrashInjectionError("before_commit")

            object_count = await session.scalar(
                text("SELECT count(*) FROM catalog.content_objects WHERE content_sha256 = :d"),
                {"d": ref.content_sha256},
            )
            ref_count = await session.scalar(
                text(
                    "SELECT count(*) FROM ledger.event_content_refs "
                    "WHERE event_id = :event_id AND content_id = :content_id"
                ),
                {"event_id": event.event_id, "content_id": item.claim.content_id},
            )
        assert object_count == 0
        assert ref_count == 0

        sweeper = OrphanSweeper(
            s3_client, s3_settings.bucket_name, content_session_factory, min_age=timedelta(0)
        )
        await _assert_orphan_swept(sweeper, blob_store, ref.object_key, ref.content_sha256)

    _run(exercise())


# ---------------------------------------------------------------------------
# Advisory-lock races between attach() and OrphanSweeper
# ---------------------------------------------------------------------------


def test_attach_holds_the_lock_and_blocks_the_sweeper(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
    s3_client: Any,
    s3_settings: Any,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        item = _item("race-attach-first", data)

        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])
        assert ref.object_key is not None

        sweeper = OrphanSweeper(
            s3_client, s3_settings.bucket_name, content_session_factory, min_age=timedelta(0)
        )

        session_a = content_session_factory()
        sweep_task: asyncio.Task[bool] | None = None
        try:
            await asyncio.wait_for(session_a.begin(), timeout=30)
            await asyncio.wait_for(content_service.attach(session_a, prepared), timeout=30)

            sweep_task = asyncio.create_task(sweeper._sweep_one(ref.content_sha256, ref.object_key))
            await asyncio.wait_for(
                _wait_for_blocked_advisory_lock(content_session_factory), timeout=30
            )

            # attach() has not committed yet: the sweeper must still be
            # blocked, not have already decided to delete.
            assert not sweep_task.done()

            await asyncio.wait_for(session_a.commit(), timeout=30)
            deleted = await asyncio.wait_for(sweep_task, timeout=30)
        finally:
            if sweep_task is not None and not sweep_task.done():
                sweep_task.cancel()
            await session_a.close()

        assert deleted is False
        got = await blob_store.get_verified(ref.object_key, ref.content_sha256)
        assert got == data

    _run(exercise())


def test_cancelled_sweep_keeps_the_lock_until_the_delete_returns(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    s3_client: Any,
    s3_settings: Any,
) -> None:
    """Cancelling a sweep cannot stop ``delete_object``'s worker thread.

    If the sweep's transaction exited on cancellation, the advisory lock would
    be released while the thread was still about to delete: a concurrent
    ``attach()`` could re-verify the still-present object, commit a reference,
    and then have it orphaned by the delete. The sweep must hold the lock until
    the delete has actually returned, and only then propagate the cancellation.
    """

    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        item = _item("race-sweep-cancelled", data)
        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])
        assert ref.object_key is not None

        gated_client = _GatedDeleteClient(s3_client, blocked_key=ref.object_key)
        sweeper = OrphanSweeper(
            gated_client, s3_settings.bucket_name, content_session_factory, min_age=timedelta(0)
        )

        session_b = content_session_factory()
        sweep_task = asyncio.create_task(sweeper._sweep_one(ref.content_sha256, ref.object_key))
        attach_task: asyncio.Task[None] | None = None
        try:
            await asyncio.wait_for(_wait_until_true(gated_client.entered.is_set), timeout=30)

            sweep_task.cancel()
            await asyncio.sleep(0.5)
            # The thread is still blocked inside delete_object, so the sweep
            # must not have finished (and released its lock) yet.
            assert not sweep_task.done()

            await asyncio.wait_for(session_b.begin(), timeout=30)
            attach_task = asyncio.create_task(content_service.attach(session_b, prepared))
            await asyncio.wait_for(
                _wait_for_blocked_advisory_lock(content_session_factory), timeout=30
            )
            assert not attach_task.done()

            gated_client.release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(sweep_task, timeout=30)

            # The delete completed before the lock was released, so attach()
            # sees the object gone rather than committing a dangling reference.
            with pytest.raises(BlobNotFoundError):
                await asyncio.wait_for(attach_task, timeout=30)
        finally:
            gated_client.release.set()
            if attach_task is not None and not attach_task.done():
                attach_task.cancel()
            if not sweep_task.done():
                sweep_task.cancel()
            await session_b.rollback()
            await session_b.close()

    _run(exercise())


def test_sweep_holds_the_lock_and_blocks_attach(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
    s3_client: Any,
    s3_settings: Any,
) -> None:
    async def exercise() -> None:
        data = _sized_payload(size=INLINE_MAX_BYTES + 1, nonce=time.time_ns())
        item = _item("race-sweep-first", data)

        # A real, genuinely orphaned upload: prepare() only. attach() is
        # deliberately never called, so the sweeper's re-check under the
        # lock finds no referencing row and proceeds to delete.
        prepared = await content_service.prepare([item])
        [ref] = prepared.resolve([item.claim])
        assert ref.object_key is not None

        gated_client = _GatedDeleteClient(s3_client, blocked_key=ref.object_key)
        sweeper = OrphanSweeper(
            gated_client, s3_settings.bucket_name, content_session_factory, min_age=timedelta(0)
        )

        session_b = content_session_factory()
        sweep_task = asyncio.create_task(sweeper._sweep_one(ref.content_sha256, ref.object_key))
        attach_task: asyncio.Task[None] | None = None
        try:
            await asyncio.wait_for(_wait_until_true(gated_client.entered.is_set), timeout=30)

            await asyncio.wait_for(session_b.begin(), timeout=30)
            attach_task = asyncio.create_task(content_service.attach(session_b, prepared))
            await asyncio.wait_for(
                _wait_for_blocked_advisory_lock(content_session_factory), timeout=30
            )

            # The sweeper is still inside delete_object: attach() must
            # still be blocked, not have already re-verified anything.
            assert not attach_task.done()

            gated_client.release.set()
            deleted = await asyncio.wait_for(sweep_task, timeout=30)
            assert deleted is True

            with pytest.raises(BlobNotFoundError):
                await asyncio.wait_for(attach_task, timeout=30)
        finally:
            gated_client.release.set()
            if attach_task is not None and not attach_task.done():
                attach_task.cancel()
            if not sweep_task.done():
                sweep_task.cancel()
            await session_b.rollback()
            await session_b.close()

        # The digest is fully usable again: a fresh prepare()+attach() for
        # the same content succeeds now that the orphan is gone.
        fresh_prepared = await content_service.prepare([item])
        async with content_session_factory() as session_c, session_c.begin():
            await content_service.attach(session_c, fresh_prepared)

        got = await blob_store.get_verified(ref.object_key, ref.content_sha256)
        assert got == data

    _run(exercise())
