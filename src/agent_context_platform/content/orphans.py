"""Delete unreferenced content-addressed objects, safely, on a delay.

An upload made by :meth:`~agent_context_platform.content.service.ContentService.prepare`
that never reaches a committed :meth:`...ContentService.attach` (the caller
crashed, the batch was rejected downstream, ...) leaves a real object in
S3 with no referencing row anywhere. :class:`OrphanSweeper` finds objects
under the ``sha256/`` prefix older than a cutoff, and only deletes ones
that are still unreferenced *at the moment of deletion*, using the exact
same per-digest advisory lock as ``attach()`` to make that check race-free.

The 24-hour delay (``min_age``) is a hard floor, independent of whatever
``cutoff`` a caller passes to :meth:`OrphanSweeper.run`: an object that was
just uploaded by a ``prepare()`` call that has not yet reached ``attach()``
must never be deleted out from under it. A caller may pass an earlier
(stricter) cutoff, but can never make the effective cutoff more recent than
``min_age`` -- except in tests, which may pass ``min_age=timedelta(0)`` to
exercise deletion deterministically without waiting.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.catalog.models import ContentObjectRow
from agent_context_platform.content.service import content_digest_lock_key

_CANONICAL_KEY = re.compile(
    r"^sha256/(?P<first>[0-9a-f]{2})/(?P<second>[0-9a-f]{2})/(?P<digest>[0-9a-f]{64})\.zst$"
)


def _parse_canonical_key(object_key: str) -> str | None:
    """Return the embedded digest if `object_key` is a canonical content key.

    Returns ``None`` (never touched by the sweeper) for any key that is
    not exactly the canonical ``sha256/aa/bb/<digest>.zst`` shape this
    platform writes, including a mismatch between the prefix folders and
    the digest itself.
    """
    match = _CANONICAL_KEY.fullmatch(object_key)
    if match is None:
        return None
    digest = match.group("digest")
    if match.group("first") != digest[:2] or match.group("second") != digest[2:4]:
        return None
    return digest


@dataclass(frozen=True, slots=True)
class SweepReport:
    """Aggregate counts for one sweep -- never object keys or digests."""

    examined: int
    deleted: int
    retained: int
    skipped_recent: int


class OrphanSweeper:
    """Delete S3 objects that are old and still unreferenced at delete time."""

    def __init__(
        self,
        client: Any,
        bucket_name: str,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        min_age: timedelta = timedelta(hours=24),
    ) -> None:
        if min_age < timedelta(0):
            raise ValueError(f"min_age must be >= 0, got {min_age}")
        self._client = client
        self._bucket_name = bucket_name
        self._session_factory = session_factory
        self._min_age = min_age

    async def run(self, cutoff: datetime) -> SweepReport:
        """Delete unreferenced content-addressed objects older than `cutoff`.

        The effective cutoff is never more recent than ``min_age`` before
        now (see the module docstring), regardless of `cutoff`: this method
        uses ``min(cutoff, datetime.now(UTC) - self._min_age)`` so a caller
        cannot accidentally (or a bug cannot make it) delete an object still
        inside its grace window.

        For each canonical ``sha256/...`` object older than the effective
        cutoff, takes the same per-digest advisory lock
        ``ContentService.attach`` takes (via :func:`content_digest_lock_key`),
        re-checks ``catalog.content_objects`` for a row under that lock, and
        deletes from S3 while still holding the lock. That ordering is what
        makes the attach/sweep race safe: either a concurrent ``attach()``
        commits its row before this re-check runs, so the object is
        retained; or this delete completes and releases the lock before
        ``attach()`` acquires it, so ``attach()``'s own re-verification
        against the blob store fails and its transaction rolls back
        without ever committing a dangling reference.

        A single candidate's failure aborts the run. Deleting an object is
        idempotent and the DB re-check is repeated fresh each time, so the
        caller may simply retry the whole sweep.
        """
        effective_cutoff = min(cutoff, datetime.now(UTC) - self._min_age)
        examined = 0
        deleted = 0
        retained = 0
        skipped_recent = 0
        async for object_key, last_modified in self._list_candidates():
            digest = _parse_canonical_key(object_key)
            if digest is None:
                continue
            examined += 1
            if last_modified >= effective_cutoff:
                skipped_recent += 1
                continue
            if await self._sweep_one(digest, object_key):
                deleted += 1
            else:
                retained += 1
        return SweepReport(
            examined=examined,
            deleted=deleted,
            retained=retained,
            skipped_recent=skipped_recent,
        )

    async def _sweep_one(self, digest: str, object_key: str) -> bool:
        """Return True if the object was deleted, False if it was retained."""
        async with self._session_factory() as session, session.begin():
            namespace, digest_key = content_digest_lock_key(digest)
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :digest_key)"),
                {"namespace": namespace, "digest_key": digest_key},
            )
            exists = await session.scalar(
                select(ContentObjectRow.id).where(ContentObjectRow.content_sha256 == digest)
            )
            if exists is not None:
                return False
            await _run_delete_to_completion(
                self._client.delete_object, Bucket=self._bucket_name, Key=object_key
            )
            return True

    async def _list_candidates(self) -> AsyncIterator[tuple[str, datetime]]:
        continuation_token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": self._bucket_name, "Prefix": "sha256/"}
            if continuation_token is not None:
                kwargs["ContinuationToken"] = continuation_token
            response = await asyncio.to_thread(self._client.list_objects_v2, **kwargs)
            for entry in response.get("Contents", []):
                key = entry.get("Key")
                last_modified = entry.get("LastModified")
                if isinstance(key, str) and isinstance(last_modified, datetime):
                    yield key, last_modified
            if not response.get("IsTruncated"):
                return
            continuation_token = response.get("NextContinuationToken")


async def _run_delete_to_completion(delete: Callable[..., Any], **kwargs: Any) -> None:
    """Run ``delete`` in a thread; on cancellation, wait for it to finish first.

    Cancelling ``asyncio.to_thread`` abandons the wait but cannot stop the
    thread, so a plain cancellation would let the caller's transaction exit
    (releasing the per-digest advisory lock) while the delete is still about
    to run. Shielding the future and waiting it out keeps the lock held until
    the delete has really returned; the cancellation is re-raised afterwards.
    """
    delete_future = asyncio.ensure_future(asyncio.to_thread(delete, **kwargs))
    try:
        await asyncio.shield(delete_future)
    except asyncio.CancelledError:
        while not delete_future.done():
            try:
                await asyncio.wait([delete_future])
            except asyncio.CancelledError:
                continue
        if not delete_future.cancelled():
            delete_future.exception()  # mark retrieved; cancellation takes precedence
        raise
