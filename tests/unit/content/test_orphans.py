"""Unit tests for OrphanSweeper: candidate listing, the min_age floor, cutoff
filtering, and the lock-then-recheck delete decision.

Fakes a boto3-like S3 client and a Postgres session/session-factory -- no
real services required. tests/integration/content/test_crash_matrix.py
covers the same delete decision against real Postgres and S3, including the
advisory-lock race with a concurrent ``ContentService.attach``.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import TextClause

from agent_context_platform.content.orphans import OrphanSweeper, SweepReport, _parse_canonical_key
from agent_context_platform.content.service import content_digest_lock_key

pytestmark = pytest.mark.unit


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _digest(byte: int) -> str:
    return format(byte, "02x") * 32


def _key(digest: str) -> str:
    return f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"


class _NullAsyncContext:
    async def __aenter__(self) -> _NullAsyncContext:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeSweepSession:
    """Records lock calls; resolves the ``content_objects`` existence check
    against a fixed set of "known" digests supplied by the test."""

    def __init__(self, known_digests: set[str], call_log: list[str]) -> None:
        self._known_digests = known_digests
        self._call_log = call_log

    async def __aenter__(self) -> FakeSweepSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    def begin(self) -> _NullAsyncContext:
        return _NullAsyncContext()

    async def execute(self, clause: Any, params: Any = None) -> None:
        assert isinstance(clause, TextClause)
        self._call_log.append(f"lock:{params['digest_key']}")

    async def scalar(self, stmt: Any) -> Any:
        digest = next(iter(stmt.compile().params.values()))
        self._call_log.append(f"scalar:{digest}")
        return object() if digest in self._known_digests else None


class FakeSessionFactory:
    """A fresh ``FakeSweepSession`` per call, matching ``async_sessionmaker``."""

    def __init__(self, known_digests: set[str], call_log: list[str]) -> None:
        self._known_digests = known_digests
        self._call_log = call_log

    def __call__(self) -> FakeSweepSession:
        return FakeSweepSession(self._known_digests, self._call_log)


class FakeS3Client:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = list(pages)
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.deleted: list[str] = []

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("list_objects_v2", kwargs))
        return self._pages.pop(0)

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("delete_object", kwargs))
        self.deleted.append(kwargs["Key"])
        return {}


# ---------------------------------------------------------------------------
# _parse_canonical_key
# ---------------------------------------------------------------------------


def test_parse_canonical_key_accepts_matching_key() -> None:
    digest = _digest(0xAB)
    assert _parse_canonical_key(_key(digest)) == digest


@pytest.mark.parametrize(
    "object_key",
    [
        "not-even-close",
        "sha256/ab/cd/short.zst",
        "sha256/ab/cd/" + _digest(0xAB) + ".txt",
        "sha256/ab/cd/" + _digest(0xAB),
    ],
)
def test_parse_canonical_key_rejects_non_canonical_shape(object_key: str) -> None:
    assert _parse_canonical_key(object_key) is None


def test_parse_canonical_key_rejects_prefix_digest_mismatch() -> None:
    digest = _digest(0xAB)
    mismatched = f"sha256/00/00/{digest}.zst"
    assert _parse_canonical_key(mismatched) is None


# ---------------------------------------------------------------------------
# _list_candidates, exercised via run()
# ---------------------------------------------------------------------------


def test_run_paginates_and_skips_non_canonical_keys() -> None:
    async def exercise() -> None:
        digest = _digest(0x11)
        now = datetime.now(UTC)
        page_one = {
            "Contents": [
                {"Key": "sha256/not/canonical.zst", "LastModified": now - timedelta(days=2)},
            ],
            "IsTruncated": True,
            "NextContinuationToken": "token-1",
        }
        page_two = {
            "Contents": [{"Key": _key(digest), "LastModified": now - timedelta(days=2)}],
            "IsTruncated": False,
        }
        client = FakeS3Client([page_one, page_two])
        call_log: list[str] = []
        sweeper = OrphanSweeper(
            client, "bucket", FakeSessionFactory(set(), call_log), min_age=timedelta(0)
        )

        report = await sweeper.run(now)

        assert report == SweepReport(examined=1, deleted=1, retained=0, skipped_recent=0)
        assert client.calls[0] == ("list_objects_v2", {"Bucket": "bucket", "Prefix": "sha256/"})
        assert client.calls[1] == (
            "list_objects_v2",
            {"Bucket": "bucket", "Prefix": "sha256/", "ContinuationToken": "token-1"},
        )
        assert client.deleted == [_key(digest)]

    _run(exercise())


def test_run_skips_entries_missing_key_or_last_modified() -> None:
    async def exercise() -> None:
        now = datetime.now(UTC)
        page = {
            "Contents": [
                {"LastModified": now},
                {"Key": _key(_digest(0x22))},
            ],
            "IsTruncated": False,
        }
        client = FakeS3Client([page])
        call_log: list[str] = []
        sweeper = OrphanSweeper(client, "bucket", FakeSessionFactory(set(), call_log))

        report = await sweeper.run(now)

        assert report == SweepReport(examined=0, deleted=0, retained=0, skipped_recent=0)

    _run(exercise())


# ---------------------------------------------------------------------------
# run(): the min_age floor and cutoff filtering
# ---------------------------------------------------------------------------


def test_run_skips_recent_objects_under_the_default_min_age_floor() -> None:
    async def exercise() -> None:
        digest = _digest(0x33)
        now = datetime.now(UTC)
        page = {
            "Contents": [{"Key": _key(digest), "LastModified": now - timedelta(hours=1)}],
            "IsTruncated": False,
        }
        client = FakeS3Client([page])
        call_log: list[str] = []
        # Default min_age=24h; a permissive caller-supplied cutoff must not
        # override the floor.
        sweeper = OrphanSweeper(client, "bucket", FakeSessionFactory(set(), call_log))

        report = await sweeper.run(now + timedelta(days=365))

        assert report == SweepReport(examined=1, deleted=0, retained=0, skipped_recent=1)
        assert client.deleted == []
        assert call_log == []

    _run(exercise())


def test_run_deletes_old_unreferenced_objects_when_min_age_is_relaxed() -> None:
    async def exercise() -> None:
        digest = _digest(0x44)
        now = datetime.now(UTC)
        page = {
            "Contents": [{"Key": _key(digest), "LastModified": now - timedelta(seconds=1)}],
            "IsTruncated": False,
        }
        client = FakeS3Client([page])
        call_log: list[str] = []
        sweeper = OrphanSweeper(
            client, "bucket", FakeSessionFactory(set(), call_log), min_age=timedelta(0)
        )

        report = await sweeper.run(now)

        assert report == SweepReport(examined=1, deleted=1, retained=0, skipped_recent=0)
        assert client.deleted == [_key(digest)]
        _, digest_key = content_digest_lock_key(digest)
        assert call_log == [f"lock:{digest_key}", f"scalar:{digest}"]

    _run(exercise())


def test_run_retains_objects_still_referenced_in_the_catalog() -> None:
    async def exercise() -> None:
        digest = _digest(0x55)
        now = datetime.now(UTC)
        page = {
            "Contents": [{"Key": _key(digest), "LastModified": now - timedelta(seconds=1)}],
            "IsTruncated": False,
        }
        client = FakeS3Client([page])
        call_log: list[str] = []
        sweeper = OrphanSweeper(
            client, "bucket", FakeSessionFactory({digest}, call_log), min_age=timedelta(0)
        )

        report = await sweeper.run(now)

        assert report == SweepReport(examined=1, deleted=0, retained=1, skipped_recent=0)
        assert client.deleted == []

    _run(exercise())


def test_constructor_rejects_a_negative_min_age() -> None:
    """A negative grace period would put the cutoff in the future."""
    with pytest.raises(ValueError, match="min_age"):
        OrphanSweeper(
            FakeS3Client([]), "bucket", FakeSessionFactory(set(), []), min_age=-timedelta(seconds=1)
        )


def test_constructor_allows_a_zero_min_age() -> None:
    OrphanSweeper(FakeS3Client([]), "bucket", FakeSessionFactory(set(), []), min_age=timedelta(0))
