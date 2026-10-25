"""The orphan sweep racing a batch that reuses an old object (PLATFORM-054 S4), real PG and S3.

An old unreferenced object the sweep deletes between `prepare` and `attach` loses nothing: the
batch fails and is retried, and no integrity alarm is raised. A REFERENCED object, or one the same
request uploaded, that disappears is a real loss: `attach` raises
`content_integrity_failure check=missing_object`.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from agent_context_sdk import (
    ContentClaimV1,
    ContentDisposition,
    RedactionReportV1,
    SanitizedContentItemV1,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent_context_platform.content.blob_store import BlobNotFoundError, S3BlobStore
from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.orphans import OrphanSweeper
from agent_context_platform.content.service import ContentService
from agent_context_platform.settings import S3Settings

pytestmark = pytest.mark.integration


def _item(label: str) -> SanitizedContentItemV1:
    data = f"sweep race {label} nonce={time.time_ns()} ".encode() + b"a" * (INLINE_MAX_BYTES + 10)
    return SanitizedContentItemV1(
        claim=ContentClaimV1(
            content_id=f"race-{time.time_ns()}",
            content_sha256=hashlib.sha256(data).hexdigest(),
            media_type="text/plain",
            uncompressed_bytes=len(data),
        ),
        sanitized_bytes_base64=base64.b64encode(data).decode("ascii"),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )


def _alarms(caplog: pytest.LogCaptureFixture) -> list[Any]:
    return [r for r in caplog.records if r.getMessage().startswith("content_integrity_failure")]


def test_a_swept_orphan_fails_the_attach_quietly_but_a_lost_referenced_object_alarms(
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
    s3_client: Any,
    s3_settings: S3Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from unit.observability._capture import capture_telemetry

    from agent_context_platform.observability import contract

    caplog.set_level(logging.INFO, logger="agent_context_platform.content.service")

    async def exercise() -> None:
        orphan = _item("orphan")
        # An earlier request uploaded it and never attached it: an old, unreferenced object.
        await content_service.prepare([orphan])
        with capture_telemetry() as telemetry:
            prepared = await content_service.prepare([orphan])  # reuses it without an upload
            [stored] = [item.stored_blob for item in prepared._items]
            assert stored is not None and not stored.created
            sweeper = OrphanSweeper(
                s3_client, s3_settings.bucket_name, content_session_factory, min_age=timedelta(0)
            )
            assert (await sweeper.run(cutoff=datetime.now(UTC))).deleted >= 1
            with pytest.raises(BlobNotFoundError):
                async with content_session_factory() as session, session.begin():
                    await content_service.attach(session, prepared)
            assert telemetry.points(contract.LEDGER_INTEGRITY_FAILURES) == {}
        assert _alarms(caplog) == []
        assert any(
            r.getMessage().startswith("content_object_swept_before_attach") for r in caplog.records
        )
        # The retry simply uploads it again.
        retried = await content_service.prepare([orphan])
        async with content_session_factory() as session, session.begin():
            await content_service.attach(session, retried)

        kept = _item("referenced")
        first = await content_service.prepare([kept])
        async with content_session_factory() as session, session.begin():
            await content_service.attach(session, first)
        again = await content_service.prepare([kept])  # reuses the referenced object
        [ref] = again.resolve([kept.claim])
        s3_client.delete_object(Bucket=s3_settings.bucket_name, Key=ref.object_key)  # lost
        with capture_telemetry() as telemetry:
            with pytest.raises(BlobNotFoundError):
                async with content_session_factory() as session, session.begin():
                    await content_service.attach(session, again)
            assert telemetry.points(contract.LEDGER_INTEGRITY_FAILURES) == {
                (("check", "missing_object"),): 1
            }
        [alarm] = _alarms(caplog)
        assert (alarm.check, alarm.stage) == ("missing_object", "attach")  # type: ignore[attr-defined]

        # An object this very request uploaded that vanishes is no old orphan: a loss.
        fresh = _item("fresh")
        uploaded = await content_service.prepare([fresh])
        [ref] = uploaded.resolve([fresh.claim])
        s3_client.delete_object(Bucket=s3_settings.bucket_name, Key=ref.object_key)
        with capture_telemetry() as telemetry:
            with pytest.raises(BlobNotFoundError):
                async with content_session_factory() as session, session.begin():
                    await content_service.attach(session, uploaded)
            assert telemetry.points(contract.LEDGER_INTEGRITY_FAILURES) == {
                (("check", "missing_object"),): 1
            }

    asyncio.run(exercise())
