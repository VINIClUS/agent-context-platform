"""`agent-context content sweep-orphans` against real PostgreSQL and S3 (Garage).

An unreferenced upload and an attached one sit in the bucket; a dry run counts the orphan and
deletes nothing, a real run deletes only the orphan. The bucket is shared with other modules,
whose objects this module's database does not reference, so counts are lower bounds.
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
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.content.blob_store import BlobNotFoundError, S3BlobStore
from agent_context_platform.content.models import INLINE_MAX_BYTES
from agent_context_platform.content.service import ContentService
from agent_context_platform.settings import S3Settings

pytestmark = pytest.mark.integration


def _payload(label: str) -> bytes:
    prefix = f"sweep-cli {label} nonce={time.time_ns()} ".encode("ascii")
    return prefix + b"a" * (INLINE_MAX_BYTES + 100)


def _item(data: bytes) -> SanitizedContentItemV1:
    return SanitizedContentItemV1(
        claim=ContentClaimV1(
            content_id=f"sweep-{time.time_ns()}",
            content_sha256=hashlib.sha256(data).hexdigest(),
            media_type="text/plain",
            uncompressed_bytes=len(data),
        ),
        sanitized_bytes_base64=base64.b64encode(data).decode("ascii"),
        redaction_report=RedactionReportV1(
            policy_version="1.0.0", disposition=ContentDisposition.SANITIZED
        ),
    )


async def _exists(store: S3BlobStore, object_key: str, digest: str) -> bool:
    try:
        await store.get_verified(object_key, digest)
    except BlobNotFoundError:
        return False
    return True


def test_sweep_orphans_counts_in_a_dry_run_and_deletes_only_unreferenced_objects(
    postgres_dsn: str,
    content_session_factory: async_sessionmaker[AsyncSession],
    content_service: ContentService,
    blob_store: S3BlobStore,
    s3_settings: S3Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orphan_data, kept_data = _payload("orphan"), _payload("kept")

    async def seed() -> tuple[str, str]:
        orphan = await blob_store.put_verified(orphan_data, "application/octet-stream")
        prepared = await content_service.prepare([_item(kept_data)])
        async with content_session_factory() as session, session.begin():
            await content_service.attach(session, prepared)
        return orphan.object_key, hashlib.sha256(kept_data).hexdigest()

    orphan_key, kept_digest = asyncio.run(seed())
    kept_key = f"sha256/{kept_digest[:2]}/{kept_digest[2:4]}/{kept_digest}.zst"
    orphan_digest = hashlib.sha256(orphan_data).hexdigest()
    secret = s3_settings.secret_access_key
    for name, value in {
        "AGENT_CONTEXT_ENVIRONMENT": "test",
        # As the least-privileged API role, so a missing grant fails here.
        "AGENT_CONTEXT_POSTGRESQL__API_DSN": f"{postgres_dsn}{'&' if '?' in postgres_dsn else '?'}"
        "options=-c%20role%3Dagent_context_api",
        "AGENT_CONTEXT_S3__ENDPOINT_URL": str(s3_settings.endpoint_url),
        "AGENT_CONTEXT_S3__REGION_NAME": s3_settings.region_name,
        "AGENT_CONTEXT_S3__BUCKET_NAME": s3_settings.bucket_name,
        "AGENT_CONTEXT_S3__ACCESS_KEY_ID": s3_settings.access_key_id.get_secret_value()  # type: ignore[union-attr]
        if s3_settings.access_key_id
        else "",
        "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY": secret.get_secret_value() if secret else "",
    }.items():
        monkeypatch.setenv(name, value)

    def sweep(*extra: str) -> dict[str, Any]:
        args = ["content", "sweep-orphans", "--min-age", "0", "--allow-short-min-age", "--json"]
        result = CliRunner().invoke(cli.app, [*args, *extra])
        assert result.exit_code == 0, result.output
        assert orphan_key not in result.output and orphan_digest not in result.output
        payload: dict[str, Any] = json.loads(result.stdout)
        return payload

    dry = sweep("--dry-run")
    assert dry["dry_run"] is True and dry["deleted"] == 0
    assert dry["would_delete"] >= 1 and dry["retained"] >= 1
    assert asyncio.run(_exists(blob_store, orphan_key, orphan_digest))

    real = sweep()
    assert real["dry_run"] is False and real["deleted"] >= 1 and real["retained"] >= 1
    assert not asyncio.run(_exists(blob_store, orphan_key, orphan_digest))
    assert asyncio.run(_exists(blob_store, kept_key, kept_digest))

    # Without the test flag the 24-hour floor stands: a fresh orphan is only skipped.
    fresh_key = asyncio.run(
        blob_store.put_verified(_payload("fresh"), "application/octet-stream")
    ).object_key
    safe = CliRunner().invoke(cli.app, ["content", "sweep-orphans", "--json"])
    assert safe.exit_code == 0 and json.loads(safe.stdout)["skipped_recent"] >= 1
    fresh_digest = fresh_key.rsplit("/", 1)[-1].removesuffix(".zst")
    assert asyncio.run(_exists(blob_store, fresh_key, fresh_digest))
