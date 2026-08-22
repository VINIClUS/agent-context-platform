from __future__ import annotations

import asyncio
import os

import pytest

from agent_context_platform.content.blob_store import S3BlobStore
from agent_context_platform.settings import S3Settings

pytestmark = [pytest.mark.integration, pytest.mark.contract]

_REQUIRED = {
    "endpoint_url": "AGENT_CONTEXT_TEST_S3_ENDPOINT_URL",
    "region_name": "AGENT_CONTEXT_TEST_S3_REGION_NAME",
    "bucket_name": "AGENT_CONTEXT_TEST_S3_BUCKET_NAME",
    "access_key_id": "AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID",
    "secret_access_key": "AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY",
}


def _contract_settings() -> S3Settings:
    configured = {name: os.getenv(variable) for name, variable in _REQUIRED.items()}
    present = [name for name, value in configured.items() if value is not None]
    if not present:
        pytest.skip("S3 contract test requires AGENT_CONTEXT_TEST_S3_* configuration")
    missing = [variable for name, variable in _REQUIRED.items() if not configured[name]]
    if missing:
        pytest.fail("partial S3 contract configuration; missing: " + ", ".join(missing))
    return S3Settings(**configured)  # type: ignore[arg-type]


def test_s3_contract_put_head_get_delete() -> None:
    store = S3BlobStore.from_settings(_contract_settings())
    content = b"agent-context S3 contract payload"

    stored = asyncio.run(store.put_verified(content, "text/plain"))
    assert stored.uncompressed_bytes == len(content)
    assert asyncio.run(store.get_verified(stored.object_key, stored.sha256)) == content
    asyncio.run(store.delete(stored.object_key))


def test_contract_settings_skip_only_when_no_variables_are_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for variable in _REQUIRED.values():
        monkeypatch.delenv(variable, raising=False)

    with pytest.raises(pytest.skip.Exception):
        _contract_settings()


@pytest.mark.parametrize("value", ["", "https://objects.example.test"])
def test_contract_settings_fail_for_empty_or_partial_configuration(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    for variable in _REQUIRED.values():
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("AGENT_CONTEXT_TEST_S3_ENDPOINT_URL", value)

    with pytest.raises(pytest.fail.Exception, match="partial S3 contract configuration"):
        _contract_settings()


def test_contract_settings_loads_complete_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "endpoint_url": "https://objects.example.test",
        "region_name": "garage",
        "bucket_name": "agent-context-content",
        "access_key_id": "access",
        "secret_access_key": "secret",
    }
    for name, variable in _REQUIRED.items():
        monkeypatch.setenv(variable, values[name])

    settings = _contract_settings()

    assert settings.bucket_name == "agent-context-content"
