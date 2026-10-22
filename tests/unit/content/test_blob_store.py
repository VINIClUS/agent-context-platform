from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import pytest
from botocore.exceptions import ClientError

from agent_context_platform.content.blob_store import (
    BlobInputLimitError,
    BlobIntegrityError,
    BlobNotFoundError,
    BlobStoreError,
    S3BlobStore,
)
from agent_context_platform.settings import S3Settings

pytestmark = pytest.mark.unit


class FakeStreamingBody:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False

    def read(self, amount: int | None = None) -> bytes:
        return self.content if amount is None else self.content[:amount]

    def close(self) -> None:
        self.closed = True


class FakeS3Client:
    """Small in-memory S3 boundary with the responses this adapter consumes."""

    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.put_attempts = 0
        self.put_error_codes: list[str] = []
        self.last_get_body: FakeStreamingBody | None = None
        self.deleted_keys: list[str] = []

    def put_object(self, **request: Any) -> dict[str, Any]:
        self.put_attempts += 1
        if self.put_error_codes:
            raise _client_error(self.put_error_codes.pop(0), "PutObject")
        key = request["Key"]
        if request.get("IfNoneMatch") == "*" and key in self.objects:
            raise _client_error("PreconditionFailed", "PutObject")
        self.objects[key] = {
            "Body": request["Body"],
            "ContentLength": len(request["Body"]),
            "Metadata": request["Metadata"],
            "ContentType": request["ContentType"],
            "ContentEncoding": request["ContentEncoding"],
        }
        return {}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        try:
            object_data = self.objects[Key]
        except KeyError as error:
            raise _client_error("404", "HeadObject") from error
        return {name: value for name, value in object_data.items() if name != "Body"}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        head = self.head_object(Bucket=Bucket, Key=Key)
        body = FakeStreamingBody(self.objects[Key]["Body"])
        self.last_get_body = body
        return {**head, "Body": body}

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.deleted_keys.append(Key)
        self.objects.pop(Key, None)
        return {}


class FakeSession:
    def __init__(self, client: FakeS3Client) -> None:
        self.client_value = client
        self.client_kwargs: dict[str, Any] | None = None

    def client(self, service_name: str, **kwargs: Any) -> FakeS3Client:
        assert service_name == "s3"
        self.client_kwargs = kwargs
        return self.client_value


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


def _store(client: FakeS3Client, **settings: Any) -> S3BlobStore:
    return S3BlobStore(S3Settings(**settings), client=client)


def _run(awaitable: Any) -> Any:
    return asyncio.run(awaitable)


def test_put_verified_uses_literal_content_addressed_key_and_zstd_round_trip() -> None:
    content = b"verified blob content"
    client = FakeS3Client()
    store = _store(client)

    stored = _run(store.put_verified(content, "text/plain"))

    assert stored.sha256 == "db3bcfd4fe23e22fe4e4c55402be81ae983be716347445394250f4db76e8e48e"
    assert stored.object_key == (
        "sha256/db/3b/db3bcfd4fe23e22fe4e4c55402be81ae983be716347445394250f4db76e8e48e.zst"
    )
    assert stored.uncompressed_bytes == len(content)
    assert _run(store.get_verified(stored.object_key, stored.sha256)) == content


def test_two_identical_content_addressed_puts_reuse_the_verified_object() -> None:
    client = FakeS3Client()
    store = _store(client)

    first = _run(store.put_verified(b"same", "application/octet-stream"))
    second = _run(store.put_verified(b"same", "application/octet-stream"))

    assert second == first
    assert client.put_attempts == 1


def test_content_addressed_put_rejects_conflicting_media_type_without_overwrite() -> None:
    client = FakeS3Client()
    store = _store(client)
    stored = _run(store.put_verified(b"same", "text/plain"))

    with pytest.raises(BlobIntegrityError, match="media type"):
        _run(store.put_verified(b"same", "application/json"))

    assert client.put_attempts == 1
    assert client.objects[stored.object_key]["ContentType"] == "text/plain"


def test_conditional_put_accepts_existing_object_after_precondition_failure() -> None:
    client = FakeS3Client()
    store = _store(client, write_mode="if_none_match")
    first = _run(store.put_verified(b"same", "text/plain"))

    second = _run(store.put_verified(b"same", "text/plain"))

    assert second == first
    assert client.put_attempts == 2


def test_conditional_put_retries_conflict_only_up_to_max_attempts() -> None:
    client = FakeS3Client()
    client.put_error_codes = ["ConditionalRequestConflict"] * 3
    store = _store(client, write_mode="if_none_match", max_attempts=3)

    with pytest.raises(BlobStoreError):
        _run(store.put_verified(b"retry", "text/plain"))

    assert client.put_attempts == 3


def test_put_rejects_corrupt_head_metadata_before_returning() -> None:
    client = FakeS3Client()
    store = _store(client)
    original_head = client.head_object

    def corrupt_head(*, Bucket: str, Key: str) -> dict[str, Any]:
        result = original_head(Bucket=Bucket, Key=Key)
        result["Metadata"] = {"sha256": "0" * 64, "uncompressed-bytes": "1"}
        return result

    client.head_object = corrupt_head  # type: ignore[method-assign]

    with pytest.raises(BlobIntegrityError, match="metadata"):
        _run(store.put_verified(b"metadata", "text/plain"))


@pytest.mark.parametrize(
    ("object_key", "expected_sha256"),
    [
        ("not/canonical", "0" * 64),
        ("sha256/aa/bb/" + "a" * 64 + ".zst", "b" * 64),
    ],
)
def test_get_rejects_invalid_key_or_digest_before_network_io(
    object_key: str, expected_sha256: str
) -> None:
    client = FakeS3Client()
    store = _store(client)

    with pytest.raises(BlobIntegrityError):
        _run(store.get_verified(object_key, expected_sha256))


def test_get_rejects_invalid_zstd_frame_and_closes_stream() -> None:
    client = FakeS3Client()
    digest = hashlib.sha256(b"content").hexdigest()
    key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"
    client.objects[key] = {
        "Body": b"not a zstandard frame",
        "ContentLength": len(b"not a zstandard frame"),
        "Metadata": {"sha256": digest, "uncompressed-bytes": "7"},
        "ContentType": "text/plain",
        "ContentEncoding": "zstd",
    }
    store = _store(client)

    with pytest.raises(BlobIntegrityError, match="Zstandard"):
        _run(store.get_verified(key, digest))

    assert client.last_get_body is not None and client.last_get_body.closed


def test_get_closes_stream_when_get_metadata_is_corrupt() -> None:
    client = FakeS3Client()
    store = _store(client)
    stored = _run(store.put_verified(b"metadata", "text/plain"))
    original_get = client.get_object

    def corrupt_get(*, Bucket: str, Key: str) -> dict[str, Any]:
        result = original_get(Bucket=Bucket, Key=Key)
        result["Metadata"] = {"sha256": "0" * 64, "uncompressed-bytes": "8"}
        return result

    client.get_object = corrupt_get  # type: ignore[method-assign]

    with pytest.raises(BlobIntegrityError, match="metadata"):
        _run(store.get_verified(stored.object_key, stored.sha256))

    assert client.last_get_body is not None and client.last_get_body.closed


def test_get_closes_stream_when_get_sizes_do_not_match_head() -> None:
    client = FakeS3Client()
    store = _store(client)
    stored = _run(store.put_verified(b"head-get mismatch", "text/plain"))
    original_get = client.get_object

    def changed_get(*, Bucket: str, Key: str) -> dict[str, Any]:
        result = original_get(Bucket=Bucket, Key=Key)
        result["ContentLength"] += 1
        return result

    client.get_object = changed_get  # type: ignore[method-assign]

    with pytest.raises(BlobIntegrityError, match="changed"):
        _run(store.get_verified(stored.object_key, stored.sha256))

    assert client.last_get_body is not None and client.last_get_body.closed


def test_get_rejects_a_get_media_type_that_differs_from_head() -> None:
    client = FakeS3Client()
    store = _store(client)
    stored = _run(store.put_verified(b"media type", "text/plain"))
    original_get = client.get_object

    def changed_get(*, Bucket: str, Key: str) -> dict[str, Any]:
        result = original_get(Bucket=Bucket, Key=Key)
        result["ContentType"] = "application/json"
        return result

    client.get_object = changed_get  # type: ignore[method-assign]

    with pytest.raises(BlobIntegrityError, match="media type"):
        _run(store.get_verified(stored.object_key, stored.sha256))

    assert client.last_get_body is not None and client.last_get_body.closed


def test_conditional_put_rejects_a_corrupt_existing_object_after_412() -> None:
    client = FakeS3Client()
    store = _store(client, write_mode="if_none_match")
    stored = _run(store.put_verified(b"existing", "text/plain"))
    client.objects[stored.object_key]["Metadata"] = {
        "sha256": "0" * 64,
        "uncompressed-bytes": "8",
    }

    with pytest.raises(BlobIntegrityError, match="metadata"):
        _run(store.put_verified(b"existing", "text/plain"))


def test_conditional_put_rejects_corrupt_existing_bytes_after_412() -> None:
    client = FakeS3Client()
    store = _store(client, write_mode="if_none_match")
    stored = _run(store.put_verified(b"existing", "text/plain"))
    body = client.objects[stored.object_key]["Body"]
    client.objects[stored.object_key]["Body"] = body[:-1] + bytes([body[-1] ^ 0xFF])

    with pytest.raises(BlobIntegrityError):
        _run(store.put_verified(b"existing", "text/plain"))


def test_get_rejects_valid_zstd_frame_with_trailing_bytes() -> None:
    client = FakeS3Client()
    store = _store(client)
    stored = _run(store.put_verified(b"trailing", "text/plain"))
    object_data = client.objects[stored.object_key]
    object_data["Body"] += b"trailing bytes"
    object_data["ContentLength"] = len(object_data["Body"])

    with pytest.raises(BlobIntegrityError, match="invalid Zstandard frame"):
        _run(store.get_verified(stored.object_key, stored.sha256))


def test_get_enforces_compressed_and_uncompressed_limits() -> None:
    content = b"a" * 1024
    client = FakeS3Client()
    store = _store(client)
    stored = _run(store.put_verified(content, "text/plain"))
    small_compressed_store = _store(client, max_compressed_bytes=1)
    small_uncompressed_store = _store(client, max_uncompressed_bytes=1)

    with pytest.raises(BlobIntegrityError, match="compressed"):
        _run(small_compressed_store.get_verified(stored.object_key, stored.sha256))
    with pytest.raises(BlobIntegrityError, match="uncompressed"):
        _run(small_uncompressed_store.get_verified(stored.object_key, stored.sha256))


def test_missing_object_and_repeated_delete_are_safe() -> None:
    client = FakeS3Client()
    store = _store(client)
    digest = "a" * 64
    key = f"sha256/aa/aa/{digest}.zst"

    with pytest.raises(BlobNotFoundError):
        _run(store.get_verified(key, digest))
    _run(store.delete(key))
    _run(store.delete(key))

    assert client.deleted_keys == [key, key]


def test_from_settings_creates_one_path_style_client_with_timeouts_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeS3Client()
    session = FakeSession(client)
    monkeypatch.setattr(
        "agent_context_platform.content.blob_store.boto3.session.Session", lambda **_: session
    )
    settings = S3Settings(
        endpoint_url="https://objects.example.test",
        access_key_id="access",
        secret_access_key="secret",
        connect_timeout_seconds=1.5,
        read_timeout_seconds=2.5,
        max_attempts=4,
    )

    store = S3BlobStore.from_settings(settings)

    assert isinstance(store, S3BlobStore)
    assert session.client_kwargs is not None
    assert session.client_kwargs["endpoint_url"] == "https://objects.example.test/"
    assert session.client_kwargs["config"].connect_timeout == 1.5
    assert session.client_kwargs["config"].read_timeout == 2.5
    assert session.client_kwargs["config"].retries == {"total_max_attempts": 4, "mode": "standard"}
    assert session.client_kwargs["config"].s3 == {"addressing_style": "path"}


def test_content_over_the_upload_limits_is_an_input_error_not_a_stored_object_failure() -> None:
    client = FakeS3Client()
    with pytest.raises(BlobInputLimitError, match="uncompressed"):
        _run(_store(client, max_uncompressed_bytes=1).put_verified(b"ab", "text/plain"))
    with pytest.raises(BlobInputLimitError, match="compressed"):
        _run(_store(client, max_compressed_bytes=1).put_verified(b"ab", "text/plain"))
    assert issubclass(BlobInputLimitError, BlobIntegrityError)  # existing callers unchanged


@pytest.mark.parametrize("write_mode", ["content_addressed", "if_none_match"])
@pytest.mark.parametrize("level", [1, 19])
def test_an_object_compressed_differently_but_intact_is_reused_as_is(
    write_mode: str, level: int
) -> None:
    # Another zstd version, level or tool: the frame checksum, the declared lengths and the
    # SHA-256 of the decompressed bytes prove the object, not its compressed size.
    import zstandard as zstd

    content = b" ".join(
        hashlib.sha256(str(n).encode()).hexdigest()[: n % 7 + 3].encode() for n in range(400)
    )
    digest = hashlib.sha256(content).hexdigest()
    key = f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"
    other = zstd.ZstdCompressor(level=level, write_content_size=True, write_checksum=True)
    body = other.compress(content)
    ours = zstd.ZstdCompressor(level=3, write_content_size=True, write_checksum=True)
    assert len(body) != len(ours.compress(content))  # the case this test is about
    client = FakeS3Client()
    client.objects[key] = {
        "Body": body,
        "ContentLength": len(body),
        "Metadata": {"sha256": digest, "uncompressed-bytes": str(len(content))},
        "ContentType": "text/plain",
        "ContentEncoding": "zstd",
    }
    attempts = client.put_attempts

    stored = _run(_store(client, write_mode=write_mode).put_verified(content, "text/plain"))

    assert stored.object_key == key and stored.compressed_bytes == len(body)
    assert stored.uncompressed_bytes == len(content)
    assert client.objects[key]["Body"] == body  # never rewritten
    if write_mode == "content_addressed":
        assert client.put_attempts == attempts


def test_a_reused_object_still_fails_on_a_wrong_declared_length_or_media_type() -> None:
    content = b"reusable content " * 64
    client = FakeS3Client()
    stored = _run(_store(client).put_verified(content, "text/plain"))
    original = dict(client.objects[stored.object_key])

    client.objects[stored.object_key]["Metadata"] = {
        "sha256": stored.sha256,
        "uncompressed-bytes": str(len(content) + 1),
    }
    with pytest.raises(BlobIntegrityError, match="uncompressed"):
        _run(_store(client).put_verified(content, "text/plain"))
    client.objects[stored.object_key] = {**original, "ContentType": "application/json"}
    with pytest.raises(BlobIntegrityError, match="media type"):
        _run(_store(client).put_verified(content, "text/plain"))


def test_a_conditional_put_retries_once_when_the_existing_object_was_swept_meanwhile() -> None:
    # if_none_match: the PUT is refused because the object exists, then the orphan sweep deletes
    # it before it is verified. The upload is retried once, never reported as a missing object.
    content = b"swept between the put and the head"
    client = FakeS3Client()
    store = _store(client, write_mode="if_none_match")
    _run(store.put_verified(content, "text/plain"))
    put = client.put_object
    keys: list[str] = []

    def put_then_sweep_once(**request: Any) -> dict[str, Any]:
        keys.append(request["Key"])
        try:
            return put(**request)
        finally:
            if len(keys) == 1:
                client.objects.pop(request["Key"], None)  # the sweep, right after the 412

    client.put_object = put_then_sweep_once  # type: ignore[method-assign]
    stored = _run(store.put_verified(content, "text/plain"))

    assert keys == [stored.object_key, stored.object_key]
    assert stored.object_key in client.objects
