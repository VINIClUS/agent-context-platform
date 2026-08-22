"""S3-compatible, content-addressed storage with end-to-end verification."""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from typing import Any, Protocol

import boto3
import zstandard as zstd
from botocore.config import Config
from botocore.exceptions import ClientError

from agent_context_platform.settings import S3Settings

_CANONICAL_KEY = re.compile(
    r"^sha256/(?P<first>[0-9a-f]{2})/(?P<second>[0-9a-f]{2})/"
    r"(?P<digest>[0-9a-f]{64})\.zst$"
)


@dataclass(frozen=True)
class StoredBlob:
    """The verified identity and stored representation of a blob."""

    sha256: str
    object_key: str
    compressed_bytes: int
    uncompressed_bytes: int
    media_type: str


class BlobStore(Protocol):
    """Minimal async contract for verified blob persistence."""

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob: ...

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes: ...

    async def delete(self, object_key: str) -> None: ...


class BlobStoreError(Exception):
    """An S3 operation failed before a verified result was available."""

    def __init__(self, message: str, *, s3_error_code: str = "") -> None:
        super().__init__(message)
        self.s3_error_code = s3_error_code


class BlobNotFoundError(BlobStoreError):
    """A requested verified object does not exist."""


class BlobIntegrityError(BlobStoreError):
    """Stored data or metadata violates the verified blob protocol."""


@dataclass(frozen=True)
class _ObjectMetadata:
    """Verified metadata shared by the HEAD and GET sides of one S3 object."""

    sha256: str
    compressed_bytes: int
    uncompressed_bytes: int
    media_type: str


class S3BlobStore:
    """One shared S3 client wrapped in asynchronous verification operations."""

    def __init__(self, settings: S3Settings, *, client: Any) -> None:
        self._settings = settings
        self._client = client

    @classmethod
    def from_settings(cls, settings: S3Settings) -> S3BlobStore:
        """Build the single, explicitly configured S3 client for this store."""
        access_key_id = (
            None if settings.access_key_id is None else settings.access_key_id.get_secret_value()
        )
        secret_access_key = (
            None
            if settings.secret_access_key is None
            else settings.secret_access_key.get_secret_value()
        )
        session = boto3.session.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=settings.region_name,
        )
        client = session.client(
            "s3",
            endpoint_url=None if settings.endpoint_url is None else str(settings.endpoint_url),
            region_name=settings.region_name,
            config=Config(
                connect_timeout=settings.connect_timeout_seconds,
                read_timeout=settings.read_timeout_seconds,
                retries={"total_max_attempts": settings.max_attempts, "mode": "standard"},
                s3={"addressing_style": settings.addressing_style},
            ),
        )
        return cls(settings, client=client)

    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        """Store content and verify server-visible metadata before returning it."""
        if len(content) > self._settings.max_uncompressed_bytes:
            raise BlobIntegrityError("uncompressed payload exceeds configured limit")

        digest = hashlib.sha256(content).hexdigest()
        object_key = _object_key(digest)
        compressed = zstd.ZstdCompressor(
            level=3, write_content_size=True, write_checksum=True
        ).compress(content)
        if len(compressed) > self._settings.max_compressed_bytes:
            raise BlobIntegrityError("compressed payload exceeds configured limit")

        expected_metadata = _ObjectMetadata(
            sha256=digest,
            compressed_bytes=len(compressed),
            uncompressed_bytes=len(content),
            media_type=media_type,
        )
        if self._settings.write_mode == "content_addressed" and await self._reuse_existing(
            object_key, content, expected_metadata
        ):
            return self._stored_blob(object_key, expected_metadata)

        request: dict[str, Any] = {
            "Bucket": self._settings.bucket_name,
            "Key": object_key,
            "Body": compressed,
            "Metadata": {"sha256": digest, "uncompressed-bytes": str(len(content))},
            "ContentType": media_type,
            "ContentEncoding": "zstd",
        }
        created = await self._put(request)
        if created:
            head = await self._head(object_key)
            self._verify_expected_metadata(
                self._validate_response(head, expected_sha256=digest), expected_metadata
            )
        else:
            await self._verify_existing(object_key, content, expected_metadata)
        return self._stored_blob(object_key, expected_metadata)

    @staticmethod
    def _stored_blob(object_key: str, metadata: _ObjectMetadata) -> StoredBlob:
        return StoredBlob(
            sha256=metadata.sha256,
            object_key=object_key,
            compressed_bytes=metadata.compressed_bytes,
            uncompressed_bytes=metadata.uncompressed_bytes,
            media_type=metadata.media_type,
        )

    async def _reuse_existing(
        self, object_key: str, content: bytes, expected_metadata: _ObjectMetadata
    ) -> bool:
        try:
            await self._verify_existing(object_key, content, expected_metadata)
        except BlobNotFoundError:
            return False
        return True

    async def _verify_existing(
        self, object_key: str, content: bytes, expected_metadata: _ObjectMetadata
    ) -> None:
        head = await self._head(object_key)
        self._verify_expected_metadata(
            self._validate_response(head, expected_sha256=expected_metadata.sha256),
            expected_metadata,
        )
        existing_content = await self.get_verified(object_key, expected_metadata.sha256)
        if existing_content != content:
            raise BlobIntegrityError("stored content does not match the requested content")

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        """Download only a canonical object and prove its compressed and raw identities."""
        digest = _validate_key(object_key)
        if expected_sha256 != digest:
            raise BlobIntegrityError("object key digest does not match expected SHA-256")

        head = await self._head(object_key)
        head_metadata = self._validate_response(head, expected_sha256=expected_sha256)
        response = await self._call("get_object", Bucket=self._settings.bucket_name, Key=object_key)
        body = response.get("Body")
        if body is None:
            raise BlobIntegrityError("GET response did not contain a streaming body")
        try:
            get_metadata = self._validate_response(
                response,
                expected_sha256=expected_sha256,
                expected_media_type=head_metadata.media_type,
            )
            if get_metadata != head_metadata:
                raise BlobIntegrityError("object changed between HEAD and GET")
            compressed = await asyncio.to_thread(body.read, self._settings.max_compressed_bytes + 1)
            if not isinstance(compressed, bytes):
                raise BlobIntegrityError("streaming body did not return bytes")
            if len(compressed) > self._settings.max_compressed_bytes:
                raise BlobIntegrityError("compressed payload exceeds configured limit")
            if len(compressed) != get_metadata.compressed_bytes:
                raise BlobIntegrityError("compressed payload length does not match metadata")

            try:
                frame = zstd.get_frame_parameters(compressed)
                if not frame.has_checksum:
                    raise BlobIntegrityError("Zstandard frame is missing its checksum")
                if frame.content_size != get_metadata.uncompressed_bytes:
                    raise BlobIntegrityError("Zstandard frame content size does not match metadata")
                content = zstd.ZstdDecompressor().decompress(
                    compressed,
                    max_output_size=self._settings.max_uncompressed_bytes,
                    allow_extra_data=False,
                )
            except BlobIntegrityError:
                raise
            except zstd.ZstdError as error:
                raise BlobIntegrityError("invalid Zstandard frame") from error
            if len(content) > self._settings.max_uncompressed_bytes:
                raise BlobIntegrityError("uncompressed payload exceeds configured limit")
            if len(content) != get_metadata.uncompressed_bytes:
                raise BlobIntegrityError("uncompressed payload length does not match metadata")
            if hashlib.sha256(content).hexdigest() != expected_sha256:
                raise BlobIntegrityError(
                    "uncompressed payload SHA-256 does not match expected digest"
                )
            return content
        finally:
            await asyncio.to_thread(body.close)

    async def delete(self, object_key: str) -> None:
        """Delete a canonical content-addressed key; S3 delete remains idempotent."""
        _validate_key(object_key)
        try:
            await self._call("delete_object", Bucket=self._settings.bucket_name, Key=object_key)
        except BlobNotFoundError:
            return

    async def _put(self, request: dict[str, Any]) -> bool:
        if self._settings.write_mode == "content_addressed":
            await self._call("put_object", **request)
            return True
        request["IfNoneMatch"] = "*"
        for attempt in range(self._settings.max_attempts):
            try:
                await self._call("put_object", **request)
                return True
            except BlobStoreError as error:
                code = _error_code(error)
                if code in {"412", "PreconditionFailed"}:
                    return False
                if code not in {"409", "ConditionalRequestConflict"}:
                    raise
                if attempt + 1 == self._settings.max_attempts:
                    raise
        raise BlobStoreError("conditional PUT exhausted configured attempts")

    async def _head(self, object_key: str) -> dict[str, Any]:
        result = await self._call("head_object", Bucket=self._settings.bucket_name, Key=object_key)
        if not isinstance(result, dict):
            raise BlobIntegrityError("HEAD response was not a mapping")
        return result

    async def _call(self, operation: str, **kwargs: Any) -> dict[str, Any]:
        method = getattr(self._client, operation)
        try:
            result = await asyncio.to_thread(method, **kwargs)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise BlobNotFoundError(f"S3 {operation} could not find the object") from error
            raise BlobStoreError(
                f"S3 {operation} failed ({code or 'unknown error'})", s3_error_code=code
            ) from error
        if not isinstance(result, dict):
            raise BlobStoreError(f"S3 {operation} returned an unexpected response")
        return result

    def _validate_response(
        self,
        response: dict[str, Any],
        *,
        expected_sha256: str,
        expected_media_type: str | None = None,
    ) -> _ObjectMetadata:
        metadata = response.get("Metadata")
        if not isinstance(metadata, dict) or metadata.get("sha256") != expected_sha256:
            raise BlobIntegrityError("object metadata SHA-256 does not match")
        try:
            uncompressed_bytes = int(metadata["uncompressed-bytes"])
        except (KeyError, TypeError, ValueError) as error:
            raise BlobIntegrityError(
                "object metadata has no valid uncompressed byte count"
            ) from error
        compressed_bytes = response.get("ContentLength")
        if not isinstance(compressed_bytes, int) or compressed_bytes < 0:
            raise BlobIntegrityError("object has no valid compressed byte count")
        if uncompressed_bytes < 0:
            raise BlobIntegrityError("object metadata has a negative uncompressed byte count")
        if compressed_bytes > self._settings.max_compressed_bytes:
            raise BlobIntegrityError("compressed payload exceeds configured limit")
        if uncompressed_bytes > self._settings.max_uncompressed_bytes:
            raise BlobIntegrityError("uncompressed payload exceeds configured limit")
        content_type = response.get("ContentType")
        if not isinstance(content_type, str) or not content_type:
            raise BlobIntegrityError("object has no valid media type")
        if expected_media_type is not None and content_type != expected_media_type:
            raise BlobIntegrityError("object media type does not match")
        if response.get("ContentEncoding") != "zstd":
            raise BlobIntegrityError("object content encoding is not zstd")
        return _ObjectMetadata(
            sha256=expected_sha256,
            compressed_bytes=compressed_bytes,
            uncompressed_bytes=uncompressed_bytes,
            media_type=content_type,
        )

    @staticmethod
    def _verify_expected_metadata(actual: _ObjectMetadata, expected: _ObjectMetadata) -> None:
        if actual.compressed_bytes != expected.compressed_bytes:
            raise BlobIntegrityError("object compressed byte count does not match")
        if actual.uncompressed_bytes != expected.uncompressed_bytes:
            raise BlobIntegrityError("object uncompressed byte count does not match")
        if actual.media_type != expected.media_type:
            raise BlobIntegrityError("object media type does not match")


def _object_key(digest: str) -> str:
    return f"sha256/{digest[:2]}/{digest[2:4]}/{digest}.zst"


def _validate_key(object_key: str) -> str:
    match = _CANONICAL_KEY.fullmatch(object_key)
    if match is None:
        raise BlobIntegrityError("object key is not a canonical SHA-256 key")
    digest = match.group("digest")
    if match.group("first") != digest[:2] or match.group("second") != digest[2:4]:
        raise BlobIntegrityError("object key prefix does not match its SHA-256")
    return digest


def _error_code(error: BlobStoreError) -> str:
    return str(getattr(error, "s3_error_code", ""))
