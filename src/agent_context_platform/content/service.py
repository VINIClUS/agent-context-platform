"""Coordinate sanitized content persistence: prepare, verify, and attach.

Security boundary: sanitized content is the only content this platform may
store, and no committed ``ledger.event_content_refs`` row may reference
inline or object-storage content that was never durably written. Every
public exception in this module carries a stable, content-free
``error_code`` and a content-free message; no exception message, log
statement, or docstring in this module may echo raw or decoded payload
bytes, field names, or SDK-reported findings.

Required call order
--------------------
1. ``await ContentService.prepare(items)`` validates every item, re-checks
   the platform's own redaction policy against each item's already
   "sanitized" bytes, and uploads object-storage bytes to the configured
   :class:`BlobStore`. It returns a :class:`PreparedContent` holding, per
   ``content_id``, both the resolved :class:`ContentRefV1` and the item's
   original :class:`RedactionReportV1` (see :meth:`PreparedContent.report_for`,
   the PLATFORM-020 seam).
2. ``await ContentService.attach(session, prepared)`` durably records every
   prepared digest in ``catalog.content_objects`` / ``catalog.inline_contents``
   within the caller's transaction.
3. Only after step 2 returns may the caller append the owning event(s) and
   their ``ledger.event_content_refs`` rows **in the same transaction** as
   step 2. Because the storage rows and the referencing rows are written in
   one transaction, a rollback of the caller's transaction undoes both, and
   a commit makes both durable together -- no reference can ever dangle.

Advisory lock key derivation
-----------------------------
``attach()`` and :class:`~agent_context_platform.content.orphans.OrphanSweeper`
race on the same object-storage digests: one is trying to durably record a
reference to a digest, the other is trying to delete an apparently
unreferenced one. Both sides MUST call :func:`content_digest_lock_key` --
the *same* function, not independent reimplementations -- to compute the
``pg_advisory_xact_lock`` key for a digest, and both MUST take an
*exclusive* transaction-scoped lock (``pg_advisory_xact_lock``, not a
shared-lock variant) before acting on that digest. The key is the two-
``int4`` form: a fixed namespace constant plus the first four bytes of the
lowercase hex SHA-256 digest reinterpreted as a signed big-endian int32.
The namespace exists so this lock key space can never collide with an
unrelated advisory lock elsewhere in the platform. This mechanism assumes
the default ``READ COMMITTED`` isolation level used throughout this
codebase (see ``db.py``): each statement after the lock is acquired sees a
fresh snapshot, so a re-check made after acquiring the lock correctly
observes the other side's commit. A stricter isolation level would need a
different design.

Object-storage media type is a fixed constant, not the claimed media type
---------------------------------------------------------------------------
Dedupe-by-digest (coordinator decision 4) means two items with identical
bytes but different claimed media types must both attach successfully,
each keeping its own ``ContentRefV1.media_type``. But ``BlobStore`` is
content-addressed purely by digest, and re-uploading an already-stored
digest re-verifies the *new* call's media type against the *first*
upload's stored ``ContentType`` -- a second, different media type fails
closed with ``BlobIntegrityError``. So every object-storage upload uses
one fixed, content-free media type (``application/octet-stream``)
regardless of the claim, and ``catalog.content_objects.media_type`` is
therefore not meaningful per reference by design; the meaningful,
per-reference media type is always ``ContentRefV1.media_type`` /
``claim.media_type``, populated independently of what was stored.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Final, cast

from agent_context_sdk import (
    ContentClaimV1,
    ContentRefV1,
    ContentStorage,
    JsonValue,
    RedactionError,
    RedactionPolicyV1,
    RedactionReportV1,
    SanitizedContentItemV1,
    canonical_json_bytes,
    redact_json,
)
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from agent_context_platform.catalog.models import ContentObjectRow
from agent_context_platform.content.blob_store import BlobStore, StoredBlob
from agent_context_platform.content.models import INLINE_MAX_BYTES, InlineContentRow
from agent_context_platform.operations.faults import fault_point

#: Fixed namespace for every advisory lock this module (and the orphan
#: sweeper) takes on a content digest. Arbitrary but stable: changing it
#: is a breaking change for any in-flight lock holder.
_ADVISORY_LOCK_NAMESPACE: Final[int] = 0x434F_4E54  # b"CONT"

_JSON_MEDIA_TYPE: Final[str] = "application/json"
_TEXT_MEDIA_TYPE: Final[str] = "text/plain"

#: Ceiling on JSON nesting depth enforced by ``_parse_json_strict``, independent
#: of ``json.loads``'s own ``RecursionError`` guard (see that function's
#: docstring). Matches ``agent_context_sdk.RedactionPolicyV1.max_depth``'s own
#: upper bound (``le=256``), so this can never be the reason a caller-configured
#: policy's otherwise-legitimate content is rejected: nothing a valid policy
#: would accept is deeper than this.
_MAX_JSON_DEPTH: Final[int] = 256

#: Fixed, content-free media type used for *every* object-storage upload,
#: regardless of the claimed media type. See "Object-storage media type is
#: a fixed constant" in the module docstring.
_OBJECT_STORAGE_MEDIA_TYPE: Final[str] = "application/octet-stream"


class ContentServiceError(ValueError):
    """Base for content preparation and resolution failures.

    Every subclass carries a stable ``error_code`` a caller may surface
    publicly (compare :class:`agent_context_sdk.RejectedEventV1.error_code`).
    Messages are deliberately content-free: never format decoded content,
    field names, or a caught exception's own message into one of these.
    """

    error_code: ClassVar[str] = "content_service_error"


class DuplicateContentIdError(ContentServiceError):
    """Two items in the same ``prepare()`` batch share a ``content_id``."""

    error_code: ClassVar[str] = "duplicate_content_id"


class UnsupportedMediaTypeError(ContentServiceError):
    """The claimed media type is not one this service can process."""

    error_code: ClassVar[str] = "unsupported_media_type"


class InvalidContentEncodingError(ContentServiceError):
    """The content is not valid UTF-8 / canonical JSON for its media type."""

    error_code: ClassVar[str] = "invalid_content_encoding"


class ContentRequiresRedactionError(ContentServiceError):
    """Re-applying the redaction policy to this content would change it."""

    error_code: ClassVar[str] = "content_requires_redaction"


class ContentResolutionError(ContentServiceError):
    """``PreparedContent.resolve()`` claims do not match the prepared batch."""

    error_code: ClassVar[str] = "content_resolution_mismatch"


def content_digest_lock_key(content_sha256: str) -> tuple[int, int]:
    """Derive the ``pg_advisory_xact_lock(int4, int4)`` key for a digest.

    ``ContentService.attach`` and ``OrphanSweeper.run`` MUST both call this
    exact function to serialize on the same per-digest lock. A divergent
    derivation on either side silently creates two disjoint lock key
    spaces and defeats the mutual exclusion this mechanism exists for.
    """
    digest_key = int.from_bytes(bytes.fromhex(content_sha256[:8]), byteorder="big", signed=True)
    return (_ADVISORY_LOCK_NAMESPACE, digest_key)


def _media_type_base(media_type: str) -> str:
    return media_type.split(";", 1)[0].strip().lower()


def _decode_utf8_strict(data: bytes) -> str:
    try:
        return data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise InvalidContentEncodingError("content is not valid UTF-8") from None


class _DuplicateJsonKeyError(ValueError):
    """Internal signal raised by the strict object_pairs_hook, caught locally."""


class _NonFiniteJsonConstantError(ValueError):
    """Internal signal raised when JSON uses NaN / Infinity / -Infinity."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise _DuplicateJsonKeyError(key)
        seen[key] = value
    return seen


def _reject_non_finite_constant(constant: str) -> float:
    raise _NonFiniteJsonConstantError(constant)


def _exceeds_max_json_depth(value: JsonValue, *, max_depth: int) -> bool:
    """Return whether ``value`` nests deeper than ``max_depth``.

    Walks depth-first using an explicit stack, never Python recursion, so
    arbitrarily deep input costs heap, not call-stack, to check -- see
    ``_parse_json_strict``'s docstring for why that distinction matters.
    """
    stack: list[tuple[JsonValue, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            return True
        if type(current) is list:
            stack.extend((item, depth + 1) for item in current)
        elif type(current) is dict:
            stack.extend((item, depth + 1) for item in current.values())
    return False


def _parse_json_strict(text_value: str) -> JsonValue:
    """Parse ``text_value`` as JSON, rejecting anything not safe to process further.

    ``json.loads`` uses CPython's C-accelerated scanner here, which recurses
    in C, not Python, to walk nested arrays/objects, guarded against runaway
    C recursion by a check whose trip point is not a portable constant: it
    was observed to vary by available C stack, not just by interpreter
    version. 50,000 levels of nesting raised ``RecursionError`` during
    parsing on every environment this suite ran on except one -- a CI job
    running the identical CPython 3.14.6 patch release as a local run that
    rejected the same input (reproduced locally with ``ulimit -s
    unlimited``). The explicit, iterative ``_exceeds_max_json_depth`` check
    below is what makes rejection deterministic across every platform: it
    runs whether or not ``json.loads`` itself happened to raise.
    """
    try:
        parsed = json.loads(
            text_value,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite_constant,
        )
    except _DuplicateJsonKeyError:
        raise InvalidContentEncodingError("content contains duplicate JSON object keys") from None
    except _NonFiniteJsonConstantError:
        raise InvalidContentEncodingError(
            "content contains a non-finite JSON numeric constant"
        ) from None
    except RecursionError:
        raise InvalidContentEncodingError("content is nested too deeply to parse") from None
    except json.JSONDecodeError:
        raise InvalidContentEncodingError("content is not valid JSON") from None
    if _exceeds_max_json_depth(parsed, max_depth=_MAX_JSON_DEPTH):
        raise InvalidContentEncodingError("content is nested too deeply to parse") from None
    return cast(JsonValue, parsed)


def _canonicalize_or_reject(value: JsonValue) -> bytes:
    try:
        # agent_context_sdk ships no py.typed marker (see the pyproject.toml
        # mypy override), so canonical_json_bytes is Any-typed to mypy; cast
        # to the type its own source declares.
        return cast(bytes, canonical_json_bytes(value))
    except Exception:
        # canonical_json_bytes's own numeric-range errors can embed the
        # rejected value in their message; never surface str(exc) here.
        raise InvalidContentEncodingError("content could not be canonicalized") from None


@dataclass(frozen=True, slots=True)
class _PreparedItem:
    """One prepared item: its resolved ref, its report, and its bytes."""

    claim: ContentClaimV1
    ref: ContentRefV1
    redaction_report: RedactionReportV1
    data: bytes | None
    stored_blob: StoredBlob | None


class PreparedContent:
    """The verified outcome of :meth:`ContentService.prepare`.

    Holds one prepared item per ``content_id``. Not constructed directly by
    callers outside this module.
    """

    def __init__(self, items: tuple[_PreparedItem, ...]) -> None:
        self._items = items
        self._by_content_id: dict[str, _PreparedItem] = {
            item.claim.content_id: item for item in items
        }

    @property
    def content_ids(self) -> tuple[str, ...]:
        """The ``content_id`` of every prepared item, in preparation order."""
        return tuple(item.claim.content_id for item in self._items)

    def resolve(self, claims: Sequence[ContentClaimV1]) -> tuple[ContentRefV1, ...]:
        """Resolve event-declared claims to their prepared content refs.

        ``claims`` may be any subset of this prepared batch: one
        ``prepare()`` call covers a whole ingestion batch
        (``IngestBatchRequestV1.content_items``), while ``resolve()`` is
        called once per event with that event's own ``content_claims``, and
        the SDK's batch shape explicitly allows the same ``content_id`` to
        be claimed by more than one event. So this is a per-claim
        membership and identity check, not a batch-vs-claims exact match:
        every ``content_id`` must be unique within ``claims``, and every
        claim must match a prepared item's ``(content_sha256, media_type,
        uncompressed_bytes)`` exactly. It is not an error for a prepared
        item to go unclaimed by this call -- the batch-level "every item is
        claimed by some event" invariant belongs to the ingestion batch
        validator, and ``seal_event`` still performs its own exact match
        against one event's claims. Returns refs in claim order.
        """
        claims_by_id: dict[str, ContentClaimV1] = {}
        for claim in claims:
            if claim.content_id in claims_by_id:
                raise ContentResolutionError("duplicate content_id in claims")
            claims_by_id[claim.content_id] = claim

        resolved: list[ContentRefV1] = []
        for claim in claims:
            item = self._by_content_id.get(claim.content_id)
            if item is None:
                raise ContentResolutionError("claim does not match any prepared content")
            claim_identity = (claim.content_sha256, claim.media_type, claim.uncompressed_bytes)
            ref_identity = (
                item.ref.content_sha256,
                item.ref.media_type,
                item.ref.uncompressed_bytes,
            )
            if claim_identity != ref_identity:
                raise ContentResolutionError("claim metadata does not match prepared content")
            resolved.append(item.ref)
        return tuple(resolved)

    def report_for(self, content_id: str) -> RedactionReportV1:
        """Return the prepared item's original :class:`RedactionReportV1`."""
        try:
            return self._by_content_id[content_id].redaction_report
        except KeyError:
            raise ContentResolutionError("no prepared content for that content_id") from None

    @property
    def redaction_reports(self) -> Mapping[str, RedactionReportV1]:
        """Every prepared item's report, keyed by ``content_id``."""
        return {
            content_id: item.redaction_report for content_id, item in self._by_content_id.items()
        }


class ContentService:
    """Prepare, verify, and durably attach sanitized content.

    See the module docstring for the required ``prepare`` -> ``attach`` ->
    append-events-and-refs ordering and the advisory lock key derivation
    shared with :class:`~agent_context_platform.content.orphans.OrphanSweeper`.
    """

    def __init__(
        self,
        blob_store: BlobStore,
        policy: RedactionPolicyV1,
        *,
        inline_threshold: int = INLINE_MAX_BYTES,
    ) -> None:
        if not (0 < inline_threshold <= INLINE_MAX_BYTES):
            raise ValueError(
                f"inline_threshold must be in (0, {INLINE_MAX_BYTES}], got {inline_threshold}"
            )
        self._blob_store = blob_store
        self._policy = policy
        self._inline_threshold = inline_threshold

    async def prepare(self, items: Sequence[SanitizedContentItemV1]) -> PreparedContent:
        """Validate, re-check, and store every item; fail the whole batch atomically.

        Every item is validated (unique ``content_id``, supported media
        type, valid encoding, and a defense-in-depth redaction re-check)
        *before* any bytes are uploaded to object storage, so a batch that
        is ultimately rejected never leaves a partial upload behind.

        The redaction re-check is value equality, not "any finding
        rejects": this item's decoded value is re-run through
        ``redact_json`` using the platform's own policy (never the
        producer-supplied ``correlation_key``, which does not affect the
        placeholder text substituted into the value and is therefore
        irrelevant to this check); the item is rejected with
        :class:`ContentRequiresRedactionError` iff that produces a
        canonically different value. See the PR description for a known
        SDK limitation this implies for some already-redacted shapes.
        """
        validated: list[tuple[SanitizedContentItemV1, bytes]] = []
        seen_content_ids: set[str] = set()
        for item in items:
            content_id = item.claim.content_id
            if content_id in seen_content_ids:
                raise DuplicateContentIdError("duplicate content_id in prepare() batch")
            seen_content_ids.add(content_id)

            try:
                data = base64.b64decode(item.sanitized_bytes_base64, validate=True)
            except (binascii.Error, ValueError):
                raise InvalidContentEncodingError("content base64 could not be decoded") from None

            # Pydantic validates the claim's shape, not its relation to the
            # bytes, and model_construct() skips even that: recompute both
            # so bytes can never be stored under a digest they do not have.
            if (
                hashlib.sha256(data).hexdigest() != item.claim.content_sha256
                or len(data) != item.claim.uncompressed_bytes
            ):
                raise ContentResolutionError("content bytes do not match their claim")

            self._recheck_redaction(item.claim.media_type, data)
            validated.append((item, data))

        object_blob_cache: dict[str, StoredBlob] = {}
        prepared_items = [
            await self._store_one(item, data, object_blob_cache) for item, data in validated
        ]
        return PreparedContent(tuple(prepared_items))

    def _recheck_redaction(self, media_type: str, data: bytes) -> None:
        base_media_type = _media_type_base(media_type)
        if base_media_type == _JSON_MEDIA_TYPE:
            value: JsonValue = _parse_json_strict(_decode_utf8_strict(data))
        elif base_media_type == _TEXT_MEDIA_TYPE:
            value = _decode_utf8_strict(data)
        else:
            raise UnsupportedMediaTypeError("media type is not supported for content storage")

        try:
            result = redact_json(value, self._policy)
        except (RedactionError, RecursionError):
            # RecursionError is a defense-in-depth backstop, not a reachable
            # path today: _parse_json_strict's _exceeds_max_json_depth check
            # already bounds `value` to _MAX_JSON_DEPTH (<= any valid
            # policy.max_depth) before this call. Caught here too in case a
            # future caller reaches this method with unvalidated depth, or a
            # future SDK release changes redact_json's own guard.
            raise InvalidContentEncodingError(
                "content could not be processed by the redaction policy"
            ) from None

        original_canonical = _canonicalize_or_reject(value)
        redacted_canonical = _canonicalize_or_reject(result.value)
        if redacted_canonical != original_canonical:
            raise ContentRequiresRedactionError(
                "applying the redaction policy to this content would change it"
            )

    async def _store_one(
        self,
        item: SanitizedContentItemV1,
        data: bytes,
        object_blob_cache: dict[str, StoredBlob],
    ) -> _PreparedItem:
        claim = item.claim
        if len(data) <= self._inline_threshold:
            ref = ContentRefV1(
                content_id=claim.content_id,
                content_sha256=claim.content_sha256,
                media_type=claim.media_type,
                uncompressed_bytes=claim.uncompressed_bytes,
                disposition=item.redaction_report.disposition,
                storage=ContentStorage.INLINE,
                inline_id=claim.content_sha256,
            )
            return _PreparedItem(
                claim=claim,
                ref=ref,
                redaction_report=item.redaction_report,
                data=data,
                stored_blob=None,
            )

        stored_blob = object_blob_cache.get(claim.content_sha256)
        if stored_blob is None:
            # Fixed media type (see module docstring); also cached per
            # digest for this batch so a repeated digest reuses the first
            # upload's verified result instead of paying a redundant
            # get_verified() round-trip inside put_verified()'s own reuse
            # check.
            fault_point("content.before_s3_put")
            stored_blob = await self._blob_store.put_verified(data, _OBJECT_STORAGE_MEDIA_TYPE)
            fault_point("content.after_head_verification")
            object_blob_cache[claim.content_sha256] = stored_blob
        ref = ContentRefV1(
            content_id=claim.content_id,
            content_sha256=claim.content_sha256,
            media_type=claim.media_type,
            uncompressed_bytes=claim.uncompressed_bytes,
            disposition=item.redaction_report.disposition,
            storage=ContentStorage.OBJECT,
            object_key=stored_blob.object_key,
            encoding="zstd",
        )
        return _PreparedItem(
            claim=claim,
            ref=ref,
            redaction_report=item.redaction_report,
            data=None,
            stored_blob=stored_blob,
        )

    async def attach(self, session: AsyncSession, prepared: PreparedContent) -> None:
        """Durably record prepared content in the caller's transaction.

        Must be called after :meth:`prepare` and before the caller appends
        the owning event(s) and their ``ledger.event_content_refs`` rows,
        all within the same transaction as this call (see the module
        docstring). Object-storage digests are locked and re-verified
        against the :class:`BlobStore` before being recorded, serialized
        against :class:`~agent_context_platform.content.orphans.OrphanSweeper`
        by :func:`content_digest_lock_key`; inline digests need no lock
        because nothing ever deletes an inline row.

        Raises :class:`~agent_context_platform.content.blob_store.BlobStoreError`
        (or a subclass, e.g. ``BlobNotFoundError``) if an object-storage
        digest cannot be re-verified -- for example because
        ``OrphanSweeper`` concurrently deleted it between ``prepare()`` and
        ``attach()``. The caller's transaction is expected to roll back in
        that case, and ``prepare()`` must be retried with fresh content.
        """
        object_items: dict[str, StoredBlob] = {}
        inline_items: dict[str, tuple[str, bytes]] = {}
        # PreparedContent and ContentService live in this module together;
        # attach() reads PreparedContent's internal items directly rather
        # than growing PreparedContent's public surface for one caller.
        for item in prepared._items:
            if item.stored_blob is not None:
                object_items[item.claim.content_sha256] = item.stored_blob
            elif item.data is not None:
                inline_items[item.claim.content_sha256] = (item.claim.media_type, item.data)
            else:
                raise AssertionError("prepared item has neither inline data nor a stored blob")

        # Sorted and deduped so two concurrent attach() calls touching
        # overlapping digest sets always acquire locks in the same order --
        # this is what prevents a lock-ordering deadlock between them.
        for digest in sorted(object_items):
            stored_blob = object_items[digest]
            await self._lock_digest(session, digest)
            await self._blob_store.get_verified(stored_blob.object_key, digest)
            insert_object = (
                pg_insert(ContentObjectRow)
                .values(
                    content_sha256=digest,
                    object_key=stored_blob.object_key,
                    media_type=stored_blob.media_type,
                    compressed_bytes=stored_blob.compressed_bytes,
                    uncompressed_bytes=stored_blob.uncompressed_bytes,
                )
                .on_conflict_do_nothing(index_elements=["content_sha256"])
            )
            await session.execute(insert_object)

        # Sorted for the same deadlock-avoidance reason as the object loop
        # above: two concurrent attach() calls sharing inline digests must
        # take their ON CONFLICT row locks in a consistent order.
        for digest in sorted(inline_items):
            media_type, data = inline_items[digest]
            insert_inline = (
                pg_insert(InlineContentRow)
                .values(
                    inline_id=digest,
                    content_sha256=digest,
                    media_type=media_type,
                    uncompressed_bytes=len(data),
                    data=data,
                )
                .on_conflict_do_nothing(index_elements=["inline_id"])
            )
            await session.execute(insert_inline)

    @staticmethod
    async def _lock_digest(session: AsyncSession, content_sha256: str) -> None:
        namespace, digest_key = content_digest_lock_key(content_sha256)
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:namespace, :digest_key)"),
            {"namespace": namespace, "digest_key": digest_key},
        )
