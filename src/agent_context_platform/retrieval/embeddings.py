"""Local text embeddings: the provider interface, the pinned model manifest and its verification.

Embeddings are a projection: rebuildable from sanitized content, never a source of truth. The
default provider runs the multilingual MiniLM sentence model on `onnxruntime` (no torch, which
does not fit the small VPS profile) and loads the model's ONNX export and tokenizer from one
directory.

Model files are never downloaded by the service. `fetch_model` (the `agent-context models fetch`
command) is the only code that touches the network: it downloads each file of `models.lock.json`
by revision and refuses any file whose sha256 differs. `LocalMiniLMProvider.load` re-checks every
digest and fails closed (`ModelIntegrityError`), so a swapped or truncated file never runs.

Output matches sentence-transformers: mean pooling over the attention mask, then L2
normalization, 384 dimensions. Input is truncated at the model's max sequence length (read from
the manifest, 128 tokens); a text that reaches that window is reported as truncated on its result.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from importlib import resources
from pathlib import Path, PurePosixPath
from typing import IO, Any, Final, Protocol, cast, runtime_checkable
from urllib.parse import quote
from urllib.request import Request, urlopen

import numpy as np

MANIFEST_NAME: Final = "models.lock.json"
DEFAULT_BATCH_SIZE: Final = 16
MAX_BATCH_SIZE: Final = 256
# Characters, not tokens: bounds the tokenizer's work before it truncates at the token limit.
DEFAULT_MAX_INPUT_CHARS: Final = 16_384
_CHUNK: Final = 1 << 20
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")
_REVISION: Final = re.compile(r"^[0-9a-f]{40}$")
_DOWNLOAD_TIMEOUT_SECONDS: Final = 60.0
_HUB_URL: Final = "https://huggingface.co"


class EmbeddingError(RuntimeError):
    """An embedding provider could not produce vectors."""


class ModelManifestError(ValueError):
    """`models.lock.json` is malformed."""


class ModelIntegrityError(EmbeddingError):
    """A model file is missing or does not match the digest pinned in the manifest."""


@dataclass(frozen=True, slots=True)
class EmbeddedText:
    """One text's vector and whether the input was cut to fit the model."""

    vector: tuple[float, ...]
    truncated: bool


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Turns texts into fixed-size vectors; implementations bound batch size and input length."""

    model_id: str
    model_revision: str
    dimensions: int

    async def embed(self, texts: Sequence[str]) -> list[EmbeddedText]:
        """Embed `texts` in order; the result has one entry per input."""
        ...


@dataclass(frozen=True, slots=True)
class ModelFile:
    path: str
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ModelManifest:
    """The pinned model: repository, commit revision and the digest of every file."""

    model_id: str
    revision: str
    dimensions: int
    max_sequence_length: int
    files: tuple[ModelFile, ...]

    def file(self, path: str) -> ModelFile:
        for item in self.files:
            if item.path == path:
                return item
        raise ModelManifestError(f"manifest has no file {path}")


def _parse_file(raw: object) -> ModelFile:
    if not isinstance(raw, dict):
        raise ModelManifestError("manifest file entry must be an object")
    path, size, sha256 = raw.get("path"), raw.get("size"), raw.get("sha256")
    if not isinstance(path, str) or not isinstance(size, int) or not isinstance(sha256, str):
        raise ModelManifestError("manifest file entry needs path, size and sha256")
    pure = PurePosixPath(path)
    if pure.is_absolute() or ".." in pure.parts or not path or "\\" in path:
        raise ModelManifestError("manifest file path must be relative and stay in the model dir")
    if size < 0 or not _SHA256.fullmatch(sha256):
        raise ModelManifestError("manifest file size or sha256 is invalid")
    return ModelFile(path=path, size=size, sha256=sha256)


def parse_manifest(document: object) -> ModelManifest:
    """Validate a decoded manifest document."""
    if not isinstance(document, dict) or document.get("schema") != 1:
        raise ModelManifestError("unsupported manifest schema")
    model_id, revision = document.get("model_id"), document.get("revision")
    dimensions, max_length = document.get("dimensions"), document.get("max_sequence_length")
    files = document.get("files")
    if not isinstance(model_id, str) or not model_id.strip():
        raise ModelManifestError("manifest model_id is required")
    if not isinstance(revision, str) or not _REVISION.fullmatch(revision):
        raise ModelManifestError("manifest revision must be a 40-character commit SHA")
    if not isinstance(dimensions, int) or dimensions < 1:
        raise ModelManifestError("manifest dimensions must be a positive integer")
    if not isinstance(max_length, int) or max_length < 2:
        raise ModelManifestError("manifest max_sequence_length must be at least 2")
    if not isinstance(files, list) or not files:
        raise ModelManifestError("manifest files must be a non-empty list")
    parsed = tuple(_parse_file(item) for item in files)
    if len({item.path for item in parsed}) != len(parsed):
        raise ModelManifestError("manifest lists a file twice")
    return ModelManifest(model_id, revision, dimensions, max_length, parsed)


def load_manifest() -> ModelManifest:
    """The manifest shipped with the package (the release lock consumes the same file)."""
    text = resources.files(__package__).joinpath(MANIFEST_NAME).read_text(encoding="utf-8")
    return parse_manifest(json.loads(text))


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def verify_model_dir(manifest: ModelManifest, model_dir: Path) -> None:
    """Raise `ModelIntegrityError` unless every manifest file is present with its pinned digest.

    Messages name the file, never its contents.
    """
    for item in manifest.files:
        path = model_dir / item.path
        if not path.is_file() or path.is_symlink():
            raise ModelIntegrityError(f"model file missing: {item.path}")
        if path.stat().st_size != item.size or _sha256_of(path) != item.sha256:
            raise ModelIntegrityError(f"model file digest mismatch: {item.path}")


Opener = Callable[[Request, float], IO[bytes]]


def _open_url(request: Request, timeout: float) -> IO[bytes]:
    return cast(IO[bytes], urlopen(request, timeout=timeout))


def model_url(manifest: ModelManifest, item: ModelFile, *, hub_url: str = _HUB_URL) -> str:
    """The revision-pinned download URL of one model file."""
    return f"{hub_url}/{manifest.model_id}/resolve/{manifest.revision}/{quote(item.path)}"


def fetch_model(
    manifest: ModelManifest,
    dest: Path,
    *,
    opener: Opener = _open_url,
    hub_url: str = _HUB_URL,
) -> list[str]:
    """Download every manifest file into `dest` by revision, checking each sha256.

    A file already present with the right digest is kept. A download is written to a `.part`
    file, hashed while streaming, and renamed into place only when the digest matches; a mismatch
    deletes it and raises `ModelIntegrityError`. Returns the paths that were downloaded.
    """
    if not hub_url.startswith("https://"):
        raise ModelManifestError("model downloads need an https URL")
    downloaded: list[str] = []
    for item in manifest.files:
        target = dest / item.path
        if (
            target.is_file()
            and not target.is_symlink()
            and target.stat().st_size == item.size
            and _sha256_of(target) == item.sha256
        ):
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        digest = hashlib.sha256()
        size = 0
        request = Request(model_url(manifest, item, hub_url=hub_url))
        try:
            with opener(request, _DOWNLOAD_TIMEOUT_SECONDS) as response, part.open("wb") as handle:
                while chunk := response.read(_CHUNK):
                    size += len(chunk)
                    if size > item.size:
                        break
                    digest.update(chunk)
                    handle.write(chunk)
            if size != item.size or digest.hexdigest() != item.sha256:
                raise ModelIntegrityError(f"model file digest mismatch: {item.path}")
            part.replace(target)
        except BaseException:
            with suppress(OSError):
                part.unlink()
            raise
        downloaded.append(item.path)
    return downloaded


class _Session(Protocol):
    def get_inputs(self) -> Sequence[Any]: ...

    def run(self, output_names: Any, input_feed: dict[str, Any]) -> Sequence[Any]: ...


class LocalMiniLMProvider:
    """Sentence embeddings from the ONNX export of the pinned MiniLM model, on the CPU."""

    def __init__(
        self,
        *,
        manifest: ModelManifest,
        session: _Session,
        tokenizer: Any,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_input_chars: int = DEFAULT_MAX_INPUT_CHARS,
    ) -> None:
        if not 1 <= batch_size <= MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
        if max_input_chars < 1:
            raise ValueError("max_input_chars must be positive")
        self.model_id = manifest.model_id
        self.model_revision = manifest.revision
        self.dimensions = manifest.dimensions
        self._session = session
        self._tokenizer = tokenizer
        self._input_names = frozenset(str(item.name) for item in session.get_inputs())
        self._max_tokens = manifest.max_sequence_length
        self._batch_size = batch_size
        self._max_input_chars = max_input_chars

    @classmethod
    def load(
        cls,
        model_dir: Path,
        *,
        manifest: ModelManifest | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        threads: int = 1,
    ) -> LocalMiniLMProvider:
        """Verify every pinned digest, then load the tokenizer and the ONNX session (blocking)."""
        # Imported here so importing the platform never pays for the runtime.
        import onnxruntime
        from tokenizers import Tokenizer

        pinned = manifest or load_manifest()
        verify_model_dir(pinned, model_dir)
        tokenizer = Tokenizer.from_file(str(model_dir / "tokenizer.json"))
        pad_id = tokenizer.token_to_id("<pad>")
        if pad_id is None:
            raise EmbeddingError("tokenizer has no <pad> token")
        tokenizer.enable_truncation(max_length=pinned.max_sequence_length)
        tokenizer.enable_padding(pad_id=pad_id, pad_token="<pad>")
        options = onnxruntime.SessionOptions()
        options.intra_op_num_threads = threads
        options.inter_op_num_threads = 1
        session = onnxruntime.InferenceSession(
            str(model_dir / "onnx/model.onnx"),
            sess_options=options,
            providers=["CPUExecutionProvider"],
        )
        return cls(manifest=pinned, session=session, tokenizer=tokenizer, batch_size=batch_size)

    async def embed(self, texts: Sequence[str]) -> list[EmbeddedText]:
        results: list[EmbeddedText] = []
        for start in range(0, len(texts), self._batch_size):
            batch = list(texts[start : start + self._batch_size])
            results.extend(await asyncio.to_thread(self._embed_batch, batch))
        return results

    def _embed_batch(self, texts: list[str]) -> list[EmbeddedText]:
        clipped = [text[: self._max_input_chars] for text in texts]
        encodings = self._tokenizer.encode_batch(clipped)
        input_ids = np.array([item.ids for item in encodings], dtype=np.int64)
        mask = np.array([item.attention_mask for item in encodings], dtype=np.int64)
        feed: dict[str, Any] = {"input_ids": input_ids, "attention_mask": mask}
        if "token_type_ids" in self._input_names:
            feed["token_type_ids"] = np.zeros_like(input_ids)
        hidden = np.asarray(self._session.run(None, feed)[0], dtype=np.float32)
        if hidden.ndim != 3 or hidden.shape[2] != self.dimensions:
            raise EmbeddingError("model output has an unexpected shape")
        weights = mask[:, :, None].astype(np.float32)
        pooled = (hidden * weights).sum(axis=1) / np.maximum(weights.sum(axis=1), 1e-9)
        norms = np.maximum(np.linalg.norm(pooled, axis=1, keepdims=True), 1e-12)
        unit = pooled / norms
        return [
            EmbeddedText(
                vector=tuple(float(value) for value in unit[index]),
                # `Encoding.overflowing` is not reliably filled, so a text that reaches the window
                # counts as truncated (a text of exactly the window length is reported too).
                truncated=len(texts[index]) > self._max_input_chars
                or int(mask[index].sum()) >= self._max_tokens,
            )
            for index in range(len(texts))
        ]
