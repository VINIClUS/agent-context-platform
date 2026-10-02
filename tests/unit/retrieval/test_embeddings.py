"""The pinned model manifest, its verification and download, and the ONNX provider's maths.

No model is ever downloaded or loaded here: the provider runs against a fake session and a tiny
real tokenizer, and the download against an in-memory opener.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.request import Request

import numpy as np
import pytest
from tokenizers import Tokenizer, models, pre_tokenizers

from agent_context_platform.retrieval import embeddings
from agent_context_platform.retrieval.embeddings import (
    EmbeddingError,
    EmbeddingProvider,
    LocalMiniLMProvider,
    ModelFile,
    ModelIntegrityError,
    ModelManifest,
    ModelManifestError,
    fetch_model,
    load_manifest,
    model_url,
    parse_manifest,
    verify_model_dir,
)

pytestmark = pytest.mark.unit

REVISION = "a" * 40


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _manifest(files: dict[str, bytes], *, dimensions: int = 4, window: int = 6) -> ModelManifest:
    return ModelManifest(
        model_id="org/model",
        revision=REVISION,
        dimensions=dimensions,
        max_sequence_length=window,
        files=tuple(ModelFile(path, len(data), _sha(data)) for path, data in files.items()),
    )


def _document(**changes: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema": 1,
        "model_id": "org/model",
        "revision": REVISION,
        "dimensions": 4,
        "max_sequence_length": 8,
        "files": [{"path": "a/b.bin", "size": 3, "sha256": _sha(b"abc")}],
    }
    document.update(changes)
    return document


def test_the_shipped_manifest_pins_the_model_by_revision_and_digest() -> None:
    manifest = load_manifest()

    assert manifest.model_id == "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    assert len(manifest.revision) == 40
    assert manifest.dimensions == 384
    assert manifest.max_sequence_length == 128
    assert manifest.file("onnx/model.onnx").sha256
    assert {item.path for item in manifest.files} >= {"onnx/model.onnx", "tokenizer.json"}
    with pytest.raises(ModelManifestError):
        manifest.file("missing")


@pytest.mark.parametrize(
    "document",
    [
        [],
        _document(schema=2),
        _document(model_id=" "),
        _document(revision="main"),
        _document(dimensions=0),
        _document(max_sequence_length=1),
        _document(files=[]),
        _document(files=["x"]),
        _document(files=[{"path": "../x", "size": 1, "sha256": _sha(b"a")}]),
        _document(files=[{"path": "/x", "size": 1, "sha256": _sha(b"a")}]),
        _document(files=[{"path": "a\\b", "size": 1, "sha256": _sha(b"a")}]),
        _document(files=[{"path": "x", "size": 1, "sha256": "short"}]),
        _document(files=[{"path": "x", "size": "1", "sha256": _sha(b"a")}]),
        _document(
            files=[
                {"path": "x", "size": 1, "sha256": _sha(b"a")},
                {"path": "x", "size": 1, "sha256": _sha(b"a")},
            ]
        ),
    ],
)
def test_a_malformed_manifest_is_rejected(document: object) -> None:
    with pytest.raises(ModelManifestError):
        parse_manifest(document)


def test_verify_model_dir_accepts_exact_files_and_fails_closed_otherwise(tmp_path: Path) -> None:
    files = {"onnx/model.onnx": b"weights", "tokenizer.json": b"{}"}
    manifest = _manifest(files)
    for name, data in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_bytes(data)

    verify_model_dir(manifest, tmp_path)

    (tmp_path / "tokenizer.json").write_bytes(b"{ }")  # same size, different digest
    with pytest.raises(ModelIntegrityError, match=r"digest mismatch: tokenizer.json"):
        verify_model_dir(manifest, tmp_path)
    (tmp_path / "tokenizer.json").write_bytes(b"{")  # truncated
    with pytest.raises(ModelIntegrityError, match="digest mismatch"):
        verify_model_dir(manifest, tmp_path)
    (tmp_path / "tokenizer.json").unlink()
    with pytest.raises(ModelIntegrityError, match=r"missing: tokenizer.json"):
        verify_model_dir(manifest, tmp_path)
    (tmp_path / "tokenizer.json").symlink_to(tmp_path / "onnx/model.onnx")
    with pytest.raises(ModelIntegrityError, match="missing"):
        verify_model_dir(manifest, tmp_path)


class _Server:
    """An in-memory stand-in for the model hub, keyed by URL."""

    def __init__(self, responses: dict[str, bytes]) -> None:
        self.responses = responses
        self.requests: list[str] = []

    def __call__(self, request: Request, timeout: float) -> io.BytesIO:
        assert timeout > 0
        self.requests.append(request.full_url)
        return io.BytesIO(self.responses[request.full_url])


def test_fetch_downloads_by_revision_verifies_digests_and_skips_good_files(
    tmp_path: Path,
) -> None:
    files = {"onnx/model.onnx": b"weights", "tokenizer.json": b"{}"}
    manifest = _manifest(files)
    server = _Server({model_url(manifest, item): files[item.path] for item in manifest.files})

    first = fetch_model(manifest, tmp_path, opener=server)

    assert first == ["onnx/model.onnx", "tokenizer.json"]
    assert all(REVISION in url for url in server.requests)
    verify_model_dir(manifest, tmp_path)
    assert not list(tmp_path.rglob("*.part"))

    server.requests.clear()
    assert fetch_model(manifest, tmp_path, opener=server) == []  # already verified: no request
    assert server.requests == []

    (tmp_path / "tokenizer.json").write_bytes(b"xx")  # corrupt: fetched again
    assert fetch_model(manifest, tmp_path, opener=server) == ["tokenizer.json"]


@pytest.mark.parametrize("served", [b"tampered-bytes", b"short", b"much longer than pinned!!"])
def test_fetch_refuses_a_wrong_download_and_leaves_nothing_behind(
    tmp_path: Path, served: bytes
) -> None:
    manifest = _manifest({"model.bin": b"weights"})
    server = _Server({model_url(manifest, manifest.files[0]): served})

    with pytest.raises(ModelIntegrityError, match=r"digest mismatch: model.bin"):
        fetch_model(manifest, tmp_path, opener=server)

    assert list(tmp_path.rglob("*")) == []


def test_fetch_refuses_a_non_https_hub(tmp_path: Path) -> None:
    with pytest.raises(ModelManifestError, match="https"):
        fetch_model(_manifest({"a": b"x"}), tmp_path, hub_url="http://hub.example.test")


def test_the_default_opener_is_the_standard_library_one(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, float]] = []

    def fake_urlopen(request: Request, timeout: float) -> io.BytesIO:
        seen.append((request.full_url, timeout))
        return io.BytesIO(b"x")

    monkeypatch.setattr(embeddings, "urlopen", fake_urlopen)

    with embeddings._open_url(Request("https://hub.example.test/x"), 3.0) as response:
        assert response.read() == b"x"
    assert seen == [("https://hub.example.test/x", 3.0)]


def _tokenizer(*, max_length: int = 6) -> Tokenizer:
    vocab = {"<pad>": 0, "<unk>": 1, "hello": 2, "world": 3, "again": 4}
    tokenizer = Tokenizer(models.WordLevel(vocab, unk_token="<unk>"))
    tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
    tokenizer.enable_truncation(max_length=max_length)
    tokenizer.enable_padding(pad_id=0, pad_token="<pad>")
    return tokenizer


class _Session:
    """Hidden state for token id `i` at position `p` is `[i, 1, 0, p]`, so pooling is checkable."""

    def __init__(self, *, input_names: tuple[str, ...], dims: int = 4) -> None:
        self._names = input_names
        self._dims = dims
        self.feeds: list[dict[str, np.ndarray[Any, Any]]] = []

    def get_inputs(self) -> list[SimpleNamespace]:
        return [SimpleNamespace(name=name) for name in self._names]

    def run(self, output_names: Any, input_feed: dict[str, Any]) -> list[np.ndarray[Any, Any]]:
        self.feeds.append(input_feed)
        ids = input_feed["input_ids"].astype(np.float32)
        hidden = np.zeros((*ids.shape, self._dims), dtype=np.float32)
        hidden[:, :, 0] = ids
        hidden[:, :, 1] = 1.0
        hidden[:, :, 3] = np.arange(ids.shape[1])
        return [hidden]


def _provider(
    session: Any, *, batch_size: int = 2, max_input_chars: int = 40, max_length: int = 6
) -> LocalMiniLMProvider:
    return LocalMiniLMProvider(
        manifest=_manifest({"x": b"x"}, window=max_length),
        session=session,
        tokenizer=_tokenizer(max_length=max_length),
        batch_size=batch_size,
        max_input_chars=max_input_chars,
    )


def test_the_provider_satisfies_the_embedding_interface() -> None:
    provider = _provider(_Session(input_names=("input_ids", "attention_mask")))

    assert isinstance(provider, EmbeddingProvider)
    assert (provider.model_id, provider.model_revision, provider.dimensions) == (
        "org/model",
        REVISION,
        4,
    )


def test_embedding_is_mask_weighted_mean_pooling_then_l2_normalization() -> None:
    session = _Session(input_names=("input_ids", "attention_mask", "token_type_ids"))
    provider = _provider(session)

    short, long = asyncio.run(provider.embed(["hello", "hello world again"]))

    # "hello" is [CLS-less] one token (id 2, position 0); padding must not enter the mean.
    expected_short = np.array([2.0, 1.0, 0.0, 0.0])
    # three tokens: ids 2,3,4 at positions 0,1,2 -> mean [3, 1, 0, 1]
    expected_long = np.array([3.0, 1.0, 0.0, 1.0])
    for result, expected in ((short, expected_short), (long, expected_long)):
        vector = np.array(result.vector)
        assert vector == pytest.approx(expected / np.linalg.norm(expected), abs=1e-6)
        assert np.linalg.norm(vector) == pytest.approx(1.0, abs=1e-6)
        assert not result.truncated
    assert session.feeds[0]["token_type_ids"].shape == session.feeds[0]["input_ids"].shape
    assert not np.any(session.feeds[0]["token_type_ids"])


def test_inputs_the_model_does_not_declare_are_not_fed() -> None:
    session = _Session(input_names=("input_ids", "attention_mask"))

    asyncio.run(_provider(session).embed(["hello"]))

    assert set(session.feeds[0]) == {"input_ids", "attention_mask"}


def test_batches_are_bounded_and_order_is_preserved() -> None:
    session = _Session(input_names=("input_ids", "attention_mask"))
    texts = ["hello", "world", "again", "hello world", "world again"]

    results = asyncio.run(_provider(session, batch_size=2).embed(texts))

    assert [feed["input_ids"].shape[0] for feed in session.feeds] == [2, 2, 1]
    assert len(results) == 5
    assert results[0].vector != results[1].vector
    assert asyncio.run(_provider(session).embed([])) == []


def test_over_long_input_is_truncated_and_reported() -> None:
    session = _Session(input_names=("input_ids", "attention_mask"))
    provider = _provider(session, max_length=3, max_input_chars=24)

    within, too_many_tokens, too_many_chars = asyncio.run(
        provider.embed(["hello world", "hello world again", "hello " * 20])
    )

    assert not within.truncated
    assert too_many_tokens.truncated  # reaches the 3-token window
    assert too_many_chars.truncated  # cut to 24 characters first
    assert session.feeds[0]["input_ids"].shape[1] == 3  # never longer than the window


def test_unexpected_model_output_is_an_error() -> None:
    provider = _provider(_Session(input_names=("input_ids", "attention_mask"), dims=5))

    with pytest.raises(EmbeddingError, match="unexpected shape"):
        asyncio.run(provider.embed(["hello"]))


@pytest.mark.parametrize(("batch_size", "chars"), [(0, 10), (257, 10), (2, 0)])
def test_the_provider_bounds_are_validated(batch_size: int, chars: int) -> None:
    with pytest.raises(ValueError, match="must be"):
        _provider(
            _Session(input_names=("input_ids",)), batch_size=batch_size, max_input_chars=chars
        )


def _model_dir(tmp_path: Path, tokenizer_json: str) -> tuple[Path, ModelManifest]:
    files = {"onnx/model.onnx": b"not-a-real-onnx-file", "tokenizer.json": tokenizer_json.encode()}
    for name, data in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_bytes(data)
    return tmp_path, _manifest(files)


def test_load_verifies_digests_before_touching_the_runtime(tmp_path: Path) -> None:
    model_dir, manifest = _model_dir(tmp_path, _tokenizer().to_str())
    (model_dir / "onnx/model.onnx").write_bytes(b"swapped-for-another-model")

    with pytest.raises(ModelIntegrityError, match=r"digest mismatch: onnx/model.onnx"):
        LocalMiniLMProvider.load(model_dir, manifest=manifest)


def test_load_builds_a_cpu_session_and_a_bounded_tokenizer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import onnxruntime

    model_dir, manifest = _model_dir(tmp_path, _tokenizer().to_str())
    created: dict[str, Any] = {}

    def fake_session(path: str, *, sess_options: Any, providers: list[str]) -> _Session:
        created.update(path=path, threads=sess_options.intra_op_num_threads, providers=providers)
        return _Session(input_names=("input_ids", "attention_mask"))

    monkeypatch.setattr(onnxruntime, "InferenceSession", fake_session)

    provider = LocalMiniLMProvider.load(model_dir, manifest=manifest, batch_size=3, threads=2)

    assert created == {
        "path": str(model_dir / "onnx/model.onnx"),
        "threads": 2,
        "providers": ["CPUExecutionProvider"],
    }
    results = asyncio.run(provider.embed(["hello world again hello world again hello"]))
    assert results[0].truncated  # the manifest's 6-token window was applied to the tokenizer


def test_load_requires_a_pad_token(tmp_path: Path) -> None:
    vocab = {"<unk>": 0, "hello": 1}
    tokenizer = Tokenizer(models.WordLevel(vocab, unk_token="<unk>"))
    model_dir, manifest = _model_dir(tmp_path, tokenizer.to_str())

    with pytest.raises(EmbeddingError, match="pad"):
        LocalMiniLMProvider.load(model_dir, manifest=manifest)


def test_the_manifest_file_is_valid_json_with_sorted_unique_paths() -> None:
    text = (Path(embeddings.__file__).parent / "models.lock.json").read_text()
    paths = [item["path"] for item in json.loads(text)["files"]]

    assert paths == sorted(set(paths))
