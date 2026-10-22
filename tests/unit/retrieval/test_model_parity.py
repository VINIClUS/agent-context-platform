"""Parity of the ONNX provider with sentence-transformers' reference output (opt-in only).

Never runs in CI: it needs the real model. The reference vectors in
`fixtures/minilm_reference.json` were produced once by sentence-transformers (torch, CPU) at the
pinned revision, so this test needs neither torch nor sentence-transformers. To run it:

    agent-context models fetch --dest /path/to/model
    AGENT_CONTEXT_TEST_MODEL_PARITY=1 AGENT_CONTEXT_TEST_MODEL_DIR=/path/to/model \\
        uv run pytest tests/unit/retrieval/test_model_parity.py --no-cov

The test never downloads anything. Regenerate the fixture only when `models.lock.json` changes.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import numpy as np
import pytest

from agent_context_platform.retrieval.embeddings import LocalMiniLMProvider, load_manifest

pytestmark = pytest.mark.unit

FLAG = "AGENT_CONTEXT_TEST_MODEL_PARITY"
DIRECTORY = "AGENT_CONTEXT_TEST_MODEL_DIR"
FIXTURE = Path(__file__).parent / "fixtures" / "minilm_reference.json"


def test_the_reference_fixture_is_for_the_pinned_revision() -> None:
    reference = json.loads(FIXTURE.read_text())
    manifest = load_manifest()

    assert (reference["model_id"], reference["revision"]) == (manifest.model_id, manifest.revision)
    assert len(reference["sentences"]) == len(reference["vectors"]) >= 5
    assert all(len(vector) == manifest.dimensions for vector in reference["vectors"])


@pytest.mark.skipif(os.environ.get(FLAG) != "1", reason=f"opt-in: set {FLAG}=1")
def test_onnx_embeddings_match_sentence_transformers() -> None:
    model_dir = os.environ.get(DIRECTORY)
    if not model_dir:
        pytest.fail(f"{DIRECTORY} must point at a directory made by `agent-context models fetch`")
    reference = json.loads(FIXTURE.read_text())
    expected = np.array(reference["vectors"])
    provider = LocalMiniLMProvider.load(Path(model_dir))

    actual = np.array([item.vector for item in asyncio.run(provider.embed(reference["sentences"]))])

    assert actual.shape == expected.shape
    cosines = (actual * expected).sum(axis=1) / (
        np.linalg.norm(actual, axis=1) * np.linalg.norm(expected, axis=1)
    )
    assert cosines.min() >= 0.999, cosines
