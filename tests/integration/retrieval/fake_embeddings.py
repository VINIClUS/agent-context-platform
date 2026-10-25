"""A deterministic embedding provider for tests: no model, no network, no randomness.

Texts listed in `fixed` get exactly the given direction (so a test can choose cosine orders);
every other text gets a hash-seeded unit vector, stable across runs and processes.
"""

from __future__ import annotations

import hashlib
import math
import random
from collections.abc import Mapping, Sequence

from agent_context_platform.retrieval.embeddings import EmbeddedText

DIMENSIONS = 384


def unit(*components: float) -> tuple[float, ...]:
    """A unit vector whose first components are `components` (the rest zero)."""
    padded = [*components, *([0.0] * (DIMENSIONS - len(components)))]
    norm = math.sqrt(sum(value * value for value in padded))
    return tuple(value / norm for value in padded)


def hashed(text: str) -> tuple[float, ...]:
    rng = random.Random(hashlib.sha256(text.encode()).digest())
    values = [rng.gauss(0.0, 1.0) for _ in range(DIMENSIONS)]
    norm = math.sqrt(sum(value * value for value in values))
    return tuple(value / norm for value in values)


class FakeEmbeddingProvider:
    model_id = "fake/hash-embedding"
    model_revision = "f" * 40
    dimensions = DIMENSIONS

    def __init__(
        self,
        fixed: Mapping[str, Sequence[float]] | None = None,
        *,
        model_revision: str | None = None,
    ) -> None:
        self._fixed = {text: tuple(vector) for text, vector in (fixed or {}).items()}
        self.calls: list[list[str]] = []
        if model_revision is not None:
            self.model_revision = model_revision

    async def embed(self, texts: Sequence[str]) -> list[EmbeddedText]:
        self.calls.append(list(texts))
        return [
            EmbeddedText(self._fixed.get(text) or hashed(text), truncated=False) for text in texts
        ]
