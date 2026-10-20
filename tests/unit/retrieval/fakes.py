"""Minimal doubles for the search code's PostgreSQL and Neo4j boundaries (no services)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from types import SimpleNamespace
from typing import Any

Script = Callable[[str, dict[str, Any]], Sequence[Any]]


class FakeResult:
    def __init__(self, rows: Sequence[Any]) -> None:
        self.rows = list(rows)

    def __iter__(self) -> Any:
        return iter(self.rows)

    def first(self) -> Any:
        return self.rows[0] if self.rows else None

    def scalars(self) -> Any:
        return iter(self.rows)


class FakeSession:
    """An async session whose statements are answered by `script(sql, params)`."""

    def __init__(self, script: Script, log: list[tuple[str, dict[str, Any]]]) -> None:
        self._script = script
        self._log = log

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def begin(self) -> FakeSession:
        return self

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        sql, values = str(statement), dict(params or {})
        self._log.append((sql, values))
        return FakeResult(self._script(sql, values))

    async def get(self, model: Any, key: Any) -> Any:
        self._log.append((f"get {model.__name__}", {"key": key}))
        return self._script("get", {"key": key})[0] if self._script("get", {"key": key}) else None


class FakeSessions:
    def __init__(self, script: Script | None = None) -> None:
        self.log: list[tuple[str, dict[str, Any]]] = []
        self.script: Script = script or (lambda _sql, _params: [])

    def __call__(self) -> FakeSession:
        return FakeSession(lambda sql, params: self.script(sql, params), self.log)

    def statements(self, fragment: str) -> list[dict[str, Any]]:
        return [params for sql, params in self.log if fragment in sql]


def row(**values: Any) -> SimpleNamespace:
    return SimpleNamespace(**values)


class FakeTx:
    """A Neo4j transaction that records queries and answers from `script(query, parameters)`."""

    def __init__(self, script: Callable[[str, dict[str, Any]], list[dict[str, Any]]] | None = None):
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self._script = script or (lambda _query, _parameters: [])

    async def run(self, query: str, *, parameters: dict[str, Any]) -> Any:
        self.queries.append((query, parameters))
        return SimpleNamespace(records=self._script(query, parameters))

    def with_fragment(self, fragment: str) -> list[dict[str, Any]]:
        return [parameters for query, parameters in self.queries if fragment in query]


class FakeGraph:
    """The read facade: runs the callback against one shared `FakeTx`."""

    def __init__(self, tx: FakeTx) -> None:
        self.tx = tx

    async def execute_read(self, callback: Callable[[Any], Any]) -> Any:
        return await callback(self.tx)


class StubEmbedder:
    """A provider that returns the same unit vector for every text."""

    model_id = "stub/embedder"
    model_revision = "f" * 40
    dimensions = 384

    async def embed(self, texts: Sequence[str]) -> list[Any]:
        from agent_context_platform.retrieval.embeddings import EmbeddedText

        return [EmbeddedText((1.0, *([0.0] * 383)), truncated=False) for _ in texts]
