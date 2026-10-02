"""`agent-context models fetch` and the search projector's CLI wiring, with no network or model."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.projection.registry import registered_projectors
from agent_context_platform.retrieval.embeddings import ModelIntegrityError, load_manifest
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit

_TARGET = ["--target-uri", "bolt://standby.example.test:7687", "--target-database", "neo4j"]


def _invoke(*args: str) -> Any:
    return CliRunner().invoke(cli.app, list(args))


def test_models_fetch_downloads_by_revision_into_dest_and_verifies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: dict[str, Any] = {}

    def fake_fetch(manifest: Any, dest: Path) -> list[str]:
        seen["fetch"] = (manifest.revision, dest)
        return ["tokenizer.json"]

    def fake_verify(manifest: Any, dest: Path) -> None:
        seen["verify"] = dest

    monkeypatch.setattr(cli, "fetch_model", fake_fetch)
    monkeypatch.setattr(cli, "verify_model_dir", fake_verify)

    human = _invoke("models", "fetch", "--dest", str(tmp_path))
    machine = _invoke("models", "fetch", "--dest", str(tmp_path), "--json")

    manifest = load_manifest()
    assert human.exit_code == 0
    assert f"{manifest.model_id}@{manifest.revision}: 1 file(s) downloaded" in human.output
    assert seen == {"fetch": (manifest.revision, tmp_path), "verify": tmp_path}
    payload = json.loads(machine.output)
    assert payload["downloaded"] == ["tokenizer.json"]
    assert payload["verified"] == [item.path for item in manifest.files]
    assert payload["revision"] == manifest.revision


def test_models_fetch_reports_a_digest_mismatch_by_class_name(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def refuse(manifest: Any, dest: Path) -> list[str]:
        raise ModelIntegrityError("model file digest mismatch: onnx/model.onnx")

    monkeypatch.setattr(cli, "fetch_model", refuse)

    result = _invoke("models", "fetch", "--dest", str(tmp_path))

    assert result.exit_code == 1
    assert "ModelIntegrityError" in result.stderr


def test_models_fetch_requires_a_destination() -> None:
    result = _invoke("models", "fetch")

    assert result.exit_code == 2


def test_without_a_model_directory_search_can_still_purge_but_not_index() -> None:
    sessions = object()
    runtime = SimpleNamespace(sessions=sessions)

    projectors = asyncio.run(
        cli._search_projectors(Settings(), runtime, write_documents=True)  # type: ignore[arg-type]
    )

    assert [item.name for item in projectors] == [item.name for item in registered_projectors()]
    search = projectors[-1]
    assert search._backend is None  # type: ignore[attr-defined]
    assert search._purge_access.sessions is sessions  # type: ignore[attr-defined]


def test_with_a_model_directory_the_search_projector_gets_a_verified_provider_and_content(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    provider = object()
    loaded: dict[str, Any] = {}

    def fake_load(model_dir: Path, **options: Any) -> object:
        loaded.update(model_dir=model_dir, **options)
        return provider

    monkeypatch.setattr(cli.LocalMiniLMProvider, "load", staticmethod(fake_load))
    monkeypatch.setattr(cli.S3BlobStore, "from_settings", staticmethod(lambda _settings: object()))
    settings = Settings(search={"model_dir": tmp_path, "embedding_batch_size": 4})
    sessions = object()
    runtime = cli.Runtime(sessions=sessions, recording_sessions=None, store=object())  # type: ignore[arg-type]

    projectors = asyncio.run(cli._search_projectors(settings, runtime, write_documents=False))

    assert loaded == {"model_dir": tmp_path, "batch_size": 4, "threads": 1}
    search = projectors[-1]
    assert search.name == "search"
    assert search._backend.embeddings is provider  # type: ignore[attr-defined]
    assert search._backend.sessions is sessions  # type: ignore[attr-defined]
    assert search._backend.write_documents is False  # type: ignore[attr-defined]
    assert [item.name for item in projectors[:-1]] == [
        item.name for item in registered_projectors()[:-1]
    ]


def test_search_grants_are_demanded_only_when_search_is_bound_and_writes_only_in_a_writing_mode(
    tmp_path: Path,
) -> None:
    unbound = Settings()
    bound = Settings(search={"model_dir": tmp_path})

    assert cli._search_grants(unbound, write_documents=True) == ()
    reading = {(g.obj, g.privilege) for g in cli._search_grants(bound, write_documents=False)}
    writing = {
        (g.obj, g.privilege, g.column) for g in cli._search_grants(bound, write_documents=True)
    }

    assert reading == {
        ("catalog.inline_contents", "SELECT"),
        ("retrieval.content_tombstones", "SELECT"),
    }
    assert {
        ("retrieval.search_documents", "INSERT", None),
        ("retrieval.search_documents", "DELETE", None),
        ("retrieval.content_tombstones", "INSERT", None),
        ("retrieval.search_documents", "UPDATE", "tsv"),
    } <= writing
    assert {item[:2] for item in writing} >= reading


@pytest.mark.parametrize(
    ("args", "writes"),
    [
        (["projection", "rebuild", "--in-place", "--confirm", "neo4j"], True),
        (["projection", "rebuild", *_TARGET], False),
    ],
)
def test_a_missing_search_grant_fails_the_rebuild_before_the_projectors_or_graph_are_touched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, args: list[str], writes: bool
) -> None:
    from contextlib import asynccontextmanager

    from agent_context_platform.projection.verify import MissingGrantError

    seen: dict[str, Any] = {"order": []}

    @asynccontextmanager
    async def runtime(_settings: Any, *, record: bool = False) -> Any:
        yield cli.Runtime(sessions=object(), recording_sessions=None, store=object())  # type: ignore[arg-type]

    async def refuse(*_args: Any, extra_grants: Any = (), **_kwargs: Any) -> None:
        seen["order"].append("preflight")
        seen["grants"] = {(g.obj, g.privilege) for g in extra_grants}
        raise MissingGrantError("projector", ["INSERT on retrieval.search_documents"])

    async def never(*_args: Any, **_kwargs: Any) -> Any:
        seen["order"].append("touched")

    monkeypatch.setenv(
        "AGENT_CONTEXT_POSTGRESQL__DSN", "postgresql+psycopg://u:p@db.example.test/a"
    )
    monkeypatch.setenv("AGENT_CONTEXT_NEO4J__URI", "bolt://live.example.test:7687")
    monkeypatch.setenv("AGENT_CONTEXT_NEO4J__USERNAME", "neo4j")
    monkeypatch.setenv("AGENT_CONTEXT_NEO4J__PASSWORD", "pw")
    monkeypatch.setenv(cli.TARGET_PASSWORD_ENV, "pw")
    monkeypatch.setenv("AGENT_CONTEXT_SEARCH__MODEL_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "_runtime", runtime)
    monkeypatch.setattr(cli, "preflight", refuse)
    monkeypatch.setattr(cli, "_search_projectors", never)
    monkeypatch.setattr(cli, "rebuild_projections", never)

    result = _invoke(*args, "--target-username", "op") if not writes else _invoke(*args)

    assert result.exit_code == 1 and "lacks" in result.stderr
    assert seen["order"] == ["preflight"]
    assert ("retrieval.search_documents", "INSERT") in seen["grants"] or not writes
    assert ("catalog.inline_contents", "SELECT") in seen["grants"]


def _env(monkeypatch: pytest.MonkeyPatch, *, model: bool, embeddings: int = 0) -> dict[str, Any]:
    from contextlib import asynccontextmanager

    seen: dict[str, Any] = {"calls": []}

    class Store:
        async def execute_read(self, callback: Any) -> int:
            class Tx:
                async def run(self, _query: str, *, parameters: dict[str, Any]) -> Any:
                    return SimpleNamespace(records=[{"n": embeddings}])

            return int(await callback(Tx()))

    @asynccontextmanager
    async def runtime(_settings: Any, *, record: bool = False) -> Any:
        seen["calls"].append("runtime")
        yield cli.Runtime(sessions=object(), recording_sessions=None, store=Store())  # type: ignore[arg-type]

    async def preflight(*_args: Any, **_kwargs: Any) -> None:
        seen["calls"].append("preflight")

    async def rebuild(*_args: Any, **kwargs: Any) -> Any:
        seen["calls"].append("rebuild")
        seen["rebuild"] = kwargs
        raise RuntimeError

    for name, value in {
        "AGENT_CONTEXT_POSTGRESQL__DSN": "postgresql+psycopg://u:p@db.example.test/a",
        "AGENT_CONTEXT_NEO4J__URI": "bolt://live.example.test:7687",
        "AGENT_CONTEXT_NEO4J__USERNAME": "neo4j",
        "AGENT_CONTEXT_NEO4J__PASSWORD": "pw",
        cli.TARGET_PASSWORD_ENV: "pw",
    }.items():
        monkeypatch.setenv(name, value)
    if model:
        monkeypatch.setenv("AGENT_CONTEXT_SEARCH__MODEL_DIR", "/models/minilm")
    monkeypatch.setattr(cli, "_runtime", runtime)
    monkeypatch.setattr(cli, "preflight", preflight)
    monkeypatch.setattr(cli, "rebuild_projections", rebuild)
    return seen


@pytest.mark.parametrize(
    "args",
    [
        ["projection", "rebuild", "--in-place", "--confirm", "neo4j"],
        ["projection", "rebuild", *_TARGET, "--target-username", "op"],
        ["projection", "verify", "--replay-check", *_TARGET, "--target-username", "op"],
    ],
)
def test_a_replay_without_a_model_is_refused_before_anything_is_touched(
    monkeypatch: pytest.MonkeyPatch, args: list[str]
) -> None:
    seen = _env(monkeypatch, model=False)

    result = _invoke(*args)

    assert result.exit_code == 2
    assert (
        "AGENT_CONTEXT_SEARCH__MODEL_DIR" in result.stderr and "--without-search" in result.stderr
    )
    assert seen["calls"] == []  # no connection, no preflight, no reset, no wipe


def test_a_model_that_fails_verification_stops_the_rebuild_before_it_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _env(monkeypatch, model=True)

    def corrupt(*_args: Any, **_kwargs: Any) -> Any:
        raise ModelIntegrityError("model file digest mismatch: onnx/model.onnx")

    monkeypatch.setattr(cli.LocalMiniLMProvider, "load", staticmethod(corrupt))
    monkeypatch.setattr(cli.S3BlobStore, "from_settings", staticmethod(lambda _s: object()))

    result = _invoke("projection", "rebuild", "--in-place", "--confirm", "neo4j")

    assert result.exit_code == 1 and "ModelIntegrityError" in result.stderr
    assert seen["calls"] == ["runtime", "preflight"]  # never reached the rebuild


def test_without_search_skips_the_projector_and_the_search_table_reset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _env(monkeypatch, model=False)

    _invoke("projection", "rebuild", "--in-place", "--confirm", "neo4j", "--without-search")

    assert seen["calls"] == ["runtime", "preflight", "rebuild"]
    assert seen["rebuild"]["before_replay"] is None
    projectors = asyncio.run(
        cli._search_projectors(Settings(), object(), write_documents=True, without_search=True)  # type: ignore[arg-type]
    )
    assert "search" not in [item.name for item in projectors] and len(projectors) == 4


def test_an_in_place_rebuild_with_search_resets_the_search_tables_a_standby_one_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _env(monkeypatch, model=True)

    async def projectors(*_args: Any, **_kwargs: Any) -> tuple[Any, ...]:
        return ()

    monkeypatch.setattr(cli, "_search_projectors", projectors)

    _invoke("projection", "rebuild", "--in-place", "--confirm", "neo4j")
    in_place = seen["rebuild"]["before_replay"]
    _invoke("projection", "rebuild", *_TARGET, "--target-username", "op")

    assert callable(in_place)
    assert seen["rebuild"]["before_replay"] is None


@pytest.mark.parametrize(
    "args",
    [
        ["projection", "rebuild", "--in-place", "--confirm", "neo4j", "--without-search"],
        [
            "projection",
            "verify",
            "--replay-check",
            *_TARGET,
            "--target-username",
            "op",
            "--without-search",
        ],
    ],
)
def test_without_search_is_refused_while_the_live_graph_holds_embeddings(
    monkeypatch: pytest.MonkeyPatch, args: list[str]
) -> None:
    seen = _env(monkeypatch, model=False, embeddings=3)

    result = _invoke(*args)

    assert result.exit_code == 2 and "ContentEmbedding" in result.stderr
    assert seen["calls"] == ["runtime"]  # before the preflight, the reset and the wipe


def test_a_standby_rebuild_without_search_is_allowed_even_with_embeddings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _env(monkeypatch, model=False, embeddings=3)

    _invoke("projection", "rebuild", *_TARGET, "--target-username", "op", "--without-search")

    assert seen["calls"] == ["runtime", "preflight", "rebuild"]
