from __future__ import annotations

from pathlib import Path

import pytest

from agent_context_platform.indexing import registry
from agent_context_platform.indexing.tree_sitter import python as python_adapter
from agent_context_platform.indexing.tree_sitter import typescript as typescript_adapter
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit


def test_python_registration_carries_identity_inputs() -> None:
    support = registry.support_for("python")

    assert support is not None
    assert support.extensions == (".py", ".pyi")
    assert support.extractor_name == python_adapter.ADAPTER_NAME
    assert support.extractor_version == python_adapter.ADAPTER_VERSION
    assert support.parser_fingerprint == python_adapter.FINGERPRINT
    assert support.scip is not None and support.scip.tool_name == "scip-python"
    assert registry.language_for_path("pkg/a.py") == "python"
    assert registry.language_for_path("pkg/a.PY") is None
    assert registry.language_for_path("pkg/a.rs") is None
    assert registry.support_for("cobol") is None


def test_typescript_and_javascript_share_one_adapter_with_their_own_labels() -> None:
    support = registry.support_for("typescript")

    assert support is not None
    assert support.extensions == (".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs")
    assert support.max_request_source_bytes == 512 * 1024
    assert support.parser_fingerprint == typescript_adapter.FINGERPRINT
    for path, label in (
        ("a.ts", "typescript"),
        ("a.tsx", "typescript"),
        ("a.js", "javascript"),
        ("a.jsx", "javascript"),
        ("a.mjs", "javascript"),
        ("a.cjs", "javascript"),
    ):
        assert registry.language_for_path(path) == "typescript"
        assert registry.label_for_path(path) == label


def test_adapters_are_built_from_settings_limits(tmp_path: Path) -> None:
    adapters = registry.build_adapters(Settings(indexer_checkout_roots=(str(tmp_path),)))

    assert set(adapters) == {"python", "typescript", "go"}
    assert adapters["go"].language == "go"
    assert registry.label_for_path("cmd/main.go") == "go"
    assert registry.LANGUAGES["go"].max_request_source_bytes == 512 * 1024
    assert adapters["python"].language == "python"
    assert adapters["typescript"].language == "typescript"
    assert registry.build_adapters(Settings(indexer_checkout_roots=(str(tmp_path),)), ()) == {}


def test_duplicate_registrations_are_refused() -> None:
    entry = registry.LANGUAGES["python"]

    with pytest.raises(ValueError, match="duplicate language"):
        registry._index_entries((entry, entry))
    other = registry.LanguageSupport(
        "other", (".py",), "x", "1", entry.parser_fingerprint, entry.factory
    )
    with pytest.raises(ValueError, match="duplicate extension"):
        registry._index_entries((entry, other))


def test_one_adapter_can_serve_several_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    entry = registry.LanguageSupport(
        "typescript",
        (".ts", ".js"),
        "ts",
        "1",
        "0" * 64,
        registry.LANGUAGES["python"].factory,
        labels={".js": "javascript"},
    )
    monkeypatch.setattr(registry, "LANGUAGES", {"typescript": entry})
    monkeypatch.setattr(registry, "_EXTENSIONS", {".ts": "typescript", ".js": "typescript"})

    assert registry.language_for_path("a/b.js") == "typescript"  # picks the adapter
    assert registry.label_for_path("a/b.js") == "javascript"
    assert registry.label_for_path("a/b.ts") == "typescript"
    assert registry.label_for_path("a/b.rs") is None
