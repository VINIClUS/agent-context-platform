"""The sole registry of code-index languages, structural adapters and SCIP indexers.

Everything the indexer needs to know about a language lives here, and nowhere else:

- which file extensions belong to it;
- which structural adapter parses it, that adapter's extractor name and version, and the
  parser fingerprint that feeds ``identity.file_revision_id``;
- which SCIP indexer produces semantic evidence for it (a record only: SCIP indexes are
  produced outside this process and imported by ``scip.import_scip``).

``build_adapters`` is the single place a ``SandboxedAdapter`` is constructed for indexing, and
it always goes through ``Limits.from_settings`` so the configured ``checkout_roots`` and the
Landlock read set are enforced (FU-39). Nothing else in the platform builds an adapter.

To register a language (Python, TypeScript/JavaScript and Go are registered) add ONE entry to
``_ENTRIES`` below. Its ``factory`` is that adapter module's ``<language>_adapter(limits=...)``.
``language`` is the ADAPTER's language (what ``ParsedModule.language`` carries); ``labels`` maps
an extension to the label events use when it differs, e.g. ``{".js": "javascript"}``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Final

from agent_context_platform.indexing.tree_sitter import go as go_adapter
from agent_context_platform.indexing.tree_sitter import python as python_adapter
from agent_context_platform.indexing.tree_sitter import typescript as typescript_adapter
from agent_context_platform.indexing.tree_sitter.base import StructuralAdapter
from agent_context_platform.indexing.tree_sitter.runner import Limits
from agent_context_platform.settings import Settings


@dataclass(frozen=True, slots=True)
class ScipIndexer:
    """The SCIP indexer recorded for a language (its output arrives as bytes)."""

    tool_name: str
    tool_version: str


# Default source bytes per adapter request: about 50% of the 5 s request backstop at the measured
# walk throughput (Python ~0.38 MB/s of child CPU). Slower adapters declare less (TS/JS 512 KiB).
DEFAULT_REQUEST_SOURCE_BYTES: Final = 1 << 20


@dataclass(frozen=True, slots=True)
class LanguageSupport:
    """One language: extensions, structural adapter identity and optional SCIP indexer."""

    language: str
    extensions: tuple[str, ...]
    extractor_name: str
    extractor_version: str
    parser_fingerprint: str
    factory: Callable[[Limits], StructuralAdapter]
    scip: ScipIndexer | None = None
    # One adapter can serve several labels: extension -> the language label written to events
    # (``ParsedModule.language`` is the adapter's, e.g. ``typescript`` for .js files too).
    labels: Mapping[str, str] = field(default_factory=dict)
    # At most this many bytes of source per adapter request (a larger single file goes alone);
    # the file count is capped separately at 64.
    max_request_source_bytes: int = DEFAULT_REQUEST_SOURCE_BYTES


_ENTRIES: Final[tuple[LanguageSupport, ...]] = (
    LanguageSupport(
        language=python_adapter.LANGUAGE,
        extensions=(".py", ".pyi"),
        extractor_name=python_adapter.ADAPTER_NAME,
        extractor_version=python_adapter.ADAPTER_VERSION,
        parser_fingerprint=python_adapter.FINGERPRINT,
        factory=lambda limits: python_adapter.python_adapter(limits=limits),
        scip=ScipIndexer("scip-python", "0.6.6"),
    ),
    LanguageSupport(
        language=typescript_adapter.LANGUAGE,
        extensions=(".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"),
        extractor_name=typescript_adapter.ADAPTER_NAME,
        extractor_version=typescript_adapter.ADAPTER_VERSION,
        parser_fingerprint=typescript_adapter.FINGERPRINT,
        factory=lambda limits: typescript_adapter.typescript_adapter(limits=limits),
        labels={
            ".js": "javascript",
            ".jsx": "javascript",
            ".mjs": "javascript",
            ".cjs": "javascript",
        },
        max_request_source_bytes=512 * 1024,
    ),
    LanguageSupport(
        language=go_adapter.LANGUAGE,
        extensions=(".go",),
        extractor_name=go_adapter.ADAPTER_NAME,
        extractor_version=go_adapter.ADAPTER_VERSION,
        parser_fingerprint=go_adapter.FINGERPRINT,
        factory=lambda limits: go_adapter.go_adapter(limits=limits),
        # A 1 MiB Go request measured 4.4 s of child CPU against the 5 s backstop.
        max_request_source_bytes=512 * 1024,
    ),
)


def _index_entries(entries: tuple[LanguageSupport, ...]) -> Mapping[str, LanguageSupport]:
    by_language: dict[str, LanguageSupport] = {}
    by_extension: set[str] = set()
    for entry in entries:
        if entry.language in by_language:
            raise ValueError("duplicate language registration")
        for extension in entry.extensions:
            if extension in by_extension:
                raise ValueError("duplicate extension registration")
            by_extension.add(extension)
        by_language[entry.language] = entry
    return MappingProxyType(by_language)


LANGUAGES: Final[Mapping[str, LanguageSupport]] = _index_entries(_ENTRIES)
_EXTENSIONS: Final[Mapping[str, str]] = MappingProxyType(
    {extension: entry.language for entry in _ENTRIES for extension in entry.extensions}
)


def language_for_path(path: str) -> str | None:
    """The registered language of a repo-relative path, from its extension (case-sensitive)."""
    return _EXTENSIONS.get(PurePosixPath(path).suffix)


def label_for_path(path: str) -> str | None:
    """The language label events carry for ``path``: the extension's label, never the parser's.

    A single adapter may parse several labelled languages (the TS/JS adapter returns
    ``typescript`` for ``.js`` files, which are labelled ``javascript``).
    """
    language = language_for_path(path)
    if language is None:
        return None
    return LANGUAGES[language].labels.get(PurePosixPath(path).suffix, language)


def support_for(language: str) -> LanguageSupport | None:
    """The registration of ``language``, or ``None`` when it has no structural adapter."""
    return LANGUAGES.get(language)


def build_adapters(
    settings: Settings, languages: tuple[str, ...] | None = None
) -> dict[str, StructuralAdapter]:
    """Sandboxed adapters for ``languages`` (default: all), confined by ``settings`` (FU-39)."""
    limits = Limits.from_settings(settings)
    wanted = tuple(LANGUAGES) if languages is None else languages
    return {language: LANGUAGES[language].factory(limits) for language in sorted(set(wanted))}
