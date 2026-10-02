"""Index one checkout end to end: scan, lineage, SCIP, structural parse, events, ingestion.

``index_checkout`` composes the existing pieces and nothing else: the safe scanner, the
history-derived lineage (``lineage.derive_lineage``), the optional SCIP import given the SAME
lineage mapping, the registry's sandboxed structural adapters (Landlock stays on) and
``IndexingService.index`` / ``ingest``. It is what ``agent-context index`` runs and what INFRA can
call in process; it never reads the environment or opens a connection, so the caller supplies the
``IndexingService`` (and so the database role) and the ``Settings``.

``IndexReport`` is content-free by construction: identifiers, counts and fixed codes only. A
failure is a report too: an ``IndexingError`` (``shallow_history``, ``history_too_large``,
``idempotency_conflict``...), a refused scan or an unusable SCIP index sets ``error_class`` and
``success=False`` with whatever counts exist, so a caller can always print them.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from agent_context_platform.indexing import registry
from agent_context_platform.indexing.emitter import (
    IndexingError,
    IndexingService,
    parse_structural,
    read_sources,
)
from agent_context_platform.indexing.identity import ProvisionalFile, Supersession
from agent_context_platform.indexing.lineage import DEFAULT_MAX_COMMITS, derive_lineage
from agent_context_platform.indexing.scanner import RepositoryScan, ScanError, scan_repository
from agent_context_platform.indexing.scip import (
    DEFAULT_LIMITS,
    ScipImportError,
    SemanticIndex,
    import_scip,
)
from agent_context_platform.indexing.tree_sitter.base import StructuralError
from agent_context_platform.settings import Settings

_LOG = logging.getLogger(__name__)

# Stable, content-free codes for failures that are not an ``IndexingError`` of the emitter.
SCAN_FAILED: Final = "scan_failed"
SCIP_UNREADABLE: Final = "scip_unreadable"
SCIP_NEEDS_COMMIT: Final = "scip_requires_commit"


@dataclass(frozen=True, slots=True)
class IndexRequest:
    """What to index. ``provisional`` are earlier uncommitted observations to link to history.

    The checkout id (and workspace and project) come from the ``IndexingConfig`` of the service the
    caller passes to ``index_checkout``; the request carries no second copy of them. The service's
    ``repository_id`` must equal ``repository_id`` here (``repository_mismatch`` otherwise).
    """

    repository_id: str
    checkout: Path
    scip: Path | None = None
    max_commits: int = DEFAULT_MAX_COMMITS
    provisional: tuple[ProvisionalFile, ...] = ()


@dataclass(frozen=True, slots=True)
class IndexReport:
    """The outcome of one run: ledger counts, the completion outcome and a diagnostics summary.

    ``submitted`` events were new to the ledger and ``skipped`` were already there (a re-index of
    an unchanged target submits none). ``files`` and ``files_not_indexed`` come from the target's
    membership and coverage events. ``target_kind`` is ``commit`` or ``snapshot``.
    """

    repository_id: str
    success: bool
    error_class: str | None
    index_id: str | None = None
    target_kind: str | None = None
    target: str | None = None
    submitted: int = 0
    skipped: int = 0
    files: int = 0
    files_not_indexed: int = 0
    diagnostics: Mapping[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "repository_id": self.repository_id,
            "success": self.success,
            "error_class": self.error_class,
            "index_id": self.index_id,
            "target": {"kind": self.target_kind, "id": self.target},
            "submitted": self.submitted,
            "skipped": self.skipped,
            "files": self.files,
            "files_not_indexed": self.files_not_indexed,
            "diagnostics": dict(sorted(self.diagnostics.items())),
        }


def _failure(request: IndexRequest, code: str, **counts: Any) -> IndexReport:
    return IndexReport(request.repository_id, False, code, **counts)


def _semantic(
    request: IndexRequest, scan: RepositoryScan, ids: Mapping[str, uuid.UUID]
) -> SemanticIndex | None:
    if request.scip is None:
        return None
    head = scan.workspace.head_commit
    if head is None:
        raise IndexingError(SCIP_NEEDS_COMMIT)
    try:
        with request.scip.open("rb") as handle:
            data = handle.read(DEFAULT_LIMITS.max_index_bytes + 1)
    except OSError:
        raise IndexingError(SCIP_UNREADABLE) from None
    try:
        return import_scip(data, request.repository_id, head, file_logical_ids=ids)
    except ScipImportError as error:
        raise IndexingError(f"scip_{error.reason}") from None


def _build(
    request: IndexRequest, settings: Settings, service: IndexingService
) -> tuple[list[Any], Counter[str]]:
    """Everything up to the drafts: blocking (git, child processes), so run off the event loop."""
    checkout = request.checkout.resolve(strict=True)
    scan = scan_repository(checkout)
    lineage = derive_lineage(scan, request.repository_id, max_commits=request.max_commits)
    ids = lineage.file_logical_ids
    supersessions: tuple[Supersession, ...] = (
        *lineage.supersessions,
        *lineage.link_provisional(request.provisional),
    )
    semantic = _semantic(request, scan, ids)
    sources = read_sources(scan)
    # The checkout is never readable by an adapter (Landlock): add it to the checkout roots.
    roots = tuple(dict.fromkeys((*settings.indexer_checkout_roots, str(checkout))))
    confined = settings.model_copy(update={"indexer_checkout_roots": roots})
    adapters = registry.build_adapters(confined)
    structural = parse_structural(adapters, sources)
    drafts = service.index(
        scan,
        semantic,
        structural.files,
        file_logical_ids=ids,
        supersessions=supersessions,
        sources=sources,
    )
    return drafts, Counter(structural.diagnostics)


def _summarize(drafts: Iterable[Any], diagnostics: Counter[str]) -> dict[str, Any]:
    """Counts only: the completion payload and the coverage losses (never paths or content)."""
    summary: dict[str, Any] = {"completed": None, "files": 0, "not_indexed": 0}
    for draft in drafts:
        payload = draft.payload
        if draft.event_type == "code.index.completed":
            summary["completed"] = payload
        elif draft.event_type == "code.file.indexed":
            summary["files"] += 1
        elif draft.event_type == "code.file.coverage_reported":
            for loss in payload.get("losses", ()):
                diagnostics[f"loss_{loss}"] += 1
                if loss == "not_indexed":
                    summary["not_indexed"] += 1
    return summary


async def index_checkout(
    request: IndexRequest, settings: Settings, service: IndexingService
) -> IndexReport:
    """Index ``request.checkout`` and submit the events through ``service``.

    Never raises for an indexing failure: the report carries ``error_class`` and the counts known
    so far. A bad ``repository_id`` raises ``ValueError`` and a missing checkout ``OSError``
    (usage errors the caller reports before running).
    """
    if service.config.repository_id != request.repository_id:
        return _failure(request, "repository_mismatch")
    try:
        drafts, diagnostics = await asyncio.to_thread(_build, request, settings, service)
    except IndexingError as error:
        return _failure(request, error.code)
    except StructuralError as error:
        # Landlock unavailable, an adapter that cannot spawn, a rejected read set: content-free.
        return _failure(
            request,
            f"structural_{error.code.value}",
            diagnostics={f"structural_{error.code.value}": 1},
        )
    except ScanError as error:
        _LOG.warning("scan refused: %s", error.reason.value)
        return _failure(request, SCAN_FAILED, diagnostics={f"scan_{error.reason.value}": 1})
    summary = _summarize(drafts, diagnostics)
    completed = summary["completed"] or {}
    target_kind = "snapshot" if completed.get("snapshot_id") else "commit"
    common: dict[str, Any] = {
        "index_id": completed.get("index_id"),
        "target_kind": target_kind,
        "target": completed.get("snapshot_id") or completed.get("commit_id"),
        "files": summary["files"],
        "files_not_indexed": summary["not_indexed"],
        "diagnostics": dict(diagnostics),
    }
    try:
        outcome = await service.ingest(drafts)
    except IndexingError as error:
        return _failure(request, error.code, **common)
    error_class = completed.get("error_class")
    return IndexReport(
        request.repository_id,
        bool(completed.get("success")),
        error_class,
        submitted=outcome.submitted,
        skipped=outcome.existing,
        **common,
    )
