"""Idempotency keys are digests of the claim; the claim view is shared with the conflict check."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest
from agent_context_sdk import EventContextV1

from agent_context_platform.indexing.emitter import (
    OBSERVATION_FIELDS,
    claim_key,
    parse_structural,
    read_sources,
)
from agent_context_platform.indexing.tree_sitter.base import ParsedDiagnostic

from .conftest import RepoBuilder
from .test_emitter import (
    HELPER,
    NOW,
    _events,
    _payloads,
    by_type,
    lineage,
    parse,
    scan_of,
    seed,
    service,
)
from .test_emitter_resolution import TableAdapter, _reference

pytestmark = pytest.mark.unit


def _mutated(value: Any) -> Any:
    if isinstance(value, bool):
        return not value
    if isinstance(value, int | float):
        return value + 1
    if value is None:
        return "changed"
    if isinstance(value, str):
        return value + "x"
    return [*value, "changed"] if isinstance(value, list) else {**value, "changed": "1"}


def test_the_key_changes_with_every_claim_field_and_with_no_observation_field(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    drafts, _ = _events(repo)
    assert {d.event_type for d in drafts} == set(OBSERVATION_FIELDS)  # every event type is covered

    for draft in drafts:
        assert claim_key(draft) == draft.idempotency_key
        observed = OBSERVATION_FIELDS[draft.event_type]
        for name, value in draft.payload.items():
            changed = draft.model_copy(update={"payload": {**draft.payload, name: _mutated(value)}})
            same = claim_key(changed) == draft.idempotency_key
            assert same == (name in observed), (draft.event_type, name)
        # Where and when an event was seen is never part of the claim.
        elsewhere = draft.model_copy(
            update={
                "context": EventContextV1(
                    repository_id=draft.context.repository_id, checkout_id="other"
                ),
                "occurred_at": NOW + timedelta(days=9),
                "observed_at": NOW + timedelta(days=9),
            }
        )
        assert claim_key(elsewhere) == draft.idempotency_key
        # ...but the envelope is: another stream is another claim.
        assert (
            claim_key(draft.model_copy(update={"stream_id": "code-index:other"}))
            != draft.idempotency_key
        )


def test_an_incomplete_index_reports_failure_and_a_clean_one_does_not(
    make_repo: Callable[[str], RepoBuilder],
) -> None:
    repo = make_repo("repo")
    seed(repo, {"pkg/util.py": HELPER})
    repo.commit("seed")
    scan = scan_of(repo)
    ids = lineage(scan, scan.workspace.head_commit or "")
    clean = parse(scan)

    degraded = [
        p.model_copy(
            update={
                "symbols": (),
                "relations": (),
                "references": (),
                "diagnostics": (ParsedDiagnostic(code="file_degraded", count=1),),
            }
        )
        if p.path == "pkg/util.py"
        else p
        for p in clean
    ]
    skipped = [p for p in clean if p.path != "pkg/util.py"]  # one file never reached the run

    def completed(parsed: list) -> dict[str, Any]:  # type: ignore[type-arg]
        drafts = service().index(scan, None, parsed, file_logical_ids=ids)
        return dict(by_type(drafts, "code.index.completed")[0].payload)

    ok, bad_degraded, bad_skipped = completed(clean), completed(degraded), completed(skipped)

    assert ok["success"] is True and ok["error_class"] is None
    assert bad_degraded["success"] is False and bad_degraded["error_class"] == "files_degraded"
    assert bad_skipped["success"] is False and bad_skipped["error_class"] == "files_skipped"
    keys = {
        claim_key(
            by_type(
                service().index(scan, None, parsed, file_logical_ids=ids), "code.index.completed"
            )[0]
        )
        for parsed in (clean, degraded, skipped)
    }
    assert len(keys) == 3  # a different outcome is a different completed event, never a conflict
    started = {
        by_type(service().index(scan, None, parsed, file_logical_ids=ids), "code.index.started")[
            0
        ].idempotency_key
        for parsed in (clean, degraded, skipped)
    }
    assert len(started) == 1  # the target's started event is one claim


def test_removing_a_call_between_surviving_symbols_is_visible_per_file_revision(
    make_repo: Callable[[str], RepoBuilder], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = make_repo("repo")
    repo.write("pkg/a.py", "a\n")
    repo.write("pkg/b.py", "b\n")
    repo.commit("seed")
    first_scan = scan_of(repo)
    ids = lineage(first_scan, first_scan.workspace.head_commit or "")
    members = {"pkg/a.py": ("caller",), "pkg/b.py": ("callee",)}
    calls = {
        "pkg/a.py": (
            _reference("call", "callee", qualifier="b"),
            _reference("import", "b", level=1),
        )
    }

    def index(scan: Any, table: dict) -> list:  # type: ignore[type-arg]
        adapter = TableAdapter("python", table, members)
        parsed = list(parse_structural({"python": adapter}, read_sources(scan)).files)  # type: ignore[dict-item]
        return service().index(scan, None, parsed, file_logical_ids=ids)

    def assertions(drafts: list, predicate: str) -> set[str]:  # type: ignore[type-arg]
        return {
            p["assertion_id"]
            for p in _payloads(drafts, "code.relation.asserted")
            if p["predicate"] == predicate
        }

    before = index(first_scan, calls)
    assert assertions(before, "CALLS")

    # Unchanged file, same call: the very same assertion (no new event on the ledger).
    repo.write("pkg/b.py", "b changed\n")  # the callee's file changes, the caller's does not
    repo.commit("touch callee")
    second_scan = scan_of(repo)
    unchanged_caller = index(second_scan, calls)
    assert assertions(unchanged_caller, "CALLS") == assertions(before, "CALLS")

    # The caller's file changes and the call is gone: nothing carries the new revision, and the
    # old assertion (bound to the old revision) is absent from the new target's claims.
    repo.write("pkg/a.py", "a rewritten\n")
    repo.commit("drop the call")
    third_scan = scan_of(repo)
    dropped = index(third_scan, {})
    assert not assertions(dropped, "CALLS")
    assert not assertions(dropped, "CALLS") & assertions(before, "CALLS")
    revisions = {
        d.payload["path"]: d.payload["file_revision_id"]
        for d in by_type(dropped, "code.file.indexed")
    }
    old = {
        d.payload["path"]: d.payload["file_revision_id"]
        for d in by_type(before, "code.file.indexed")
    }
    assert (
        revisions["pkg/a.py"] != old["pkg/a.py"]
    )  # the caller's revision changed; its call is not
