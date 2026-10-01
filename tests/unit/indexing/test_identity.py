from __future__ import annotations

import json
import unicodedata
import uuid
from pathlib import Path
from typing import Any

import pytest

from agent_context_platform.indexing import identity
from agent_context_platform.indexing.identity import (
    EVIDENCE_KINDS,
    NewFile,
    PriorFile,
    ProvisionalFile,
    SimilarRename,
    file_logical_id,
    file_revision_id,
    provisional_file_logical_id,
    provisional_file_revision_id,
    resolve_committed,
    resolve_rename,
    symbol_fallback_id,
    symbol_logical_id,
    symbol_revision_id,
    uncommitted_file_logical_id,
)

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "indexing" / "rename_cases.json"
GOLDEN: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
REPO = GOLDEN["repository_id"]
OTHER_REPO = GOLDEN["other_repository_id"]
C1, C2, C3 = GOLDEN["commits"]
SHA1_A, SHA1_B = GOLDEN["oids"]["a"], GOLDEN["oids"]["b"]
DIGEST_1, DIGEST_2 = GOLDEN["digests"]["one"], GOLDEN["digests"]["two"]
PARSER_1, PARSER_2 = GOLDEN["digests"]["parser_one"], GOLDEN["digests"]["parser_two"]
BODY_1, BODY_2 = GOLDEN["digests"]["body_one"], GOLDEN["digests"]["body_two"]
PY = "python"


def uid(name: str) -> uuid.UUID:
    return uuid.UUID(GOLDEN["ids"][name])


def test_namespaces_are_frozen() -> None:
    assert str(identity.ROOT_NAMESPACE) == GOLDEN["root_namespace"]
    assert str(identity.repository_namespace(REPO)) == GOLDEN["repository_namespace"]


def test_unchanged_file_keeps_logical_and_revision_ids() -> None:
    first = file_logical_id(REPO, "src/app.py", C1)
    again = file_logical_id(REPO, "src/app.py", C1)
    assert first == again == uid("app_logical")
    assert file_revision_id(REPO, first, SHA1_A, PY, PARSER_1) == uid("app_revision_a")
    assert file_revision_id(REPO, again, SHA1_A, PY, PARSER_1) == uid("app_revision_a")


def test_content_change_keeps_logical_id_and_changes_revision() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    changed = file_revision_id(REPO, logical, SHA1_B, PY, PARSER_1)
    assert changed == uid("app_revision_b")
    assert changed != file_revision_id(REPO, logical, SHA1_A, PY, PARSER_1)


def test_sha256_object_format_oids_are_accepted() -> None:
    logical = file_logical_id(REPO, "src/app.py", GOLDEN["sha256_oid"])
    assert file_revision_id(REPO, logical, GOLDEN["sha256_oid"], PY, PARSER_1) == uid(
        "app_revision_sha256"
    )


def test_repository_identity_never_depends_on_a_local_path() -> None:
    assert file_logical_id(REPO, "src/app.py", C1) != file_logical_id(OTHER_REPO, "src/app.py", C1)
    assert file_logical_id(OTHER_REPO, "src/app.py", C1) == uid("app_logical_other_repo")


def test_exact_git_rename_preserves_logical_id() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    resolution = resolve_rename(REPO, C2, [old], [NewFile("src/main.py", SHA1_A)])
    assert resolution.logical_ids == {"src/main.py": uid("app_logical")}
    assert resolution.supersessions == ()


def test_rename_with_content_change_is_not_deterministic() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    resolution = resolve_rename(REPO, C2, [old], [NewFile("src/main.py", SHA1_B)])
    new_id = resolution.logical_ids["src/main.py"]
    assert new_id == uid("main_logical_c2")
    assert new_id != old.logical_id
    # Without a similarity candidate nothing beyond a path change links them.
    assert resolution.supersessions == ()


def test_similarity_candidate_yields_new_id_and_low_confidence_supersession() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    guess = SimilarRename("src/app.py", "src/main.py", 0.8)
    resolution = resolve_rename(REPO, C2, [old], [NewFile("src/main.py", SHA1_B)], [guess])
    (assertion,) = resolution.supersessions
    assert resolution.logical_ids["src/main.py"] != old.logical_id
    assert (assertion.basis, assertion.confidence) == ("similarity", 0.8)
    assert assertion.old_logical_id == old.logical_id


@pytest.mark.parametrize("score", [0.0, -0.1, 1.5])
def test_similarity_score_must_be_in_unit_interval(score: float) -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    with pytest.raises(ValueError, match="score"):
        resolve_rename(
            REPO,
            C2,
            [old],
            [NewFile("src/main.py", SHA1_B)],
            [SimilarRename("src/app.py", "src/main.py", score)],
        )


def test_similarity_candidate_must_link_removed_to_added() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    with pytest.raises(ValueError, match="similar rename"):
        resolve_rename(
            REPO,
            C2,
            [old],
            [NewFile("src/main.py", SHA1_B)],
            [SimilarRename("src/other.py", "src/main.py", 0.8)],
        )


@pytest.mark.parametrize("case", GOLDEN["resolutions"], ids=lambda case: case["name"])
def test_resolutions_match_frozen_golden_cases(case: dict[str, Any]) -> None:
    resolution = resolve_rename(
        REPO,
        case["commit"],
        [PriorFile(x["path"], uuid.UUID(x["logical_id"]), x["blob_oid"]) for x in case["removed"]],
        [NewFile(x["path"], x["blob_oid"]) for x in case["added"]],
        [SimilarRename(x["old_path"], x["new_path"], x["score"]) for x in case["similar"]],
    )
    assert {path: str(value) for path, value in resolution.logical_ids.items()} == case["expected"][
        "logical_ids"
    ]
    assert [
        {
            "old": str(x.old_logical_id),
            "new": str(x.new_logical_id),
            "confidence": x.confidence,
            "evidence_kind": x.evidence_kind,
            "basis": x.basis,
        }
        for x in resolution.supersessions
    ] == case["expected"]["supersessions"]
    assert all(x.confidence < 1 for x in resolution.supersessions)


def test_delete_then_recreate_at_same_path_is_a_new_lineage() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    resolution = resolve_rename(REPO, C3, [old], [NewFile("src/app.py", SHA1_A)])
    new_id = resolution.logical_ids["src/app.py"]
    assert new_id == uid("app_logical_recreated_c3")
    assert new_id != old.logical_id
    (assertion,) = resolution.supersessions
    assert assertion.old_logical_id == old.logical_id
    assert assertion.new_logical_id == new_id
    assert assertion.basis == "path_reuse"
    assert assertion.evidence_kind == "git"
    assert 0 < assertion.confidence < 1


def test_ambiguous_copy_one_source_many_targets() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    added = [NewFile("src/a.py", SHA1_A), NewFile("src/b.py", SHA1_A)]
    resolution = resolve_rename(REPO, C2, [old], added)
    assert resolution.logical_ids == {
        "src/a.py": uid("copy_a_logical_c2"),
        "src/b.py": uid("copy_b_logical_c2"),
    }
    assert old.logical_id not in resolution.logical_ids.values()
    assert [(s.old_logical_id, s.new_logical_id) for s in resolution.supersessions] == [
        (old.logical_id, uid("copy_a_logical_c2")),
        (old.logical_id, uid("copy_b_logical_c2")),
    ]
    assert all(
        s.basis == "ambiguous_blob_match" and 0 < s.confidence < 1 for s in resolution.supersessions
    )


def test_ambiguous_merge_many_sources_one_target() -> None:
    olds = [
        PriorFile("src/a.py", file_logical_id(REPO, "src/a.py", C1), SHA1_A),
        PriorFile("src/b.py", file_logical_id(REPO, "src/b.py", C1), SHA1_A),
    ]
    resolution = resolve_rename(REPO, C2, olds, [NewFile("src/c.py", SHA1_A)])
    new_id = resolution.logical_ids["src/c.py"]
    assert new_id not in {old.logical_id for old in olds}
    assert {s.old_logical_id for s in resolution.supersessions} == {o.logical_id for o in olds}
    assert all(s.new_logical_id == new_id and s.confidence < 1 for s in resolution.supersessions)


def test_resolve_rename_is_order_independent() -> None:
    olds = [
        PriorFile("src/a.py", file_logical_id(REPO, "src/a.py", C1), SHA1_A),
        PriorFile("src/b.py", file_logical_id(REPO, "src/b.py", C1), SHA1_B),
    ]
    added = [NewFile("src/x.py", SHA1_A), NewFile("src/y.py", SHA1_B)]
    forward = resolve_rename(REPO, C2, olds, added)
    backward = resolve_rename(REPO, C2, olds[::-1], added[::-1])
    assert forward == backward
    assert forward.logical_ids == {
        "src/x.py": olds[0].logical_id,
        "src/y.py": olds[1].logical_id,
    }


def test_unrelated_added_file_gets_a_fresh_id_and_no_assertion() -> None:
    resolution = resolve_rename(REPO, C2, [], [NewFile("docs/new.md", SHA1_B)])
    assert resolution.logical_ids == {"docs/new.md": file_logical_id(REPO, "docs/new.md", C2)}
    assert resolution.supersessions == ()


def test_scip_symbol_is_preferred_over_the_fallback() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    scip = "scip-python python pkg . `app`/run()."
    with_scip = symbol_logical_id(
        REPO,
        scip_symbol=scip,
        language="python",
        file_logical_id=logical,
        qualified_name="app.run",
        kind="function",
    )
    assert with_scip == uid("symbol_scip")
    fallback = symbol_fallback_id(REPO, "python", logical, "app.run", "function")
    assert fallback == uid("symbol_fallback")
    assert with_scip != fallback
    without = symbol_logical_id(
        REPO,
        scip_symbol=None,
        language="python",
        file_logical_id=logical,
        qualified_name="app.run",
        kind="function",
    )
    assert without == fallback


def test_scip_local_symbols_fall_back() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    local = symbol_logical_id(
        REPO,
        scip_symbol="local 7",
        language="python",
        file_logical_id=logical,
        qualified_name="app.run",
        kind="function",
    )
    assert local == uid("symbol_fallback")


def test_fallback_disambiguator_separates_overloads() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    first = symbol_fallback_id(REPO, "python", logical, "app.run", "function", "1")
    assert first == uid("symbol_fallback_overload_1")
    assert first != symbol_fallback_id(REPO, "python", logical, "app.run", "function")


def test_signature_change_keeps_symbol_id_and_changes_revision() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    symbol = symbol_fallback_id(REPO, "python", logical, "app.run", "function")
    before = symbol_revision_id(REPO, symbol, DIGEST_1, BODY_1)
    after = symbol_revision_id(REPO, symbol, DIGEST_2, BODY_1)
    assert before == uid("symbol_revision_1")
    assert after == uid("symbol_revision_2")
    assert before != after
    assert symbol == symbol_fallback_id(REPO, "python", logical, "app.run", "function")


def test_symbol_survives_a_file_rename_only_through_scip() -> None:
    old = file_logical_id(REPO, "src/app.py", C1)
    scip = "scip-python python pkg . `app`/run()."
    kwargs: dict[str, Any] = {"language": "python", "qualified_name": "app.run", "kind": "function"}
    a = symbol_logical_id(REPO, scip_symbol=scip, file_logical_id=old, **kwargs)
    b = symbol_logical_id(
        REPO, scip_symbol=scip, file_logical_id=file_logical_id(REPO, "src/x.py", C2), **kwargs
    )
    assert a == b


def test_components_cannot_collide_through_concatenation() -> None:
    # A naive "a/b" + "c" versus "a" + "b/c" join would collide; length prefixes do not.
    assert identity.encode_components("file", "a/b", "c") != identity.encode_components(
        "file", "a", "b/c"
    )
    assert identity.encode_components("x", "1:a") != identity.encode_components("x1", ":a")
    logical = file_logical_id(REPO, "src/app.py", C1)
    assert symbol_fallback_id(REPO, "py", logical, "ab", "c") != symbol_fallback_id(
        REPO, "py", logical, "a", "bc"
    )
    assert symbol_fallback_id(REPO, "py", logical, "a", "b", "c") != symbol_fallback_id(
        REPO, "py", logical, "a", "b/c"
    )
    assert file_logical_id(REPO, "a/b/c", C1) != file_logical_id(REPO, "a/b", C1)


def test_path_bytes_are_identity_preserving_nfc_and_nfd_are_distinct() -> None:
    nfc = unicodedata.normalize("NFC", "src/caf\u00e9.py")
    nfd = unicodedata.normalize("NFD", nfc)
    assert nfc != nfd
    assert file_logical_id(REPO, nfc, C1) == uid("nfc_logical")
    assert file_logical_id(REPO, nfd, C1) == uid("nfd_logical")
    assert uid("nfc_logical") != uid("nfd_logical")
    resolution = resolve_rename(REPO, C1, [], [NewFile(nfc, SHA1_A), NewFile(nfd, SHA1_A)])
    assert resolution.logical_ids == {nfc: uid("nfc_logical"), nfd: uid("nfd_logical")}


def test_file_revision_depends_on_language_and_parser_fingerprint() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    base = file_revision_id(REPO, logical, SHA1_A, PY, PARSER_1)
    assert base == uid("app_revision_a")
    assert file_revision_id(REPO, logical, SHA1_A, PY, PARSER_2) == uid("app_revision_a_parser_2")
    assert file_revision_id(REPO, logical, SHA1_A, "typescript", PARSER_1) == uid(
        "app_revision_a_typescript"
    )
    assert len({base, uid("app_revision_a_parser_2"), uid("app_revision_a_typescript")}) == 3


def test_body_change_with_same_signature_is_a_new_symbol_revision() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    symbol = symbol_fallback_id(REPO, "python", logical, "app.run", "function")
    assert symbol_revision_id(REPO, symbol, DIGEST_1, BODY_1) == uid("symbol_revision_1")
    body_changed = symbol_revision_id(REPO, symbol, DIGEST_1, BODY_2)
    assert body_changed == uid("symbol_revision_1_body_2")
    assert body_changed != uid("symbol_revision_1")


def test_malformed_parser_and_semantic_fingerprints_are_rejected() -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    symbol = symbol_fallback_id(REPO, "python", logical, "app.run", "function")
    with pytest.raises(ValueError, match="parser fingerprint"):
        file_revision_id(REPO, logical, SHA1_A, PY, "bad")
    with pytest.raises(ValueError, match="language"):
        file_revision_id(REPO, logical, SHA1_A, "", PARSER_1)
    with pytest.raises(ValueError, match="semantic fingerprint"):
        symbol_revision_id(REPO, symbol, DIGEST_1, "bad")


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/abs.py",
        "a/../b",
        "../b",
        "a//b",
        "./a",
        "a/./b",
        "a\\b",
        "a/.git/x",
        "a/\x00b",
        "a/\ud800",
    ],
)
def test_unsafe_paths_are_rejected(path: str) -> None:
    with pytest.raises(ValueError, match="path"):
        file_logical_id(REPO, path, C1)
    with pytest.raises(ValueError, match="path"):
        resolve_rename(REPO, C2, [], [NewFile(path, SHA1_A)])


@pytest.mark.parametrize("oid", ["", "xyz", "A" * 40, "a" * 39, "a" * 41, "a" * 63])
def test_malformed_oids_are_rejected(oid: str) -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    with pytest.raises(ValueError, match="oid"):
        file_revision_id(REPO, logical, oid, PY, PARSER_1)


@pytest.mark.parametrize("digest", ["", "abc", "A" * 64, "g" * 64])
def test_malformed_digests_are_rejected(digest: str) -> None:
    logical = file_logical_id(REPO, "src/app.py", C1)
    symbol = symbol_fallback_id(REPO, "python", logical, "app.run", "function")
    with pytest.raises(ValueError, match="digest"):
        symbol_revision_id(REPO, symbol, digest, BODY_1)


@pytest.mark.parametrize("repository_id", ["", "bad\x00id", "bad\ud800id"])
def test_empty_or_control_repository_ids_are_rejected(repository_id: str) -> None:
    with pytest.raises(ValueError, match="repository_id"):
        file_logical_id(repository_id, "src/app.py", C1)


def test_duplicate_paths_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        resolve_rename(REPO, C2, [], [NewFile("a.py", SHA1_A), NewFile("a.py", SHA1_B)])


def test_derivation_matches_a_hand_computed_uuid5_chain() -> None:
    root = uuid.UUID("6ff9b492-0edb-50ba-bb24-6c063fa0a74f")
    namespace = uuid.uuid5(root, f"10:repository{len(REPO)}:{REPO}")
    assert namespace == identity.repository_namespace(REPO)
    expected = uuid.uuid5(namespace, f"4:file10:src/app.py{len(C1)}:{C1}")
    assert file_logical_id(REPO, "src/app.py", C1) == expected


def introducing_commit(history: list[dict[str, Any]], path: str, seen_from: str) -> str:
    """Test-only stand-in for the indexer: first-parent history, not where it started."""
    del seen_from  # the introducing commit must not depend on where indexing began
    return next(entry["commit"] for entry in history if path in entry["added"])


def test_indexers_starting_at_different_commits_derive_the_same_id() -> None:
    case = GOLDEN["introducing_commit"]
    ids = set()
    for start in case["indexer_starts"]:
        introducing = introducing_commit(case["history"], case["path"], start)
        assert introducing == case["expected_introducing_commit"]
        ids.add(file_logical_id(REPO, case["path"], introducing))
    assert ids == {uuid.UUID(case["expected_logical_id"])}


def test_rename_preserved_lineage_keeps_the_original_origin() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    later = resolve_rename(REPO, C3, [old], [NewFile("src/main.py", SHA1_A)])
    assert later.logical_ids["src/main.py"] == uid("app_logical")


def test_provisional_ids_are_frozen_and_disjoint_from_committed_ids() -> None:
    checkout = GOLDEN["checkout_id"]
    provisional = provisional_file_logical_id(REPO, checkout, "src/new.py")
    assert provisional == uid("provisional_new")
    assert provisional_file_revision_id(REPO, provisional, SHA1_A, PY, PARSER_1) == uid(
        "provisional_new_revision_a"
    )
    other = provisional_file_logical_id(REPO, checkout[:-1] + "1", "src/new.py")
    assert other == uid("provisional_new_other_checkout") != provisional
    for commit in GOLDEN["commits"]:
        assert provisional != file_logical_id(REPO, "src/new.py", commit)


@pytest.mark.parametrize("case", GOLDEN["provisional_cases"], ids=lambda case: case["name"])
def test_committed_resolution_matches_frozen_golden_cases(case: dict[str, Any]) -> None:
    resolution = resolve_committed(
        REPO,
        case["introducing_commit"],
        [
            ProvisionalFile(x["path"], uuid.UUID(x["logical_id"]), x["blob_oid"])
            for x in case["provisional"]
        ],
        [NewFile(x["path"], x["blob_oid"]) for x in case["committed"]],
    )
    assert {path: str(value) for path, value in resolution.logical_ids.items()} == case["expected"][
        "logical_ids"
    ]
    assert [
        {
            "old": str(x.old_logical_id),
            "new": str(x.new_logical_id),
            "confidence": x.confidence,
            "evidence_kind": x.evidence_kind,
            "basis": x.basis,
        }
        for x in resolution.supersessions
    ] == case["expected"]["supersessions"]


def test_untracked_then_committed_same_blob_is_direct_evidence() -> None:
    (assertion,) = resolve_committed(
        REPO,
        C2,
        [
            ProvisionalFile(
                "src/new.py",
                provisional_file_logical_id(REPO, GOLDEN["checkout_id"], "src/new.py"),
                SHA1_A,
            )
        ],
        [NewFile("src/new.py", SHA1_A)],
    ).supersessions
    assert (assertion.confidence, assertion.evidence_kind) == (1.0, "git")


def test_duplicate_committed_paths_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        resolve_committed(REPO, C2, [], [NewFile("a.py", SHA1_A), NewFile("a.py", SHA1_B)])


def test_every_emitted_evidence_kind_is_a_design_assertion_kind() -> None:
    assert {
        "git",
        "scip",
        "tree_sitter",
        "test",
        "user",
        "agent",
        "llm_inference",
    } == EVIDENCE_KINDS
    emitted: set[str] = set()
    for case in GOLDEN["resolutions"]:
        emitted |= {x["evidence_kind"] for x in case["expected"]["supersessions"]}
    for case in GOLDEN["provisional_cases"]:
        emitted |= {x["evidence_kind"] for x in case["expected"]["supersessions"]}
    assert emitted == {"git"}
    assert emitted <= EVIDENCE_KINDS


def test_duplicate_similarity_pairs_keep_the_maximum_and_are_order_independent() -> None:
    old = PriorFile("src/app.py", file_logical_id(REPO, "src/app.py", C1), SHA1_A)
    added = [NewFile("src/main.py", SHA1_B)]
    low = SimilarRename("src/app.py", "src/main.py", 0.4)
    high = SimilarRename("src/app.py", "src/main.py", 0.7)
    forward = resolve_rename(REPO, C2, [old], added, [low, high])
    backward = resolve_rename(REPO, C2, [old], added, [high, low])
    assert forward == backward
    (assertion,) = forward.supersessions
    assert assertion.confidence == 0.7


def test_duplicate_provisional_paths_are_rejected() -> None:
    checkout = GOLDEN["checkout_id"]
    first = provisional_file_logical_id(REPO, checkout, "a.py")
    second = provisional_file_logical_id(REPO, checkout[:-1] + "1", "a.py")
    with pytest.raises(ValueError, match="duplicate provisional path"):
        resolve_committed(
            REPO,
            C2,
            [ProvisionalFile("a.py", first, SHA1_A), ProvisionalFile("a.py", second, SHA1_B)],
            [NewFile("a.py", SHA1_A)],
        )


def test_duplicate_removed_paths_and_ids_are_rejected() -> None:
    one = file_logical_id(REPO, "a.py", C1)
    other = file_logical_id(REPO, "b.py", C1)
    added = [NewFile("c.py", SHA1_A)]
    with pytest.raises(ValueError, match="duplicate removed path"):
        resolve_rename(
            REPO, C2, [PriorFile("a.py", one, SHA1_A), PriorFile("a.py", other, SHA1_B)], added
        )
    with pytest.raises(ValueError, match="duplicate removed logical id"):
        resolve_rename(
            REPO, C2, [PriorFile("a.py", one, SHA1_A), PriorFile("b.py", one, SHA1_B)], added
        )


def test_duplicate_added_and_committed_paths_and_similar_pairs_are_deterministic() -> None:
    # Added/committed duplicates raise (covered above); similar duplicates keep the maximum.
    old = PriorFile("a.py", file_logical_id(REPO, "a.py", C1), SHA1_A)
    pairs = [SimilarRename("a.py", "b.py", score) for score in (0.3, 0.6, 0.5)]
    outcomes = [
        resolve_rename(REPO, C2, [old], [NewFile("b.py", SHA1_B)], ordering)
        for ordering in (pairs, pairs[::-1], [pairs[1], pairs[2], pairs[0]])
    ]
    assert outcomes[0] == outcomes[1] == outcomes[2]
    assert outcomes[0].supersessions[0].confidence == 0.6


def test_an_uncommitted_file_id_is_per_repository_and_path_only() -> None:
    frozen = uncommitted_file_logical_id("repo-1", "src/new.py")

    assert str(frozen) == "176df8d5-0f43-58eb-9ee6-432f7d1a6db6"
    assert uncommitted_file_logical_id("repo-2", "src/new.py") != frozen
    assert uncommitted_file_logical_id("repo-1", "src/other.py") != frozen
