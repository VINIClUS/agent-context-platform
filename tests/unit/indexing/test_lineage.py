"""File lineage from first-parent history, through real temporary Git repositories."""

from __future__ import annotations

import json
import shutil
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from agent_context_platform.indexing import identity
from agent_context_platform.indexing.emitter import IndexingError, blob_oid
from agent_context_platform.indexing.identity import ProvisionalFile
from agent_context_platform.indexing.lineage import Lineage, derive_lineage
from agent_context_platform.indexing.scanner import scan_repository

from .conftest import RepoBuilder

pytestmark = pytest.mark.unit

GOLDEN: dict[str, Any] = json.loads(
    (Path(__file__).resolve().parents[2] / "fixtures" / "indexing" / "rename_cases.json").read_text(
        encoding="utf-8"
    )
)
REPO = "repo-039c"
A = "".join(f"line {number} of the original module\n" for number in range(20))
A_EDITED = A.replace("line 19 of the original module", "line 19 was edited")
OTHER = "".join(f"nothing alike {number * 7919}\n" for number in range(20))

Factory = Callable[[str], RepoBuilder]


def lineage_of(repo: RepoBuilder, **options: Any) -> Lineage:
    return derive_lineage(scan_repository(repo.root), REPO, **options)


def lid(path: str, commit: str) -> uuid.UUID:
    return identity.file_logical_id(REPO, path, commit)


def remove(repo: RepoBuilder, *paths: str) -> None:
    for path in paths:
        (repo.root / path).unlink()


def test_an_unborn_head_has_no_lineage(make_repo: Factory) -> None:
    result = lineage_of(make_repo("repo"))
    assert (
        result.target_commit is None and not result.file_logical_ids and result.commits_walked == 0
    )


@pytest.mark.parametrize("case", GOLDEN["resolutions"], ids=lambda case: case["name"])
def test_rename_cases_through_real_repositories(make_repo: Factory, case: dict[str, Any]) -> None:
    """Each frozen ``resolve_rename`` case, played as real commits, resolves the same way."""
    repo = make_repo("repo")
    contents = {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": A}
    contents["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"] = (
        A_EDITED if "similarity" in case["name"] else OTHER
    )
    removed = {item["path"]: item for item in case["removed"]}
    added = {item["path"]: item for item in case["added"]}
    for path, item in removed.items():
        repo.write(path, contents[item["blob_oid"]])
    first = repo.commit("seed") if removed else None
    before = lineage_of(repo) if removed else None
    recreated = set(removed) & set(added)
    remove(repo, *removed)
    if recreated:
        repo.commit("delete")
    for path, item in added.items():
        repo.write(path, contents[item["blob_oid"]])
    introducing = repo.commit("change")
    after = lineage_of(repo)

    kept_ids = {item["logical_id"]: path for path, item in removed.items()}
    old_ids = {path: (before.file_logical_ids[path] if before else None) for path in removed}
    expected_ids: dict[str, uuid.UUID] = {}
    for path, golden_id in case["expected"]["logical_ids"].items():
        if golden_id in kept_ids:
            expected_ids[path] = old_ids[kept_ids[golden_id]]  # type: ignore[assignment]
        else:
            expected_ids[path] = lid(path, introducing)
    assert dict(after.file_logical_ids) == expected_ids
    assert first is None or all(
        after.origins[path].introducing_commit == first
        for path, value in expected_ids.items()
        if value in old_ids.values()
    )

    def real(golden: str, path: str | None = None) -> uuid.UUID:
        if golden in kept_ids:
            return old_ids[kept_ids[golden]]  # type: ignore[return-value]
        return expected_ids[path or ""]

    wanted = {
        (real(item["old"]), real(item["new"], _path_of(case, item["new"])), item["basis"])
        for item in case["expected"]["supersessions"]
    }
    got = {(s.old_logical_id, s.new_logical_id, s.basis) for s in after.supersessions}
    assert got == wanted
    golden_confidence = {
        item["basis"]: item["confidence"] for item in case["expected"]["supersessions"]
    }
    for claim in after.supersessions:
        assert claim.evidence_kind == "git" and 0 < claim.confidence < 1
        if claim.basis == "similarity":
            assert claim.confidence == pytest.approx(0.9)
        elif claim.basis in golden_confidence:
            assert claim.confidence == golden_confidence[claim.basis]


def _path_of(case: dict[str, Any], golden_id: str) -> str:
    return next(
        path for path, value in case["expected"]["logical_ids"].items() if value == golden_id
    )


@pytest.mark.parametrize("case", GOLDEN["provisional_cases"], ids=lambda case: case["name"])
def test_provisional_cases_through_real_repositories(
    make_repo: Factory, case: dict[str, Any]
) -> None:
    repo = make_repo("repo")
    repo.write("seed.txt", "seed\n")
    repo.commit("seed")
    path = case["committed"][0]["path"]
    committed_blob = case["committed"][0]["blob_oid"]
    first_text = A if case["provisional"] else None
    if first_text is not None:
        repo.write(path, first_text)
        dirty = scan_repository(repo.root)
        assert path in {item.path for item in dirty.untracked}
        provisional = [
            ProvisionalFile(
                path,
                identity.uncommitted_file_logical_id(REPO, path),
                blob_oid(first_text.encode(), dirty.object_format),
            )
        ]
    else:
        provisional = []
    repo.write(path, A if committed_blob.startswith("a") else A_EDITED)
    commit = repo.commit("add")
    result = lineage_of(repo)

    assert result.file_logical_ids[path] == lid(path, commit)
    links = result.link_provisional(provisional)
    expected = case["expected"]["supersessions"]
    assert [(s.confidence, s.evidence_kind, s.basis) for s in links] == [
        (item["confidence"], item["evidence_kind"], item["basis"]) for item in expected
    ]
    for claim in links:
        assert claim.old_logical_id == identity.uncommitted_file_logical_id(REPO, path)
        assert claim.new_logical_id == lid(path, commit)


def test_a_provisional_file_links_to_the_origin_path_of_a_renamed_lineage(
    make_repo: Factory,
) -> None:
    repo = make_repo("repo")
    repo.write("old.py", A)
    first = repo.commit("add")
    (repo.root / "old.py").rename(repo.root / "new.py")
    repo.commit("rename")
    result = lineage_of(repo)
    old_provisional = identity.uncommitted_file_logical_id(REPO, "old.py")
    wrong = identity.uncommitted_file_logical_id(REPO, "new.py")
    blob = result.origins["new.py"].origin_blob_oid
    links = result.link_provisional(
        [ProvisionalFile("old.py", old_provisional, blob), ProvisionalFile("new.py", wrong, blob)]
    )
    assert [(s.old_logical_id, s.new_logical_id, s.confidence) for s in links] == [
        (old_provisional, lid("old.py", first), 1.0)
    ]


def test_the_introducing_commit_is_the_same_whichever_commit_the_indexer_starts_from(
    make_repo: Factory,
) -> None:
    """The frozen ``introducing_commit`` case: app.py is added first, x.py later, then a no-op."""
    repo = make_repo("repo")
    repo.write("src/app.py", A)
    c1 = repo.commit("one")
    repo.write("src/x.py", OTHER)
    c2 = repo.commit("two")
    repo.write("src/app.py", A_EDITED)
    c3 = repo.commit("three")
    scan = scan_repository(repo.root)
    seen = {commit: derive_lineage(scan, REPO, target_commit=commit) for commit in (c1, c2, c3)}
    for commit, result in seen.items():
        assert result.file_logical_ids["src/app.py"] == lid("src/app.py", c1)
        assert result.origins["src/app.py"].introducing_commit == c1
        assert result.target_commit == commit
    assert set(seen[c1].file_logical_ids) == {"src/app.py"}
    assert seen[c3].file_logical_ids["src/x.py"] == lid("src/x.py", c2)


def test_add_modify_rename_and_rename_back_keep_one_lineage(make_repo: Factory) -> None:
    repo = make_repo("repo")
    repo.write("a.py", A)
    first = repo.commit("add")
    repo.write("a.py", A_EDITED)
    repo.commit("modify")
    (repo.root / "a.py").rename(repo.root / "b.py")
    repo.commit("rename")
    middle = lineage_of(repo)
    assert middle.file_logical_ids == {"b.py": lid("a.py", first)}
    (repo.root / "b.py").rename(repo.root / "a.py")
    repo.commit("rename back")
    final = lineage_of(repo)
    assert final.file_logical_ids == {"a.py": lid("a.py", first)}
    assert final.origins["a.py"].origin_path == "a.py"
    assert final.origins["a.py"].introducing_commit == first
    assert not final.supersessions


def test_a_rename_with_an_edit_in_the_same_commit_is_a_new_lineage_with_similarity(
    make_repo: Factory,
) -> None:
    repo = make_repo("repo")
    repo.write("a.py", A)
    first = repo.commit("add")
    (repo.root / "a.py").unlink()
    repo.write("b.py", A_EDITED)
    second = repo.commit("rename and edit")
    result = lineage_of(repo)
    assert result.file_logical_ids == {"b.py": lid("b.py", second)}
    (claim,) = result.supersessions
    assert (claim.old_logical_id, claim.basis) == (lid("a.py", first), "similarity")
    assert 0 < claim.confidence <= identity.SIMILARITY_MAX_CONFIDENCE


def test_delete_then_recreate_is_a_new_lineage_with_path_reuse(make_repo: Factory) -> None:
    repo = make_repo("repo")
    repo.write("a.py", A)
    repo.write("keep.py", OTHER)
    first = repo.commit("add")
    remove(repo, "a.py")
    repo.commit("delete")
    mid = lineage_of(repo)
    assert set(mid.file_logical_ids) == {"keep.py"}
    repo.write("keep2.py", "unrelated\n")
    repo.commit("noise")
    repo.write("a.py", A_EDITED)
    third = repo.commit("recreate")
    result = lineage_of(repo)
    assert result.file_logical_ids["a.py"] == lid("a.py", third) != lid("a.py", first)
    assert result.file_logical_ids["keep.py"] == lid("keep.py", first)
    (claim,) = result.supersessions
    assert (claim.old_logical_id, claim.new_logical_id) == (lid("a.py", first), lid("a.py", third))
    assert (claim.basis, claim.confidence) == ("path_reuse", identity.PATH_REUSE_CONFIDENCE)


def test_a_long_deleted_file_is_never_a_rename_source(make_repo: Factory) -> None:
    repo = make_repo("repo")
    repo.write("gone.py", A)
    repo.write("stay.py", OTHER)
    first = repo.commit("add")
    remove(repo, "gone.py")
    repo.commit("delete")
    repo.write("later.py", A)  # same bytes as the deleted file, added commits later
    third = repo.commit("add the same bytes")
    result = lineage_of(repo)
    assert result.file_logical_ids["later.py"] == lid("later.py", third)
    assert result.file_logical_ids["stay.py"] == lid("stay.py", first)
    assert not result.supersessions


def test_a_copy_is_not_a_rename(make_repo: Factory) -> None:
    repo = make_repo("repo")
    repo.write("a.py", A)
    first = repo.commit("add")
    repo.write("copy.py", A)
    second = repo.commit("copy")
    result = lineage_of(repo)
    assert result.file_logical_ids == {
        "a.py": lid("a.py", first),
        "copy.py": lid("copy.py", second),
    }
    assert not result.supersessions


def test_a_branch_merged_without_fast_forward_is_introduced_by_the_merge(
    make_repo: Factory,
) -> None:
    repo = make_repo("repo")
    repo.write("main.py", A)
    repo.commit("main one")
    repo.git("checkout", "-q", "-b", "side")
    repo.write("side.py", OTHER)
    side_commit = repo.commit("side one")
    repo.git("checkout", "-q", "main")
    repo.write("main2.py", "x = 1\n")
    repo.commit("main two")
    repo.git("merge", "-q", "--no-ff", "side", "-m", "merge side")
    merge = repo.git("rev-parse", "HEAD")
    result = lineage_of(repo)
    assert result.file_logical_ids["side.py"] == lid("side.py", merge)
    assert result.file_logical_ids["side.py"] != lid("side.py", side_commit)
    assert result.origins["side.py"].introducing_commit == merge
    # first-parent only: main one, main two and the merge; the side commit is never walked
    assert result.commits_walked == 3


def test_a_shallow_clone_is_refused(make_repo: Factory, tmp_path: Path) -> None:
    repo = make_repo("repo")
    repo.write("a.py", A)
    repo.commit("one")
    repo.write("a.py", A_EDITED)
    repo.commit("two")
    shallow = tmp_path / "shallow"
    repo.git(
        "-c", "protocol.file.allow=always", "clone", "-q", "--depth", "1",
        f"file://{repo.root}", str(shallow),
    )  # fmt: skip
    assert (shallow / ".git" / "shallow").is_file()
    with pytest.raises(IndexingError) as refused:
        derive_lineage(scan_repository(shallow), REPO)
    assert refused.value.code == "shallow_history"


def test_a_history_over_the_limit_is_refused(make_repo: Factory) -> None:
    repo = make_repo("repo")
    for index in range(3):
        repo.write(f"f{index}.py", f"v = {index}\n")
        repo.commit(f"c{index}")
    assert lineage_of(repo, max_commits=3).commits_walked == 3
    with pytest.raises(IndexingError) as refused:
        lineage_of(repo, max_commits=2)
    assert refused.value.code == "history_too_large"


def test_a_submodule_is_skipped(make_repo: Factory) -> None:
    repo = make_repo("repo")
    repo.write("a.py", A)
    repo.commit("add")
    repo.git("update-index", "--add", "--cacheinfo", f"160000,{'1' * 40},vendor/sub")
    repo.git("commit", "-q", "-m", "gitlink")
    result = lineage_of(repo)
    assert set(result.file_logical_ids) == {"a.py"}


def test_lineage_is_identical_from_scratch_and_incrementally_on_another_machine(
    make_repo: Factory, tmp_path: Path
) -> None:
    repo = make_repo("repo")
    steps: list[Callable[[], None]] = [
        lambda: repo.write("pkg/a.py", A),
        lambda: repo.write("pkg/b.py", OTHER),
        lambda: (repo.root / "pkg" / "a.py").rename(repo.root / "pkg" / "c.py"),
        lambda: remove(repo, "pkg/b.py"),
        lambda: repo.write("pkg/b.py", A_EDITED),
        lambda: repo.write("pkg/c.py", A + "more\n"),
        lambda: (repo.root / "pkg" / "c.py").rename(repo.root / "pkg" / "a.py"),
    ]
    commits: list[str] = []
    incremental: list[Lineage] = []
    for step in steps:
        step()
        commits.append(repo.commit("step"))
        incremental.append(lineage_of(repo))
    final = incremental[-1]

    elsewhere = tmp_path / "elsewhere" / "clone"
    shutil.copytree(repo.root, elsewhere)
    scratch = derive_lineage(scan_repository(elsewhere), REPO)
    assert dict(scratch.file_logical_ids) == dict(final.file_logical_ids)
    assert dict(scratch.origins) == dict(final.origins)
    assert scratch.supersessions == final.supersessions

    scan = scan_repository(repo.root)
    for commit, seen in zip(commits, incremental, strict=True):
        from_there = derive_lineage(scan, REPO, target_commit=commit)
        assert dict(from_there.file_logical_ids) == dict(seen.file_logical_ids)
        assert from_there.supersessions == seen.supersessions
    # a file that is live both at an early commit and at the end keeps its ID
    shared = set(incremental[1].file_logical_ids) & set(final.file_logical_ids)
    assert "pkg/b.py" in shared
    assert final.file_logical_ids["pkg/b.py"] != incremental[1].file_logical_ids["pkg/b.py"]
    assert final.file_logical_ids["pkg/a.py"] == lid("pkg/a.py", commits[0])


def test_a_hostile_repository_config_runs_nothing(make_repo: Factory, tmp_path: Path) -> None:
    repo = make_repo("repo")
    repo.write("a.py", A)
    repo.commit("one")
    repo.write("a.py", A_EDITED)
    (repo.root / ".gitattributes").write_text("*.py diff=evil\n")
    repo.commit("two")
    marker = tmp_path / "ran"
    config = repo.root / ".git" / "config"
    config.write_text(
        config.read_text()
        + f"[diff]\n\texternal = touch {marker}\n[core]\n\tfsmonitor = touch {marker}\n"
        + f"\thooksPath = {tmp_path}\n"
    )
    result = lineage_of(repo)
    assert set(result.file_logical_ids) == {"a.py", ".gitattributes"}
    assert not marker.exists()
