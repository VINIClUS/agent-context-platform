from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from agent_context_platform.indexing import scanner
from agent_context_platform.indexing.scanner import (
    FileKind,
    ScanError,
    ScanFailure,
    ScanLimits,
    SkipReason,
    TruncationReason,
    scan_repository,
)

from .conftest import RepoBuilder

pytestmark = pytest.mark.unit

MakeRepo = Callable[[str], RepoBuilder]
# Which stage refuses a bad config value depends on the git version; the
# property under test is only that the scan fails closed.
FAIL_CLOSED = {ScanFailure.NOT_A_REPOSITORY, ScanFailure.UNSUPPORTED_REPOSITORY}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def by_path(items: object) -> dict[str, object]:
    return {item.path: item for item in items}  # type: ignore[attr-defined]


def failure_of(root: Path, limits: ScanLimits | None = None) -> ScanFailure:
    with pytest.raises(ScanError) as caught:
        scan_repository(root, limits) if limits else scan_repository(root)
    return caught.value.reason


# --- tracked, untracked, ignored -------------------------------------------------


def test_clean_repository_lists_tracked_files_with_content_digests(repo: RepoBuilder) -> None:
    result = scan_repository(repo.root)

    files = by_path(result.files)
    assert sorted(files) == [".gitignore", "README.md", "data.bin", "src/app.py"]
    app = files["src/app.py"]
    assert app.content_sha256 == sha((repo.root / "src/app.py").read_bytes())  # type: ignore[attr-defined]
    assert app.kind is FileKind.FILE  # type: ignore[attr-defined]
    assert app.change is None  # type: ignore[attr-defined]
    assert result.object_format == "sha1"
    assert result.workspace.head_commit == repo.git("rev-parse", "HEAD")
    assert result.workspace.branch == "main"
    assert not result.workspace.detached
    assert not result.workspace.is_dirty
    assert result.workspace.dirty_state_sha256 is None
    assert not result.truncated
    assert result.root == repo.root.resolve()


def test_untracked_files_are_listed_and_ignored_files_are_not(repo: RepoBuilder) -> None:
    repo.write("notes/todo.txt", "todo\n")
    repo.write("debug.log", "ignored by *.log\n")
    repo.write("build/out.o", "ignored by build/\n")

    result = scan_repository(repo.root)

    assert [item.path for item in result.untracked] == ["notes/todo.txt"]
    assert result.untracked[0].content_sha256 == sha(b"todo\n")
    assert result.workspace.untracked_paths == ("notes/todo.txt",)
    assert result.workspace.is_dirty
    assert result.workspace.dirty_state_sha256 is not None
    everything = {item.path for item in result.files} | {item.path for item in result.untracked}
    assert "debug.log" not in everything
    assert "build/out.o" not in everything


def test_binary_file_is_flagged_and_hashed(repo: RepoBuilder) -> None:
    result = scan_repository(repo.root)

    data = by_path(result.files)["data.bin"]
    assert data.is_binary is True  # type: ignore[attr-defined]
    assert data.content_sha256 == sha((repo.root / "data.bin").read_bytes())  # type: ignore[attr-defined]
    assert by_path(result.files)["README.md"].is_binary is False  # type: ignore[attr-defined]


# --- dirty workspace ---------------------------------------------------------------


def test_dirty_state_tracks_modified_deleted_and_staged_files(repo: RepoBuilder) -> None:
    clean = scan_repository(repo.root)
    (repo.root / "README.md").write_text("changed readme\n")
    (repo.root / "data.bin").unlink()
    repo.write("staged.txt", "staged\n")
    repo.git("add", "staged.txt")

    dirty = scan_repository(repo.root)

    files = by_path(dirty.files)
    assert files["README.md"].change == ".M"  # type: ignore[attr-defined]
    assert files["README.md"].content_sha256 == sha(b"changed readme\n")  # type: ignore[attr-defined]
    assert files["data.bin"].change == ".D"  # type: ignore[attr-defined]
    assert files["data.bin"].skipped is SkipReason.MISSING  # type: ignore[attr-defined]
    assert files["staged.txt"].change == "A."  # type: ignore[attr-defined]
    assert dirty.workspace.modified_paths == ("README.md", "data.bin", "staged.txt")
    assert dirty.workspace.is_dirty
    assert dirty.workspace.dirty_state_sha256 not in (None, clean.workspace.dirty_state_sha256)
    assert dirty.scan_sha256 != clean.scan_sha256


def test_dirty_digest_follows_worktree_content(repo: RepoBuilder) -> None:
    (repo.root / "README.md").write_text("first edit\n")
    first = scan_repository(repo.root)
    assert scan_repository(repo.root) == first

    (repo.root / "README.md").write_text("second edit\n")
    second = scan_repository(repo.root)

    assert second.workspace.dirty_state_sha256 != first.workspace.dirty_state_sha256


def test_digest_binds_paths_to_contents(repo: RepoBuilder) -> None:
    repo.write("a.txt", "alpha\n")
    repo.write("b.txt", "bravo\n")
    repo.commit("two files")
    before = scan_repository(repo.root)
    repo.write("a.txt", "bravo\n")
    repo.write("b.txt", "alpha\n")

    after = scan_repository(repo.root)

    assert sorted(f.content_sha256 for f in before.files if f.path in ("a.txt", "b.txt")) == sorted(
        f.content_sha256 for f in after.files if f.path in ("a.txt", "b.txt")
    )
    assert after.scan_sha256 != before.scan_sha256
    assert after.workspace.dirty_state_sha256 is not None


def test_identical_repositories_scan_identically(make_repo: MakeRepo) -> None:
    first = make_repo("one").seed()
    second = make_repo("two").seed()
    for builder in (first, second):
        builder.commit("seed")
        builder.write("z-untracked.txt", "z\n")
        builder.write("a-untracked.txt", "a\n")

    one = scan_repository(first.root)
    two = scan_repository(second.root)

    assert one.scan_sha256 == two.scan_sha256
    assert one.workspace.dirty_state_sha256 == two.workspace.dirty_state_sha256
    assert [item.path for item in one.untracked] == ["a-untracked.txt", "z-untracked.txt"]


def test_detached_head(repo: RepoBuilder) -> None:
    head = repo.git("rev-parse", "HEAD")
    repo.git("checkout", "-q", "--detach", head)

    result = scan_repository(repo.root)

    assert result.workspace.detached
    assert result.workspace.branch is None
    assert result.workspace.head_commit == head


def test_unborn_branch_has_no_head_commit(make_repo: MakeRepo) -> None:
    builder = make_repo("fresh")
    builder.write("only.txt", "x\n")
    builder.git("add", "only.txt")

    result = scan_repository(builder.root)

    assert result.workspace.head_commit is None
    assert result.workspace.branch == "main"
    assert [f.path for f in result.files] == ["only.txt"]
    assert result.files[0].change == "A."


def test_empty_repository_is_clean(make_repo: MakeRepo) -> None:
    result = scan_repository(make_repo("empty").root)

    assert result.files == ()
    assert not result.workspace.is_dirty


def test_sha256_repository(make_repo: MakeRepo) -> None:
    builder = make_repo("sha256")
    builder.root.joinpath(".git").rename(builder.root / "gone")
    subprocess.run(["rm", "-rf", str(builder.root / "gone")], check=True)
    builder.git("init", "-q", "--object-format=sha256")
    builder.write("f.txt", "x\n")
    builder.commit("sha256")
    builder.write("g.txt", "y\n")

    result = scan_repository(builder.root)

    assert result.object_format == "sha256"
    assert len(result.workspace.head_commit or "") == 64
    assert [item.path for item in result.untracked] == ["g.txt"]


def test_linked_worktree(repo: RepoBuilder, tmp_path: Path) -> None:
    linked = tmp_path / "linked"
    repo.git("worktree", "add", "-q", "-b", "side", str(linked))
    (linked / "README.md").write_text("linked edit\n")

    result = scan_repository(linked)

    assert result.workspace.branch == "side"
    assert by_path(result.files)["README.md"].change == ".M"  # type: ignore[attr-defined]


def test_merge_conflict_is_recorded_without_oid(repo: RepoBuilder) -> None:
    repo.git("checkout", "-q", "-b", "other")
    repo.write("README.md", "other side\n")
    repo.commit("other")
    repo.git("checkout", "-q", "main")
    repo.write("README.md", "main side\n")
    repo.commit("main")
    subprocess.run(
        ["git", "merge", "other"], cwd=repo.root, env=repo.env, capture_output=True, check=False
    )

    result = scan_repository(repo.root)

    readme = by_path(result.files)["README.md"]
    assert readme.oid is None  # type: ignore[attr-defined]
    assert readme.change is not None and readme.change.startswith("U")  # type: ignore[attr-defined]
    assert "README.md" in result.workspace.modified_paths


# --- submodules ----------------------------------------------------------------------


def test_gitlinks_are_recorded_with_oid_and_never_recursed(repo: RepoBuilder) -> None:
    fake_oid = "1" * 40
    repo.git("update-index", "--add", "--cacheinfo", f"160000,{fake_oid},vendor/sub")
    (repo.root / "vendor" / "sub").mkdir(parents=True)
    (repo.root / "vendor" / "sub" / "inner.txt").write_text("inside submodule\n")

    result = scan_repository(repo.root)

    link = by_path(result.files)["vendor/sub"]
    assert link.kind is FileKind.GITLINK  # type: ignore[attr-defined]
    assert link.oid == fake_oid  # type: ignore[attr-defined]
    assert link.content_sha256 is None  # type: ignore[attr-defined]
    assert "vendor/sub/inner.txt" not in by_path(result.files)
    assert "vendor/sub" in result.workspace.modified_paths
    assert result.workspace.is_dirty


def test_committed_gitlink_bump_changes_dirty_digest(repo: RepoBuilder) -> None:
    repo.git("update-index", "--add", "--cacheinfo", f"160000,{'1' * 40},sub")
    repo.git("commit", "-q", "-m", "add sub")
    clean = scan_repository(repo.root)
    assert not clean.workspace.is_dirty

    repo.git("update-index", "--cacheinfo", f"160000,{'2' * 40},sub")
    bumped = scan_repository(repo.root)
    repo.git("rm", "-q", "--cached", "sub")
    removed = scan_repository(repo.root)

    assert bumped.workspace.modified_paths == ("sub",)
    assert bumped.workspace.dirty_state_sha256 != removed.workspace.dirty_state_sha256
    assert removed.workspace.modified_paths == ("sub",)


def test_untracked_nested_repository_is_not_entered(repo: RepoBuilder) -> None:
    nested = repo.root / "nested"
    nested.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=nested, env=repo.env, check=True, capture_output=True)
    (nested / "secret.txt").write_text("nested content\n")

    result = scan_repository(repo.root)

    assert [(item.path, item.kind) for item in result.untracked] == [
        ("nested", FileKind.NESTED_REPOSITORY)
    ]


# --- symlinks and path escapes ----------------------------------------------------------


def test_symlink_is_recorded_as_link_and_not_followed(repo: RepoBuilder, tmp_path: Path) -> None:
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"OUTSIDE-SECRET")
    os.symlink(outside, repo.root / "link.txt")
    repo.commit("link")

    result = scan_repository(repo.root)

    link = by_path(result.files)["link.txt"]
    assert link.kind is FileKind.SYMLINK  # type: ignore[attr-defined]
    assert link.mode == "120000"  # type: ignore[attr-defined]
    assert link.link_target == str(outside)  # type: ignore[attr-defined]
    assert link.content_sha256 == sha(str(outside).encode())  # type: ignore[attr-defined]
    assert link.content_sha256 != sha(b"OUTSIDE-SECRET")  # type: ignore[attr-defined]


def test_untracked_symlink_is_a_link_record(repo: RepoBuilder, tmp_path: Path) -> None:
    os.symlink(tmp_path, repo.root / "dirlink")

    result = scan_repository(repo.root)

    (item,) = result.untracked
    assert item.kind is FileKind.SYMLINK
    assert item.content_sha256 == sha(str(tmp_path).encode())


def test_directory_swapped_for_symlink_escapes_and_is_not_read(
    repo: RepoBuilder, tmp_path: Path
) -> None:
    repo.write("pkg/inner.txt", "tracked inner\n")
    repo.commit("pkg")
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (outside / "inner.txt").write_bytes(b"OUTSIDE-SECRET")
    subprocess.run(["rm", "-rf", str(repo.root / "pkg")], check=True)
    os.symlink(outside, repo.root / "pkg")

    result = scan_repository(repo.root)

    inner = by_path(result.files)["pkg/inner.txt"]
    assert inner.skipped is SkipReason.ESCAPES_REPOSITORY  # type: ignore[attr-defined]
    assert inner.content_sha256 is None  # type: ignore[attr-defined]
    assert sha(b"OUTSIDE-SECRET") not in repr(result)


def test_directory_replaced_by_file_is_missing(repo: RepoBuilder) -> None:
    subprocess.run(["rm", "-rf", str(repo.root / "src")], check=True)
    repo.write("src", "now a file\n")

    result = scan_repository(repo.root)

    assert by_path(result.files)["src/app.py"].skipped is SkipReason.MISSING  # type: ignore[attr-defined]


def test_special_file_is_never_read(repo: RepoBuilder) -> None:
    (repo.root / "README.md").unlink()
    os.mkfifo(repo.root / "README.md")

    result = scan_repository(repo.root)

    readme = by_path(result.files)["README.md"]
    assert readme.skipped is SkipReason.UNREADABLE  # type: ignore[attr-defined]
    assert readme.kind is FileKind.FILE  # type: ignore[attr-defined]


def test_unsafe_paths_are_rejected(repo: RepoBuilder) -> None:
    repo.write("back\\slash.txt", "x\n")
    (repo.root / os.fsdecode(b"bad-\xff-name")).write_text("x\n")
    repo.write("ok.txt", "x\n")

    result = scan_repository(repo.root)

    assert [item.path for item in result.untracked] == ["ok.txt"]
    assert {item.path for item in result.rejections} == {"back\\\\slash.txt", None}
    assert {item.reason for item in result.rejections} == {"unsafe_path"}


@pytest.mark.parametrize(
    ("path", "safe"),
    [
        ("a/b.txt", True),
        ("a/.gitignore", True),
        ("", False),
        ("/abs", False),
        ("../up", False),
        ("a/../b", False),
        ("a//b", False),
        ("./a", False),
        (".git/config", False),
        ("sub/.GIT/hooks", False),
        ("a\nb", False),
        ("a\\b", False),
    ],
)
def test_is_safe_path(path: str, safe: bool) -> None:
    assert scanner._is_safe_path(path) is safe


# --- bounds -----------------------------------------------------------------------------


def test_max_files_truncates_after_canonical_sort(repo: RepoBuilder) -> None:
    repo.write("zzz.txt", "z\n")
    repo.write("aaa-untracked.txt", "a\n")

    result = scan_repository(repo.root, ScanLimits(max_files=3))

    assert [f.path for f in result.files] == [".gitignore", "README.md", "data.bin"]
    assert result.untracked == ()
    assert result.truncated
    assert result.truncation_reasons == (TruncationReason.MAX_FILES,)
    assert result.omitted_files == 3
    assert "aaa-untracked.txt" in result.workspace.untracked_paths
    full = scan_repository(repo.root)
    assert full.scan_sha256 != result.scan_sha256
    assert scan_repository(repo.root, ScanLimits(max_files=3)) == result


def test_truncation_changes_dirty_digest(repo: RepoBuilder) -> None:
    repo.write("a-untracked.txt", "a\n")
    limited = scan_repository(repo.root, ScanLimits(max_files=4))
    full = scan_repository(repo.root)

    assert limited.workspace.dirty_state_sha256 != full.workspace.dirty_state_sha256
    assert limited.workspace.untracked_paths == full.workspace.untracked_paths


def test_oversized_file_is_skipped_and_reported(repo: RepoBuilder) -> None:
    repo.write("big.txt", b"x" * 100)

    result = scan_repository(repo.root, ScanLimits(max_file_bytes=50))

    big = by_path(result.untracked)["big.txt"]
    assert big.skipped is SkipReason.TOO_LARGE  # type: ignore[attr-defined]
    assert big.size == 100  # type: ignore[attr-defined]
    assert big.content_sha256 is None  # type: ignore[attr-defined]
    assert result.truncated
    assert TruncationReason.MAX_FILE_BYTES in result.truncation_reasons


def test_total_byte_budget_is_a_hard_ceiling(repo: RepoBuilder) -> None:
    repo.write("a.txt", b"a" * 40)
    repo.write("b.txt", b"b" * 40)
    repo.write("c.txt", b"c" * 40)

    result = scan_repository(repo.root, ScanLimits(max_total_bytes=100, max_file_bytes=1000))

    assert result.bytes_read <= 100
    skipped = [item for item in result.untracked if item.skipped is SkipReason.BUDGET_EXHAUSTED]
    assert skipped
    assert TruncationReason.MAX_TOTAL_BYTES in result.truncation_reasons
    assert result.truncated


def test_file_growing_past_cap_is_not_truncated(monkeypatch: pytest.MonkeyPatch) -> None:
    chunks = [b"x" * 30, b"x" * 30, b""]

    def fake_read(fd: int, size: int) -> bytes:
        return chunks.pop(0)

    class Info:
        st_mode = 0o100644
        st_size = 10
        st_dev = 1
        st_ino = 2

    monkeypatch.setattr(scanner.os, "stat", lambda *a, **k: Info())
    monkeypatch.setattr(scanner.os, "open", lambda *a, **k: 99)
    monkeypatch.setattr(scanner.os, "fstat", lambda fd: Info())
    monkeypatch.setattr(scanner.os, "close", lambda fd: None)
    monkeypatch.setattr(scanner.os, "read", fake_read)

    seen = scanner._observe_leaf(3, "f", file_cap=50, budget=1000)

    assert seen.skipped is SkipReason.TOO_LARGE
    assert seen.sha256 is None


def test_command_output_over_cap_is_an_error(repo: RepoBuilder) -> None:
    assert failure_of(repo.root, ScanLimits(max_command_output_bytes=10)) is (
        ScanFailure.OUTPUT_TOO_LARGE
    )


def test_command_timeout_is_an_error(repo: RepoBuilder) -> None:
    assert failure_of(repo.root, ScanLimits(timeout_seconds=1e-9)) is ScanFailure.TIMED_OUT


def test_hanging_command_is_killed(repo: RepoBuilder, tmp_path: Path) -> None:
    slow = tmp_path / "bin"
    slow.mkdir()
    fake = slow / "git"
    fake.write_text("#!/bin/sh\nexec sleep 30\n")
    fake.chmod(0o755)
    limits = ScanLimits(timeout_seconds=0.3)

    with pytest.raises(ScanError) as caught:
        scanner._run_git(str(fake), ["status"], cwd=repo.root, env={}, limits=limits, max_bytes=10)

    assert caught.value.reason is ScanFailure.TIMED_OUT


def test_limits_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_files"):
        ScanLimits(max_files=0)


# --- failures ------------------------------------------------------------------------------


def test_missing_root(tmp_path: Path) -> None:
    assert failure_of(tmp_path / "absent") is ScanFailure.NOT_A_REPOSITORY


def test_file_root(tmp_path: Path) -> None:
    (tmp_path / "f").write_text("x")
    assert failure_of(tmp_path / "f") is ScanFailure.NOT_A_REPOSITORY


def test_directory_without_repository(tmp_path: Path) -> None:
    assert failure_of(tmp_path) is ScanFailure.NOT_A_REPOSITORY


def test_subdirectory_of_repository_is_not_a_root(repo: RepoBuilder) -> None:
    assert failure_of(repo.root / "src") is ScanFailure.NOT_A_REPOSITORY


def test_broken_git_directory(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    assert failure_of(tmp_path) is ScanFailure.NOT_A_REPOSITORY


def test_symlinked_dot_git_is_refused(repo: RepoBuilder, tmp_path: Path) -> None:
    moved = tmp_path / "moved-git"
    (repo.root / ".git").rename(moved)
    os.symlink(moved, repo.root / ".git")

    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_core_worktree_cannot_redirect_the_scan(repo: RepoBuilder, tmp_path: Path) -> None:
    other = tmp_path / "other-tree"
    other.mkdir()
    repo.git("config", "core.worktree", str(other))

    assert failure_of(repo.root) is ScanFailure.ROOT_MISMATCH


def test_forged_gitfile_cannot_borrow_another_repository(
    repo: RepoBuilder, make_repo: MakeRepo, tmp_path: Path
) -> None:
    victim = make_repo("victim")
    victim.write("secret.txt", "victim tree\n")
    victim.commit("victim")
    forged = tmp_path / "forged"
    forged.mkdir()
    (forged / ".git").write_text(f"gitdir: {victim.root / '.git'}\n")

    assert failure_of(forged) is ScanFailure.ROOT_MISMATCH


def test_linked_worktree_with_foreign_commondir_is_refused(
    repo: RepoBuilder, make_repo: MakeRepo, tmp_path: Path
) -> None:
    victim = make_repo("victim-common")
    victim.write("secret.txt", "victim tree\n")
    victim_head = victim.commit("victim")
    linked = tmp_path / "linked-forged"
    repo.git("worktree", "add", "-q", "-b", "forged", str(linked))
    admin = repo.root / ".git" / "worktrees" / "linked-forged"
    assert (admin / "gitdir").is_file()  # the backlink is valid
    (admin / "commondir").write_text(f"{victim.root / '.git'}\n")

    with pytest.raises(ScanError) as caught:
        scan_repository(linked)

    assert caught.value.reason is ScanFailure.ROOT_MISMATCH
    assert victim_head not in repr(caught.value)


def test_ordinary_repository_with_foreign_commondir_is_refused(
    repo: RepoBuilder, make_repo: MakeRepo
) -> None:
    victim = make_repo("victim-plain")
    victim.write("secret.txt", "victim tree\n")
    victim.commit("victim")
    (repo.root / ".git" / "commondir").write_text(f"{victim.root / '.git'}\n")

    assert failure_of(repo.root) in {ScanFailure.ROOT_MISMATCH, *FAIL_CLOSED}


def test_linked_worktree_admin_dir_must_belong_to_its_common_dir(
    repo: RepoBuilder, tmp_path: Path
) -> None:
    linked = tmp_path / "linked-real"
    repo.git("worktree", "add", "-q", "-b", "real", str(linked))
    admin = repo.root / ".git" / "worktrees" / "linked-real"
    common = repo.root / ".git"

    assert scanner._is_admin_dir_of(common, admin)
    assert not scanner._is_admin_dir_of(tmp_path, admin)
    assert not scanner._is_admin_dir_of(common, common)
    assert not scanner._is_admin_dir_of(common, common / "worktrees" / "absent")


def test_submodule_style_checkout_is_accepted(repo: RepoBuilder, tmp_path: Path) -> None:
    modules = tmp_path / "modules-store"
    checkout = tmp_path / "sub-checkout"
    repo.git("init", "-q", f"--separate-git-dir={modules}", str(checkout))
    repo.git("config", "core.worktree", str(checkout), cwd=checkout)
    (checkout / "f.txt").write_text("x\n")

    result = scan_repository(checkout)

    assert [item.path for item in result.untracked] == ["f.txt"]


def test_unreadable_worktree_backlink_is_refused(repo: RepoBuilder, tmp_path: Path) -> None:
    other = tmp_path / "other-git"
    other.mkdir()
    (other / "gitdir").write_bytes(b"\xff\xfe")

    assert not scanner._owns_git_dir("git", repo.root, other, other, ScanLimits())


def test_include_in_repository_config_is_refused(repo: RepoBuilder) -> None:
    repo.git("config", "include.path", "../evil.cfg")
    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_unknown_extension_is_refused(repo: RepoBuilder) -> None:
    repo.git("config", "core.repositoryformatversion", "1")
    repo.git("config", "extensions.partialclone", "origin")
    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_unknown_format_version_is_refused(repo: RepoBuilder) -> None:
    repo.git("config", "core.repositoryformatversion", "9")
    assert failure_of(repo.root) in FAIL_CLOSED


def test_split_index_is_refused(repo: RepoBuilder) -> None:
    repo.git("update-index", "--split-index")
    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_bad_boolean_config_is_refused(repo: RepoBuilder) -> None:
    repo.git("config", "core.filemode", "maybe")
    assert failure_of(repo.root) in FAIL_CLOSED


@pytest.mark.parametrize(
    ("key", "value", "rejected"),
    [
        ("core.autocrlf", "bogus", True),
        ("core.eol", "bogus", True),
        ("core.autocrlf", "input", False),
    ],
)
def test_conversion_settings_are_validated(
    repo: RepoBuilder, key: str, value: str, rejected: bool
) -> None:
    repo.git("config", key, value)
    if rejected:
        assert failure_of(repo.root) in FAIL_CLOSED
    else:
        assert scan_repository(repo.root).files


def test_allowlisted_config_is_carried_into_the_view(repo: RepoBuilder) -> None:
    repo.git("config", "core.autocrlf", "true")
    repo.git("config", "core.eol", "lf")
    repo.git("config", "core.filemode", "yes")
    assert not scan_repository(repo.root).workspace.is_dirty


def test_git_missing(repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    empty = tmp_path / "empty-path"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    assert failure_of(repo.root) is ScanFailure.GIT_UNAVAILABLE


def test_git_inside_repository_is_refused(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    trap = repo.root / "bin"
    trap.mkdir()
    fake = trap / "git"
    marker = tmp_path / "fake-git-ran"
    fake.write_text(f"#!/bin/sh\ntouch {marker}\n")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", str(trap))
    assert failure_of(repo.root) is ScanFailure.GIT_UNAVAILABLE
    assert not marker.exists()


def test_unsupported_platform(repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scanner.os, "name", "nt")
    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_PLATFORM


def test_popen_failures_are_typed(repo: RepoBuilder, tmp_path: Path) -> None:
    limits = ScanLimits()
    with pytest.raises(ScanError) as missing:
        scanner._run_git(
            str(tmp_path / "no-such-git"), [], cwd=repo.root, env={}, limits=limits, max_bytes=9
        )
    with pytest.raises(ScanError) as unrunnable:
        scanner._run_git(str(tmp_path), [], cwd=repo.root, env={}, limits=limits, max_bytes=9)
    assert missing.value.reason is ScanFailure.GIT_UNAVAILABLE
    assert unrunnable.value.reason is ScanFailure.GIT_FAILED


def test_nonzero_exit_is_typed(repo: RepoBuilder) -> None:
    git = scanner._resolve_git(repo.root)
    with pytest.raises(ScanError) as caught:
        scanner._run_git(
            git,
            ["rev-parse", "--verify", "no-such-ref"],
            cwd=repo.root,
            env=scanner._base_env(git, repo.root),
            limits=ScanLimits(),
            max_bytes=999,
        )
    assert caught.value.reason is ScanFailure.GIT_FAILED


def test_pinned_root_must_match_identity(repo: RepoBuilder, tmp_path: Path) -> None:
    info = os.stat(repo.root)
    with pytest.raises(ScanError) as wrong:
        scanner._open_pinned_root(repo.root.resolve(), (info.st_dev, info.st_ino + 1))
    with pytest.raises(ScanError) as gone:
        scanner._open_pinned_root(tmp_path / "gone", (0, 0))
    assert wrong.value.reason is ScanFailure.ROOT_MISMATCH
    assert gone.value.reason is ScanFailure.ROOT_MISMATCH
    os.close(scanner._open_pinned_root(repo.root.resolve(), (info.st_dev, info.st_ino)))


# --- parsers ---------------------------------------------------------------------------------


def test_status_parser_handles_every_record_kind() -> None:
    oid = "a" * 40
    zero = "0" * 40
    raw = b"\x00".join(
        [
            f"1 .M N... 100644 100644 100644 {oid} {oid} one.txt".encode(),
            f"2 R. N... 100644 100644 100644 {oid} {oid} R100 new.txt".encode(),
            b"old.txt",
            f"u UU N... 100644 100644 100644 100644 {oid} {oid} {oid} both.txt".encode(),
            f"1 A. N... 000000 100644 100644 {zero} {oid} added.txt".encode(),
            b"# branch.head main",
            b"! ignored.log",
            b"? new-file.txt",
            b"? nested/",
            b"",
        ]
    )
    rejections: list[scanner.Rejection] = []

    changes, untracked = scanner._parse_status(raw, rejections)

    assert sorted(changes) == ["added.txt", "both.txt", "new.txt", "one.txt"]
    assert changes["added.txt"].base_oid is None
    assert changes["both.txt"].modes == ("100644", "100644", "100644", "100644")
    assert changes["new.txt"].index_oid == oid
    assert untracked == [("new-file.txt", False), ("nested", True)]
    assert rejections == []


@pytest.mark.parametrize(
    "raw",
    [b"1 .M short", b"x unknown", b"1 .M N... 100644 100644 100644 " + b"a" * 40],
)
def test_status_parser_rejects_malformed_output(raw: bytes) -> None:
    with pytest.raises(ScanError) as caught:
        scanner._parse_status(raw, [])
    assert caught.value.reason is ScanFailure.MALFORMED_OUTPUT


def test_status_parser_rejects_unsafe_tracked_path() -> None:
    oid = "a" * 40
    raw = f"1 .M N... 100644 100644 100644 {oid} {oid} ../up".encode()
    rejections: list[scanner.Rejection] = []
    changes, _ = scanner._parse_status(raw, rejections)
    assert changes == {}
    assert [item.path for item in rejections] == ["../up"]


@pytest.mark.parametrize("raw", [b"100644 x 0 a", b"100644 " + b"a" * 40 + b" 0"])
def test_index_parser_rejects_malformed_output(raw: bytes) -> None:
    with pytest.raises(ScanError) as caught:
        scanner._parse_index(raw, [])
    assert caught.value.reason is ScanFailure.MALFORMED_OUTPUT


def test_index_parser_rejects_unsafe_paths() -> None:
    oid = "a" * 40
    rejections: list[scanner.Rejection] = []
    entries = scanner._parse_index(f"100644 {oid} 0\t../x".encode(), rejections)
    assert entries == {}
    assert len(rejections) == 1


def test_head_tree_parser() -> None:
    oid = "b" * 40
    raw = b"\x00".join(
        [f"160000 commit {oid}\tsub".encode(), f"100644 blob {oid}\tfile".encode(), b""]
    )
    assert scanner._parse_head_gitlinks(raw) == {"sub": oid}
    with pytest.raises(ScanError):
        scanner._parse_head_gitlinks(b"garbage")


# --- hostile repository ---------------------------------------------------------------------


def _hostile_repo(make_repo: MakeRepo, tmp_path: Path) -> tuple[RepoBuilder, Path]:
    markers = tmp_path / "markers"
    markers.mkdir()
    script = tmp_path / "evil.sh"
    script.write_text(f'#!/bin/sh\n: > "{markers}/$1"\ncat 2>/dev/null\nexit 0\n')
    builder = make_repo("hostile")
    builder.write("a.txt", "committed\n")
    builder.write(".gitattributes", "* filter=evil\n")
    builder.commit("hostile fixture")
    with (builder.root / ".git" / "config").open("a") as config:
        config.write(
            f'[filter "evil"]\n\tclean = sh {script} clean\n\tsmudge = sh {script} smudge\n'
            f"[core]\n\tfsmonitor = sh {script} fsmonitor\n\thooksPath = {tmp_path}/hooks\n"
            f"[diff]\n\texternal = sh {script} diff\n"
        )
    builder.write("a.txt", "modified with a different length\n")
    builder.write("untracked.txt", "untracked\n")
    return builder, markers


def test_hostile_repository_config_never_executes(make_repo: MakeRepo, tmp_path: Path) -> None:
    builder, markers = _hostile_repo(make_repo, tmp_path)

    # Positive control: without the scanner's protections these commands fire
    # every hook, so an empty marker directory below is meaningful.
    for args in (["status", "--porcelain"], ["diff"]):
        subprocess.run(
            ["git", *args], cwd=builder.root, env=builder.env, capture_output=True, check=False
        )
    assert {path.name for path in markers.iterdir()} >= {"clean", "fsmonitor", "diff"}
    for marker in markers.iterdir():
        marker.unlink()

    result = scan_repository(builder.root)

    assert list(markers.iterdir()) == []
    assert by_path(result.files)["a.txt"].content_sha256 == sha(  # type: ignore[attr-defined]
        b"modified with a different length\n"
    )
    assert [item.path for item in result.untracked] == ["untracked.txt"]
    assert result.workspace.modified_paths == ("a.txt",)


def test_hostile_info_attributes_are_inert(make_repo: MakeRepo, tmp_path: Path) -> None:
    builder, markers = _hostile_repo(make_repo, tmp_path)
    info = builder.root / ".git" / "info"
    info.mkdir(exist_ok=True)
    (info / "attributes").write_text("* filter=evil diff=evil\n")
    (info / "exclude").write_text("ignored-here.txt\n")
    builder.write("ignored-here.txt", "x\n")

    result = scan_repository(builder.root)

    assert list(markers.iterdir()) == []
    assert [item.path for item in result.untracked] == ["untracked.txt"]


def test_info_files_that_are_links_or_huge_are_refused(repo: RepoBuilder, tmp_path: Path) -> None:
    info = repo.root / ".git" / "info"
    info.mkdir(exist_ok=True)
    target = tmp_path / "target"
    target.write_text("*.tmp\n")
    (info / "exclude").unlink(missing_ok=True)
    os.symlink(target, info / "exclude")
    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY
    (info / "exclude").unlink()
    (info / "exclude").write_bytes(b"x" * (scanner._INFO_COPY_CAP + 1))
    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY
    (info / "exclude").unlink()
    (info / "exclude").mkdir()
    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


# --- content-free output ----------------------------------------------------------------------

CANARY = "CANARY-9d41c7e2-not-for-logs"


def test_file_content_never_reaches_logs_reprs_or_errors(
    repo: RepoBuilder, caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    repo.write("secret.txt", f"token = {CANARY}\n")
    repo.write("staged.txt", f"staged {CANARY}\n")
    repo.git("add", "staged.txt")
    os.symlink("secret.txt", repo.root / "alias")
    caplog.set_level(logging.DEBUG)

    result = scan_repository(repo.root)
    with pytest.raises(ScanError) as caught:
        scan_repository(repo.root, ScanLimits(max_command_output_bytes=1))

    rendered = "\n".join(
        [
            caplog.text,
            repr(result),
            str(result),
            repr(caught.value),
            str(caught.value),
            "".join(traceback.format_exception(caught.value)),
        ]
    )
    assert CANARY not in rendered
    assert sha(f"token = {CANARY}\n".encode()) in repr(result)
    assert any(record.name == scanner.__name__ for record in caplog.records)


def test_discarded_growing_read_is_charged_to_the_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    chunks = [b"x" * 30, b"x" * 30, b""]

    class Info:
        st_mode = 0o100644
        st_size = 10
        st_dev = 1
        st_ino = 2

    monkeypatch.setattr(scanner.os, "stat", lambda *a, **k: Info())
    monkeypatch.setattr(scanner.os, "open", lambda *a, **k: 99)
    monkeypatch.setattr(scanner.os, "fstat", lambda fd: Info())
    monkeypatch.setattr(scanner.os, "close", lambda fd: None)
    monkeypatch.setattr(scanner.os, "read", lambda fd, size: chunks.pop(0))

    seen = scanner._observe_leaf(3, "f", file_cap=50, budget=1000)

    assert seen.skipped is SkipReason.TOO_LARGE
    assert seen.consumed == 60


def test_growing_file_bytes_count_toward_total(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo.write("grow.txt", b"x" * 10)
    real = scanner._observe

    def grown(
        root_fd: int, path: str, *, file_cap: int, budget: int, object_format: str
    ) -> scanner._Observed:
        if path == "grow.txt":
            return scanner._Observed(
                FileKind.FILE, 9_000, None, None, None, SkipReason.TOO_LARGE, consumed=9_000
            )
        return real(root_fd, path, file_cap=file_cap, budget=budget, object_format=object_format)

    monkeypatch.setattr(scanner, "_observe", grown)

    result = scanner.scan_repository(repo.root, ScanLimits(max_total_bytes=10_000))

    assert result.bytes_read >= 9_000


def test_rejected_status_entries_keep_workspace_dirty(repo: RepoBuilder) -> None:
    repo.write("back\\slash.txt", "one\n")
    repo.commit("backslash")
    clean = scan_repository(repo.root)
    assert not clean.workspace.is_dirty
    repo.write("back\\slash.txt", "changed and longer\n")

    dirty = scan_repository(repo.root)

    assert dirty.workspace.is_dirty
    assert dirty.workspace.dirty_state_sha256 is not None
    assert "slash" not in repr(dirty.workspace)
    assert dirty.workspace.modified_paths == ()


def test_only_rejected_untracked_path_is_dirty(repo: RepoBuilder) -> None:
    (repo.root / os.fsdecode(b"bad-\xff-name")).write_text("x\n")

    result = scan_repository(repo.root)

    assert result.workspace.is_dirty
    assert result.workspace.dirty_state_sha256 is not None


def test_omitted_tracked_paths_are_bound_into_scan_digest(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    limits = ScanLimits(max_files=2)
    before = scan_repository(repo.root, limits)
    real = scanner._parse_index

    def other_oid_for_omitted(raw: bytes, rejections: list[scanner.Rejection]) -> object:
        entries = real(raw, rejections)
        entries["src/app.py"] = scanner._IndexEntry("100644", "e" * 40, False)
        return entries

    monkeypatch.setattr(scanner, "_parse_index", other_oid_for_omitted)
    after = scan_repository(repo.root, limits)

    assert before.workspace == after.workspace
    assert [f.path for f in before.files] == [f.path for f in after.files]
    assert before.omitted_files == after.omitted_files == 2
    assert before.scan_sha256 != after.scan_sha256


class _FileInfo:
    st_mode = 0o100644
    st_size = 1
    st_dev = 1
    st_ino = 2


def _fake_file(monkeypatch: pytest.MonkeyPatch, read: Callable[[int, int], bytes]) -> None:
    monkeypatch.setattr(scanner.os, "stat", lambda *a, **k: _FileInfo())
    monkeypatch.setattr(scanner.os, "open", lambda *a, **k: 99)
    monkeypatch.setattr(scanner.os, "fstat", lambda fd: _FileInfo())
    monkeypatch.setattr(scanner.os, "close", lambda fd: None)
    monkeypatch.setattr(scanner.os, "read", read)


def test_read_error_after_partial_read_charges_consumed_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = [b"x" * 20]

    def read(fd: int, size: int) -> bytes:
        if calls:
            return calls.pop()
        raise OSError

    _fake_file(monkeypatch, read)

    seen = scanner._observe_leaf(3, "f", file_cap=100, budget=100)

    assert seen.skipped is SkipReason.UNREADABLE
    assert seen.consumed == 20


def test_read_requests_never_exceed_remaining_allowance(monkeypatch: pytest.MonkeyPatch) -> None:
    requested: list[int] = []

    def read(fd: int, size: int) -> bytes:
        requested.append(size)
        return b"x" * size

    _fake_file(monkeypatch, read)

    seen = scanner._observe_leaf(3, "f", file_cap=100, budget=7)

    assert seen.skipped is SkipReason.BUDGET_EXHAUSTED
    assert sum(requested) <= 8
    assert seen.consumed <= 7


def test_growing_file_with_little_budget_left_never_exceeds_total(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo.write("grow.txt", b"x")
    real_read = os.read
    real_open = os.open
    worktree_fds: set[int] = set()

    def tracking_open(path: str, flags: int, *args: object, **kwargs: object) -> int:
        fd = real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]
        if path == "grow.txt" and flags & os.O_NONBLOCK:
            worktree_fds.add(fd)
        return fd

    def greedy_read(fd: int, size: int) -> bytes:
        # A worktree file that keeps growing: always more to give than asked.
        data = real_read(fd, size)
        return data or b"y" * size if fd in worktree_fds else data

    monkeypatch.setattr(scanner.os, "open", tracking_open)
    monkeypatch.setattr(scanner.os, "read", greedy_read)
    limits = ScanLimits(max_total_bytes=500, max_file_bytes=1_000_000)

    result = scan_repository(repo.root, limits)

    assert result.bytes_read <= limits.max_total_bytes
    assert TruncationReason.MAX_TOTAL_BYTES in result.truncation_reasons


def _linked(repo: RepoBuilder, tmp_path: Path, name: str) -> tuple[Path, Path]:
    linked = tmp_path / name
    repo.git("worktree", "add", "-q", "-b", name, str(linked))
    return linked, repo.root / ".git" / "worktrees" / name


def test_symlinked_worktree_backlink_is_refused(repo: RepoBuilder, tmp_path: Path) -> None:
    linked, admin = _linked(repo, tmp_path, "sym-backlink")
    real = tmp_path / "elsewhere-gitdir"
    real.write_text(str(linked / ".git"))
    (admin / "gitdir").unlink()
    (admin / "gitdir").symlink_to(real)

    assert failure_of(linked) is ScanFailure.ROOT_MISMATCH


def test_oversized_worktree_backlink_is_refused_without_reading_past_cap(
    repo: RepoBuilder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    linked, admin = _linked(repo, tmp_path, "big-backlink")
    (admin / "gitdir").write_bytes(str(linked / ".git").encode() + b"\n" + b"x" * 200_000)
    asked: list[int] = []
    real_read = os.read

    def spy(fd: int, size: int) -> bytes:
        asked.append(size)
        return real_read(fd, size)

    monkeypatch.setattr(scanner.os, "read", spy)

    assert failure_of(linked) is ScanFailure.ROOT_MISMATCH
    assert scanner._METADATA_FILE_CAP + 1 in asked


@pytest.mark.parametrize("name", ["commondir", "HEAD"])
def test_symlinked_pointer_files_are_refused(repo: RepoBuilder, tmp_path: Path, name: str) -> None:
    linked, admin = _linked(repo, tmp_path, f"sym-{name.lower()}")
    target = admin / name
    copy = tmp_path / f"copy-{name}"
    copy.write_bytes(target.read_bytes())
    target.unlink()
    target.symlink_to(copy)

    # Git's own bootstrap may reject a symlinked HEAD first; either way it fails closed.
    assert failure_of(linked) in {ScanFailure.ROOT_MISMATCH, *FAIL_CLOSED}


def test_read_metadata_is_bounded_and_content_free(tmp_path: Path) -> None:
    ok = tmp_path / "ok"
    ok.write_bytes(b"a" * scanner._METADATA_FILE_CAP)
    big = tmp_path / "big"
    big.write_bytes(b"secret-" * 5000)
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)

    assert scanner._read_metadata(ok) == b"a" * scanner._METADATA_FILE_CAP
    assert scanner._read_metadata(tmp_path / "absent") is None
    for bad in (big, fifo):
        with pytest.raises(ScanError) as caught:
            scanner._read_metadata(bad)
        assert "secret" not in repr(caught.value)


def test_file_edited_between_status_and_read_is_dirty(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert not scan_repository(repo.root).workspace.is_dirty
    real = scanner._run_in_private_view

    def edit_after_status(*args: Any, **kwargs: Any) -> Any:
        output = real(*args, **kwargs)
        (repo.root / "README.md").write_bytes(b"edited after status\n")
        return output

    monkeypatch.setattr(scanner, "_run_in_private_view", edit_after_status)

    result = scan_repository(repo.root)

    assert result.workspace.is_dirty
    assert "README.md" in result.workspace.modified_paths
    assert result.workspace.dirty_state_sha256 is not None


def test_file_deleted_between_status_and_read_is_dirty(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = scanner._run_in_private_view

    def delete_after_status(*args: Any, **kwargs: Any) -> Any:
        output = real(*args, **kwargs)
        (repo.root / "README.md").unlink()
        return output

    monkeypatch.setattr(scanner, "_run_in_private_view", delete_after_status)

    assert "README.md" in scan_repository(repo.root).workspace.modified_paths


def test_assume_unchanged_edit_is_not_flagged_by_the_blob_check(repo: RepoBuilder) -> None:
    repo.git("update-index", "--assume-unchanged", "README.md")
    repo.write("README.md", "edited but hidden by the flag\n")

    assert "README.md" not in scan_repository(repo.root).workspace.modified_paths


def test_clean_repository_has_no_blob_mismatch(repo: RepoBuilder) -> None:
    assert not scan_repository(repo.root).workspace.is_dirty


def test_blob_oid_matches_git_for_sha256_repository(tmp_path: Path) -> None:
    data = b"hello\n"
    header = b"blob 6\0"
    hasher = scanner._blob_hasher("sha256", len(data))
    hasher.update(data)
    assert hasher.hexdigest() == hashlib.sha256(header + data).hexdigest()
    sha1 = scanner._blob_hasher("sha1", len(data))
    sha1.update(data)
    assert sha1.hexdigest() == "ce013625030ba8dba906f756967f9e9ca394464a"


def test_many_long_symlinks_never_exceed_total_budget(repo: RepoBuilder) -> None:
    for index in range(30):
        os.symlink("t" * 60 + str(index), repo.root / f"link{index:02d}")
    limits = ScanLimits(max_total_bytes=200)

    result = scan_repository(repo.root, limits)

    assert 0 < result.bytes_read <= limits.max_total_bytes
    assert TruncationReason.MAX_TOTAL_BYTES in result.truncation_reasons


def test_skipped_symlink_is_charged_up_to_the_budget(tmp_path: Path) -> None:
    root = tmp_path / "w"
    root.mkdir()
    os.symlink("t" * 80, root / "l")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        over_file = scanner._observe_leaf(fd, "l", file_cap=10, budget=1000)
        over_budget = scanner._observe_leaf(fd, "l", file_cap=1000, budget=50)
        ok = scanner._observe_leaf(fd, "l", file_cap=1000, budget=1000)
    finally:
        os.close(fd)

    assert over_file.skipped is SkipReason.TOO_LARGE and over_file.consumed == 80
    assert over_budget.skipped is SkipReason.BUDGET_EXHAUSTED and over_budget.consumed == 50
    assert ok.skipped is None and ok.consumed == 80


def test_symlinked_index_is_refused_and_leaks_nothing(
    repo: RepoBuilder, make_repo: MakeRepo
) -> None:
    victim = make_repo("victim-index")
    victim.write("victim-secret-path.txt", "victim tree\n")
    victim.commit("victim")
    index = repo.root / ".git" / "index"
    index.unlink()
    index.symlink_to(victim.root / ".git" / "index")

    with pytest.raises(ScanError) as caught:
        scan_repository(repo.root)

    assert caught.value.reason is ScanFailure.ROOT_MISMATCH
    assert "victim-secret-path" not in repr(caught.value)


def test_symlinked_linked_worktree_index_is_refused(
    repo: RepoBuilder, make_repo: MakeRepo, tmp_path: Path
) -> None:
    victim = make_repo("victim-linked-index")
    victim.write("victim-secret-path.txt", "victim tree\n")
    victim.commit("victim")
    linked, admin = _linked(repo, tmp_path, "linked-index")
    (admin / "index").unlink()
    (admin / "index").symlink_to(victim.root / ".git" / "index")

    assert failure_of(linked) is ScanFailure.ROOT_MISMATCH


def test_scan_uses_a_private_index_copy(repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []
    real = scanner._run_git

    def spy(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["env"].get("GIT_INDEX_FILE", ""))
        return real(*args, **kwargs)

    monkeypatch.setattr(scanner, "_run_git", spy)

    scan_repository(repo.root)

    private = [value for value in seen if value]
    assert private
    assert all(str(repo.root) not in value for value in private)


def test_oversized_index_is_refused(repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scanner, "_INDEX_COPY_CAP", 16)

    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_alternates_to_a_foreign_object_store_are_refused(
    repo: RepoBuilder, make_repo: MakeRepo
) -> None:
    victim = make_repo("victim-alt")
    victim.write("victim-secret-path.txt", "victim tree\n")
    victim.commit("victim")
    info = repo.root / ".git" / "objects" / "info"
    info.mkdir(exist_ok=True)
    (info / "alternates").write_text(f"{victim.root / '.git' / 'objects'}\n")

    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_symlinked_objects_directory_is_refused(
    repo: RepoBuilder, make_repo: MakeRepo, tmp_path: Path
) -> None:
    moved = tmp_path / "moved-objects"
    (repo.root / ".git" / "objects").rename(moved)
    (repo.root / ".git" / "objects").symlink_to(moved)

    assert failure_of(repo.root) in {ScanFailure.ROOT_MISMATCH, *FAIL_CLOSED}


@pytest.mark.parametrize("name", ["packed-refs", "shallow", "config"])
def test_symlinked_ref_and_config_files_are_refused(
    repo: RepoBuilder, tmp_path: Path, name: str
) -> None:
    target = repo.root / ".git" / name
    copy = tmp_path / f"elsewhere-{name}"
    copy.write_bytes(target.read_bytes() if target.exists() else b"")
    target.unlink(missing_ok=True)
    target.symlink_to(copy)

    assert failure_of(repo.root) in {ScanFailure.ROOT_MISMATCH, *FAIL_CLOSED}


def test_symlinked_info_directory_is_not_honoured(repo: RepoBuilder, tmp_path: Path) -> None:
    elsewhere = tmp_path / "foreign-info"
    elsewhere.mkdir()
    (elsewhere / "exclude").write_text("*\n")
    info = repo.root / ".git" / "info"
    shutil.rmtree(info)
    info.symlink_to(elsewhere)

    assert failure_of(repo.root) in {ScanFailure.UNSUPPORTED_REPOSITORY, *FAIL_CLOSED}


def test_symlink_read_is_skipped_once_budget_is_zero(tmp_path: Path) -> None:
    root = tmp_path / "w"
    root.mkdir()
    os.symlink("t" * 80, root / "l")
    calls: list[str] = []
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    real = os.readlink
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                scanner.os, "readlink", lambda *a, **k: calls.append("x") or real(*a, **k)
            )
            empty = scanner._observe_leaf(fd, "l", file_cap=1000, budget=0)
            some = scanner._observe_leaf(fd, "l", file_cap=1000, budget=1000)
    finally:
        os.close(fd)

    assert empty.skipped is SkipReason.BUDGET_EXHAUSTED and empty.consumed == 0
    assert calls == ["x"]  # only the call with budget left
    assert some.skipped is None


def _victim_with_objects(make_repo: MakeRepo, name: str) -> tuple[RepoBuilder, str]:
    victim = make_repo(name)
    victim.write("victim-secret-path.txt", "victim tree\n")
    head = victim.commit("victim")
    return victim, head


def _fanout_of(repo: RepoBuilder, oid: str) -> Path:
    return repo.root / ".git" / "objects" / oid[:2]


def test_symlinked_fanout_directory_is_refused(repo: RepoBuilder, make_repo: MakeRepo) -> None:
    victim, victim_head = _victim_with_objects(make_repo, "victim-fanout")
    fanout = repo.root / ".git" / "objects" / victim_head[:2]
    fanout.symlink_to(_fanout_of(victim, victim_head))

    with pytest.raises(ScanError) as caught:
        scan_repository(repo.root)

    assert caught.value.reason is ScanFailure.UNSUPPORTED_REPOSITORY
    assert victim_head not in repr(caught.value)


def test_symlinked_loose_object_file_is_refused(repo: RepoBuilder, make_repo: MakeRepo) -> None:
    victim, victim_head = _victim_with_objects(make_repo, "victim-loose")
    fanout = repo.root / ".git" / "objects" / victim_head[:2]
    fanout.mkdir(exist_ok=True)
    (fanout / victim_head[2:]).symlink_to(_fanout_of(victim, victim_head) / victim_head[2:])

    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_symlinked_packfile_is_refused(repo: RepoBuilder, make_repo: MakeRepo) -> None:
    victim, _ = _victim_with_objects(make_repo, "victim-pack")
    victim.git("gc", "-q")
    packs = sorted((victim.root / ".git" / "objects" / "pack").glob("*.pack"))
    assert packs
    pack_dir = repo.root / ".git" / "objects" / "pack"
    pack_dir.mkdir(exist_ok=True)
    (pack_dir / packs[0].name).symlink_to(packs[0])

    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_repository_with_loose_objects_and_packs_still_scans(repo: RepoBuilder) -> None:
    repo.git("gc", "-q")
    repo.write("later.txt", "loose object after gc\n")
    repo.commit("later")

    result = scan_repository(repo.root)

    assert not result.workspace.is_dirty
    assert result.workspace.head_commit is not None


def test_object_store_entry_cap_refuses_huge_stores(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scanner, "_OBJECT_STORE_MAX_ENTRIES", 1)

    assert failure_of(repo.root) is ScanFailure.UNSUPPORTED_REPOSITORY


def test_git_dir_swapped_for_a_symlink_after_bootstrap_fails_closed(
    repo: RepoBuilder, make_repo: MakeRepo, monkeypatch: pytest.MonkeyPatch
) -> None:
    victim, victim_head = _victim_with_objects(make_repo, "victim-swap")
    real = scanner._validate_repository_files

    def swap_then_validate(*args: Any, **kwargs: Any) -> Any:
        if not (repo.root / ".git-original").exists():
            (repo.root / ".git").rename(repo.root / ".git-original")
            (repo.root / ".git").symlink_to(victim.root / ".git")
        return real(*args, **kwargs)

    monkeypatch.setattr(scanner, "_validate_repository_files", swap_then_validate)

    with pytest.raises(ScanError) as caught:
        scan_repository(repo.root)

    assert caught.value.reason is ScanFailure.ROOT_MISMATCH
    assert victim_head not in repr(caught.value)


def test_git_children_receive_pinned_descriptors(
    repo: RepoBuilder, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[dict[str, str]] = []
    real = scanner._run_git

    def spy(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("pass_fds"):
            seen.append(dict(kwargs["env"]))
        return real(*args, **kwargs)

    monkeypatch.setattr(scanner, "_run_git", spy)

    scan_repository(repo.root)

    assert seen
    for env in seen:
        assert env["GIT_WORK_TREE"].startswith("/proc/self/fd/")
        assert env["GIT_OBJECT_DIRECTORY"].startswith("/proc/self/fd/")
    assert any(env["GIT_DIR"].startswith("/proc/self/fd/") for env in seen)


def test_regular_file_is_not_read_when_budget_is_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "w"
    root.mkdir()
    (root / "f").write_bytes(b"")  # an empty file is the case a size check lets through
    reads: list[int] = []
    real_read = os.read
    monkeypatch.setattr(scanner.os, "read", lambda fd, n: reads.append(n) or real_read(fd, n))
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        seen = scanner._observe_leaf(fd, "f", file_cap=100, budget=0)
    finally:
        os.close(fd)

    assert seen.skipped is SkipReason.BUDGET_EXHAUSTED
    assert seen.consumed == 0
    assert reads == []
