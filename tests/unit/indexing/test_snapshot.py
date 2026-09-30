"""The workspace snapshot identity is a pure, checkout-independent, frozen derivation."""

from __future__ import annotations

import pytest

from agent_context_platform.indexing.identity import uncommitted_file_logical_id
from agent_context_platform.indexing.snapshot import gitlink_digest, workspace_snapshot_id

pytestmark = pytest.mark.unit

HEAD = "a" * 40
ENTRIES = [
    ("b.py", "modified", "1" * 64),
    ("a.py", "untracked", "2" * 64),
    ("c.py", "deleted", None),
    ("d.py", "added", "3" * 64),
]


def test_the_snapshot_id_is_frozen() -> None:
    assert (
        workspace_snapshot_id("repo-1", HEAD, ENTRIES)
        == "snap_fd74f523f57678d74273532945df25f8b60bc701"
    )
    assert (
        workspace_snapshot_id("repo-1", HEAD, []) == "snap_477c1f66e41275023ab108c6583549e06993794f"
    )
    assert (
        workspace_snapshot_id("repo-1", None, ENTRIES[:1])
        == "snap_7143dcc3337ba9c50d7bd0c6eb2e932d79848f05"
    )


def test_the_order_of_the_entries_does_not_matter() -> None:
    assert workspace_snapshot_id("repo-1", HEAD, reversed(ENTRIES)) == workspace_snapshot_id(
        "repo-1", HEAD, ENTRIES
    )


@pytest.mark.parametrize(
    "other",
    [
        ("repo-2", HEAD, ENTRIES),
        ("repo-1", "b" * 40, ENTRIES),
        ("repo-1", HEAD, [*ENTRIES[:3], ("d.py", "added", "4" * 64)]),
        ("repo-1", HEAD, [*ENTRIES[:3], ("d.py", "modified", "3" * 64)]),
        ("repo-1", HEAD, ENTRIES[:3]),
    ],
)
def test_every_input_is_part_of_the_identity(
    other: tuple[str, str, list[tuple[str, str, str | None]]],
) -> None:
    assert workspace_snapshot_id(*other) != workspace_snapshot_id("repo-1", HEAD, ENTRIES)


@pytest.mark.parametrize(
    "entries",
    [
        [("a.py", "renamed", "1" * 64)],
        [("a.py", "deleted", "1" * 64)],
        [("a.py", "modified", "1" * 64), ("a.py", "untracked", "1" * 64)],
    ],
)
def test_a_malformed_entry_is_refused(entries: list[tuple[str, str, str | None]]) -> None:
    with pytest.raises(ValueError):
        workspace_snapshot_id("repo-1", HEAD, entries)


def test_an_uncommitted_file_id_is_per_repository_and_path_only() -> None:
    frozen = uncommitted_file_logical_id("repo-1", "src/new.py")

    assert str(frozen) == "176df8d5-0f43-58eb-9ee6-432f7d1a6db6"
    assert uncommitted_file_logical_id("repo-2", "src/new.py") != frozen
    assert uncommitted_file_logical_id("repo-1", "src/other.py") != frozen


@pytest.mark.parametrize(
    "path",
    [
        "",
        "./a.py",
        "a/../b.py",
        "a//b.py",
        "/abs.py",
        "a\\b.py",
        "a\x00b.py",
        ".git/x",
        "a/",
        "\ud800.py",
    ],
)
def test_a_path_that_is_not_canonical_is_refused(path: str) -> None:
    with pytest.raises(ValueError):
        workspace_snapshot_id("repo-1", HEAD, [(path, "modified", "1" * 64)])


@pytest.mark.parametrize("digest", [None, "", "A" * 64, "1" * 63, "1" * 65, "g" * 64])
def test_a_non_deleted_entry_needs_a_canonical_digest(digest: str | None) -> None:
    for state in ("modified", "added", "untracked", "gitlink"):
        with pytest.raises(ValueError):
            workspace_snapshot_id("repo-1", HEAD, [("a.py", state, digest)])


@pytest.mark.parametrize("base", ["", "A" * 40, "a" * 39, "a" * 41, "a" * 65, "z" * 40])
def test_a_base_commit_is_an_oid_or_none(base: str) -> None:
    with pytest.raises(ValueError):
        workspace_snapshot_id("repo-1", base, [])


def test_a_sha256_base_commit_and_none_are_accepted() -> None:
    assert workspace_snapshot_id("repo-1", "a" * 64, []) != workspace_snapshot_id(
        "repo-1", None, []
    )


def test_a_repository_id_is_required() -> None:
    with pytest.raises(ValueError):
        workspace_snapshot_id("", HEAD, [])


def test_entries_sort_by_the_utf8_bytes_of_the_path() -> None:
    entries = [
        ("\u00e9.py", "modified", "1" * 64),
        ("z.py", "modified", "2" * 64),
        ("A.py", "added", "3" * 64),
    ]

    assert workspace_snapshot_id("repo-1", HEAD, entries) == workspace_snapshot_id(
        "repo-1", HEAD, list(reversed(entries))
    )


def test_a_duplicate_path_is_refused_even_across_states() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        workspace_snapshot_id(
            "repo-1", HEAD, [("a.py", "deleted", None), ("a.py", "added", "1" * 64)]
        )


def test_a_gitlink_digest_binds_both_oids() -> None:
    assert gitlink_digest(None, "a" * 40) != gitlink_digest("a" * 40, None)
    with pytest.raises(ValueError):
        gitlink_digest("nothex", None)
