from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "repos" / "scanner"
# ``example.invalid`` is reserved (RFC 2606): never a real address.
_IDENTITY = {
    "GIT_AUTHOR_NAME": "Scanner Test",
    "GIT_AUTHOR_EMAIL": "scanner@example.invalid",
    "GIT_COMMITTER_NAME": "Scanner Test",
    "GIT_COMMITTER_EMAIL": "scanner@example.invalid",
    "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
    "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
}


class RepoBuilder:
    """Builds a real, isolated Git repository for one test."""

    def __init__(self, root: Path, home: Path) -> None:
        self.root = root
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(home),
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            **_IDENTITY,
        }

    def git(self, *args: str, cwd: Path | None = None) -> str:
        completed = subprocess.run(
            [
                "git",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "init.defaultBranch=main",
                "-c",
                "protocol.file.allow=never",
                *args,
            ],
            cwd=cwd or self.root,
            env=self.env,
            capture_output=True,
            check=True,
            text=True,
        )
        return completed.stdout.strip()

    def init(self, *extra: str) -> RepoBuilder:
        self.root.mkdir(parents=True, exist_ok=True)
        self.git("init", "-q", *extra)
        return self

    def write(self, relative: str, data: bytes | str) -> Path:
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data.encode() if isinstance(data, str) else data)
        return target

    def seed(self) -> RepoBuilder:
        """Copy the fixture tree in (``gitignore.txt`` becomes ``.gitignore``)."""
        for source in sorted(FIXTURE_ROOT.rglob("*")):
            if source.is_file():
                name = ".gitignore" if source.name == "gitignore.txt" else source.name
                destination = self.root / source.relative_to(FIXTURE_ROOT).parent / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
        return self

    def commit(self, message: str = "commit") -> str:
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)
        return self.git("rev-parse", "HEAD")


@pytest.fixture
def make_repo(tmp_path: Path) -> Callable[[str], RepoBuilder]:
    home = tmp_path / "home"
    home.mkdir()

    def factory(name: str = "repo") -> RepoBuilder:
        return RepoBuilder(tmp_path / name, home).init()

    return factory


@pytest.fixture
def repo(make_repo: Callable[[str], RepoBuilder]) -> RepoBuilder:
    """A committed repository seeded from the fixture tree."""
    builder = make_repo("repo").seed()
    builder.commit("seed")
    return builder
