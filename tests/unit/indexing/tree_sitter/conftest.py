"""Landlock visibility: the probed ABI is printed in every run, skips carry their reason."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

import agent_context_platform
from agent_context_platform.indexing.tree_sitter import landlock, runner
from agent_context_platform.indexing.tree_sitter.runner import Limits

ABI = landlock.abi_version()
# The fake adapter parses stdin with the real ``ParseRequest``; under Landlock it may read the
# platform package (and the directory holding it, which Python lists) and nothing else more.
_PACKAGE = Path(agent_context_platform.__file__).resolve().parent
PACKAGE_READ_PATHS = (str(_PACKAGE), str(_PACKAGE.parent))


@pytest.fixture(autouse=True)
def legacy_sandbox_tests_need_the_escape_hatch_without_landlock(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On a kernel without Landlock the pre-existing sandbox tests (rlimits, supervisor,
    sweeps, output bounds) run through the dev escape hatch, i.e. unconfined, so they still
    exercise the runner; the summary line says so. ``test_landlock.py`` decides for itself.
    Where Landlock exists (CI) they all run confined.
    """
    if ABI >= 1 or request.module.__name__.endswith("test_landlock"):
        return

    def unconfined(command: Sequence[str], limits: Limits) -> None:
        return None

    monkeypatch.setattr(runner, "_confinement", unconfined)


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    if ABI >= 1:
        terminalreporter.write_line(f"landlock: kernel ABI {ABI} (sandbox tests ran confined)")
    else:
        terminalreporter.write_line(
            "landlock: kernel ABI 0 (UNAVAILABLE: Landlock tests are SKIPPED and the legacy "
            "sandbox tests ran UNCONFINED through the dev escape hatch)"
        )
