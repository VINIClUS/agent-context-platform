"""Landlock visibility: the probed ABI is printed in every run, skips carry their reason."""

from __future__ import annotations

import pytest

from agent_context_platform.indexing.tree_sitter import landlock


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    abi = landlock.abi_version()
    terminalreporter.write_line(
        f"landlock: kernel ABI {abi} "
        + ("(sandbox tests ran)" if abi >= 1 else "(UNAVAILABLE: Landlock tests are SKIPPED)")
    )
