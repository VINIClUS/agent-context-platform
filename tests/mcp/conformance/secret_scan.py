"""Fail closed when a conformance artifact holds a credential (PLATFORM-047).

Run over the runner's output directory before anything is archived. It looks for the exact
bearer token the loopback profiles used (passed in the environment, never on the command line)
and for credential shapes: bearer tokens (`<prefix>.<secret>`, `security/bearer.py`), an
``Authorization: Bearer`` value, URLs or DSNs with a password, private keys, cloud and GitHub
tokens, and the repository's test canaries (``AGENT_CONTEXT_CANARY``). A hit prints the file,
line and pattern name, never the match, and exits 1.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Final

TOKEN_ENV: Final = "AGENT_CONTEXT_CONFORMANCE_TOKEN"
PATTERNS: Final[tuple[tuple[str, re.Pattern[str]], ...]] = (
    ("mcp_token", re.compile(r"mcp_[A-Za-z0-9_-]{4,60}\.[A-Za-z0-9_-]{32,256}")),
    ("authorization_bearer", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9_-]{8,64}\.[A-Za-z0-9_-]{8,}")),
    ("url_password", re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s/:@\"']+:[^\s/@\"']+@")),
    ("private_key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b|\bgithub_pat_[A-Za-z0-9_]{40,}")),
    ("test_canary", re.compile(r"AGENT_CONTEXT_CANARY")),
)
MAX_FILE_BYTES: Final = 64 * 1024 * 1024


def scan_text(text: str, token: str | None = None) -> Iterator[tuple[int, str]]:
    """Yield ``(line, pattern name)`` for every hit; never the matched text."""
    for number, line in enumerate(text.splitlines(), start=1):
        if token and token in line:
            yield number, "runtime_token"
        for name, pattern in PATTERNS:
            if pattern.search(line):
                yield number, name


def scan_paths(paths: Iterable[Path], token: str | None = None) -> list[str]:
    findings = []
    for root in paths:
        files = [root] if root.is_file() else sorted(p for p in root.rglob("*") if p.is_file())
        for path in files:
            if path.stat().st_size > MAX_FILE_BYTES:
                findings.append(f"{path}: larger than {MAX_FILE_BYTES} bytes, not scanned")
                continue
            text = path.read_bytes().decode("utf-8", errors="replace")
            findings.extend(f"{path}:{line}: {name}" for line, name in scan_text(text, token))
    return findings


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    arguments = parser.parse_args(None if argv is None else list(argv))
    missing = [str(path) for path in arguments.paths if not path.exists()]
    if missing:
        print(f"secret scan: missing {missing}", file=sys.stderr)
        return 1
    findings = scan_paths(arguments.paths, os.environ.get(TOKEN_ENV) or None)
    for finding in findings:
        print(f"secret scan: {finding}", file=sys.stderr)
    print(f"secret scan: {len(findings)} finding(s)")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
