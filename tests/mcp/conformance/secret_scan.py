"""Fail closed when a conformance artifact holds a credential (PLATFORM-047).

Run over the runner's output directory before anything is archived. It looks for the exact
bearer token the loopback profiles used (passed in the environment, never on the command line),
and for each of its segments on its own (the secret after the `.` most of all), and for
credential shapes: bearer tokens (`<prefix>.<secret>`, `security/bearer.py`), an
``Authorization: Bearer`` value, URLs or DSNs with a password, private keys, cloud and GitHub
tokens, and the repository's test canaries (``AGENT_CONTEXT_CANARY``). A hit prints the file,
line and pattern name, never the match, and exits 1.

The runner's own scan is the only one that knows the token. ``--verdict FILE`` records its
outcome (pass or fail and counts, never a value) in the output directory, and
``--check-verdict FILE`` exits 0 only for a passing token-aware verdict: CI uploads artifacts only
when that check and its own token-less rescan both pass.
"""

from __future__ import annotations

import argparse
import json
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
# A token segment shorter than this (the `mcp_` lookup prefix is 13) is not secret on its own.
MIN_SEGMENT_CHARS: Final = 16


def token_needles(token: str | None) -> tuple[str, ...]:
    """The whole token and every segment long enough to be secret on its own."""
    if not token:
        return ()
    segments = [segment for segment in token.split(".") if len(segment) >= MIN_SEGMENT_CHARS]
    return (token, *segments)


def scan_text(text: str, token: str | None = None) -> Iterator[tuple[int, str]]:
    """Yield ``(line, pattern name)`` for every hit; never the matched text."""
    needles = token_needles(token)
    for number, line in enumerate(text.splitlines(), start=1):
        if any(needle in line for needle in needles):
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


def verdict_passed(path: Path) -> bool:
    """A recorded verdict passes only if it is token-aware and found nothing."""
    try:
        verdict = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return (
        isinstance(verdict, dict)
        and verdict.get("passed") is True
        and verdict.get("token_aware") is True
        and verdict.get("findings") == 0
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--verdict", type=Path, help="record the outcome here (counts only)")
    parser.add_argument("--check-verdict", type=Path, help="exit 0 only for a passing verdict")
    arguments = parser.parse_args(None if argv is None else list(argv))
    if arguments.check_verdict is not None:
        passed = verdict_passed(arguments.check_verdict)
        print(f"secret scan: recorded token-aware verdict {'passed' if passed else 'FAILED'}")
        return 0 if passed else 1
    if not arguments.paths:
        parser.error("paths are required unless --check-verdict is given")
    token = os.environ.get(TOKEN_ENV) or None
    missing = [str(path) for path in arguments.paths if not path.exists()]
    findings = [] if missing else scan_paths(arguments.paths, token)
    if missing:
        print(f"secret scan: missing {missing}", file=sys.stderr)
    for finding in findings:
        print(f"secret scan: {finding}", file=sys.stderr)
    print(f"secret scan: {len(findings)} finding(s)")
    passed = not missing and not findings
    if arguments.verdict is not None:
        verdict = {"passed": passed, "findings": len(findings), "token_aware": token is not None}
        arguments.verdict.write_text(json.dumps(verdict) + "\n", encoding="utf-8")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
