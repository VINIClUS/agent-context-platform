"""Misbehaving stand-in adapter for the sandbox tests; argv[1] selects the behaviour.

Standalone on purpose: it runs inside the sandbox as an untrusted child.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import json
import os
import signal
import socket
import sys
import time

MODE = sys.argv[1]
ARG = sys.argv[2] if len(sys.argv) > 2 else ""
FINGERPRINT = os.environ.get("FAKE_FINGERPRINT", "")


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def symbol(ref: str, name: str, start: int, end: int, **over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "ref": ref,
        "language": "python",
        "qualified_name": name,
        "kind": "function",
        "disambiguator": "",
        "start_byte": start,
        "end_byte": end,
        "signature": "",
        "signature_digest": digest("sig" + name),
        "semantic_fingerprint": digest("body" + name),
        "evidence_kind": "tree_sitter",
    }
    base.update(over)
    return base


def reference(start: int, end: int, name: str, **over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "source": None,
        "kind": "call",
        "target_name": name,
        "relative_level": 0,
        "start_byte": start,
        "end_byte": end,
        "evidence_kind": "tree_sitter",
        "confidence": "heuristic",
    }
    base.update(over)
    return base


def emit(document: object) -> None:
    sys.stdout.write(json.dumps(document))
    sys.stdout.flush()


def spawn_sleepers() -> None:
    """Leave a sleeping descendant marked by ARG in its argv (escaping the process group)."""
    ignore = MODE == "ignore_term"
    if MODE == "kill_supervisor":
        os.kill(os.getppid(), signal.SIGKILL)  # a compromised parser attacking its supervisor
    if os.fork() != 0:
        time.sleep(0.5)  # let the descendants exec before the adapter exits
        return
    try:
        if MODE != "ignore_term":
            os.setsid()
        if MODE in ("double_fork", "kill_supervisor") and os.fork() != 0:
            os._exit(0)
        os.closerange(0, 3)
        if ignore:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.execv(sys.executable, [sys.executable, "-c", "import time; time.sleep(600)", ARG])
    finally:
        os._exit(1)


def attempt(action: object) -> str:
    try:
        action()  # type: ignore[operator]
    except OSError as error:
        return errno.errorcode.get(error.errno or 0, "OSError")
    except ImportError:
        return "ImportError"
    return "ok"


def read_one(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read(1)


def create(path: str) -> None:
    with open(path, "x"):
        pass


def write_one(path: str) -> None:
    with open(path, "r+b") as handle:
        handle.write(b"x")


def probe(paths: list[str]) -> dict[str, str]:
    """Try to read each path, list its directory, create files and write; report errnos."""
    result: dict[str, str] = {}
    for path in paths:
        result["read:" + path] = attempt(lambda path=path: read_one(path))
        result["list:" + os.path.dirname(path)] = attempt(
            lambda path=path: os.listdir(os.path.dirname(path))
        )
    result["create:cwd"] = attempt(lambda: create("created"))
    result["create:" + paths[0]] = attempt(lambda: create(paths[0] + ".new"))
    result["write:" + paths[0]] = attempt(lambda: write_one(paths[0]))
    result["unlink:" + paths[0]] = attempt(lambda: os.unlink(paths[0]))
    result["truncate:" + paths[0]] = attempt(lambda: os.truncate(paths[0], 0))
    result["listdir:/"] = attempt(lambda: os.listdir("/"))
    result["listdir:/proc"] = attempt(lambda: os.listdir("/proc"))
    result["tcp"] = attempt(lambda: socket.create_connection(("127.0.0.1", 9), timeout=1))
    return result


def main() -> int:
    if MODE == "no_read_hang":
        time.sleep(600)
    if MODE == "ignore_stdin":
        emit({"protocol_version": 1, "files": []})
        return 0
    request = json.loads(sys.stdin.read())
    if MODE == "read_other_file":
        # Prints {attempt: errno name or "ok"}; ARG is the os.pathsep-joined list to open.
        emit({"probe": probe(ARG.split(os.pathsep))})
        return 0
    file = request["files"][0]
    size = len(base64.b64decode(file["content_b64"]))
    fingerprint = os.environ.get("FAKE_FINGERPRINT", "")
    good = symbol("1", "pkg.a", 0, min(size, 8), signature="def a()")
    parsed: dict[str, object] = {
        "path": file["path"],
        "language": file["language"],
        "parser_fingerprint": fingerprint,
        "symbols": [good],
        "relations": [],
    }
    document: dict[str, object] = {"protocol_version": 1, "files": [parsed]}
    if MODE == "ok":
        pass
    elif MODE == "references_ok":
        document["protocol_version"] = 2
        parsed["references"] = [
            reference(4, 5, "a", source="1"),
            reference(13, 19, "return", kind="import", confidence="syntactic", relative_level=2),
        ]
        parsed["diagnostics"] = [{"code": "syntax_recovered", "count": 1}]
    elif MODE == "references_exfil":
        document["protocol_version"] = 2
        parsed["references"] = [reference(4, 5, "AWS_SECRET_KEY")]
    elif MODE == "references_v1":
        parsed["references"] = [reference(4, 5, "a")]
    elif MODE == "degraded_ok":
        document["protocol_version"] = 2
        parsed["symbols"] = []
        parsed["diagnostics"] = [
            {"code": "file_degraded", "count": 1},
            {"code": "work_budget_exceeded", "count": 1},
        ]
    elif MODE == "diagnostics_free_text":
        document["protocol_version"] = 2
        parsed["diagnostics"] = [{"code": "syntax_recovered", "count": 1, "detail": "secret"}]
    elif MODE == "range":
        parsed["symbols"] = [symbol("1", "pkg.a", 0, size + 1)]
    elif MODE == "foreign_path":
        parsed["path"] = "other/file.py"
    elif MODE == "dangling":
        parsed["relations"] = [
            {
                "source_ref": "1",
                "target_ref": "99",
                "kind": "calls",
                "start_byte": 0,
                "end_byte": 1,
                "evidence_kind": "tree_sitter",
            }
        ]
    elif MODE == "wrong_evidence":
        parsed["symbols"] = [symbol("1", "pkg.a", 0, 1, evidence_kind="scip")]
    elif MODE == "extra_field":
        good["source_text"] = "secret"
    elif MODE == "duplicate":
        parsed["symbols"] = [symbol("1", "pkg.a", 0, 8), symbol("1", "pkg.a", 0, 8)]
    elif MODE == "bad_language":
        parsed["language"] = "go"
    elif MODE == "bad_json":
        sys.stdout.write("{not json")
        return 0
    elif MODE == "huge":
        sys.stdout.write(" " * (int(ARG or "3000000")))
        return 0
    elif MODE == "stderr_flood":
        sys.stderr.write("x" * 500_000)
        sys.stderr.flush()
        emit(document)
        return 0
    elif MODE == "hang":
        time.sleep(600)
    elif MODE == "spin":
        while True:
            pass
    elif MODE == "memhog":
        hog = bytearray(1024 * 1024 * 1024)
        hog[0] = 1
    elif MODE == "forkbomb":
        for _ in range(200):
            try:
                if os.fork() == 0:
                    time.sleep(600)
                    os._exit(0)
            except OSError:
                return 7
        return 0
    elif MODE == "write":
        try:
            with open(ARG, "wb") as handle:
                handle.write(b"escaped")
                handle.flush()
        except OSError:
            return 5
        return 0
    elif MODE == "env":
        if "AGENT_CONTEXT_SECRET" in os.environ:
            return 6
    elif MODE in (
        "setsid_child",
        "double_fork",
        "ignore_term",
        "hang_with_daemon",
        "kill_supervisor",
    ):
        spawn_sleepers()
        if MODE == "hang_with_daemon":
            time.sleep(600)
    elif MODE == "deps":
        # The interpreter's own compiled dependencies must still load under the ruleset.
        import _ctypes  # noqa: F401
        import ssl  # noqa: F401

        import pydantic_core

        if pydantic_core.to_json({"a": 1}) != b'{"a":1}':
            return 8
    elif MODE == "exit121":
        return 121  # an ordinary adapter failure that must not look like a confinement failure
    elif MODE == "crash":
        return 9
    emit(document)
    return 0


sys.exit(main())
