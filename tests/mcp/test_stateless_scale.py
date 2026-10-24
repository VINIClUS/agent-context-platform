"""Two shipping replicas, alternating requests and a restart in between (PLATFORM-047).

Two real `uvicorn` processes serve the shipping application (`tests/mcp/conformance/loopback.py`,
shipping profile) with the same configuration: the same bearer token table and the same cursor
key. Every request goes to the other replica than the one before, and one replica is killed and
started again as a new process between calls, so no in-memory state can survive. The sequence is
discovery, listing, a call of every memory tool and a paginated call whose cursor continues on
the other replica, including the restarted one.

Asserted: no response carries a session or affinity header; `server/discover` and `tools/list`
are byte-identical across replicas and across the restart; identical calls give identical
results; a cursor issued by one replica is accepted by the other, and refused by a replica with
another cursor key. No external service is needed; each step is bounded by a deadline.
"""

from __future__ import annotations

import json
import os
import secrets
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from itertools import cycle
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp_helpers import mcp_headers, rpc_body

pytestmark = pytest.mark.integration

LOOPBACK = Path(__file__).resolve().parent / "conformance" / "loopback.py"
STARTUP_SECONDS = 60.0
REQUEST_SECONDS = 20.0
SESSION_HEADERS = frozenset(
    {
        "mcp-session-id",
        "set-cookie",
        "set-cookie2",
        "x-session-id",
        "x-backend",
        "x-backend-id",
        "x-served-by",
        "x-upstream",
        "x-instance-id",
        "x-replica",
        "affinity",
    }
)
BUDGET = {"max_items": 5, "max_bytes": 20_000, "max_tokens": 5_000}
COMMIT = "a" * 40
CALLS: dict[str, dict[str, Any]] = {
    "context_for_task": {
        "repository_id": "repo-1",
        "revision": {"kind": "commit", "commit_id": COMMIT},
        "task": "fix the parser",
        "budget": BUDGET,
    },
    "repository_overview": {"repository_id": "repo-1", "commit_id": COMMIT, "budget": BUDGET},
    "symbol_context": {
        "repository_id": "repo-1",
        "commit_id": COMMIT,
        "symbol_name": "parse",
        "budget": BUDGET,
    },
    "decision_history": {
        "scope": {"kind": "project", "project_id": "project-1"},
        "query": "storage engine",
        "valid_at": "2026-09-01T00:00:00Z",
        "budget": BUDGET,
    },
    "failure_history": {
        "repository_id": "repo-1",
        "query": "timeout",
        "valid_from": "2026-08-01T00:00:00Z",
        "valid_to": "2026-09-01T00:00:00Z",
        "budget": BUDGET,
    },
}


class Replica:
    """One shipping-profile process on an ephemeral loopback port."""

    def __init__(self, name: str, workdir: Path, env: dict[str, str]) -> None:
        self.name = name
        self._workdir = workdir
        self._env = env
        self._process: subprocess.Popen[bytes] | None = None
        self.url = ""
        self.starts = 0

    def start(self) -> None:
        self.starts += 1
        port_file = self._workdir / f"{self.name}-{self.starts}.port"
        log = (self._workdir / f"{self.name}-{self.starts}.log").open("wb")
        self._process = subprocess.Popen(
            [sys.executable, str(LOOPBACK), "--profile", "shipping", "--port-file", str(port_file)],
            env=self._env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + STARTUP_SECONDS
        while not port_file.exists() or not port_file.read_text(encoding="utf-8"):
            if self._process.poll() is not None or time.monotonic() > deadline:
                self.stop()
                pytest.fail(f"replica {self.name} did not start")
            time.sleep(0.05)
        self.url = f"http://127.0.0.1:{port_file.read_text(encoding='utf-8')}/mcp"

    @property
    def pid(self) -> int:
        assert self._process is not None
        return self._process.pid

    def kill(self) -> None:
        """Kill without any shutdown: nothing it holds in memory can be handed over."""
        if self._process is not None and self._process.poll() is None:
            self._process.send_signal(signal.SIGKILL)
            self._process.wait(timeout=REQUEST_SECONDS)

    def stop(self) -> None:
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=REQUEST_SECONDS)
            except subprocess.TimeoutExpired:  # pragma: no cover - a hung replica
                self.kill()


@pytest.fixture
def config(tmp_path: Path) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AGENT_CONTEXT_", "OTEL_"))
    }
    env["AGENT_CONTEXT_CONFORMANCE_TOKEN"] = f"mcp_scale0001.{secrets.token_urlsafe(32)}"
    env["AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY"] = secrets.token_urlsafe(48)
    env["PYTHONPATH"] = os.pathsep.join([str(LOOPBACK.parent), env.get("PYTHONPATH", "")])
    return env


@pytest.fixture
def replicas(tmp_path: Path, config: dict[str, str]) -> Iterator[tuple[Replica, Replica]]:
    pair = (Replica("a", tmp_path, config), Replica("b", tmp_path, config))
    try:
        for replica in pair:
            replica.start()
        yield pair
    finally:
        for replica in pair:
            replica.stop()


class Caller:
    """Sends every request to the next replica in turn and checks every response's headers."""

    def __init__(self, token: str, replicas: tuple[Replica, ...]) -> None:
        self._token = token
        self._next = cycle(replicas)
        self._client = httpx2.Client(timeout=REQUEST_SECONDS)
        self.served_by: list[str] = []

    def close(self) -> None:
        self._client.close()

    def post(
        self, method: str, params: dict[str, Any] | None = None, replica: Replica | None = None
    ) -> httpx2.Response:
        target = replica or next(self._next)
        self.served_by.append(f"{target.name}:{target.pid}")
        extra = {"authorization": f"Bearer {self._token}"}
        if method == "tools/call" and params:
            extra["mcp-name"] = params["name"]
        response = self._client.post(
            target.url, headers=mcp_headers(method, **extra), json=rpc_body(method, params)
        )
        assert response.status_code == 200, (method, response.status_code)
        leaked = {name.lower() for name in response.headers} & SESSION_HEADERS
        assert not leaked, f"{method} answered session or affinity headers {sorted(leaked)}"
        return response

    def call(
        self, name: str, arguments: dict[str, Any], replica: Replica | None = None
    ) -> dict[str, Any]:
        response = self.post("tools/call", {"name": name, "arguments": arguments}, replica)
        result: dict[str, Any] = response.json()["result"]
        return result


def _page(result: dict[str, Any]) -> tuple[str, str | None]:
    assert result.get("isError") is not True, result
    package = result["structuredContent"]
    return package["items"][0]["item_id"], package["next_cursor"]


def _error_code(result: dict[str, Any]) -> str:
    assert result["isError"] is True
    code: str = json.loads(result["content"][0]["text"])["error"]["code"]
    return code


def test_alternating_replicas_with_a_restart_answer_identically(
    tmp_path: Path, config: dict[str, str], replicas: tuple[Replica, Replica]
) -> None:
    a, b = replicas
    caller = Caller(config["AGENT_CONTEXT_CONFORMANCE_TOKEN"], (a, b))
    try:
        discover = [caller.post("server/discover").content for _ in range(2)]
        listing = [caller.post("tools/list").content for _ in range(2)]
        assert discover[0] == discover[1]
        assert listing[0] == listing[1]
        assert {tool["name"] for tool in json.loads(listing[0])["result"]["tools"]} == set(CALLS)

        # A new process replaces b: nothing held in memory survives.
        old_pid = b.pid
        b.kill()
        b.start()
        assert b.pid != old_pid
        assert caller.post("server/discover", replica=b).content == discover[0]
        assert caller.post("tools/list", replica=b).content == listing[0]

        for name, arguments in CALLS.items():
            first = caller.call(name, arguments)
            second = caller.call(name, arguments)  # the other replica
            assert first == second, name
            assert _page(first)[0] == "item-0"

        # Pagination: page 1 on a, page 2 on the restarted b, page 3 on a, each with the
        # explicit cursor the previous page returned and nothing else.
        name, arguments = "context_for_task", CALLS["context_for_task"]
        item, cursor = _page(caller.call(name, arguments, a))
        assert (item, cursor is not None) == ("item-0", True)
        item, cursor = _page(caller.call(name, {**arguments, "cursor": cursor}, b))
        assert (item, cursor is not None) == ("item-1", True)
        again = caller.call(name, {**arguments, "cursor": cursor}, b)
        item, last = _page(caller.call(name, {**arguments, "cursor": cursor}, a))
        assert (item, last) == ("item-2", None)
        assert again == caller.call(name, {**arguments, "cursor": cursor}, a)

        # A replica with another cursor key refuses that cursor; a fresh query still works.
        other = Replica(
            "c",
            tmp_path,
            {**config, "AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY": secrets.token_urlsafe(48)},
        )
        other.start()
        try:
            refused = caller.call(name, {**arguments, "cursor": cursor}, other)
            assert _error_code(refused) == "cursor_invalid"
            assert _page(caller.call(name, arguments, other))[0] == "item-0"
        finally:
            other.stop()
        # Requests really alternated and reached the new b process.
        assert {served.split(":")[0] for served in caller.served_by} == {"a", "b", "c"}
        assert f"b:{b.pid}" in caller.served_by
    finally:
        caller.close()
