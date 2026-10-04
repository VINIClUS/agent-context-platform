"""Run the real platform processes (uvicorn API, projection CLI) for crash-point tests.

Scenarios treat the platform as a black box: they set the documented
``AGENT_CONTEXT_FAULT_INJECTION__*`` variables, start the process, and observe its exit
code, stderr and datastores. Nothing here imports platform internals to patch them.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType
from typing import IO, Self

import httpx2
import pytest

CRASH_EXIT_CODE = 137
START_TIMEOUT_SECONDS = 60.0
EXIT_TIMEOUT_SECONDS = 60.0
CLI_TIMEOUT_SECONDS = 120.0

_TEST_ENV = {
    "AGENT_CONTEXT_TEST_S3_ENDPOINT_URL": "AGENT_CONTEXT_S3__ENDPOINT_URL",
    "AGENT_CONTEXT_TEST_S3_REGION_NAME": "AGENT_CONTEXT_S3__REGION_NAME",
    "AGENT_CONTEXT_TEST_S3_BUCKET_NAME": "AGENT_CONTEXT_S3__BUCKET_NAME",
    "AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID": "AGENT_CONTEXT_S3__ACCESS_KEY_ID",
    "AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY": "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY",
    "AGENT_CONTEXT_TEST_NEO4J_URI": "AGENT_CONTEXT_NEO4J__URI",
    "AGENT_CONTEXT_TEST_NEO4J_USERNAME": "AGENT_CONTEXT_NEO4J__USERNAME",
    "AGENT_CONTEXT_TEST_NEO4J_PASSWORD": "AGENT_CONTEXT_NEO4J__PASSWORD",
    "AGENT_CONTEXT_TEST_NEO4J_DATABASE": "AGENT_CONTEXT_NEO4J__DATABASE",
}


def process_env(
    postgres_dsn: str,
    *,
    crash_at: str | None = None,
    after: int | None = None,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """A clean environment for one platform process; ``crash_at`` arms a fault label."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("AGENT_CONTEXT_")}
    env["AGENT_CONTEXT_ENVIRONMENT"] = "test"
    env["AGENT_CONTEXT_POSTGRESQL__DSN"] = postgres_dsn
    for source, target in _TEST_ENV.items():
        value = os.environ.get(source)
        if value is not None:
            env[target] = value
    # Verifiers carry their own cost; a cheap dummy hash keeps the API start-up quick.
    env["AGENT_CONTEXT_INGESTION__ARGON2_TIME_COST"] = "1"
    env["AGENT_CONTEXT_INGESTION__ARGON2_MEMORY_COST_KIB"] = "8"
    env["AGENT_CONTEXT_INGESTION__ARGON2_PARALLELISM"] = "1"
    if crash_at is not None:
        env["AGENT_CONTEXT_FAULT_INJECTION__ENABLED"] = "true"
        env["AGENT_CONTEXT_FAULT_INJECTION__CRASH_AT"] = crash_at
        if after is not None:
            env["AGENT_CONTEXT_FAULT_INJECTION__AFTER"] = str(after)
    env.update(extra or {})
    return env


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _Process:
    def __init__(self, args: list[str], env: dict[str, str]) -> None:
        self._stderr: IO[bytes] = tempfile.TemporaryFile()  # noqa: SIM115 - closed in close()
        self.process = subprocess.Popen(
            args,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=self._stderr,
        )

    def wait(self, timeout: float = EXIT_TIMEOUT_SECONDS) -> int:
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            raise

    @property
    def stderr(self) -> str:
        self._stderr.seek(0)
        return self._stderr.read().decode("utf-8", errors="replace")

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self._stderr.close()


class ApiServer(_Process):
    """The platform HTTP API as a uvicorn subprocess on an ephemeral loopback port."""

    def __init__(self, env: dict[str, str]) -> None:
        self.port = _free_port()
        super().__init__(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "agent_context_platform.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            env,
        )

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> Self:
        deadline = time.monotonic() + START_TIMEOUT_SECONDS
        try:
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    pytest.fail(f"API exited during start-up ({self.process.returncode})")
                try:
                    if httpx2.get(f"{self.base_url}/health/live", timeout=2).status_code == 200:
                        return self
                except httpx2.TransportError:
                    time.sleep(0.2)
            pytest.fail("API did not become healthy in time")
        except BaseException:
            self.close()
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def post_batch(self, token: str, batch_id: str, body: str) -> httpx2.Response:
        return httpx2.post(
            f"{self.base_url}/v1/ingestion/batches",
            content=body,
            headers={
                "authorization": f"Bearer {token}",
                "idempotency-key": batch_id,
                "content-type": "application/json",
            },
            timeout=60,
        )


def run_cli(args: list[str], env: dict[str, str]) -> tuple[int, str]:
    """Run the ``agent-context`` console script to completion; return (exit code, stderr)."""
    cli = _Process([str(Path(sys.executable).parent / "agent-context"), *args], env)
    try:
        code = cli.wait(CLI_TIMEOUT_SECONDS)
        return code, cli.stderr
    finally:
        cli.close()
