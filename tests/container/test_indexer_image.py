"""Container security tests for the indexer image (PLATFORM-032).

Opt-in: they build the image, so they only run with ``AGENT_CONTEXT_TEST_CONTAINER=1``
and a working Docker. They use the documented run flags (see the Dockerfile header).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = pytest.mark.container

ROOT = Path(__file__).resolve().parents[2]
FLAGS = [
    "--rm",
    "--read-only",
    "--network",
    "none",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--pids-limit",
    "64",
    "--memory",
    "512m",
]


def _docker(*args: str, timeout: int = 600) -> subprocess.CompletedProcess[str]:
    docker = shutil.which("docker")
    assert docker is not None
    return subprocess.run(
        [docker, *args], capture_output=True, text=True, timeout=timeout, check=False
    )


@pytest.fixture(scope="module")
def image() -> Iterator[str]:
    if os.environ.get("AGENT_CONTEXT_TEST_CONTAINER") != "1":
        pytest.skip("set AGENT_CONTEXT_TEST_CONTAINER=1 to build and test the indexer image")
    if shutil.which("docker") is None or _docker("info", timeout=60).returncode != 0:
        pytest.skip("Docker is not available")
    tag = f"agent-context-indexer-test:{uuid.uuid4().hex[:12]}"
    built = _docker("build", "-f", "containers/indexer/Dockerfile", "-t", tag, str(ROOT))
    assert built.returncode == 0, built.stderr[-2000:]
    yield tag
    _docker("image", "rm", "--force", tag, timeout=120)


def _run(image: str, code: str, *extra: str) -> subprocess.CompletedProcess[str]:
    return _docker("run", *FLAGS, *extra, image, "-c", code, timeout=120)


def test_runs_as_non_root_numeric_uid(image: str) -> None:
    result = _run(image, "import os; print(os.getuid(), os.getgid())")
    assert result.stdout.split() == ["10001", "10001"]
    configured = _docker("image", "inspect", "--format", "{{.Config.User}}", image)
    assert configured.stdout.strip() == "10001:10001"


def test_root_filesystem_is_read_only(image: str) -> None:
    code = (
        "import errno\n"
        "try:\n"
        "    open('/probe', 'w')\n"
        "except OSError as error:\n"
        "    print(errno.errorcode[error.errno])\n"
    )
    assert _run(image, code).stdout.strip() == "EROFS"


def test_no_network_interface_but_loopback(image: str) -> None:
    code = "import os; print(sorted(os.listdir('/sys/class/net')))"
    assert _run(image, code).stdout.strip() == "['lo']"
    connect = (
        "import socket\n"
        "try:\n"
        "    socket.create_connection(('192.0.2.1', 80), timeout=2)\n"
        "except OSError:\n"
        "    print('blocked')\n"
    )
    assert _run(image, connect).stdout.strip() == "blocked"


def test_capabilities_dropped_and_no_new_privileges(image: str) -> None:
    code = (
        "fields = dict(line.split(':\\t') for line in open('/proc/self/status')"
        " if line.startswith(('CapEff', 'NoNewPrivs')))\n"
        "print(fields['CapEff'].strip(), fields['NoNewPrivs'].strip())"
    )
    assert _run(image, code).stdout.split() == ["0000000000000000", "1"]


def test_runner_sandbox_works_inside_the_image(image: str) -> None:
    code = (
        "from agent_context_platform.indexing.tree_sitter.runner import Limits, _run\n"
        "import sys\n"
        "try:\n"
        "    _run(['/usr/local/bin/python', '-c', 'open(\"/x\", \"w\")'], b'', Limits(), {})\n"
        "except Exception as error:\n"
        "    print(type(error).__name__, error)\n"
    )
    assert _run(image, code).stdout.split() == ["StructuralError", "nonzero_exit"]
