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


def test_daemonizing_adapters_do_not_exhaust_nproc(image: str) -> None:
    """50 runs, each leaving a setsid daemon, all succeed and leave nothing behind.

    Without the subreaper supervisor about 14 leftovers fill RLIMIT_NPROC=16 for UID 10001
    and every later run fails with ``nonzero_exit``.
    """
    code = (
        "import os\n"
        "from agent_context_platform.indexing.tree_sitter.runner import Limits, _run\n"
        "daemon = 'import os, sys, time\\n'"
        "'if os.fork() == 0:\\n'"
        "'    os.setsid(); os.closerange(0, 3); time.sleep(600)\\n'"
        "'time.sleep(0.2)'\n"
        "ok = 0\n"
        "for _ in range(50):\n"
        "    _run(['/usr/local/bin/python', '-c', daemon, 'ac-daemon-marker'], b'', Limits(), {})\n"
        "    ok += 1\n"
        "left = 0\n"
        "for name in os.listdir('/proc'):\n"
        "    if name.isdigit() and int(name) != os.getpid():\n"
        "        try:\n"
        "            left += b'ac-daemon-marker' in open(f'/proc/{name}/cmdline', 'rb').read()\n"
        "        except OSError:\n"
        "            pass\n"
        "print(ok, left)\n"
    )
    result = _run(image, code)
    assert result.stdout.split() == ["50", "0"], result.stderr[-500:]


def test_killing_the_supervisor_does_not_wedge_nproc(image: str) -> None:
    """60 runs whose adapter SIGKILLs its supervisor and leaves setsid daemons, then a benign run.

    Without the runner acting as a subreaper the orphans escape to PID 1 and fill
    RLIMIT_NPROC=16 for UID 10001, so the benign run fails with ``nonzero_exit``.
    """
    code = (
        "import os\n"
        "from agent_context_platform.indexing.tree_sitter.base import StructuralError\n"
        "from agent_context_platform.indexing.tree_sitter.runner import Limits, _run\n"
        "attack = ('import os, signal, time\\n'\n"
        "'os.kill(os.getppid(), signal.SIGKILL)\\n'\n"
        "'if os.fork() == 0:\\n'\n"
        "'    os.setsid()\\n'\n"
        "'    if os.fork() != 0:\\n'\n"
        "'        os._exit(0)\\n'\n"
        "'    os.closerange(0, 3); time.sleep(600)\\n'\n"
        "'time.sleep(0.2)')\n"
        "for _ in range(60):\n"
        "    try:\n"
        "        _run(['/usr/local/bin/python', '-c', attack, 'ac-kill-marker'], b'', Limits(), {})\n"
        "    except StructuralError:\n"
        "        pass\n"
        "_run(['/usr/local/bin/python', '-c', 'pass'], b'', Limits(), {})\n"
        "left = 0\n"
        "for name in os.listdir('/proc'):\n"
        "    if name.isdigit() and int(name) != os.getpid():\n"
        "        try:\n"
        "            left += b'ac-kill-marker' in open(f'/proc/{name}/cmdline', 'rb').read()\n"
        "        except OSError:\n"
        "            pass\n"
        "zombies = sum(1 for n in os.listdir('/proc') if n.isdigit() and n != str(os.getpid())"
        " and open(f'/proc/{n}/stat').read().rsplit(')', 1)[1].split()[0] == 'Z')\n"
        "print('benign-ok', left, zombies)\n"
    )
    result = _run(image, code)
    assert result.stdout.split() == ["benign-ok", "0", "0"], result.stderr[-500:]
