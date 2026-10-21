"""Container tests for the platform service image (PLATFORM-049).

Opt-in like the indexer image tests: they need ``AGENT_CONTEXT_TEST_CONTAINER=1`` and a working
Docker. They build the image with the label build arguments computed from this checkout, or use
``AGENT_CONTEXT_TEST_PLATFORM_IMAGE`` when CI already built it. Every run uses the documented
hardening flags (see the Dockerfile header).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import tomllib
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from agent_context_platform.operations import migrate

pytestmark = pytest.mark.container

ROOT = Path(__file__).resolve().parents[2]
UID = "10002"
FLAGS = [
    "--read-only",
    "--tmpfs",
    "/tmp:rw,noexec,nosuid,size=64m",
    "--network",
    "none",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--pids-limit",
    "256",
    "--memory",
    "1g",
]


def _docker(*args: str, timeout: int = 900) -> subprocess.CompletedProcess[str]:
    docker = shutil.which("docker")
    assert docker is not None
    return subprocess.run(
        [docker, *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def _expected_labels() -> dict[str, str]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    revision = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    return {
        "org.opencontainers.image.revision": revision or "unknown",
        "org.opencontainers.image.version": project["project"]["version"],
        "io.agent-context.sdk.version": project["tool"]["agent-context"]["sdk"]["version"],
        "io.agent-context.alembic.head": migrate.image_head(
            migrate.alembic_config(ROOT / "alembic.ini")
        ),
    }


@pytest.fixture(scope="module")
def image() -> Iterator[str]:
    if os.environ.get("AGENT_CONTEXT_TEST_CONTAINER") != "1":
        pytest.skip("set AGENT_CONTEXT_TEST_CONTAINER=1 to build and test the platform image")
    if shutil.which("docker") is None or _docker("info", timeout=60).returncode != 0:
        pytest.skip("Docker is not available")
    prebuilt = os.environ.get("AGENT_CONTEXT_TEST_PLATFORM_IMAGE")
    if prebuilt:
        yield prebuilt
        return
    labels = _expected_labels()
    tag = f"agent-context-platform-test:{uuid.uuid4().hex[:12]}"
    built = _docker(
        "build",
        "-f",
        "containers/platform/Dockerfile",
        "--build-arg",
        f"REVISION={labels['org.opencontainers.image.revision']}",
        "--build-arg",
        f"VERSION={labels['org.opencontainers.image.version']}",
        "--build-arg",
        f"SDK_VERSION={labels['io.agent-context.sdk.version']}",
        "--build-arg",
        f"ALEMBIC_HEAD={labels['io.agent-context.alembic.head']}",
        "-t",
        tag,
        str(ROOT),
    )
    assert built.returncode == 0, built.stderr[-2000:]
    yield tag
    _docker("image", "rm", "--force", tag, timeout=120)


def _run(
    image: str, *command: str, extra: tuple[str, ...] = ()
) -> subprocess.CompletedProcess[str]:
    return _docker("run", "--rm", *FLAGS, *extra, image, *command, timeout=120)


def _python(image: str, code: str) -> subprocess.CompletedProcess[str]:
    return _run(image, "python", "-c", code)


def _inspect(image: str) -> dict[str, object]:
    result = _docker("image", "inspect", "--format", "{{json .Config}}", image)
    assert result.returncode == 0, result.stderr
    config: dict[str, object] = json.loads(result.stdout)
    return config


def test_runs_as_a_dedicated_non_root_numeric_uid(image: str) -> None:
    result = _python(image, "import os; print(os.getuid(), os.getgid(), os.getgroups())")
    assert result.stdout.split() == [UID, UID, f"[{UID}]"]
    assert _inspect(image)["User"] == f"{UID}:{UID}"


def test_root_filesystem_is_read_only_and_tmp_is_scratch(image: str) -> None:
    code = (
        "import errno\n"
        "for path in ('/probe', '/app/probe', '/opt/venv/probe', '/tmp/probe'):\n"
        "    try:\n"
        "        open(path, 'w').close()\n"
        "        print(path, 'written')\n"
        "    except OSError as error:\n"
        "        print(path, errno.errorcode[error.errno])\n"
    )
    assert _python(image, code).stdout.splitlines() == [
        "/probe EROFS",
        "/app/probe EROFS",
        "/opt/venv/probe EROFS",
        "/tmp/probe written",
    ]


def test_capabilities_dropped_and_no_new_privileges(image: str) -> None:
    code = (
        "fields = dict(line.split(':\\t') for line in open('/proc/self/status')"
        " if line.startswith(('CapEff', 'CapPrm', 'NoNewPrivs')))\n"
        "print(fields['CapEff'].strip(), fields['CapPrm'].strip(), fields['NoNewPrivs'].strip())"
    )
    assert _python(image, code).stdout.split() == ["0000000000000000", "0000000000000000", "1"]


def test_labels_record_the_revision_versions_and_alembic_head(image: str) -> None:
    labels = _inspect(image)["Labels"]
    assert isinstance(labels, dict)
    expected = _expected_labels()
    assert {name: labels.get(name) for name in expected} == expected
    assert labels["org.opencontainers.image.source"] == (
        "https://github.com/VINIClUS/agent-context-platform"
    )


def test_the_command_is_exec_form_and_needs_no_shell(image: str) -> None:
    config = _inspect(image)
    assert config.get("Entrypoint") in (None, [])
    command = config["Cmd"]
    assert isinstance(command, list)
    assert command[:2] == ["uvicorn", "agent_context_platform.app:create_app"]
    assert not any(Path(part).name == "sh" or part == "-c" for part in command)
    # Only the API role serves HTTP: its compose service declares the health check.
    assert config.get("Healthcheck") is None


def test_no_compilers_or_network_tools_are_installed(image: str) -> None:
    code = (
        "import shutil\n"
        "print([tool for tool in ('gcc', 'cc', 'g++', 'make', 'curl', 'wget', 'git', 'uv')"
        " if shutil.which(tool)])"
    )
    assert _python(image, code).stdout.strip() == "[]"


def test_the_cli_and_the_shipped_alembic_head_work_inside_the_image(image: str) -> None:
    help_result = _run(image, "agent-context", "--help")
    assert help_result.returncode == 0, help_result.stderr[-500:]
    assert "db" in help_result.stdout and "projection" in help_result.stdout

    head = _python(
        image,
        "from agent_context_platform.operations.migrate import alembic_config, image_head;"
        "print(image_head(alembic_config()))",
    )
    assert head.stdout.strip() == _expected_labels()["io.agent-context.alembic.head"]

    no_dsn = _run(image, "agent-context", "db", "upgrade")
    assert no_dsn.returncode == 2
    assert "AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN (or __DSN) is required" in no_dsn.stderr


def test_the_migrate_command_reports_an_unreachable_database_without_the_dsn(
    image: str,
) -> None:
    password = uuid.uuid4().hex
    result = _run(
        image,
        "agent-context",
        "db",
        "upgrade",
        "--expect-head",
        _expected_labels()["io.agent-context.alembic.head"],
        extra=(
            "--env",
            "AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN="
            f"postgresql+psycopg://migrator:{password}@db.invalid/agent_context",
        ),
    )
    assert result.returncode == 1
    assert result.stderr.startswith("error: ")
    assert password not in result.stdout + result.stderr
    assert "db.invalid" not in result.stdout + result.stderr


def test_the_api_boots_read_only_and_answers_health(image: str) -> None:
    name = f"agent-context-platform-api-{uuid.uuid4().hex[:12]}"
    started = _docker("run", "--detach", "--name", name, *FLAGS, image, timeout=120)
    assert started.returncode == 0, started.stderr
    try:
        live = None
        for _ in range(60):
            live = _docker("exec", name, "python", "/app/healthcheck.py", timeout=30)
            if live.returncode == 0:
                break
            time.sleep(0.5)
        assert live is not None and live.returncode == 0, _docker("logs", name).stderr[-1000:]
        # Nothing is configured, so the dependencies are not ready: the probe fails closed.
        ready = _docker("exec", name, "python", "/app/healthcheck.py", "/health/ready")
        assert ready.returncode == 1
        body = _docker(
            "exec",
            name,
            "python",
            "-c",
            "import urllib.request, urllib.error\n"
            "try:\n"
            "    urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=5)\n"
            "except urllib.error.HTTPError as error:\n"
            "    print(error.code, error.headers['Cache-Control'], error.read().decode())\n",
        )
        status, cache, payload = body.stdout.strip().split(" ", 2)
        assert (status, cache) == ("503", "no-store")
        assert json.loads(payload)["status"] == "not_ready"
        assert _docker("exec", name, "python", "/app/healthcheck.py", "/other").returncode == 1
        logs = _docker("logs", name)
        assert "Traceback" not in logs.stdout + logs.stderr
        assert "GET /health" not in logs.stdout + logs.stderr  # no access log
    finally:
        _docker("rm", "--force", name, timeout=120)
