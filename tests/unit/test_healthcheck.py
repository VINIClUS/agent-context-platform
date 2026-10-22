"""The container health probe (containers/platform/healthcheck.py, PLATFORM-049)."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).parents[2] / "containers" / "platform" / "healthcheck.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("platform_healthcheck", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        status = {"/health/live": 200, "/health/ready": 503}.get(self.path, 404)
        self.send_response(status)
        self.end_headers()

    def log_message(self, *_args: object) -> None:
        return None


@pytest.fixture
def port() -> Iterator[int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield int(server.server_address[1])
    server.shutdown()
    server.server_close()


def test_the_probe_reports_the_status_of_a_health_path(port: int) -> None:
    healthcheck = _load()

    assert healthcheck.main(["healthcheck", "/health/live", str(port)]) == 0
    assert healthcheck.main(["healthcheck", "/health/ready", str(port)]) == 1
    assert healthcheck.main(["healthcheck", "/other", str(port)]) == 1
    assert healthcheck.main(["healthcheck", "/health/live", "1"]) == 1


def test_the_probe_never_goes_through_a_configured_proxy(port: int) -> None:
    # A fresh process: urlopen caches its opener (and the proxies it read) process-wide.
    environment = {
        name: value for name, value in os.environ.items() if not name.lower().endswith("_proxy")
    }
    environment.update(http_proxy="http://127.0.0.1:1", HTTP_PROXY="http://127.0.0.1:1")

    result = subprocess.run(
        [sys.executable, "-I", str(SCRIPT), "/health/live", str(port)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr[-500:]
    assert result.stdout == "" and result.stderr == ""
