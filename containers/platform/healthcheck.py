"""Container health probe that needs no curl or shell (exec-form HEALTHCHECK).

    python /app/healthcheck.py [path] [port]

Exits 0 when http://127.0.0.1:<port><path> answers 200 within two seconds, 1 otherwise.
The default path is /health/live; use /health/ready to gate traffic on the dependencies.
It prints nothing: the response body never reaches the container log.
"""

from __future__ import annotations

import sys
import urllib.error
import urllib.request

DEFAULT_PATH = "/health/live"
DEFAULT_PORT = 8000
TIMEOUT_SECONDS = 2.0


def main(argv: list[str]) -> int:
    path = argv[1] if len(argv) > 1 else DEFAULT_PATH
    port = int(argv[2]) if len(argv) > 2 else DEFAULT_PORT
    if not path.startswith("/health/"):
        return 1
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}{path}", timeout=TIMEOUT_SECONDS
        ) as response:
            return 0 if response.status == 200 else 1
    except (OSError, ValueError, urllib.error.URLError):
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
