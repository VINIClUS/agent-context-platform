from __future__ import annotations

import sys
from pathlib import Path

# The MCP suite's in-process helpers (`tests/mcp/mcp_helpers.py`) are a plain module there, not a
# package: make them importable for the documentation checks that replay requests.
_MCP_TESTS = str(Path(__file__).resolve().parents[1] / "mcp")
if _MCP_TESTS not in sys.path:
    sys.path.insert(0, _MCP_TESTS)
