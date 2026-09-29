"""Export the frozen ingestion v1 OpenAPI snapshot.

The snapshot covers ``/v1/ingestion/*`` and the components those paths
reference, transitively. Health, MCP and admin routes are deliberately outside
it so later tasks can add them without touching the v1 contract.

    uv run python scripts/export-openapi.py           # rewrite the snapshot
    uv run python scripts/export-openapi.py --check   # exit 1 on drift

Output is deterministic: sorted keys, 2-space indent, UTF-8, trailing newline.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Final

from agent_context_platform.app import create_app
from agent_context_platform.settings import Settings

SNAPSHOT_PATH: Final = Path(__file__).resolve().parents[1] / "openapi" / "agent-context-v1.json"
INGESTION_PREFIX: Final = "/v1/ingestion/"
_REF_PREFIX: Final = "#/components/"


def _references(node: Any) -> set[tuple[str, str]]:
    """Every ``(component kind, name)`` referenced anywhere below ``node``."""
    found: set[tuple[str, str]] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str) and value.startswith(_REF_PREFIX):
                kind, _, name = value.removeprefix(_REF_PREFIX).partition("/")
                found.add((kind, name))
            else:
                found |= _references(value)
    elif isinstance(node, list):
        for item in node:
            found |= _references(item)
    return found


def ingestion_openapi() -> dict[str, Any]:
    """The ingestion-only OpenAPI document derived from the live application."""
    full = create_app(Settings(environment="test")).openapi()
    paths = {
        path: item for path, item in full["paths"].items() if path.startswith(INGESTION_PREFIX)
    }
    components = full.get("components", {})
    selected: dict[str, dict[str, Any]] = {}
    pending = _references(paths)
    while pending:
        kind, name = pending.pop()
        if name in selected.get(kind, {}):
            continue
        component = components[kind][name]
        selected.setdefault(kind, {})[name] = component
        pending |= _references(component)
    for operation in (op for item in paths.values() for op in item.values()):
        for requirement in operation.get("security", []):
            for name in requirement:
                selected.setdefault("securitySchemes", {})[name] = components["securitySchemes"][
                    name
                ]
    return {
        "openapi": full["openapi"],
        "info": {"title": "Agent Context Ingestion API", "version": "v1"},
        "paths": paths,
        "components": selected,
    }


def render() -> bytes:
    document = json.dumps(ingestion_openapi(), sort_keys=True, indent=2, ensure_ascii=False)
    return (document + "\n").encode("utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the snapshot differs")
    arguments = parser.parse_args(argv)
    rendered = render()
    if arguments.check:
        if SNAPSHOT_PATH.read_bytes() != rendered:
            print(
                f"{SNAPSHOT_PATH.name} drifted: ingestion API v1 is immutable; "
                "a change needs a new API version.",
                file=sys.stderr,
            )
            return 1
        return 0
    SNAPSHOT_PATH.write_bytes(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
