"""Every memory tool is annotated read-only, non-destructive, idempotent and closed-world, and
says why in its description (PLATFORM-046)."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from mcp_helpers import TOKEN, AuthHarness, mcp_headers, rpc_body

from agent_context_platform.app import create_app
from agent_context_platform.mcp.tools import ANNOTATIONS, memory_tools
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit

EXPECTED = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}


def listed_tools() -> list[dict[str, Any]]:
    harness = AuthHarness()
    harness.add()
    with TestClient(
        create_app(Settings(environment="test"), mcp_auth=harness.runtime()),
        base_url="http://127.0.0.1:8000",
    ) as client:
        response = client.post(
            "/mcp",
            headers=mcp_headers("tools/list", authorization=f"Bearer {TOKEN}"),
            json=rpc_body("tools/list"),
        )
    tools: list[dict[str, Any]] = response.json()["result"]["tools"]
    return tools


def test_every_tool_carries_the_same_four_hints_on_the_wire() -> None:
    tools = listed_tools()

    assert len(tools) == 5
    for tool in tools:
        assert tool["annotations"] == EXPECTED, tool["name"]


@pytest.mark.parametrize("name", sorted(memory_tools()))
def test_each_description_justifies_its_hints(name: str) -> None:
    description = memory_tools()[name].definition.description or ""

    for phrase in ("Read-only", "changes nothing", "Idempotent", "no side effects", "Closed world"):
        assert phrase in description, (name, phrase)
    assert "untrusted evidence" in description


def test_the_hints_are_one_shared_definition() -> None:
    assert all(tool.definition.annotations is ANNOTATIONS for tool in memory_tools().values())
    assert ANNOTATIONS.read_only_hint is True and ANNOTATIONS.destructive_hint is False
    assert ANNOTATIONS.idempotent_hint is True and ANNOTATIONS.open_world_hint is False
