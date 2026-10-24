"""The conformance profiles and the facts the applicability manifest relies on (PLATFORM-047).

A capability exclusion is verified, never assumed: the shipping `server/discover` must not
advertise the capability and every method that would serve it must answer -32601. The fixture
tools exist only on the fixture profile, which runs them on the shipping transport stack; nothing
in `src/` can reach either the fixture server or the loopback principal shim; and the pin is kept
in one place that matches the infra lock.
"""

from __future__ import annotations

import ast
import json
import re
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import evaluate
import fixture_server
import loopback
import pytest
from mcp_helpers import mcp_headers, rpc_body
from starlette.testclient import TestClient

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
MANIFEST = evaluate.load_manifest(HERE / "applicability.toml")
MEMORY_TOOLS = {
    "context_for_task",
    "decision_history",
    "failure_history",
    "repository_overview",
    "symbol_context",
}
EXCLUDED_METHODS = sorted(
    (capability.name, method)
    for capability in MANIFEST.capabilities.values()
    if capability.name != "response_stream"
    for method in capability.methods
)


@pytest.fixture(scope="module")
def token() -> str:
    return loopback.new_token()


@pytest.fixture(scope="module")
def shipping(token: str) -> Iterator[TestClient]:
    with TestClient(loopback.shipping_app(token), base_url="http://127.0.0.1:8000") as client:
        yield client


@pytest.fixture(scope="module")
def fixture(token: str) -> Iterator[TestClient]:
    with TestClient(loopback.fixture_app(token), base_url="http://127.0.0.1:8000") as client:
        yield client


def rpc(
    client: TestClient,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    capabilities: dict[str, Any] | None = None,
    **headers: str,
) -> Any:
    body = rpc_body(method, params)
    if capabilities is not None:
        body["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] = capabilities
    if method in {"tools/call", "prompts/get"} and params:
        headers.setdefault("mcp-name", params["name"])
    elif method == "resources/read" and params:
        headers.setdefault("mcp-name", params["uri"])
    return client.post("/mcp", headers=mcp_headers(method, **headers), json=body)


def _tools(client: TestClient) -> list[dict[str, Any]]:
    response = rpc(client, "tools/list")
    assert response.status_code == 200, response.text
    tools: list[dict[str, Any]] = response.json()["result"]["tools"]
    return tools


# --- capability exclusions are verified on the shipping profile --------------------------------


def test_the_shipping_profile_advertises_tools_and_no_excluded_capability(
    shipping: TestClient,
) -> None:
    response = rpc(shipping, "server/discover")
    assert response.status_code == 200, response.text
    capabilities = response.json()["result"]["capabilities"]

    assert capabilities == {"tools": {"listChanged": False}}
    for capability in MANIFEST.capabilities.values():
        if capability.discover_key is not None:
            assert capability.discover_key not in capabilities
    assert "extensions" not in capabilities  # tasks and other extensions are not offered


@pytest.mark.parametrize(("capability", "method"), EXCLUDED_METHODS)
def test_every_method_of_an_excluded_capability_is_not_found(
    shipping: TestClient, capability: str, method: str
) -> None:
    response = rpc(shipping, method, {"name": "x", "uri": "test://x", "ref": {}})

    assert response.status_code == 404, (capability, response.text)
    assert response.json()["error"]["code"] == -32601


@pytest.mark.parametrize("profile", ["shipping", "fixture"])
def test_a_tool_call_is_answered_with_one_json_body_even_when_a_stream_is_accepted(
    request: pytest.FixtureRequest, profile: str
) -> None:
    """`response_stream`: no SSE response, so request-scoped notifications have no wire."""
    client: TestClient = request.getfixturevalue(profile)
    name = "repository_overview" if profile == "shipping" else "test_tool_with_progress"
    arguments: dict[str, Any] = (
        {"repository_id": "repo-1", "commit_id": "a" * 40, "budget": _BUDGET}
        if profile == "shipping"
        else {}
    )
    params = {"name": name, "arguments": arguments, "_meta": {"progressToken": "p-1"}}
    body = rpc_body("tools/call", params)
    body["params"]["_meta"] = {**body["params"]["_meta"], "progressToken": "p-1"}
    response = client.post(
        "/mcp", headers=mcp_headers("tools/call", **{"mcp-name": name}), json=body
    )

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/json")
    payload = response.json()
    assert isinstance(payload, dict) and "result" in payload  # one response, no notification
    assert payload["result"].get("resultType", "complete") == "complete"


_BUDGET = {"max_items": 5, "max_bytes": 20_000, "max_tokens": 5_000}


@pytest.mark.parametrize("name", sorted(MEMORY_TOOLS))
def test_no_memory_tool_asks_the_client_for_input(shipping: TestClient, name: str) -> None:
    """`input_requests`: a call carrying input responses is answered, never re-asked."""
    arguments = _arguments(name)
    params = {"name": name, "arguments": arguments, "inputResponses": {"x": {"action": "accept"}}}
    response = rpc(shipping, "tools/call", params, capabilities={"elicitation": {}, "sampling": {}})

    assert response.status_code == 200, response.text
    result = response.json()["result"]
    assert result.get("resultType", "complete") == "complete"
    assert "inputRequests" not in result


def _arguments(name: str) -> dict[str, Any]:
    commit = "a" * 40
    return {
        "context_for_task": {
            "repository_id": "repo-1",
            "revision": {"kind": "commit", "commit_id": commit},
            "task": "fix the parser",
            "budget": _BUDGET,
        },
        "repository_overview": {"repository_id": "repo-1", "commit_id": commit, "budget": _BUDGET},
        "symbol_context": {
            "repository_id": "repo-1",
            "commit_id": commit,
            "symbol_name": "parse",
            "budget": _BUDGET,
        },
        "decision_history": {
            "scope": {"kind": "project", "project_id": "project-1"},
            "query": "storage engine",
            "valid_at": "2026-09-01T00:00:00Z",
            "budget": _BUDGET,
        },
        "failure_history": {
            "repository_id": "repo-1",
            "query": "timeout",
            "valid_from": "2026-08-01T00:00:00Z",
            "valid_to": "2026-09-01T00:00:00Z",
            "budget": _BUDGET,
        },
    }[name]


# --- fixture tools: only on the fixture profile, on the shipping stack ---------------------------


def test_the_shipping_profile_lists_exactly_the_memory_tools(shipping: TestClient) -> None:
    names = {tool["name"] for tool in _tools(shipping)}

    assert names == MEMORY_TOOLS
    assert not names & fixture_server.FIXTURE_TOOLS
    assert not names & {entry.reason for entry in MANIFEST.entries if entry.kind == "fixture"}


def test_the_fixture_profile_lists_every_fixture_tool_the_manifest_relies_on(
    fixture: TestClient,
) -> None:
    tools = {tool["name"]: tool for tool in _tools(fixture)}

    assert set(tools) == fixture_server.FIXTURE_TOOLS
    assert {e.reason for e in MANIFEST.entries if e.kind == "fixture"} <= set(tools)
    assert all(tool.get("description") for tool in tools.values())
    schema_tool = tools[fixture_server.JSON_SCHEMA_TOOL]
    assert schema_tool["inputSchema"] == fixture_server.JSON_SCHEMA_2020_12
    header = tools["test_custom_headers"]["inputSchema"]["properties"]["region"]
    assert header["x-mcp-header"] == "Region"


def _call(client: TestClient, name: str, **options: Any) -> Any:
    return rpc(client, "tools/call", {"name": name, "arguments": {}}, **options)


@pytest.mark.parametrize(
    ("name", "kinds"),
    [
        ("test_simple_text", ["text"]),
        ("test_image_content", ["image"]),
        ("test_audio_content", ["audio"]),
        ("test_embedded_resource", ["resource"]),
        ("test_multiple_content_types", ["text", "image", "resource"]),
        ("test_streaming_elicitation", ["text"]),
        ("test_logging_tool", ["text"]),
    ],
)
def test_fixture_tools_answer_their_content(
    fixture: TestClient, name: str, kinds: list[str]
) -> None:
    response = _call(fixture, name)

    assert response.status_code == 200, response.text
    result = response.json()["result"]
    assert [block["type"] for block in result["content"]] == kinds
    assert not result.get("isError")


def test_the_error_fixture_answers_a_tool_error(fixture: TestClient) -> None:
    result = _call(fixture, "test_error_handling").json()["result"]
    assert result["isError"] is True
    assert result["content"][0]["text"].endswith(
        "This tool intentionally returns an error for testing"
    )


def test_an_undeclared_client_capability_is_refused_with_32021_and_http_400(
    fixture: TestClient,
) -> None:
    refused = _call(fixture, "test_missing_capability")
    assert refused.status_code == 400
    error = refused.json()["error"]
    assert error["code"] == -32021
    assert error["data"]["requiredCapabilities"] == {"sampling": {}}

    accepted = _call(fixture, "test_missing_capability", capabilities={"sampling": {}})
    assert accepted.status_code == 200
    assert accepted.json()["result"]["content"][0]["type"] == "text"


# --- the loopback principal shim ----------------------------------------------------------------


def test_the_shim_adds_its_token_only_when_no_credential_is_sent(token: str) -> None:
    seen: list[list[tuple[bytes, bytes]]] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append(list(scope.get("headers", [])))

    shim = loopback.LoopbackPrincipal(app, token)
    import asyncio

    asyncio.run(shim({"type": "http", "headers": []}, None, None))
    asyncio.run(
        shim({"type": "http", "headers": [(b"Authorization", b"Bearer other")]}, None, None)
    )
    asyncio.run(shim({"type": "lifespan"}, None, None))

    assert seen[0] == [(b"authorization", f"Bearer {token}".encode())]
    assert seen[1] == [(b"Authorization", b"Bearer other")]
    assert seen[2] == []


def test_a_wrong_credential_is_still_refused_behind_the_shim(shipping: TestClient) -> None:
    wrong = f"{loopback.TOKEN_PREFIX}.{'w' * 43}"
    response = rpc(shipping, "tools/list", authorization=f"Bearer {wrong}")
    assert response.status_code == 401
    assert rpc(shipping, "tools/list").status_code == 200


def test_the_profiles_bind_loopback_only() -> None:
    source = (HERE / "loopback.py").read_text(encoding="utf-8")
    assert 'LOOPBACK: Final = "127.0.0.1"' in source
    assert "listener.bind((LOOPBACK, 0))" in source


# --- isolation from src and the single pin -------------------------------------------------------

TEST_ONLY_MODULES = {"loopback", "fixture_server", "evaluate", "secret_scan", "mcp_helpers"}


def _imports(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_src_never_imports_the_conformance_profiles_or_any_test_module() -> None:
    offenders = []
    for path in sorted((ROOT / "src").rglob("*.py")):
        for name in _imports(path):
            root = name.split(".")[0]
            if root in TEST_ONLY_MODULES or root in {"tests", "conftest"}:
                offenders.append(f"{path.relative_to(ROOT)}: {name}")
        text = path.read_text(encoding="utf-8")
        for marker in ("LoopbackPrincipal", "create_fixture_server", "tests.mcp"):
            if marker in text:
                offenders.append(f"{path.relative_to(ROOT)}: mentions {marker}")
    assert offenders == []


def test_the_pin_lives_in_one_place_and_matches_the_infra_lock() -> None:
    pin = tomllib.loads((HERE / "conformance.lock").read_text(encoding="utf-8"))
    doc = (ROOT / "docs" / "mcp" / "conformance.md").read_text(encoding="utf-8")
    block = re.search(r"```json\n(\{\n  \"name\": \"mcp-conformance\".*?\n\})\n```", doc, re.S)
    assert block is not None, "the doc must quote the infra lock entry"
    infra = json.loads(block.group(1))

    assert infra["constraint"] == "exact"
    assert infra["version"] == pin["version"]
    assert infra["source"]["package"] == pin["package"]
    assert infra["source"]["protocol_requirements"] == pin["requirements"]
    for path in (
        ROOT / "scripts" / "run-mcp-conformance.sh",
        ROOT / ".github" / "workflows" / "mcp-conformance.yml",
    ):
        text = path.read_text(encoding="utf-8")
        assert pin["version"] not in text, f"{path.name} must read the pin, not repeat it"
    assert "conformance.lock" in (ROOT / "scripts" / "run-mcp-conformance.sh").read_text("utf-8")


def test_the_doc_tables_every_manifest_entry() -> None:
    doc = (ROOT / "docs" / "mcp" / "conformance.md").read_text(encoding="utf-8")
    table = doc[doc.index("## Manifest") : doc.index("## Last run")]
    rows = [line for line in table.splitlines() if line.startswith("| `")]

    assert len(rows) == len(MANIFEST.entries)
    for row, entry in zip(rows, MANIFEST.entries, strict=True):
        assert row.startswith(f"| `{entry.scenario}` | ")
        assert all(f"`{check}`" in row for check in entry.checks)
        assert f"`{entry.reason}`" in row
        assert entry.source in row
