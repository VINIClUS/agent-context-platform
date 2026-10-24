"""The MCP documentation set must match the code (PLATFORM-048).

Offline and fast: no service, no network. Every relative link resolves; every external link points
at an official MCP, MCP Python SDK or Codex source; every `AGENT_CONTEXT_*` variable is a setting
(or one of the few the Codex client reads, cited below); every `agent-context` command and flag
exists in the CLI. Tables marked `<!-- docs-check: <name> -->` are checked against the code: header
names, JSON-RPC codes, tool names and models, schema files (SDK `0.6.0` schemas have no `$id`, so
a schema is its packaged file, `title` and SHA-256), annotations, tool error codes and their
`retryable` flag, the recorded versions, and the observed behaviour of disabled and legacy
methods, which is replayed against an in-process application.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
import shlex
import tomllib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import mcp.server.streamable_http as sdk_streamable_http
import mcp.shared.inbound as sdk_inbound
import pytest
from agent_context_sdk.registry import TOOL_INPUT_MODELS, TOOL_OUTPUT_MODELS
from fastapi.testclient import TestClient
from mcp_helpers import TOKEN, AuthHarness, mcp_headers, rpc_body
from mcp_types import jsonrpc
from mcp_types.version import MODERN_PROTOCOL_VERSIONS
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import BaseModel
from typer.main import get_command

from agent_context_platform import cli
from agent_context_platform.app import create_app
from agent_context_platform.mcp import auth, tools
from agent_context_platform.mcp.server import MCP_PATH
from agent_context_platform.settings import SECRETS_DIR_ENV, Settings

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
MCP_DOCS = ROOT / "docs" / "mcp"
# The pages this task owns; `conformance.md` belongs to PLATFORM-047.
PAGES = (
    MCP_DOCS / "README.md",
    MCP_DOCS / "protocol-profile.md",
    MCP_DOCS / "tool-contracts.md",
    MCP_DOCS / "deployment-and-scaling.md",
    MCP_DOCS / "legacy-compatibility.md",
    MCP_DOCS / "security.md",
    ROOT / "docs" / "adr" / "0002-stateless-mcp-2026-07-28.md",
)
CONFORMANCE = MCP_DOCS / "conformance.md"

# Documentation placeholders (RFC 2606 example domains), never sources.
PLACEHOLDER_HOSTS = ("example.com",)
# Official sources only (PLATFORM-048): the MCP site, the MCP Python SDK and conformance repos,
# the Codex repo and guide, and the package registries of the pinned versions.
OFFICIAL_URL_PREFIXES = (
    "https://modelcontextprotocol.io/specification/2026-07-28",
    "https://modelcontextprotocol.io/seps/",
    "https://github.com/modelcontextprotocol/python-sdk",
    "https://github.com/modelcontextprotocol/conformance",
    "https://github.com/openai/codex",
    "https://developers.openai.com/codex/",
    "https://pypi.org/project/mcp/",
    "https://www.npmjs.com/package/@modelcontextprotocol/conformance",
)
# Read by the Codex client, not the platform (`agent-context-codex`:
# `src/agent_context_codex/install/mcp.py` and `settings.py`).
CLIENT_VARIABLES = frozenset(
    {
        "AGENT_CONTEXT_MCP_TOKEN",
        "AGENT_CONTEXT_CF_ACCESS_CLIENT_ID",
        "AGENT_CONTEXT_CF_ACCESS_CLIENT_SECRET",
    }
)

_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)\)")
_URL = re.compile(r"https?://[^\s)>`\"']+")
_CODE = re.compile(r"`([^`]+)`")
_VARIABLE = re.compile(r"\bAGENT_CONTEXT_[A-Z0-9_]*[A-Z0-9]")
# Fenced blocks, also those indented under a list item.
_FENCE = re.compile(r"^[ \t]*```[a-z]*\n(.*?)^[ \t]*```", re.MULTILINE | re.DOTALL)
_MARKER = "<!-- docs-check: {name} -->"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _slug(heading: str) -> str:
    """GitHub's heading anchor: lowercase, punctuation dropped, spaces to hyphens."""
    kept = re.sub(r"[^\w\- ]", "", heading.strip().lower().replace("`", ""))
    return kept.replace(" ", "-")


def _anchors(path: Path) -> set[str]:
    return {
        _slug(line.lstrip("#"))
        for line in _text(path).splitlines()
        if line.startswith("#") and not line.startswith("#!")
    }


@dataclass(frozen=True)
class _Link:
    page: Path
    target: str

    def __str__(self) -> str:
        return f"{self.page.relative_to(ROOT)} -> {self.target}"


def _links() -> list[_Link]:
    found: list[_Link] = []
    readme = _text(ROOT / "README.md")
    section = readme[
        readme.index("## MCP memory tools") : readme.index("## Ingestion API contract")
    ]
    for page, text in ((p, _text(p)) for p in PAGES):
        found += [_Link(page, target) for target in _LINK.findall(text)]
    found += [_Link(ROOT / "README.md", target) for target in _LINK.findall(section)]
    return [link for link in found if not link.target.startswith(("http://", "https://"))]


@pytest.mark.parametrize("link", _links(), ids=str)
def test_every_relative_link_resolves(link: _Link) -> None:
    path_part, _, anchor = link.target.partition("#")
    target = (link.page.parent / path_part).resolve() if path_part else link.page
    if target == CONFORMANCE and not target.exists():
        pytest.skip("docs/mcp/conformance.md is owned by PLATFORM-047 and lands at integration")
    assert target.is_file(), f"{link}: no such file"
    if anchor:
        assert anchor in _anchors(target), f"{link}: no heading #{anchor}"


def test_external_links_are_official_sources() -> None:
    urls = {url.rstrip(".,") for page in PAGES for url in _URL.findall(_text(page))}
    assert urls
    sources = {u for u in urls if not (urlparse(u).hostname or "").endswith(PLACEHOLDER_HOSTS)}
    unofficial = sorted(u for u in sources if not u.startswith(OFFICIAL_URL_PREFIXES))
    assert not unofficial, f"not an official MCP, MCP SDK or Codex source: {unofficial}"
    assert all(urlparse(u).scheme == "https" for u in urls)


def _setting_variables() -> set[str]:
    names = {SECRETS_DIR_ENV}
    for field_name, info in Settings.model_fields.items():
        annotation = info.annotation
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            names |= {
                f"AGENT_CONTEXT_{field_name.upper()}__{sub.upper()}"
                for sub in annotation.model_fields
            }
        else:
            names.add(f"AGENT_CONTEXT_{field_name.upper()}")
    return names


def test_every_documented_variable_exists() -> None:
    known = _setting_variables() | CLIENT_VARIABLES
    documented = {name for page in PAGES for name in _VARIABLE.findall(_text(page))}
    assert "AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY" in documented
    assert not documented - known, f"unknown variables: {sorted(documented - known)}"


def _commands() -> Iterator[tuple[Path, str]]:
    for page in PAGES:
        text = _text(page)
        for span in _CODE.findall(text):
            if span.startswith("agent-context "):
                yield page, span
        for block in _FENCE.findall(text):
            for line in (raw.strip() for raw in block.splitlines()):
                if line.startswith("agent-context "):
                    yield page, line.split(" #", 1)[0]


def _leaf(tokens: list[str]) -> tuple[Any, list[str]]:
    """The CLI command a documented line runs (what `--help` describes), and its arguments.

    Duck-typed: Typer builds its commands on its own Click classes."""
    command: Any = get_command(cli.app)
    rest = tokens[1:]
    while rest and rest[0] in getattr(command, "commands", {}):
        command = command.commands[rest.pop(0)]
    return command, rest


def test_every_documented_cli_command_and_flag_exists() -> None:
    seen = 0
    for page, line in _commands():
        command, rest = _leaf(shlex.split(line.replace("<", "'<").replace(">", ">'")))
        assert not hasattr(command, "commands"), f"{page.name}: `{line}` is not a command"
        options = {opt for param in command.params for opt in (*param.opts, *param.secondary_opts)}
        flags = {token.split("=", 1)[0] for token in rest if token.startswith("--")}
        assert flags <= options, f"{page.name}: `{line}` has unknown flags {flags - options}"
        seen += 1
    assert seen >= 4


def test_the_documented_mint_module_flags_exist(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        auth._main(["mint", "--help"])
    help_text = capsys.readouterr().out
    text = _text(MCP_DOCS / "security.md")
    paragraph = text[text.index("`python -m agent_context_platform.mcp.auth mint") :].split("\n\n")[
        0
    ]
    documented = set(re.findall(r"--[a-z-]+", paragraph))
    assert documented and all(flag in help_text for flag in documented)


# --- marked tables ---------------------------------------------------------------------------


def _table(page: Path, name: str) -> list[dict[str, str]]:
    text = _text(page)
    marker = _MARKER.format(name=name)
    assert marker in text, f"{page.name}: missing {marker}"
    lines = text.split(marker, 1)[1].lstrip("\n").splitlines()
    rows = []
    for line in lines:
        if not line.startswith("|"):
            break
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    header, body = rows[0], rows[2:]
    assert body, f"{page.name}: empty {name} table"
    return [dict(zip(header, row, strict=True)) for row in body]


def _code(cell: str) -> list[str]:
    return _CODE.findall(cell)


def _first(cell: str) -> str:
    return _code(cell)[0]


def test_documented_headers_are_handled_in_code() -> None:
    sources = [inspect.getsource(module) for module in (sdk_inbound, sdk_streamable_http, auth)] + [
        (ROOT / "src/agent_context_platform/mcp" / name).read_text(encoding="utf-8")
        for name in ("protocol_guard.py", "server.py")
    ]
    literals = {
        match.lower()
        for source in sources
        for match in re.findall(r"b?[\"']([A-Za-z][A-Za-z0-9-]{2,40})[\"']", source)
    }
    literals |= TraceContextTextMapPropagator().fields | W3CBaggagePropagator().fields
    for row in _table(MCP_DOCS / "protocol-profile.md", "headers"):
        assert _first(row["Header"]).lower() in literals, row["Header"]


def test_documented_jsonrpc_codes_match_the_sdk() -> None:
    for row in _table(MCP_DOCS / "protocol-profile.md", "jsonrpc-codes"):
        code, name = int(_first(row["Code"])), _first(row["Name"])
        assert getattr(jsonrpc, name) == code, row
        if code in sdk_inbound.ERROR_CODE_HTTP_STATUS:
            assert sdk_inbound.ERROR_CODE_HTTP_STATUS[code] == int(row["HTTP"]), row


def test_documented_tools_are_the_registered_tools() -> None:
    rows = _table(MCP_DOCS / "tool-contracts.md", "tools")
    registered = tools.memory_tools()
    assert sorted(_first(row["Tool"]) for row in rows) == sorted(registered)
    for row in rows:
        name = _first(row["Tool"])
        assert _first(row["Input model"]) == TOOL_INPUT_MODELS[name].__name__
        assert _first(row["Output model"]) == TOOL_OUTPUT_MODELS[name].__name__
        schema = registered[name].definition.input_schema
        fields = set(schema["properties"])
        for definition in schema.get("$defs", {}).values():
            fields |= set(definition.get("properties", {}))
        named = {token for token in _code(row["Required scope fields"]) if token.isidentifier()}
        assert named <= fields, f"{name}: {named - fields} are not input fields"
        required = set(schema.get("required", ())) - {"budget"}
        assert required <= named, f"{name}: required fields {required - named} undocumented"


def test_documented_schemas_are_the_sdk_files() -> None:
    directory = resources.files("agent_context_sdk").joinpath("schemas/v1")
    rows = _table(MCP_DOCS / "tool-contracts.md", "schemas")
    assert {(_first(r["Tool"]), r["Direction"]) for r in rows} == {
        (name, direction) for name in tools.memory_tools() for direction in ("input", "output")
    }
    for row in rows:
        name, direction = _first(row["Tool"]), row["Direction"]
        file = _first(row["Schema file"])
        assert file == f"mcp.{name}.{direction}.v1.json"
        data = directory.joinpath(file).read_bytes()
        schema = json.loads(data)
        assert "$id" not in schema  # if the SDK adds one, document it here
        assert _first(row["`title`"]) == schema["title"]
        assert _first(row["SHA-256"]) == hashlib.sha256(data).hexdigest()
        served = tools.memory_tools()[name].definition
        assert (served.input_schema if direction == "input" else served.output_schema) == schema


def test_documented_annotations_are_the_served_ones() -> None:
    page = _text(MCP_DOCS / "tool-contracts.md")
    served = tools.ANNOTATIONS.model_dump(by_alias=True, exclude_none=True)
    section = page.split("## Annotations", 1)[1].split("\n## ", 1)[0]
    documented = {
        cells[0]: json.loads(cells[1])
        for line in section.splitlines()
        if line.startswith("| `") and len(cells := _code(line)) >= 2
    }
    assert documented == {k: v for k, v in served.items() if k.endswith("Hint")}


def _retryable() -> dict[str, bool]:
    kinds = [kind for _, kind in tools._ERRORS]
    kinds += [tools._UNAVAILABLE, tools._INTERNAL, tools._UNAUTHENTICATED]
    flags: dict[str, bool] = {tools.ToolErrorCode.INVALID_ARGUMENTS: False}
    for kind in kinds:
        assert flags.setdefault(kind.code, kind.retryable) == kind.retryable, kind.code
    return flags


def test_documented_tool_error_codes_are_exactly_the_served_ones() -> None:
    rows = _table(MCP_DOCS / "tool-contracts.md", "tool-error-codes")
    documented = {_first(row["Code"]): row["Retryable"] for row in rows}
    assert set(documented) == set(tools.ERROR_CODES)
    retryable = _retryable()
    for code, flag in documented.items():
        assert flag == ("yes" if retryable[code] else "no"), code


def _versions() -> dict[str, str]:
    return {row["Component"]: row["Version"] for row in _table(MCP_DOCS / "README.md", "versions")}


def test_recorded_versions_match_the_locks() -> None:
    versions = _versions()
    pyproject = tomllib.loads(_text(ROOT / "pyproject.toml"))["tool"]["agent-context"]["sdk"]
    lock = tomllib.loads(_text(ROOT / "uv.lock"))
    locked = {package["name"]: package["version"] for package in lock["package"]}
    assert _code(versions["MCP protocol"])[0] in MODERN_PROTOCOL_VERSIONS
    assert list(MODERN_PROTOCOL_VERSIONS) == [_code(versions["MCP protocol"])[0]]
    sdk_cells = _code(versions["Agent Context SDK"])
    assert sdk_cells == [pyproject["version"], pyproject["wheel-sha256"]]
    mcp_cells = _code(versions["MCP Python SDK"])
    assert mcp_cells[1] == locked["mcp"] and mcp_cells[3] == locked["mcp-types"]


def test_the_codex_config_block_names_the_served_tools() -> None:
    text = _text(MCP_DOCS / "README.md")
    block = re.search(r"```toml\n(.*?)```", text, re.DOTALL)
    assert block is not None
    table = tomllib.loads(block.group(1))["mcp_servers"]["agent-context"]
    assert sorted(table["enabled_tools"]) == sorted(tools.memory_tools())
    assert table["url"].endswith(MCP_PATH)
    assert table["bearer_token_env_var"] in CLIENT_VARIABLES
    assert set(table["env_http_headers"].values()) <= CLIENT_VARIABLES


def test_the_deploy_table_lists_every_production_mcp_key() -> None:
    readme = _text(ROOT / "README.md")
    row = next(line for line in readme.splitlines() if line.startswith("| `platform-api`"))
    keys = [name for name in Settings.model_fields["mcp"].annotation.model_fields if "key" in name]  # type: ignore[union-attr]
    assert keys
    for key in keys:
        assert f"`__MCP__{key.upper()}`" in row, key


# --- observed behaviour, replayed in-process -------------------------------------------------


@pytest.fixture(scope="module")
def app_client() -> Iterator[TestClient]:
    harness = AuthHarness()
    harness.add()
    application = create_app(Settings(environment="test"), mcp_auth=harness.runtime())
    with TestClient(application, base_url="http://127.0.0.1:8000") as client:
        client.headers["authorization"] = f"Bearer {TOKEN}"
        yield client


def _post(client: TestClient, method: str, **extra: str) -> Any:
    return client.post(MCP_PATH, headers=mcp_headers(method, **extra), json=rpc_body(method))


def test_disabled_methods_answer_as_documented(app_client: TestClient) -> None:
    for row in _table(MCP_DOCS / "protocol-profile.md", "disabled-methods"):
        method = _first(row["Method"])
        assert _code(row["Observed"]) == ["404", "-32601"], row
        response = _post(app_client, method)
        assert response.status_code == 404, method
        assert response.json()["error"]["code"] == jsonrpc.METHOD_NOT_FOUND, method


def test_the_discovery_example_is_what_the_server_answers(app_client: TestClient) -> None:
    text = _text(MCP_DOCS / "protocol-profile.md").split("## Discovery", 1)[1]
    example = json.loads(text.split("```json\n", 1)[1].split("```", 1)[0])
    assert _post(app_client, "server/discover").json()["result"] == example


def _legacy_initialize(version: str | None) -> Callable[[TestClient], Any]:
    def send(client: TestClient) -> Any:
        headers = mcp_headers("initialize")
        del headers["mcp-protocol-version"]
        if version is not None:
            headers["mcp-protocol-version"] = version
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
        }
        return client.post(MCP_PATH, headers=headers, json=body)

    return send


def _notification(client: TestClient) -> Any:
    body = rpc_body("notifications/initialized")
    del body["id"]
    return client.post(MCP_PATH, headers=mcp_headers("notifications/initialized"), json=body)


def _discover_with(header: str) -> Callable[[TestClient], Any]:
    return lambda client: _post(client, "server/discover", **{header: "legacy-0001"})


# Legacy request (first column, verbatim) -> how to send it, expected status, expected code.
LEGACY: dict[str, tuple[Callable[[TestClient], Any], int, int | None]] = {
    "`initialize` without `MCP-Protocol-Version`": (_legacy_initialize(None), 400, -32022),
    "`initialize` with `MCP-Protocol-Version: 2025-06-18`": (
        _legacy_initialize("2025-06-18"),
        400,
        -32022,
    ),
    "`initialize` with `MCP-Protocol-Version: 2025-11-25`": (
        _legacy_initialize("2025-11-25"),
        400,
        -32022,
    ),
    "`initialize` with `2026-07-28` headers and `_meta`": (
        lambda client: _post(client, "initialize"),
        404,
        -32601,
    ),
    "`notifications/initialized` with `2026-07-28` headers and `_meta`": (_notification, 202, None),
    "`GET /mcp` (standalone SSE stream)": (
        lambda client: client.get(MCP_PATH, headers={"accept": "text/event-stream"}),
        405,
        None,
    ),
    "`DELETE /mcp` (end a session)": (
        lambda client: client.delete(MCP_PATH, headers={"mcp-session-id": "legacy-0001"}),
        405,
        None,
    ),
    "`Mcp-Session-Id` on a `2026-07-28` request": (_discover_with("mcp-session-id"), 200, None),
    "`Last-Event-ID` on a `2026-07-28` request": (_discover_with("last-event-id"), 200, None),
    "`GET /sse` (HTTP+SSE endpoint)": (lambda client: client.get("/sse"), 404, None),
    "`POST /messages` (HTTP+SSE endpoint)": (
        lambda client: client.post("/messages", json=rpc_body("initialize")),
        404,
        None,
    ),
}


def test_legacy_requests_answer_as_documented(app_client: TestClient) -> None:
    rows = _table(MCP_DOCS / "legacy-compatibility.md", "legacy")
    assert [row["Legacy request"] for row in rows] == list(LEGACY)
    for row in rows:
        send, status, code = LEGACY[row["Legacy request"]]
        observed = row["Observed"]
        assert f"`{status}`" in observed, row
        response = send(app_client)
        assert response.status_code == status, row["Legacy request"]
        assert "mcp-session-id" not in response.headers
        if code is not None:
            assert f"`{code}`" in observed, row
            assert response.json()["error"]["code"] == code
        if status == 405:
            assert response.headers["allow"] == "POST"
        if code == jsonrpc.UNSUPPORTED_PROTOCOL_VERSION:
            assert response.json()["error"]["data"]["supported"] == list(MODERN_PROTOCOL_VERSIONS)


def test_documented_input_limits_are_the_enforced_ones() -> None:
    rows = _table(MCP_DOCS / "tool-contracts.md", "input-limits")
    free_text, other = rows
    assert set(_code(free_text["Arguments"])) == set(tools._TEXT_FIELDS)
    assert free_text["At most"] == f"{tools.MAX_TEXT_CHARS} characters"
    assert other["At most"] == f"{tools.MAX_ID_CHARS} characters"
    assert set(_code(other["Arguments"])) >= set(tools._UNBOUNDED_FIELDS) - {"kind"}
