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

import asyncio
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

from agent_context_platform import app as app_module
from agent_context_platform import cli
from agent_context_platform.app import create_app
from agent_context_platform.mcp import auth, tools
from agent_context_platform.mcp.auth import MEMORY_READ_SCOPE, McpAuthHolder, require_scope
from agent_context_platform.mcp.server import (
    LIST_CACHE_HINT,
    MCP_PATH,
    build_mcp_mount,
    create_mcp_server,
)
from agent_context_platform.retrieval import composer, cursor
from agent_context_platform.settings import SECRETS_DIR_ENV, MCPSettings, Settings

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
# Package registries: version and distribution references only, never a protocol source, so
# they may appear only in the README's verified-versions table.
REGISTRY_PREFIXES = ("https://pypi.org/", "https://www.npmjs.com/")
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


def test_the_codex_config_block_is_what_the_installer_writes() -> None:
    """Value by value against the installer's owned values, vendored with their source commit
    (`agent-context-codex` is not importable here)."""
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/codex_mcp_owned_values.json").read_text()
    )
    text = _text(MCP_DOCS / "README.md")
    block = re.search(r"```toml\n(.*?)```", text, re.DOTALL)
    assert block is not None
    table = tomllib.loads(block.group(1))["mcp_servers"]["agent-context"]
    assert list(table) == ["url", *fixture["values"]]
    assert {k: v for k, v in table.items() if k != "url"} == fixture["values"]
    assert table["url"].endswith(MCP_PATH) and fixture["url"].endswith(MCP_PATH)
    assert sorted(fixture["values"]["enabled_tools"]) == sorted(tools.memory_tools())
    assert fixture["values"]["bearer_token_env_var"] in CLIENT_VARIABLES
    assert set(fixture["values"]["env_http_headers"].values()) <= CLIENT_VARIABLES


def test_registries_are_cited_only_as_version_references() -> None:
    versions = _text(MCP_DOCS / "README.md").split(_MARKER.format(name="versions"), 1)[1]
    versions_rows = versions.lstrip("\n").split("\n\n", 1)[0]
    for page in PAGES:
        for line in _text(page).splitlines():
            if any(prefix in line for prefix in REGISTRY_PREFIXES):
                assert line in versions_rows, f"{page.name}: a registry cited outside versions"


def test_the_replica_shared_key_is_the_cursor_key() -> None:
    wired = re.search(
        r"CursorCodec\.from_key\(settings\.mcp\.(\w+)\)", inspect.getsource(app_module)
    )
    assert wired is not None
    expected = f"AGENT_CONTEXT_MCP__{wired.group(1).upper()}"
    shared = [
        name
        for page in PAGES
        for name in re.findall(r"must share `(AGENT_CONTEXT_[A-Z_]+)`", _flat(page))
    ]
    assert shared and set(shared) == {expected}


def _flat(page: Path) -> str:
    return " ".join(_text(page).split())


def _hmac_minimum() -> int:
    found = re.search(r"< (\d+)", inspect.getsource(MCPSettings.require_long_hmac_key))
    assert found is not None
    return int(found.group(1))


def _ttl_maximum() -> int:
    field = MCPSettings.model_fields["principal_cache_ttl_seconds"]
    return int(next(meta.le for meta in field.metadata if hasattr(meta, "le")))


def _defaults() -> MCPSettings:
    return MCPSettings()


# Every documented number, as a pattern over the pages (whitespace folded) and the source values
# its groups must equal; each pattern must match at least once.
FACTS: tuple[tuple[str, Callable[[], tuple[int, ...]]], ...] = (
    (
        r"(\d+) per second, burst (\d+)",
        lambda: (int(_defaults().rate_limit_per_second), _defaults().rate_limit_burst),
    ),
    (r"(\d+) MiB", lambda: (_defaults().max_request_body_bytes // 1_048_576,)),
    (r"(\d+) minutes", lambda: (int(cursor.DEFAULT_TTL.total_seconds() // 60),)),
    (
        r"(\d+) connections plus (\d+) overflow, (\d+) s",
        lambda: (
            app_module.COMPOSER_POOL_SIZE,
            app_module.COMPOSER_POOL_OVERFLOW,
            int(app_module.COMPOSER_POOL_TIMEOUT_SECONDS),
        ),
    ),
    (
        r"connect within at most (\d+) s, wait at most (\d+) s for a connection and retry a "
        r"transaction for at most (\d+) s",
        lambda: (
            int(app_module.COMPOSER_NEO4J_CONNECT_SECONDS),
            int(app_module.COMPOSER_NEO4J_ACQUIRE_SECONDS),
            int(app_module.COMPOSER_NEO4J_RETRY_SECONDS),
        ),
    ),
    (r"(\d+) s deadline", lambda: (int(composer.DEFAULT_DEADLINE_SECONDS),)),
    (r"Tool request deadline \| (\d+) s", lambda: (int(composer.DEFAULT_DEADLINE_SECONDS),)),
    (r"at most (\d+) items whatever the budget", lambda: (composer.MAX_PAGE_ITEMS,)),
    (
        r"at most (\d+) characters, at most (\d+) items",
        lambda: (tools.MAX_TEXT_TITLE_CHARS, tools.MAX_TEXT_ITEMS),
    ),
    (r"(\d+) s by default", lambda: (int(_defaults().principal_cache_ttl_seconds),)),
    (
        r"(\d+) s by default, at most (\d+)",
        lambda: (int(_defaults().principal_cache_ttl_seconds), _ttl_maximum()),
    ),
    (
        r"(\d+) s \(max (\d+)\)",
        lambda: (int(_defaults().principal_cache_ttl_seconds), _ttl_maximum()),
    ),
    (
        r"default (\d+), max (\d+)",
        lambda: (int(_defaults().principal_cache_ttl_seconds), _ttl_maximum()),
    ),
    (r"[Dd]efault (\d+) s\b", lambda: (int(_defaults().principal_cache_ttl_seconds),)),
    (r"[Aa]t least (\d+) characters", lambda: (_hmac_minimum(),)),
)


@pytest.mark.parametrize(("pattern", "source"), FACTS, ids=[fact[0] for fact in FACTS])
def test_documented_numbers_are_the_source_constants(
    pattern: str, source: Callable[[], tuple[int, ...]]
) -> None:
    found = [
        tuple(int(group) for group in match.groups())
        for page in PAGES
        for match in re.finditer(pattern, _flat(page))
    ]
    assert found, f"no documented value matches {pattern!r}"
    assert set(found) == {source()}, f"{pattern!r}: documented {sorted(set(found))}"


def test_documented_cache_lifetimes_are_the_served_ones() -> None:
    found = {int(v) for page in PAGES for v in re.findall(r"ttlMs: (\d+)", _flat(page))}
    assert LIST_CACHE_HINT.ttl_ms in found
    assert found <= {LIST_CACHE_HINT.ttl_ms, 0}  # 0: `server/discover`, checked live below


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


# --- raw ASGI requests: no client-supplied default header (an HTTP client adds `Accept: */*`) ----


@dataclass(frozen=True)
class _Raw:
    status: int
    headers: dict[str, str]
    body: bytes


def _raw(
    headers: dict[str, str],
    body: bytes,
    *,
    method: str = "POST",
    holder: McpAuthHolder | None = None,
    repeat: int = 1,
) -> _Raw:
    """Send `repeat` identical raw requests to the `/mcp` stack the application mounts; the
    last answer is returned."""

    async def run() -> _Raw:
        if holder is None:
            harness = AuthHarness()
            harness.add()
            gate_holder = McpAuthHolder(harness.runtime())
        else:
            gate_holder = holder
        mount = build_mcp_mount(
            create_mcp_server(composer=tools.ComposerHolder()),
            MCPSettings(),
            access_gate=require_scope(MEMORY_READ_SCOPE, gate_holder),
        )
        sent: list[dict[str, Any]] = []
        async with mount.lifespan():
            for _ in range(repeat):
                sent = await _exchange(mount.asgi_app, method, headers, body)
        start = next(m for m in sent if m["type"] == "http.response.start")
        return _Raw(
            int(start["status"]),
            {k.decode().lower(): v.decode() for k, v in start["headers"]},
            b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body"),
        )

    return asyncio.run(run())


async def _exchange(
    app: Any, method: str, headers: dict[str, str], body: bytes
) -> list[dict[str, Any]]:
    """One raw ASGI request; every message the application sent."""
    sent: list[dict[str, Any]] = []
    messages = [{"type": "http.request", "body": body, "more_body": False}]

    async def receive() -> dict[str, Any]:
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": MCP_PATH,
        "raw_path": MCP_PATH.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
        "client": ("127.0.0.1", 50000),
        "server": ("127.0.0.1", 8000),
    }
    await app(scope, receive, send)
    return sent


def _raw_headers(method: str = "server/discover", **changes: str | None) -> dict[str, str]:
    headers: dict[str, str] = {
        "host": "127.0.0.1:8000",
        "authorization": f"Bearer {TOKEN}",
        "content-type": "application/json",
        "accept": "application/json, text/event-stream",
        "mcp-protocol-version": MODERN_PROTOCOL_VERSIONS[0],
        "mcp-method": method,
    }
    for name, value in changes.items():
        key = name.replace("_", "-")
        if value is None:
            headers.pop(key, None)
        else:
            headers[key] = value
    return headers


_DISCOVER = json.dumps(rpc_body("server/discover")).encode()
TRANSPORT: dict[str, dict[str, str]] = {
    "no `Accept` header": _raw_headers(accept=None),
    "`Accept: text/event-stream` only": _raw_headers(accept="text/event-stream"),
    "`Accept: application/json`": _raw_headers(accept="application/json"),
    "`Accept: */*`": _raw_headers(accept="*/*"),
    "no `Content-Type` header": _raw_headers(content_type=None),
    "`Content-Type: text/plain`": _raw_headers(content_type="text/plain"),
}


def _expect(observed: str) -> tuple[int, bytes | None]:
    """The status and, when the cell states one, the exact body of an Observed cell."""
    status = int(_code(observed)[0])
    if "empty body" in observed or observed.endswith("empty"):
        return status, b""
    if "plain text" in observed:
        return status, _code(observed)[1].encode()
    return status, None


def test_transport_rejections_answer_as_documented() -> None:
    rows = _table(MCP_DOCS / "protocol-profile.md", "transport-rejections")
    assert [row["Request"] for row in rows] == list(TRANSPORT)
    for row in rows:
        status, body = _expect(row["Observed"])
        answer = _raw(TRANSPORT[row["Request"]], _DISCOVER)
        assert answer.status == status, row["Request"]
        if body is not None:
            assert answer.body == body, row["Request"]


def _notification() -> bytes:
    body = rpc_body("notifications/initialized")
    del body["id"]
    return json.dumps(body).encode()


def _scopeless_holder() -> McpAuthHolder:
    harness = AuthHarness()
    harness.add(scopes=("events:ingest",))
    return McpAuthHolder(harness.runtime())


def _limited_holder() -> McpAuthHolder:
    harness = AuthHarness()
    harness.add()
    return McpAuthHolder(harness.runtime(rate_per_second=0.001, burst=1))


def test_non_jsonrpc_rejections_answer_as_documented() -> None:
    probes: dict[int, _Raw] = {
        401: _raw(_raw_headers(authorization=None), _DISCOVER),
        403: _raw(_raw_headers(), _DISCOVER, holder=_scopeless_holder()),
        503: _raw(_raw_headers(), _DISCOVER, holder=McpAuthHolder(None)),
        429: _raw(_raw_headers(), _DISCOVER, holder=_limited_holder(), repeat=2),
        405: _raw(_raw_headers(), b"", method="GET"),
        406: _raw(_raw_headers(accept=None), _DISCOVER),
        400: _raw(_raw_headers(content_type=None), _DISCOVER),
        202: _raw(_raw_headers("notifications/initialized"), _notification()),
    }
    rows = _table(MCP_DOCS / "protocol-profile.md", "non-jsonrpc")
    documented = {int(code) for row in rows for code in _code(row["HTTP"])}
    assert documented == set(probes)
    for row in rows:
        for code in (int(c) for c in _code(row["HTTP"])):
            answer = probes[code]
            assert answer.status == code, row
            if row["Body"] == "empty":
                assert answer.body == b"", code
            elif row["Body"].startswith("JSON"):
                assert set(json.loads(answer.body)) == {"error", "message"}, code
            else:
                assert answer.body == _code(row["Body"])[0].encode(), code
    assert "retry-after" in probes[429].headers and probes[405].headers["allow"] == "POST"


def _library_class(library: str, name: str) -> type[BaseException]:
    import builtins

    import botocore.exceptions
    import neo4j.exceptions
    import sqlalchemy.exc

    module = {
        "botocore": botocore.exceptions,
        "SQLAlchemy": sqlalchemy.exc,
        "Neo4j": neo4j.exceptions,
        "": builtins,
    }[library]
    found = getattr(module, name)
    assert isinstance(found, type) and issubclass(found, BaseException), name
    return found


def _kind_of(error_type: type[BaseException]) -> Any:
    """What `_classify` answers for an exception of `error_type` (its order, without an instance)."""
    for handled, kind in tools._ERRORS:
        if issubclass(error_type, handled):
            return kind
    if issubclass(error_type, OSError):
        return tools._UNAVAILABLE
    return tools._INTERNAL


def test_documented_store_failures_are_classified_as_documented() -> None:
    rows = _table(MCP_DOCS / "tool-contracts.md", "store-failures")
    for row in rows:
        exceptions = row["Exceptions"]
        library = next(
            (name for name in ("botocore", "SQLAlchemy", "Neo4j") if exceptions.startswith(name)),
            "",
        )
        code, retryable, logged = _first(row["Code"]), row["Retryable"], row["Logged"]
        if "DatabaseNotFound" in exceptions:
            assert _code(exceptions)[1] == tools._DATABASE_NOT_FOUND
            kinds = [tools._MISCONFIGURED_KIND]
        else:
            names = [name for name in _code(exceptions) if name.isidentifier()]
            kinds = [_kind_of(_library_class(library, name)) for name in names]
        for kind in kinds:
            assert kind.code == code, row
            assert ("yes" if kind.retryable else "no") == retryable, row
            is_logged = kind is tools._INTERNAL or kind.misconfigured
            assert ("yes" if is_logged else "no") == logged, row
