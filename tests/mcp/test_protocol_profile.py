from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp_helpers import (
    CAPABILITIES_META_KEY,
    PROTOCOL_VERSION,
    SERVER_INFO_META_KEY,
    TOKEN,
    VERSION_META_KEY,
    AuthHarness,
    OpenGate,
    mcp_client,
    mcp_headers,
    rpc_body,
)
from mcp_types import HEADER_MISMATCH
from opentelemetry import baggage, trace

from agent_context_platform.mcp.server import create_mcp_server

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736


async def post(
    client: httpx2.AsyncClient,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> httpx2.Response:
    if headers is None:
        name = {"mcp-name": params["name"]} if method == "tools/call" and params else {}
        headers = mcp_headers(method, **name)
    return await client.post(
        "/mcp",
        headers=headers,
        json=body if body is not None else rpc_body(method, params),
    )


def build_tool_server() -> Any:
    server = create_mcp_server(advertise_tools=True)

    # Registered out of order on purpose: listing must not depend on it.
    @server.tool()
    def zeta() -> str:
        return "z"

    @server.tool()
    def alpha() -> str:
        return "a"

    @server.tool()
    def baggage_seen() -> str:
        return json.dumps(dict(baggage.get_all()))

    @server.tool()
    def trace_id() -> str:
        return format(trace.get_current_span().get_span_context().trace_id, "032x")

    return server


@pytest.fixture
async def tool_client() -> AsyncIterator[httpx2.AsyncClient]:
    async with mcp_client(build_tool_server()) as http_client:
        yield http_client


async def test_direct_request_without_discovery_is_served(tool_client: httpx2.AsyncClient) -> None:
    response = await post(tool_client, "tools/call", {"name": "alpha", "arguments": {}})

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["resultType"] == "complete"
    assert result["content"][0]["text"] == "a"
    assert "mcp-session-id" not in response.headers


async def test_discovery_reports_versions_and_only_served_capabilities(
    client: httpx2.AsyncClient,
) -> None:
    response = await post(client, "server/discover")

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["resultType"] == "complete"
    assert result["supportedVersions"] == [PROTOCOL_VERSION]
    # Tools are advertised only once PLATFORM-046 registers them.
    assert result["capabilities"] == {}
    assert "mcp-session-id" not in response.headers


async def test_discovery_advertises_tools_without_change_streams(
    tool_client: httpx2.AsyncClient,
) -> None:
    result = (await post(tool_client, "server/discover")).json()["result"]

    assert result["capabilities"] == {"tools": {"listChanged": False}}


async def test_results_carry_server_info(client: httpx2.AsyncClient) -> None:
    result = (await post(client, "server/discover")).json()["result"]

    info = result["_meta"][SERVER_INFO_META_KEY]
    assert info["name"] == "agent-context-platform"
    assert info["version"]


@pytest.mark.parametrize("value", [None, "", "2025-06-18", "2025-11-25", "1999-01-01", "garbage"])
async def test_missing_or_unsupported_version_header_is_rejected(
    client: httpx2.AsyncClient, value: str | None
) -> None:
    headers = mcp_headers("server/discover")
    if value is None:
        del headers["mcp-protocol-version"]
    else:
        headers["mcp-protocol-version"] = value

    response = await post(client, "server/discover", headers=headers)

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["data"]["supported"] == [PROTOCOL_VERSION]
    assert response.json()["id"] == 1
    assert "mcp-session-id" not in response.headers


async def test_handshake_request_is_never_served(client: httpx2.AsyncClient) -> None:
    legacy = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    no_version = mcp_headers("initialize")
    del no_version["mcp-protocol-version"]
    old_version = {**no_version, "mcp-protocol-version": "2025-06-18"}
    modern = mcp_headers("initialize")

    for headers in (no_version, old_version):
        response = await post(client, "initialize", headers=headers, body=legacy)
        assert response.status_code == 400
        assert "result" not in response.json()

    response = await post(client, "initialize", headers=modern)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == -32601


async def test_header_and_body_version_mismatch_is_rejected(client: httpx2.AsyncClient) -> None:
    body = rpc_body("server/discover")
    body["params"]["_meta"][VERSION_META_KEY] = "2025-11-25"

    response = await post(client, "server/discover", body=body)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == HEADER_MISMATCH


async def test_header_and_body_method_mismatch_is_rejected(client: httpx2.AsyncClient) -> None:
    response = await post(
        client,
        "server/discover",
        headers=mcp_headers("tools/list"),
        body=rpc_body("server/discover"),
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == HEADER_MISMATCH


async def test_body_without_version_envelope_is_rejected(client: httpx2.AsyncClient) -> None:
    body = {"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {}}

    response = await post(client, "server/discover", body=body)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32602


async def test_results_declare_result_type_and_cache_metadata(
    tool_client: httpx2.AsyncClient,
) -> None:
    listing = (await post(tool_client, "tools/list")).json()["result"]
    call = (await post(tool_client, "tools/call", {"name": "alpha", "arguments": {}})).json()[
        "result"
    ]

    assert listing["resultType"] == "complete"
    assert call["resultType"] == "complete"
    assert listing["ttlMs"] == 60_000
    assert listing["cacheScope"] == "private"


async def test_tool_listing_order_is_deterministic(tool_client: httpx2.AsyncClient) -> None:
    first = (await post(tool_client, "tools/list")).json()["result"]["tools"]
    second = (await post(tool_client, "tools/list")).json()["result"]["tools"]

    assert [tool["name"] for tool in first] == ["alpha", "baggage_seen", "trace_id", "zeta"]
    assert first == second


async def test_two_independent_replicas_answer_identically() -> None:
    async with mcp_client(build_tool_server()) as replica_a, mcp_client(build_tool_server()) as b:
        first = await post(replica_a, "tools/list")
        second = await post(b, "tools/list")

    assert first.json() == second.json()


@pytest.mark.parametrize("method", ["GET", "DELETE", "PUT", "PATCH"])
async def test_non_post_methods_are_rejected(client: httpx2.AsyncClient, method: str) -> None:
    response = await client.request(method, "/mcp", headers=mcp_headers("server/discover"))

    assert response.status_code == 405
    assert response.headers["allow"] == "POST"
    assert "mcp-session-id" not in response.headers


async def test_get_stream_is_rejected_even_without_version_header(
    client: httpx2.AsyncClient,
) -> None:
    response = await client.get("/mcp", headers={"accept": "text/event-stream"})

    assert response.status_code == 405


@pytest.mark.parametrize("method", ["resources/list", "prompts/list", "subscriptions/listen"])
async def test_unbacked_methods_are_not_found(client: httpx2.AsyncClient, method: str) -> None:
    response = await post(client, method)

    assert response.json()["error"]["code"] == -32601


async def test_tools_are_not_served_before_they_exist(client: httpx2.AsyncClient) -> None:
    response = await post(client, "tools/list")

    assert response.json()["error"]["code"] == -32601


async def test_no_redirect_on_exact_path(client: httpx2.AsyncClient) -> None:
    response = await post(client, "server/discover")

    assert response.status_code == 200
    assert not response.history


async def test_disallowed_host_is_rejected_without_echo(client: httpx2.AsyncClient) -> None:
    headers = {**mcp_headers("server/discover"), "host": "evil.example"}

    response = await post(client, "server/discover", headers=headers)

    assert response.status_code == 421
    assert "evil.example" not in response.text


async def test_host_port_wildcard_and_exact_matches() -> None:
    async with mcp_client(allowed_hosts=("mcp.example.test", "local:*")) as client:
        for host, status in (
            ("mcp.example.test", 200),
            ("local:9000", 200),
            ("local", 421),
            ("local:", 421),
            ("mcp.example.test:8080", 421),
            ("localx:1", 421),
        ):
            headers = {**mcp_headers("server/discover"), "host": host}
            response = await post(client, "server/discover", headers=headers)
            assert response.status_code == status, host


async def test_origin_is_optional_but_checked_when_present() -> None:
    async with mcp_client(allowed_origins=("https://app.example.test",)) as client:
        absent = await post(client, "server/discover")
        listed = await post(
            client,
            "server/discover",
            headers={**mcp_headers("server/discover"), "origin": "https://app.example.test"},
        )
        unlisted = await post(
            client,
            "server/discover",
            headers={**mcp_headers("server/discover"), "origin": "https://evil.example"},
        )

    assert absent.status_code == 200
    assert listed.status_code == 200
    assert unlisted.status_code == 403
    assert "evil.example" not in unlisted.text


async def test_origin_port_wildcard() -> None:
    async with mcp_client(allowed_origins=("http://localhost:*",)) as client:
        ok = await post(
            client,
            "server/discover",
            headers={**mcp_headers("server/discover"), "origin": "http://localhost:3000"},
        )
        bad = await post(
            client,
            "server/discover",
            headers={**mcp_headers("server/discover"), "origin": "http://localhost"},
        )

    assert ok.status_code == 200
    assert bad.status_code == 403


async def test_declared_oversized_body_is_rejected() -> None:
    async with mcp_client(max_request_body_bytes=200) as client:
        response = await client.post(
            "/mcp", headers=mcp_headers("server/discover"), content=b"x" * 500
        )

    assert response.status_code == 413
    assert "xxx" not in response.text


async def test_invalid_content_length_is_rejected() -> None:
    async with mcp_client() as client:
        response = await client.post(
            "/mcp",
            headers={**mcp_headers("server/discover"), "content-length": "abc"},
            content=b"{}",
        )

    assert response.status_code == 413


async def test_streamed_body_over_limit_is_rejected_without_content_length() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(10):
            yield b"x" * 100

    async with mcp_client(max_request_body_bytes=250) as client:
        request = client.build_request(
            "POST", "/mcp", headers=mcp_headers("server/discover"), content=chunks()
        )
        assert "content-length" not in request.headers
        response = await client.send(request)

    assert response.status_code == 413


async def test_understated_content_length_cannot_bypass_limit() -> None:
    sent: list[dict[str, Any]] = []
    received: list[dict[str, Any]] = [
        {"type": "http.request", "body": b"x" * 100, "more_body": True},
        {"type": "http.request", "body": b"x" * 100, "more_body": False},
    ]

    async def receive() -> dict[str, Any]:
        return received.pop(0)

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    from agent_context_platform.mcp.protocol_guard import ProtocolGuard

    async def downstream(*_args: Any) -> None:
        raise AssertionError("oversized request reached the MCP app")

    guard = ProtocolGuard(
        downstream,  # type: ignore[arg-type]
        allowed_hosts=("h",),
        allowed_origins=(),
        max_body_bytes=150,
        access_gate=OpenGate(),
    )
    scope = {
        "type": "http",
        "method": "POST",
        "headers": [
            (b"host", b"h"),
            (b"content-length", b"10"),
            (b"mcp-protocol-version", PROTOCOL_VERSION.encode()),
        ],
    }
    await guard(scope, receive, send)  # type: ignore[arg-type]

    assert sent[0]["status"] == 413


async def test_client_disconnect_while_reading_body_is_silent() -> None:
    from agent_context_platform.mcp.protocol_guard import ProtocolGuard

    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    async def downstream(*_args: Any) -> None:
        raise AssertionError("disconnected request reached the MCP app")

    guard = ProtocolGuard(
        downstream,  # type: ignore[arg-type]
        allowed_hosts=("h",),
        allowed_origins=(),
        max_body_bytes=10,
        access_gate=OpenGate(),
    )
    scope = {
        "type": "http",
        "method": "POST",
        "headers": [(b"host", b"h"), (b"mcp-protocol-version", PROTOCOL_VERSION.encode())],
    }
    await guard(scope, receive, send)  # type: ignore[arg-type]

    assert sent == []


async def test_non_http_scopes_pass_through() -> None:
    from agent_context_platform.mcp.protocol_guard import ProtocolGuard

    seen: list[str] = []

    async def downstream(scope: Any, _receive: Any, _send: Any) -> None:
        seen.append(scope["type"])

    guard = ProtocolGuard(
        downstream, allowed_hosts=(), allowed_origins=(), max_body_bytes=1, access_gate=OpenGate()
    )
    await guard({"type": "lifespan"}, None, None)  # type: ignore[arg-type]

    assert seen == ["lifespan"]


async def test_rejections_and_logs_do_not_contain_request_bodies(
    client: httpx2.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "sk-secret-body-marker"
    body = rpc_body("server/discover", {"arguments": {"token": secret}})
    body["params"]["_meta"][CAPABILITIES_META_KEY] = "not-an-object"

    with caplog.at_level("DEBUG"):
        response = await post(client, "server/discover", body=body)
        malformed = await client.post(
            "/mcp", headers=mcp_headers("server/discover"), content=secret.encode()
        )

    assert secret not in response.text
    assert secret not in malformed.text
    assert secret not in caplog.text


async def test_traceparent_header_parents_the_request(tool_client: httpx2.AsyncClient) -> None:
    headers = mcp_headers("tools/call", **{"mcp-name": "trace_id", "traceparent": TRACEPARENT})

    response = await post(
        tool_client, "tools/call", {"name": "trace_id", "arguments": {}}, headers=headers
    )

    text = response.json()["result"]["content"][0]["text"]
    assert int(text, 16) == TRACE_ID


async def test_traceparent_in_meta_parents_the_request(tool_client: httpx2.AsyncClient) -> None:
    body = rpc_body("tools/call", {"name": "trace_id", "arguments": {}})
    body["params"]["_meta"]["traceparent"] = TRACEPARENT

    headers = mcp_headers("tools/call", **{"mcp-name": "trace_id"})
    response = await post(tool_client, "tools/call", body=body, headers=headers)

    text = response.json()["result"]["content"][0]["text"]
    assert int(text, 16) == TRACE_ID


SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src" / "agent_context_platform"
FORBIDDEN_SYMBOLS = {
    "initialize request": r"\binitialize\b",
    "initialized notification": r"notifications/initialized",
    "session header": r"mcp-session-id",
    "session id attribute": r"mcp_session_id",
    "session manager": r"session_manager|SessionManager",
    "session idle/limits": r"session_idle_timeout|max_sessions",
    "event store": r"event_store|EventStore|Last-Event-ID",
    "change-stream bus": r"SubscriptionBus|subscriptions/listen",
    "stateful transport": r"stateless_http\s*=\s*False|stateless=False",
    "sticky routing": r"\bsticky\b|affinity_cookie",
}


def scoped_sources(package_root: Path) -> list[Path]:
    """The MCP package plus app.py, where the MCP route is wired."""
    return sorted([*(package_root / "mcp").rglob("*.py"), package_root / "app.py"])


def find_offenders(package_root: Path) -> list[str]:
    return [
        f"{path.relative_to(package_root)}: {label}"
        for path in scoped_sources(package_root)
        if path.exists()
        for label, pattern in FORBIDDEN_SYMBOLS.items()
        if re.search(pattern, path.read_text(encoding="utf-8"), re.IGNORECASE)
    ]


def test_project_source_uses_no_session_or_handshake_symbols() -> None:
    """Invariant: no file under mcp/ or app.py may contain a forbidden symbol.

    Other modules are out of scope so unrelated code may use generic words.
    """
    sources = scoped_sources(SOURCE_ROOT)
    assert SOURCE_ROOT / "app.py" in sources
    assert SOURCE_ROOT / "mcp" / "server.py" in sources

    assert find_offenders(SOURCE_ROOT) == []


@pytest.mark.parametrize("relative", ["app.py", "mcp/new_module.py", "mcp/deep/nested.py"])
def test_scoped_scan_fails_when_a_symbol_is_added_in_scope(tmp_path: Path, relative: str) -> None:
    (tmp_path / "mcp" / "deep").mkdir(parents=True)
    (tmp_path / "app.py").write_text("x = 1\n")
    (tmp_path / relative).write_text('h = "Mcp-Session-Id"\n')

    assert find_offenders(tmp_path) == [f"{relative}: session header"]


def test_scoped_scan_ignores_unrelated_modules(tmp_path: Path) -> None:
    (tmp_path / "mcp").mkdir()
    (tmp_path / "app.py").write_text("x = 1\n")
    (tmp_path / "projection").mkdir()
    (tmp_path / "projection" / "runtime.py").write_text("def initialize(): ...\n")

    assert find_offenders(tmp_path) == []


def test_source_scan_detects_forbidden_symbols() -> None:
    sample = 'headers["Mcp-Session-Id"] = x; method == "initialize"'

    hits = [
        label for label, pattern in FORBIDDEN_SYMBOLS.items() if re.search(pattern, sample, re.I)
    ]

    assert hits == ["initialize request", "session header"]


async def test_missing_host_header_is_rejected() -> None:
    from agent_context_platform.mcp.protocol_guard import ProtocolGuard

    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    async def downstream(*_args: Any) -> None:
        raise AssertionError("request without Host reached the MCP app")

    guard = ProtocolGuard(
        downstream,
        allowed_hosts=("h",),
        allowed_origins=(),
        max_body_bytes=1,
        access_gate=OpenGate(),
    )
    await guard({"type": "http", "method": "POST", "headers": []}, None, send)  # type: ignore[arg-type]

    assert sent[0]["status"] == 421


def test_application_mounts_mcp_at_exact_path_and_keeps_health() -> None:
    from fastapi.testclient import TestClient

    from agent_context_platform.app import create_app
    from agent_context_platform.settings import Settings

    harness = AuthHarness()
    harness.add()
    with TestClient(
        create_app(Settings(environment="test"), mcp_auth=harness.runtime()),
        base_url="http://127.0.0.1:8000",
    ) as c:
        discovered = c.post(
            "/mcp",
            headers=mcp_headers("server/discover", authorization=f"Bearer {TOKEN}"),
            json=rpc_body("server/discover"),
            follow_redirects=False,
        )
        health = c.get("/health/live")

    assert discovered.status_code == 200
    assert discovered.json()["result"]["supportedVersions"] == [PROTOCOL_VERSION]
    assert health.status_code == 200


@pytest.mark.parametrize("raw", [b"not json", b"[1]", b'{"id": true}', b'{"id": {"a": 1}}'])
async def test_version_rejection_without_usable_id_answers_null_id(
    client: httpx2.AsyncClient, raw: bytes
) -> None:
    headers = mcp_headers("server/discover")
    del headers["mcp-protocol-version"]

    response = await client.post("/mcp", headers=headers, content=raw)

    assert response.status_code == 400
    assert response.json()["id"] is None


async def test_baggage_header_is_dropped_but_traceparent_is_honoured(
    tool_client: httpx2.AsyncClient,
) -> None:
    extra = {"mcp-name": "baggage_seen", "traceparent": TRACEPARENT, "baggage": "secret=value"}
    headers = mcp_headers("tools/call", **extra)
    headers_trace = {**headers, "mcp-name": "trace_id"}

    seen = await post(
        tool_client, "tools/call", {"name": "baggage_seen", "arguments": {}}, headers=headers
    )
    traced = await post(
        tool_client, "tools/call", {"name": "trace_id", "arguments": {}}, headers=headers_trace
    )

    assert json.loads(seen.json()["result"]["content"][0]["text"]) == {}
    assert int(traced.json()["result"]["content"][0]["text"], 16) == TRACE_ID


@pytest.mark.parametrize(
    ("host", "status"),
    [
        ("localhost:8000", 200),
        ("localhost:1", 200),
        ("localhost:65535", 200),
        ("localhost:abc", 421),
        ("localhost:80.evil.com", 421),
        ("localhost:80x", 421),
        ("localhost:123456", 421),
        ("localhost:-1", 421),
        ("localhost:8 0", 421),
    ],
)
async def test_port_wildcard_accepts_only_one_to_five_digits(host: str, status: int) -> None:
    async with mcp_client(allowed_hosts=("localhost:*",)) as client:
        headers = {**mcp_headers("server/discover"), "host": host}
        response = await post(client, "server/discover", headers=headers)

    assert response.status_code == status


async def test_origin_port_wildcard_rejects_malformed_ports() -> None:
    async with mcp_client(allowed_origins=("http://localhost:*",)) as client:
        statuses = []
        for origin in (
            "http://localhost:3000",
            "http://localhost:abc",
            "http://localhost:80.evil.com",
        ):
            headers = {**mcp_headers("server/discover"), "origin": origin}
            statuses.append((await post(client, "server/discover", headers=headers)).status_code)

    assert statuses == [200, 403, 403]


@pytest.mark.parametrize(("name", "status"), [("host", 421), ("origin", 403)])
async def test_duplicate_host_or_origin_is_rejected(name: str, status: int) -> None:
    from agent_context_platform.mcp.protocol_guard import ProtocolGuard

    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    async def downstream(*_args: Any) -> None:
        raise AssertionError("request with duplicate header reached the MCP app")

    guard = ProtocolGuard(
        downstream,
        allowed_hosts=("h",),
        allowed_origins=("https://ok.test",),
        max_body_bytes=10,
        access_gate=OpenGate(),
    )
    headers = [(b"host", b"h"), (b"origin", b"https://ok.test")]
    value = b"h" if name == "host" else b"https://ok.test"
    headers.append((name.encode(), value))  # duplicate of an otherwise allowed value
    await guard({"type": "http", "method": "POST", "headers": headers}, None, send)  # type: ignore[arg-type]

    assert sent[0]["status"] == status


def test_port_wildcard_rejects_non_ascii_digits() -> None:
    from agent_context_platform.mcp.protocol_guard import _matches

    assert not _matches("localhost:\u0663\u0663", ("localhost:*",))
    assert _matches("localhost:33", ("localhost:*",))
