"""`GET /v1/auth/whoami`: a fixed answer per credential state, outside the frozen OpenAPI."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from agent_context_platform.app import create_app
from agent_context_platform.ledger.api import IngestionRuntime
from agent_context_platform.ledger.auth import ProducerPrincipal
from agent_context_platform.ledger.whoami import TOKEN_KIND_HEADER, WHOAMI_PATH
from agent_context_platform.mcp.auth import McpAuthRuntime, McpPrincipal, RateLimitedError
from agent_context_platform.security.bearer import (
    AuthOverloadedError,
    InsufficientScopeError,
    InvalidCredentialError,
)
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit

PRODUCER_TOKEN = "prod0001." + "p" * 43
MCP_TOKEN = "mcp_0001." + "m" * 43


class FakeAuthenticator:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls: list[str | None] = []

    async def authenticate(self, authorization: str | None) -> Any:
        self.calls.append(authorization)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class FakeLimiter:
    def __init__(self) -> None:
        self.error: Exception | None = None
        self.acquired: list[str] = []

    def acquire(self, principal: str) -> None:
        self.acquired.append(principal)
        if self.error is not None:
            raise self.error


def _mcp_principal(*scopes: str) -> McpPrincipal:
    return McpPrincipal(
        principal="codex-doctor",
        token_id=uuid4(),
        scopes=frozenset(scopes),
        expires_at=datetime.now(UTC) + timedelta(days=1),
    )


class Probe:
    def __init__(self) -> None:
        self.producer = FakeAuthenticator(ProducerPrincipal(producer_id="codex-cli"))
        self.mcp = FakeAuthenticator(_mcp_principal("memory:read"))
        self.limiter = FakeLimiter()
        ingestion = IngestionRuntime(
            authenticator=self.producer,  # type: ignore[arg-type]
            service=object(),  # type: ignore[arg-type]
            max_request_body_bytes=1024,
        )
        mcp = McpAuthRuntime(authenticator=self.mcp, limiter=self.limiter)  # type: ignore[arg-type]
        self.client = TestClient(
            create_app(Settings(environment="test"), ingestion=ingestion, mcp_auth=mcp),
            base_url="http://127.0.0.1:8000",  # an allowed MCP Host
        )

    def get(self, *authorizations: str) -> Any:
        headers = [("authorization", value) for value in authorizations]
        return self.client.get(WHOAMI_PATH, headers=headers)


@pytest.fixture
def probe() -> Probe:
    return Probe()


@pytest.mark.parametrize(("token", "kind"), [(PRODUCER_TOKEN, "producer"), (MCP_TOKEN, "mcp")])
def test_a_valid_token_is_a_bodyless_204_naming_its_kind_only(
    probe: Probe, caplog: pytest.LogCaptureFixture, token: str, kind: str
) -> None:
    caplog.set_level(logging.DEBUG)

    response = probe.get(f"Bearer {token}")

    assert response.status_code == 204 and response.content == b""
    assert response.headers[TOKEN_KIND_HEADER] == kind
    assert response.headers["cache-control"] == "no-store"
    used, unused = (
        (probe.producer, probe.mcp) if kind == "producer" else (probe.mcp, probe.producer)
    )
    assert used.calls == [f"Bearer {token}"] and unused.calls == []
    assert probe.limiter.acquired == ([] if kind == "producer" else ["codex-doctor"])
    records = "\n".join(str(record.__dict__) for record in caplog.records)
    assert token not in records and "codex-cli" not in records and "codex-doctor" not in records


@pytest.mark.parametrize(
    ("headers", "result", "status", "code"),
    [
        ((), None, 401, "missing_credential"),
        (("Basic abc",), None, 401, "invalid_credential"),
        ((f"Bearer {PRODUCER_TOKEN}", f"Bearer {PRODUCER_TOKEN}"), None, 401, "invalid_credential"),
        ((f"Bearer {PRODUCER_TOKEN}",), InvalidCredentialError(), 401, "invalid_credential"),
        ((f"Bearer {PRODUCER_TOKEN}",), InsufficientScopeError(), 403, "insufficient_scope"),
        ((f"Bearer {PRODUCER_TOKEN}",), AuthOverloadedError(), 503, "auth_overloaded"),
        (
            (f"Bearer {PRODUCER_TOKEN}",),
            OperationalError("SELECT 1", {}, Exception(PRODUCER_TOKEN)),
            503,
            "auth_unavailable",
        ),
        ((f"Bearer {PRODUCER_TOKEN}",), RuntimeError(PRODUCER_TOKEN), 500, "internal_error"),
    ],
)
def test_every_failure_is_a_fixed_content_free_body(
    probe: Probe,
    caplog: pytest.LogCaptureFixture,
    headers: tuple[str, ...],
    result: Any,
    status: int,
    code: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    if result is not None:
        probe.producer.result = result

    response = probe.get(*headers)

    assert response.status_code == status
    body = response.json()
    assert body["error"]["code"] == code and set(body) == {"error", "request_id"}
    assert TOKEN_KIND_HEADER.lower() not in response.headers
    assert (response.headers.get("www-authenticate") == "Bearer") is (status == 401)
    assert ("retry-after" in response.headers) is (status == 503)
    assert PRODUCER_TOKEN not in response.text
    records = "\n".join(str(record.__dict__) for record in caplog.records)
    assert PRODUCER_TOKEN not in records


def test_an_mcp_token_needs_the_read_scope_and_spends_the_rate_limit(probe: Probe) -> None:
    probe.mcp.result = _mcp_principal("memory:write")
    refused = probe.get(f"Bearer {MCP_TOKEN}")
    assert refused.status_code == 403 and refused.json()["error"]["code"] == "insufficient_scope"
    assert probe.limiter.acquired == []

    probe.mcp.result = _mcp_principal("memory:read")
    probe.limiter.error = RateLimitedError(7)
    limited = probe.get(f"Bearer {MCP_TOKEN}")
    assert limited.status_code == 429 and limited.headers["retry-after"] == "7"
    assert limited.json()["error"]["code"] == "rate_limited"


def test_an_unconfigured_plane_fails_closed() -> None:
    client = TestClient(create_app(Settings(environment="test")), base_url="http://localhost:8080")
    for token in (PRODUCER_TOKEN, MCP_TOKEN):
        response = client.get(WHOAMI_PATH, headers={"authorization": f"Bearer {token}"})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "auth_unavailable"


def test_the_probe_is_not_in_the_published_schema_and_only_answers_get(probe: Probe) -> None:
    assert WHOAMI_PATH not in probe.client.app.openapi()["paths"]  # type: ignore[attr-defined]
    response = probe.client.post(WHOAMI_PATH, headers={"authorization": f"Bearer {MCP_TOKEN}"})
    assert response.status_code == 405 and probe.mcp.calls == []


@pytest.mark.parametrize(
    ("extra", "status", "code"),
    [
        ([("host", "rebind.example")], 421, "invalid_host"),
        ([("host", "127.0.0.1:8000"), ("host", "127.0.0.1:8000")], 421, "invalid_host"),
        ([("origin", "https://rebind.example")], 403, "invalid_origin"),
        (
            [("origin", "http://127.0.0.1:1"), ("origin", "http://127.0.0.1:1")],
            403,
            "invalid_origin",
        ),
    ],
)
def test_an_mcp_token_meets_the_mcp_host_and_origin_allowlist_before_any_lookup(
    probe: Probe, extra: list[tuple[str, str]], status: int, code: str
) -> None:
    # A DNS-rebinding page must not reach the MCP Argon2 gate through the probe.
    headers = [("authorization", f"Bearer {MCP_TOKEN}"), *extra]
    if not any(name == "host" for name, _ in extra):
        headers.append(("host", "127.0.0.1:8000"))

    response = probe.client.get(WHOAMI_PATH, headers=headers)

    assert response.status_code == status and response.json()["error"]["code"] == code
    assert probe.mcp.calls == [] and probe.limiter.acquired == []


def test_a_browser_origin_is_refused_for_producer_tokens_too(probe: Probe) -> None:
    response = probe.client.get(
        WHOAMI_PATH,
        headers={"authorization": f"Bearer {PRODUCER_TOKEN}", "origin": "https://rebind.example"},
    )
    assert response.status_code == 403 and response.json()["error"]["code"] == "invalid_origin"
    assert probe.producer.calls == []
    # An allowed Origin is fine; the producer plane keeps the ingestion route's Host rules.
    allowed = probe.client.get(
        WHOAMI_PATH,
        headers={"authorization": f"Bearer {PRODUCER_TOKEN}", "origin": "http://localhost:3000"},
    )
    assert allowed.status_code == 204
    elsewhere = probe.client.get(
        WHOAMI_PATH,
        headers={"authorization": f"Bearer {PRODUCER_TOKEN}", "host": "ingest.example"},
    )
    assert elsewhere.status_code == 204 and elsewhere.headers[TOKEN_KIND_HEADER] == "producer"
