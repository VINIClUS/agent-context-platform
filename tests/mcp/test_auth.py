from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx2
import pytest
from argon2 import PasswordHasher
from mcp_helpers import (
    HMAC_KEY,
    NOW,
    SECRET,
    TOKEN,
    AuthHarness,
    Clock,
    mcp_client,
    mcp_headers,
    rpc_body,
)
from sqlalchemy.exc import OperationalError

from agent_context_platform.mcp import auth as mcp_auth
from agent_context_platform.mcp.auth import (
    MCP_TOKEN_PREFIX,
    McpAuthHolder,
    McpAuthRuntime,
    mint_token,
    require_scope,
)
from agent_context_platform.security.bearer import Argon2Verifier

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

CANARY = "canary-7f3a9c1e"


@pytest.fixture
def harness() -> AuthHarness:
    value = AuthHarness()
    value.add()
    return value


async def discover(client: httpx2.AsyncClient, **headers: str) -> httpx2.Response:
    return await client.post(
        "/mcp", headers=mcp_headers("server/discover", **headers), json=rpc_body("server/discover")
    )


def assert_challenge_only(response: httpx2.Response) -> None:
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert SECRET not in response.text


async def test_valid_token_reads_every_method_including_discovery(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime()) as client:
        response = await discover(client)

    assert response.status_code == 200


async def test_absent_token_is_401_with_bare_challenge(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        response = await discover(client)

    assert_challenge_only(response)
    assert harness.lookups == []


@pytest.mark.parametrize(
    "authorization",
    ["Basic abc", "Bearer", "Bearer nodot", f"Bearer mcp_test0001.{'s' * 10}", "Bearer x.y"],
)
async def test_malformed_credentials_are_401(harness: AuthHarness, authorization: str) -> None:
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        response = await discover(client, authorization=authorization)

    assert_challenge_only(response)
    assert harness.lookups == []


async def test_unknown_prefix_and_wrong_secret_are_401(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        unknown = await discover(client, authorization=f"Bearer mcp_other0001.{SECRET}")
        wrong = await discover(client, authorization=f"Bearer mcp_test0001.{'t' * 43}")

    assert_challenge_only(unknown)
    assert_challenge_only(wrong)
    assert unknown.json() == wrong.json()


async def test_expired_and_revoked_tokens_are_401() -> None:
    harness = AuthHarness()
    harness.add("mcp_expired01." + SECRET, expires_at=NOW - timedelta(seconds=1))
    harness.add("mcp_revoked01." + SECRET, revoked_at=NOW - timedelta(seconds=1))
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        expired = await discover(client, authorization=f"Bearer mcp_expired01.{SECRET}")
        revoked = await discover(client, authorization=f"Bearer mcp_revoked01.{SECRET}")

    assert_challenge_only(expired)
    assert_challenge_only(revoked)


async def test_duplicated_authorization_header_is_401(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        response = await client.post(
            "/mcp",
            headers=[  # type: ignore[arg-type]
                *mcp_headers("server/discover").items(),
                ("authorization", f"Bearer {TOKEN}"),
                ("authorization", f"Bearer {TOKEN}"),
            ],
            json=rpc_body("server/discover"),
        )

    assert_challenge_only(response)


async def test_ingestion_token_is_refused_before_any_lookup(harness: AuthHarness) -> None:
    ingest_token = f"prod0001.{SECRET}"
    async with mcp_client(runtime=harness.runtime(), token=ingest_token) as client:
        response = await discover(client)

    assert_challenge_only(response)
    assert harness.lookups == []
    assert not "prod0001".startswith(MCP_TOKEN_PREFIX)


async def test_missing_scope_is_403_for_every_method() -> None:
    harness = AuthHarness()
    harness.add(scopes=("events:ingest",))
    harness.add("mcp_empty0001." + SECRET, scopes=())
    async with mcp_client(runtime=harness.runtime()) as client:
        responses = [
            await client.post("/mcp", headers=mcp_headers(m), json=rpc_body(m))
            for m in ("server/discover", "tools/list", "tools/call", "unknown/method")
        ]
        empty = await discover(client, authorization=f"Bearer mcp_empty0001.{SECRET}")

    assert [r.status_code for r in responses] == [403] * 4
    assert empty.status_code == 403
    assert "www-authenticate" not in responses[0].headers


async def test_get_without_token_is_401_not_405(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        anonymous = await client.get("/mcp")
    async with mcp_client(runtime=harness.runtime()) as client:
        authenticated = await client.get("/mcp")

    assert anonymous.status_code == 401
    assert authenticated.status_code == 405


async def test_bad_origin_is_403_even_without_a_token(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        anonymous = await discover(client, origin="https://evil.test")
    async with mcp_client(runtime=harness.runtime()) as client:
        authenticated = await discover(client, origin="https://evil.test")

    assert anonymous.status_code == 403
    assert authenticated.status_code == 403
    assert harness.lookups == []


async def test_bad_host_is_rejected_before_authentication(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        response = await discover(client, host="evil.test")

    assert response.status_code == 421


async def test_successful_verification_is_cached_and_failure_is_not(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime()) as client:
        assert (await discover(client)).status_code == 200
        assert (await discover(client)).status_code == 200
        assert harness.lookups == ["mcp_test0001"]

    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        bad = f"Bearer mcp_test0001.{'t' * 43}"
        assert (await discover(client, authorization=bad)).status_code == 401
        assert (await discover(client, authorization=bad)).status_code == 401
        assert harness.lookups.count("mcp_test0001") == 3


async def test_revocation_takes_effect_within_the_ttl(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime(ttl_seconds=30)) as client:
        assert (await discover(client)).status_code == 200
        harness.add(revoked_at=NOW + timedelta(seconds=1))

        harness.clock.advance(20)
        assert (await discover(client)).status_code == 200  # still inside the bound

        harness.clock.advance(11)
        assert (await discover(client)).status_code == 401  # ttl elapsed: re-verified


async def test_cache_never_outlives_token_expiry() -> None:
    harness = AuthHarness()
    harness.add(expires_at=NOW + timedelta(seconds=5))
    async with mcp_client(runtime=harness.runtime(ttl_seconds=300)) as client:
        assert (await discover(client)).status_code == 200
        harness.clock.advance(6)
        assert (await discover(client)).status_code == 401


async def test_cache_holds_only_hmac_keys_and_principal_metadata(harness: AuthHarness) -> None:
    async with mcp_client(runtime=harness.runtime()) as client:
        await discover(client)

    (key,) = harness.cache._entries
    assert key not in {TOKEN.encode(), hashlib.sha256(TOKEN.encode()).digest()}
    assert key == harness.cache.key_for(TOKEN)
    assert harness.cache.key_for(TOKEN) != mcp_auth.PrincipalCache(
        b"z" * 32, ttl=timedelta(seconds=1), max_entries=1, clock=harness.clock
    ).key_for(TOKEN)
    assert HMAC_KEY not in key
    assert TOKEN not in repr(harness.cache._entries)


async def test_cache_is_bounded() -> None:
    harness = AuthHarness()
    for index in range(12):
        harness.add(f"mcp_bulk{index:04d}.{SECRET}")
    runtime = harness.runtime()
    async with mcp_client(runtime=runtime, token=None) as client:
        for index in range(12):
            await discover(client, authorization=f"Bearer mcp_bulk{index:04d}.{SECRET}")

    assert len(harness.cache._entries) == 8


async def test_rate_limit_is_per_principal_with_retry_after_and_empty_body() -> None:
    harness = AuthHarness()
    harness.add()
    harness.add("mcp_second001." + SECRET, principal="other-reader")
    runtime = harness.runtime(rate_per_second=0.5, burst=2)
    async with mcp_client(runtime=runtime) as client:
        assert (await discover(client)).status_code == 200
        assert (await discover(client)).status_code == 200
        limited = await discover(client)
        other = await discover(client, authorization=f"Bearer mcp_second001.{SECRET}")
        harness.clock.advance(2)
        refilled = await discover(client)

    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "2"
    assert limited.content == b""
    assert other.status_code == 200
    assert refilled.status_code == 200


async def test_overloaded_verifier_sheds_load_with_503(harness: AuthHarness) -> None:
    runtime = harness.runtime(max_concurrency=1, max_queue_depth=0)
    async with mcp_client(runtime=runtime, token=None) as client, harness.verifier.admission():
        response = await discover(client, authorization=f"Bearer {TOKEN}")

    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert SECRET not in response.text


async def test_database_outage_is_503_without_leaking_the_error(harness: AuthHarness) -> None:
    harness.lookup_error = OperationalError("SELECT ...", {"p": CANARY}, Exception(CANARY))
    async with mcp_client(runtime=harness.runtime()) as client:
        response = await discover(client)

    assert response.status_code == 503
    assert CANARY not in response.text


async def test_unbound_runtime_fails_closed() -> None:
    from starlette.applications import Starlette
    from starlette.routing import Route

    from agent_context_platform.mcp.server import MCP_PATH, build_mcp_mount, create_mcp_server
    from agent_context_platform.settings import MCPSettings

    mount = build_mcp_mount(
        create_mcp_server(),
        MCPSettings(),
        access_gate=require_scope("memory:read", McpAuthHolder()),
    )
    application = Starlette(routes=[Route(MCP_PATH, mount.asgi_app)])
    async with mount.lifespan():
        transport = httpx2.ASGITransport(app=application)
        async with httpx2.AsyncClient(transport=transport, base_url="http://127.0.0.1:8000") as c:
            anonymous = await discover(c)
            with_token = await discover(c, authorization=f"Bearer {TOKEN}")

    assert anonymous.status_code == 401
    assert with_token.status_code == 503


async def test_logs_carry_audit_metadata_but_never_secrets_or_content(
    harness: AuthHarness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    record = harness.records["mcp_test0001"]
    body = rpc_body("tools/call", {"name": "search", "arguments": {"query": CANARY}})
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        await client.post(
            "/mcp",
            headers=mcp_headers(
                "tools/call", authorization=f"Bearer {TOKEN}", **{"mcp-name": "search"}
            ),
            json=body,
        )
        await discover(client, authorization=f"Bearer mcp_test0001.{'t' * 43}")
        await discover(client, authorization=f"Bearer {CANARY}")

    audit = [r for r in caplog.records if r.name == "agent_context_platform.mcp.auth"]
    assert [r.status for r in audit if hasattr(r, "status")][-2:] == [401, 401]  # type: ignore[attr-defined]
    served = audit[0]
    assert (served.principal, served.token_id, served.method) == (  # type: ignore[attr-defined]
        "codex-reader",
        str(record.token_id),
        "tools/call",
    )
    assert isinstance(served.latency_ms, float)  # type: ignore[attr-defined]

    haystack = [SECRET, TOKEN, "t" * 43, CANARY, record.token_verifier]
    for log in caplog.records:
        flattened = [log.getMessage(), *(str(v) for v in vars(log).values())]
        for value in flattened:
            assert not any(secret in value for secret in haystack), (log.name, value)


async def test_audited_method_comes_from_the_body_not_the_header(
    harness: AuthHarness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    async with mcp_client(runtime=harness.runtime()) as client:
        # The header claims server/discover; the body is a different request.
        await client.post(
            "/mcp",
            headers=mcp_headers("server/discover"),
            json=rpc_body("tools/call", {"name": "search"}),
        )
        await client.post(
            "/mcp",
            headers=mcp_headers(f"{CANARY} spoof"),
            json=rpc_body("server/discover"),
        )
        for raw in (b"not json", b'{"method": "bad method!"}', b'{"method": 7}', b"{}"):
            await client.post("/mcp", headers=mcp_headers("server/discover"), content=raw)

    methods = [r.method for r in caplog.records if hasattr(r, "method")]  # type: ignore[attr-defined]
    assert methods == ["tools/call", "server/discover", "invalid", "invalid", "invalid", "invalid"]
    assert all(CANARY not in str(vars(r)) for r in caplog.records)


async def test_rejections_before_the_body_is_read_audit_a_fixed_method(
    harness: AuthHarness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    async with mcp_client(runtime=harness.runtime(), token=None) as client:
        await client.post("/mcp", headers=mcp_headers(CANARY), json=rpc_body("tools/call"))

    (record,) = [r for r in caplog.records if hasattr(r, "method")]
    assert record.method == "unread"  # type: ignore[attr-defined]
    assert record.status == 401  # type: ignore[attr-defined]


def _audit_records(caplog: pytest.LogCaptureFixture) -> list[Any]:
    return [r for r in caplog.records if r.name == "agent_context_platform.mcp.auth"]


async def test_client_disconnect_during_body_read_is_audited_once(
    harness: AuthHarness, caplog: pytest.LogCaptureFixture
) -> None:
    from agent_context_platform.mcp.protocol_guard import ProtocolGuard

    caplog.set_level(logging.INFO)

    async def downstream(*_args: Any) -> None:
        raise AssertionError("disconnected request reached the MCP app")

    async def receive() -> dict[str, Any]:
        return {"type": "http.disconnect"}

    async def send(_message: dict[str, Any]) -> None:
        raise AssertionError("nothing may be sent to a disconnected client")

    guard = ProtocolGuard(
        downstream,  # type: ignore[arg-type]
        allowed_hosts=("h",),
        allowed_origins=(),
        max_body_bytes=10,
        access_gate=require_scope("memory:read", McpAuthHolder(harness.runtime())),
    )
    scope = {
        "type": "http",
        "method": "POST",
        "headers": [(b"host", b"h"), (b"authorization", f"Bearer {TOKEN}".encode())],
    }
    await guard(scope, receive, send)  # type: ignore[arg-type]

    (record,) = _audit_records(caplog)
    assert (record.status, record.principal) == ("disconnect", "codex-reader")


async def test_exception_before_a_response_is_audited_once_and_propagates(
    harness: AuthHarness, caplog: pytest.LogCaptureFixture
) -> None:
    from agent_context_platform.mcp.protocol_guard import ProtocolGuard

    caplog.set_level(logging.INFO)

    async def downstream(*_args: Any) -> None:
        raise RuntimeError(CANARY)

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"{}", "more_body": False}

    async def send(_message: dict[str, Any]) -> None:
        raise AssertionError("no response is expected")

    guard = ProtocolGuard(
        downstream,  # type: ignore[arg-type]
        allowed_hosts=("h",),
        allowed_origins=(),
        max_body_bytes=10,
        access_gate=require_scope("memory:read", McpAuthHolder(harness.runtime())),
    )
    scope = {
        "type": "http",
        "method": "POST",
        "headers": [
            (b"host", b"h"),
            (b"authorization", f"Bearer {TOKEN}".encode()),
            (b"mcp-protocol-version", b"2026-07-28"),
        ],
    }
    with pytest.raises(RuntimeError):
        await guard(scope, receive, send)  # type: ignore[arg-type]

    (record,) = _audit_records(caplog)
    assert record.status == "error"
    assert CANARY not in str(vars(record))


async def test_response_and_completion_are_audited_once(
    harness: AuthHarness, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    async with mcp_client(runtime=harness.runtime()) as client:
        await discover(client)

    assert [r.status for r in _audit_records(caplog)] == [200]


async def test_principal_is_exposed_on_the_asgi_scope(harness: AuthHarness) -> None:
    seen: list[Any] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append(scope[mcp_auth.PRINCIPAL_SCOPE_KEY])

    gate = require_scope("memory:read", McpAuthHolder(harness.runtime()))
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {TOKEN}".encode())]}
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    admitted = await gate.admit(scope, send)  # type: ignore[arg-type]
    assert admitted is not None
    await app(scope, None, admitted.send)
    admitted.finish(None)
    assert seen[0].principal == "codex-reader"
    assert seen[0].scopes == frozenset({"memory:read"})


def test_mint_token_stores_only_a_verifier_and_verifies() -> None:
    minted = mint_token(
        "codex-reader",
        NOW + timedelta(days=30),
        now=NOW,
        hasher=PasswordHasher(time_cost=1, memory_cost=8, parallelism=1),
    )

    prefix, _, secret = minted.token.partition(".")
    assert prefix.startswith(MCP_TOKEN_PREFIX)
    assert minted.row["token_prefix"] == prefix
    assert minted.row["scopes"] == ["memory:read"]
    assert minted.row["token_verifier"].startswith("$argon2id$v=19$")
    assert secret not in repr(minted.row) and secret not in repr(minted)
    assert PasswordHasher().verify(minted.row["token_verifier"], minted.token)


@pytest.mark.parametrize(
    ("principal", "expires_at"),
    [
        ("  ", NOW + timedelta(days=1)),
        ("x" * 256, NOW + timedelta(days=1)),
        ("codex", datetime(2026, 12, 1)),
        ("codex", NOW - timedelta(seconds=1)),
    ],
)
def test_mint_token_rejects_bad_input(principal: str, expires_at: datetime) -> None:
    with pytest.raises(ValueError):
        mint_token(principal, expires_at, now=NOW)


def test_mint_cli_prints_the_secret_once_and_inserts_the_verifier(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    inserted: list[Any] = []

    class Connection:
        def execute(self, _statement: Any, rows: list[dict[str, Any]]) -> None:
            inserted.extend(rows)

    class Engine:
        @contextlib.contextmanager
        def begin(self) -> Any:
            yield Connection()

        def dispose(self) -> None:
            pass

    monkeypatch.setenv(mcp_auth.DSN_ENVIRONMENT_VARIABLE, "postgresql+psycopg://op@db/agent")
    monkeypatch.setattr(mcp_auth, "create_engine", lambda _dsn: Engine())
    expires = (datetime.now(UTC) + timedelta(days=7)).isoformat()

    assert mcp_auth._main(["mint", "--principal", "codex-reader", "--expires", expires]) == 0

    output = capsys.readouterr()
    token = output.out.strip()
    assert token.startswith(MCP_TOKEN_PREFIX)
    assert "token_id=" in output.err and token not in output.err
    (row,) = inserted
    assert token not in repr(row)
    assert row["principal"] == "codex-reader"


@pytest.mark.parametrize(
    ("dsn", "expires"),
    [
        (None, "2099-01-01T00:00:00+00:00"),
        ("postgresql+psycopg://op@db/a", "2000-01-01T00:00:00+00:00"),
    ],
)
def test_mint_cli_refuses_missing_dsn_and_past_expiry(
    monkeypatch: pytest.MonkeyPatch, dsn: str | None, expires: str
) -> None:
    monkeypatch.delenv(mcp_auth.DSN_ENVIRONMENT_VARIABLE, raising=False)
    if dsn is not None:
        monkeypatch.setenv(mcp_auth.DSN_ENVIRONMENT_VARIABLE, dsn)

    assert mcp_auth._main(["mint", "--principal", "p", "--expires", expires]) == 2


async def test_concurrent_requests_share_one_bounded_verifier(harness: AuthHarness) -> None:
    runtime = harness.runtime(max_concurrency=1, max_queue_depth=8)
    async with mcp_client(runtime=runtime) as client:
        responses = await asyncio.gather(*(discover(client) for _ in range(4)))

    assert [r.status_code for r in responses] == [200] * 4
    assert isinstance(harness.verifier, Argon2Verifier)
    assert isinstance(runtime, McpAuthRuntime)


def test_limiter_evicts_idle_buckets_and_is_hard_capped() -> None:
    clock = Clock()
    limiter = mcp_auth.TokenBucketLimiter(
        rate_per_second=1, burst=2, max_buckets=3, clock=clock.monotonic
    )
    for name in ("a", "b", "c"):
        limiter.acquire(name)
    assert len(limiter._buckets) == 3

    # Past the refill horizon every bucket is full again, so a sweep drops them all.
    clock.advance(10)
    limiter.acquire("d")
    assert set(limiter._buckets) == {"d"}

    # Busy principals fill the cap: the least recently used is dropped, the cap holds.
    for name in ("e", "f", "g", "h"):
        limiter.acquire(name)
        assert len(limiter._buckets) <= 3
    assert "d" not in limiter._buckets and "h" in limiter._buckets
