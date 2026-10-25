"""The provisioning, `index` and `db upgrade` commands and the worker's helpers, without services.

The service functions are doubles (the provisioning module's, `index_checkout`, `migrate.upgrade`)
and engines are never connected; what is asserted is the command line's own contract: exit codes,
the stdout/stderr split, the JSON shape, the error text, and that no DSN, password or token is
printed where it must not be. The services themselves are proven against PostgreSQL by the
integration suites (`tests/integration/operations/test_provisioning.py` and others).
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import stat
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar
from uuid import UUID

import pytest
from typer.testing import CliRunner

from agent_context_platform import cli
from agent_context_platform.indexing.pipeline import IndexReport
from agent_context_platform.operations import migrate, provisioning
from agent_context_platform.projection.projectors.search import SearchProjector
from agent_context_platform.projection.verify import REBUILD_LOCK_KEY

pytestmark = pytest.mark.unit

PASSWORD = "shared-dsn-password"
DSN = f"postgresql+psycopg://operator:{PASSWORD}@db.example.test/agent"
ADMIN_PASSWORD = "admin-only-password"
ADMIN_DSN = f"postgresql+psycopg://admin:{ADMIN_PASSWORD}@db.example.test/agent"
PRODUCER_PREFIX = "prd_abcdefghijkl"
PRODUCER_TOKEN = PRODUCER_PREFIX + "." + "s" * 43
MCP_PREFIX = "mcp_mnopqrstuvwx"
MCP_TOKEN = MCP_PREFIX + "." + "m" * 43
MCP_TOKEN_ID = "0192d1c3-0000-7000-8000-000000000001"
EXPIRES = datetime(2026, 11, 24, tzinfo=UTC)
PRODUCERS = ("operations.registered_producers",)
TOKENS = ("operations.mcp_tokens",)
SECRETS = (PASSWORD, ADMIN_PASSWORD, "db.example.test")


class Engine:
    """`create_async_engine` double: never connects; records its URL and its disposal."""

    created: ClassVar[list[Engine]] = []

    def __init__(self, url: str) -> None:
        self.url = url
        self.disposed = 0
        Engine.created.append(self)

    async def dispose(self) -> None:
        self.disposed += 1


@pytest.fixture(autouse=True)
def environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_POSTGRESQL__DSN", DSN)
    for name in ("ADMIN_DSN", "API_DSN", "MIGRATION_DSN", "PROJECTOR_DSN"):
        monkeypatch.delenv(f"AGENT_CONTEXT_POSTGRESQL__{name}", raising=False)
    Engine.created = []
    monkeypatch.setattr(cli, "create_async_engine", lambda url, **_options: Engine(url))


def run(*args: str) -> Any:
    return CliRunner().invoke(cli.app, list(args))


def no_secret_in(result: Any) -> bool:
    return not any(secret in result.output for secret in SECRETS)


# --- `producer` and `mcp-token` (PLATFORM-039B) ---------------------------------------------------


class Admin:
    """The provisioning module's doubles: what was asked, and what to answer or raise."""

    def __init__(self) -> None:
        self.grants: list[tuple[str, tuple[str, ...], bool]] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.grant_error: Exception | None = None
        self.error: Exception | None = None  # raised before the token is delivered
        self.after_delivery: Exception | None = None  # raised after it (a failed commit)
        self.changed = True
        self.producers: list[provisioning.ProducerSummary] = []
        self.tokens: list[provisioning.McpTokenSummary] = []


@pytest.fixture
def admin(monkeypatch: pytest.MonkeyPatch) -> Admin:
    state = Admin()

    async def check_grants(sessions: Any, *, tables: Any, write: bool) -> None:
        state.grants.append((sessions.kw["bind"].url, tuple(tables), write))
        if state.grant_error is not None:
            raise state.grant_error

    def issue(credential: provisioning.IssuedCredential, deliver: Any) -> Any:
        if state.error is not None:
            raise state.error
        deliver(credential)  # inside the open transaction, before the commit
        if state.after_delivery is not None:
            raise state.after_delivery
        return credential

    async def register_producer(_sessions: Any, *, deliver: Any, **options: Any) -> Any:
        state.calls.append(("register", options))
        credential = provisioning.IssuedCredential(
            "producer",
            options["producer_id"],
            PRODUCER_PREFIX,
            PRODUCER_TOKEN,
            "events:ingest",
            EXPIRES,
            rotated=options["rotate"],
        )
        return issue(credential, deliver)

    async def create_mcp_token(_sessions: Any, *, deliver: Any, **options: Any) -> Any:
        state.calls.append(("create", options))
        credential = provisioning.IssuedCredential(
            "mcp", MCP_TOKEN_ID, MCP_PREFIX, MCP_TOKEN, options["scope"], EXPIRES
        )
        return issue(credential, deliver)

    async def revoke(name: str, _sessions: Any, key: str) -> bool:
        state.calls.append((name, {"key": key}))
        if state.error is not None:
            raise state.error
        return state.changed

    async def revoke_producer(sessions: Any, producer_id: str) -> bool:
        return await revoke("revoke_producer", sessions, producer_id)

    async def revoke_mcp_token(sessions: Any, prefix: str) -> bool:
        return await revoke("revoke_mcp_token", sessions, prefix)

    async def list_producers(_sessions: Any) -> list[provisioning.ProducerSummary]:
        return state.producers

    async def list_mcp_tokens(_sessions: Any) -> list[provisioning.McpTokenSummary]:
        return state.tokens

    for name, double in {
        "check_grants": check_grants,
        "register_producer": register_producer,
        "create_mcp_token": create_mcp_token,
        "revoke_producer": revoke_producer,
        "revoke_mcp_token": revoke_mcp_token,
        "list_producers": list_producers,
        "list_mcp_tokens": list_mcp_tokens,
    }.items():
        monkeypatch.setattr(provisioning, name, double)
    return state


REGISTER = ("producer", "register", "--producer-id", "p-1", "--expires-in", "30")
CREATE = ("mcp-token", "create", "--principal", "alice", "--expires-in", "7")


def metadata(kind: str, identifier: str, prefix: str, scope: str, *, rotated: bool) -> str:
    return (
        f"kind {kind}, id {identifier}, prefix {prefix}, scope {scope}, "
        f"expires_at {EXPIRES.isoformat()}, rotated {rotated}"
    )


def test_register_prints_only_the_token_on_stdout_and_the_metadata_on_stderr(
    admin: Admin, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in {"TIME_COST": "2", "MEMORY_COST_KIB": "1024", "PARALLELISM": "1"}.items():
        monkeypatch.setenv(f"AGENT_CONTEXT_INGESTION__ARGON2_{name}", value)

    result = run(*REGISTER)

    assert result.exit_code == 0, result.output
    assert result.stdout == PRODUCER_TOKEN + "\n"  # `$(...)` captures just the token
    assert result.stderr.strip() == metadata(
        "producer", "p-1", PRODUCER_PREFIX, "events:ingest", rotated=False
    )
    assert admin.calls == [
        (
            "register",
            {
                "producer_id": "p-1",
                "expires_in_days": 30,
                "cost": provisioning.HashCost(2, 1024, 1),
                "rotate": False,
            },
        )
    ]
    # The operator connection's grants are checked first, on the shared DSN as a fallback.
    assert admin.grants == [(DSN, PRODUCERS, True)]
    assert [engine.disposed for engine in Engine.created] == [1]
    assert no_secret_in(result)


def test_register_as_json_prints_one_document_with_the_token_and_rotate_is_passed(
    admin: Admin, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_POSTGRESQL__ADMIN_DSN", ADMIN_DSN)

    result = run(*REGISTER, "--rotate", "--json")

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "kind": "producer",
        "id": "p-1",
        "prefix": PRODUCER_PREFIX,
        "scope": "events:ingest",
        "expires_at": EXPIRES.isoformat(),
        "rotated": True,
        "token": PRODUCER_TOKEN,
    }
    assert result.stderr == ""
    assert admin.calls[0][1]["rotate"] is True
    assert admin.grants == [(ADMIN_DSN, PRODUCERS, True)]
    assert no_secret_in(result)


@pytest.mark.parametrize("as_json", [True, False])
def test_output_writes_the_token_to_a_new_private_file_and_never_prints_it(
    admin: Admin, tmp_path: Path, as_json: bool
) -> None:
    path = tmp_path / "producer.token"

    result = run(*REGISTER, "--output", str(path), *(["--json"] if as_json else []))

    assert result.exit_code == 0, result.output
    assert path.read_text(encoding="ascii") == PRODUCER_TOKEN + "\n"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert PRODUCER_TOKEN not in result.output and PRODUCER_TOKEN.split(".")[1] not in result.output
    document = {
        "kind": "producer",
        "id": "p-1",
        "prefix": PRODUCER_PREFIX,
        "scope": "events:ingest",
        "expires_at": EXPIRES.isoformat(),
        "rotated": False,
        "output": str(path),
    }
    if as_json:
        assert json.loads(result.stdout) == document and result.stderr == ""
    else:
        assert result.stdout == ""
        assert f"output {path}" in result.stderr


@pytest.mark.parametrize(
    ("code", "hint"),
    [
        (
            "producer_exists",
            " (an active or revoked registration exists; use --rotate to replace it)",
        ),
        ("producer_conflict", " (a concurrent registration won; retry)"),
        ("token_conflict", " (retry)"),
        ("invalid_expiry", " (--expires-in is 1-3650 days)"),
        ("invalid_producer_id", ""),
    ],
)
def test_a_refused_request_is_a_usage_error_with_its_code_and_hint(
    admin: Admin, tmp_path: Path, code: str, hint: str
) -> None:
    admin.error = provisioning.ProvisioningError(code)
    path = tmp_path / "token"

    result = run(*REGISTER, "--output", str(path))

    assert result.exit_code == 2
    assert result.stderr == f"error: {code}{hint}\n" and result.stdout == ""
    assert not path.exists()  # the file created up front is removed again


def test_a_missing_grant_exits_1_before_anything_is_issued(admin: Admin, tmp_path: Path) -> None:
    admin.grant_error = provisioning.MissingProvisioningGrantError(
        ["INSERT on operations.registered_producers", "UPDATE on operations.registered_producers"]
    )
    path = tmp_path / "token"

    result = run(*REGISTER, "--output", str(path))

    assert result.exit_code == 1
    assert result.stderr == (
        "error: the admin connection lacks: INSERT on operations.registered_producers; "
        "UPDATE on operations.registered_producers\n"
    )
    assert admin.calls == [] and not path.exists()
    assert [engine.disposed for engine in Engine.created] == [1]


def test_an_undeliverable_token_is_rolled_back_and_reported(
    admin: Admin, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def broken_pipe() -> None:
        raise BrokenPipeError

    monkeypatch.setattr(cli, "_flush_stdout", broken_pipe)

    printed = run(*REGISTER)

    def full_disk(_self: Any, _token: str) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(cli.SecretFile, "write", full_disk)
    path = tmp_path / "token"
    written = run(*CREATE, "--output", str(path))

    message = (
        "error: the token could not be delivered, so nothing was changed "
        "(a rotated producer keeps its previous credential)\n"
    )
    assert (printed.exit_code, printed.stderr) == (1, message)
    assert (written.exit_code, written.stderr) == (1, message)
    assert not path.exists()  # the half-delivered file is closed and removed
    assert MCP_TOKEN not in written.output


def test_a_failed_commit_after_delivery_names_the_producer_recovery(
    admin: Admin, tmp_path: Path
) -> None:
    admin.after_delivery = provisioning.CommitAfterDeliveryError("producer", PRODUCER_PREFIX)
    path = tmp_path / "token"

    result = run(*REGISTER, "--output", str(path))

    assert result.exit_code == 1
    assert result.stderr == (
        f"error: the token with prefix {PRODUCER_PREFIX} was delivered but the database commit "
        "failed, so it is probably not valid: discard it, check `producer list`, and if the "
        f"registration shows prefix {PRODUCER_PREFIX} or the producer is missing, run "
        "`producer register ... --rotate` again for a credential you can rely on\n"
    )
    assert not path.exists()  # the delivered file is removed: the token is unusable
    assert PRODUCER_TOKEN not in result.output


def test_a_failed_commit_after_delivery_names_the_mcp_token_recovery(admin: Admin) -> None:
    admin.after_delivery = provisioning.CommitAfterDeliveryError("mcp", MCP_PREFIX)

    result = run(*CREATE)

    assert result.exit_code == 1
    assert result.stdout == MCP_TOKEN + "\n"  # it was handed over before the commit failed
    assert result.stderr == (
        f"error: the token with prefix {MCP_PREFIX} was delivered but the database commit "
        f"failed, so it is probably not valid: discard it, run `mcp-token list`, and if prefix "
        f"{MCP_PREFIX} is listed run `mcp-token revoke {MCP_PREFIX}`; then create a new token\n"
    )
    assert "--rotate" not in result.stderr


def test_mcp_token_create_defaults_to_memory_read_and_checks_its_own_table(
    admin: Admin,
) -> None:
    result = run(*CREATE, "--json")

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "kind": "mcp",
        "id": MCP_TOKEN_ID,
        "prefix": MCP_PREFIX,
        "scope": "memory:read",
        "expires_at": EXPIRES.isoformat(),
        "rotated": False,
        "token": MCP_TOKEN,
    }
    assert admin.calls == [
        (
            "create",
            {
                "principal": "alice",
                "scope": "memory:read",
                "expires_in_days": 7,
                "cost": provisioning.HashCost(3, 65_536, 4),
            },
        )
    ]
    assert admin.grants == [(DSN, TOKENS, True)]


def test_an_unknown_mcp_scope_is_refused_before_any_connection(admin: Admin) -> None:
    result = run(*CREATE, "--scope", "events:ingest")

    assert result.exit_code == 2
    assert result.stderr == "error: --scope must be one of: memory:read\n"
    assert Engine.created == [] and admin.grants == [] and admin.calls == []


@pytest.mark.parametrize(
    ("command", "key", "label", "table", "document_key"),
    [
        ("producer", "p-1", "producer p-1", PRODUCERS, "producer_id"),
        ("mcp-token", MCP_PREFIX, f"mcp token {MCP_PREFIX}", TOKENS, "prefix"),
    ],
)
def test_revoke_reports_whether_anything_changed(
    admin: Admin, command: str, key: str, label: str, table: tuple[str, ...], document_key: str
) -> None:
    revoked = run(command, "revoke", key)
    as_json = run(command, "revoke", key, "--json")
    admin.changed = False
    again = run(command, "revoke", key)

    assert (revoked.exit_code, revoked.stdout) == (0, f"{label}: revoked\n")
    assert json.loads(as_json.stdout) == {document_key: key, "revoked": True, "changed": True}
    assert (again.exit_code, again.stdout) == (0, f"{label}: already revoked\n")
    assert admin.grants == [(DSN, table, True)] * 3
    assert [call[1] for call in admin.calls] == [{"key": key}] * 3


@pytest.mark.parametrize(
    ("command", "code"),
    [("producer", "producer_not_found"), ("mcp-token", "token_not_found")],
)
def test_revoking_an_unknown_credential_is_a_usage_error(
    admin: Admin, command: str, code: str
) -> None:
    admin.error = provisioning.ProvisioningError(code)

    result = run(command, "revoke", "unknown")

    assert (result.exit_code, result.stdout, result.stderr) == (2, "", f"error: {code}\n")


def _moments() -> dict[str, datetime]:
    now = datetime.now(UTC)
    return {
        "created": now - timedelta(days=40),
        "past": now - timedelta(days=1),
        "future": now + timedelta(days=30),
    }


def test_producer_list_shows_each_state_read_only_and_never_a_token(admin: Admin) -> None:
    at = _moments()
    admin.producers = [
        provisioning.ProducerSummary(
            "active", "prd_a", "events:ingest", at["created"], at["future"], None, at["past"]
        ),
        provisioning.ProducerSummary(
            "expired", "prd_e", "events:ingest", at["created"], at["past"], None, None
        ),
        provisioning.ProducerSummary(
            "revoked", "prd_r", "events:ingest", at["created"], at["future"], at["past"], None
        ),
        # A revocation dated in the future has not happened yet.
        provisioning.ProducerSummary(
            "pending", "prd_p", "events:ingest", at["created"], at["future"], at["future"], None
        ),
    ]

    human = run("producer", "list")
    machine = run("producer", "list", "--json")

    assert human.exit_code == 0 and machine.exit_code == 0
    expires = {name: at[moment].isoformat() for name, moment in (("f", "future"), ("p", "past"))}
    assert human.stdout.splitlines() == [
        f"active prd_a events:ingest active expires {expires['f']}",
        f"expired prd_e events:ingest expired expires {expires['p']}",
        f"revoked prd_r events:ingest revoked expires {expires['f']}",
        f"pending prd_p events:ingest active expires {expires['f']}",
    ]
    records = json.loads(machine.stdout)["producers"]
    assert records[0] == {
        "producer_id": "active",
        "token_prefix": "prd_a",
        "scope": "events:ingest",
        "created_at": at["created"].isoformat(),
        "expires_at": expires["f"],
        "revoked_at": None,
        "last_used_at": at["past"].isoformat(),
    }
    assert [record["revoked_at"] for record in records[1:]] == [
        None,
        at["past"].isoformat(),
        at["future"].isoformat(),
    ]
    assert admin.grants == [(DSN, PRODUCERS, False)] * 2  # SELECT only


def test_mcp_token_list_shows_each_state_and_its_scopes(admin: Admin) -> None:
    at = _moments()
    token_id = UUID("0192d1c3-0000-7000-8000-000000000002")
    admin.tokens = [
        provisioning.McpTokenSummary(
            token_id, MCP_PREFIX, "alice", ("memory:read",), at["created"], at["future"], None
        ),
        provisioning.McpTokenSummary(
            token_id, "mcp_old", "bob", ("memory:read",), at["created"], at["past"], at["past"]
        ),
    ]

    human = run("mcp-token", "list")
    machine = run("mcp-token", "list", "--json")

    assert human.stdout.splitlines() == [
        f"{MCP_PREFIX} alice memory:read active expires {at['future'].isoformat()}",
        f"mcp_old bob memory:read revoked expires {at['past'].isoformat()}",
    ]
    assert json.loads(machine.stdout) == {
        "tokens": [
            {
                "token_id": str(token_id),
                "token_prefix": MCP_PREFIX,
                "principal": "alice",
                "scopes": ["memory:read"],
                "created_at": at["created"].isoformat(),
                "expires_at": at["future"].isoformat(),
                "revoked_at": None,
            },
            {
                "token_id": str(token_id),
                "token_prefix": "mcp_old",
                "principal": "bob",
                "scopes": ["memory:read"],
                "created_at": at["created"].isoformat(),
                "expires_at": at["past"].isoformat(),
                "revoked_at": at["past"].isoformat(),
            },
        ]
    }
    assert admin.grants == [(DSN, TOKENS, False)] * 2


@pytest.mark.parametrize(
    "args",
    [("producer", "revoke", "p-1"), ("producer", "list"), ("mcp-token", "list"), REGISTER],
)
def test_every_provisioning_command_needs_an_admin_dsn(
    admin: Admin, monkeypatch: pytest.MonkeyPatch, args: tuple[str, ...]
) -> None:
    monkeypatch.delenv("AGENT_CONTEXT_POSTGRESQL__DSN")

    result = run(*args)

    assert result.exit_code == 2
    assert result.stderr == "error: AGENT_CONTEXT_POSTGRESQL__ADMIN_DSN (or __DSN) is required\n"
    assert admin.calls == [] and admin.grants == []


# --- the `--output` file ---------------------------------------------------------------------


def test_an_output_name_the_filesystem_refuses_is_a_usage_error(
    admin: Admin, tmp_path: Path
) -> None:
    result = run(*REGISTER, "--output", str(tmp_path / ("x" * 300)))

    assert (result.exit_code, result.stderr) == (2, "error: --output cannot be created\n")
    assert admin.calls == []  # refused before any credential is issued


@pytest.mark.parametrize("problem", ["readable", "linked"])
def test_a_descriptor_that_is_not_a_private_file_is_refused_and_removed(
    tmp_path: Path, problem: str
) -> None:
    path = tmp_path / "token"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    if problem == "readable":
        os.fchmod(descriptor, 0o644)
    else:
        os.link(path, tmp_path / "second-name")

    with pytest.raises(cli.CliUsageError, match="--output is not a private regular file"):
        cli.SecretFile(path, descriptor)

    assert not path.exists()  # closed and removed: no credential is ever written to it


def test_an_output_is_written_once_and_a_vanished_one_is_left_alone(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    written = cli.SecretFile.create(tmp_path / "token")
    written.write(PRODUCER_TOKEN)

    with pytest.raises(cli.CliUsageError, match="--output was already written"):
        written.write("prd_second." + "x" * 43)
    assert written.path.read_text() == PRODUCER_TOKEN + "\n"

    vanished = cli.SecretFile.create(tmp_path / "other")
    vanished.path.unlink()
    vanished.abandon()  # nothing to remove, and nothing to complain about
    assert not vanished.path.exists()
    assert capsys.readouterr().err == ""


# --- `index` (PLATFORM-039C) ---------------------------------------------------------------------


@pytest.fixture
def indexing(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {
        "report": IndexReport("repo-1", True, None),
        "requests": [],
    }

    async def index_checkout(request: Any, _settings: Any, service: Any) -> IndexReport:
        state["requests"].append(request)
        state["service_repository"] = service.config.repository_id
        report: IndexReport = state["report"]
        return report

    monkeypatch.setattr(cli, "index_checkout", index_checkout)
    return state


@pytest.mark.parametrize(
    ("repository_id", "checkout", "scip", "message"),
    [
        ("", "dir", None, "--repository-id must be non-empty text without control characters"),
        (
            "repo\x01",
            "dir",
            None,
            "--repository-id must be non-empty text without control characters",
        ),
        ("repo-1", "missing", None, "--checkout must be an existing directory"),
        ("repo-1", "file", None, "--checkout must be an existing directory"),
        ("repo-1", "dir", "missing.scip", "--scip must be an existing file"),
        ("repo-1", "dir", "dir", "--scip must be an existing file"),
    ],
)
def test_index_usage_errors_are_refused_before_any_connection(
    indexing: dict[str, Any],
    tmp_path: Path,
    repository_id: str,
    checkout: str,
    scip: str | None,
    message: str,
) -> None:
    (tmp_path / "dir").mkdir()
    (tmp_path / "file").write_text("")
    args = ["index", "--repository-id", repository_id, "--checkout", str(tmp_path / checkout)]
    if scip is not None:
        args += ["--scip", str(tmp_path / scip)]

    result = run(*args)

    assert (result.exit_code, result.stderr) == (2, f"error: {message}\n")
    assert indexing["requests"] == [] and Engine.created == []


def test_index_runs_as_the_api_role_and_prints_counts_only(
    indexing: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api_dsn = "postgresql+psycopg://api:api-only-password@db.example.test/agent"
    monkeypatch.setenv("AGENT_CONTEXT_POSTGRESQL__API_DSN", api_dsn)
    scip = tmp_path / "index.scip"
    scip.write_bytes(b"\x00")
    indexing["report"] = IndexReport(
        "repo-1",
        True,
        None,
        index_id="idx-1",
        target_kind="commit",
        target="0123abcd",
        submitted=3,
        skipped=1,
        files=5,
        files_not_indexed=2,
        diagnostics={"unsupported_language": 2, "binary": 1},
    )
    args = ("index", "--repository-id", "repo-1", "--checkout", str(tmp_path), "--scip", str(scip))

    human = run(*args)
    machine = run(*args, "--json")

    assert human.exit_code == 0, human.output
    assert human.stdout.splitlines() == [
        "index idx-1 (commit 0123abcd)",
        "events: 3 submitted, 1 already present; files: 5 indexed, 2 not indexed",
        "completed",
        "diagnostics: binary=1, unsupported_language=2",
    ]
    assert json.loads(machine.stdout) == indexing["report"].to_dict()
    request = indexing["requests"][0]
    assert (request.repository_id, request.checkout, request.scip) == ("repo-1", tmp_path, scip)
    assert indexing["service_repository"] == "repo-1"
    assert [(engine.url, engine.disposed) for engine in Engine.created] == [(api_dsn, 1)] * 2
    assert "api-only-password" not in human.output + machine.output


@pytest.mark.parametrize(
    ("error_class", "line"),
    [("checkout_unreadable", "failed: checkout_unreadable"), (None, "failed: unsuccessful")],
)
def test_a_failed_index_still_prints_its_counts_and_exits_1(
    indexing: dict[str, Any], tmp_path: Path, error_class: str | None, line: str
) -> None:
    indexing["report"] = IndexReport("repo-1", False, error_class, submitted=2)

    result = run("index", "--repository-id", "repo-1", "--checkout", str(tmp_path))

    assert result.exit_code == 1
    assert result.stdout.splitlines() == [
        "index none (no target)",
        "events: 2 submitted, 0 already present; files: 0 indexed, 0 not indexed",
        line,
    ]
    assert [engine.disposed for engine in Engine.created] == [1]


# --- `db upgrade` stopped by a signal (PLATFORM-049) -------------------------------------------


def test_a_signal_during_db_upgrade_cancels_it_and_names_db_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alembic_ini = Path(__file__).parents[2] / "alembic.ini"
    previous = signal.getsignal(signal.SIGTERM)

    async def stopped_while_waiting(*_args: Any, **_options: Any) -> migrate.MigrationReport:
        # Only signal once the command's handler is installed, so the test process never dies.
        assert signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, signal.SIG_IGN, None)
        signal.raise_signal(signal.SIGTERM)
        await asyncio.Event().wait()  # a lock wait; cancelled by the handler
        raise AssertionError("not cancelled")

    monkeypatch.setattr(migrate, "upgrade", stopped_while_waiting)
    try:
        result = run("db", "upgrade", "--alembic-config", str(alembic_ini))
        after = signal.getsignal(signal.SIGTERM)
    finally:
        signal.signal(signal.SIGTERM, previous)

    assert result.exit_code == 1
    assert result.stderr == (
        "error: interrupted: stopped by a signal; run `db current` to see the revision\n"
    )
    assert result.stdout == ""
    assert after == signal.SIG_DFL  # a second signal during clean-up gets the default action
    assert [engine.disposed for engine in Engine.created] == [1]
    assert no_secret_in(result)


# --- the worker's and the rebuild's database helpers ---------------------------------------------


class Session:
    """An `AsyncSession` double for the raw-SQL helpers: answers in order, logs every call."""

    def __init__(self, log: list[Any], answers: list[Any]) -> None:
        self.log = log
        self.answers = answers

    async def __aenter__(self) -> Session:
        self.log.append("open")
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self.log.append("close")

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[None]:
        self.log.append("begin")
        yield
        self.log.append("commit")

    async def scalar(self, statement: Any, parameters: Any = None) -> Any:
        self.log.append((str(statement), parameters))
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    async def rollback(self) -> None:
        self.log.append("rollback")

    async def invalidate(self) -> None:
        self.log.append("invalidate")


def sessions(log: list[Any], *answers: Any) -> Any:
    return lambda: Session(log, list(answers))


TRY_SHARED = ("SELECT pg_try_advisory_lock_shared(:key)", {"key": REBUILD_LOCK_KEY})
UNLOCK_SHARED = ("SELECT pg_advisory_unlock_shared(:key)", {"key": REBUILD_LOCK_KEY})


async def _hold(factory: Any, body_error: BaseException | None = None) -> bool:
    async with cli._shared_rebuild_lock(factory) as blocked:
        if body_error is not None:
            raise body_error
        return blocked


def test_a_free_rebuild_lock_is_held_shared_for_the_body_and_released() -> None:
    log: list[Any] = []

    blocked = asyncio.run(_hold(sessions(log, True, True)))

    assert blocked is False
    assert log == ["open", TRY_SHARED, "rollback", UNLOCK_SHARED, "rollback", "close"]


def test_a_held_rebuild_lock_blocks_and_nothing_is_released() -> None:
    log: list[Any] = []

    blocked = asyncio.run(_hold(sessions(log, False)))

    assert blocked is True
    assert log == ["open", TRY_SHARED, "rollback", "close"]


def test_the_shared_lock_is_released_when_the_body_raises() -> None:
    log: list[Any] = []

    with pytest.raises(RuntimeError, match="batch failed"):
        asyncio.run(_hold(sessions(log, True, True), RuntimeError("batch failed")))

    assert log == ["open", TRY_SHARED, "rollback", UNLOCK_SHARED, "rollback", "close"]


def test_a_failed_unlock_invalidates_the_connection_instead_of_pooling_it() -> None:
    log: list[Any] = []

    with pytest.raises(ConnectionError):
        asyncio.run(_hold(sessions(log, True, ConnectionError("gone"))))

    assert log == ["open", TRY_SHARED, "rollback", UNLOCK_SHARED, "invalidate", "close"]


class Graph:
    def __init__(self, embeddings: int) -> None:
        self.embeddings = embeddings
        self.queries: list[tuple[str, Any]] = []

    async def execute_read(self, work: Any) -> Any:
        graph = self

        class Transaction:
            async def run(self, query: str, parameters: Any) -> Any:
                graph.queries.append((query, parameters))
                return SimpleNamespace(records=[{"n": graph.embeddings}])

        return await work(Transaction())


@pytest.mark.parametrize(
    ("documents", "embeddings", "without"), [(5, 3, 2), (3, 3, 0), (2, 4, 0), (None, 1, 0)]
)
def test_search_status_counts_rows_against_embedding_nodes(
    documents: int | None, embeddings: int, without: int
) -> None:
    log: list[Any] = []
    graph = Graph(embeddings)
    runtime = cli.Runtime(sessions(log, documents), None, graph)  # type: ignore[arg-type]

    counts = asyncio.run(cli._search_status(runtime))

    assert counts == {
        "documents": documents or 0,
        "embeddings": embeddings,
        "documents_without_embeddings": without,  # an approximation, never negative
    }
    assert log == ["open", ("SELECT count(*) FROM retrieval.search_documents", None), "close"]
    assert graph.queries == [("MATCH (n:ContentEmbedding) RETURN count(n) AS n", {})]


class CheckedSearch(SearchProjector):
    def __init__(self) -> None:
        super().__init__()
        self.checked: list[Any] = []

    async def check_replay(self, sessions: Any) -> int:
        self.checked.append(sessions)
        return 0


def test_only_a_bound_search_projector_resets_and_checks_the_search_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resets: list[Any] = []

    async def reset(session: Any) -> int:
        resets.append(session)
        return 0

    monkeypatch.setattr(cli, "reset_search_tables", reset)
    other = SimpleNamespace(name="git")
    search = CheckedSearch()
    log: list[Any] = []
    factory = sessions(log)

    asyncio.run(cli._prepare_search_replay([other], factory))
    asyncio.run(cli._check_search_replay([other], factory))
    assert log == [] and resets == [] and search.checked == []

    asyncio.run(cli._prepare_search_replay([other, search], factory))
    asyncio.run(cli._check_search_replay([other, search], factory))
    assert log == ["open", "begin", "commit", "close"]  # one transaction around the reset
    assert len(resets) == 1 and search.checked == [factory]
