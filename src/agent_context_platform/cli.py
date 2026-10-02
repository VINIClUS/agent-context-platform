"""`agent-context`: the operator command line.

It exposes `agent-context projection rebuild|verify|status` (PLATFORM-039) and the credential
provisioning commands `agent-context producer register|revoke|list` and
`agent-context mcp-token create|revoke|list` (PLATFORM-039B), plus `agent-context index` (PLATFORM-039C,
which indexes one git checkout into the ledger as the indexer producer). Every command reads the process environment through `Settings` (`AGENT_CONTEXT_*`), runs in one `asyncio.run`,
and never prints a DSN, a password or an exception message: failures are reported by class name.

Exit codes: 0 success, 1 verification mismatch or a failed operation, 2 usage error (bad or
missing options, an unsafe refusal such as a non-empty target without `--wipe-target` or a
`producer register` of an active id without `--rotate`).

A freshly issued bearer is printed ONCE: to stdout (metadata goes to stderr, so `$(...)` captures
just the token) or, with `--output`, to a new 0600 file outside any git work tree. It is never
logged.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import stat
import sys
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Protocol
from uuid import uuid4

import typer
from agent_context_sdk import RedactionPolicyV1
from pydantic import PostgresDsn, Secret, ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from agent_context_platform.content.service import ContentService
from agent_context_platform.db import session_factory
from agent_context_platform.indexing.emitter import IndexingConfig, IndexingService
from agent_context_platform.indexing.identity import repository_namespace
from agent_context_platform.indexing.pipeline import IndexReport, IndexRequest, index_checkout
from agent_context_platform.ledger.service import IngestionService
from agent_context_platform.operations import provisioning
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.registry import registered_projectors
from agent_context_platform.projection.runtime import (
    OutboxBacklog,
    ProjectionRunner,
    ProjectionRunReport,
    Projector,
)
from agent_context_platform.projection.verify import (
    CLI_APPLICATION_NAME,
    DEFAULT_BATCH_SIZE,
    DEFAULT_RUNNER_QUIET_SECONDS,
    REBUILD_LOCK_KEY,
    MissingGrantError,
    NoContentBlobStore,
    ProjectionEventRecorder,
    ProjectorStatus,
    RebuildReport,
    RunnerActiveError,
    TargetNotEmptyError,
    VerificationReport,
    preflight,
    projection_status,
    public_error_class,
    rebuild_projections,
    verify_projections,
)
from agent_context_platform.settings import Neo4jSettings, Settings

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2

TARGET_USERNAME_ENV = "AGENT_CONTEXT_REBUILD_TARGET_NEO4J_USERNAME"
TARGET_PASSWORD_ENV = "AGENT_CONTEXT_REBUILD_TARGET_NEO4J_PASSWORD"

app = typer.Typer(
    name="agent-context",
    help="Agent Context platform operations.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
projection_app = typer.Typer(
    help="Rebuild, verify and inspect the graph projections.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
app.add_typer(projection_app, name="projection")
producer_app = typer.Typer(
    help="Register, revoke and list ingestion producers.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
app.add_typer(producer_app, name="producer")
mcp_token_app = typer.Typer(
    help="Create, revoke and list read-only MCP tokens.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
app.add_typer(mcp_token_app, name="mcp-token")

_JSON = Annotated[bool, typer.Option("--json", help="Machine-readable output.")]
_BATCH = Annotated[int, typer.Option("--batch-size", min=1, help="Graph scan page size.")]
_TARGET_URI = Annotated[
    str | None,
    typer.Option(
        "--target-uri", help="Neo4j URI of the scratch/standby instance (no credentials)."
    ),
]
_TARGET_DATABASE = Annotated[
    str | None, typer.Option("--target-database", help="Database name on the target instance.")
]
_TARGET_USERNAME = Annotated[
    str | None,
    typer.Option(
        "--target-username",
        envvar=TARGET_USERNAME_ENV,
        help=f"Target Neo4j user (or ${TARGET_USERNAME_ENV}). The password is read only from "
        f"${TARGET_PASSWORD_ENV}.",
    ),
]
_WIPE_TARGET = Annotated[
    bool,
    typer.Option("--wipe-target", help="Delete the target's graph data first (needs --confirm)."),
]
_CONFIRM = Annotated[
    str | None,
    typer.Option("--confirm", help="Repeat the database name to confirm a destructive step."),
]


class DeliveryError(Exception):
    """The credential was created but could not be written to ``--output``."""


class CliUsageError(Exception):
    """A usage error, reported as exit code 2; the message never holds a secret."""


@dataclass(frozen=True, slots=True)
class Runtime:
    """The opened resources one command runs with.

    `sessions` is the projector-role connection (checkpoints, dead letters, stream heads, in-place
    resets); `recording_sessions` is the API-role connection, used only to record
    `projection.rebuilt` through `IngestionService`, and is None when nothing is recorded.
    """

    sessions: async_sessionmaker[AsyncSession]
    recording_sessions: async_sessionmaker[AsyncSession] | None
    store: Neo4jStore


def _load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError:
        raise CliUsageError("invalid AGENT_CONTEXT_* configuration") from None


def _engine(dsn: Secret[PostgresDsn] | None, name: str) -> AsyncEngine:
    if dsn is None:
        raise CliUsageError(f"AGENT_CONTEXT_POSTGRESQL__{name} (or __DSN) is required")
    return create_async_engine(
        dsn.get_secret_value().unicode_string(),
        pool_pre_ping=True,
        connect_args={"application_name": CLI_APPLICATION_NAME},
    )


@asynccontextmanager
async def _runtime(settings: Settings, *, record: bool = False) -> AsyncIterator[Runtime]:
    postgresql = settings.postgresql
    engines = [_engine(postgresql.effective_projector_dsn, "PROJECTOR_DSN")]
    if record:
        try:
            engines.append(_engine(postgresql.effective_api_dsn, "API_DSN"))
        except CliUsageError:
            await engines[0].dispose()
            raise
    store = Neo4jStore(settings.neo4j)
    try:
        yield Runtime(
            session_factory(engines[0]),
            session_factory(engines[1]) if record else None,
            store,
        )
    finally:
        await store.close()
        for engine in engines:
            await engine.dispose()


def _server_key(settings: Neo4jSettings) -> tuple[str, int, str]:
    uri = settings.uri
    host = "" if uri is None or uri.host is None else uri.host.lower().strip("[]")
    if host in {"localhost", "::1"}:
        host = "127.0.0.1"
    port = 7687 if uri is None or uri.port is None else uri.port
    return host, port, settings.database


def _describe(settings: Neo4jSettings) -> str:
    host, port, database = _server_key(settings)
    return f"{host}:{port}/{database}"


def _target_settings(
    live: Neo4jSettings, uri: str | None, database: str | None, username: str | None
) -> Neo4jSettings:
    """The explicit target connection: no defaults, and the password from the environment only."""
    password = os.environ.get(TARGET_PASSWORD_ENV)
    missing = [
        flag
        for flag, value in (
            ("--target-uri", uri),
            ("--target-database", database),
            (f"--target-username or {TARGET_USERNAME_ENV}", username),
            (TARGET_PASSWORD_ENV, password),
        )
        if not value
    ]
    if missing:
        raise CliUsageError("a target connection needs " + ", ".join(missing))
    timeouts = live.model_dump(
        include={
            "connection_timeout",
            "connection_acquisition_timeout",
            "max_transaction_retry_time",
            "transaction_timeout",
            "schema_timeout",
        }
    )
    try:
        target = Neo4jSettings.model_validate(
            {
                **timeouts,
                "uri": uri,
                "username": username,
                "password": password,
                "database": database,
            }
        )
    except ValidationError:
        raise CliUsageError("the target connection settings are invalid") from None
    return target


def _require_distinct_target(live: Neo4jSettings, target: Neo4jSettings) -> None:
    if live.uri is not None and _server_key(live) == _server_key(target):
        raise CliUsageError(
            "the target is the live projection; use --in-place --confirm <database> to rebuild it"
        )


def _confirmed(confirm: str | None, database: str, what: str) -> None:
    if confirm != database:
        raise CliUsageError(f"{what} needs --confirm {database}")


def _forbid_target_options(*values: object) -> None:
    if any(values):
        raise CliUsageError("target options are only valid for a standby rebuild or --replay-check")


def _emit(payload: dict[str, Any], *, as_json: bool, lines: list[str]) -> None:
    typer.echo(json.dumps(payload, sort_keys=True) if as_json else "\n".join(lines))


def _recovery_message(error: provisioning.CommitAfterDeliveryError) -> str:
    """What to do when the token was delivered but the commit failed (its outcome is unknown)."""
    head = f"the token with prefix {error.prefix} was delivered but the database commit failed"
    if error.kind == "producer":
        return (
            f"{head}, so it is probably not valid: discard it, check `producer list`, and if the "
            "registration shows prefix " + error.prefix + " or the producer is missing, run "
            "`producer register ... --rotate` again for a credential you can rely on"
        )
    return (
        f"{head}, so it is probably not valid: discard it, run `mcp-token list`, and if prefix "
        f"{error.prefix} is listed run `mcp-token revoke {error.prefix}`; then create a new token"
    )


_PROVISIONING_HINTS = {
    "producer_exists": " (an active or revoked registration exists; use --rotate to replace it)",
    "producer_conflict": " (a concurrent registration won; retry)",
    "token_conflict": " (retry)",
    "invalid_expiry": " (--expires-in is 1-3650 days)",
}


def _run(main: Callable[[], Coroutine[Any, Any, int]]) -> None:
    """Run one command in one event loop and translate failures to exit codes."""
    code: int
    try:
        code = asyncio.run(main())
    except CliUsageError as error:
        typer.echo(f"error: {error}", err=True)
        code = EXIT_USAGE
    except TargetNotEmptyError:
        typer.echo(
            "error: the target holds graph data; use --wipe-target --confirm <database>", err=True
        )
        code = EXIT_USAGE
    except provisioning.MissingProvisioningGrantError as error:
        typer.echo(f"error: {error}", err=True)
        code = EXIT_FAILED
    except DeliveryError:
        typer.echo(
            "error: the token could not be delivered, so nothing was changed "
            "(a rotated producer keeps its previous credential)",
            err=True,
        )
        code = EXIT_FAILED
    except provisioning.CommitAfterDeliveryError as error:
        typer.echo(f"error: {_recovery_message(error)}", err=True)
        code = EXIT_FAILED
    except provisioning.ProvisioningError as error:
        typer.echo(f"error: {error.code}{_PROVISIONING_HINTS.get(error.code, '')}", err=True)
        code = EXIT_USAGE
    except MissingGrantError as error:
        typer.echo(f"error: {error}", err=True)
        code = EXIT_FAILED
    except RunnerActiveError as error:
        typer.echo(f"error: {error}", err=True)
        code = EXIT_FAILED
    except Exception as error:
        typer.echo(f"error: {public_error_class(error)}", err=True)
        code = EXIT_FAILED
    raise typer.Exit(code)


def _recorder(runtime: Runtime) -> ProjectionEventRecorder | None:
    if runtime.recording_sessions is None:
        return None
    return ProjectionEventRecorder.in_process(runtime.recording_sessions)


def _verification_lines(report: VerificationReport, *, require_caught_up: bool) -> list[str]:
    lines = [
        f"graph digest {report.graph.digest} "
        f"({report.graph.node_count} nodes, {report.graph.relationship_count} relationships)"
    ]
    for item in report.projectors:
        state = "ok" if item.ok else "MISMATCH " + ",".join(item.issues)
        lines.append(
            f"projector {item.name} {item.version}: {state} "
            f"(handled {item.handled_events}, covered {item.covered_events}, lag {item.lag})"
        )
    lines.append(
        f"streams: {report.streams.streams} ({report.streams.mismatched} head mismatches, "
        f"{report.streams.behind} behind)"
    )
    lines.append(f"orphan source ids: {report.orphan_count} of {report.source_ids_checked} checked")
    if report.replay_digest is not None:
        lines.append(f"replay digest {report.replay_digest} (matches: {report.replay_matches})")
    if report.record_error is not None:
        lines.append(f"recording failed: {report.record_error}")
    if report.recorded is not None:
        lines.append(
            f"recorded {report.recorded.appended} new projection.rebuilt events "
            f"({report.recorded.existing} already present)"
        )
    problems = report.problems(require_caught_up=require_caught_up)
    lines.append("verified" if not problems else "MISMATCH: " + "; ".join(problems))
    return lines


@projection_app.command("verify")
def verify_command(
    require_caught_up: Annotated[
        bool, typer.Option("--require-caught-up", help="Fail when any projector lags the ledger.")
    ] = False,
    replay_check: Annotated[
        bool,
        typer.Option(
            "--replay-check", help="Also replay into a scratch target and compare digests."
        ),
    ] = False,
    record: Annotated[
        bool, typer.Option("--record/--no-record", help="Record a projection.rebuilt event.")
    ] = True,
    target_uri: _TARGET_URI = None,
    target_database: _TARGET_DATABASE = None,
    target_username: _TARGET_USERNAME = None,
    wipe_target: _WIPE_TARGET = False,
    confirm: _CONFIRM = None,
    json_output: _JSON = False,
    batch_size: _BATCH = DEFAULT_BATCH_SIZE,
) -> None:
    """Check checkpoints, coverage, source IDs and stream heads against the ledger."""

    async def main() -> int:
        settings = _load_settings()
        scratch: Neo4jStore | None = None
        if replay_check:
            target = _target_settings(settings.neo4j, target_uri, target_database, target_username)
            _require_distinct_target(settings.neo4j, target)
            if wipe_target:
                _confirmed(confirm, target_database or "", "--wipe-target")
            scratch = Neo4jStore(target)
        else:
            _forbid_target_options(target_uri, target_database, wipe_target, confirm)
        try:
            async with _runtime(settings, record=record) as runtime:
                await preflight(
                    runtime.sessions, runtime.recording_sessions, write_checkpoints=False
                )
                report = await verify_projections(
                    runtime.sessions,
                    runtime.store,
                    registered_projectors(),
                    require_caught_up=require_caught_up,
                    replay_target=scratch,
                    wipe_replay_target=wipe_target,
                    recorder=_recorder(runtime),
                    batch_size=batch_size,
                )
        finally:
            if scratch is not None:
                await scratch.close()
        _emit(
            report.to_dict(require_caught_up=require_caught_up),
            as_json=json_output,
            lines=_verification_lines(report, require_caught_up=require_caught_up),
        )
        if report.record_error:
            typer.echo(f"error: {report.record_error}", err=True)
            return EXIT_FAILED
        return EXIT_OK if report.ok(require_caught_up=require_caught_up) else EXIT_FAILED

    _run(main)


def _rebuild_lines(report: RebuildReport) -> list[str]:
    lines = [
        f"rebuild ({report.mode}) into {report.target}: {report.outcome.value}",
        f"projected {report.projected_events} events, wiped {report.wiped_nodes} nodes",
    ]
    lines.extend(_verification_lines(report.verification, require_caught_up=False))
    if report.skipped_dead_lettered:
        skipped = report.skipped_dead_lettered
        lines.append(
            f"skipped {len(skipped)} dead-lettered events: "
            + ", ".join(str(item) for item in skipped[:10])
            + (" ..." if len(skipped) > 10 else "")
        )
    if report.record_error is not None:
        lines.append(f"recording failed: {report.record_error}")
    if report.ok:
        lines.append(f"verified target {report.target} digest {report.verification.graph.digest}")
        lines.append("cutover is an operator step: point the API and projector at this target")
    return lines


@projection_app.command("rebuild")
def rebuild_command(
    in_place: Annotated[
        bool,
        typer.Option(
            "--in-place",
            help="Rebuild the live graph. The projection runner MUST be stopped first: there is "
            "no runner lock, so only leases, recent outbox activity and other projector-role "
            "connections are checked.",
        ),
    ] = False,
    target_uri: _TARGET_URI = None,
    target_database: _TARGET_DATABASE = None,
    target_username: _TARGET_USERNAME = None,
    wipe_target: _WIPE_TARGET = False,
    confirm: _CONFIRM = None,
    record: Annotated[
        bool, typer.Option("--record/--no-record", help="Record a projection.rebuilt event.")
    ] = True,
    runner_quiet_seconds: Annotated[
        int,
        typer.Option(
            "--runner-quiet-seconds",
            min=0,
            help="--in-place refuses if an outbox row changed this recently (0 disables).",
        ),
    ] = DEFAULT_RUNNER_QUIET_SECONDS,
    json_output: _JSON = False,
    batch_size: _BATCH = DEFAULT_BATCH_SIZE,
) -> None:
    """Replay the ledger into a standby target (default) or the live graph (--in-place)."""

    async def main() -> int:
        settings = _load_settings()
        if in_place:
            _forbid_target_options(target_uri, target_database, wipe_target)
            _confirmed(confirm, settings.neo4j.database, "--in-place")
            target_settings = settings.neo4j
        else:
            target_settings = _target_settings(
                settings.neo4j, target_uri, target_database, target_username
            )
            _require_distinct_target(settings.neo4j, target_settings)
            if wipe_target:
                _confirmed(confirm, target_settings.database, "--wipe-target")
        async with _runtime(settings, record=record) as runtime:
            # Before anything is mutated: a missing grant must not surface after a target is wiped.
            await preflight(
                runtime.sessions, runtime.recording_sessions, write_checkpoints=in_place
            )
            target = runtime.store if in_place else Neo4jStore(target_settings)
            try:
                report = await rebuild_projections(
                    runtime.sessions,
                    target,
                    registered_projectors(),
                    target_description=_describe(target_settings),
                    in_place=in_place,
                    wipe_target=wipe_target,
                    recorder=_recorder(runtime),
                    batch_size=batch_size,
                    runner_quiet_seconds=runner_quiet_seconds,
                )
            finally:
                if not in_place:
                    await target.close()
        _emit(report.to_dict(), as_json=json_output, lines=_rebuild_lines(report))
        if report.record_error:
            # The rebuild and its verified target stand and are printed above; the report is lost.
            typer.echo(f"error: {report.record_error}", err=True)
            return EXIT_FAILED
        return EXIT_OK if report.ok else EXIT_FAILED

    _run(main)


@projection_app.command("status")
def status_command(json_output: _JSON = False) -> None:
    """Show each projector's checkpoint, lag, last error class and last run (read-only)."""

    async def main() -> int:
        settings = _load_settings()
        async with _runtime(settings) as runtime:
            statuses = await projection_status(runtime.sessions, registered_projectors())
        lines = [
            f"{item.name} {item.version}: state {item.state or 'none'}, "
            f"checkpoint {item.checkpoint_outbox_id}/{item.ledger_head_outbox_id}, "
            f"lag {item.lag}, last error {item.last_error_class or 'none'}, "
            f"last run {item.last_run_at.isoformat() if item.last_run_at else 'never'}"
            for item in statuses
        ]
        _emit(
            {"projectors": [item.to_dict() for item in statuses]}, as_json=json_output, lines=lines
        )
        return EXIT_OK

    _run(main)


# --- projection worker (PLATFORM-039D) ---------------------------------------------------------
# `agent-context projection run`: the process that drains the outbox through `ProjectionRunner`.
# Self-contained block: the loop (`_drive_worker`) takes its collaborators as arguments so it is
# unit-testable without services; `run_command` wires the real runner, probe and signals.

WORKER_MAX_IDLE_SECONDS = 5.0


class _WorkerRunner(Protocol):
    async def run_once(self, limit: int) -> ProjectionRunReport: ...

    async def backlog(self) -> OutboxBacklog: ...


@dataclass(slots=True)
class WorkerTotals:
    """What one `projection run` did; counts only, never content."""

    claimed: int = 0
    delivered: int = 0
    retried: int = 0
    dead_lettered: int = 0
    lost_leases: int = 0
    iterations: int = 0
    paused_for_rebuild: int = 0
    outcome: str = "stopped"  # drained | timeout | stopped | interrupted

    def add(self, report: ProjectionRunReport) -> None:
        self.claimed += report.claimed
        self.delivered += report.delivered
        self.retried += report.retried
        self.dead_lettered += report.dead_lettered
        self.lost_leases += report.lost_leases


def build_projectors(settings: Settings) -> tuple[Projector, ...]:
    """The projector set the worker runs: the ONE place a projector needing configuration binds.

    Today that is the registry as is. A projector that needs a backend built from `settings` (the
    search projector's embedding provider and PostgreSQL writer, PLATFORM-042) is added here, so
    the worker never assembles projectors anywhere else. It must fail closed with a clear error
    when its configuration is missing.
    """
    del settings  # no registered projector needs configuration yet
    return registered_projectors()


def _worker_exit_code(totals: WorkerTotals, *, once: bool) -> int:
    """1 on timeout, an interrupted `--once`, or (`--once` only) a dead-lettered event; else 0.

    A long-running worker that is told to stop exits 0 whatever it dead-lettered meanwhile: a
    routine deploy stop must not look like a failure.
    """
    if totals.outcome in {"timeout", "interrupted"} or (once and totals.dead_lettered):
        return EXIT_FAILED
    return EXIT_OK


def _idle_delay(poll_seconds: float, idle_polls: int) -> float:
    """Idle poll interval: doubles per empty poll up to `WORKER_MAX_IDLE_SECONDS`.

    The exponent is capped before the power, so a worker idle for days cannot overflow a float.
    """
    return max(
        poll_seconds, min(poll_seconds * float(2 ** min(idle_polls, 30)), WORKER_MAX_IDLE_SECONDS)
    )


@asynccontextmanager
async def _shared_rebuild_lock(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[bool]:
    """Hold the rebuild lock SHARED for the body; yield whether a rebuild blocks us (FU-62).

    A rebuild takes the exclusive session-level lock, so a shared try-lock fails exactly while one
    is held (yield True, nothing held). Otherwise the shared lock stays held on this dedicated
    connection for the whole body (a claim and its batch), and is released afterwards, also on a
    stop or an error: a rebuild's exclusive try-lock is refused meanwhile, which is retryable.
    Workers share the lock with each other.
    """
    async with sessions() as session:
        free = bool(
            await session.scalar(
                text("SELECT pg_try_advisory_lock_shared(:key)"), {"key": REBUILD_LOCK_KEY}
            )
        )
        try:
            yield not free
        finally:
            if free:
                try:
                    await session.rollback()  # session-level: the lock survives the rollback
                    await session.scalar(
                        text("SELECT pg_advisory_unlock_shared(:key)"), {"key": REBUILD_LOCK_KEY}
                    )
                except BaseException:
                    # Never hand a connection that may still hold the lock back to the pool.
                    await session.invalidate()
                    raise
            await session.rollback()


async def _drive_worker(
    runner: _WorkerRunner,
    rebuild_guard: Callable[[], AbstractAsyncContextManager[bool]],
    stop: asyncio.Event,
    *,
    once: bool,
    batch_size: int,
    poll_seconds: float,
    max_seconds: float | None,
    log_seconds: float,
    touch: Callable[[], None],
    log: Callable[[str], None],
    now: Callable[[], float] = time.monotonic,
) -> WorkerTotals:
    """Poll the outbox until drained (`once`), timed out or told to stop.

    Each iteration: pause while a rebuild holds its lock, else claim and project one batch with
    the lock held shared throughout. A
    batch is always finished (never abandoned mid-way), so a stop request leaves no lease behind.
    """
    totals = WorkerTotals()
    started = last_log = now()
    paused = False
    idle = 0

    async def nap(seconds: float) -> None:
        if max_seconds is not None:
            seconds = min(seconds, max(started + max_seconds - now(), 0.0))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)

    while not stop.is_set():
        if max_seconds is not None and now() - started >= max_seconds:
            totals.outcome = "timeout"
            return totals
        totals.iterations += 1
        # The shared rebuild lock is held from before the claim until the batch is finalized.
        async with rebuild_guard() as blocked:
            if not blocked:
                if paused:
                    log("resumed_after_rebuild")
                    paused = False
                # Entering the guard is a round trip: a stop or the deadline may have arrived.
                if stop.is_set():
                    break
                if max_seconds is not None and now() - started >= max_seconds:
                    totals.outcome = "timeout"
                    return totals
                report = await runner.run_once(batch_size)
        if blocked:
            if not paused:
                log("paused_for_rebuild")
                paused = True
            totals.paused_for_rebuild += 1
            touch()
            await nap(poll_seconds)
            continue
        totals.add(report)
        touch()
        if report.claimed:
            idle = 0
        else:
            if once and (await runner.backlog()).outstanding == 0:
                totals.outcome = "drained"
                return totals
            idle += 1
            # Idle polls back off; a `once` drain waiting out a retry delay polls steadily.
            await nap(poll_seconds if once else _idle_delay(poll_seconds, idle))
        if now() - last_log >= log_seconds:
            last_log = now()
            backlog = await runner.backlog()
            log(
                f"progress delivered={totals.delivered} retried={totals.retried} "
                f"dead_lettered={totals.dead_lettered} lag={backlog.outstanding} "
                f"leased={backlog.leased} dead_letters_total={backlog.dead_lettered}"
            )
    # Only a stop request leaves the loop here: a `--once` drain cut short is not a drain.
    totals.outcome = "interrupted" if once else "stopped"
    return totals


def _worker_lines(totals: WorkerTotals, statuses: Sequence[ProjectorStatus]) -> list[str]:
    lines = [
        f"{totals.outcome}: claimed {totals.claimed}, delivered {totals.delivered}, "
        f"retried {totals.retried}, dead-lettered {totals.dead_lettered}, "
        f"lost leases {totals.lost_leases}"
    ]
    lines.extend(
        f"{item.name} {item.version}: checkpoint {item.checkpoint_outbox_id}/"
        f"{item.ledger_head_outbox_id}, lag {item.lag}"
        for item in statuses
    )
    return lines


@projection_app.command("run")
def run_command(
    once: Annotated[
        bool,
        typer.Option(
            "--once",
            help="Drain the outbox (waiting out retry delays), print a summary and exit.",
        ),
    ] = False,
    max_seconds: Annotated[
        float | None,
        typer.Option("--max-seconds", min=0.001, help="Stop after N seconds: exit 1, `timeout`."),
    ] = None,
    health_file: Annotated[
        Path | None,
        typer.Option(
            "--health-file",
            help="Touch this file after every loop iteration; removed on shutdown.",
        ),
    ] = None,
    batch_size: Annotated[
        int, typer.Option("--batch-size", min=1, max=1000, help="Outbox rows claimed per poll.")
    ] = 10,
    poll_seconds: Annotated[
        float,
        typer.Option("--poll-seconds", min=0.01, help="Idle poll interval (idle polls back off)."),
    ] = 1.0,
    log_seconds: Annotated[
        float,
        typer.Option("--log-seconds", min=0.1, help="At most one progress line per N seconds."),
    ] = 30.0,
    json_output: _JSON = False,
) -> None:
    """Run the projection worker: claim outbox rows and project them into the graph.

    Without `--once` it runs until SIGTERM or SIGINT: it stops claiming, finishes the batch in
    hand (no lease outlives the process), closes its connections and exits 0. With `--once` it
    drains the outbox and exits 0, or 1 when an event was dead-lettered during the run, or on
    `--max-seconds` expiring (`timeout`), or when SIGTERM/SIGINT cuts it short (`interrupted`). Pauses (`paused_for_rebuild`) while a rebuild holds the
    rebuild lock. Uses the projector role. Progress and the summary are counts only; never a DSN
    or event content.
    """

    async def main() -> int:
        if health_file is not None and not health_file.parent.is_dir():
            raise CliUsageError("--health-file must be in an existing directory")
        settings = _load_settings()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        handled: list[signal.Signals] = []
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.add_signal_handler(sig, stop.set)
                handled.append(sig)

        def touch() -> None:
            if health_file is not None:
                health_file.touch()

        def log(message: str) -> None:
            typer.echo(f"projection_worker {message}", err=True)

        worker_id = f"projection-worker-{os.getpid()}-{uuid4().hex[:8]}"
        runner: ProjectionRunner | None = None
        try:
            async with _runtime(settings) as runtime:
                projectors = build_projectors(settings)
                runner = ProjectionRunner(
                    runtime.sessions, runtime.store, projectors, worker_id=worker_id
                )
                touch()
                log("started")
                try:
                    totals = await _drive_worker(
                        runner,
                        lambda: _shared_rebuild_lock(runtime.sessions),
                        stop,
                        once=once,
                        batch_size=batch_size,
                        poll_seconds=poll_seconds,
                        max_seconds=max_seconds,
                        log_seconds=log_seconds,
                        touch=touch,
                        log=log,
                    )
                finally:
                    # Never leave a lease behind, even when the loop died on an error.
                    with contextlib.suppress(Exception):
                        await asyncio.shield(runner.release_leases())
                statuses = await projection_status(runtime.sessions, projectors)
        finally:
            for sig in handled:
                loop.remove_signal_handler(sig)
            if health_file is not None:
                health_file.unlink(missing_ok=True)
        log("stopped")
        _emit(
            {**asdict(totals), "projectors": [item.to_dict() for item in statuses]},
            as_json=json_output,
            lines=_worker_lines(totals, statuses),
        )
        return _worker_exit_code(totals, once=once)

    _run(main)


# --- indexing (PLATFORM-039C) ------------------------------------------------------------------


def _index_lines(report: IndexReport) -> list[str]:
    target = "no target" if report.target is None else f"{report.target_kind} {report.target}"
    lines = [
        f"index {report.index_id or 'none'} ({target})",
        f"events: {report.submitted} submitted, {report.skipped} already present; "
        f"files: {report.files} indexed, {report.files_not_indexed} not indexed",
        "completed" if report.success else f"failed: {report.error_class or 'unsuccessful'}",
    ]
    if report.diagnostics:
        lines.append(
            "diagnostics: " + ", ".join(f"{k}={v}" for k, v in sorted(report.diagnostics.items()))
        )
    return lines


@app.command("index")
def index_command(
    repository_id: Annotated[
        str, typer.Option("--repository-id", help="Canonical platform repository id.")
    ],
    checkout: Annotated[Path, typer.Option("--checkout", help="Git checkout to index.")],
    scip: Annotated[
        Path | None, typer.Option("--scip", help="SCIP index of the checked-out HEAD commit.")
    ] = None,
    json_output: _JSON = False,
) -> None:
    """Index a git checkout (a commit, or the dirty snapshot) into the ledger.

    Runs as the API role (`AGENT_CONTEXT_POSTGRESQL__API_DSN` or `__DSN`). Prints ids and counts
    only; never file content, a DSN or a secret. Exit 0 on success, 1 when the index failed
    (the counts are still printed), 2 on a usage error.
    """

    async def main() -> int:
        try:
            repository_namespace(repository_id)
        except ValueError:
            raise CliUsageError(
                "--repository-id must be non-empty text without control characters"
            ) from None
        if not checkout.is_dir():
            raise CliUsageError("--checkout must be an existing directory")
        if scip is not None and not scip.is_file():
            raise CliUsageError("--scip must be an existing file")
        settings = _load_settings()
        engine = _engine(settings.postgresql.effective_api_dsn, "API_DSN")
        try:
            sessions = session_factory(engine)
            ingestion = IngestionService(
                ContentService(NoContentBlobStore(), RedactionPolicyV1()), sessions
            )
            service = IndexingService(IndexingConfig(repository_id), ingestion, sessions)
            report = await index_checkout(
                IndexRequest(repository_id, checkout, scip), settings, service
            )
        finally:
            await engine.dispose()
        _emit(report.to_dict(), as_json=json_output, lines=_index_lines(report))
        return EXIT_OK if report.success else EXIT_FAILED

    _run(main)


# --- credential provisioning (PLATFORM-039B) ---------------------------------------------------

_EXPIRES_IN = Annotated[
    int,
    typer.Option(
        "--expires-in", min=1, max=3650, help="Days until the credential expires (required)."
    ),
]
_OUTPUT = Annotated[
    Path | None,
    typer.Option(
        "--output",
        help="Write the token to this NEW file (mode 0600, outside any git work tree) instead of "
        "stdout.",
    ),
]


@asynccontextmanager
async def _admin_sessions(
    settings: Settings, *, tables: Sequence[str], write: bool
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """The operator connection, with its `operations.*` grants verified before any write."""
    engine = _engine(settings.postgresql.effective_admin_dsn, "ADMIN_DSN")
    try:
        sessions = session_factory(engine)
        await provisioning.check_grants(sessions, tables=tables, write=write)
        yield sessions
    finally:
        await engine.dispose()


def _hash_cost(settings: Settings) -> provisioning.HashCost:
    ingestion = settings.ingestion
    return provisioning.HashCost(
        ingestion.argon2_time_cost, ingestion.argon2_memory_cost_kib, ingestion.argon2_parallelism
    )


def _inside_git_work_tree(directory: Path) -> bool:
    return any((parent / ".git").exists() for parent in (directory, *directory.parents))


class SecretFile:
    """A new 0600 file whose descriptor is held from creation until the token is written.

    The path is never reopened: a writer of the directory who swaps the name during the slow
    hash/database step cannot receive the token, and cleanup unlinks only what is still ours.
    """

    def __init__(self, path: Path, descriptor: int) -> None:
        self.path = path
        self._descriptor: int | None = descriptor
        status = os.fstat(descriptor)
        self._identity = (status.st_dev, status.st_ino)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_nlink != 1
            or status.st_uid != os.geteuid()
            or stat.S_IMODE(status.st_mode) != 0o600
        ):
            self.abandon()
            raise CliUsageError("--output is not a private regular file")

    @classmethod
    def create(cls, path: Path) -> SecretFile:
        """Create ``path`` (a new file only, mode 0600) before any credential is issued."""
        parent = path.expanduser().absolute().parent
        try:
            parent = parent.resolve(strict=True)
        except OSError:
            raise CliUsageError("the --output directory does not exist") from None
        if _inside_git_work_tree(parent):
            raise CliUsageError("--output must not be inside a git work tree")
        target = parent / path.name
        try:
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                stat.S_IRUSR | stat.S_IWUSR,
            )
        except FileExistsError:
            raise CliUsageError("--output already exists; it is never overwritten") from None
        except OSError:
            raise CliUsageError("--output cannot be created") from None
        return cls(target, descriptor)

    def write(self, token: str) -> None:
        """Write through the held descriptor and close it."""
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is None:
            raise CliUsageError("--output was already written")
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            handle.write(token + "\n")

    def abandon(self) -> None:
        """Close and remove the file, but only if the path still names the file we created."""
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is not None:
            os.close(descriptor)
        try:
            current = self.path.lstat()
        except OSError:
            return
        if (current.st_dev, current.st_ino) != self._identity:
            typer.echo("warning: --output was replaced meanwhile; left in place", err=True)
            return
        self.path.unlink(missing_ok=True)


def _flush_stdout() -> None:
    sys.stdout.flush()


async def _issue(
    output: Path | None,
    json_output: bool,
    issue: Callable[[provisioning.Deliver], Coroutine[Any, Any, provisioning.IssuedCredential]],
) -> int:
    """Run ``issue``, delivering the token INSIDE its still-open transaction.

    ``deliver`` writes the plaintext (to the held ``--output`` descriptor, or flushed stdout)
    before the commit, so a delivery failure rolls back and no undeliverable credential exists.
    If the commit fails afterwards the delivered file is removed; the token is unusable.
    """
    destination = None if output is None else SecretFile.create(output)

    def deliver(credential: provisioning.IssuedCredential) -> None:
        _hand_over(credential, destination, as_json=json_output)

    try:
        credential = await issue(deliver)
    except BaseException:
        if destination is not None:
            destination.abandon()
        raise
    _report(credential, destination, as_json=json_output)
    return EXIT_OK


def _document(
    credential: provisioning.IssuedCredential, destination: SecretFile | None
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "kind": credential.kind,
        "id": credential.identifier,
        "prefix": credential.prefix,
        "scope": credential.scope,
        "expires_at": credential.expires_at.isoformat(),
        "rotated": credential.rotated,
    }
    if destination is not None:
        document["output"] = str(destination.path)
    return document


def _hand_over(
    credential: provisioning.IssuedCredential, destination: SecretFile | None, *, as_json: bool
) -> None:
    """Deliver the plaintext exactly once: into the held file, or flushed on stdout."""
    try:
        if destination is not None:
            destination.write(credential.token)
            return
        if as_json:
            typer.echo(
                json.dumps(
                    {**_document(credential, None), "token": credential.token}, sort_keys=True
                )
            )
        else:
            typer.echo(credential.token)
        _flush_stdout()
    except OSError:
        raise DeliveryError from None


def _report(
    credential: provisioning.IssuedCredential, destination: SecretFile | None, *, as_json: bool
) -> None:
    """Everything that is not the plaintext, after the commit."""
    document = _document(credential, destination)
    if as_json:
        if destination is not None:
            typer.echo(json.dumps(document, sort_keys=True))
        return
    typer.echo(", ".join(f"{key} {value}" for key, value in document.items()), err=True)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


@producer_app.command("register")
def producer_register_command(
    producer_id: Annotated[str, typer.Option("--producer-id", help="Producer to register.")],
    expires_in: _EXPIRES_IN,
    rotate: Annotated[
        bool,
        typer.Option("--rotate", help="Replace the credential of an active registration."),
    ] = False,
    output: _OUTPUT = None,
    json_output: _JSON = False,
) -> None:
    """Register a producer and print its `events:ingest` bearer once."""

    async def main() -> int:
        settings = _load_settings()

        async def issue(
            deliver: provisioning.Deliver,
        ) -> provisioning.IssuedCredential:
            async with _admin_sessions(
                settings, tables=("operations.registered_producers",), write=True
            ) as sessions:
                return await provisioning.register_producer(
                    sessions,
                    producer_id=producer_id,
                    expires_in_days=expires_in,
                    cost=_hash_cost(settings),
                    rotate=rotate,
                    deliver=deliver,
                )

        return await _issue(output, json_output, issue)

    _run(main)


@producer_app.command("revoke")
def producer_revoke_command(
    producer_id: Annotated[str, typer.Argument(help="Producer to revoke.")],
    json_output: _JSON = False,
) -> None:
    """Revoke a producer's credential (the registration row is kept)."""

    async def main() -> int:
        settings = _load_settings()
        async with _admin_sessions(
            settings, tables=("operations.registered_producers",), write=True
        ) as sessions:
            changed = await provisioning.revoke_producer(sessions, producer_id)
        _emit(
            {"producer_id": producer_id, "revoked": True, "changed": changed},
            as_json=json_output,
            lines=[f"producer {producer_id}: " + ("revoked" if changed else "already revoked")],
        )
        return EXIT_OK

    _run(main)


@producer_app.command("list")
def producer_list_command(json_output: _JSON = False) -> None:
    """List registered producers (never their verifiers)."""

    async def main() -> int:
        settings = _load_settings()
        async with _admin_sessions(
            settings, tables=("operations.registered_producers",), write=False
        ) as sessions:
            items = await provisioning.list_producers(sessions)
        now = datetime.now(UTC)
        records = [
            {
                "producer_id": item.producer_id,
                "token_prefix": item.token_prefix,
                "scope": item.scope,
                "created_at": _iso(item.created_at),
                "expires_at": _iso(item.expires_at),
                "revoked_at": _iso(item.revoked_at),
                "last_used_at": _iso(item.last_used_at),
            }
            for item in items
        ]
        lines = [
            f"{item.producer_id} {item.token_prefix} {item.scope} "
            f"{_state(item.revoked_at, item.expires_at, now)} expires {item.expires_at.isoformat()}"
            for item in items
        ]
        _emit({"producers": records}, as_json=json_output, lines=lines)
        return EXIT_OK

    _run(main)


def _state(revoked_at: datetime | None, expires_at: datetime, now: datetime) -> str:
    if revoked_at is not None and revoked_at <= now:
        return "revoked"
    return "expired" if expires_at <= now else "active"


@mcp_token_app.command("create")
def mcp_token_create_command(
    principal: Annotated[str, typer.Option("--principal", help="Who the token identifies.")],
    expires_in: _EXPIRES_IN,
    scope: Annotated[
        str, typer.Option("--scope", help="Token scope (memory:read).")
    ] = "memory:read",
    output: _OUTPUT = None,
    json_output: _JSON = False,
) -> None:
    """Create a read-only MCP bearer and print it once."""

    async def main() -> int:
        if scope not in provisioning.MCP_SCOPES:
            raise CliUsageError("--scope must be one of: " + ", ".join(provisioning.MCP_SCOPES))
        settings = _load_settings()

        async def issue(
            deliver: provisioning.Deliver,
        ) -> provisioning.IssuedCredential:
            async with _admin_sessions(
                settings, tables=("operations.mcp_tokens",), write=True
            ) as sessions:
                return await provisioning.create_mcp_token(
                    sessions,
                    principal=principal,
                    scope=scope,
                    expires_in_days=expires_in,
                    cost=_hash_cost(settings),
                    deliver=deliver,
                )

        return await _issue(output, json_output, issue)

    _run(main)


@mcp_token_app.command("revoke")
def mcp_token_revoke_command(
    prefix: Annotated[str, typer.Argument(help="Token prefix (see `mcp-token list`).")],
    json_output: _JSON = False,
) -> None:
    """Revoke an MCP token by its prefix. A replica may honour it for up to the cache TTL."""

    async def main() -> int:
        settings = _load_settings()
        async with _admin_sessions(
            settings, tables=("operations.mcp_tokens",), write=True
        ) as sessions:
            changed = await provisioning.revoke_mcp_token(sessions, prefix)
        _emit(
            {"prefix": prefix, "revoked": True, "changed": changed},
            as_json=json_output,
            lines=[f"mcp token {prefix}: " + ("revoked" if changed else "already revoked")],
        )
        return EXIT_OK

    _run(main)


@mcp_token_app.command("list")
def mcp_token_list_command(json_output: _JSON = False) -> None:
    """List MCP tokens (never their verifiers)."""

    async def main() -> int:
        settings = _load_settings()
        async with _admin_sessions(
            settings, tables=("operations.mcp_tokens",), write=False
        ) as sessions:
            items = await provisioning.list_mcp_tokens(sessions)
        now = datetime.now(UTC)
        records = [
            {
                "token_id": str(item.token_id),
                "token_prefix": item.token_prefix,
                "principal": item.principal,
                "scopes": list(item.scopes),
                "created_at": _iso(item.created_at),
                "expires_at": _iso(item.expires_at),
                "revoked_at": _iso(item.revoked_at),
            }
            for item in items
        ]
        lines = [
            f"{item.token_prefix} {item.principal} {','.join(item.scopes)} "
            f"{_state(item.revoked_at, item.expires_at, now)} expires {item.expires_at.isoformat()}"
            for item in items
        ]
        _emit({"tokens": records}, as_json=json_output, lines=lines)
        return EXIT_OK

    _run(main)


if __name__ == "__main__":
    app()
