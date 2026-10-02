"""`agent-context`: the operator command line.

Today it exposes `agent-context projection rebuild|verify|status` (PLATFORM-039). Every command
reads the process environment through `Settings` (`AGENT_CONTEXT_*`), runs in one `asyncio.run`,
and never prints a DSN, a password or an exception message: failures are reported by class name.

Exit codes: 0 success, 1 verification mismatch or a failed operation, 2 usage error (bad or
missing options, an unsafe refusal such as a non-empty target without `--wipe-target`).
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any

import typer
from pydantic import PostgresDsn, Secret, ValidationError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from agent_context_platform.db import session_factory
from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.projection.registry import registered_projectors
from agent_context_platform.projection.verify import (
    CLI_APPLICATION_NAME,
    DEFAULT_BATCH_SIZE,
    DEFAULT_RUNNER_QUIET_SECONDS,
    MissingGrantError,
    ProjectionEventRecorder,
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


if __name__ == "__main__":
    app()
