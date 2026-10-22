"""`GET /health/ready`: whether this replica can serve (PLATFORM-049).

Three checks run concurrently, each bounded by its own short timeout: PostgreSQL connectivity
plus the schema being at exactly the Alembic head this image ships, Neo4j connectivity, and S3
bucket reachability. They only read: one ``SELECT`` in a read-only transaction, a driver
connectivity check and a ``HeadBucket``.

The result holds component names and coarse states only, never a DSN, a host or an exception
text. Concurrent requests share one in-flight probe, so a burst of polls costs one round of
checks. PostgreSQL is checked over readiness's own unpooled engine with libpq connect and TCP
timeouts, so a dead socket left by a failover is never reused.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import PostgresDsn, Secret
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.settings import S3Settings, Settings

DEFAULT_TIMEOUT_SECONDS: Final = 2.0
MAX_ABANDONED_PER_CHECK: Final = 4
READINESS_APPLICATION_NAME: Final = "agent-context-readiness"

ComponentState = Literal[
    "ok",  # reachable and as expected
    "unconfigured",  # its settings (or the image's Alembic head) are missing
    "unavailable",  # unreachable or failed
    "timeout",  # no answer within the timeout
    "forbidden",  # reachable, but the credential may not perform the check
    "mismatch",  # the schema is not at the image's head
    "unknown",  # not checked because a prerequisite failed
]
Components = dict[str, ComponentState]
Check = Callable[[], Awaitable[Components]]

_INSUFFICIENT_PRIVILEGE: Final = "42501"
_UNDEFINED_TABLE: Final = "42P01"
# What the API role reads outside the request path: the schema head (readiness) and projection
# progress (freshness). Migration 20261022_0001 grants SELECT on exactly these.
API_READ_TABLES: Final = (
    "public.alembic_version",
    "projection.projection_checkpoints",
    "projection.dead_letters",
)
_FORBIDDEN_S3_CODES: Final = frozenset({"403", "AccessDenied", "Forbidden"})


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    components: Mapping[str, ComponentState]

    @property
    def ready(self) -> bool:
        # Nothing checked is not ready.
        return bool(self.components) and all(state == "ok" for state in self.components.values())

    def to_dict(self) -> dict[str, Any]:
        if self.ready:
            return {"status": "ready"}
        return {"status": "not_ready", "components": dict(sorted(self.components.items()))}


def _failed(on_timeout: Components) -> Components:
    return {
        name: "unavailable" if state == "timeout" else state for name, state in on_timeout.items()
    }


@dataclass(slots=True)
class _Entry:
    check: Check
    on_timeout: Components
    # Timed-out checks, cancelled but not awaited: strong references until they finish.
    abandoned: set[asyncio.Task[Components]] = field(default_factory=set)

    def reap(self, task: asyncio.Task[Components]) -> None:
        self.abandoned.discard(task)
        if not task.cancelled():
            task.exception()  # retrieved, so it is never logged; its text may hold a host


class ReadinessProbe:
    """Runs the named checks concurrently; a check's ``on_timeout`` states report a timeout.

    The bound holds even for a check that ignores cancellation (psycopg answers a cancellation
    by asking the server to cancel and then waiting for it): a timed-out check is cancelled but
    never awaited, and at most ``MAX_ABANDONED_PER_CHECK`` of them are kept per check; beyond
    that the check reports ``on_timeout`` without starting another. A request joins an
    in-flight probe only while it is younger than the timeout.
    """

    def __init__(
        self,
        checks: Mapping[str, tuple[Check, Components]],
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._entries = {
            name: _Entry(check, dict(on_timeout)) for name, (check, on_timeout) in checks.items()
        }
        self._timeout = timeout_seconds
        self._in_flight: tuple[asyncio.Future[ReadinessReport], float] | None = None
        self._probes: set[asyncio.Future[ReadinessReport]] = set()

    def abandoned(self, name: str) -> int:
        return len(self._entries[name].abandoned)

    async def probe(self) -> ReadinessReport:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if (
            self._in_flight is None
            or self._in_flight[0].done()
            or now - self._in_flight[1] >= self._timeout
        ):
            task = asyncio.ensure_future(self._probe())
            self._probes.add(task)
            task.add_done_callback(self._probes.discard)
            self._in_flight = (task, now)
        # Shielded: a client that disconnects must not cancel the probe others are awaiting.
        return await asyncio.shield(self._in_flight[0])

    async def _probe(self) -> ReadinessReport:
        results = await asyncio.gather(*(self._bounded(entry) for entry in self._entries.values()))
        components: Components = {}
        for result in results:
            components.update(result)
        return ReadinessReport(components)

    async def _bounded(self, entry: _Entry) -> Components:
        if len(entry.abandoned) >= MAX_ABANDONED_PER_CHECK:
            return dict(entry.on_timeout)
        task = asyncio.ensure_future(entry.check())
        try:
            done, _ = await asyncio.wait({task}, timeout=self._timeout)
        except asyncio.CancelledError:
            task.cancel()
            raise
        if not done:
            task.cancel()
            entry.abandoned.add(task)
            task.add_done_callback(entry.reap)
            return dict(entry.on_timeout)
        try:
            return task.result()
        except Exception:
            # Never surface the exception: its text may hold a host or a DSN.
            return _failed(entry.on_timeout)


def unconfigured(*names: str) -> tuple[Check, Components]:
    states: Components = dict.fromkeys(names, "unconfigured")

    async def check() -> Components:
        return dict(states)

    return check, states


def readiness_connect_args(timeout_seconds: float) -> dict[str, Any]:
    """libpq options that bound a connect and make a dead socket fail instead of hang."""
    return {
        "application_name": READINESS_APPLICATION_NAME,
        "connect_timeout": max(2, math.ceil(timeout_seconds)),  # libpq's minimum is 2
        "keepalives": 1,
        "keepalives_idle": 5,
        "keepalives_interval": 2,
        "keepalives_count": 2,
        "tcp_user_timeout": max(1000, math.ceil(timeout_seconds * 2000)),
    }


def readiness_engine(dsn: Secret[PostgresDsn], timeout_seconds: float) -> AsyncEngine:
    """Readiness's own engine: a fresh, bounded connection per probe, never a pooled one."""
    return create_async_engine(
        dsn.get_secret_value().unicode_string(),
        poolclass=NullPool,
        connect_args=readiness_connect_args(timeout_seconds),
    )


def postgresql_check(
    engine: AsyncEngine,
    expected_head: str | None,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> tuple[Check, Components]:
    """Connect, read ``alembic_version`` read-only and compare it with the image's head.

    The connecting role needs ``SELECT`` on every table of ``API_READ_TABLES`` (migration
    20261022_0001 grants exactly that to ``agent_context_api``); a missing one reports the
    schema ``forbidden`` (fail closed), never assumed current.
    """

    statement_ms = max(1, math.ceil(timeout_seconds * 1000))

    async def check() -> Components:
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                await connection.execute(text(f"SET LOCAL statement_timeout = {statement_ms}"))
                try:
                    rows = (
                        await connection.execute(
                            text("SELECT version_num FROM public.alembic_version")
                        )
                    ).scalars()
                    revisions = tuple(rows)
                    privileged = (
                        await connection.execute(
                            text(
                                "SELECT bool_and(has_table_privilege(name, 'SELECT')) "
                                "FROM unnest(CAST(:tables AS text[])) AS name"
                            ),
                            {"tables": list(API_READ_TABLES)},
                        )
                    ).scalar()
                except DBAPIError as error:
                    sqlstate = getattr(error.orig, "sqlstate", None)
                    if sqlstate == _INSUFFICIENT_PRIVILEGE:
                        return {"postgresql": "ok", "schema": "forbidden"}
                    if sqlstate == _UNDEFINED_TABLE:
                        return {"postgresql": "ok", "schema": "mismatch"}
                    raise
                finally:
                    await connection.rollback()
        except (SQLAlchemyError, OSError):
            return {"postgresql": "unavailable", "schema": "unknown"}
        if privileged is not True:
            return {"postgresql": "ok", "schema": "forbidden"}
        if expected_head is None:
            return {"postgresql": "ok", "schema": "unconfigured"}
        return {
            "postgresql": "ok",
            "schema": "ok" if revisions == (expected_head,) else "mismatch",
        }

    return check, {"postgresql": "timeout", "schema": "unknown"}


def neo4j_check(store: Neo4jStore) -> tuple[Check, Components]:
    async def check() -> Components:
        health = await store.verify_connectivity()
        if health.status == "ok":
            return {"neo4j": "ok"}
        states: dict[str | None, ComponentState] = {
            "authentication": "forbidden",
            "configuration": "unconfigured",
        }
        return {"neo4j": states.get(health.reason, "unavailable")}

    return check, {"neo4j": "timeout"}


def s3_client(settings: S3Settings, timeout_seconds: float) -> Any:
    """A client of its own: one attempt, timeouts no longer than the probe's, no retries."""
    session = boto3.session.Session(
        aws_access_key_id=None
        if settings.access_key_id is None
        else settings.access_key_id.get_secret_value(),
        aws_secret_access_key=None
        if settings.secret_access_key is None
        else settings.secret_access_key.get_secret_value(),
        region_name=settings.region_name,
    )
    return session.client(
        "s3",
        endpoint_url=None if settings.endpoint_url is None else str(settings.endpoint_url),
        region_name=settings.region_name,
        config=BotoConfig(
            connect_timeout=timeout_seconds,
            read_timeout=timeout_seconds,
            retries={"total_max_attempts": 1, "mode": "standard"},
            s3={"addressing_style": settings.addressing_style},
        ),
    )


def s3_check(client: Any, bucket_name: str) -> tuple[Check, Components]:
    async def check() -> Components:
        try:
            await asyncio.to_thread(client.head_bucket, Bucket=bucket_name)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            return {"s3": "forbidden" if code in _FORBIDDEN_S3_CODES else "unavailable"}
        except BotoCoreError:
            return {"s3": "unavailable"}
        return {"s3": "ok"}

    return check, {"s3": "timeout"}


def build_probe(
    settings: Settings,
    engine: AsyncEngine | None,
    store: Neo4jStore | None,
    expected_head: str | None,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> ReadinessProbe:
    """Wire the checks from settings; a missing dependency is ``unconfigured`` (not ready)."""
    s3 = settings.s3
    checks = {
        "postgresql": unconfigured("postgresql", "schema")
        if engine is None
        else postgresql_check(engine, expected_head, timeout_seconds=timeout_seconds),
        "neo4j": unconfigured("neo4j") if store is None else neo4j_check(store),
        "s3": unconfigured("s3")
        if s3.endpoint_url is None
        else s3_check(s3_client(s3, timeout_seconds), s3.bucket_name),
    }
    return ReadinessProbe(checks, timeout_seconds=timeout_seconds)
