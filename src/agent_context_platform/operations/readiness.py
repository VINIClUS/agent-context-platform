"""`GET /health/ready`: whether this replica can serve (PLATFORM-049).

Three checks run concurrently, each bounded by its own short timeout: PostgreSQL connectivity
plus the schema being at exactly the Alembic head this image ships, Neo4j connectivity, and S3
bucket reachability. They only read: one ``SELECT`` in a read-only transaction, a driver
connectivity check and a ``HeadBucket``.

The result holds component names and coarse states only, never a DSN, a host or an exception
text. Concurrent requests share one in-flight probe, so a burst of polls costs one round of
checks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.projection.neo4j import Neo4jStore
from agent_context_platform.settings import S3Settings, Settings

DEFAULT_TIMEOUT_SECONDS: Final = 2.0

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


async def _bounded(check: Check, timeout: float, on_timeout: Components) -> Components:
    try:
        return await asyncio.wait_for(check(), timeout)
    except TimeoutError:
        return on_timeout
    except Exception:
        # Never surface the exception: its text may hold a host or a DSN.
        return {
            name: "unavailable" if state == "timeout" else state
            for name, state in on_timeout.items()
        }


class ReadinessProbe:
    """Runs the named checks concurrently; ``timeout_states`` are reported when one times out."""

    def __init__(
        self,
        checks: Mapping[str, tuple[Check, Components]],
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._checks = dict(checks)
        self._timeout = timeout_seconds
        self._in_flight: asyncio.Task[ReadinessReport] | None = None

    async def probe(self) -> ReadinessReport:
        if self._in_flight is None or self._in_flight.done():
            self._in_flight = asyncio.create_task(self._probe())
        # Shielded: a client that disconnects must not cancel the probe others are awaiting.
        return await asyncio.shield(self._in_flight)

    async def _probe(self) -> ReadinessReport:
        results = await asyncio.gather(
            *(
                _bounded(check, self._timeout, on_timeout)
                for check, on_timeout in self._checks.values()
            )
        )
        components: Components = {}
        for result in results:
            components.update(result)
        return ReadinessReport(components)


def unconfigured(*names: str) -> tuple[Check, Components]:
    states: Components = dict.fromkeys(names, "unconfigured")

    async def check() -> Components:
        return dict(states)

    return check, states


def postgresql_check(engine: AsyncEngine, expected_head: str | None) -> tuple[Check, Components]:
    """Connect, read ``alembic_version`` read-only and compare it with the image's head.

    The connecting role needs ``SELECT`` on ``public.alembic_version``; without it the schema
    is reported ``forbidden`` (fail closed), never assumed current.
    """

    async def check() -> Components:
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                try:
                    rows = (
                        await connection.execute(
                            text("SELECT version_num FROM public.alembic_version")
                        )
                    ).scalars()
                    revisions = tuple(rows)
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
        else postgresql_check(engine, expected_head),
        "neo4j": unconfigured("neo4j") if store is None else neo4j_check(store),
        "s3": unconfigured("s3")
        if s3.endpoint_url is None
        else s3_check(s3_client(s3, timeout_seconds), s3.bucket_name),
    }
    return ReadinessProbe(checks, timeout_seconds=timeout_seconds)
