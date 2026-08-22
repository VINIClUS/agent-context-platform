from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, LiteralString, Self, TypeVar

from neo4j import (
    AsyncDriver,
    AsyncGraphDatabase,
    AsyncManagedTransaction,
    EagerResult,
    unit_of_work,
)
from neo4j.exceptions import AuthError, ConfigurationError, DriverError, Neo4jError

from agent_context_platform.settings import Neo4jSettings

HealthStatus = Literal["ok", "error"]
HealthReason = Literal["authentication", "configuration", "unavailable"]
ResultT = TypeVar("ResultT")


@dataclass(frozen=True, slots=True)
class Neo4jHealth:
    """Credential-safe connectivity result."""

    status: HealthStatus
    reason: HealthReason | None = None


class Neo4jTransaction:
    """Bounded, parameter-only transaction interface."""

    def __init__(self, transaction: AsyncManagedTransaction) -> None:
        self._transaction = transaction

    async def run(
        self,
        query: LiteralString,
        *,
        parameters: dict[str, object],
    ) -> EagerResult:
        result = await self._transaction.run(
            query,
            parameters=parameters,
        )
        return await result.to_eager_result()


TransactionCallback = Callable[[Neo4jTransaction], Awaitable[ResultT]]


class Neo4jStore:
    """Lazily initialized async Neo4j driver boundary."""

    def __init__(self, settings: Neo4jSettings) -> None:
        self._settings = settings
        self._driver: AsyncDriver | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    def _get_driver(self) -> AsyncDriver:
        if self._driver is not None:
            return self._driver
        if (
            self._settings.uri is None
            or self._settings.username is None
            or self._settings.password is None
        ):
            raise ConfigurationError("Neo4j connection settings are incomplete")

        self._driver = AsyncGraphDatabase.driver(
            str(self._settings.uri),
            auth=(self._settings.username, self._settings.password.get_secret_value()),
            connection_timeout=self._settings.connection_timeout,
            connection_acquisition_timeout=self._settings.connection_acquisition_timeout,
            max_transaction_retry_time=self._settings.max_transaction_retry_time,
        )
        return self._driver

    async def verify_connectivity(self) -> Neo4jHealth:
        try:
            await self._get_driver().verify_connectivity()
        except AuthError:
            return Neo4jHealth(status="error", reason="authentication")
        except (ConfigurationError, ValueError):
            return Neo4jHealth(status="error", reason="configuration")
        except (DriverError, Neo4jError):
            return Neo4jHealth(status="error", reason="unavailable")
        return Neo4jHealth(status="ok")

    async def close(self) -> None:
        if self._driver is None:
            return
        driver, self._driver = self._driver, None
        await driver.close()

    async def execute_read(self, callback: TransactionCallback[ResultT]) -> ResultT:
        async with self._get_driver().session(database=self._settings.database) as session:
            return await session.execute_read(self._bind_transaction(callback))

    async def execute_write(self, callback: TransactionCallback[ResultT]) -> ResultT:
        async with self._get_driver().session(database=self._settings.database) as session:
            return await session.execute_write(self._bind_transaction(callback))

    def read_only(self) -> Neo4jReadFacade:
        return Neo4jReadFacade(self)

    @property
    def schema_timeout(self) -> float:
        return self._settings.schema_timeout

    def _bind_transaction(
        self, callback: TransactionCallback[ResultT]
    ) -> Callable[[AsyncManagedTransaction], Awaitable[ResultT]]:
        async def execute(transaction: AsyncManagedTransaction) -> ResultT:
            bounded_transaction = Neo4jTransaction(transaction)
            return await callback(bounded_transaction)

        return unit_of_work(timeout=self._settings.transaction_timeout)(execute)


class Neo4jReadFacade:
    """Capability-limited graph access for retrieval consumers."""

    __slots__ = ("__store",)

    def __init__(self, store: Neo4jStore) -> None:
        self.__store = store

    async def verify_connectivity(self) -> Neo4jHealth:
        return await self.__store.verify_connectivity()

    async def execute_read(self, callback: TransactionCallback[ResultT]) -> ResultT:
        return await self.__store.execute_read(callback)
