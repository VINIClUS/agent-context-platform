from __future__ import annotations

import asyncio
from typing import Any, LiteralString

import pytest
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import AuthError, ConfigurationError, ServiceUnavailable

from agent_context_platform.projection.neo4j import Neo4jStore, Neo4jTransaction
from agent_context_platform.settings import Neo4jSettings

pytestmark = pytest.mark.unit


class ConnectivityDriver:
    def __init__(self, connectivity_error: Exception | None = None) -> None:
        self.verify_calls = 0
        self.close_calls = 0
        self.connectivity_error = connectivity_error

    async def verify_connectivity(self) -> None:
        self.verify_calls += 1
        if self.connectivity_error is not None:
            raise self.connectivity_error

    async def close(self) -> None:
        self.close_calls += 1


def configured_settings() -> Neo4jSettings:
    return Neo4jSettings.model_validate(
        {
            "uri": "neo4j://graph.example.test",
            "username": "neo4j-user",
            "password": "neo4j-secret",
        }
    )


def test_store_constructs_driver_lazily_with_bounded_pool_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = ConnectivityDriver()
    factory_calls: list[tuple[str, dict[str, Any]]] = []

    def driver_factory(uri: str, **config: Any) -> ConnectivityDriver:
        factory_calls.append((uri, config))
        return driver

    monkeypatch.setattr(AsyncGraphDatabase, "driver", driver_factory)

    store = Neo4jStore(configured_settings())
    assert factory_calls == []

    health = asyncio.run(store.verify_connectivity())

    assert health.status == "ok"
    assert health.reason is None
    assert factory_calls == [
        (
            "neo4j://graph.example.test",
            {
                "auth": ("neo4j-user", "neo4j-secret"),
                "connection_timeout": 5.0,
                "connection_acquisition_timeout": 10.0,
                "max_transaction_retry_time": 15.0,
            },
        )
    ]
    assert driver.verify_calls == 1


def test_store_representation_never_exposes_credentials() -> None:
    store = Neo4jStore(configured_settings())

    representation = repr(store)

    assert "neo4j-secret" not in representation
    assert "neo4j-user" not in representation


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (AuthError("invalid credentials"), "authentication"),
        (ConfigurationError("invalid driver setting"), "configuration"),
        (ServiceUnavailable("server offline"), "unavailable"),
    ],
)
def test_health_classifies_driver_failures_without_exposing_details(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    reason: str,
) -> None:
    driver = ConnectivityDriver(error)
    monkeypatch.setattr(AsyncGraphDatabase, "driver", lambda *_args, **_kwargs: driver)

    health = asyncio.run(Neo4jStore(configured_settings()).verify_connectivity())

    assert health.status == "error"
    assert health.reason == reason
    assert str(error) not in repr(health)


def test_health_reports_incomplete_settings_as_configuration_error() -> None:
    health = asyncio.run(Neo4jStore(Neo4jSettings()).verify_connectivity())

    assert health.status == "error"
    assert health.reason == "configuration"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("username", "   "),
        ("password", ""),
        ("database", "\t"),
    ],
)
def test_health_reports_blank_connection_fields_as_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    values = {
        "uri": "neo4j://graph.example.test",
        "username": "neo4j-user",
        "password": "neo4j-secret",
        "database": "neo4j",
        name: value,
    }

    def unexpected_driver_call(*_args: object, **_kwargs: object) -> None:
        pytest.fail("blank connection settings must not reach the Neo4j driver")

    monkeypatch.setattr(AsyncGraphDatabase, "driver", unexpected_driver_call)
    health = asyncio.run(Neo4jStore(Neo4jSettings.model_validate(values)).verify_connectivity())

    assert health.status == "error"
    assert health.reason == "configuration"


def test_async_context_closes_an_initialized_driver_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = ConnectivityDriver()
    monkeypatch.setattr(AsyncGraphDatabase, "driver", lambda *_args, **_kwargs: driver)
    store = Neo4jStore(configured_settings())

    async def use_store() -> None:
        async with store as entered:
            assert entered is store
            await entered.verify_connectivity()
        await store.close()

    asyncio.run(use_store())

    assert driver.close_calls == 1


class EagerResultSource:
    def __init__(self, eager_result: object) -> None:
        self.eager_result = eager_result
        self.eager_calls = 0

    async def to_eager_result(self) -> object:
        self.eager_calls += 1
        return self.eager_result


class ManagedTransaction:
    def __init__(self, result: EagerResultSource) -> None:
        self.result = result
        self.runs: list[tuple[str, dict[str, Any]]] = []

    async def run(self, query: str, parameters: dict[str, Any] | None = None) -> EagerResultSource:
        self.runs.append((query, parameters or {}))
        return self.result


def test_transaction_uses_literal_query_parameters_and_eager_result() -> None:
    eager_result = object()
    result = EagerResultSource(eager_result)
    managed = ManagedTransaction(result)
    transaction = Neo4jTransaction(managed)
    query: LiteralString = "RETURN $value AS value"

    returned = asyncio.run(transaction.run(query, parameters={"value": 7}))

    assert returned is eager_result
    assert result.eager_calls == 1
    [(configured_query, parameters)] = managed.runs
    assert configured_query == "RETURN $value AS value"
    assert parameters == {"value": 7}


class TransactionSession:
    def __init__(self, transaction: ManagedTransaction) -> None:
        self.transaction = transaction
        self.read_calls = 0
        self.write_calls = 0
        self.transaction_timeouts: list[float | None] = []

    async def __aenter__(self) -> TransactionSession:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute_read(self, callback: Any) -> Any:
        self.read_calls += 1
        self.transaction_timeouts.append(getattr(callback, "timeout", None))
        return await callback(self.transaction)

    async def execute_write(self, callback: Any) -> Any:
        self.write_calls += 1
        self.transaction_timeouts.append(getattr(callback, "timeout", None))
        return await callback(self.transaction)


class TransactionDriver(ConnectivityDriver):
    def __init__(self, session: TransactionSession) -> None:
        super().__init__()
        self.transaction_session = session
        self.session_configs: list[dict[str, Any]] = []

    def session(self, **config: Any) -> TransactionSession:
        self.session_configs.append(config)
        return self.transaction_session


def test_execute_read_wraps_transaction_and_uses_configured_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eager_result = object()
    managed = ManagedTransaction(EagerResultSource(eager_result))
    session = TransactionSession(managed)
    driver = TransactionDriver(session)
    monkeypatch.setattr(AsyncGraphDatabase, "driver", lambda *_args, **_kwargs: driver)

    async def read(transaction: Neo4jTransaction) -> object:
        return await transaction.run("RETURN $value", parameters={"value": 11})

    returned = asyncio.run(Neo4jStore(configured_settings()).execute_read(read))

    assert returned is eager_result
    assert session.read_calls == 1
    assert session.write_calls == 0
    assert session.transaction_timeouts == [10.0]
    assert driver.session_configs == [{"database": "neo4j"}]


def test_execute_write_uses_managed_write_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = TransactionSession(ManagedTransaction(EagerResultSource(object())))
    driver = TransactionDriver(session)
    monkeypatch.setattr(AsyncGraphDatabase, "driver", lambda *_args, **_kwargs: driver)

    async def write(_transaction: Neo4jTransaction) -> str:
        return "written"

    returned = asyncio.run(Neo4jStore(configured_settings()).execute_write(write))

    assert returned == "written"
    assert session.read_calls == 0
    assert session.write_calls == 1


def test_read_facade_exposes_reads_but_not_writes_or_driver() -> None:
    facade = Neo4jStore(configured_settings()).read_only()

    assert callable(facade.execute_read)
    assert callable(facade.verify_connectivity)
    assert not hasattr(facade, "execute_write")
    assert not hasattr(facade, "driver")
    assert not hasattr(facade, "_driver")
    assert not hasattr(facade, "_store")
