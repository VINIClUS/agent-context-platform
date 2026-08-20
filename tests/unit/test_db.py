from __future__ import annotations

import socket
from typing import NoReturn

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_context_platform.db import create_engine, session_factory
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit


def database_settings() -> Settings:
    return Settings(
        environment="test",
        postgresql={"dsn": "postgresql+psycopg://platform:secret@postgres/agent_context"},
    )


def test_create_engine_is_lazy_and_uses_async_psycopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def deny_connect(_socket: socket.socket, address: object) -> NoReturn:
        raise AssertionError(f"unexpected network connection to {address!r}")

    monkeypatch.setattr(socket.socket, "connect", deny_connect)

    engine = create_engine(database_settings())

    assert isinstance(engine, AsyncEngine)
    assert engine.dialect.driver == "psycopg"
    assert engine.sync_engine.pool._pre_ping is True


def test_create_engine_requires_a_postgresql_dsn() -> None:
    with pytest.raises(ValueError, match=r"postgresql\.dsn"):
        create_engine(Settings(environment="test"))


def test_session_factory_binds_engine_without_expiring_on_commit() -> None:
    engine = create_engine(database_settings())

    factory = session_factory(engine)

    assert factory.kw["bind"] is engine
    assert factory.kw["expire_on_commit"] is False
    assert factory.kw["autoflush"] is True
