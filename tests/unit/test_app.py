from __future__ import annotations

import socket
from typing import NoReturn

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from agent_context_platform.app import create_app
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit


def test_health_live_reports_process_is_alive_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def deny_connect(_socket: socket.socket, address: object) -> NoReturn:
        raise AssertionError(f"unexpected network connection to {address!r}")

    monkeypatch.setattr(socket.socket, "connect", deny_connect)
    application = create_app(Settings(environment="test"))

    with TestClient(application) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_live_is_published_in_openapi() -> None:
    application = create_app(Settings(environment="test"))

    schema = application.openapi()

    assert "/health/live" in schema["paths"]


def test_create_app_preserves_injected_settings() -> None:
    settings = Settings(environment="test")

    application = create_app(settings)

    assert application.state.settings is settings


def test_create_app_loads_default_settings_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_ENVIRONMENT", "production")

    application = create_app()

    assert application.state.settings.environment == "production"


def test_settings_rejects_invalid_environment() -> None:
    with pytest.raises(ValidationError):
        Settings(environment="staging")  # type: ignore[arg-type]


def test_settings_rejects_unknown_initializers() -> None:
    with pytest.raises(ValidationError):
        Settings(unknown_setting="value")  # type: ignore[call-arg]
