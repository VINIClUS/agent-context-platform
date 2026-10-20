from __future__ import annotations

import socket
from pathlib import Path
from typing import NoReturn

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from agent_context_platform import app as app_module
from agent_context_platform.app import create_app
from agent_context_platform.operations import migrate
from agent_context_platform.operations.readiness import (
    ComponentState,
    ReadinessProbe,
    ReadinessReport,
)
from agent_context_platform.projection.neo4j import Neo4jStore
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
    complete_production_environment: None,
) -> None:
    application = create_app()

    assert application.state.settings.environment == "production"


def test_settings_rejects_invalid_environment() -> None:
    with pytest.raises(ValidationError):
        Settings(environment="staging")  # type: ignore[arg-type]


def test_settings_rejects_unknown_initializers() -> None:
    with pytest.raises(ValidationError):
        Settings(unknown_setting="value")  # type: ignore[call-arg]


class StaticProbe(ReadinessProbe):
    def __init__(self, components: dict[str, ComponentState]) -> None:
        super().__init__({})
        self.components = components
        self.calls = 0

    async def probe(self) -> ReadinessReport:
        self.calls += 1
        return ReadinessReport(self.components)


def test_health_ready_answers_200_when_every_component_is_ok() -> None:
    probe = StaticProbe({"postgresql": "ok", "schema": "ok", "neo4j": "ok", "s3": "ok"})
    application = create_app(Settings(environment="test"), readiness=probe)

    with TestClient(application) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert response.headers["cache-control"] == "no-store"
    assert probe.calls == 1


def test_health_ready_answers_503_with_component_states_only() -> None:
    probe = StaticProbe({"postgresql": "ok", "schema": "mismatch", "neo4j": "timeout", "s3": "ok"})
    application = create_app(Settings(environment="test"), readiness=probe)

    with TestClient(application) as client:
        first = client.get("/health/ready")
        second = client.get("/health/ready")

    assert first.status_code == 503
    assert first.json() == {
        "status": "not_ready",
        "components": {"neo4j": "timeout", "postgresql": "ok", "s3": "ok", "schema": "mismatch"},
    }
    assert first.headers["cache-control"] == "no-store"
    assert probe.calls == 2  # never cached
    assert second.status_code == 503


def test_health_ready_is_not_ready_before_the_application_starts() -> None:
    application = create_app(Settings(environment="test"))

    response = TestClient(application).get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "components": {}}


def test_health_ready_without_dependencies_is_unconfigured_and_opens_no_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def deny_connect(_socket: socket.socket, address: object) -> NoReturn:
        raise AssertionError(f"unexpected network connection to {address!r}")

    monkeypatch.setattr(socket.socket, "connect", deny_connect)
    application = create_app(Settings(environment="test"))

    with TestClient(application) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "components": {
            "neo4j": "unconfigured",
            "postgresql": "unconfigured",
            "s3": "unconfigured",
            "schema": "unconfigured",
        },
    }


def test_health_ready_wires_the_shipped_head_and_closes_its_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    closed: list[object] = []

    def fake_build(
        settings: Settings, engine: object, store: object, head: str | None
    ) -> ReadinessProbe:
        captured.update(engine=engine, store=store, head=head)
        return StaticProbe({"x": "ok"})

    async def fake_close(self: Neo4jStore) -> None:
        closed.append(self)

    monkeypatch.setattr(app_module, "build_probe", fake_build)
    monkeypatch.setattr(Neo4jStore, "close", fake_close)
    settings = Settings(
        environment="test",
        postgresql={"dsn": "postgresql+psycopg://u:p@db.example.test/agent"},
        neo4j={"uri": "bolt://graph.example.test:7687", "username": "neo4j", "password": "pw"},
    )

    with TestClient(create_app(settings)) as client:
        assert client.get("/health/ready").status_code == 200

    assert captured["head"] == migrate.image_head(migrate.alembic_config())
    assert captured["engine"] is not None
    assert isinstance(captured["store"], Neo4jStore)
    assert closed == [captured["store"]]


def test_the_shipped_head_is_none_without_the_alembic_scripts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(migrate.ALEMBIC_CONFIG_ENV, str(tmp_path / "missing.ini"))

    assert app_module._shipped_head() is None


def test_health_ready_is_published_in_openapi() -> None:
    schema = create_app(Settings(environment="test")).openapi()

    assert "503" in schema["paths"]["/health/ready"]["get"]["responses"]
