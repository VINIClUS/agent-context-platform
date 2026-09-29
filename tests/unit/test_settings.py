from __future__ import annotations

import pytest
from pydantic import ValidationError

from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit


def test_settings_loads_nested_environment_sections(
    complete_production_environment: None,
) -> None:
    settings = Settings()

    assert settings.environment == "production"
    assert settings.postgresql.dsn is not None
    assert settings.postgresql.dsn.get_secret_value().unicode_string() == (
        "postgresql+psycopg://platform:postgres-secret@postgres/agent_context"
    )
    assert settings.neo4j.uri is not None
    assert str(settings.neo4j.uri) == "neo4j+s://graph.example.test"
    assert settings.neo4j.username == "neo4j-user"
    assert settings.neo4j.password is not None
    assert settings.neo4j.password.get_secret_value() == "neo4j-secret"
    assert settings.neo4j.database == "neo4j"
    assert settings.s3.endpoint_url is not None
    assert str(settings.s3.endpoint_url) == "https://objects.example.test/"
    assert settings.s3.region_name == "garage"
    assert settings.s3.bucket_name == "agent-context-content"
    assert settings.s3.addressing_style == "path"
    assert settings.mcp.stateless_http is True


def test_incomplete_production_settings_report_only_missing_names() -> None:
    with pytest.raises(ValidationError) as error:
        Settings(environment="production")

    message = str(error.value)
    for missing_name in (
        "postgresql.dsn",
        "neo4j.uri",
        "neo4j.username",
        "neo4j.password",
        "s3.endpoint_url",
        "s3.access_key_id",
        "s3.secret_access_key",
        "mcp.bearer_token_verifier",
    ):
        assert missing_name in message
    assert "input_value" not in message
    assert "input_type" not in message


def test_complete_production_settings_are_accepted(
    complete_production_environment: None,
) -> None:
    Settings()


def test_production_treats_blank_credentials_as_missing() -> None:
    with pytest.raises(ValidationError) as error:
        Settings(
            environment="production",
            postgresql={"dsn": "postgresql+psycopg://platform:secret@postgres/agent_context"},
            neo4j={
                "uri": "neo4j://graph.example.test",
                "username": "   ",
                "password": "",
            },
            s3={
                "endpoint_url": "https://objects.example.test",
                "access_key_id": "",
                "secret_access_key": "   ",
            },
            mcp={"bearer_token_verifier": ""},
        )

    message = str(error.value)
    for missing_name in (
        "neo4j.username",
        "neo4j.password",
        "s3.access_key_id",
        "s3.secret_access_key",
        "mcp.bearer_token_verifier",
    ):
        assert missing_name in message
    assert "input_value" not in message


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://platform:secret@postgres/agent_context",
        "postgresql+asyncpg://platform:secret@postgres/agent_context",
        "postgresql+psycopg://platform:secret@postgres",
    ],
)
def test_postgresql_rejects_wrong_driver_or_missing_database(dsn: str) -> None:
    with pytest.raises(ValidationError):
        Settings(postgresql={"dsn": dsn})


@pytest.mark.parametrize(
    "uri",
    [
        "https://graph.example.test",
        "neo4j+invalid://graph.example.test",
    ],
)
def test_neo4j_rejects_unsupported_uri_schemes(uri: str) -> None:
    with pytest.raises(ValidationError):
        Settings(neo4j={"uri": uri})


def test_neo4j_rejects_credentials_embedded_in_uri_without_leaking_them() -> None:
    uri_secret = "uri-secret"

    with pytest.raises(ValidationError) as error:
        Settings(neo4j={"uri": f"neo4j://alice:{uri_secret}@graph.example.test"})

    assert uri_secret not in str(error.value)


def test_neo4j_timeouts_are_bounded_by_default() -> None:
    settings = Settings().neo4j

    assert settings.connection_timeout == 5.0
    assert settings.connection_acquisition_timeout == 10.0
    assert settings.max_transaction_retry_time == 15.0
    assert settings.transaction_timeout == 10.0
    assert settings.schema_timeout == 30.0


@pytest.mark.parametrize(
    "name",
    [
        "connection_timeout",
        "connection_acquisition_timeout",
        "max_transaction_retry_time",
        "transaction_timeout",
        "schema_timeout",
    ],
)
def test_neo4j_rejects_non_positive_timeouts(name: str) -> None:
    with pytest.raises(ValidationError):
        Settings(neo4j={name: 0.0})


@pytest.mark.parametrize(
    "name",
    [
        "connection_timeout",
        "connection_acquisition_timeout",
        "max_transaction_retry_time",
        "transaction_timeout",
        "schema_timeout",
    ],
)
def test_neo4j_rejects_unbounded_infinite_timeouts(name: str) -> None:
    with pytest.raises(ValidationError):
        Settings(neo4j={name: float("inf")})


def test_neo4j_acquisition_timeout_must_exceed_connection_timeout() -> None:
    with pytest.raises(ValidationError) as error:
        Settings(
            neo4j={
                "connection_timeout": 5.0,
                "connection_acquisition_timeout": 5.0,
            }
        )

    assert "connection_acquisition_timeout must exceed connection_timeout" in str(error.value)


def test_secrets_never_appear_in_repr_or_validation_errors() -> None:
    secrets = (
        "postgres-secret",
        "neo4j-secret",
        "garage-access-key",
        "garage-secret-key",
        "mcp-secret-verifier",
    )
    settings = Settings(
        postgresql={"dsn": "postgresql+psycopg://platform:postgres-secret@postgres/agent_context"},
        neo4j={
            "uri": "neo4j://graph.example.test",
            "username": "neo4j-user",
            "password": "neo4j-secret",
        },
        s3={
            "endpoint_url": "https://objects.example.test",
            "access_key_id": "garage-access-key",
            "secret_access_key": "garage-secret-key",
        },
        mcp={"bearer_token_verifier": "mcp-secret-verifier"},
    )

    representation = repr(settings)
    assert all(secret not in representation for secret in secrets)

    with pytest.raises(ValidationError) as error:
        Settings(
            environment="production",
            postgresql={
                "dsn": "postgresql+psycopg://platform:postgres-secret@postgres/agent_context"
            },
            neo4j={
                "uri": "neo4j://graph.example.test",
                "password": "neo4j-secret",
            },
            s3={
                "endpoint_url": "https://objects.example.test",
                "access_key_id": "garage-access-key",
                "secret_access_key": "garage-secret-key",
            },
            mcp={"bearer_token_verifier": "mcp-secret-verifier"},
        )

    error_message = str(error.value)
    assert all(secret not in error_message for secret in secrets)


def test_installed_sdk_matches_the_pinned_public_version() -> None:
    import tomllib
    from pathlib import Path

    from agent_context_sdk import __version__

    pyproject = tomllib.loads(
        (Path(__file__).resolve().parents[2] / "pyproject.toml").read_text(encoding="utf-8")
    )
    pinned = pyproject["tool"]["agent-context"]["sdk"]

    assert __version__ == pinned["version"]
    assert f"agent-context-sdk=={pinned['version']}" in pyproject["project"]["dependencies"]


def test_mcp_transport_guard_defaults_are_loopback_only_and_bounded() -> None:
    settings = Settings(environment="test")

    assert settings.mcp.allowed_hosts == ("127.0.0.1:*", "localhost:*", "[::1]:*")
    assert settings.mcp.allowed_origins == (
        "http://127.0.0.1:*",
        "http://localhost:*",
        "http://[::1]:*",
    )
    assert settings.mcp.max_request_body_bytes == 1_048_576


def test_mcp_guard_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_MCP__ALLOWED_HOSTS", '["mcp.example.test"]')
    monkeypatch.setenv("AGENT_CONTEXT_MCP__ALLOWED_ORIGINS", '["https://app.example.test"]')
    monkeypatch.setenv("AGENT_CONTEXT_MCP__MAX_REQUEST_BODY_BYTES", "4096")

    settings = Settings()

    assert settings.mcp.allowed_hosts == ("mcp.example.test",)
    assert settings.mcp.allowed_origins == ("https://app.example.test",)
    assert settings.mcp.max_request_body_bytes == 4096


@pytest.mark.parametrize("value", [0, -1])
def test_mcp_rejects_non_positive_body_limit(value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(mcp={"max_request_body_bytes": value})


def test_ingestion_settings_default_to_bounded_body_and_argon2_cost() -> None:
    ingestion = Settings().ingestion

    assert ingestion.max_request_body_bytes == 33_554_432
    assert ingestion.argon2_time_cost >= 2
    assert ingestion.argon2_memory_cost_kib >= 19_456
    assert ingestion.argon2_parallelism >= 1
    assert ingestion.argon2_max_concurrency >= 1


def test_ingestion_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_INGESTION__MAX_REQUEST_BODY_BYTES", "1024")
    monkeypatch.setenv("AGENT_CONTEXT_INGESTION__ARGON2_MAX_CONCURRENCY", "2")

    ingestion = Settings().ingestion

    assert ingestion.max_request_body_bytes == 1024
    assert ingestion.argon2_max_concurrency == 2


@pytest.mark.parametrize(
    "field",
    [
        "max_request_body_bytes",
        "argon2_time_cost",
        "argon2_memory_cost_kib",
        "argon2_parallelism",
        "argon2_max_concurrency",
    ],
)
def test_ingestion_settings_reject_non_positive_values(field: str) -> None:
    from agent_context_platform.settings import IngestionSettings

    with pytest.raises(ValidationError):
        IngestionSettings(**{field: 0})
