from __future__ import annotations

import pytest

PRODUCTION_ENV = {
    "AGENT_CONTEXT_ENVIRONMENT": "production",
    "AGENT_CONTEXT_POSTGRESQL__DSN": (
        "postgresql+psycopg://platform:postgres-secret@postgres/agent_context"
    ),
    "AGENT_CONTEXT_NEO4J__URI": "neo4j+s://graph.example.test",
    "AGENT_CONTEXT_NEO4J__USERNAME": "neo4j-user",
    "AGENT_CONTEXT_NEO4J__PASSWORD": "neo4j-secret",
    "AGENT_CONTEXT_S3__ENDPOINT_URL": "https://objects.example.test",
    "AGENT_CONTEXT_S3__ACCESS_KEY_ID": "garage-access-key",
    "AGENT_CONTEXT_S3__SECRET_ACCESS_KEY": "garage-secret-key",
    "AGENT_CONTEXT_MCP__BEARER_TOKEN_VERIFIER": "mcp-secret-verifier",
}


@pytest.fixture
def complete_production_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in PRODUCTION_ENV.items():
        monkeypatch.setenv(name, value)
