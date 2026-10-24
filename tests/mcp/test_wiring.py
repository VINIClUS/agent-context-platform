"""How the app wires the memory tools' composer (PLATFORM-046), without services."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient
from mcp_helpers import TOKEN, AuthHarness, mcp_headers, rpc_body

from agent_context_platform import app as app_module
from agent_context_platform.settings import Settings

pytestmark = pytest.mark.unit

DSN = "postgresql+psycopg://api:secret@db.example.test/agent_context"
OTHER_DSN = "postgresql+psycopg://other:secret@db.example.test/agent_context"
NEO4J = {"uri": "bolt://graph.example.test:7687", "username": "neo4j", "password": "pw"}
S3 = {"endpoint_url": "http://s3.example.test:3900"}


def settings(**sections: Any) -> Settings:
    return Settings(environment="test", **sections)


COMPLETE = {"postgresql": {"dsn": DSN}, "neo4j": NEO4J, "s3": S3}


@pytest.mark.parametrize(
    "sections",
    [
        {"neo4j": NEO4J, "s3": S3},  # no PostgreSQL
        {"postgresql": {"dsn": DSN}, "s3": S3},  # no Neo4j
        {"postgresql": {"dsn": DSN}, "neo4j": NEO4J},  # no S3
        {"postgresql": {"dsn": DSN}, "neo4j": {**NEO4J, "password": " "}, "s3": S3},
        {"postgresql": {"dsn": DSN}, "neo4j": {**NEO4J, "username": " "}, "s3": S3},
        {"postgresql": {"dsn": DSN}, "neo4j": {"uri": NEO4J["uri"]}, "s3": S3},
    ],
)
def test_a_partial_configuration_builds_no_composer(sections: dict[str, Any]) -> None:
    assert app_module._composer_configured(settings(**sections)) is False


def test_a_complete_configuration_builds_one_even_with_only_an_api_dsn() -> None:
    assert app_module._composer_configured(settings(**COMPLETE)) is True
    only_api = {**COMPLETE, "postgresql": {"api_dsn": DSN}}
    assert app_module._composer_configured(settings(**only_api)) is True


def test_a_partial_configuration_answers_unavailable_without_touching_any_store() -> None:
    harness = AuthHarness()
    harness.add()
    partial = settings(postgresql={"dsn": DSN}, neo4j=NEO4J)  # no S3
    with TestClient(
        app_module.create_app(partial, mcp_auth=harness.runtime()),
        base_url="http://127.0.0.1:8000",
    ) as client:
        response = client.post(
            "/mcp",
            headers=mcp_headers(
                "tools/call",
                **{"mcp-name": "repository_overview", "authorization": f"Bearer {TOKEN}"},
            ),
            json=rpc_body(
                "tools/call",
                {
                    "name": "repository_overview",
                    "arguments": {
                        "repository_id": "r",
                        "commit_id": "a" * 40,
                        "budget": {"max_items": 1, "max_bytes": 1000, "max_tokens": 1000},
                    },
                },
            ),
        )

    assert '\\"code\\": \\"context_unavailable\\"' in response.text


def test_the_composer_engine_is_its_own_bounded_pool_on_the_api_dsn() -> None:
    engine = app_module._composer_engine(
        settings(**{**COMPLETE, "postgresql": {"dsn": OTHER_DSN, "api_dsn": DSN}})
    )
    try:
        assert engine.url.username == "api"  # api_dsn wins over dsn
        assert engine.pool.size() == app_module.COMPOSER_POOL_SIZE  # type: ignore[attr-defined]
    finally:
        asyncio.run(engine.dispose())
    fallback = app_module._composer_engine(settings(**COMPLETE))
    try:
        assert fallback.url.username == "api"
    finally:
        asyncio.run(fallback.dispose())


def test_the_composer_neo4j_bounds_are_shorter_than_the_tool_deadline() -> None:
    bounded = app_module._composer_neo4j(settings(**COMPLETE))

    assert bounded.connection_timeout <= app_module.COMPOSER_NEO4J_CONNECT_SECONDS
    assert bounded.max_transaction_retry_time <= app_module.COMPOSER_NEO4J_RETRY_SECONDS
    assert bounded.connection_acquisition_timeout > bounded.connection_timeout
    assert bounded.uri is not None and bounded.password is not None  # the rest is unchanged


def test_a_start_up_failure_closes_what_was_already_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[str] = []

    class Graph:
        async def close(self) -> None:
            closed.append("graph")

    def temporal_fails(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("temporal driver failed")

    monkeypatch.setattr(
        app_module.GraphTraversalService, "from_settings", staticmethod(lambda *_a, **_k: Graph())
    )
    monkeypatch.setattr(app_module.TemporalService, "from_settings", staticmethod(temporal_fails))

    with pytest.raises(RuntimeError, match="temporal driver failed"):
        asyncio.run(app_module._build_composer(settings(**COMPLETE)))

    assert closed == ["graph"]


def test_without_a_model_the_composer_skips_the_vector_leg(monkeypatch: pytest.MonkeyPatch) -> None:
    built: dict[str, Any] = {}

    class Recorder:
        def __init__(self, **options: Any) -> None:
            built.update(options)

    monkeypatch.setattr(app_module, "ContextComposer", Recorder)

    _composer, closers = asyncio.run(app_module._build_composer(settings(**COMPLETE)))
    asyncio.run(app_module._close_all(closers))

    assert built["vector_search"] is False
