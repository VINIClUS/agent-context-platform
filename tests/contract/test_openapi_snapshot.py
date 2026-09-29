"""Contract test: the ingestion v1 OpenAPI document is frozen.

``scripts/export-openapi.py`` derives the ingestion-only view (``/v1/ingestion/*``
plus the components it references) from the live application; its SHA-256 must
equal the checked-in ``openapi/agent-context-v1.json``.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from agent_context_sdk import IngestBatchRequestV1, IngestBatchResponseV1

from agent_context_platform.ledger.api import REQUEST_REF_TEMPLATE

pytestmark = pytest.mark.contract

ROOT = Path(__file__).parents[2]
SNAPSHOT = ROOT / "openapi" / "agent-context-v1.json"


def _load_exporter() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "export_openapi", ROOT / "scripts/export-openapi.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


exporter = _load_exporter()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_live_ingestion_openapi_matches_the_frozen_snapshot() -> None:
    live = exporter.render()
    frozen = SNAPSHOT.read_bytes()

    assert _sha256(live) == _sha256(frozen), (
        "The ingestion OpenAPI document no longer matches openapi/agent-context-v1.json. "
        "Ingestion API v1 is immutable: a change needs a new API version, not a new snapshot."
    )


def test_snapshot_is_deterministic_json() -> None:
    frozen = SNAPSHOT.read_bytes()
    document = json.loads(frozen)

    assert frozen.endswith(b"\n") and not frozen.endswith(b"\n\n")
    assert (
        frozen
        == (json.dumps(document, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode()
    )
    assert exporter.render() == exporter.render()


def test_snapshot_freezes_only_the_ingestion_surface() -> None:
    document: dict[str, Any] = json.loads(SNAPSHOT.read_bytes())

    assert list(document["paths"]) == ["/v1/ingestion/batches"]
    assert set(document["paths"]["/v1/ingestion/batches"]) == {"post"}
    assert not any(path.startswith(("/health", "/mcp")) for path in document["paths"])


def test_snapshot_declares_every_frozen_status_with_its_schema() -> None:
    document: dict[str, Any] = json.loads(SNAPSHOT.read_bytes())
    responses = document["paths"]["/v1/ingestion/batches"]["post"]["responses"]
    schemas = document["components"]["schemas"]

    assert set(responses) == {"200", "400", "401", "403", "409", "413", "422", "500", "503"}
    assert {
        "IngestBatchResponseV1",
        "RejectedEventV1",
        "AcceptedEventV1",
        "ErrorResponseV1",
    } <= set(schemas)
    ok = responses["200"]["content"]["application/json"]["schema"]
    assert ok == {"$ref": "#/components/schemas/IngestBatchResponseV1"}
    conflict = responses["409"]["content"]["application/json"]["schema"]
    assert conflict == {"$ref": "#/components/schemas/IngestBatchResponseV1"}
    for status in ("400", "401", "403", "413", "500"):
        schema = responses[status]["content"]["application/json"]["schema"]
        assert schema == {"$ref": "#/components/schemas/ErrorResponseV1"}
    for status in ("422", "503"):
        schema = responses[status]["content"]["application/json"]["schema"]
        refs = {option["$ref"] for option in schema["anyOf"]}
        assert refs == {
            "#/components/schemas/IngestBatchResponseV1",
            "#/components/schemas/ErrorResponseV1",
        }
    assert "Retry-After" in responses["503"]["headers"]
    assert "WWW-Authenticate" in responses["401"]["headers"]


def test_snapshot_documents_bearer_auth_and_success_headers() -> None:
    document: dict[str, Any] = json.loads(SNAPSHOT.read_bytes())
    operation = document["paths"]["/v1/ingestion/batches"]["post"]

    assert operation["security"] == [{"ProducerBearer": []}]
    scheme = document["components"]["securitySchemes"]["ProducerBearer"]
    assert (scheme["type"], scheme["scheme"]) == ("http", "bearer")
    assert "events:ingest" in scheme["description"]
    assert "x-request-id" in operation["responses"]["200"]["headers"]
    assert "201" not in operation["responses"] and "202" not in operation["responses"]


def test_documented_request_schema_is_the_sdk_schema() -> None:
    document: dict[str, Any] = json.loads(SNAPSHOT.read_bytes())
    schemas = document["components"]["schemas"]
    body = document["paths"]["/v1/ingestion/batches"]["post"]["requestBody"]
    sdk = IngestBatchRequestV1.model_json_schema(ref_template=REQUEST_REF_TEMPLATE)
    defs = sdk.pop("$defs")

    assert body["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/IngestBatchRequestV1"
    }
    assert schemas["IngestBatchRequestV1"] == sdk
    assert {name: schemas[name] for name in defs} == defs


def test_check_mode_reports_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert exporter.main(["--check"]) == 0

    drifted = tmp_path / "agent-context-v1.json"
    drifted.write_bytes(SNAPSHOT.read_bytes() + b" ")
    monkeypatch.setattr(exporter, "SNAPSHOT_PATH", drifted)

    assert exporter.main(["--check"]) == 1
    assert "immutable" in capsys.readouterr().err

    assert exporter.main([]) == 0
    assert drifted.read_bytes() == SNAPSHOT.read_bytes()


FIXTURES = sorted((ROOT / "tests" / "fixtures" / "ingestion").glob("[0-9]*.json"))


def test_fixtures_cover_the_required_cases() -> None:
    names = {json.loads(path.read_text())["name"] for path in FIXTURES}

    assert names == {
        "success",
        "duplicate",
        "partial-duplicate",
        "validation-error",
        "redaction-required",
        "payload-too-large",
        "auth-failure",
        "transient-outage",
    }


@pytest.mark.parametrize("path", FIXTURES, ids=lambda path: path.stem)
def test_fixtures_are_self_describing_and_match_the_frozen_contract(path: Path) -> None:
    fixture = json.loads(path.read_text())
    document: dict[str, Any] = json.loads(SNAPSHOT.read_bytes())
    declared = document["paths"]["/v1/ingestion/batches"]["post"]["responses"]
    response = fixture["response"]

    assert set(fixture) == {"name", "description", "setup", "request", "response"}
    assert fixture["description"]
    assert str(response["status"]) in declared
    assert fixture["request"]["path"] == "/v1/ingestion/batches"
    if "batch_id" in response["body"]:
        IngestBatchResponseV1.model_validate(response["body"])
    else:
        assert set(response["body"]) == {"error", "request_id"}
        assert response["body"]["request_id"] == "<request-id>"
    if fixture["name"] != "validation-error":
        IngestBatchRequestV1.model_validate(fixture["request"]["body"])
