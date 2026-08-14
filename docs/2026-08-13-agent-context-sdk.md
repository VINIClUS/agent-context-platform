# Agent Context SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish `agent-context-sdk` v0.1.0 as the only canonical source for identifiers, events, redaction, ingestion/query/MCP contracts and fixtures.

**Architecture:** The SDK is a dependency-free domain boundary except for Pydantic and focused canonicalization/redaction libraries. Codex and platform code exchange versioned Pydantic models serialized as canonical JSON. Clients produce event drafts; the platform adds stream sequence and integrity to stored events.

**Tech Stack:** Python 3.12+, uv, Pydantic v2, uuid6, RFC 8785/JCS, pytest, Hypothesis, Ruff, mypy and JSON Schema.

## Global Constraints

- Package import name is `agent_context_sdk`; distribution name is `agent-context-sdk`.
- Public models end with a major-version suffix such as `EventDraftV1`.
- Models use `extra="forbid"` and UTC-aware timestamps.
- UUIDv7 is used for generated event/entity IDs.
- RFC 8785 canonical JSON and SHA-256 define stable integrity bytes.
- Redaction runs before any caller persists or transmits content.
- Original secret values and unsalted secret hashes never appear in a report.
- Schema changes after v0.1 require compatibility tests and a versioned model/upcaster.
- Root exports, generated schemas and lockfile have one integration owner in SDK-017.
- Execute by dependency, not numeric display order: SDK-012 completes before SDK-011 because stored envelopes consume resolved content contracts.

---

## File map

```text
agent-context-sdk/
├── pyproject.toml
├── uv.lock
├── src/agent_context_sdk/
│   ├── __init__.py
│   ├── canonical.py
│   ├── ids.py
│   ├── registry.py
│   ├── schema_export.py
│   ├── content/
│   │   └── models.py
│   ├── contracts/
│   │   ├── ingestion.py
│   │   ├── mcp.py
│   │   └── query.py
│   ├── events/
│   │   ├── envelope.py
│   │   ├── agent.py
│   │   ├── code.py
│   │   ├── git.py
│   │   ├── knowledge.py
│   │   └── quality.py
│   └── redaction/
│       ├── detectors.py
│       ├── engine.py
│       ├── models.py
│       └── policy.py
├── tests/
│   ├── fixtures/
│   ├── golden/
│   ├── test_canonical.py
│   ├── test_ids.py
│   ├── test_registry.py
│   ├── contracts/
│   ├── events/
│   └── redaction/
└── schemas/v1/
```

## Task SDK-001: Package and quality scaffold

**Files:**

- Create: `pyproject.toml`
- Create: `src/agent_context_sdk/__init__.py`
- Create: `tests/test_package.py`
- Create: `.github/workflows/quality.yml`
- Create: `README.md`

**Interfaces:**

- Produces: importable `agent_context_sdk` package and `python -m pytest` test target.
- Consumes: none.

- [ ] **Step 1: Write the package smoke test**

```python
def test_package_version_is_public() -> None:
    import agent_context_sdk

    assert agent_context_sdk.__version__ == "0.1.0"
```

- [ ] **Step 2: Run the test and observe the missing package**

Run: `uv run pytest tests/test_package.py -q`

Expected: collection fails because `agent_context_sdk` is not importable.

- [ ] **Step 3: Configure the project and minimal package**

Use Python `>=3.12,<3.15`; add runtime dependencies `pydantic>=2.11,<3`, `uuid6>=2024.7.10,<2027`, and an RFC 8785 implementation. Resolve exact versions into `uv.lock`. Add dev dependencies pytest, pytest-cov, Hypothesis, Ruff and mypy. Configure Ruff line length 100, strict mypy for `src`, and pytest coverage floor 90%.

```python
__version__ = "0.1.0"
```

- [ ] **Step 4: Run all scaffold gates**

Run:

```bash
uv lock
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -q
```

Expected: every command exits zero.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src tests .github README.md
git commit -m "chore: scaffold canonical context SDK"
```

## Task SDK-010: Typed IDs and canonical JSON

**Files:**

- Create: `src/agent_context_sdk/ids.py`
- Create: `src/agent_context_sdk/canonical.py`
- Create: `tests/test_ids.py`
- Create: `tests/test_canonical.py`

**Interfaces:**

- Produces: `new_uuid7() -> UUID`, `new_prefixed_id(prefix: IdPrefix) -> str`, `canonical_json_bytes(value: JsonValue) -> bytes`, `sha256_hex(data: bytes) -> str`.
- Consumes: no domain models.

- [ ] **Step 1: Write ordering and canonicalization tests**

```python
from agent_context_sdk.canonical import canonical_json_bytes, sha256_hex
from agent_context_sdk.ids import new_prefixed_id, new_uuid7


def test_uuid7_is_monotonic_for_sequential_calls() -> None:
    ids = [new_uuid7() for _ in range(20)]
    assert ids == sorted(ids)
    assert {item.version for item in ids} == {7}


def test_canonical_json_is_key_order_independent() -> None:
    left = canonical_json_bytes({"b": 2, "a": 1})
    right = canonical_json_bytes({"a": 1, "b": 2})
    assert left == right == b'{"a":1,"b":2}'
    assert sha256_hex(left) == sha256_hex(right)


def test_prefixed_id_contains_expected_namespace() -> None:
    assert new_prefixed_id("repo").startswith("repo_")
```

- [ ] **Step 2: Verify failure**

Run: `uv run pytest tests/test_ids.py tests/test_canonical.py -q`

Expected: imports fail because modules do not exist.

- [ ] **Step 3: Implement exact public functions**

```python
from typing import Literal, TypeAlias
from uuid import UUID

from uuid6 import uuid7

IdPrefix: TypeAlias = Literal["ws", "prj", "repo", "co", "snap", "assert", "query"]


def new_uuid7() -> UUID:
    return uuid7()


def new_prefixed_id(prefix: IdPrefix) -> str:
    return f"{prefix}_{new_uuid7().hex}"
```

`canonical_json_bytes` must reject NaN/infinity and non-JSON values instead of coercing them. It must normalize Pydantic models through `model_dump(mode="json", exclude_none=True)` before JCS encoding.

- [ ] **Step 4: Add property tests**

Use Hypothesis JSON strategies to prove dictionary insertion order does not change bytes and round-tripping canonical output preserves the JSON value.

- [ ] **Step 5: Run targeted gates and commit**

```bash
uv run pytest tests/test_ids.py tests/test_canonical.py -q
uv run mypy src/agent_context_sdk/ids.py src/agent_context_sdk/canonical.py
git add src/agent_context_sdk/ids.py src/agent_context_sdk/canonical.py tests
git commit -m "feat: add typed IDs and canonical JSON"
```

## Task SDK-011: Event envelope and integrity

**Files:**

- Create: `src/agent_context_sdk/events/__init__.py`
- Create: `src/agent_context_sdk/events/envelope.py`
- Create: `tests/events/test_envelope.py`
- Create: `tests/events/test_integrity.py`

**Interfaces:**

- Produces: `ProducerV1`, `EventContextV1`, `TraceContextV1`, `EventDraftV1`, `EventIntegrityV1`, `StoredEventV1`, `seal_event(draft, content_refs, sequence, previous_hash) -> StoredEventV1`, `verify_event(event) -> bool`.
- Consumes: ID/canonicalization helpers from SDK-010 and `ContentClaimV1`/`ContentRefV1` from SDK-012.

- [ ] **Step 1: Write envelope validation tests**

```python
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from agent_context_sdk.events.envelope import EventDraftV1, ProducerV1


def test_event_requires_utc_aware_times() -> None:
    with pytest.raises(ValidationError):
        EventDraftV1(
            event_type="agent.session.started",
            stream_id="session:thr_1",
            occurred_at=datetime(2026, 8, 13, 10, 0),
            observed_at=datetime.now(UTC),
            producer=ProducerV1(producer_id="codex:host", name="adapter", version="0.1.0"),
            payload={},
            idempotency_key="session:thr_1:start",
        )
```

- [ ] **Step 2: Write the deterministic chain test**

```python
def test_seal_and_verify_event_chain(event_draft: EventDraftV1) -> None:
    first = seal_event(event_draft, content_refs=(), sequence=1, previous_hash=None)
    second = seal_event(
        event_draft.model_copy(update={"event_id": new_uuid7()}),
        content_refs=(),
        sequence=2,
        previous_hash=first.integrity.event_sha256,
    )

    assert verify_event(first)
    assert verify_event(second)
    assert second.integrity.previous_event_sha256 == first.integrity.event_sha256
```

- [ ] **Step 3: Run tests and verify missing symbols**

Run: `uv run pytest tests/events/test_envelope.py tests/events/test_integrity.py -q`

Expected: tests fail on missing models/functions.

- [ ] **Step 4: Implement strict models and sealing**

All models use `ConfigDict(extra="forbid", frozen=True)`. `EventDraftV1` includes event ID, event type, schema version `1.0.0`, stream ID, occurred/observed timestamps, producer, context, trace, payload, content claims, event-level redaction summary and idempotency key. `StoredEventV1` replaces claims with server-resolved `ContentRefV1` values and adds positive `stream_sequence` plus integrity. `seal_event` rejects a missing, duplicate or digest-mismatched resolution. Hashing excludes only `integrity.event_sha256`, includes the previous hash, and hashes the entire canonical stored envelope.

- [ ] **Step 5: Add tamper tests**

Assert verification fails after changing payload, sequence, stream ID or previous hash. Assert a sequence of zero and empty idempotency key are rejected.

- [ ] **Step 6: Run and commit**

```bash
uv run pytest tests/events -q
uv run mypy src/agent_context_sdk/events
git add src/agent_context_sdk/events tests/events
git commit -m "feat: define canonical event envelope"
```

## Task SDK-012: Content and redaction report contracts

**Files:**

- Create: `src/agent_context_sdk/content/__init__.py`
- Create: `src/agent_context_sdk/content/models.py`
- Create: `src/agent_context_sdk/redaction/models.py`
- Create: `tests/content/test_models.py`
- Create: `tests/redaction/test_models.py`

**Interfaces:**

- Produces: `ContentDisposition`, `ContentStorage`, `ContentClaimV1`, `ContentRefV1`, `RedactionFindingSummaryV1`, `RedactionReportV1`, `MetadataOnlyReason`.
- Consumes: ID and canonical model conventions from SDK-010.

- [ ] **Step 1: Write strict content-reference tests**

```python
def test_content_ref_requires_matching_sha_shape() -> None:
    ref = ContentRefV1(
        content_id="content_1",
        content_sha256="a" * 64,
        media_type="application/json",
        disposition="sanitized",
        storage="object",
        encoding="zstd",
        uncompressed_bytes=70000,
        object_key="sha256/aa/aa/" + "a" * 64 + ".zst",
    )
    assert ref.content_sha256 in ref.object_key
```

Also assert raw secret value and secret hash fields are rejected as extras in `RedactionReportV1`.

- [ ] **Step 2: Run tests to see missing contracts**

Run: `uv run pytest tests/content tests/redaction/test_models.py -q`

Expected: imports fail.

- [ ] **Step 3: Implement enums and frozen Pydantic models**

Use dispositions `sanitized`, `metadata_only`, `dropped_redaction_failure`, `purged`. `ContentClaimV1` carries a producer content ID, SHA-256, media type and uncompressed byte count. `ContentRefV1` carries the same claim plus disposition and exactly one storage form: an inline identifier, or a verified object key plus encoding. Reports include policy version, detector versions, finding counts and optional HMAC correlation labels only.

- [ ] **Step 4: Verify schemas and commit**

```bash
uv run pytest tests/content tests/redaction/test_models.py -q
uv run mypy src/agent_context_sdk/content src/agent_context_sdk/redaction/models.py
git add src/agent_context_sdk/content src/agent_context_sdk/redaction/models.py tests
git commit -m "feat: add sanitized content contracts"
```

## Task SDK-013: Agent event payloads

**Files:**

- Create: `src/agent_context_sdk/events/agent.py`
- Create: `tests/events/test_agent_payloads.py`
- Create: `tests/fixtures/agent/session.json`
- Create: `tests/fixtures/agent/tool_call.json`

**Interfaces:**

- Produces: `SessionStartedV1`, `SessionEndedV1`, `TurnStartedV1`, `TurnCompletedV1`, `SubagentStartedV1`, `SubagentStoppedV1`, `ToolCallStartedV1`, `ToolCallCompletedV1`, `PermissionDecisionV1`, `CompactionObservedV1`.
- Consumes: envelope/context/content models from SDK-011/012.

- [ ] **Step 1: Add representative fixture-validation tests**

```python
def test_tool_call_fixture_preserves_public_ids(load_fixture) -> None:
    payload = ToolCallCompletedV1.model_validate(load_fixture("agent/tool_call.json"))
    assert payload.tool_call_id == "call_789"
    assert payload.tool_name == "Bash"
    assert payload.success is True
```

- [ ] **Step 2: Run and confirm missing payload types**

Run: `uv run pytest tests/events/test_agent_payloads.py -q`

- [ ] **Step 3: Implement payloads without transport-specific transcript parsing**

Tool payloads contain native IDs, tool name, sanitized input/output references, outcome, duration and public error class. Session/turn payloads contain source, model, permission/sandbox mode and public messages where available. No type contains `transcript_path` content or hidden reasoning.

- [ ] **Step 4: Validate all agent fixtures and commit**

```bash
uv run pytest tests/events/test_agent_payloads.py -q
git add src/agent_context_sdk/events/agent.py tests/events tests/fixtures/agent
git commit -m "feat: define agent activity events"
```

## Task SDK-014: Git, code, quality and knowledge payloads

**Files:**

- Create: `src/agent_context_sdk/events/git.py`
- Create: `src/agent_context_sdk/events/code.py`
- Create: `src/agent_context_sdk/events/quality.py`
- Create: `src/agent_context_sdk/events/knowledge.py`
- Create: `tests/events/test_git_payloads.py`
- Create: `tests/events/test_code_payloads.py`
- Create: `tests/events/test_quality_payloads.py`
- Create: `tests/events/test_knowledge_payloads.py`

**Interfaces:**

- Produces: repository/checkout/workspace-snapshot/commit events; file/symbol/index assertion events; test/CI/finding events; decision/constraint/failure/summary events.
- Consumes: typed IDs and strict Pydantic conventions.

- [ ] **Step 1: Write workspace snapshot and assertion tests**

```python
def test_workspace_snapshot_represents_uncommitted_state() -> None:
    payload = WorkspaceSnapshotCapturedV1(
        repository_id="repo_0198a4a9",
        checkout_id="co_0198a4aa",
        base_commit="f" * 40,
        dirty_patch_sha256="a" * 64,
        modified_content_sha256=["b" * 64],
        untracked_paths=["src/new_file.py"],
    )
    assert payload.dirty_patch_sha256 != payload.base_commit


def test_code_assertion_requires_provenance() -> None:
    assertion = CodeRelationAssertedV1(
        assertion_id="assert_0198a4ac",
        subject_id="symbol:a",
        predicate="CALLS",
        object_id="symbol:b",
        evidence_kind="scip",
        extractor_name="scip-python",
        extractor_version="1.0.0",
        confidence=1.0,
        valid_from="2026-08-13T13:00:00Z",
    )
    assert assertion.confidence == 1.0
```

- [ ] **Step 2: Run tests and verify missing models**

Run: `uv run pytest tests/events/test_git_payloads.py tests/events/test_code_payloads.py -q`

- [ ] **Step 3: Implement exact payload families**

Use separate enums for deterministic evidence and inference. Dependencies distinguish `declared`, `resolved` and `observed`. Decisions and constraints carry valid/recorded time and `supersedes_id`; failures carry stable fingerprint inputs without embedding secrets.

- [ ] **Step 4: Add bounds tests and commit**

Assert confidence outside `[0,1]`, non-SHA Git IDs, absolute untrusted paths and an inference marked deterministic are rejected.

```bash
uv run pytest tests/events -q
git add src/agent_context_sdk/events tests/events
git commit -m "feat: define repository and knowledge events"
```

## Task SDK-015: Ingestion, query and MCP DTOs

**Files:**

- Create: `src/agent_context_sdk/contracts/__init__.py`
- Create: `src/agent_context_sdk/contracts/ingestion.py`
- Create: `src/agent_context_sdk/contracts/query.py`
- Create: `src/agent_context_sdk/contracts/mcp.py`
- Create: `tests/contracts/test_ingestion.py`
- Create: `tests/contracts/test_context_package.py`
- Create: `tests/contracts/test_mcp_tools.py`

**Interfaces:**

- Produces: `SanitizedContentItemV1`, `IngestBatchRequestV1`, `IngestBatchResponseV1`, `AcceptedEventV1`, `RejectedEventV1`, `EvidenceRefV1`, `ContextItemV1`, `ContextPackageV1`, five tool input models and five tool output models.
- Consumes: `EventDraftV1`, IDs and content references.

- [ ] **Step 1: Write batch bounds and context provenance tests**

```python
def test_ingestion_batch_is_bounded(event_draft) -> None:
    with pytest.raises(ValidationError):
        IngestBatchRequestV1(
            batch_id=new_uuid7(),
            events=[event_draft] * 501,
            content_items=[],
        )


def test_context_item_requires_evidence() -> None:
    with pytest.raises(ValidationError):
        ContextItemV1(
            kind="decision",
            title="Use outbox",
            content="Persist the event and outbox row in one transaction.",
            evidence=[],
        )
```

- [ ] **Step 2: Run tests and see missing DTOs**

Run: `uv run pytest tests/contracts -q`

- [ ] **Step 3: Implement contracts**

Batch size is 1–500 events and request bytes are separately enforced by the API. `SanitizedContentItemV1` carries a `ContentClaimV1`, base64-encoded sanitized bytes and `RedactionReportV1`; validation decodes within a bound and verifies length/digest. Every event claim must resolve to exactly one same-batch content item, while unreferenced content items are rejected. Tool inputs require explicit repository/project IDs and revision/snapshot where applicable. `ContextPackageV1` includes `schema_version`, `query_id`, scope, `as_of_event_id`, projection checkpoint, items, `truncated`, opaque `next_cursor`, warnings and staleness milliseconds.

- [ ] **Step 4: Add JSON round-trip tests and commit**

```bash
uv run pytest tests/contracts -q
uv run mypy src/agent_context_sdk/contracts
git add src/agent_context_sdk/contracts tests/contracts
git commit -m "feat: publish ingestion and memory contracts"
```

## Task SDK-016: Layered local redaction

**Files:**

- Create: `src/agent_context_sdk/redaction/policy.py`
- Create: `src/agent_context_sdk/redaction/detectors.py`
- Create: `src/agent_context_sdk/redaction/engine.py`
- Create: `tests/redaction/test_engine.py`
- Create: `tests/redaction/test_fail_closed.py`
- Create: `tests/fixtures/redaction/canaries.json`

**Interfaces:**

- Produces: `RedactionPolicyV1`, `SecretFinding`, `RedactionResult`, `RedactionError`, `redact_json(value, policy, known_secrets=()) -> RedactionResult`.
- Consumes: redaction report models from SDK-012.

- [ ] **Step 1: Create a non-production canary corpus**

Include fake AWS, GitHub, OpenAI-style, JWT, PEM private-key, password-in-URL, database DSN and high-entropy samples. Prefix every fixture with `AGENT_CONTEXT_CANARY_` so scans are unambiguous.

- [ ] **Step 2: Write zero-leak tests**

```python
def test_every_canary_is_removed(canary_payload, policy) -> None:
    result = redact_json(canary_payload, policy)
    rendered = canonical_json_bytes(result.value).decode()
    for canary in canary_payload["raw_values"]:
        assert canary not in rendered
        assert canary not in result.report.model_dump_json()
    assert "<redacted:" in rendered
```

- [ ] **Step 3: Run and confirm failure before the engine exists**

Run: `uv run pytest tests/redaction -q`

- [ ] **Step 4: Implement detectors and recursive traversal**

Detection order is explicit known values, private keys, structured credential fields, known token patterns, credential URLs and entropy candidates. Replacement is `<redacted:<kind>:<ordinal>>`. Traverse dict keys/values and list/string leaves without executing or decoding arbitrary objects. Raise `RedactionError` on unsupported values or detector failure.

- [ ] **Step 5: Add fail-closed and fuzz tests**

Monkeypatch a detector to raise and assert no partial `RedactionResult` is returned. Use Hypothesis nested JSON and assert the original object is not mutated.

- [ ] **Step 6: Run and commit**

```bash
uv run pytest tests/redaction -q
uv run mypy src/agent_context_sdk/redaction
git add src/agent_context_sdk/redaction tests/redaction tests/fixtures/redaction
git commit -m "feat: add fail-closed secret redaction"
```

## Task SDK-017: Registry, schema bundle and v0.1.0 release

**Files:**

- Modify: `src/agent_context_sdk/__init__.py`
- Create: `src/agent_context_sdk/registry.py`
- Create: `src/agent_context_sdk/schema_export.py`
- Create: `tests/test_registry.py`
- Create: `tests/test_schema_compatibility.py`
- Create: `scripts/export-schemas.sh`
- Generate: `schemas/v1/*.json`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Interfaces:**

- Produces: `EVENT_PAYLOAD_MODELS`, `TOOL_INPUT_MODELS`, `TOOL_OUTPUT_MODELS`, `resolve_event_model(event_type, schema_version)`, deterministic schema bundle and release artifacts.
- Consumes: every SDK-013..016 model.

- [ ] **Step 1: Write registry completeness tests**

```python
def test_registry_has_unique_event_keys() -> None:
    keys = list(EVENT_PAYLOAD_MODELS)
    assert len(keys) == len(set(keys))
    assert ("agent.session.started", "1.0.0") in keys
    assert ("git.workspace_snapshot.captured", "1.0.0") in keys
```

- [ ] **Step 2: Write deterministic schema test**

Export into two temporary directories and assert file-name sets and SHA-256 digests are equal. Load all golden valid/invalid fixtures through the registry.

- [ ] **Step 3: Run tests and verify registry is missing**

Run: `uv run pytest tests/test_registry.py tests/test_schema_compatibility.py -q`

- [ ] **Step 4: Implement explicit registries and schema export**

Registries are literal mappings, not filesystem discovery. Schema filenames are `<domain>.<name>.v1.json`; generated JSON is sorted and formatted identically. Public root exports include only supported contracts.

- [ ] **Step 5: Run the full SDK gate**

```bash
uv run python -m agent_context_sdk.schema_export --output schemas/v1
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest --cov=agent_context_sdk --cov-fail-under=90 -q
uv build
git diff --exit-code -- schemas/v1
```

Expected: commands pass, wheel and sdist appear under `dist/`, generated schemas are clean.

- [ ] **Step 6: Commit and tag after review**

```bash
git add src schemas tests scripts pyproject.toml uv.lock
git commit -m "feat: freeze v0.1 context contracts"
git tag -a v0.1.0 -m "Agent Context SDK contract v0.1.0"
```

## Release acceptance

- [ ] Fresh environment installs the built wheel and validates every fixture.
- [ ] Canonical bytes and hashes match the golden files.
- [ ] No canary or known secret value appears in sanitized output or reports.
- [ ] JSON Schema export is deterministic and clean after regeneration.
- [ ] `v0.1.0` points to the reviewed commit and consumers record that SHA.
