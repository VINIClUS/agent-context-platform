# Agent Context Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the authoritative ingestion ledger, rebuildable temporal code graph, hybrid retrieval service and stateless MCP memory endpoint.

**Architecture:** A modular FastAPI service appends sanitized event drafts to PostgreSQL, stores large sanitized content through a product-neutral S3 adapter, and projects events idempotently into Neo4j. Indexers emit events rather than writing graph state. Read-only MCP `2026-07-28` tools compose bounded, provenance-rich context packages.

**Tech Stack:** Python 3.12+, uv, FastAPI, Pydantic v2, SQLAlchemy 2 async, psycopg 3, Alembic, PostgreSQL, boto3/S3, Garage, Neo4j Python driver, SCIP protobuf, Tree-sitter, sentence-transformers, MCP Python SDK v2, OpenTelemetry and pytest/testcontainers.

## Global Constraints

- Pin the immutable `agent-context-sdk` v0.1.0 artifact and commit.
- Only PostgreSQL ledger and referenced sanitized S3 objects are canonical.
- No direct graph writes outside projector modules.
- No committed event may reference a missing or unverified object.
- Application DB role cannot update/delete ledger events.
- All projectors and API retries are idempotent.
- Indexers run rootless, network-disabled and against read-only/canonicalized paths.
- MCP targets `2026-07-28` using Python SDK v2 `MCPServer` and `stateless_http=True`.
- No application code depends on `initialize`, `notifications/initialized`, `Mcp-Session-Id`, GET-session streams or sticky routing.
- MCP tools never accept SQL, Cypher, filesystem paths outside resolved repository scope, or arbitrary URLs.
- Content is never duplicated into ordinary logs or OTel attributes.

---

## File map

```text
src/agent_context_platform/
├── app.py
├── cli.py
├── settings.py
├── db.py
├── catalog/{models.py,repository.py}
├── ledger/{models.py,repository.py,service.py,api.py}
├── content/{blob_store.py,service.py,orphans.py}
├── projection/
│   ├── runtime.py
│   ├── registry.py
│   ├── verify.py
│   └── projectors/{agent.py,code.py,git.py,knowledge.py,portfolio.py,quality.py}
├── indexing/
│   ├── scanner.py
│   ├── identity.py
│   ├── emitter.py
│   ├── scip.py
│   └── tree_sitter/{base.py,python.py,typescript.py,go.py}
├── retrieval/{graph.py,temporal.py,search.py,composer.py,embeddings.py}
├── mcp/{server.py,auth.py,protocol_guard.py,tools.py}
└── observability.py
alembic/versions/
tests/{unit,integration,contract,fixtures,evaluation,mcp}/
docs/{adr,data,mcp}/
```

## Task PLATFORM-001: FastAPI and quality scaffold

**Files:** `pyproject.toml`, `uv.lock`, `src/agent_context_platform/app.py`, `src/agent_context_platform/settings.py`, `tests/unit/test_app.py`, `.github/workflows/quality.yml`, `README.md`.

**Interfaces:** Produces `create_app(settings: Settings | None = None) -> FastAPI` and `/health/live`.

- [ ] Write a test asserting `GET /health/live` returns `{"status":"ok"}` without connecting to external services.
- [ ] Run `uv run pytest tests/unit/test_app.py -q`; confirm import failure.
- [ ] Create the package, strict environment settings with secret-safe `repr=False`, and app factory. Configure Python `>=3.12,<3.15`, Ruff, strict mypy and pytest markers `unit`, `integration`, `contract`, `e2e`.
- [ ] Run `uv run ruff check . && uv run mypy src && uv run pytest -q`.
- [ ] Commit with `git commit -m "chore: scaffold context platform"`.

## Task PLATFORM-010: Settings, database and dependency boundary

**Files:** `src/agent_context_platform/settings.py`, `src/agent_context_platform/db.py`, `tests/unit/test_settings.py`, `tests/unit/test_db.py`, `pyproject.toml`, `uv.lock`.

**Interfaces:** Produces `Settings`, `create_engine(settings) -> AsyncEngine`, `session_factory(engine) -> async_sessionmaker[AsyncSession]`.

- [ ] Write tests proving required production DSNs fail validation, secret fields do not appear in `repr`, and pool pre-ping is enabled.
- [ ] Add SQLAlchemy async, psycopg, Neo4j, boto3, Alembic and the exact SDK v0.1.0 dependency. During local multi-repo development install the reviewed SDK wheel; CI downloads the release artifact and verifies its SHA-256 before `uv sync --frozen`.
- [ ] Implement one settings object with nested PostgreSQL, Neo4j, S3 and MCP sections. Do not read secrets outside settings construction.
- [ ] Run `uv run pytest tests/unit/test_settings.py tests/unit/test_db.py -q` and mypy.
- [ ] Commit `feat: establish platform dependency boundary`.

## Task PLATFORM-011: Catalog model

**Files:** `src/agent_context_platform/catalog/models.py`, `src/agent_context_platform/catalog/repository.py`, `tests/unit/catalog/test_models.py`, `tests/integration/catalog/test_repository.py`.

**Interfaces:** Produces SQLAlchemy models `WorkspaceRow`, `ProjectRow`, `RepositoryRow`, `ProjectRepositoryRow`, `CheckoutRow`, `ContentObjectRow`; repository methods `upsert_repository`, `resolve_checkout`, `get_scope`.

- [ ] Write tests for project↔repository N:N, normalized remote uniqueness and distinct checkout identities for two worktrees.
- [ ] Implement schema-qualified models with UUID primary keys, creation/observation times and no cascade that deletes ledger history.
- [ ] Normalize Git HTTPS/SSH remotes into host/owner/path while retaining the original as metadata; local path alone never identifies a repository.
- [ ] Run unit and PostgreSQL integration tests.
- [ ] Commit `feat: add repository catalog model` without touching Alembic files.

## Task PLATFORM-012: Ledger, outbox and operations model

**Files:** `src/agent_context_platform/ledger/models.py`, `src/agent_context_platform/projection/models.py`, `src/agent_context_platform/operations/models.py`, corresponding unit tests.

**Interfaces:** Produces `EventStreamRow`, `EventRow`, `EventContentRefRow`, `RedactionReportRow`, `OutboxRow`, `ProjectionCheckpointRow`, `DeadLetterRow`, `IngestionBatchRow`, `RegisteredProducerRow`, `SchemaVersionRow`, `RetentionPolicyRow`, `IntegrityCheckpointRow`.

- [ ] Write SQLAlchemy metadata tests for unique keys `(producer_id,idempotency_key)`, `(stream_id,stream_sequence)` and `event_id`.
- [ ] Implement JSONB payload/context columns, 64-character hash checks, immutable event timestamps and operational status enums.
- [ ] Give `RegisteredProducerRow` a unique producer ID, non-secret token prefix, Argon2id verifier, `events:ingest` scope, expiry/revocation and audit timestamps; never store a recoverable bearer.
- [ ] Add a SQL test proving the application role has no `UPDATE`/`DELETE` privilege on `ledger.events` after migrations apply.
- [ ] Run the model tests.
- [ ] Commit `feat: define ledger and projection persistence` without creating a migration.

## Task PLATFORM-013: Product-neutral S3 blob store

**Files:** `src/agent_context_platform/content/blob_store.py`, `tests/unit/content/test_blob_store.py`, `tests/integration/content/test_s3_contract.py`.

**Interfaces:**

```python
@dataclass(frozen=True)
class StoredBlob:
    sha256: str
    object_key: str
    compressed_bytes: int
    uncompressed_bytes: int
    media_type: str


class BlobStore(Protocol):
    async def put_verified(self, content: bytes, media_type: str) -> StoredBlob:
        raise NotImplementedError

    async def get_verified(self, object_key: str, expected_sha256: str) -> bytes:
        raise NotImplementedError

    async def delete(self, object_key: str) -> None:
        raise NotImplementedError
```

- [ ] Write unit tests for deterministic key `sha256/aa/bb/<digest>.zst`, Zstandard round-trip and digest mismatch rejection.
- [ ] Implement `S3BlobStore` with two write modes: deterministic content-addressed PUT (the
  Garage default) and `If-None-Match: *` conditional PUT where the S3 implementation supports
  it. Garage does not provide the conditional-write semantics required by the latter mode;
  deterministic content-addressed writes remain idempotent through post-PUT verification.
- [ ] Run the same integration contract against Garage. Keep endpoint/region/path-style options generic so existing MinIO can use it.
- [ ] Inject a corrupted metadata response and assert `BlobIntegrityError` before any caller can commit an event reference.
- [ ] Run tests and commit `feat: add verified S3 blob storage`.

## Task PLATFORM-014: Neo4j connection and schema

**Files:** `src/agent_context_platform/projection/neo4j.py`, `src/agent_context_platform/projection/schema.py`, `tests/integration/projection/test_neo4j_schema.py`.

**Interfaces:** Produces `Neo4jStore.verify_connectivity`, `ensure_schema`, `execute_read`, `execute_write` for projectors only.

- [ ] Write an integration test that calls `ensure_schema` twice and checks unique constraints for all logical IDs and indexes for source event, validity and vector fields.
- [ ] Implement driver lifecycle with bounded timeouts, parameter-only queries and health status that does not expose credentials.
- [ ] Make `execute_write` importable only from the projection package; retrieval receives a read-only facade.
- [ ] Run tests against Neo4j Community and commit `feat: establish rebuildable graph store`.

## Task PLATFORM-015: Initial PostgreSQL migration

**Files:** `alembic.ini`, `alembic/env.py`, one `alembic/versions/*_initial_ledger.py`, `tests/integration/test_migrations.py`.

**Interfaces:** Materializes PLATFORM-011/012 tables, constraints, roles and grants.

- [ ] Write a migration test: upgrade empty DB to head, inspect all four schemas/constraints, downgrade to base, then upgrade again.
- [ ] Configure Alembic with the combined metadata and generate one migration; manually review schema names, FK actions and indexes.
- [ ] Add SQL grants: API role can append/select ledger but cannot update/delete events; projector role can mutate only operational checkpoints and Neo4j externally.
- [ ] Run `uv run alembic check` and migration integration tests.
- [ ] Commit `feat: create authoritative ledger schema`.

## Task PLATFORM-020: Concurrent ledger repository

**Files:** `src/agent_context_platform/ledger/repository.py`, `tests/integration/ledger/test_append.py`, `tests/integration/ledger/test_concurrency.py`, `tests/integration/ledger/test_integrity.py`.

**Interfaces:** Produces internal `ResolvedEvent(draft, content_refs)`, `LedgerRepository.append(session, resolved_events) -> list[StoredEventV1]` and `get_by_idempotency_keys`.

- [ ] Write a 100-coroutine test submitting the same draft; assert one event row, one outbox row and identical accepted event ID for every caller.
- [ ] Implement sorted per-stream locking with `SELECT ... FOR UPDATE`, server sequence allocation and SDK `seal_event(draft, content_refs, sequence, previous_hash)` in a single transaction.
- [ ] On unique idempotency conflict, read and return the already stored event; never allocate a second sequence.
- [ ] Write tamper verification and two-stream deadlock tests.
- [ ] Run PostgreSQL integration tests and commit `feat: append idempotent hash-chained events`.

## Task PLATFORM-021: Content transaction protocol

**Files:** `src/agent_context_platform/content/service.py`, `src/agent_context_platform/content/orphans.py`, `tests/integration/content/test_service.py`, `tests/integration/content/test_crash_matrix.py`.

**Interfaces:** Produces `ContentService.prepare(items: Sequence[SanitizedContentItemV1]) -> PreparedContent`, `PreparedContent.resolve(claims) -> tuple[ContentRefV1, ...]`, `attach(session, prepared)`, `OrphanSweeper.run(cutoff) -> SweepReport`.

- [ ] Write tests for 65,536 bytes inline and 65,537 bytes in S3, both preserving the same uncompressed digest.
- [ ] Implement defense-in-depth redaction verification: if applying the SDK policy would change content, reject it as `content_requires_redaction` rather than storing a rewritten version.
- [ ] Upload/verify objects before database attachment. A failed DB transaction leaves an unreferenced object; the sweeper removes only objects older than 24 hours and absent from `catalog.content_objects`.
- [ ] Inject failure before put, after put, after head and before commit; assert no committed dangling reference.
- [ ] Run tests and commit `feat: coordinate sanitized content persistence`.

## Task PLATFORM-022: Batch ingestion API

**Files:** `src/agent_context_platform/ledger/auth.py`, `src/agent_context_platform/ledger/service.py`, `src/agent_context_platform/ledger/api.py`, `src/agent_context_platform/app.py`, `tests/contract/test_ingestion_api.py`, `tests/integration/ledger/test_batch_atomicity.py`, `tests/integration/ledger/test_producer_auth.py`.

**Interfaces:** `POST /v1/ingestion/batches` consumes `IngestBatchRequestV1`, returns `IngestBatchResponseV1`, and accepts `Idempotency-Key` equal to batch ID.

- [ ] Write contract tests for 1–500 events, request-size limit, invalid schema, absent/invalid/revoked/expired producer credential, producer-ID mismatch, missing registration and partial duplicate batch.
- [ ] Authenticate an `events:ingest` bearer by non-secret prefix plus Argon2id verifier and bind it to one registered `producer_id`; reject the MCP `memory:read` credential on this route.
- [ ] Implement order: authenticate producer, validate SDK schemas, prepare/verify content, start DB transaction, append events and references/outbox, commit, return accepted mappings.
- [ ] Reject the full batch on any new-event failure; already-existing idempotent events remain reported as existing, not reinserted.
- [ ] Add request ID and trace correlation without logging request bodies.
- [ ] Run contract/integration tests and commit `feat: expose atomic event ingestion`.

## Task PLATFORM-023: Freeze the HTTP contract

**Files:** `openapi/agent-context-v1.json`, `tests/contract/test_openapi_snapshot.py`, `tests/fixtures/ingestion/*.json`, `scripts/export-openapi.py`.

**Interfaces:** Produces immutable v1 OpenAPI snapshot and consumer fixtures for `agent-context-codex`.

- [ ] Write a test that exports OpenAPI with sorted keys and compares its SHA-256 to the checked-in snapshot.
- [ ] Add successful, duplicate, validation-error, redaction-required and payload-too-large request/response fixtures.
- [ ] Run the service against those fixtures from a transport-neutral test client.
- [ ] Export once, run full contract tests and commit `test: freeze ingestion API v1`.

## Task PLATFORM-024: Projector runtime

**Files:** `src/agent_context_platform/projection/runtime.py`, `src/agent_context_platform/projection/checkpoints.py`, `tests/integration/projection/test_runtime.py`, `tests/integration/projection/test_crash_replay.py`.

**Interfaces:** Produces `Projector` protocol with `name`, `version`, `handles(event_type)` and `project(tx, event)`; `ProjectionRunner.run_once(limit) -> ProjectionRunReport`.

- [ ] Write tests for ordered leases, retry count, checkpoint advancement, poison-event DLQ and two competing workers.
- [ ] Implement PostgreSQL outbox claiming with `FOR UPDATE SKIP LOCKED`. Execute idempotent Neo4j transaction, then advance checkpoint/mark outbox delivered.
- [ ] Crash after Neo4j commit and before checkpoint; replay must upsert the same graph state.
- [ ] Bound retries with exponential delay and retain original event ID/error class in DLQ without payload content.
- [ ] Run tests and commit `feat: add crash-safe projection runtime`.

## Task PLATFORM-025: Initial domain projectors

**Files:** `src/agent_context_platform/projection/projectors/{portfolio.py,agent.py,git.py}`, tests under `tests/integration/projection/projectors/`.

**Interfaces:** Produces projectors for workspace/project/repository/checkout/worktree/workspace snapshot/session/turn/tool call/commit.

- [ ] Write golden Cypher-state tests from SDK fixtures; assert one node per logical ID and source-event provenance on every relationship.
- [ ] Implement MERGE by deterministic IDs and SET only projection-owned current pointers; historical assertion/revision nodes are immutable.
- [ ] Coalesce hook and OTel observations by native session/turn/tool IDs without deleting either source event.
- [ ] Replay fixtures twice and compare graph digest.
- [ ] Commit `feat: project portfolio and agent activity`.

## Task PLATFORM-030: Safe Git scanner

**Files:** `src/agent_context_platform/indexing/scanner.py`, `tests/unit/indexing/test_scanner.py`, `tests/fixtures/repos/scanner/`.

**Interfaces:** Produces `RepositoryScan`, `TrackedFile`, `WorkspaceState`, `scan_repository(root: Path) -> RepositoryScan`.

- [ ] Write tests for tracked/untracked/ignored files, submodules, symlink escape, binary files, dirty patch and detached HEAD.
- [ ] Implement through argument-array Git subprocesses (`git ls-files`, `git diff --binary`, `git status --porcelain=v2`) with no shell interpolation.
- [ ] Resolve every candidate and reject any path outside the canonical repository root.
- [ ] Emit content digests without logging file content.
- [ ] Run tests and commit `feat: scan repository and dirty workspace safely`.

## Task PLATFORM-031: Logical code identity

**Files:** `src/agent_context_platform/indexing/identity.py`, `tests/unit/indexing/test_identity.py`, `tests/fixtures/indexing/rename_cases.json`.

**Interfaces:** Produces `file_logical_id`, `file_revision_id`, `symbol_fallback_id`, `symbol_revision_id`, `resolve_rename`.

- [ ] Write golden tests for unchanged file, content change, Git rename, delete/recreate, ambiguous copy and symbol signature change.
- [ ] Implement deterministic UUIDv5-like IDs under repository namespace using canonical identity components; do not use local path for repository identity.
- [ ] Preserve file logical ID only for deterministic rename evidence; ambiguous cases produce a supersession assertion with confidence below 1.
- [ ] Prefer SCIP symbol strings over fallback qualified names.
- [ ] Commit `feat: define temporal code identities`.

## Task PLATFORM-032: Tree-sitter adapter protocol and sandbox

**Files:** `src/agent_context_platform/indexing/tree_sitter/base.py`, `src/agent_context_platform/indexing/tree_sitter/runner.py`, `tests/unit/indexing/tree_sitter/test_base.py`, `containers/indexer/Dockerfile`.

**Interfaces:** Produces `StructuralAdapter` protocol and normalized `ParsedModule`, `ParsedFile`, `ParsedSymbol`, `StructuralRelation` values.

- [ ] Write adapter contract tests requiring byte ranges, qualified names, kinds and explicit `evidence_kind="tree_sitter"`.
- [ ] Implement bounded input/output messages; parser process has read-only mount, no network, non-root UID, memory/CPU/time limit.
- [ ] Reject output paths/ranges outside the input file and relation endpoints absent from the normalized symbol set.
- [ ] Run contract and container security tests; commit `feat: add sandboxed structural index interface`.

## Tasks PLATFORM-033, PLATFORM-034 and PLATFORM-035: Language adapters

These are three independent worktrees with the same contract but non-overlapping files.

| ID | Files | Required fixture |
| --- | --- | --- |
| PLATFORM-033 | `tree_sitter/python.py`, `test_python.py` | packages, classes, async functions, decorators, imports |
| PLATFORM-034 | `tree_sitter/typescript.py`, `test_typescript.py` | TS/JS modules, classes, functions, imports/exports |
| PLATFORM-035 | `tree_sitter/go.py`, `test_go.py` | packages, types, methods, functions, imports |

For each worktree:

- [ ] Write a golden normalized-output test before the adapter.
- [ ] Implement modules/files/symbol definitions and structural import/call candidates supported by syntax only.
- [ ] Mark unresolved/dynamic calls heuristic; never label them SCIP-semantic.
- [ ] Run the shared adapter contract plus language test.
- [ ] Commit PLATFORM-033 as `feat: index Python structure`, PLATFORM-034 as `feat: index TypeScript and JavaScript structure`, and PLATFORM-035 as `feat: index Go structure`.

## Task PLATFORM-036: SCIP importer

**Files:** `src/agent_context_platform/indexing/scip.py`, `tests/unit/indexing/test_scip.py`, `tests/fixtures/scip/`.

**Interfaces:** Produces `import_scip(index_bytes, repository_id, commit) -> SemanticIndex`.

- [ ] Write golden tests for definition, reference, external symbol, documentation and malformed range.
- [ ] Parse official SCIP protobuf without shelling out. Record tool name/version/arguments/toolchain/commit with the index run.
- [ ] Normalize symbol identity exactly from SCIP; discard invalid/out-of-file ranges with a diagnostic event, not a fabricated edge.
- [ ] Prove SCIP relation confidence/evidence outranks a conflicting Tree-sitter candidate.
- [ ] Commit `feat: import semantic SCIP evidence`.

## Task PLATFORM-037: Code-index event emitter

**Files:** `src/agent_context_platform/indexing/emitter.py`, `src/agent_context_platform/indexing/registry.py`, `tests/integration/indexing/test_emitter.py`.

**Interfaces:** Produces `IndexingService.index(scan, semantic, structural) -> list[EventDraftV1]` and the sole adapter registry.

- [ ] Write a test combining all adapter fixtures and asserting stable event IDs/idempotency keys for repeated identical input.
- [ ] Merge semantic and structural evidence by precedence without destroying lower-confidence contradictory observations.
- [ ] Emit `code.index.started/completed`, file/symbol revision and relation-assertion drafts through the ingestion service.
- [ ] Integrate the three language adapters and SCIP in the single registry file.
- [ ] Commit `feat: emit canonical code index events`.

## Task PLATFORM-038: Temporal code projector

**Files:** `src/agent_context_platform/projection/projectors/code.py`, `tests/integration/projection/projectors/test_code.py`.

**Interfaces:** Projects repository/module/file/file revision/symbol/symbol revision/dependency and assertion nodes/edges.

- [ ] Write exact graph tests for first index, unchanged index, content revision, rename, deletion, supersession and out-of-order observation.
- [ ] Implement immutable revision/assertion nodes plus bi-temporal validity. Current pointers are derived and replaceable during replay.
- [ ] Store `source_event_id`, evidence, extractor, confidence, validity and review status on every assertion.
- [ ] Replay events in occurred order and observed order; `as-of` answers must match.
- [ ] Commit `feat: project temporal code graph`.

## Task PLATFORM-039: Registry, verification and rebuild CLI

**Files:** `src/agent_context_platform/projection/registry.py`, `src/agent_context_platform/projection/verify.py`, `src/agent_context_platform/cli.py`, `tests/e2e/test_rebuild.py`.

**Interfaces:** CLI commands `agent-context projection rebuild`, `verify`, `status`.

- [ ] Write an E2E test that records graph digest, drops all graph data, replays, and compares digest/counts/IDs.
- [ ] Register all projectors in deterministic order and reject duplicate projector names/versions.
- [ ] `verify` checks checkpoint continuity, event coverage, orphan graph source IDs and current stream heads.
- [ ] `rebuild` requires an explicit target database, creates schema, replays and swaps only after verification.
- [ ] Commit `feat: rebuild and verify graph projections`.

## Tasks PLATFORM-040, PLATFORM-041 and PLATFORM-042: Independent retrieval primitives

### PLATFORM-040 — graph traversal

**Files:** `retrieval/graph.py`, `tests/integration/retrieval/test_graph.py`.

- [ ] Test bounded repo/module/file/symbol neighborhood, callers/callees and dependency paths.
- [ ] Implement parameterized Cypher with maximum depth 4, maximum 500 nodes and request deadline.
- [ ] Return evidence references with every result; commit `feat: query bounded code neighborhoods`.

### PLATFORM-041 — temporal knowledge/history

**Files:** `retrieval/temporal.py`, `tests/integration/retrieval/test_temporal.py`.

- [ ] Test `valid_at` versus `recorded_at`, superseded decisions and failure-to-fix paths.
- [ ] Implement bi-temporal queries returning active and historical assertions explicitly.
- [ ] Commit `feat: query temporal decisions and failures`.

### PLATFORM-042 — lexical/vector retrieval

**Files:** `retrieval/search.py`, `retrieval/embeddings.py`, `tests/integration/retrieval/test_search.py`.

- [ ] Test sanitized content lexical ranking, vector ranking, repository filter and purge removal.
- [ ] Implement an `EmbeddingProvider` interface and local sentence-transformers provider. Pin `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` by model revision in the release lock; embeddings are projections.
- [ ] Store vectors in the Neo4j Community-supported list/vector index path and lexical material in PostgreSQL full-text indexes.
- [ ] Commit `feat: retrieve sanitized semantic evidence`.

## Task PLATFORM-043: Context composer and evaluation harness

**Files:** `src/agent_context_platform/retrieval/composer.py`, `tests/evaluation/questions.json`, `tests/evaluation/test_context_quality.py`.

**Interfaces:** Produces `ContextComposer.context_for_task`, `repository_overview`, `symbol_context`, `decision_history`, `failure_history`, each returning `ContextPackageV1`.

- [ ] Add 30 concrete fixture questions with required evidence IDs, forbidden unsupported claims and token/item budgets.
- [ ] Implement deterministic candidate fusion: evidence tier, exact scope, graph proximity, lexical/vector score, accepted review, recency. Tie-break by stable ID.
- [ ] Enforce item, byte, token estimate and traversal budgets; produce opaque signed cursor and staleness from projector checkpoint.
- [ ] Assert every item cites at least one source event and no inference is rendered as deterministic fact.
- [ ] Commit `feat: compose provenance-rich agent context`.

## Task PLATFORM-044: Stateless MCP `2026-07-28` server

**Files:** `src/agent_context_platform/mcp/server.py`, `src/agent_context_platform/mcp/protocol_guard.py`, `src/agent_context_platform/app.py`, `tests/mcp/test_protocol_profile.py`, `pyproject.toml`, `uv.lock`.

**Interfaces:** Mounts `/mcp`; implements `server/discover`; advertises tools only after PLATFORM-046.

- [ ] Write tests for direct request without discovery, discovery, missing/unknown version, header/body mismatch, `resultType`, deterministic list order and rejection of GET/DELETE legacy paths.
- [ ] Add `mcp>=2.0.0,<3`, resolve the reviewed exact version into `uv.lock`, instantiate `MCPServer`, use Streamable HTTP and `stateless_http=True`; add Host/Origin/request-size guard before the MCP app.
- [ ] Include server info per result, list cache metadata and trace propagation.
- [ ] Add a source check that fails if project MCP source contains forbidden session/handshake symbols; exclude dependencies and historical docs.
- [ ] Commit `feat: serve stateless MCP 2026 protocol`.

## Task PLATFORM-045: Read-only MCP authentication

**Files:** `src/agent_context_platform/mcp/auth.py`, one auth-token Alembic migration, `tests/mcp/test_auth.py`, `docs/mcp/security.md`.

**Interfaces:** Produces opaque token records with ID, Argon2id verifier, scopes, expiry/revocation and principal; dependency `require_scope("memory:read")`.

- [ ] Test absent/invalid/expired/revoked token as 401, insufficient scope as 403, invalid Origin as 403 and token absence from logs.
- [ ] Parse bearer once, select token by non-secret prefix, verify Argon2id in a bounded worker and cache only successful principal metadata for a short TTL keyed by token HMAC.
- [ ] Add per-principal token bucket and audit metadata without request/response content.
- [ ] Document this as pre-provisioned bearer, not OAuth MCP.
- [ ] Commit `feat: secure MCP memory reads`.

## Task PLATFORM-046: Five MCP memory tools

**Files:** `src/agent_context_platform/mcp/tools.py`, `tests/mcp/test_tools.py`, `tests/mcp/test_annotations.py`.

**Interfaces:** Exposes exactly `context_for_task`, `repository_overview`, `symbol_context`, `decision_history`, `failure_history` using SDK input/output schemas.

- [ ] Write one schema/behavior test per tool plus tests for absent explicit repository/revision scope, cursor tampering and budget overflow.
- [ ] Bind tool functions directly to `ContextComposer`; never accept arbitrary Cypher or infer prior-call state.
- [ ] Return structured content matching output schema, concise text and sanitized `resource_link` values for oversized evidence.
- [ ] Set and justify `readOnlyHint=true`, `destructiveHint=false`, `openWorldHint=false`.
- [ ] Commit `feat: expose grounded Codex memory tools`.

## Task PLATFORM-047: MCP conformance and scale suite

**Files:** `tests/mcp/test_stateless_scale.py`, `scripts/run-mcp-conformance.sh`, `.github/workflows/mcp-conformance.yml`, `docs/mcp/conformance.md`.

**Interfaces:** Produces CI artifacts from MCP conformance `0.1.11` requirements `2026-07-28`.

- [ ] Start an auth-isolated loopback test profile and run the pinned official server requirements; no expected-failure baseline may mask a required check.
- [ ] Add two-replica test forcing alternating backend IDs and restarting one replica between calls.
- [ ] Assert no response sets a session cookie/header and list output is identical between replicas.
- [ ] Archive `checks.json` and application logs after a secret scan.
- [ ] Commit `test: enforce modern MCP conformance`.

## Task PLATFORM-048: MCP documentation set

**Files:** `docs/adr/0002-stateless-mcp-2026-07-28.md`, `docs/mcp/{README.md,protocol-profile.md,tool-contracts.md,deployment-and-scaling.md,conformance.md,legacy-compatibility.md}`.

**Interfaces:** Human contract used by Codex/infra agents.

- [ ] Document normative headers, per-request `_meta`, discovery, result/cache shape, auth, tool schemas and disabled optional capabilities.
- [ ] Put current setup first; confine `initialize`, sessions and prior transports to `legacy-compatibility.md` with historical warnings.
- [ ] Record minimum/last-tested Codex, SDK lock version, conformance pin and verification date.
- [ ] Link only official MCP, Python SDK and Codex sources for protocol/config claims.
- [ ] Run Markdown link/lint checks and commit `docs: make MCP 2026 protocol authoritative`.

## Task PLATFORM-050: Platform OpenTelemetry

**Files:** `src/agent_context_platform/observability.py`, instrumentation calls in bounded-context services, `tests/unit/test_observability.py`.

**Interfaces:** Emits standard HTTP/DB/GenAI spans and `agent_context.ingestion`, `projection`, `index`, `retrieval`, `mcp` metrics.

- [ ] Test span names/attributes with in-memory exporter and assert content/token values are absent.
- [ ] Instrument duration/outcome/count/lag, source event IDs and trace links using bounded-cardinality attributes.
- [ ] Mark platform producer namespace so ingestion ignores self-telemetry.
- [ ] Add redaction filter for exception/log fields and commit `feat: observe context platform safely`.

## Task PLATFORM-051: Codex OTel normalization

**Files:** `src/agent_context_platform/ledger/codex_otel.py`, `tests/contract/test_codex_otel.py`, `docs/data/codex-otel-mapping.md`.

**Interfaces:** Maps official Codex event types to canonical metadata events and native session/turn/tool identities.

- [ ] Create fixtures for `codex.conversation_starts`, API/stream events, user-prompt metadata, tool decisions/results and metrics.
- [ ] Map only documented fields; content stays redacted unless the separately approved public content path already provided it.
- [ ] Coalesce by native IDs in projection while preserving the telemetry source event.
- [ ] Assert platform-origin telemetry is rejected to prevent loops.
- [ ] Commit `feat: normalize Codex telemetry evidence`.

## Platform completion gate

Run:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -q
uv run alembic upgrade head
uv run pytest -m integration -q
uv run agent-context projection verify
npx --yes @modelcontextprotocol/conformance@0.1.11 server \
  --url http://127.0.0.1:8000/mcp \
  --requirements 2026-07-28
```

Expected: all commands pass, no required conformance waiver exists, graph rebuild matches its digest, and canary scan finds zero occurrences.
