# Agent Context Platform

`agent-context-platform` is the service boundary for the authoritative event ledger, verified
content storage, rebuildable projections, retrieval, indexing, and the read-only MCP endpoint.

The repository owns FastAPI and remote platform behavior. Canonical event and query contracts live
in `agent-context-sdk`; Codex-local capture and redaction live in `agent-context-codex`; deployment
inventory and secrets references live in `agent-context-infra`. Do not duplicate their contracts
here.

See the [approved architecture](docs/2026-08-13-agent-context-platform-design.md), the
[platform implementation plan](docs/2026-08-13-agent-context-platform.md), and the
[cross-repository roadmap](docs/2026-08-13-agent-context-master-roadmap.md).

## Requirements

- Python 3.12, 3.13, or 3.14
- uv 0.11.28

## Set up and run

The private `agent-context-sdk` dependency is pinned to an immutable release in `pyproject.toml`.
Before syncing, download its reviewed wheel using a fine-grained GitHub token with `Contents: read`
access to `VINIClUS/agent-context-sdk`, then verify the recorded digest:

```bash
mkdir -p build/sdk
gh release download v0.5.0 \
  --repo VINIClUS/agent-context-sdk \
  --pattern agent_context_sdk-0.5.0-py3-none-any.whl \
  --dir build/sdk
echo "0db43637cd754ba90a0006053a1f9a3a9c17c3dccd0d9524cc9aa26ae45bcda0  build/sdk/agent_context_sdk-0.5.0-py3-none-any.whl" \
  | sha256sum --check --strict -
uv sync --frozen
```

Alternatively, check out the SDK at tag `v0.5.0`, confirm it resolves to commit
`039db78d93ac562ed68feec3ef7d04488a3c0b63`, build the wheel with the SDK's pinned toolchain,
copy it to `build/sdk/`, and run the same digest check before syncing. The wheel is intentionally
ignored by Git; its tag, commit, version, and expected digest are recorded under
`[tool.agent-context.sdk]`.

After the locked environment is installed, run the application factory locally:

```bash
uv run --frozen uvicorn agent_context_platform.app:create_app \
  --factory --host 127.0.0.1 --port 8000
```

The process-only liveness endpoint is `GET http://127.0.0.1:8000/health/live`. It does not
represent readiness of PostgreSQL, object storage, Neo4j, or any other external service.

## Quality gates

Run the same gates as CI:

```bash
uv lock --check
uv sync --frozen
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv run --frozen mypy src
uv run --frozen pytest -q
```

Pytest registers the `unit`, `integration`, `contract`, and `e2e` markers. The suite enforces 90%
branch coverage.

## Integration tests

Tests marked `integration` under `tests/integration/` exercise real PostgreSQL, Neo4j, and Garage
(S3-compatible) services. `scripts/test-services.sh` manages a disposable, isolated stack for them
via `compose.test.yml`: every service binds to `127.0.0.1` on an ephemeral port, and none of its
state survives `down`.

```bash
scripts/test-services.sh up             # start services, wait for health, bootstrap Garage
eval "$(scripts/test-services.sh env)"  # export AGENT_CONTEXT_TEST_* variables
uv run --frozen pytest -q               # or: pytest -q -m integration
scripts/test-services.sh down           # stop services and remove all state
```

`env` exports `AGENT_CONTEXT_TEST_POSTGRES_DSN`,
`AGENT_CONTEXT_TEST_NEO4J_{URI,USERNAME,PASSWORD,DATABASE}`, and
`AGENT_CONTEXT_TEST_S3_{ENDPOINT_URL,REGION_NAME,BUCKET_NAME,ACCESS_KEY_ID,SECRET_ACCESS_KEY}`.
Without the exported variables, the PostgreSQL, Neo4j, and S3 integration tests skip individually
instead of failing the run. The `integration` CI job runs the same script before the suite and always
tears the stack down afterwards. `scripts/test-services.sh status` shows the running containers.
`AGENT_CONTEXT_TEST_PROJECT` overrides the derived Compose project name, which otherwise comes from
this checkout's path so concurrent worktrees never collide.

## Ingestion API contract

`openapi/agent-context-v1.json` freezes the ingestion v1 HTTP surface (`/v1/ingestion/*` and the
components it references; health, MCP and admin routes are not part of it). It is immutable: a
change needs a new API version. `tests/contract/test_openapi_snapshot.py` compares its SHA-256
with the live application, and `tests/fixtures/ingestion/` holds the consumer request/response
fixtures (see its README).

```bash
uv run python scripts/export-openapi.py --check   # exit 1 on drift
uv run python scripts/export-openapi.py           # rewrite the snapshot (new API versions only)
```

## Development workflow

Create feature worktrees as siblings of repository checkouts, using an immutable SHA captured from
the required base. Branches follow `agent/<lowercase-task-id>-<slug>`, such as
`agent/platform-001-fastapi-quality-scaffold`.

Keep each task within its declared repository and paths. Use Conventional Commits, run the targeted
test during development, and run every quality gate before requesting integration.
