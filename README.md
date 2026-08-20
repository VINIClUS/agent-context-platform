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
gh release download v0.1.0 \
  --repo VINIClUS/agent-context-sdk \
  --pattern agent_context_sdk-0.1.0-py3-none-any.whl \
  --dir build/sdk
echo "13cf6b39cae0d563e1f8f7ab263674407a81c497aee0f658a92a3a1ff21d3be1  build/sdk/agent_context_sdk-0.1.0-py3-none-any.whl" \
  | sha256sum --check --strict -
uv sync --frozen
```

Alternatively, check out the SDK at tag `v0.1.0`, confirm it resolves to commit
`a9179deb97a0402112a5bf1ebf3ba97ec547aa6c`, build the wheel with the SDK's pinned toolchain,
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

## Development workflow

Create feature worktrees as siblings of repository checkouts, using an immutable SHA captured from
the required base. Branches follow `agent/<lowercase-task-id>-<slug>`, such as
`agent/platform-001-fastapi-quality-scaffold`.

Keep each task within its declared repository and paths. Use Conventional Commits, run the targeted
test during development, and run every quality gate before requesting integration.
