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
gh release download v0.6.0 \
  --repo VINIClUS/agent-context-sdk \
  --pattern agent_context_sdk-0.6.0-py3-none-any.whl \
  --dir build/sdk
echo "11639c35114c8206ee5922adb3072f1ff31c07b39aaabe681e594d81e67ea079  build/sdk/agent_context_sdk-0.6.0-py3-none-any.whl" \
  | sha256sum --check --strict -
uv sync --frozen
```

Alternatively, check out the SDK at tag `v0.6.0`, confirm it resolves to commit
`474b763a536078adacffd93ca88a05ebfc391ada`, build the wheel with the SDK's pinned toolchain,
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

## Operator CLI

### Operating projections

`agent-context projection` (installed by `uv sync`) rebuilds, verifies and inspects the Neo4j graph,
which is always derivable from the ledger. It reads the usual `AGENT_CONTEXT_*` settings (PostgreSQL
DSN and the live Neo4j connection) and never prints a DSN, password or exception message. Exit
codes: `0` ok, `1` verification mismatch or failed operation, `2` usage error. `--json` prints one
machine-readable document.

```bash
agent-context projection status                      # checkpoint, lag, last error class, last run
agent-context projection verify [--require-caught-up] [--no-record]
agent-context projection rebuild --target-uri bolt://standby:7687 --target-database neo4j \
  --target-username neo4j                            # password: $AGENT_CONTEXT_REBUILD_TARGET_NEO4J_PASSWORD
agent-context projection rebuild --in-place --confirm neo4j   # stop the projection runner first
```

* **Database roles.** The CLI uses two connections, each falling back to
  `AGENT_CONTEXT_POSTGRESQL__DSN` when unset. `AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN` reads
  checkpoints, dead letters, stream heads and the ledger, and rewrites checkpoints for
  `--in-place`. `AGENT_CONTEXT_POSTGRESQL__API_DSN` is used only to record `projection.rebuilt`
  through the ingestion service, and is not needed with `--no-record`. Production needs two login
  roles, members of the `NOLOGIN` roles `agent_context_projector` and `agent_context_api`
  (provisioned by INFRA, I040); the owner or a shared DSN is not required. Both connections'
  grants are checked before any target is wiped or checkpoint reset, and a missing grant exits `1`
  naming the privilege. If the verification or rebuild succeeds but the report cannot be recorded,
  the result (verified target and digest) is still printed, the JSON carries
  `"record_error": "record_failed"`, and the exit code is `1`.
* `verify` is read-only. Per projector it checks checkpoint continuity (no regression, no dead
  letter below the checkpoint, processed count equals delivered events) and event coverage (lag is
  reported, and fails only with `--require-caught-up`); across the graph it checks that every
  `source_event_id(s)` exists in the ledger and that stream heads match the stored events. Run it
  with the runner idle. `--replay-check` also replays the delivered events into a scratch target
  (`--target-*`, wipe with `--wipe-target --confirm <database>`) and compares digests with the live
  graph. The graph digest is a SHA-256 over every node and relationship; the volatile properties
  excluded from it are listed in `projection/verify.py`. The outcome is recorded as a
  `projection.rebuilt` ledger event per projector (`--no-record` skips it); re-running on an
  unchanged state appends nothing.
* `rebuild` needs an explicit target. Neo4j Community has a single user database per instance, so
  the target is a separate connection (`--target-uri`, `--target-database`, username and the
  password from the environment only), never a second database beside the live graph, and it must
  not be the live projection. It refuses a non-empty target unless `--wipe-target --confirm
  <database>`, creates the schema, replays every ledger event in ledger order through the
  registered projectors, verifies coverage, orphans and digest, and prints the verified target and
  digest. It never touches the live checkpoints. **Cutover is manual:** keep the projection runner
  stopped from the start of the rebuild until cutover (an event the old graph received after the
  replay head fails verification with `delivered_after_replay`; one still pending is only lag),
  point the API and the projector at the verified target (deploy step, see the infrastructure
  runbook), then run `verify --replay-check` against it. A plain `verify` cannot notice events the
  old graph received but the target lacks, because the checkpoints say they were delivered. `--in-place --confirm <database>` rebuilds the live graph instead. **Operator
  precondition: stop the projection runner first and keep it stopped** (the runtime keeps no lock;
  a runner maintenance flag is follow-up FU-62). The command only has best-effort guards, and
  refuses (exit `1`) on an unexpired outbox lease, an outbox row changed within
  `--runner-quiet-seconds` (default 30, `0` disables), or another connection of a projector-role
  member (only this process's own connections are excluded, so a second CLI is seen); a runner idle between polls with no connection passes them. Every in-place rebuild (and a standby rebuild with `--wipe-target`) first takes a
  session-level PostgreSQL advisory lock (key derived from `agent-context.projection.rebuild`) on the
  projector connection and holds it until it ends; a second rebuild is refused with exit `1`.
  It resets the registered
  checkpoints to `rebuilding`, wipes the graph, replays the delivered events, then under a row lock
  catches up on events delivered meanwhile and writes each checkpoint as the greater of the live and
  replayed position, with the processed count recomputed from the outbox, so a checkpoint never goes
  backwards; if a delivered event was missed it fails and asks for a rerun with the runner stopped.
  It then re-verifies the live projection. Events not yet delivered stay for the runner. Both
  modes skip events the runner dead-lettered (the live graph never held them) and list them, with
  their count, in the output and JSON (`skipped_dead_lettered`).

### Provisioning credentials

`agent-context producer register|revoke|list` and `agent-context mcp-token create|revoke|list`
write `operations.registered_producers` and `operations.mcp_tokens`. A credential is
`<prefix>.<secret>`; only an Argon2id verifier (at the cost `AGENT_CONTEXT_INGESTION__ARGON2_*`
configures) is stored, so the plaintext is shown once and never logged.

```bash
agent-context producer register --producer-id codex-laptop --expires-in 90 --output ~/.codex-token
agent-context producer register --producer-id codex-laptop --expires-in 90 --rotate   # replaces it
agent-context producer revoke codex-laptop
agent-context mcp-token create --principal codex-reader --scope memory:read --expires-in 90
agent-context mcp-token revoke mcp_AbCd1234xyz            # prefix from `mcp-token list`
```

* **Delivery.** Without `--output` the token is the only thing on stdout (`$(...)` captures it;
  the id, prefix, scope and expiry go to stderr). `--json` prints one document that includes the
  token, or `output` (the path) instead with `--output`. `--output` creates a NEW file with mode
  `0600`, refuses an existing path or a path inside a git work tree, and removes it again if
  provisioning fails.
* **Namespaces.** Producer tokens start with `prd_`, MCP tokens with `mcp_` (a CHECK on
  `operations.mcp_tokens`; `registered_producers` has no prefix CHECK, so `prd_` is enforced here).
  Neither is accepted on the other plane. `--expires-in` is 1-3650 days.
* **`register`** refuses an active or explicitly revoked id without `--rotate` (exit `2`); `--rotate` keeps the
  id and `created_at` and replaces the prefix, verifier and expiry. An expired-only id is replaced
  without `--rotate`. The token is delivered (written to `--output`, or flushed to stdout) BEFORE the database commit: if
  delivery fails (disk full, broken pipe) the transaction rolls back, nothing changes and a rotated
  producer keeps its previous credential (exit `1`). If the commit itself fails after delivery, the
  `--output` file is removed (only if it is still the one created) and the error names the prefix
  and the recovery: `producer register ... --rotate` for a producer, `mcp-token revoke <prefix>`
  then a new `mcp-token create` for an MCP token (the commit outcome may be ambiguous). `revoke` sets `revoked_at` (the row stays); an MCP replica may keep serving a
  revoked token for up to `mcp.principal_cache_ttl_seconds` (default 30 s).
* **Database role.** These rows are written by an operator connection,
  `AGENT_CONTEXT_POSTGRESQL__ADMIN_DSN` (falls back to `AGENT_CONTEXT_POSTGRESQL__DSN`). The
  migrations grant the API role only `SELECT` on both tables (plus a column `UPDATE` of
  `last_used_at`/`updated_at` on producers) and the projector role nothing, so the admin
  connection must be the schema owner or a role granted `SELECT, INSERT, UPDATE` on them. The
  grants are checked before any write; a missing one exits `1` naming it. The DSN is never printed.

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
