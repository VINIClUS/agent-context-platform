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

`GET /health/ready` does: it checks, concurrently and each within two seconds, that PostgreSQL
answers and `public.alembic_version` holds exactly the Alembic head this code ships, that Neo4j
accepts a connectivity check and that the S3 bucket answers `HeadBucket`. It answers `200
{"status":"ready"}` or `503 {"status":"not_ready","components":{...}}` with only the component
names (`postgresql`, `schema`, `neo4j`, `s3`) and coarse states (`ok`, `unconfigured`,
`unavailable`, `timeout`, `forbidden`, `mismatch`, `unknown`), never a host, DSN or error text,
and always with `Cache-Control: no-store`. It only reads (one `SELECT` in a read-only
transaction) and concurrent requests share a probe while it is younger than the timeout. The
bound holds even when a dependency vanishes mid-request: a timed-out check is cancelled but never
awaited (at most four are kept per check), and PostgreSQL is reached over readiness's own unpooled
engine with libpq connect, keepalive and TCP user timeouts plus a statement timeout, so a dead
socket left by a failover is never reused. The API's login role, a member of
`agent_context_api`, needs `SELECT` on `public.alembic_version`, `projection.projection_checkpoints`
and `projection.dead_letters` (the schema head, and projection progress for freshness); migration
`20261022_0001` grants exactly these to `agent_context_api`, so a migrated database needs no extra
grant. Readiness checks all three: if one is missing the schema is reported `forbidden` (not ready),
never assumed current.

Two consequences of these rules are deliberate. Readiness requires the *exact* head, so a
replica running an older image leaves rotation as soon as a newer image's migration commits
(even a purely additive one), and an image-only rollback after a migration never becomes ready:
roll back to an image whose head matches the database, or restore. And the API requires Neo4j
although only MCP retrieval reads it, because the MCP tools query the graph (PLATFORM-046); an
API without Neo4j would answer MCP calls it cannot serve.

## Codex OTLP receiver

`POST /v1/otel/traces` accepts Codex trace-safe OTLP/HTTP exports (protobuf) from the collector,
authenticated with an `events:ingest` producer token, and appends the mapped events to the ledger.
It answers 503 until `AGENT_CONTEXT_OTEL_RECEIVER__IDEMPOTENCY_HMAC_KEY` is set. Limits, records and
the collector configuration are in
[docs/data/codex-otel-mapping.md](docs/data/codex-otel-mapping.md#receiver).

## Telemetry

The platform exports its own OpenTelemetry traces and metrics over OTLP/HTTP when
`AGENT_CONTEXT_OBSERVABILITY__OTLP_ENDPOINT` (or a standard `OTEL_EXPORTER_OTLP_*ENDPOINT`) is set,
and nothing otherwise. The instruments, attributes and privacy rules are the contract in
[docs/operations/telemetry.md](docs/operations/telemetry.md).

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

* **Rebuild required on upgrade.** Checkpoints are per `(projector, version)` and a delivered
  outbox row is never claimed again, so a projector version deployed over a populated graph only
  sees the events delivered after it. The `knowledge` and `quality` projectors are version `2`:
  they add the `DecisionVersion` and `FailureObservation` history nodes and the
  `project_id`/`repository_id`/`recorded_at` run properties the temporal reads
  (`retrieval/temporal.py`) need, and after an upgrade every older entity lacks some or all of
  that history. The graph therefore carries a projection history marker
  (`(:ProjectionHistory {marker_id: "ledger"})`, `projection/schema.py`) naming the projector
  versions that projected it from the START of the ledger. Only two things record it: a rebuild
  (in place or into a standby) once its full replay has verified, and `projection run` when it
  starts on an empty graph before any outbox row was delivered. A wipe deletes the marker in its
  own transaction BEFORE any other node, and the graph digest leaves it out. At every start the
  worker also narrows the marker to the projector versions it runs, so rolling a projector back
  and forward again drops the old claim. `TemporalService` checks the marker in every read
  transaction, before and again after reading the data (reads are read-committed, so a wipe that
  starts meanwhile is seen), and raises `TemporalProjectionOutdated` unless it names the current
  `knowledge` and `quality` versions (`projection.schema.REQUIRED_HISTORY`); it never serves
  partial history. `projection status` and `projection verify` print
  `history: complete|outdated|missing` against that same set (JSON `history.state`, with the
  versions not replayed from the start in `history.missing`, and other registered projectors in
  `history.others_missing`, for information only); it is reported, not a verification failure
  (an upgrade without a rebuild already fails `verify` with `processed_count_mismatch`).
  **Operator procedure on upgrade:** stop the projection runner, deploy, run
  `projection rebuild --in-place --confirm <database>` (or a standby rebuild and cutover), check
  that `projection status` prints `history: complete for temporal reads`, then start the runner.
  **Also rebuild after restoring a Neo4j dump** (the restored marker is only true if the dump and
  the ledger agree) **and after rolling a projector version back.** The temporal reads use
  `CALL (...) { ... }` subqueries and need Neo4j 5.23 or later (tests pin 5.26).
* **Running the projector.** `agent-context projection run` is the worker process: it drives the
  existing `ProjectionRunner` over the registered projectors with the projector-role connection
  (`AGENT_CONTEXT_POSTGRESQL__PROJECTOR_DSN`, falling back to `__DSN`) and the live Neo4j settings.
  Without `--once` it polls (idle polls back off to 5 s; `--poll-seconds`, `--batch-size`) until
  SIGTERM or SIGINT, then stops claiming, finishes the batch in hand, releases any lease it still
  holds, closes its connections and exits `0`. A progress line (counts, lag, dead letters; never
  content) goes to stderr at most every `--log-seconds`. `--once` drains the outbox, waiting out
  retry delays so a poison event reaches the dead-letter queue, then prints a summary (claimed,
  delivered, retried, dead-lettered, per-projector checkpoints; `--json` for JSON on stdout) and
  exits `0`; it exits `1` if any event was dead-lettered during the run, or when `--max-seconds`
  (approximate: checked once per loop iteration) expires (`outcome: timeout`), or when SIGTERM/SIGINT interrupts it before it has drained (`outcome: interrupted`), and `2` on a usage error. E2E scenarios use `run --once` to project
  deterministically. The worker takes the rebuild advisory lock
  SHARED (`pg_try_advisory_lock_shared`, same key as `rebuild`) before each claim and holds it
  until that batch is finalized, so a claim never overlaps a rebuild. While a rebuild holds the
  lock, nothing is claimed and `paused_for_rebuild` is logged; claiming resumes when it is
  released. Conversely a rebuild started while a batch is in flight is refused ("another rebuild
  holds the lock", exit `1`) and can simply be retried.
  `--health-file <path>` is touched after every loop iteration (a paused one too) and removed on
  shutdown. For a container healthcheck, use the file age; a Compose service with the default
  `--poll-seconds` can use
  `healthcheck: {test: ["CMD-SHELL", "test -n \"$(find /tmp/projector.health -mmin -1)\""], interval: 30s}`
  (healthy while the file was touched in the last minute; allow longer if one batch can take that
  long). There is no HTTP server.
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

### Indexing a checkout

`agent-context index --repository-id <id> --checkout <path> [--scip <file>] [--json]` indexes one
git checkout into the ledger as the indexer producer (`agent-context-platform-indexer`), through the
API-role connection (`AGENT_CONTEXT_POSTGRESQL__API_DSN`, falling back to `__DSN`). The same
composition is `agent_context_platform.indexing.pipeline.index_checkout` for in-process callers.

* **Target.** A clean checkout indexes its HEAD commit; a dirty one indexes the workspace snapshot.
  Re-indexing an unchanged target submits nothing (`submitted 0`, every event `already present`).
* **Lineage comes from history.** A file's logical ID is derived from the first-parent history of
  HEAD (`indexing/lineage.py`): the commit that added the origin path, kept across edits and exact
  renames, new after a delete and recreate. Nothing is persisted, so every checkout and every
  starting commit agrees. A shallow clone fails with `shallow_history` and a history above 100,000
  first-parent commits with `history_too_large`; no lineage is ever invented. Untracked and
  dirty-only files get provisional IDs until they are committed.
* **SCIP.** `--scip` is an index of the checked-out HEAD commit; evidence for files that differ from
  it is dropped and counted.
* **Sandbox.** Structural adapters run confined by Landlock, with the checkout added to the
  readable-nowhere roots. Without Landlock the run fails unless
  `AGENT_CONTEXT_INDEXER_ALLOW_UNCONFINED_ADAPTERS=true` (development only).
* **Output and exit codes.** index id, target, events submitted and already present, files indexed
  and not indexed, the completion `error_class` and a diagnostics summary (counts only: no file
  content, path, DSN or secret). Exit `0` on success, `1` when the index failed or was refused
  (`success=false`, `snapshot_incomplete`, `shallow_history`...; the counts are still printed), `2`
  on a usage error.

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

### Content maintenance, token probe and secret files

`agent-context content sweep-orphans [--min-age SECONDS] [--dry-run] [--json]` deletes stored
objects no catalog row references once older than the 24-hour floor (counts only).
`GET /v1/auth/whoami` answers `204` and `X-Agent-Context-Token-Kind: producer|mcp` for a valid
token and writes nothing. `AGENT_CONTEXT_SECRETS_DIR` reads secret settings from `0400` files named
after their variables (a variable both exported and mounted refuses to start). A stored object
that fails verification raises the `content_integrity_failure` log line and the
`integrity_failures` metric and answers a retryable `503` (`Retry-After: 60`); it is never repaired
automatically. See
[docs/operations/content-integrity.md](docs/operations/content-integrity.md).

### Migrating the schema

`agent-context db upgrade [--expect-head <rev>] [--lock-timeout <seconds>] [--json]` upgrades the
database to the single Alembic head shipped with the code, through
`AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN` (the schema owner; falls back to `__DSN`). It runs
Alembic in-process on one connection holding a session-level advisory lock for the whole run, so
two migrate jobs serialize: the second waits (at most `--lock-timeout`, default 600 s) and then
finds the schema at head and changes nothing. It asserts the resulting revision is exactly the
head, and `--expect-head` fails before connecting unless it names that head. A database at a
revision the code does not ship (migrated by a newer image) is refused, never downgraded.
`agent-context db current [--json]` prints the revision against the head (`at_head`, `behind`,
`empty` or `unknown`) and takes no lock. Both find `alembic.ini` through `--alembic-config` or
`AGENT_CONTEXT_ALEMBIC_CONFIG` (default `./alembic.ini`) and never print the DSN.
`--lock-timeout` must be finite, positive and at most 2147483 s (PostgreSQL's `lock_timeout`).
SIGTERM or SIGINT (`docker stop` on the migrate container) cancels the run: the lock wait or the
single migration transaction is abandoned and the connection closed, and the command exits
`1` with `interrupted` (`db current` shows where the schema is). Exit codes: `0` on success
(`current`: at head); `1` on a failed or mismatched migration, a lock timeout, an interruption
or a schema not at head; `2` on a usage error (bad option, missing DSN or Alembic
configuration) or the downgrade refusal.

## Container image

`containers/platform/Dockerfile` builds the one immutable image every platform role runs (the
INFRA-040 services): a pinned `python:3.12-slim` digest, the locked environment from
`uv sync --frozen --no-dev --no-editable` with the verified private SDK wheel, `alembic.ini` and
`alembic/`, and a stdlib health probe. It adds no compiler, curl, git or shell tooling, runs as
the dedicated numeric user `10002:10002` (distinct from the indexer image's `10001`), and its
default command is exec-form.

```bash
docker build -f containers/platform/Dockerfile \
  --build-arg REVISION="$(git rev-parse HEAD)" --build-arg VERSION=0.1.0 \
  --build-arg SDK_VERSION=0.6.0 --build-arg ALEMBIC_HEAD=20261022_0001 \
  -t agent-context-platform:dev .
```

The build arguments fill the labels `org.opencontainers.image.revision`,
`org.opencontainers.image.version`, `io.agent-context.sdk.version` and
`io.agent-context.alembic.head`; each one that is set is checked against what the image actually
contains, and the build fails on a mismatch (only `VERSION` may carry a leading `v`, as a tag
does). Unset, they read `unknown`/`dev` (local builds).

| Role | Command | Environment |
| --- | --- | --- |
| `platform-migrate` (one-shot, before the API) | `agent-context db upgrade --expect-head <io.agent-context.alembic.head>` | `AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN` |
| `platform-api` (2 replicas) | default: `uvicorn agent_context_platform.app:create_app --factory --host 0.0.0.0 --port 8000 --no-access-log --no-server-header` | `AGENT_CONTEXT_ENVIRONMENT=production`, `__POSTGRESQL__DSN` (API login), `__NEO4J__*`, `__S3__*`, `__MCP__TOKEN_HMAC_KEY`, `__MCP__CURSOR_HMAC_KEY` (the same on every replica), `__MCP__ALLOWED_HOSTS`/`__ALLOWED_ORIGINS` |
| `platform-projector` | `agent-context projection run` | `__POSTGRESQL__PROJECTOR_DSN`, `__NEO4J__*`, `__S3__*`, and `__SEARCH__MODEL_DIR` (model volume) or `AGENT_CONTEXT_PROJECTION_LEXICAL_ONLY=1` (see Search and embeddings) |
| `platform-indexer` | `agent-context index --repository-id <id> --checkout /work` | `__POSTGRESQL__API_DSN`; run with `user: "10001:10001"` |

Every serving role logs in as a LOGIN member (`INHERIT`) of the migrations' group role and needs
no grant of its own: the API login of `agent_context_api` (whose reads include
`public.alembic_version`, `projection.projection_checkpoints` and `projection.dead_letters`, see
`/health/ready`), the projector login of `agent_context_projector`. The migrate role needs only its
DSN, as the schema owner: it does not set `AGENT_CONTEXT_ENVIRONMENT=production`,
whose completeness check covers the serving roles. Only the API serves HTTP, so the image declares
no `HEALTHCHECK`; the API's compose service uses the stdlib probe (no curl, no shell):
`test: ["CMD", "python", "/app/healthcheck.py", "/health/ready"]` (`/health/live` is the
default path, port `8000` the default port). Gate the API on `service_completed_successfully` of
`platform-migrate`. The structural indexer's adapter sandbox relies on `RLIMIT_NPROC`, which is
per UID, so the indexer role runs as the indexer image's dedicated `10001`, which no other role
uses (or runs `containers/indexer/Dockerfile` itself, with its documented flags and a read-only
`/work` mount).

Every role runs with a read-only root filesystem and a scratch tmpfs; the image needs no
writable path besides `/tmp` and no capability:

```bash
docker run --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m --cap-drop ALL \
  --security-opt no-new-privileges --pids-limit 256 --memory 1g agent-context-platform:dev
```

Semantic-search model files (`AGENT_CONTEXT_SEARCH__MODEL_DIR`, PLATFORM-042) are not baked into
the image: provision them into a read-only volume and point the variable at its mount.

`tests/container/test_platform_image.py` (the `Platform image` CI job) checks the user, the
read-only boot, dropped capabilities, the labels, the exec-form command, the absence of compilers
and network tools, `agent-context --help`, the shipped head and `/health/live` inside the
container. Run it with `AGENT_CONTEXT_TEST_CONTAINER=1` (and `AGENT_CONTEXT_TEST_PLATFORM_IMAGE`
to test an already built tag). A `v*` tag runs `.github/workflows/release-image.yml`, which checks
that the tag is `v<project version>` and the lockfile, runs the unit and contract tests, builds
with the labels, runs the same container tests, pushes `ghcr.io/viniclus/agent-context-platform:<tag>` and
prints the digest; infra pins that digest (`image: ghcr.io/viniclus/agent-context-platform@sha256:...`)
in its `versions.lock`, never the tag, and passes the `io.agent-context.alembic.head` label to
`--expect-head`.

## Search and embeddings

Retrieval over sanitized content has two projections, both rebuildable from the ledger and never a
source of truth: PostgreSQL full-text rows (`retrieval.search_documents`, a `simple`-configuration
`tsvector` with a GIN index; the text itself is not stored twice) and 384-dimension vectors on
Neo4j `ContentEmbedding` nodes behind a cosine vector index. The `search` projector writes both for
every event that references sanitized content and removes both on `content.purged`. Search is
always scoped to a project and a repository, bounded to 100 results and runs under a deadline
(`retrieval/search.py`).

Embeddings come from the multilingual MiniLM model run on `onnxruntime` (no torch). The model is
pinned by revision and file digests in `src/agent_context_platform/retrieval/models.lock.json`.
The service and the tests never download it; fetch it once and point the service at it:

```bash
agent-context models fetch --dest /var/lib/agent-context/models/minilm   # about 470 MB, sha256-checked
export AGENT_CONTEXT_SEARCH__MODEL_DIR=/var/lib/agent-context/models/minilm
```

Loading re-checks every digest and fails closed on a mismatch. `AGENT_CONTEXT_SEARCH__EMBEDDING_BATCH_SIZE`
and `..._EMBEDDING_THREADS` bound the inference work.

What needs the model:

* `projection run` (the worker) refuses to start without `MODEL_DIR` (exit `2`, before any
  connection) unless it runs **lexical-only**: `--lexical-only` or
  `AGENT_CONTEXT_PROJECTION_LEXICAL_ONLY=1` (refused while `MODEL_DIR` is set). Lexical-only reads
  the content (S3 settings needed) and writes the search rows with every check a full index runs:
  tombstones, scope and content identity, readable bytes. A conflict or unreadable content is
  therefore dead-lettered live exactly as with a model, and lexical search and purges work at
  once. Only the `ContentEmbedding` nodes are missing; the worker counts them (`search_unembedded`
  in the summary, a `projection_worker search_unembedded N` log line), and `projection status`
  reports them durably (`search: N documents, M embeddings (K without embeddings ...)`; the search
  checkpoint itself shows no lag, since those events were delivered).
* Backfilling embeddings after lexical-only: run a rebuild with `MODEL_DIR` set. Either
  `projection rebuild --in-place` (stop the worker first: it resets the search tables and the live
  graph and replays the delivered events), or a standby rebuild into another Neo4j instance and
  cutover (the rows already exist in PostgreSQL; the standby graph gets the embeddings). Then run
  the worker with the model.
* `projection rebuild` and `projection verify --replay-check` refuse to run without `MODEL_DIR`
  unless `--without-search` (or `AGENT_CONTEXT_PROJECTION_WITHOUT_SEARCH=1`, which the worker
  ignores) skips the search projector. `--without-search` is itself refused (in place, or for a
  replay check) while the live graph holds `ContentEmbedding` nodes: they would be wiped, or
  reported as drift. Plain `projection verify` never loads the model; `status` never does either.
* `projection rebuild --in-place` first reads every document the replay will index, read-only and
  before it takes the rebuild lock, touches a checkpoint, resets a table or wipes the graph. It
  skips content the ledger records as purged and fails with `SearchReplayIntegrityError` (counts
  only) if any other content cannot be read or one content ID names different content. This costs
  one extra read of all indexed content. Then `retrieval.reset_search_projection()` (a SECURITY
  DEFINER function; the projector role holds no TRUNCATE and cannot delete tombstones) empties
  the documents and reseeds the tombstones from every purge in the ledger, whatever its outbox
  status. Purged content therefore stays purged across rebuilds, even when its purge event was
  dead-lettered. The reset locks in the worker's order under a 10 s lock timeout.
* Recovery: if an in-place rebuild still fails after the wipe (a transient Neo4j, PostgreSQL or
  S3 error), the checkpoints stay `rebuilding` and the worker must stay stopped; fix the cause
  and run the same `projection rebuild --in-place` again. It starts from zero. Not transient:
  content bytes that vanish without a `content.purged` between the check and the replay (storage
  loss during the rebuild) fail the replay after the wipe, and the rerun is then refused by its
  check. Restore the bytes, or purge the content through the ledger, then rerun; until then live
  search (and the graph) stays partial. The check holds one PostgreSQL transaction per page of
  500 delivered events while it reads their content serially.
* `projection status` reports `documents_without_embeddings` as rows minus nodes: an
  approximation (hidden rows of undelivered events and stale nodes left by a purge race count
  too), enough to show that a backfill is due, not an exact list.
* Ownership: `retrieval.reset_search_projection()` runs with its owner's rights, the role that
  ran the migrations. It is safe as written (fixed statements, pinned `search_path`, EXECUTE for
  the projector role only), but infra should still run migrations as a dedicated non-superuser
  schema owner (`AGENT_CONTEXT_POSTGRESQL__MIGRATION_DSN`), never a superuser.

Consistency: a search row is returned only once one of its source events is `delivered`, and a
hit's provenance (event, session, time, source event IDs) comes from delivered events only: the
earliest one, as a rebuild records it. A row left behind by a dead-lettered or not-yet-delivered
event stays hidden. Indexing and purging of one content ID serialize on a PostgreSQL advisory
lock. A producer-chosen `content_id` reused for different sanitized bytes (or in another scope) is
refused (`SearchContentConflictError`, `SearchScopeConflictError`: retried, then dead-lettered),
never merged.

Retention contract (design section 16): every removal of stored content must go through the
ledger as a `content.purged` event; replays treat content that is gone without one as an integrity
failure. Retention expiry (180 days for session content) is not implemented yet. When it is, it
must emit `content.purged` for what it deletes, or the next in-place rebuild stops at its check
(follow-up).

Containers (service image): the worker container sets `AGENT_CONTEXT_SEARCH__MODEL_DIR` to a
read-only volume the model was provisioned into once with `agent-context models fetch --dest`
(about 470 MB; never at image build or start-up). Without a provisioned model it runs
lexical-only with `AGENT_CONTEXT_PROJECTION_LEXICAL_ONLY=1` (and no `MODEL_DIR`). Either way it
needs the S3 settings, because indexing reads the content. The image needs the `onnxruntime`,
`tokenizers` and `numpy` wheels (no torch). A process serving vector search
(`SearchService.vector`, wired by PLATFORM-043) embeds the query, so it needs the same volume;
lexical search needs only PostgreSQL. Rebuild and replay-check jobs need the volume too, or
`--without-search` as above. `tests/unit/retrieval/test_model_parity.py`
compares the ONNX output with sentence-transformers and runs only when
`AGENT_CONTEXT_TEST_MODEL_PARITY=1`.

## Context packages

`retrieval/composer.py` (`ContextComposer`) answers the five memory tools (`context_for_task`,
`repository_overview`, `symbol_context`, `decision_history`, `failure_history`) with an SDK
`ContextPackageV1`. It resolves the explicit scope first (unknown repository, project, revision
or symbol: a typed error, never a wider answer), reads code as indexed at the requested commit or
workspace snapshot, fuses graph, temporal and lexical/vector candidates deterministically, and
enforces the item, byte and token budget (`ceil(bytes / 4)`). Every item cites at least one stored
event; the evidence form, including the metadata-only form of events that carry no content, is
documented in `docs/data/context-evidence.md`.

Pagination cursors are opaque and HMAC-signed, so any replica can continue a page another one
issued. Set the shared key (at least 32 characters, required in production):

```bash
export AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY=...   # from the secret store, the same on every replica
```

A cursor is valid for the same tool, input, principal and projection snapshot until it expires
(15 minutes); once the projection moves (a new event delivered to a projector the answer depends
on) the client starts again from the first page.

The composer runs as the API role (never the projector role, which can write projection state).
`FreshnessReader` also reads projection checkpoints and dead letters (for `projection_checkpoint`,
`staleness_ms` and the warnings) through the SELECT grants of migration `20261022_0001`
(`API_READ_TABLES` in `operations/readiness.py`); without one of them a request fails closed with
`ContextUnavailable`. The integration and evaluation suites run every composer read as a LOGIN
member of `agent_context_api` with no grant of its own.

`query_id`, `generated_at`, `staleness_ms` and `next_cursor` (it carries an expiry) are the only
fields that depend on the wall clock: everything else is a function of the request and the
projection snapshot.

`tests/evaluation/` holds the context-quality evaluation: 30 golden questions (6 per tool,
`questions.json`) over a world seeded through the real ingestion service and projection runner.
It needs the integration services and asserts provenance, budgets, zero unsupported claims, zero
temporal leaks, required-evidence recall (90% overall, 80% per tool) and byte-identical answers
after an empty-graph replay:

```bash
uv run pytest -q tests/evaluation
```

## MCP memory tools

The MCP documentation set is [docs/mcp/README.md](docs/mcp/README.md): connecting Codex, the
protocol profile, the tool contracts, security, deployment and scaling, conformance and legacy
compatibility.

`POST /mcp` (MCP `2026-07-28`, stateless: no `initialize`, no `Mcp-Session-Id`, no cookie; a
direct `tools/call` needs no prior `server/discover`) serves exactly five read-only tools, each
bound to the `ContextComposer` method of the same name (`mcp/tools.py`): `context_for_task`,
`repository_overview`, `symbol_context`, `decision_history` and `failure_history`. Their
`inputSchema`/`outputSchema` are the SDK's published `mcp.<tool>.{input,output}.v1.json` schemas;
`tools/list` is the same on every replica (sorted, `ttlMs` 60000, `cacheScope: private`). Every
tool is annotated `readOnlyHint: true`, `destructiveHint: false`, `idempotentHint: true`,
`openWorldHint: false`, and says why in its description. No input takes SQL, Cypher, a path or a
URL, and paging is the explicit, signed `cursor` (see Context packages).

* **Auth.** A bearer `mcp_...` token with the `memory:read` scope (401 without, 403 with another
  scope, 429 over the per-principal rate). In this MVP that single scope reads every project and
  repository of the installation (design spec §14.5); per-project authorization is future work.
  The token's principal is used only to bind cursors to their caller, never to narrow a query,
  and no argument can name a principal.
* **Results.** `structuredContent` is the `ContextPackageV1`, valid against the output schema, and
  `resultType` is `complete`. The text block is a short summary that names items only by a
  bounded, JSON-quoted title; item titles and content are untrusted evidence.
* **Errors.** An unknown tool is a JSON-RPC `-32602`. Everything else is a tool error
  (`isError: true`) whose text is `{"error": {"code", "message", "retryable"}}`, with a fixed
  message and one of these codes: `invalid_arguments` (field names only, never values; also
  free text over 1000 characters and IDs over 512),
  `invalid_request`, `scope_not_found`, `revision_not_found`, `symbol_not_found`,
  `ambiguous_symbol`, `budget_too_small`, `cursor_invalid`, `cursor_expired`, `cursor_mismatch`,
  `cursor_stale` (start again from the first page), `temporal_projection_outdated`,
  `projection_not_ready`, `deadline_exceeded`, `context_unavailable`, `unauthenticated` and
  `internal_error`. `retryable` is true only for `projection_not_ready`, `deadline_exceeded` and
  `context_unavailable`.
* **No resources.** `resources/*` and prompts are not served and no resource links are emitted.
  The `resource_uri` of an evidence reference is a stable name, not a capability
  (`docs/data/context-evidence.md`): dereferencing it would need a reader that re-checks scope,
  delivery and purge and serves content beyond the bounded excerpts, which does not exist yet.
* **Wiring.** At start-up the API builds the composer from `AGENT_CONTEXT_POSTGRESQL__API_DSN`
  (falling back to `__DSN`; the API role, never the projector role) on its own bounded pool (5
  connections plus 5 overflow, apart from the ingestion and token-lookup pool), `__NEO4J__*`
  (connect and retry bounds shortened to 2 s, so an unreachable or misconfigured graph answers
  `context_unavailable` quickly instead of waiting out the 10 s deadline), `__S3__*`,
  `__MCP__CURSOR_HMAC_KEY` (required in production; blank means unset, and outside production a
  random per-process key is used) and, for vector search, `__SEARCH__MODEL_DIR` (without it the
  tools search lexically only, with no warning). Until all of PostgreSQL, Neo4j and S3 are
  configured every call answers `context_unavailable`.

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

## Crash-test fault points

Test images can arm documented crash points (`AGENT_CONTEXT_FAULT_INJECTION__*`) that kill the API or
the projection worker with exit code 137 at fixed boundaries; settings refuse them in production.
See `docs/operations/fault-injection.md` for the labels, the variables and the recovery contract.

## Development workflow

Create feature worktrees as siblings of repository checkouts, using an immutable SHA captured from
the required base. Branches follow `agent/<lowercase-task-id>-<slug>`, such as
`agent/platform-001-fastapi-quality-scaffold`.

Keep each task within its declared repository and paths. Use Conventional Commits, run the targeted
test during development, and run every quality gate before requesting integration.
