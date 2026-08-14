# Agent Context Codex Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture supported public Codex CLI activity safely, preserve it through offline operation, upload it idempotently, and install the modern Agent Context MCP memory connection.

**Architecture:** Every capture surface is translated into SDK event drafts, redacted locally, enriched with bounded Git/worktree evidence, and committed to a protected SQLite spool before any network access. A separate uploader drains leases to the platform API. Codex hooks and `codex exec --json` share the same pure mapping/redaction pipeline. The repository configures the remote MCP server but does not implement MCP wire behavior.

**Tech Stack:** Python 3.12+, uv, Pydantic v2, `agent-context-sdk`, httpx, SQLite, tomlkit, platformdirs, OpenTelemetry configuration generation, pytest, Hypothesis, Ruff and mypy.

## Global constraints

- Distribution name is `agent-context-codex`; import package is `agent_context_codex`; CLI is `agent-context-codex`.
- First supported client is Codex CLI `0.147.0`; newer versions require capability detection, not blind version assumptions.
- Only documented hook input and `codex exec --json` fields are accepted as public capture surfaces.
- `transcript_path` is ignored and never opened because its format is explicitly unstable.
- Hidden reasoning, raw chain of thought and internal rollout files are never ingested.
- Redaction completes before SQLite, HTTP, logs, metrics or exception rendering.
- Redaction failure produces an allowlisted metadata-only event; it never fails open with content.
- Hook failures are fail-open for Codex: normal coding continues even when local capture or the platform is unavailable.
- Hook commands emit no model-visible output. They are observational, not policy hooks.
- SQLite files use a private directory mode `0700`, file mode `0600`, and an encrypted host volume. The application never claims SQLite-native encryption.
- No shell interpolation is used for Git, Codex or installer subprocesses.
- Upload and memory tokens are distinct: `AGENT_CONTEXT_INGEST_TOKEN` has `events:ingest`, while `AGENT_CONTEXT_MCP_TOKEN` has `memory:read`. Neither is written into TOML, hook files, logs or diagnostics.
- MCP configuration uses Streamable HTTP plus `bearer_token_env_var`. The adapter never implements `initialize`, `Mcp-Session-Id` or a local proxy.
- Root exports, `uv.lock`, CLI command registration and plugin manifest have one integration owner.

---

## File map

```text
agent-context-codex/
├── pyproject.toml
├── uv.lock
├── src/agent_context_codex/
│   ├── __init__.py
│   ├── cli.py
│   ├── settings.py
│   ├── clock.py
│   ├── capture/
│   │   ├── hooks/
│   │   │   ├── input.py
│   │   │   ├── mapper.py
│   │   │   └── runner.py
│   │   ├── jsonl/
│   │   │   ├── input.py
│   │   │   ├── mapper.py
│   │   │   └── reader.py
│   │   └── pipeline.py
│   ├── enrichment/
│   │   └── git.py
│   ├── safety/
│   │   ├── boundary.py
│   │   └── canaries.py
│   ├── spool/
│   │   ├── database.py
│   │   ├── migrations.py
│   │   ├── models.py
│   │   ├── queue.py
│   │   └── pressure.py
│   ├── transport/
│   │   ├── client.py
│   │   └── worker.py
│   ├── install/
│   │   ├── capabilities.py
│   │   ├── hooks.py
│   │   ├── mcp.py
│   │   ├── atomic.py
│   │   └── doctor.py
│   └── telemetry/
│       └── config.py
├── plugin/
│   ├── .codex-plugin/plugin.json
│   ├── hooks/hooks.json
│   └── skills/agent-context-memory/SKILL.md
├── tests/
│   ├── fixtures/hooks/
│   ├── fixtures/jsonl/
│   ├── capture/
│   ├── enrichment/
│   ├── spool/
│   ├── transport/
│   ├── install/
│   └── e2e/
└── docs/
```

## Task CODEX-001: Package and quality scaffold

**Files:** `pyproject.toml`, `src/agent_context_codex/__init__.py`, `src/agent_context_codex/cli.py`, `tests/test_package.py`, `.github/workflows/quality.yml`, `README.md`.

**Interfaces:** Produces importable package, `agent-context-codex --version`, and empty Typer command tree.

- [ ] Write a smoke test asserting package and CLI version `0.1.0`.
- [ ] Run `uv run pytest tests/test_package.py -q` and observe the missing package failure.
- [ ] Configure Python `>=3.12,<3.15`, strict mypy, Ruff line length 100 and pytest coverage floor 90%.
- [ ] Add runtime dependencies Typer and platformdirs; defer all other dependencies to their owning tasks.
- [ ] Run `uv lock`, lint, format, typing and tests.
- [ ] Commit `chore: scaffold Codex context adapter`.

## Task CODEX-010: Runtime settings and SDK boundary

**Files:** `src/agent_context_codex/settings.py`, `src/agent_context_codex/clock.py`, `tests/test_settings.py`, `tests/test_clock.py`, `pyproject.toml`, `uv.lock`.

**Interfaces:** Produces frozen `AdapterSettings`, `SystemClock`, `FrozenClock`; consumes exact `agent-context-sdk==0.1.0`.

- [ ] Test precedence `CLI argument > AGENT_CONTEXT_* environment > config file > default` without ever rendering secret values.
- [ ] Define platform URL, producer ID, separate ingestion/MCP token environment-variable names, spool path, batch size, timeouts, maximum request bytes, retention days and spool byte ceiling.
- [ ] Default spool path to the platformdirs user-data location, retention to seven days, maximum size to 2 GiB and upload batch to 100 events.
- [ ] Validate HTTPS except for loopback test URLs. Validate token by environment-variable name, never by token value in a settings file.
- [ ] Add deterministic clock injection for event and lease tests.
- [ ] Run targeted tests and commit `feat: define Codex adapter runtime boundary`.

## Task CODEX-011: Public Codex hook mapper

**Files:** `src/agent_context_codex/capture/hooks/input.py`, `src/agent_context_codex/capture/hooks/mapper.py`, `tests/fixtures/hooks/*.json`, `tests/capture/test_hook_input.py`, `tests/capture/test_hook_mapper.py`, `docs/public-capture-surfaces.md`.

**Interfaces:** Produces `parse_hook_input(raw) -> PublicHookInput` and `map_hook(input, observed_at) -> CaptureCandidate`; consumes SDK event payloads.

- [ ] Create documented fixtures for `SessionStart`, `UserPromptSubmit`, `PreToolUse`, `PermissionRequest`, `PostToolUse`, `PreCompact`, `PostCompact`, `SubagentStart`, `SubagentStop`, `Stop` and `SessionEnd`.
- [ ] Test shared fields `session_id`, `cwd`, `hook_event_name`, `model`, optional `turn_id` and documented permission mode. Preserve only event-specific fields in the official hook schema.
- [ ] Install a test filesystem guard that fails if mapping attempts to open `transcript_path`; include a real-looking path in every fixture.
- [ ] Treat unknown input keys as forward-compatible source metadata: ignore them and count them, but never copy their values into events.
- [ ] Derive stable idempotency keys from source, session/turn/tool native IDs, hook name and sanitized payload digest. Do not use arrival order.
- [ ] Map unsupported future hook names to allowlisted metadata event `agent.source.unmapped` instead of serializing the raw object.
- [ ] Assert no mapper output contains the fixture transcript path or hidden reasoning canary.
- [ ] Run `uv run pytest tests/capture/test_hook_input.py tests/capture/test_hook_mapper.py -q` and commit `feat: map public Codex hook events`.

## Task CODEX-012: Local redaction boundary

**Files:** `src/agent_context_codex/safety/boundary.py`, `src/agent_context_codex/safety/canaries.py`, `tests/safety/test_boundary.py`, `tests/safety/test_failure.py`, `tests/fixtures/secret_canaries.json`, `docs/redaction-boundary.md`.

**Interfaces:** Produces `sanitize_candidate(candidate, known_values) -> SanitizedCapture` and `metadata_only(candidate, reason) -> SanitizedCapture`; consumes SDK-016 redactor.

- [ ] Test known token, private key, credential URL, structured secret field, high-entropy candidate and nested tool input/output.
- [ ] Assert sanitized content and report contain neither original values nor unsalted secret hashes.
- [ ] Inject every detector exception and out-of-memory-shaped `RedactionError`; assert the result contains only event kind, native IDs, timestamps, byte counts, detector error class and `dropped_redaction_failure`.
- [ ] Make `SanitizedCapture` the only type accepted by spool APIs using static typing and runtime validation.
- [ ] Add a recursive test scanner for events, reports, logs and exception text.
- [ ] Run redaction tests plus mypy and commit `feat: enforce local secret boundary`.

## Task CODEX-013: Protected SQLite spool

**Files:** `src/agent_context_codex/spool/database.py`, `src/agent_context_codex/spool/migrations.py`, `src/agent_context_codex/spool/models.py`, `src/agent_context_codex/spool/queue.py`, `src/agent_context_codex/spool/pressure.py`, `tests/spool/*.py`.

**Interfaces:** Produces `Spool.enqueue`, `claim_batch`, `ack`, `release`, `stats`, `apply_pressure_policy`, and `recover_expired_leases`.

- [ ] Write migration tests for a fresh file and every supported schema version; use `PRAGMA user_version` and transactionally applied migrations.
- [ ] Define queue states `pending`, `leased`, `acknowledged`, `discarded`; store canonical sanitized request fragments, attempt count, next-attempt time, lease owner/deadline and priority.
- [ ] Open the directory/file with private permissions, enable WAL, `foreign_keys=ON`, `busy_timeout`, and `synchronous=FULL`; reject symlinks and non-regular database paths.
- [ ] Use `BEGIN IMMEDIATE` to claim ordered rows atomically. Two workers must never hold the same live lease.
- [ ] Make enqueue idempotent on `(producer_id, idempotency_key)` and preserve the first canonical bytes.
- [ ] Test process death after claim, expired lease recovery, duplicate acknowledgement, corrupt-row quarantine and monotonic retry scheduling.
- [ ] Enforce seven days or 2 GiB. Discard oldest unpinned detailed content first and atomically enqueue one metadata-only discard event for each pressure action.
- [ ] Test a 32-process contention run and zero canary occurrence in the SQLite file, WAL and SHM after checkpoint.
- [ ] Commit `feat: add crash-safe protected spool`.

## Task CODEX-014: Public `codex exec --json` adapter

**Files:** `src/agent_context_codex/capture/jsonl/input.py`, `src/agent_context_codex/capture/jsonl/reader.py`, `src/agent_context_codex/capture/jsonl/mapper.py`, `tests/fixtures/jsonl/*.jsonl`, `tests/capture/test_jsonl_reader.py`, `tests/capture/test_jsonl_mapper.py`.

**Interfaces:** Produces bounded streaming `read_jsonl(stream)` and `map_jsonl_event(event) -> CaptureCandidate`.

- [ ] Capture golden public output from the minimum supported CLI for thread start, turn start/completion/failure, item start/completion, command execution, file change, MCP call and error.
- [ ] Parse line-by-line with a per-line byte ceiling; never buffer an entire run or follow referenced paths.
- [ ] Reject invalid UTF-8, duplicate JSON keys, excessive nesting and non-object roots as metadata-only source errors.
- [ ] Preserve public agent messages and tool results for redaction; explicitly drop reasoning item content while retaining allowlisted duration/status metadata.
- [ ] Derive the same canonical native IDs used by hook events so graph projection can coalesce evidence without deleting either source event.
- [ ] Test truncated last line and newer unknown event/item types.
- [ ] Run targeted tests and commit `feat: adapt Codex JSONL events`.

## Task CODEX-015: Git and dirty-worktree enrichment

**Files:** `src/agent_context_codex/enrichment/git.py`, `tests/enrichment/test_git.py`, `tests/enrichment/test_worktree.py`, `tests/fixtures/git-repository/`.

**Interfaces:** Produces `GitEvidence`, `WorkspaceSnapshotEvidence`, `inspect_workspace(cwd, limits)`.

- [ ] Test normal repository, linked worktree, detached HEAD, unborn branch, submodule, no repository, rename and dirty files.
- [ ] Run `git` with an argument vector, `shell=False`, minimal environment, fixed timeout and bounded stdout/stderr. Reject repositories whose resolved root escapes the allowed workspace.
- [ ] Capture repository root identity, remote URL after credential stripping, HEAD/base commit, branch, worktree identity, porcelain-v2 status and diff digest.
- [ ] Hash sanitized modified-file bytes and the sanitized patch. Do not store unredacted patches or invoke external diff/textconv helpers.
- [ ] Handle symlinks without following targets outside the repository and cap file count, file bytes and total bytes.
- [ ] Property-test stable snapshot digest independent of status record order.
- [ ] Commit `feat: enrich captures with workspace snapshots`.

## Task CODEX-020: Hook-to-spool pipeline

**Files:** `src/agent_context_codex/capture/pipeline.py`, `src/agent_context_codex/capture/hooks/runner.py`, `src/agent_context_codex/cli.py`, `tests/capture/test_pipeline.py`, `tests/e2e/test_hook_to_spool.py`.

**Interfaces:** Adds `agent-context-codex capture-hook`, reading one JSON object from stdin and emitting no stdout/stderr on success.

- [ ] Write an E2E test that pipes a `PostToolUse` fixture through the CLI and reads back one canonical sanitized draft plus content item from SQLite.
- [ ] Compose `parse → map → sanitize → enrich sanitized evidence → sanitize merged result → enqueue`; no stage may reverse the order around persistence.
- [ ] Bound stdin, duration and enrichment. Return exit zero for remote/spool operational failure after writing a content-free diagnostic to a private rotating local log.
- [ ] Preserve nonzero only for installer/programming errors in an explicit `--strict-test-mode`; production hook invocation never uses it.
- [ ] Return an empty output object only when Codex requires valid JSON; otherwise produce zero bytes so capture cannot inject context into the model.
- [ ] Verify concurrent hooks completing out of order still deduplicate and preserve source timestamps.
- [ ] Commit `feat: capture hooks without blocking Codex`.

## Task CODEX-021: Batch uploader and acknowledged replay

**Files:** `src/agent_context_codex/transport/client.py`, `src/agent_context_codex/transport/worker.py`, `src/agent_context_codex/cli.py`, `tests/transport/test_client.py`, `tests/transport/test_worker.py`.

**Interfaces:** Adds `agent-context-codex upload --once` and `agent-context-codex upload --watch`; consumes frozen platform OpenAPI contract.

- [ ] Contract-test exact `IngestBatchRequestV1` and `IngestBatchResponseV1`, an `Idempotency-Key` equal to the request's `batch_id` string, 1–500 event limit and content claim resolution.
- [ ] Read `AGENT_CONTEXT_INGEST_TOKEN` at request time and require the configured producer identity; reject configuration that reuses the MCP token value. Install an httpx event hook that strips authorization and bodies from diagnostics.
- [ ] Use bounded connect/read/write/pool timeouts, no implicit redirects, verified TLS and a maximum response size.
- [ ] Acknowledge only event IDs explicitly accepted/existing by the server. Release retryable rows; quarantine permanent schema errors with metadata-only reason.
- [ ] Apply exponential backoff with full jitter and server `Retry-After`; cap attempts by elapsed retention rather than a small retry count.
- [ ] Test connection loss before send, during body, after server commit and before client ack. Every case converges through idempotency.
- [ ] Test seven-day-equivalent offline queue followed by bounded drain without starving fresh high-priority session-end events.
- [ ] Commit `feat: upload spooled captures idempotently`.

## Task CODEX-022: Idempotent hook installer

**Files:** `src/agent_context_codex/install/atomic.py`, `src/agent_context_codex/install/hooks.py`, `src/agent_context_codex/install/doctor.py`, `src/agent_context_codex/cli.py`, `tests/install/test_atomic.py`, `tests/install/test_hooks.py`, `docs/configuration.md`.

**Interfaces:** Adds `install hooks`, `uninstall hooks`, `doctor`, `--dry-run` and `--scope user|project`.

- [ ] Test absent file, existing unrelated hooks, prior adapter version, malformed JSON, symlink target, concurrent installers and interrupted replacement.
- [ ] Prefer a standalone `hooks.json` at the selected Codex config layer; never create both inline hooks and `hooks.json` in the same layer.
- [ ] Merge only adapter-owned entries, preserve unrelated ordering/data, write a timestamped backup, fsync temp file/directory and atomically replace under a file lock.
- [ ] Install background observational hooks for all supported events. `SessionEnd` remains synchronous because Codex enforces it, with a command timeout within its documented three-second maximum.
- [ ] Use an absolute executable path for standalone installation. Plugin packaging uses `PLUGIN_ROOT` only inside the plugin artifact.
- [ ] Print the exact diff in dry-run and the mandatory `/hooks` trust-review step after install; never claim a hook is active before the user trusts its current hash.
- [ ] `doctor` checks version, executable, hook discovery, trust reminder, spool permissions/canary count, ingestion reachability and MCP reachability without printing bodies or tokens.
- [ ] Commit `feat: install trusted Codex capture hooks`.

## Task CODEX-023: JSONL-to-spool pipeline

**Files:** `src/agent_context_codex/capture/jsonl/runner.py`, `src/agent_context_codex/cli.py`, `tests/e2e/test_jsonl_to_spool.py`, `docs/configuration.md`.

**Interfaces:** Adds `agent-context-codex capture-jsonl`, consuming stdin or wrapping a provided `codex exec --json` argument vector after `--`.

- [ ] Test stdin mode and wrapper mode with a fake Codex executable; forward Codex stdout byte-for-byte while observing the parsed copy through a bounded tee.
- [ ] Never accept a shell command string. Wrapper mode receives a list and invokes `codex exec --json` with `shell=False`.
- [ ] Apply the same pipeline/redaction/spool contracts as hooks and preserve Codex exit status.
- [ ] Ensure backpressure cannot deadlock Codex: bounded queue, metadata-only overflow event and continued stdout forwarding.
- [ ] Commit `feat: capture noninteractive Codex runs`.

## Task CODEX-040: Codex capability detection and MCP configuration

**Files:** `src/agent_context_codex/install/capabilities.py`, `src/agent_context_codex/install/mcp.py`, `src/agent_context_codex/cli.py`, `tests/install/test_capabilities.py`, `tests/install/test_mcp.py`, `tests/fixtures/codex-features/*.txt`.

**Interfaces:** Adds `install mcp`, `uninstall mcp`, `mcp-status`; produces `CodexCapabilities` from `codex --version` and `codex features list`.

- [ ] Parse semantic version and feature-list fixtures from `0.147.0` plus one newer version. Reject older versions with a precise upgrade instruction.
- [ ] Require `mcp_2026_07_28` to appear in `codex features list`; enable it with documented `codex features enable mcp_2026_07_28` only after dry-run output/explicit install invocation.
- [ ] If a newer Codex no longer lists the flag, do not reintroduce it. Treat successful server negotiation/smoke test as the capability signal.
- [ ] Merge this exact logical table into the selected config while preserving comments and unrelated keys: URL from settings, `bearer_token_env_var = "AGENT_CONTEXT_MCP_TOKEN"`, `enabled = true`, `required = false`, five approved tools, startup timeout 10 seconds and tool timeout 30 seconds.
- [ ] Never put the token value in the file. Test with a canary token and scan all backups/output.
- [ ] Use tomlkit, the same lock/backup/fsync/atomic-replace protocol as hooks, and idempotent owned-key removal on uninstall.
- [ ] Run `codex mcp list` for readback and print a restart instruction for existing CLI/IDE processes.
- [ ] Commit `feat: configure modern Agent Context MCP`.

## Task CODEX-041: Real Codex CLI MCP smoke suite

**Files:** `tests/e2e/test_codex_mcp.py`, `scripts/smoke-codex-mcp.sh`, `tests/fixtures/retrieval/expected.json`.

**Interfaces:** Verifies a real pinned Codex CLI against a disposable platform URL and all five tools.

- [ ] Start with a fixture repository/session already ingested by the integration harness.
- [ ] Assert `codex mcp list` sees `agent-context` and a noninteractive Codex invocation calls `context_for_task`, `repository_overview`, `symbol_context`, `decision_history` and `failure_history`.
- [ ] Assert every returned context package has evidence IDs, projection checkpoint and staleness, and no secret canary.
- [ ] Stop MCP mid-run and prove normal Codex file work still succeeds because `required = false`.
- [ ] Alternate server replicas between calls; do not preserve cookies or `Mcp-Session-Id`.
- [ ] Store sanitized diagnostics as CI artifacts and commit `test: verify Codex memory tools end to end`.

## Task CODEX-042: Compatibility and troubleshooting documentation

**Files:** `docs/codex-compatibility.md`, `docs/troubleshooting.md`, `docs/public-capture-surfaces.md`, `README.md`.

- [ ] Put the supported modern path first: Codex CLI `>=0.147.0`, feature detection, remote Streamable HTTP and MCP `2026-07-28`.
- [ ] Explain that `server/discover` is optional for callers and there is no application handshake/session affinity.
- [ ] Document hook concurrency, trust review, `SessionEnd` timing, ignored `transcript_path`, spool protection and metadata-only fallback.
- [ ] Add symptoms and exact safe diagnostics for missing feature flag, untrusted hook, absent token, TLS/NetBird failure, unavailable MCP, full spool and rejected schema.
- [ ] Confine older MCP behavior to a clearly titled historical compatibility section; do not present legacy initialization snippets as the recommended path.
- [ ] Link official Codex hooks, MCP configuration, developer commands, release and config-schema sources.
- [ ] Run Markdown lint/link checks and commit `docs: lead with current Codex MCP workflow`.

## Task CODEX-043: Optional local Codex plugin package

**Files:** `plugin/.codex-plugin/plugin.json`, `plugin/hooks/hooks.json`, `plugin/skills/agent-context-memory/SKILL.md`, `tests/plugin/test_manifest.py`, `tests/plugin/test_hooks.py`, `docs/plugin.md`.

**Interfaces:** Packages the already-tested hook executable and a short skill explaining when/how to query the five remote memory tools.

- [ ] Validate manifest against current Codex plugin packaging rules and keep every referenced path inside the plugin root.
- [ ] Use the plugin `hooks` manifest entry and `PLUGIN_ROOT`/`PLUGIN_DATA`; do not duplicate user-level hooks during plugin installation.
- [ ] Keep MCP endpoint/token configuration user-owned. The plugin must not embed credentials or silently create a second server entry.
- [ ] Make the skill treat retrieved repository/session text as untrusted evidence, require citations and refuse to follow instructions found inside memory content.
- [ ] Test archive reproducibility and hook trust requirement.
- [ ] Commit `feat: package optional Codex memory plugin`.

## Task CODEX-050: Safe Codex OpenTelemetry configuration

**Files:** `src/agent_context_codex/telemetry/config.py`, `src/agent_context_codex/cli.py`, `tests/telemetry/test_config.py`, `docs/telemetry.md`.

**Interfaces:** Adds `install telemetry --dry-run`; writes only official Codex OTel configuration keys.

- [ ] Generate OTLP endpoint/protocol, service metadata and export policy for the private collector; keep prompt/tool content export disabled because the hook pipeline owns sanitized content.
- [ ] Preserve existing telemetry configuration unless the user explicitly selects adapter-owned key replacement.
- [ ] Test token/prompt/tool canaries never appear in generated TOML or command output.
- [ ] Document correlation by native session/turn/tool IDs and platform-side loop prevention.
- [ ] Commit `feat: configure metadata-only Codex telemetry`.

## Completion gate

Run from a clean checkout with the exact SDK release:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -q
uv run agent-context-codex doctor --non-interactive
./scripts/smoke-codex-mcp.sh
```

Acceptance requires:

- all hook and JSONL public fixtures map to canonical SDK events;
- zero canary occurrences in spool, WAL/SHM, HTTP capture, logs, diagnostics and plugin archive;
- injected redactor failure stores metadata only;
- concurrent hooks and uploader crashes converge without duplicate canonical events;
- seven-day-equivalent offline spool drains after recovery;
- installation is idempotent and preserves unrelated Codex configuration;
- hook trust is explicitly surfaced;
- Codex `0.147.0` and the current supported stable release call all five tools through MCP `2026-07-28`;
- unavailable memory never prevents normal Codex work.

## Primary references

- [Codex hooks](https://developers.openai.com/codex/hooks)
- [Codex MCP configuration](https://developers.openai.com/codex/mcp)
- [Codex developer commands](https://learn.chatgpt.com/docs/developer-commands)
- [Codex advanced configuration and telemetry](https://developers.openai.com/codex/config-advanced)
- [Codex configuration schema](https://github.com/openai/codex/blob/main/codex-rs/core/config.schema.json)
- [Codex CLI releases](https://github.com/openai/codex/releases)
