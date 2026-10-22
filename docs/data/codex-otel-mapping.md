# Codex OTel mapping (PLATFORM-051)

`agent_context_platform.ledger.codex_otel` is a pure mapping library: it turns one decoded Codex
OTel record into an `EventDraftV1`, or into nothing. It does no I/O; the OTLP receiver
(PLATFORM-053) is described under [Receiver](#receiver). The SDK is pinned at **v0.3.2**; no private
contract exists. Anything without a v0.3.2 contract is listed in the gap table below and is the
input to **SDK-019** (v0.4.0, then the PIN-P update).

Sources (all paths under `codex-rs/`, cited at both tags `rust-v0.147.0` = supported floor and
`rust-v0.157.1` = installed):
`otel/src/events/session_telemetry.rs` (emitters), `otel/src/events/shared.rs` (common
attributes, `log_event!`/`trace_event!`), `otel/src/tool_result.rs` (0.157.1 only; the same event
lives in `session_telemetry.rs` at 0.147.0), `otel/src/metrics/names.rs`,
`core/src/otel_init.rs`, `login/src/auth/default_client.rs`, `exec/src/lib.rs`,
`protocol/src/protocol.rs`. `docs/config.md` has no otel section at either tag (it links out to
developers.openai.com/codex); Context7 was tried first and had nothing usable for these
emitters, so the source is the reference. Fixtures under `tests/fixtures/codex_otel/` are
hand-built from this source ("source-derived", not captured); G4 diffs them against a real
sandbox export.

## Input contract

- **Signal (CODEX-050).** Exactly one OTLP signal is consumed, chosen by the mapper config field
  `accepted_signal` (default `trace`); the other is rejected with `unsupported_signal`, so a
  deployment never consumes both. Codex emits most events twice (`log_event!` and `trace_event!`,
  `shared.rs`) with independently computed `event.timestamp` values, so consuming both would
  break idempotency. Decision CODEX-050: Codex OTel **logs are disabled** (`exporter = "none"`)
  because log records carry tool `arguments` verbatim, tool `output` and
  `user.email`/`user.account_id`; only trace-safe records (the `codex_otel.trace_safe` target,
  `targets.rs`) and metrics are exported, to a loopback collector. The mapper therefore defaults
  to `trace`; `accepted_signal=log` exists for tests and for a deliberately log-only deployment.
  Log-only fields (`arguments`, `output`, `user.email`, `user.account_id`, `mcp_servers`,
  `prompt`, `agent_name`) are never exported under the CODEX-050 config and are never read
  under either signal.
- **Loop prevention.** `service.name` (the Codex `originator`, or an explicit override:
  `otel_init.rs`) must be in the explicit allowlist `codex_cli_rs` (`DEFAULT_ORIGINATOR`,
  `default_client.rs`) or `codex_exec` (`set_default_originator`, `exec/src/lib.rs`). Names that
  identify the platform (`agent-context`, `agent-context-*`, `agent_context*`, case-insensitive)
  are rejected with `platform_origin`, and so is any record whose resource carries the platform
  telemetry marker `agent_context.origin` (any value, whatever the service name; see
  `docs/operations/telemetry.md`); every other or missing name is rejected with
  `unknown_service` / `missing_service`. Other Codex surfaces (TUI, IDE, SDK) or overridden names
  (`CODEX_INTERNAL_ORIGINATOR_OVERRIDE`, `service_name_override`) are rejected until added to the
  allowlist deliberately. The check runs before the event name is inspected, so it also covers
  unmapped events.
- **Bounds (fail closed, content-free errors).** At most 128 attributes and 64 resource
  attributes; keys at most 128 chars; declared string values at most 256 chars; any string at most
  1 MiB; every attribute and resource value must be a scalar (str, int, float, bool, null).
  `CodexOtelError` carries only a fixed reason code, never an attribute value.
- **Never read.** `prompt`, `arguments`, `output`, `error.message`, `user.email`,
  `user.account_id`, `auth.request_id`, `auth.cf_ray`, `auth.agent_id`, `auth.task_id`: content or
  personal/identifying data. `error.message` is **free text that is also present on the trace
  signal** (`record_api_request` `common` block and `sse_event` `trace_event!` in
  `session_telemetry.rs`), so the trace signal is not free-text-free: the mapper reads
  only the explicit allowlist below and a test seeds canaries in `error.message` and other
  free-text attributes. Config-derived strings (`provider_name`, `auth.env_provider_key_name`,
  `slug`, `terminal.type`, `app.version`, `reasoning_effort`) are likewise never read (`log_event!` in `shared.rs`; `tool_result.rs`;
  `session_telemetry.rs`). Codex gates prompt logging behind `log_user_prompt`; even when
  enabled the text stays out. Mapped events carry no content claims.
- **Idempotency.** The key is
  `codex_otel:<event.name>:<conversation.id>:<event.timestamp>:<HMAC-SHA256 hex>`. The HMAC is
  keyed with an injected key (>= 32 bytes) over the canonical JSON of the declared attributes, so
  re-export of the same record dedupes and no attribute value is recoverable from the key.
- **Native identity.** `conversation.id` is the session id (`session_id` in the payload and the
  envelope context), the same input the P025 agent projector uses (`session_node_id`), so hook-
  and OTel-origin observations of a session land on one `Session` node. The telemetry source
  event is preserved as its own ledger event; coalescing happens in projection.

## Receiver

`POST /v1/otel/traces` (`ledger/otlp_api.py`) takes an OTLP/HTTP `ExportTraceServiceRequest` and
feeds every Codex event record through this mapper. It is not part of the frozen
`/v1/ingestion/*` OpenAPI document.

- **Encoding.** `Content-Type: application/x-protobuf` only (OTLP/JSON is not accepted: 415), with
  `Content-Encoding` identity or `gzip` (anything else: 415). A gzip body must be exactly one
  member: truncated, trailing or multi-member data is 400.
- **Auth.** Exactly the ingestion producers' credentials: `Authorization: Bearer <prefix>.<secret>`
  of a registered producer with scope `events:ingest` (a `memory:read` token is 403), verified on the
  same bounded Argon2 path, before any byte of the body is read. Every mapped draft names that
  producer (`name` `codex-otel-receiver`, version `1.0.0`).
- **Limits** (`AGENT_CONTEXT_OTEL_RECEIVER__*`). Request-level limits answer 413 with a
  `google.rpc.Status` whose message starts with a fixed code, so the collector's logs show which:
  | Limit | Default | Code |
  | --- | --- | --- |
  | `MAX_REQUEST_BODY_BYTES`, counted while streaming | 4 MiB | `request_too_large` |
  | `MAX_DECOMPRESSED_BYTES`, gzip output (a bomb is never inflated past it) | 6 MiB | `inflated_too_large` |
  | `MAX_RESOURCES`, resource spans per request | 16 | `too_many_resources` |
  | `MAX_SCOPES`, scope spans per request | 64 | `too_many_scopes` |
  | `MAX_SPANS` per request (at most 500) | 500 | `too_many_spans` |
  | `MAX_RECORDS`, spans plus span events per request | 1000 | `too_many_records` |
  | `MAX_LINKS`, span links per request (never mapped) | 1000 | `too_many_links` |
  | `MAX_TOTAL_ATTRIBUTES`, every attribute at every level, nested values included | 65536 | `too_many_attributes` |
  | `MAX_ATTRIBUTES` per resource or scope | 128 | `too_many_attributes` |
  | nested attribute values deeper than 4 | | `too_many_attributes` |
  | distinct mapped events per request (one ingestion batch) | 500 | `too_many_drafts` |

  These are checked on the **wire bytes before parsing** (`ledger/otlp_wire.py`): the scan descends
  only into containers and attributes, skips everything else by length and stops at the first
  exceeded limit, so its work is bounded by the limits and the parser only sees a message whose
  object count is within them. Per-span limits reject that span only (partial success):
  `MAX_EVENTS_PER_SPAN` (64, `too_many_events`) and `MAX_ATTRIBUTES` per span or span event (128,
  `too_many_attributes`); their events still count toward `MAX_RECORDS`, so a flood is a
  request-level 413. A string value over 16 KiB rejects its record (`attribute_value_too_long`).
  Inflation, the scan, parsing and mapping run on a worker thread behind an admission gate: two
  decodes at once per process and four waiting; beyond that the request is refused at once with
  503 and `Retry-After`, and a queued request whose client has gone is not decoded. Measured on the
  development container at the limits: the collector's largest batch (15 spans x 64 events x 44
  attributes of 64 characters, 3.73 MiB, 25 KiB gzip) takes about 0.12 s and 11 MiB of extra peak
  memory; the densest message the limits allow (65,536 attributes over 1000 records) about 0.13 s
  and 7 MiB; a flood of resources or links is refused within milliseconds. `MAX_DECOMPRESSED_BYTES`
  is that largest batch plus about 60%. The ingestion append that follows is async database work on
  the event loop, one statement round per event. A per-producer token bucket of 5/s, burst 20
  (`RATE_LIMIT_PER_SECOND`, `RATE_LIMIT_BURST`) answers 429 with `Retry-After`.
- **Idempotency key.** `IDEMPOTENCY_HMAC_KEY` (at least 32 characters, at least 8 distinct) keys the
  mapper's digest. Until it is set (and ingestion is configured) the route answers 503 with
  `Retry-After`; the collector retries for at most an hour and then drops the batch.
  **Rotation** (or a change of the producer name or version) changes the keys of re-exported
  records, so queued re-exports would become duplicate events: drain the collector's sending queue
  first (stop Codex export, wait until the queue is empty), then rotate and restart.
- **Records.** A span whose attributes carry `event.name` is one record; so is each span event that
  does (Codex's `trace_event!` inside a span). The resource is checked once per `ResourceSpans`
  (`check_resource`): a platform-origin or unknown resource rejects all its spans. The
  `agent_context.origin` marker rejects at every level: resource, scope (all its spans), span and
  span event (the span). Records the mapper does not model are unmapped (counted, not errors);
  records it refuses reject their span. Records with the same idempotency key in one export collapse
  into one draft and the **first one wins**: a span and a span event describing the same Codex event
  with different attributes would map to different keys, and so to two events (Codex emits each
  trace-safe event once, so this is not expected).
- **Times.** `event.timestamp` (`occurred_at`) and the OTLP time (span event time, else span end)
  are **Codex's clock**, not the platform's. A record with no OTLP time is rejected
  (`missing_timestamp`); one whose OTLP time or `occurred_at` is more than `MAX_CLOCK_SKEW_SECONDS`
  (300) in the future is rejected (`future_timestamp`). `observed_at` is the later of `occurred_at`
  and the OTLP time: deterministic, so a collector retry of the same export maps to identical drafts
  and is answered `existing`.
- **Streams and batches.** Drafts go to the stream `codex-otel:<producer_id>:<session_id>`, one per
  producer and Codex session, both components percent-encoded (a `:` becomes `%3A`), so no
  producer/session pair can name another pair's stream. A stream-level failure (a quarantined
  stream after a failed integrity verification) stays within one session. All drafts of one
  request are appended as one atomic batch. When the batch is refused, the events it is attributable
  to (an `idempotency_conflict`, a `stream_quarantined` stream) are rejected and the others are
  resubmitted, until a batch is accepted, the ledger names no offender, or 8 batches were tried (the
  ledger names one conflict per batch; what is left then is rejected as `batch_rejected`). Events
  already stored are `existing`, never rejected. One conflicting record therefore never discards the
  rest of its export.
- **Response.** 200 with an `ExportTraceServiceResponse`; when spans were rejected,
  `partial_success.rejected_spans` and the fixed `error_message` `spans rejected: <reason codes>`
  (sorted fixed codes such as `platform_origin`, `future_timestamp` or `idempotency_conflict`, never
  a value). Partial success is final: the collector does not retry it. Errors answer a
  `google.rpc.Status` with a fixed message: 400 malformed, 401/403 credentials, 413 (code above),
  415, 429, and 503 (with `Retry-After`) when unconfigured, busy (the admission gate is full) or on
  a retryable outage. Logs carry ids, counts and fixed codes only. Telemetry:
  `agent_context.otel.requests` and `agent_context.otel.spans` are the receiver's only signals
  (these batches do not go through the ingestion route, so `agent_context.ingestion.*` and its span
  never count them; see `docs/operations/telemetry.md`).
- **Threat note.** Any `events:ingest` producer can append `agent.session.started` for any
  `conversation.id` it chooses, but only into its own streams (`codex-otel:<its producer_id>:...`,
  unambiguous because both components are percent-encoded)
  and under its own producer id: projections can tell the sources apart, and revoking the producer
  stops it. Only the collector should hold the receiver's producer token.
- **For the collector (INFRA-050).** The `otlphttp` exporter targets
  `http://platform-api:8000/v1/otel/traces` (set `traces_endpoint`, since the path is not the OTLP
  default) with `encoding: proto`, optionally `compression: gzip`, and the header
  `Authorization: Bearer <producer token>` of a producer registered with scope `events:ingest`.
  The batch processor must keep requests inside the limits above: `send_batch_max_size` at most
  500, and batches well under 4 MiB (and 8 MiB inflated). A 413 is permanent for `otlphttp`, which
  drops the batch. It forwards only the Codex trace-safe pipeline, never platform-origin signals.

## Mapped (SDK v0.3.2)

| Codex event | SDK event | Payload field | Source attribute | Notes | Codex file:tag |
| --- | --- | --- | --- | --- | --- |
| `codex.conversation_starts` | `agent.session.started` | `source` | (constant `codex_otel`) | | |
| | | `session_id` | `conversation.id` | required; `[A-Za-z0-9._:-]{1,128}` | `events/shared.rs` (`trace_event!`, `log_event!`) @ both tags |
| | | `model` | `model` | blank becomes null | `events/shared.rs` @ both tags |
| | | `permission_mode` | `approval_policy` | closed set `untrusted`, `on-request`, `granular`, `never`; else null | `session_telemetry.rs::conversation_starts` @ both tags; `protocol.rs::AskForApproval` |
| | | `sandbox_mode` | `sandbox_policy` | closed set `danger-full-access`, `read-only`, `external-sandbox`, `workspace-write`; else null (strum `Display` prints the variant only, never `writable_roots`) | `session_telemetry.rs::conversation_starts` @ both tags; `protocol.rs::SandboxPolicy` |
| | | `start_reason` | none | always null | |
| | envelope `occurred_at` | | `event.timestamp` | RFC 3339, millisecond `Z` | `events/shared.rs::timestamp` @ both tags |

Present in both tags with identical names (checked by diff of `session_telemetry.rs`). Not
mapped from this event: `provider_name`, `reasoning_effort`, `reasoning_summary`,
`context_window`, `auto_compact_token_limit`, `auth.*` presence flags, `mcp_servers` (log) /
`mcp_server_count` (trace): no v0.3.2 field.

### `codex.conversation_starts` attributes per signal

Emitted by `session_telemetry.rs::conversation_starts` via `log_and_trace_event!` (`common`,
`log`, `trace` blocks) with the per-signal macro suffixes from `shared.rs`; identical at
`rust-v0.147.0` and `rust-v0.157.1`. "Read" means the mapper reads it; everything else is ignored.

| Attribute | Log | Trace-safe | Read by mapper | Source (file:tag) |
| --- | --- | --- | --- | --- |
| `event.name` | yes | yes | yes (selects the event) | `session_telemetry.rs` `common` @ both tags |
| `event.timestamp` | yes | yes | yes (`occurred_at`, key) | `shared.rs` `log_event!`/`trace_event!` @ both tags |
| `conversation.id` | yes | yes | yes (`session_id`, key) | `shared.rs` @ both tags |
| `model` | yes | yes | yes | `shared.rs` @ both tags |
| `approval_policy` | yes | yes | yes (closed set) | `session_telemetry.rs` `common` @ both tags |
| `sandbox_policy` | yes | yes | yes (closed set) | `session_telemetry.rs` `common` @ both tags |
| `provider_name`, `reasoning_effort`, `reasoning_summary`, `context_window`, `auto_compact_token_limit`, `auth.env_*` (presence flags, `provider_key_name`) | yes | yes | no | `session_telemetry.rs` `common` @ both tags |
| `slug`, `app.version`, `originator`, `terminal.type`, `auth_mode` | yes | yes | no | `shared.rs` @ both tags |
| `mcp_servers` (server names, joined) | yes | no | no | `session_telemetry.rs` `log` block @ both tags |
| `mcp_server_count` | no | yes | no | `session_telemetry.rs` `trace` block @ both tags |
| `user.email`, `user.account_id` | yes | no | never | `shared.rs` `log_event!` only @ both tags |

For the other events the trace-safe variant is the log variant minus `user.email` /
`user.account_id` (`shared.rs`) and any `log:`-only block; `common:` attributes reach both
signals. That includes free text and identifiers: `error.message` and `auth.request_id`,
`auth.cf_ray`, `auth.error`, `auth.agent_id`, `auth.task_id` on `codex.api_request`
(`record_api_request` `common` block) and `error.message` on `codex.sse_event` failures
(`trace_event!` and `see_event_completed_failed`) @ both tags. These events are unmapped, and the
mapper never reads these attributes.

## Recognised but unmapped: field-level gap table (input to SDK-019)

Types are the SDK-side type proposed; "wire" notes the OTel attribute type. Attributes emitted
with `%` (tracing `Display`) arrive as **strings** even when numeric (for example
`duration_ms`, `input_token_count`, `success`); attributes emitted without `%` keep their native
type. All events share `conversation.id`, `event.timestamp`, `model`, `slug`, `app.version`,
`originator`, `terminal.type`.

### Blockers on existing v0.3.2 events (why no mapping)

| Codex event | SDK event | Missing or incompatible | Codex evidence |
| --- | --- | --- | --- |
| `codex.tool_result` | `agent.tool_call.completed` | `turn_id` is required but the record has no turn id (only `call_id`, `conversation.id`); `output_content_id` is required but output must not be stored. Available: `tool_name` (str), `call_id` (str) -> `tool_call_id`, `success` (wire str `"true"`/`"false"`) -> `success`, `duration_ms` (wire str) -> `duration_ms`. | `session_telemetry.rs::tool_result_with_tags` @ v0.147.0; `tool_result.rs::emit_tool_result` @ v0.157.1 |
| `codex.tool_decision` | `agent.permission.decision` | `turn_id` is required but absent. `decision` accepts only `allow`/`deny`/`ask`; Codex emits `approved`, `approved_with_amendment`, `approved_for_session`, `approved_mcp_policy_amendment` (0.157.1 only), `approved_with_network_policy_allow`, `denied_with_network_policy_deny`, `denied`, `timed_out`, `abort` (0.157.1: `ReviewDecision::to_opaque_string`; 0.147.0: lowercase `Display` of the variant, which differs). Also `source` (str), `tool_namespace` (0.157.1 only). | `session_telemetry.rs::tool_decision` @ both tags; `protocol.rs::ReviewDecision` |

SDK-019 options: make `turn_id` optional on those two contracts (the P025 projector keys tool
calls by session + call id, so it does not need it, but its `turn_id`/`INVOKED` writes would need
a projector change), or require a producer-side turn correlation; make `output_content_id`
optional for outcome-only observers; add a decision vocabulary (or a separate `native_decision`
token).

### Proposed new events

| Proposed event | Field | Type | Source attribute | Codex file:tag |
| --- | --- | --- | --- | --- |
| `agent.model_request.completed` | `source` | str | constant | |
| | `session_id` | str | `conversation.id` | `shared.rs` |
| | `duration_ms` | int >= 0 | `duration_ms` (wire str) | `session_telemetry.rs::record_api_request` @ both tags |
| | `status_code` | int? | `http.response.status_code` (wire int) | same |
| | `attempt` | int | `attempt` | same |
| | `endpoint` | str | `endpoint` | same |
| | `error_class` | str? | derived from `error.message` (message itself never stored) | same |
| | `retry_after_unauthorized`, `recovery_mode`, `recovery_phase` | bool / str? / str? | `auth.retry_after_unauthorized`, `auth.recovery_mode`, `auth.recovery_phase` | same |
| `agent.model_stream.completed` (`codex.sse_event`, `event.kind = response.completed`; websocket equivalents `codex.websocket_connect` / `codex.websocket_request`) | `session_id` | str | `conversation.id` | `session_telemetry.rs::sse_event_completed` @ both tags |
| | `input_tokens`, `output_tokens`, `total_tokens` | int >= 0 | `input_token_count`, `output_token_count`, `tool_token_count` (wire str; the last name is Codex's for `total_tokens`) | same |
| | `cached_input_tokens`, `cache_write_input_tokens`, `reasoning_output_tokens` | int? | `cached_token_count`, `cache_write_token_count`, `reasoning_token_count` (wire int) | same |
| | `ttft_ms` | int? | `ttft_ms` | same |
| | `service_tier`, `reasoning_effort` | str? | `service_tier`, `model_reasoning_effort` | same |
| | `duration_ms` | int? | `duration_ms` (non-completed kinds) | `session_telemetry.rs::sse_event` @ both tags |
| `agent.user_prompt.submitted` (`codex.user_prompt`) | `session_id` | str | `conversation.id` | `session_telemetry.rs::user_prompt` @ both tags |
| | `prompt_length` | int >= 0 | `prompt_length` (wire str) | same |
| | `text_input_count`, `image_input_count`, `local_image_input_count` | int | same names (trace signal only) | same (`trace_event!`) |
| `agent.sandbox.outcome` (`codex.sandbox_outcome`) | `session_id`, `tool_call_id`, `tool_name` | str | `conversation.id`, `call_id`, `tool_name` | `session_telemetry.rs::sandbox_outcome` @ both tags |
| | `outcome` | str | `outcome` | same |
| | `initial_duration_ms`, `escalated_duration_ms` | int, int? | same names | same |
| `agent.turn.cost_observed` (0.157.1 only) | `session_id`, `turn_id` | str | `conversation.id`, `turn.id` (the only event that carries a turn id) | `session_telemetry.rs::record_turn_cost` @ v0.157.1 |
| | `estimated_usd` | decimal str | `usage.estimated_usd` | same |
| | `interrupted` | bool | `turn.interrupted` | same |
| | `speed`, `reasoning_effort` | str? | `speed`, `reasoning_effort` | same |
| `agent.turn.first_token_observed` (`codex.turn_ttft`) / `agent.startup.phase_observed` (`codex.startup_phase`) | `duration_ms`, `startup_phase`, `startup_status` | int, str, str? | `duration_ms`, `startup.phase`, `startup.status` | `session_telemetry.rs::record_turn_ttft`, `record_startup_phase` @ both tags |

Turn events: Codex OTel has no turn-start or turn-completed record. `turn.id` exists only on
`codex.turn_cost` and the `codex.turn.cost_microusd` metric at 0.157.1, so turn entities keep
coming from hooks/JSONL.

### Metrics (OTLP metric signal, not log records)

Not mapped; the receiver should keep them as metrics, or SDK-019 should define a metric-point
event. Names (`otel/src/metrics/names.rs` @ both tags unless noted): `codex.tool.call`,
`codex.tool.call.duration_ms` (tags `tool`, `success`), `codex.api_request`,
`codex.api_request.duration_ms` (tags `status`, `success`), `codex.sse_event`,
`codex.sse_event.duration_ms` (tags `kind`, `success`), `codex.websocket.request`,
`codex.websocket.event`, `codex.turn.e2e_duration_ms`, `codex.turn.ttft.duration_ms`,
`codex.turn.token_usage`, `codex.turn.tool.call`, `codex.turn.cost_microusd` (0.157.1 only,
tags `turn.id`, `conversation.id`, `turn.interrupted`, `speed`, `reasoning_effort`), plus
process-start, startup, plugin, hook, guardian and goal counters.

## Version differences

`rust-v0.157.1` adds `codex.turn_cost`, `tool_namespace` on `tool_decision`/`tool_result`,
`tool_result_seq` and `output_truncated` on `tool_result`, `agent_name` on log-only
`tool_result`, and the `approved_mcp_policy_amendment` decision; `tool_decision.source` becomes
optional. None of these affect the mapped `codex.conversation_starts` fields.

`codex.auth_recovery` (`session_telemetry.rs`, both tags) carries auth-recovery diagnostics only; no agent-context event is proposed for it, and it stays unmapped.
