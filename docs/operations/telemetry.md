# Platform telemetry contract v1

The platform exports its own traces and metrics with OpenTelemetry. This document is the
contract the infrastructure collector, dashboards and alerts (INFRA-050/051) are built from; the
implementation is `src/agent_context_platform/observability/` and the tests under
`tests/unit/observability/` assert it. A change here is a contract change for both sides.

Telemetry is metadata only. No signal carries event content, prompts, tool arguments or results,
authorization headers, cookies, tokens, DSNs, file paths, repository names or exception messages.

## Export

| Setting | Source | Default |
| --- | --- | --- |
| Endpoint | `AGENT_CONTEXT_OBSERVABILITY__OTLP_ENDPOINT` (base URL, `/v1/traces` and `/v1/metrics` are appended), else the standard `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | unset: telemetry is **disabled** (no-op providers, nothing is exported) |
| Kill switch | `OTEL_SDK_DISABLED=true` or `AGENT_CONTEXT_OBSERVABILITY__ENABLED=false` | enabled when an endpoint is set |
| Headers | standard `OTEL_EXPORTER_OTLP_HEADERS` / `..._TRACES_HEADERS` / `..._METRICS_HEADERS` (for example the Cloudflare Access service-token headers). A signal whose headers are not all `key=value` entries is not exported, and only a fixed, value-free warning is logged | none |
| Protocol | OTLP/HTTP protobuf | fixed |
| Export timeout | `AGENT_CONTEXT_OBSERVABILITY__EXPORT_TIMEOUT_SECONDS` | 5 s |
| Span queue | `..._MAX_QUEUE_SIZE` / `..._MAX_EXPORT_BATCH_SIZE` / `..._SCHEDULE_DELAY_SECONDS` | 2048 / 512 / 5 s |
| Metric interval | `..._METRIC_EXPORT_INTERVAL_SECONDS` | 30 s |
| Shutdown flush | `..._SHUTDOWN_TIMEOUT_SECONDS` | 5 s |
| Instance id | `..._SERVICE_INSTANCE_ID` | a random UUID per process |

The endpoint must not embed credentials. Spans are exported by a batch processor with a bounded
queue (a full queue drops spans, it never blocks); metrics by a periodic reader on its own thread.
A collector that is down or slow never fails or delays a request. Providers are flushed on
application shutdown (`lifespan` end) and at the end of the `projection run` and `index`
commands, bounded by the shutdown timeout.

## Resource

Every signal carries:

| Attribute | Value |
| --- | --- |
| `service.name` | `agent-context-platform` |
| `service.namespace` | `agent-context` |
| `service.version` | the installed package version |
| `service.instance.id` | per process (replica) |
| `deployment.environment` | `AGENT_CONTEXT_ENVIRONMENT` (`development`, `test`, `production`) |
| `agent_context.origin` | `platform` |

`agent_context.origin=platform` is the loop-prevention marker: the Codex OTel normalization
(`ledger/codex_otel.py`) rejects any record whose resource carries `agent_context.origin` (any
value) or a platform `service.name` with the fixed reason `platform_origin`, and the collector must
never route platform signals into a Codex-ingestion pipeline.

## Metrics

Attribute values are closed sets. A value outside the set is recorded as `other`, so every
attribute below may also take `other` (a guard, never expected in normal operation). Prometheus
names assume the collector's Prometheus exporter normalization (unit suffix, `_total` for
counters).

| OTel instrument | Kind | Unit | Attributes | Prometheus |
| --- | --- | --- | --- | --- |
| `agent_context.ingestion.batches` | counter | `{batch}` | `outcome` = accepted / rejected / error; `reason` = none / auth / schema / redaction_required / too_large / producer_mismatch / conflict / internal | `agent_context_ingestion_batches_total` |
| `agent_context.ingestion.events` | counter | `{event}` | `result` = created / existing | `agent_context_ingestion_events_total` |
| `agent_context.ingestion.duration` | histogram | `s` | `outcome` | `agent_context_ingestion_duration_seconds_*` |
| `agent_context.content.objects` | counter | `{object}` | `storage` = inline / object; `result` = stored / reused / rejected | `agent_context_content_objects_total` |
| `agent_context.projection.events` | counter | `{event}` | `projector`; `result` = delivered / retried / dead_lettered | `agent_context_projection_events_total` |
| `agent_context.projection.duration` | histogram | `s` | `projector` | `agent_context_projection_duration_seconds_*` |
| `agent_context.projection.lag` | gauge | `s` | `projector` | `agent_context_projection_lag_seconds` |
| `agent_context.projection.outbox_pending` | gauge | `{event}` | none | `agent_context_projection_outbox_pending` |
| `agent_context.projection.dead_letters` | gauge | `{event}` | `projector` | `agent_context_projection_dead_letters` |
| `agent_context.ledger.integrity_failures` | counter | `{failure}` | `check` = hash_chain / missing_object / digest_mismatch / checkpoint | `agent_context_ledger_integrity_failures_total` |
| `agent_context.index.runs` | counter | `{run}` | `result` = success / failed / refused | `agent_context_index_runs_total` |
| `agent_context.index.duration` | histogram | `s` | `result` | `agent_context_index_duration_seconds_*` |
| `agent_context.retrieval.duration` | histogram | `s` | `operation` = context_for_task / repository_overview / symbol_context / decision_history / failure_history / graph / temporal / search; `outcome` = ok / client_error / server_error / deadline | `agent_context_retrieval_duration_seconds_*` |
| `agent_context.mcp.requests` | counter | `{request}` | `method` = server/discover / tools/list / tools/call / other; `tool` = context_for_task / repository_overview / symbol_context / decision_history / failure_history / none; `outcome` = ok / client_error / auth_error / rate_limited / server_error | `agent_context_mcp_requests_total` |
| `agent_context.mcp.duration` | histogram | `s` | `method`, `tool` | `agent_context_mcp_duration_seconds_*` |
| `http.server.request.duration` | histogram | `s` | `http.request.method` (standard methods or `_OTHER`), `http.route` (the matched route template under `/v1/`, `/health/`, `/mcp`, `/openapi.json`, `/docs` or `/redoc`, else `other`), `http.response.status_code` | `http_server_request_duration_seconds_*` |

Duration histograms use the explicit bucket boundaries
`0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300` seconds.

### Semantics

- **Ingestion.** One `batches` sample per `POST /v1/ingestion/batches`, with the duration of the
  whole request. `accepted` means every event was stored or already present (HTTP 200). `rejected`
  reasons: `auth` (401/403 credential or scope), `schema` (missing or mismatched `Idempotency-Key`,
  invalid request or payload schema, and the batch-level content codes `duplicate_idempotency_key`,
  `duplicate_content_id`, `unsupported_media_type`, `invalid_content_encoding`,
  `content_resolution_mismatch`), `redaction_required` (`content_requires_redaction`), `too_large`
  (413), `producer_mismatch`, `conflict` (`idempotency_conflict`, `stream_quarantined`). `error`
  with `internal` covers ingestion not configured, authentication overload, retryable datastore or
  blob-store outages and unexpected failures. `events` counts the per-event results of accepted
  batches and the already-stored events reported in rejected ones.
- **Content.** One sample per content item of a batch. `stored` (written, or inline and recorded
  idempotently) and `reused` (an object-storage digest already uploaded earlier in the same batch)
  are recorded once the batch has committed, so a rejected or rolled-back batch counts none of them.
  `rejected` is recorded when the item fails validation or the redaction re-check; its `storage` is
  derived from the claimed size.
- **Projection.** Recorded by the `projection run` worker. A delivered outbox row counts once per
  matching projector (`projector=none` when no projector handles the event type). A retried or
  dead-lettered row counts once for the failing projector (`unattributed` when the event could not
  be loaded or verified). A lost lease is not counted. `duration` is the time of each projector's
  `project` call. The gauges are refreshed by the worker every
  `AGENT_CONTEXT_OBSERVABILITY__PROJECTION_SAMPLE_SECONDS` (default 15 s), only while metrics are
  exported, in one short transaction on the projector connection: a 2 s statement timeout and two
  queries (outstanding outbox rows through the status index, open dead letters by projector); a
  scrape never queries the database. `outbox_pending` counts outstanding rows (pending, including
  those waiting out a retry backoff, plus leased). `lag` is the age of the oldest outstanding outbox
  row, 0 when none is outstanding; it is reported for every running projector because projectors of
  one event commit together. When a refresh fails or times out the gauges stop reporting until the
  next successful refresh (alert on absence). The API process never reports these gauges.
- **Integrity.** `digest_mismatch`: a stored event failed its payload/envelope digest verification
  when the projector loaded it, or stored content failed verification when a batch reused it
  (prepare), attached it, or the search projector read it. `missing_object`: stored content was gone
  where nothing may have deleted it (at attach: referenced by the catalog or uploaded by the same
  request; at prepare: again after the retried conditional PUT; at read: always). An old
  unreferenced object swept by the orphan sweep is not counted. Each content failure also logs one WARNING `content_integrity_failure` line (see
  [content-integrity.md](content-integrity.md)). `hash_chain` and `checkpoint` are
  reserved for the stream hash-chain and signed checkpoint verifiers; no code path emits them yet.
- **Index.** One sample per `index_checkout`: `success`, `failed` (indexing or ingestion error, or
  an unsuccessful completion) or `refused` (repository mismatch, refused scan, structural sandbox
  unavailable).
- **Retrieval.** One sample per retrieval operation, recorded with
  `observability.retrieval_span(operation, client_errors=..., deadline_errors=...)` (or the
  `traced_retrieval` decorator); the composer and MCP tools adopt it. `client_error` is a typed
  request error (bad scope, depth, anchor not found), `deadline` the request deadline,
  `server_error` anything else. Graph traversals record `operation=graph`.
- **MCP.** One sample per request reaching `/mcp`, authentication and transport rejections included,
  classified from the HTTP status and its cause: 2xx `ok`, 401 and 403 answered by the access gate
  `auth_error`, 429 `rate_limited`, 5xx `server_error`, any other 4xx (the guard's Host 421 and
  Origin 403 before authentication, method, version, size, unknown JSON-RPC method) and a request
  abandoned before any answer `client_error`. A tool result flagged `isError` is a protocol-level
  `ok`; its cause is visible in the retrieval metrics. `method` is the validated body method (else
  the `Mcp-Method` header) and `tool` the `Mcp-Name` header of a `tools/call` (the SDK rejects a
  header that disagrees with the body), both bounded to the sets above; the body, arguments and
  results are never read.

## Spans

| Span | Kind | Notes |
| --- | --- | --- |
| `{METHOD} {route}` (e.g. `POST /v1/ingestion/batches`) | server | HTTP server span, route template only, never the raw path or query. Child of an incoming W3C `traceparent`. |
| `agent_context.ingest_batch` | internal | `agent_context.batch_id`, `agent_context.event_count`, `agent_context.outcome`, `agent_context.reason` |
| `agent_context.content.prepare` | internal | `agent_context.item_count` |
| `agent_context.ledger.append` | internal | `agent_context.event_count`, `agent_context.events.created` |
| `agent_context.projection.batch` | internal | `agent_context.claimed`, `agent_context.delivered`, `agent_context.retried`, `agent_context.dead_lettered` |
| `agent_context.projection.event` | internal | `agent_context.event_id`, `agent_context.outbox_id`, `agent_context.result` |
| `agent_context.index.run` | internal | `agent_context.result`, `agent_context.events.submitted` |
| `agent_context.retrieval.<operation>` | internal | `agent_context.operation`, `agent_context.outcome` |
| MCP request, named by `method` (e.g. `tools/call`) | internal | `mcp.method.name`, `gen_ai.tool.name` (one of the five tools), `agent_context.outcome`. Spans recorded by tools nest under it. |

Every platform span is sampled: a caller's `traceparent` links the platform spans into the
caller's trace, but its sampled flag never drops them. Exported span, parent and link contexts carry
no `tracestate`. The MCP SDK's own OpenTelemetry middleware (it imports the whole `params._meta`
carrier, `tracestate` and baggage included) is replaced: per the 2026-07-28 profile a request's
`params._meta.traceparent` still parents the spans recorded while serving it, but only `traceparent`
is read. Without a valid one, those spans nest under the platform's MCP request span.

Span attributes pass an allowlist when a span ends: keys outside the list are dropped, closed-set
values are checked, ids must be UUIDs and counts integers. Span events are dropped except
exceptions, which keep only `exception.type` (the class name): never `exception.message` or
`exception.stacktrace`. Span status descriptions are dropped. Failed spans have status `ERROR` and
`error.type` set to the exception class name or a fixed outcome.

## Loop prevention and privacy checks

- `tests/unit/observability/` injects runtime canaries (content, prompt text, tool arguments,
  bearer tokens, DSNs and paths) through real requests and scans every exported span, metric data
  point, resource and captured log record for them.
- `tests/contract/test_codex_otel.py` asserts that platform-origin records are rejected.
