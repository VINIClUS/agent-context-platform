# Ingestion v1 consumer fixtures

Request/response pairs for `POST /v1/ingestion/batches`, frozen with
`openapi/agent-context-v1.json`. `agent-context-codex` (the uploader) consumes them
as its wire-level test vectors. They are content-free: every value is synthetic, and
the only "secret" is the canary in `05-redaction-required.json`.

## Format

Each file is one JSON object:

- `name`, `description`: what the case shows and what state it needs.
- `request`: `method`, `path`, `headers`, `body`. Serialize `body` as compact JSON
  (`separators=(",", ":")`, UTF-8); the size limit in `06` is measured on those bytes.
- `response`: `status`, `headers` (only contract headers: `retry-after`,
  `www-authenticate`) and `body`.

Files are numbered and stateful: run them in order against a fresh ledger
(`02` replays `01`, `03` repeats an event from `01`).

## Placeholders and normalization

- `Bearer <producer-token>` in `request.headers.authorization` stands for a valid
  credential whose producer id is `fixture-producer` and scope is `events:ingest`.
  `07-auth-failure` sends no credential on purpose.
- `request_id` in error envelopes is `<request-id>`. The service generates a new one
  per request (or echoes a safe `X-Request-ID`), so a consumer replaces the actual value
  with the placeholder before comparing. It is the only volatile field: responses hold
  no timestamps, and event and batch ids come from the request.
- The `x-request-id` and `content-type` response headers are not part of `response.headers`.

## Server assumptions

- `06-payload-too-large` needs a max request body of 4096 bytes; every other request is
  smaller. `08-transient-outage` needs the ledger database unreachable while
  authentication still works, and yields `Retry-After: 5`.

The platform runs these against the real service in
`tests/integration/ledger/test_ingestion_fixtures.py`.
