# Content integrity, orphan sweep, token probe and secret files

Operator contract for PLATFORM-054. The implementation is in `content/`, `ledger/service.py`,
`ledger/whoami.py`, `cli.py` and `settings.py`; the tests under `tests/unit/` and
`tests/integration/` (real PostgreSQL and Garage) assert it.

## Integrity alarm

Every object-storage item of a batch is verified against S3 before it is referenced: when a batch
reuses a digest already stored (`prepare`) and again under the per-digest lock when it is attached
(`attach`). A stored object whose bytes (the Zstandard frame checksum, lengths, SHA-256) or
metadata (`sha256`, `uncompressed-bytes`, `ContentType`, `Content-Encoding: zstd`) no longer
verify, or which has disappeared, raises the alarm:

* one WARNING log line `content_integrity_failure check=<check> stage=<prepare|attach>
  content_sha256=<digest>`, with the structured fields `check`, `stage`, `content_sha256` and
  `object_key` (`sha256/aa/bb/<digest>.zst`). It never carries bytes, request content, tokens or the
  exception text;
* the counter `agent_context.ledger.integrity_failures{check}` (`check` = `digest_mismatch` or
  `missing_object`), exported as `agent_context_ledger_integrity_failures_total`
  ([telemetry.md](telemetry.md)); INFRA-051 alerts on it.

The batch is rejected as a whole and nothing is committed. **The response stays a retryable
`503`**: every new event is rejected with `service_unavailable`, `retryable: true`, and the
response carries `Retry-After: 60` (an ordinary outage carries `5`). The frozen ingestion contract
(`openapi/agent-context-v1.json`, SDK 0.6.0) has no non-retryable code for "the platform's stored
copy is corrupt": `internal_error` is a 500 error envelope without per-event results, and the 422
content codes say that the *request* is wrong, which it is not. The batch is valid and succeeds
once the object is repaired, so a producer keeps it and retries slowly; the alarm, not the
response, tells the operator.

The platform **never repairs, overwrites or deletes** a failing object. To repair it:

1. Find the digest in the alarm (`content_sha256`, `object_key`).
2. Keep a copy of the object for analysis if wanted, then delete the object key from the bucket
   (an operator action; the platform has no command for it).
3. The next retry of any producer holding that content uploads it again: `prepare` finds no
   object and writes a fresh, verified one. Rows that referenced the digest keep pointing at the
   same key. A `missing_object` alarm on `attach` that does not repeat is the expected race with
   the orphan sweep below and needs no action.

Content over the configured upload size (`AGENT_CONTEXT_S3__MAX_*_BYTES`) is an input limit, not an
integrity failure, and raises no alarm.

## Orphan sweep

```bash
agent-context content sweep-orphans [--min-age SECONDS] [--dry-run] [--json]
```

Deletes content objects under `sha256/` that no `catalog.content_objects` row references and that
are older than `--min-age` (default and floor `86400`, 24 h: an upload whose batch has not attached
yet must never be deleted). For each candidate it takes the same per-digest advisory lock as
`attach`, re-checks the catalog under it and deletes while holding it. `--dry-run` runs the same
listing and locked check and deletes nothing (`would_delete`). A min-age below the floor needs
`--allow-short-min-age` (tests and development only); `AGENT_CONTEXT_ENVIRONMENT=production`
refuses that flag.

It uses `AGENT_CONTEXT_POSTGRESQL__API_DSN` (falls back to `__DSN`; the API role already has the
`SELECT` it needs) and the `AGENT_CONTEXT_S3__*` settings (`ENDPOINT_URL` is required). Output is
counts only, never keys or digests:

```json
{"examined": 12, "deleted": 2, "retained": 9, "skipped_recent": 1, "would_delete": 0,
 "dry_run": false, "min_age_seconds": 86400}
```

Exit codes: `0` done, `1` failed (error class only on stderr; a partial sweep is safe to rerun,
deletion is idempotent and every check is repeated), `2` usage error. **For the INFRA timer
(INFRA-053/060):** run it daily as a one-shot job of the platform image with the default min-age,
alert on a non-zero exit, and do not run two at once (a second run is harmless but wasted work).

## Token probe

`GET /v1/auth/whoami` with `Authorization: Bearer <token>` checks a credential without sending or
writing anything. It is not part of the frozen ingestion OpenAPI and is not in the published
schema.

| Answer | Meaning |
| --- | --- |
| `204`, `X-Agent-Context-Token-Kind: producer` | a valid, unexpired, unrevoked `events:ingest` producer token |
| `204`, `X-Agent-Context-Token-Kind: mcp` | a valid `memory:read` MCP token (`mcp_` prefix) |
| `401` `missing_credential` / `invalid_credential` (`WWW-Authenticate: Bearer`) | no, malformed, repeated, unknown, wrong, expired or revoked token |
| `403` `insufficient_scope` | a valid token without the plane's scope |
| `429` `rate_limited` (`Retry-After`) | the MCP principal's rate limit |
| `503` `auth_overloaded` / `auth_unavailable` (`Retry-After`) | the Argon2 gate is full, or the plane or its database is unavailable |
| `500` `internal_error` | unexpected failure |

Error bodies are the fixed ingestion envelope (`error.code`, `error.message`, `request_id`). The
probe changes no row: neither authenticator writes a last-used time or an audit row (an
integration test digests every platform table before and after). It is no cheaper an oracle than
the real planes: producer tokens go through the ingestion authenticator and its Argon2 admission
gate, MCP tokens through the MCP authenticator, its own gate and principal cache, and the
per-principal MCP rate limit. One INFO line `whoami status=<status>` with the token kind or error
code is logged; never the token or the principal. The Codex doctor (CODEX follow-up) uses it.

## Secret files

`AGENT_CONTEXT_SECRETS_DIR=/run/secrets/agent-context` makes every secret setting readable from a
file named after its variable, holding only the value (one trailing newline is dropped):

```text
/run/secrets/agent-context/AGENT_CONTEXT_POSTGRESQL__API_DSN        0400, owner = the process user
/run/secrets/agent-context/AGENT_CONTEXT_S3__SECRET_ACCESS_KEY      0400
```

The secret settings are `AGENT_CONTEXT_POSTGRESQL__{DSN,API_DSN,PROJECTOR_DSN,ADMIN_DSN,MIGRATION_DSN}`,
`AGENT_CONTEXT_NEO4J__PASSWORD`, `AGENT_CONTEXT_S3__{ACCESS_KEY_ID,SECRET_ACCESS_KEY}` and
`AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY` (`settings.SECRET_FILE_NAMES`). **Precedence: a variable set
in the environment wins over its file**, so a file only takes effect once the variable is no
longer exported (which is the point: `/proc/<pid>/environ` then holds no secret). Everything else
stays in the environment.

Settings refuse to load, naming the file but never its content (the CLI exits `2`), when:

* the directory is not an absolute path, does not exist, is a symlink, is writable by group or
  others, or is not owned by the process user or root;
* a secret file is a symlink or not a regular file, is not owned by the process user or root, has
  any group or other permission bit (use `0400` or `0600`), is empty, has more than one line, is
  not UTF-8 or exceeds 64 KiB;
* a file named `AGENT_CONTEXT_*` is not a secret setting (it would never apply).

Files whose names do not start with `AGENT_CONTEXT_` are ignored, so the directory can be a shared
secrets mount.
