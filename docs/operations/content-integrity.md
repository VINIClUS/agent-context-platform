# Content integrity, orphan sweep, token probe and secret files

Operator contract for PLATFORM-054. The implementation is in `content/`, `ledger/service.py`,
`ledger/whoami.py`, `cli.py` and `settings.py`; the tests under `tests/unit/` and
`tests/integration/` (real PostgreSQL and Garage) assert it.

## Integrity alarm

Every object-storage item of a batch is verified against S3 before it is referenced: when a batch
reuses a digest already stored (`prepare`) and again under the per-digest lock when it is attached
(`attach`); the search projector's read path verifies what it reads (`read`). An object is proven
by its own frame: its declared lengths, the Zstandard frame checksum and the SHA-256 of the
decompressed bytes, plus its metadata (`sha256`, `uncompressed-bytes`, `ContentType`,
`Content-Encoding: zstd`). Its compressed size is not compared with what this process would write,
so an object compressed at another level, by another zstd version or by another tool is reused as
it is. The alarm is raised when:

* `digest_mismatch`: a stored object (at `prepare`, `attach` or `read`) or an inline row (at
  `read`) fails that verification;
* `missing_object`: an object is gone where nothing may have deleted it: at `attach`, when the
  catalog already references the digest (the sweep below never deletes a referenced object) or
  when this very request uploaded it; at `prepare`, when it vanishes again after the retried
  conditional PUT (`if_none_match` mode); at `read`, for any object or inline row (nothing in the
  platform deletes referenced bytes, and readers skip content the ledger records as purged before
  reading).

It consists of:

* one WARNING log line `content_integrity_failure check=<check> stage=<prepare|attach|read>
  content_sha256=<digest>`, with the structured fields `check`, `stage`, `content_sha256` and
  `object_key` (`sha256/aa/bb/<digest>.zst`). It never carries bytes, request content, tokens or the
  exception text;
* the counter `agent_context.ledger.integrity_failures{check}` (`check` = `digest_mismatch` or
  `missing_object`), exported as `agent_context_ledger_integrity_failures_total`
  ([telemetry.md](telemetry.md)); INFRA-051 alerts on it.

The batch is rejected as a whole and nothing is committed. **The response stays a retryable
`503`**: every new event is rejected with `service_unavailable`, `retryable: true`. A corrupt
object (`digest_mismatch`) answers `Retry-After: 60`; a missing object, like an ordinary outage,
answers `Retry-After: 5` (its retry uploads the object again). The frozen ingestion contract
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
   same key.

A read-path alarm (`stage=read`) makes the search projector fail closed for that content
(`SearchReplayIntegrityError` on a replay); repair the object the same way. An unreachable store
(an outage) is not an integrity failure and raises no alarm.

**Race with the orphan sweep (no alarm by design).** An old object nothing references yet can be
reused by `prepare` without a new upload, so its age does not change, and the sweep may delete it
before the batch attaches. Nothing referenced it, so nothing is lost: `attach` finds no catalog row
for the digest under its lock and the request did not upload it itself, logs
`content_object_swept_before_attach` at INFO (no metric) and fails the batch retryably
(`Retry-After: 5`); the retry uploads it again. In `if_none_match` write
mode the same deletion can happen between the refused conditional PUT and its verification at
`prepare`: the PUT is retried once, so that is no alarm either. Any `missing_object` alarm is
therefore worth a look; `increase(agent_context_ledger_integrity_failures_total[15m]) > 0` is a
suitable INFRA-051 rule for both checks.

Content over the configured upload size (`AGENT_CONTEXT_S3__MAX_*_BYTES`) is an input limit, not an
integrity failure, and raises no alarm.

**Write mode.** The default and the mode to deploy is `AGENT_CONTEXT_S3__WRITE_MODE=content_addressed`:
an existing object is verified before anything is written, and a failing one is never written over.
`if_none_match` relies on the backend refusing `PUT` with `If-None-Match: *` over an existing key
(`412`). **Garage v2.3.0 ignores that header** and accepts the PUT, so against Garage
`if_none_match` would silently overwrite an existing object instead of verifying it (the repair
this document forbids). Use `if_none_match` only with a backend that honours conditional writes
(for example AWS S3); INFRA must keep `content_addressed` for Garage.

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
`SELECT` it needs) and the `AGENT_CONTEXT_S3__*` settings (`ENDPOINT_URL` is required). The S3 key
needs `ListBucket` on the bucket (prefix `sha256/`) and `DeleteObject` on `sha256/*`, besides the
read and write the API already uses: do not scope the API's key as write-once if the sweep runs
with it, or give the sweep its own key. Output is
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
schema. Before any lookup it applies the MCP transport's browser defences
(`AGENT_CONTEXT_MCP__ALLOWED_ORIGINS` / `__ALLOWED_HOSTS`, matched like `/mcp`): an `Origin`
header, when sent, must be allowed for every token, and an `mcp_` token also needs an allowed,
single `Host`, so a DNS-rebinding page cannot spend the MCP Argon2 gate. A producer token keeps the
ingestion route's rules (no Host allowlist). Command-line clients send no `Origin`; a client
probing an MCP token must use the MCP host name.

| Answer | Meaning |
| --- | --- |
| `204`, `X-Agent-Context-Token-Kind: producer` | a valid, unexpired, unrevoked `events:ingest` producer token |
| `204`, `X-Agent-Context-Token-Kind: mcp` | a valid `memory:read` MCP token (`mcp_` prefix) |
| `421` `invalid_host` / `403` `invalid_origin` | a Host or Origin the MCP allowlists refuse (no lookup is made) |
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
`AGENT_CONTEXT_NEO4J__PASSWORD`, `AGENT_CONTEXT_S3__{ACCESS_KEY_ID,SECRET_ACCESS_KEY}`,
`AGENT_CONTEXT_MCP__{TOKEN_HMAC_KEY,CURSOR_HMAC_KEY}` and
`AGENT_CONTEXT_OTEL_RECEIVER__IDEMPOTENCY_HMAC_KEY` (`settings.SECRET_FILE_NAMES`). **A variable set both in the
environment (even to an empty value, in any letter case) and as a file refuses to start**, naming
the variable: a stale variable left after a rotation would otherwise keep the old value in use
and visible in `/proc/<pid>/environ`. Export a secret OR mount it, never both. Everything else
stays in the environment.

Settings refuse to load, naming the file but never its content (the CLI exits `2`), when:

* the directory is not an absolute, canonical path (no `.`, `..` or `//`), does not exist, has a
  symlink in ANY component of its path, is writable by group or others, or is not owned by the
  process user or root;
* a secret file is a symlink or not a regular file, is not owned by the process user or root, has
  any group or other permission bit (use `0400` or `0600`), exceeds 64 KiB (the final newline
  included), is not UTF-8, or its value (after dropping one final `\n` or `\r\n`) is empty, has
  more than one line, starts or ends with whitespace, or contains a byte-order mark or any other
  control, format or separator character (for example U+2028, vertical tab, form feed, NUL);
* a file named `AGENT_CONTEXT_*` is not a secret setting (it would never apply).

Only upper-case `AGENT_CONTEXT_*` names are ours: files whose names do not start with it
(including lower-case ones) are ignored, so the directory can be a shared secrets mount.

**Kubernetes secret volumes are refused by design**: their entries are symlinks into a `..data`
directory, the volume is often a world-writable (`1777`) tmpfs and files default to `0644`. Use a
plain bind mount of `0400` files owned by the container user (Docker/Compose `secrets:` with the
right `uid` and `mode`, or a host directory), or project the files into such a directory first.
