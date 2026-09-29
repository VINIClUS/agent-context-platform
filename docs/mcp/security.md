# MCP security

The `/mcp` endpoint is a read-only Streamable HTTP server. Its authentication is a
**pre-provisioned bearer, not OAuth MCP**. There is no authorization server, no dynamic client
registration and no token endpoint: an operator mints a token, stores its verifier, and hands the
token to one Codex MCP client.

The OAuth profile (Authentik as the authorization server, PKCE, Protected Resource Metadata (PRM)
and Client ID Metadata Documents (CIMD)) is deferred. It replaces this bearer without changing the
`memory:read` scope model or the request pipeline below.

## Deployment posture

The platform runs on a VPS behind a Cloudflare Tunnel plus Cloudflare Access. Cloudflare Access is
the outer gate; the bearer described here is the application gate and stays mandatory. The
`AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY` secret is delivered from an ansible-vault-encrypted variable.

## Request pipeline

For every request to `/mcp`, in order:

1. Host allowlist (421) and Origin check (403). A present Origin must be listed; a duplicated
   Host or Origin is rejected. This runs before authentication, so a bad Origin is 403 with or
   without a token.
2. Authentication and scope, `require_scope("memory:read")`, applied to **every** MCP method,
   including `server/discover`, and to non-POST requests:
   * no token, a malformed token, an unknown, wrong, expired or revoked token: **401** with
     `WWW-Authenticate: Bearer` (no realm, no error detail; one indistinguishable body per failure);
   * a valid token without `memory:read`: **403**;
   * verification queue full: **503** with `Retry-After`; PostgreSQL unavailable: **503**;
   * per-principal bucket empty: **429** with `Retry-After` and an empty body.
3. Method allowlist, POST-only, request size limit and protocol version (`2026-07-28`).

Authentication happens before the body is read, so an unauthenticated client costs no buffering.

## Tokens

A token is `mcp_<id>.<secret>`. The non-secret prefix (`mcp_` plus random characters) selects one row of
`operations.mcp_tokens`; the row stores an Argon2id verifier, the principal, the scopes (currently only
`memory:read`, enforced by a CHECK), `created_at`, `expires_at` and `revoked_at`. Only the verifier is
stored; the secret exists once, at mint time.

Ingestion and MCP credentials are mutually unusable: separate tables, a mandatory `mcp_` prefix on this
plane (a producer token is refused before any database access), and a producer table that never holds
an `mcp_` token. Presenting an `events:ingest` token to `/mcp`, or a `memory:read` token to
`/v1/ingestion/batches`, fails with 401.

The API role `agent_context_api` has `SELECT` only on `operations.mcp_tokens`. It cannot mint, extend
or un-revoke a token.

### Verification and caching

* The bearer is parsed once. The prefix selects the row; Argon2id verification runs in a bounded worker
  pool with its own admission gate (separate from ingestion), so a flood of bad MCP tokens cannot shed
  ingestion load. An unknown prefix still pays one dummy Argon2 verification.
* Only a **successful** verification is cached, and only the principal, token id, scopes and expiry.
  The key is `HMAC-SHA256(server key, token)`; the raw token is never stored or used as a key.
* An entry lives for `min(principal_cache_ttl_seconds, time until the token expires)`. Default 30 s,
  maximum 300 s. Failures are never cached.
* **Revocation bound:** setting `revoked_at` takes effect on each replica within
  `principal_cache_ttl_seconds` (at most 30 s by default). Restarting a replica clears its cache at once.

### Rate limit

Each principal has an in-memory token bucket (`rate_limit_per_second`, `rate_limit_burst`). It is per
replica: with N replicas a principal's effective ceiling is N times the configured rate. An empty
bucket answers 429 with `Retry-After` and no body.

### Audit

Each request emits one structured log line (`agent_context_platform.mcp.auth`) with `principal`,
`token_id`, `method` (from the `mcp-method` header, `unknown` if it is not a plain method name),
`status` and `latency_ms`. Tokens, request bodies, tool arguments and driver errors are never logged.

## Settings

| Environment variable | Meaning |
| --- | --- |
| `AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY` | Server key for the cache HMAC, at least 32 characters. Required in production; replaces the former `bearer_token_verifier` setting. Elsewhere a random per-process key is used. |
| `AGENT_CONTEXT_MCP__PRINCIPAL_CACHE_TTL_SECONDS` | Cache TTL and revocation bound (default 30, max 300). |
| `AGENT_CONTEXT_MCP__PRINCIPAL_CACHE_MAX_ENTRIES` | Cache size bound (default 1024). |
| `AGENT_CONTEXT_MCP__RATE_LIMIT_PER_SECOND` / `..._BURST` | Per-principal bucket (default 5 per second, burst 20). |

Until PostgreSQL is configured the endpoint fails closed: 401 without a token, 503 with one.

## Provisioning

Use an operator DSN (a role that may `INSERT` into `operations.mcp_tokens`; the API role cannot):

```sh
export AGENT_CONTEXT_POSTGRESQL__DSN='postgresql+psycopg://<operator>@<host>/<database>'
python -m agent_context_platform.mcp.auth mint --principal codex-reader --expires 2027-01-01T00:00:00+00:00
```

The command inserts the Argon2id verifier and prints the token to stdout exactly once (the token id goes
to stderr). Store the token in the client's secret store; it cannot be recovered. `--dsn-env` names a
different environment variable.

To provision by hand, generate `mcp_<random>` and a random secret, hash `"<prefix>.<secret>"` with
Argon2id (`argon2.PasswordHasher().hash(token)`), and insert `token_id`, `token_prefix`, `token_verifier`,
`principal`, `scopes = '{memory:read}'`, `created_at` and `expires_at`.

To revoke, set `revoked_at = now()` on the row; it takes effect within the cache TTL.

The planned CLI (PLATFORM-050) will wrap this command.
