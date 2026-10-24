# MCP deployment and scaling

`/mcp` is served by the `platform-api` role (the default command of the platform image; see the
README's container-image table). Because the profile is stateless
([ADR 0002](../adr/0002-stateless-mcp-2026-07-28.md)), replicas need no affinity: any replica can
answer any request, including the next page of another replica's answer.

The deployment procedure itself (Compose services, Cloudflare Tunnel, secret delivery from
ansible-vault) belongs to the infrastructure repositories; see `agent-context-infra`
`runbooks/deploy.md` and `runbooks/secrets.md` rather than repeating them here.

## Replicas

- Run as many `platform-api` replicas as needed behind the tunnel, with no sticky routing. Nothing
  is kept per client between requests; the server never mints a session.
- Every replica must share `AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY`: it signs and verifies pagination
  cursors, so a cursor one replica issues is valid on another only with the same key. Rotating it
  invalidates outstanding cursors (`cursor_invalid`; clients start again from the first page).
- `AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY` keys each replica's in-memory principal cache. It is required
  in production; replicas need not share it for correctness (each cache is private), but keeping one
  value in the secret store with the cursor key is simplest.
- Per-replica state is limited to caches that are safe to lose:
  - the principal cache: a revoked token keeps working on a replica for at most
    `AGENT_CONTEXT_MCP__PRINCIPAL_CACHE_TTL_SECONDS` (30 s by default, at most 300); a restart
    clears it;
  - the rate-limit buckets: `AGENT_CONTEXT_MCP__RATE_LIMIT_PER_SECOND` and
    `AGENT_CONTEXT_MCP__RATE_LIMIT_BURST` apply per replica, so a principal's effective ceiling is
    the replica count times the rate;
  - the Argon2id verifier pool, sized by `AGENT_CONTEXT_INGESTION__ARGON2_MAX_CONCURRENCY` and
    `AGENT_CONTEXT_INGESTION__ARGON2_MAX_QUEUE_DEPTH` (MCP has its own verifier and admission gate
    with these bounds); beyond the queue, requests fail fast with `503` and `Retry-After`.

## Settings

| Variable | Required in production | Notes |
| --- | --- | --- |
| `AGENT_CONTEXT_ENVIRONMENT` | set to `production` | Enables the completeness check below. |
| `AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY` | yes | At least 32 characters; the same on every replica. |
| `AGENT_CONTEXT_MCP__TOKEN_HMAC_KEY` | yes | At least 32 characters. |
| `AGENT_CONTEXT_MCP__ALLOWED_HOSTS` | yes, in practice | JSON list; the default only allows loopback, so the public hostname must be listed, e.g. `["memory.example.com"]`. |
| `AGENT_CONTEXT_MCP__ALLOWED_ORIGINS` | no | JSON list; Codex sends no `Origin`. Only browser clients need an entry. |
| `AGENT_CONTEXT_MCP__MAX_REQUEST_BODY_BYTES` | no | Default 1 MiB. |
| `AGENT_CONTEXT_POSTGRESQL__DSN` | yes | The API login (below): token lookup, ingestion, readiness. |
| `AGENT_CONTEXT_POSTGRESQL__API_DSN` | no (falls back to `AGENT_CONTEXT_POSTGRESQL__DSN`) | The composer's reads, when they use a separate login. |
| `AGENT_CONTEXT_NEO4J__URI`, `AGENT_CONTEXT_NEO4J__USERNAME`, `AGENT_CONTEXT_NEO4J__PASSWORD` | yes | The graph the tools read. |
| `AGENT_CONTEXT_S3__ENDPOINT_URL`, `AGENT_CONTEXT_S3__ACCESS_KEY_ID`, `AGENT_CONTEXT_S3__SECRET_ACCESS_KEY` | yes | Sanitized content for evidence excerpts. |
| `AGENT_CONTEXT_SEARCH__MODEL_DIR` | no | Enables vector search (lexical-only mode below). |

With `AGENT_CONTEXT_ENVIRONMENT=production` the API refuses to start unless both MCP keys and the
PostgreSQL, Neo4j and S3 settings are present. Secrets can be mounted as files instead of
environment variables through `AGENT_CONTEXT_SECRETS_DIR` (one `0400` file per variable, named
after it; both MCP keys are secret file names).

## The API read role

The API reads PostgreSQL as the API role, never the projector role (which can write projection
state). In production `AGENT_CONTEXT_POSTGRESQL__DSN` (and `AGENT_CONTEXT_POSTGRESQL__API_DSN`, if
the composer's reads use a separate login) logs in as a LOGIN member (`INHERIT`) of
`agent_context_api` with no grant of its own; the migrations grant that role everything the
composer and readiness read, including migration `20261022_0001`'s `SELECT` on
`public.alembic_version`, `projection.projection_checkpoints` and `projection.dead_letters`
(`API_READ_TABLES` in `src/agent_context_platform/operations/readiness.py`). A missing grant
fails closed: the request answers `context_unavailable` and readiness reports the schema
`forbidden`. Token lookup reads `operations.mcp_tokens` through `AGENT_CONTEXT_POSTGRESQL__DSN`
(`SELECT` only: the API role cannot mint, extend or un-revoke a token).

## Composer dependencies

At start-up the API builds the tools' composer when PostgreSQL, a complete Neo4j login and S3
are configured. It needs:

| Dependency | Used for | Unavailable |
| --- | --- | --- |
| PostgreSQL (API role) | ledger evidence, lexical search, projection freshness | `context_unavailable` (retryable) |
| Neo4j | code graph, revisions, decisions and failures | `context_unavailable` (retryable) |
| S3-compatible store | sanitized content for bounded excerpts | `context_unavailable` (retryable) |
| Model directory (optional) | query embeddings for vector search | lexical-only mode |

Until all three stores are configured every tool call answers `context_unavailable`; the MCP
surface itself (discovery, listing, authentication) still works.

## Lexical-only mode

Without `AGENT_CONTEXT_SEARCH__MODEL_DIR` the API has no embedder and the tools search lexically
only; answers then carry the warning `vector_search_unavailable`. A configured model directory is
verified at start-up and a digest mismatch refuses to start (fail closed). The projection worker
has its own lexical-only switch; see the README's "Search and embeddings" section.

## Readiness

Route traffic only to replicas whose `GET /health/ready` answers `200`. It checks, each within
two seconds, that PostgreSQL answers with the schema at exactly the image's Alembic head and the
API role holds every `API_READ_TABLES` grant, that Neo4j accepts a connectivity check, and that the
S3 bucket answers. `GET /health/live` is process liveness only. Neither endpoint needs a token, and
neither is part of `/mcp`.
