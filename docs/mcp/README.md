# MCP server

The platform serves its memory over MCP at `POST /mcp`: the stateless `2026-07-28` profile on
Streamable HTTP, read-only, with exactly five tools. This page is the entry point; the pages below
are the contract.

## Connect Codex

1. **Mint a token** (operator, once per client). The secret is printed once; only an Argon2id
   verifier is stored:

   ```bash
   agent-context mcp-token create --principal codex-reader --scope memory:read --expires-in 90
   ```

   `--output <new file>` writes it to a new `0600` file instead of stdout. Revoke with
   `agent-context mcp-token revoke <prefix>` (prefix from `agent-context mcp-token list`); a
   replica honours a revocation within `AGENT_CONTEXT_MCP__PRINCIPAL_CACHE_TTL_SECONDS` (30 s by
   default). See [security](security.md).

2. **Export the credentials** in the environment Codex runs in. Codex reads them by NAME; no value
   is ever written to `config.toml`:

   | Variable | Sent as |
   | --- | --- |
   | `AGENT_CONTEXT_MCP_TOKEN` | `Authorization: Bearer <token>` |
   | `AGENT_CONTEXT_CF_ACCESS_CLIENT_ID` | `CF-Access-Client-Id` (Cloudflare Access service token) |
   | `AGENT_CONTEXT_CF_ACCESS_CLIENT_SECRET` | `CF-Access-Client-Secret` |

   Cloudflare Access is the outer gate in front of the tunnel; the bearer token stays mandatory.

3. **Install the server table** with the Codex adapter
   (`agent-context-codex`, `src/agent_context_codex/install/mcp.py`), which also enables the Codex
   feature `mcp_2026_07_28` when the installed release still lists it, reads the result back with
   `codex mcp list` and smoke-tests `server/discover`:

   ```bash
   agent-context-codex install mcp --codex-home ~/.codex --dry-run   # show the plan and the table
   agent-context-codex install mcp --codex-home ~/.codex
   ```

   For a platform at `https://memory.example.com` it merges exactly this table (rendered by the
   installer's `render_install`; the URL is `<platform_url>/mcp`):

   ```toml
   [mcp_servers.agent-context]
   url = "https://memory.example.com/mcp"
   bearer_token_env_var = "AGENT_CONTEXT_MCP_TOKEN"
   env_http_headers = {CF-Access-Client-Id = "AGENT_CONTEXT_CF_ACCESS_CLIENT_ID", CF-Access-Client-Secret = "AGENT_CONTEXT_CF_ACCESS_CLIENT_SECRET"}
   enabled = true
   required = false
   enabled_tools = ["context_for_task", "repository_overview", "symbol_context", "decision_history", "failure_history"]
   startup_timeout_sec = 10
   tool_timeout_sec = 30
   ```

   The keys are Codex's own MCP server settings (`openai/codex@rust-v0.147.0:codex-rs/config/src/mcp_types.rs`,
   schema `openai/codex@rust-v0.147.0:codex-rs/core/config.schema.json`; user guide:
   [Codex MCP](https://developers.openai.com/codex/mcp)). `required = false` keeps Codex usable
   when the platform is down. Restart running Codex sessions afterwards.

The platform side of the deployment (hosts, keys, roles, readiness) is in
[deployment and scaling](deployment-and-scaling.md).

## Pages

| Page | Contents |
| --- | --- |
| [Protocol profile](protocol-profile.md) | Headers, `_meta`, discovery, results and caching, errors, limits, disabled capabilities |
| [Tool contracts](tool-contracts.md) | The five tools: inputs, outputs, schemas, budgets, cursors, annotations, error codes, content trust, authorization |
| [Security](security.md) | Bearer tokens, the request pipeline, caching, rate limits, audit |
| [Deployment and scaling](deployment-and-scaling.md) | Replicas, shared keys, the API role, dependencies, lexical-only mode, readiness |
| [Conformance](conformance.md) | The official conformance suite run and its results (PLATFORM-047) |
| [Legacy compatibility](legacy-compatibility.md) | What happens to handshake-era and HTTP+SSE clients (historical) |
| [ADR 0002](../adr/0002-stateless-mcp-2026-07-28.md) | Why the server is stateless `2026-07-28` only |

## Verified versions

Verified on 2026-10-24 against the code on this branch.

<!-- docs-check: versions -->
| Component | Version | Source |
| --- | --- | --- |
| MCP protocol | `2026-07-28` (the only supported version) | `mcp_types.version.MODERN_PROTOCOL_VERSIONS`; [specification](https://modelcontextprotocol.io/specification/2026-07-28) |
| Codex CLI, minimum | `0.147.0` | `agent-context-codex`: `src/agent_context_codex/install/capabilities.py` (`MIN_VERSION`) |
| Codex CLI, last tested | `0.147.0` | `agent-context-infra`: `integration/fixtures/versions-under-test.lock` (component `codex-cli`) |
| MCP Python SDK | `mcp` `2.2.0` (and `mcp-types` `2.2.0`) | `uv.lock`; [PyPI](https://pypi.org/project/mcp/2.2.0/), [source](https://github.com/modelcontextprotocol/python-sdk) tag `v2.2.0` |
| Agent Context SDK | `0.6.0`, wheel SHA-256 `11639c35114c8206ee5922adb3072f1ff31c07b39aaabe681e594d81e67ea079` | `pyproject.toml` `[tool.agent-context.sdk]` |
| Conformance suite | `@modelcontextprotocol/conformance@0.2.0-alpha.11`, requirements `2026-07-28` | [conformance](conformance.md); [npm](https://www.npmjs.com/package/@modelcontextprotocol/conformance) |

## Sources

Protocol and configuration claims cite only official sources:

- MCP specification `2026-07-28`: [overview](https://modelcontextprotocol.io/specification/2026-07-28),
  [changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog),
  [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http),
  [base protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic),
  [`server/discover`](https://modelcontextprotocol.io/specification/2026-07-28/server/discover),
  [tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools),
  [caching](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/caching),
  [pagination](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/pagination),
  [versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning),
  [deprecated features](https://modelcontextprotocol.io/specification/2026-07-28/deprecated),
  [SEP-2575 stateless MCP](https://modelcontextprotocol.io/seps/2575-stateless-mcp).
- MCP Python SDK: [`modelcontextprotocol/python-sdk`](https://github.com/modelcontextprotocol/python-sdk)
  at tag `v2.2.0` (the server, `mcp.shared.inbound` and `mcp_types` modules this server uses).
- Codex: `openai/codex` at tag `rust-v0.147.0` (paths above) and
  [the Codex MCP guide](https://developers.openai.com/codex/mcp).
