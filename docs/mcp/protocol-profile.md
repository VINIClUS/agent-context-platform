# MCP protocol profile

What `/mcp` implements of MCP [`2026-07-28`](https://modelcontextprotocol.io/specification/2026-07-28),
read from the code (`src/agent_context_platform/mcp/`) and the MCP Python SDK `2.2.0` it runs on.
The tables marked for checking are verified by `tests/docs/test_mcp_docs.py` against the code and
against an in-process server.

## Transport

- **One endpoint, POST only.** `POST /mcp`, the exact path (no redirect to `/mcp/`). Any other
  HTTP method answers `405` with `Allow: POST` after authentication. There is no GET stream and no
  DELETE.
- **Stateless Streamable HTTP.** Every request is complete in itself: no `initialize`, no session,
  no cookie, no affinity ([ADR 0002](../adr/0002-stateless-mcp-2026-07-28.md)). The server never
  sets `Mcp-Session-Id`.
- **JSON responses.** Results and errors are one `application/json` JSON-RPC response (the SDK app
  runs with `json_response=True`); the server never opens an SSE stream and never sends a request
  to the client. A notification is acknowledged with `202` and no body.
- **One message per body.** The body must be one JSON-RPC request or notification object; a batch
  array or a response object is `400` (`-32600`).
- **Size.** At most `AGENT_CONTEXT_MCP__MAX_REQUEST_BODY_BYTES` (1 MiB by default), counted while
  streaming; a larger declared or streamed body is `413`.

## Request pipeline

`ProtocolGuard` (`mcp/protocol_guard.py`) runs these checks in order; the first failure answers.
Rejections carry fixed bodies and never echo request data.

1. **Host** must match `AGENT_CONTEXT_MCP__ALLOWED_HOSTS` (exact, or `name:*` for any 1-5 digit
   port); missing, unlisted or repeated: `421`.
2. **Origin** is optional (a non-browser client sends none); when present it must match
   `AGENT_CONTEXT_MCP__ALLOWED_ORIGINS`; unlisted or repeated: `403`.
3. **Authentication** (`mcp/auth.py`, before any body is read): a bearer `mcp_...` token with the
   `memory:read` scope, for every method and every HTTP verb. `401` (bare `WWW-Authenticate:
   Bearer`), `403` (no `memory:read`), `429` (rate limit, `Retry-After`, empty body), `503`
   (verifier overloaded or PostgreSQL unavailable, `Retry-After`). Details in [security](security.md).
4. **POST only**: `405`, `Allow: POST`.
5. **Size limit**: `413`.
6. **Protocol version header**: `MCP-Protocol-Version` must be `2026-07-28`; absent or anything
   else is `400` with `-32022` and `data.supported = ["2026-07-28"]`.

The MCP SDK's stateless transport (`mcp.server._streamable_http_modern`) then checks, in order:

7. **Content-Type** must start with `application/json`; missing or anything else is `400` with
   the plain-text body `Invalid Content-Type header` (not JSON-RPC).
8. **Accept** must admit JSON: `application/json`, `application/*` or `*/*`. Without one of
   them (no `Accept` at all, or only `text/event-stream`) the answer is `406` with an empty body.
9. **Body**: not JSON is `-32700`; not one request or notification object is `-32600`. A
   notification is acknowledged here with `202` and no body.
10. **Routing headers**: a repeated `MCP-Protocol-Version`, `Mcp-Method` or `Mcp-Name` is `-32020`.
11. **Envelope ladder** (`mcp.shared.inbound.classify_inbound_request`): `params._meta` must carry
    the protocol version and the client capabilities (`-32602`); the version header, `Mcp-Method`
    and, for `tools/call`, `Mcp-Name` must be present and equal the body (`-32020`).
12. **Served methods**: anything other than `server/discover`, `tools/list` and `tools/call` is
    `404` with `-32601`.

The checks the transport answers before any JSON-RPC parsing, replayed as raw ASGI requests (no
client-supplied defaults) by `tests/docs/test_mcp_docs.py`:

<!-- docs-check: transport-rejections -->
| Request | Observed |
| --- | --- |
| no `Accept` header | `406`, empty body |
| `Accept: text/event-stream` only | `406`, empty body |
| `Accept: application/json` | `200` |
| `Accept: */*` | `200` |
| no `Content-Type` header | `400`, plain text `Invalid Content-Type header` |
| `Content-Type: text/plain` | `400`, plain text `Invalid Content-Type header` |

## Headers

<!-- docs-check: headers -->
| Header | Direction | Rule |
| --- | --- | --- |
| `MCP-Protocol-Version` | request | Required on every POST; must be `2026-07-28` and equal `_meta` `io.modelcontextprotocol/protocolVersion`. |
| `Mcp-Method` | request | Required; must equal the body's `method`. |
| `Mcp-Name` | request | Required for `tools/call`; must equal `params.name` (Base64 sentinel form decoded). |
| `Authorization` | request | `Bearer mcp_<id>.<secret>`; exactly one. |
| `Host` | request | Exactly one, on the allowlist. |
| `Origin` | request | Optional; checked when present; at most one. |
| `Content-Length` | request | Optional; a declared length over the limit is refused before reading. |
| `Content-Type` | request | Required; must start with `application/json` (else `400`, plain text). |
| `Accept` | request | Required; must admit `application/json` (`application/json`, `application/*` or `*/*`), else `406` with an empty body. |
| `traceparent` | request | Optional W3C trace context; parents the request span. |
| `baggage` | request | Ignored: no client baggage is ever imported. |
| `WWW-Authenticate` | response | `Bearer` on every `401`, with no realm or error detail. |
| `Retry-After` | response | Whole seconds on `429` and `503`. |
| `Allow` | response | `POST` on `405`. |

Clients should send `Accept: application/json, text/event-stream`, as the transport asks; the
server only requires the JSON part, because it answers with JSON only. `Mcp-Session-Id` and
`Last-Event-ID` are ignored if a client sends them (see
[legacy compatibility](legacy-compatibility.md)). The `tracestate` header is read with
`traceparent` but stripped from every exported span (`observability/redaction.py`).

## Per-request `_meta`

Every request carries its protocol metadata in `params._meta`
([base protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic)):

| Key | Required | Use |
| --- | --- | --- |
| `io.modelcontextprotocol/protocolVersion` | yes | Must be `2026-07-28` and equal the header. |
| `io.modelcontextprotocol/clientCapabilities` | yes | Read by the SDK; no served method needs a client capability. |
| `io.modelcontextprotocol/clientInfo` | no | Optional client identity. |
| `traceparent` | no | W3C trace context; when valid it parents the spans recorded while serving the request. `tracestate` and baggage are not imported. |

Every result carries `_meta` `io.modelcontextprotocol/serverInfo`:
`{"name": "agent-context-platform", "version": "0.1.0"}`.

Example `tools/call`:

```http
POST /mcp HTTP/1.1
Host: memory.example.com
Authorization: Bearer mcp_<id>.<secret>
Content-Type: application/json
Accept: application/json, text/event-stream
MCP-Protocol-Version: 2026-07-28
Mcp-Method: tools/call
Mcp-Name: repository_overview

{"jsonrpc": "2.0", "id": 7, "method": "tools/call",
 "params": {"name": "repository_overview",
            "arguments": {"repository_id": "repo_x", "commit_id": "<40 or 64 hex>",
                          "budget": {"max_items": 20, "max_bytes": 32000, "max_tokens": 8000}},
            "_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
                      "io.modelcontextprotocol/clientCapabilities": {}}}}
```

## Discovery

[`server/discover`](https://modelcontextprotocol.io/specification/2026-07-28/server/discover) is
served but optional: a client may call `tools/list` or `tools/call` directly. It needs the same
token as every other method.

```json
{"supportedVersions": ["2026-07-28"],
 "capabilities": {"tools": {"listChanged": false}},
 "resultType": "complete", "ttlMs": 0, "cacheScope": "private",
 "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "agent-context-platform", "version": "0.1.0"}}}
```

Only served capabilities are advertised: `tools`, with `listChanged: false` (the list changes
only on deploy and no change stream exists).

## Results and caching

- Every result has `resultType: "complete"`; nothing is deferred or streamed.
- `tools/list` carries `ttlMs: 60000` and `cacheScope: "private"`
  ([caching](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/caching)):
  the list only changes on deploy, and every method needs the caller's token, so a shared cache
  must not serve it to another principal. `server/discover` carries `ttlMs: 0`.
- The tool listing is deterministic: built once per process from the SDK's packaged schemas and
  sorted by name, so every replica answers byte for byte the same.

## Pagination

`tools/list` returns all five tools in one page and never a `nextCursor`; a `cursor` parameter is
ignored ([pagination](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/pagination)).
Tool results page through the tools' own `cursor` input and the package's `next_cursor`
([tool contracts](tool-contracts.md#budgets-truncation-and-cursors)).

## Errors

Two layers, never mixed:

- **Protocol errors** are JSON-RPC errors in the HTTP response: the request was not served.
- **Tool errors** are `tools/call` results with `isError: true` and a stable code in the text
  ([tool contracts](tool-contracts.md#error-codes)): the request was served and the tool failed.

<!-- docs-check: jsonrpc-codes -->
| Code | Name | HTTP | When |
| --- | --- | --- | --- |
| `-32700` | `PARSE_ERROR` | 400 | The body is not JSON. |
| `-32600` | `INVALID_REQUEST` | 400 | Not one request or notification object; also the bodies of the Host (`421`), Origin (`403`) and size (`413`) rejections. |
| `-32602` | `INVALID_PARAMS` | 400 | `params._meta` lacks a required key, or `tools/call` names an unknown tool. |
| `-32601` | `METHOD_NOT_FOUND` | 404 | The method is not served. |
| `-32020` | `HEADER_MISMATCH` | 400 | `Mcp-Method` or, for `tools/call`, `Mcp-Name` is missing or disagrees with the body; the version header disagrees with `_meta`; or a routing header is repeated. A missing or unsupported `MCP-Protocol-Version` is `-32022` instead. |
| `-32022` | `UNSUPPORTED_PROTOCOL_VERSION` | 400 | The version header is absent or not `2026-07-28`; `data.supported` lists `2026-07-28`. |

Not every rejection is JSON-RPC:

<!-- docs-check: non-jsonrpc -->
| HTTP | Body | When |
| --- | --- | --- |
| `401`, `403`, `503` | JSON `{"error": <code>, "message": <fixed text>}` | Authentication, scope, verifier overload or PostgreSQL unavailable ([security](security.md)). |
| `429` | empty | Rate limit; `Retry-After`. |
| `405` | empty | Not POST; `Allow: POST`. |
| `406` | empty | `Accept` does not admit `application/json`. |
| `400` | plain text `Invalid Content-Type header` | `Content-Type` missing or not `application/json`. |
| `202` | empty | A notification was accepted. |

## Limits

| Limit | Default | Setting |
| --- | --- | --- |
| Request body | 1 MiB | `AGENT_CONTEXT_MCP__MAX_REQUEST_BODY_BYTES` |
| Requests per principal, per replica | 5 per second, burst 20 | `AGENT_CONTEXT_MCP__RATE_LIMIT_PER_SECOND`, `AGENT_CONTEXT_MCP__RATE_LIMIT_BURST` |
| Revocation bound (principal cache) | 30 s (max 300) | `AGENT_CONTEXT_MCP__PRINCIPAL_CACHE_TTL_SECONDS` |
| Tool request deadline | 10 s | composer default (`DEFAULT_DEADLINE_SECONDS`); a timeout is the tool error `deadline_exceeded` |
| Tool argument length | 1000 characters of free text, 512 for other strings | checked before the composer runs ([tool contracts](tool-contracts.md#tools)) |

The rate limit is per replica: with N replicas a principal's ceiling is N times the rate.

## Disabled optional capabilities

The SDK can serve more than the memory tools need. `_ServedSurface` (`mcp/server.py`) answers
everything outside `server/discover`, `tools/list` and `tools/call` with "method not found", and
discovery advertises none of it.

<!-- docs-check: disabled-methods -->
| Method | Observed | Why it is off |
| --- | --- | --- |
| `resources/list` | `404`, `-32601` | No resource is served; evidence `resource_uri` values are names, not readable resources ([tool contracts](tool-contracts.md#content-trust)). |
| `resources/read` | `404`, `-32601` | No reader re-checks scope, delivery and purge yet; an unsigned name must never become a capability. |
| `resources/templates/list` | `404`, `-32601` | As above. |
| `prompts/list` | `404`, `-32601` | The server offers data, not prompts. |
| `prompts/get` | `404`, `-32601` | As above. |
| `completion/complete` | `404`, `-32601` | No prompt or resource arguments to complete. |
| `logging/setLevel` | `404`, `-32601` | Logs go to the operator's telemetry, never to the client. |
| `tasks/list` | `404`, `-32601` | Every call completes within one request; no deferred results. |
| `tasks/get` | `404`, `-32601` | As above. |
| `subscriptions/listen` | `404`, `-32601` | No change stream: lists change only on deploy, and a stream is per-process state. |
| `ping` | `404`, `-32601` | Liveness is a plain request (`server/discover`) or `GET /health/ready`. |
| `sampling/createMessage` | `404`, `-32601` | Client feature: the server never sends requests to the client. |
| `elicitation/create` | `404`, `-32601` | Client feature: as above. |
| `roots/list` | `404`, `-32601` | Client feature: as above; tools take explicit scope instead. |

## Tracing

A `traceparent` header parents the platform's request span; a valid `_meta` `traceparent` parents
the spans recorded while serving the request (without one, the request span stays their parent).
The `tracestate` header is extracted with `traceparent` but removed from every span before export
(`observability/redaction.py`); `_meta` `tracestate` and all client `baggage` are never imported,
and the caller's sampled flag is ignored.
The MCP SDK's own OpenTelemetry middleware, which would import the whole `_meta` carrier, is
replaced by one that keeps only `traceparent` (`mcp/telemetry.py`). Metrics and spans are
in [the telemetry contract](../operations/telemetry.md).
