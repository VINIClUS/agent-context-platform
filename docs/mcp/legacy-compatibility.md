# MCP legacy compatibility

> **Historical.** This page describes protocol mechanisms that are NOT part of MCP `2026-07-28` and
> that this server does not implement: the `initialize` handshake, transport sessions and earlier
> transports. Nothing here is a supported way to connect. Use the current setup in the
> [MCP README](README.md) and the [protocol profile](protocol-profile.md). It exists so that an
> operator who sees an old client fail knows what happened and why.

## What changed

Protocol revisions up to `2025-11-25` began every connection with an `initialize` request and a
`notifications/initialized` notification, and on Streamable HTTP could bind later requests to a
session (`Mcp-Session-Id`, ended with HTTP DELETE), a standalone GET stream for server-initiated
messages, and resumable SSE (`Last-Event-ID`). The `2024-11-05` HTTP+SSE transport used a GET SSE
endpoint plus a separate POST endpoint. `2026-07-28` removed the handshake and sessions: every
request carries its version and capabilities in `_meta` and its headers
([changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog),
[Streamable HTTP: backward compatibility](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http),
[deprecated features](https://modelcontextprotocol.io/specification/2026-07-28/deprecated)).

This server supports only `2026-07-28` ([ADR 0002](../adr/0002-stateless-mcp-2026-07-28.md)). The
MCP Python SDK it runs on still contains a handshake-era transport; the protocol guard keeps it
unreachable, because every request without `MCP-Protocol-Version: 2026-07-28` is rejected before
the SDK sees it.

## What a legacy client gets today

Every row below is replayed against an in-process server by `tests/docs/test_mcp_docs.py`. All
requests carry a valid `memory:read` token, so they pass authentication; without one the answer is
`401`, as for any client.

<!-- docs-check: legacy -->
| Legacy request | Observed | Meaning for the client |
| --- | --- | --- |
| `initialize` without `MCP-Protocol-Version` | `400`, `-32022`, `data.supported = ["2026-07-28"]` | A modern-era server: retry with `2026-07-28`, do not fall back. |
| `initialize` with `MCP-Protocol-Version: 2025-06-18` | `400`, `-32022` | As above. |
| `initialize` with `MCP-Protocol-Version: 2025-11-25` | `400`, `-32022` | As above. |
| `initialize` with `2026-07-28` headers and `_meta` | `404`, `-32601` | The handshake is not a method of this protocol. |
| `notifications/initialized` with `2026-07-28` headers and `_meta` | `202`, no body | Accepted like any notification; it has no effect. |
| `GET /mcp` (standalone SSE stream) | `405`, `Allow: POST` | No server-initiated stream exists. |
| `DELETE /mcp` (end a session) | `405`, `Allow: POST` | There is no session to end. |
| `Mcp-Session-Id` on a `2026-07-28` request | `200`; no `Mcp-Session-Id` in the response | The header is ignored; no session is ever minted. |
| `Last-Event-ID` on a `2026-07-28` request | `200` | The header is ignored; streams are not resumable. |
| `GET /sse` (HTTP+SSE endpoint) | `404` | The `2024-11-05` transport is not hosted. |
| `POST /messages` (HTTP+SSE endpoint) | `404` | As above. |

A client that supports both eras and probes with a modern request first sees a recognized modern
JSON-RPC error (`-32022` carries `data.supported`), so it should retry with `2026-07-28` rather
than fall back to `initialize`, as the specification's backward-compatibility rules ask.
