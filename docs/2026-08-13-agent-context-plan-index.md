# Agent Context — Approved Plan Index

Status: approved for implementation on 2026-08-13.

## Repositories

| Repository | Responsibility | First release gate |
| --- | --- | --- |
| `agent-context-sdk` | Canonical IDs, events, content/redaction and query/MCP contracts | SDK `v0.1.0` / G1 |
| `agent-context-platform` | PostgreSQL ledger, S3 content, Neo4j projection, indexing, retrieval and `/mcp` | Platform G4 |
| `agent-context-codex` | Codex hooks/JSONL, local redaction, protected spool, uploader and Codex MCP config | Codex G4 |
| `agent-context-infra` | Compose, Ansible, Garage, private ingress, OTel, backup, restore and E2E | Homelab G6 |

These names are final and must not be replaced by a monorepo or a fifth integration repository.

## Documents

1. `specs/2026-08-13-agent-context-platform-design.md` — normative architecture, data ownership, graph, capture, MCP profile, security, retention and recovery.
2. `plans/2026-08-13-agent-context-master-roadmap.md` — cross-repository DAG, waves, task ownership, worktrees, merge protocol and gates.
3. `plans/2026-08-13-agent-context-sdk.md` — executable SDK task plan.
4. `plans/2026-08-13-agent-context-platform.md` — executable platform task plan.
5. `plans/2026-08-13-agent-context-codex.md` — executable Codex adapter task plan.
6. `plans/2026-08-13-agent-context-infra.md` — executable homelab infrastructure task plan.
7. `plans/2026-08-13-agent-context-integration.md` — cross-repository scenario and release-gate plan, implemented under `agent-context-infra/integration`.

## Approved technical profile

- PostgreSQL is the append-only event/control authority.
- Verified sanitized S3 objects are canonical content; Garage v2.3 is the maintained new-install default and existing MinIO remains adapter-compatible.
- Neo4j Community and retrieval indexes are deterministic, rebuildable projections.
- Code granularity is `repository → module → file → symbol`, using SCIP before Tree-sitter heuristics.
- Complete supported public Codex content is redacted locally before any persistence or transmission.
- Producer upload and MCP memory use distinct `events:ingest` and `memory:read` credentials.
- Codex CLI is first; initial compatibility floor is `0.147.0`.
- Memory is exposed by five read-only tools over stateless MCP `2026-07-28` using Python SDK v2.
- The application implements no initialization handshake, `Mcp-Session-Id` or sticky routing; `server/discover` is optional for callers.
- MVP bearer authentication is private-NetBird, HTTPS and read-only; Authentik OAuth/CIMD is deferred.

## Execution start

Start the four `*-001` repository scaffolds in parallel. Then execute SDK-010 → SDK-012 → SDK-011, fan out SDK-013 through SDK-016 in separate worktrees, and freeze SDK `v0.1.0` at G1. Platform storage, Codex capture and infrastructure lanes can then proceed concurrently according to the master roadmap. No consumer starts from an unmerged SDK branch.

Implementation agents must receive the exact task card, immutable base SHA, allowed/forbidden paths, consumed/produced contracts and verification commands from the master roadmap. Only wave integrators edit shared exports, registries, root Compose, migrations, generated schemas or lockfiles.
