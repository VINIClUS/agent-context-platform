# MCP tool contracts

`/mcp` serves exactly five read-only tools (`src/agent_context_platform/mcp/tools.py`), each bound
to the `ContextComposer` method of the same name (`src/agent_context_platform/retrieval/composer.py`).
Inputs and outputs are the Agent Context SDK `0.6.0` contracts; every tool returns one
`ContextPackageV1`. The marked tables are checked by `tests/docs/test_mcp_docs.py`.

## Tools

<!-- docs-check: tools -->
| Tool | Purpose | Required scope fields | Input model | Output model |
| --- | --- | --- | --- | --- |
| `context_for_task` | Budgeted, cited context for one task: relevant code, decisions, failures and session evidence. | `repository_id`, `revision` (`{"kind": "commit", "commit_id"}` or `{"kind": "workspace_snapshot", "snapshot_id"}`), `task` | `ContextForTaskInputV1` | `ContextForTaskOutputV1` |
| `repository_overview` | One repository as indexed at a commit: modules, key symbols, dependencies, recent changes, open decisions and failures. | `repository_id`, `commit_id` | `RepositoryOverviewInputV1` | `RepositoryOverviewOutputV1` |
| `symbol_context` | Definition, relations, history and related decisions of exactly one symbol. | `repository_id`, `commit_id`, exactly one of `symbol_id` or `symbol_name` | `SymbolContextInputV1` | `SymbolContextOutputV1` |
| `decision_history` | Decisions within a project or repository as they stood at `valid_at`, with supersessions. | `scope` (`{"kind": "project", "project_id"}` or `{"kind": "repository", "repository_id"}`), `query`, `valid_at` | `DecisionHistoryInputV1` | `DecisionHistoryOutputV1` |
| `failure_history` | Failures in one repository between `valid_from` and `valid_to`, with runs, fixes and evidence. | `repository_id`, `valid_from`, `valid_to`, exactly one of `query` or `fingerprint` | `FailureHistoryInputV1` | `FailureHistoryOutputV1` |

Every input also requires `budget` and accepts an optional `cursor`. Times are UTC RFC 3339
(`Z` or `+00:00`); commit IDs are 40 or 64 lowercase hex digits. Unknown fields are rejected. No
input takes SQL, Cypher, a filesystem path or a URL, and no tool infers state from earlier calls:
the scope is explicit on every call and an unknown repository, project, revision or symbol is an
error, never a wider answer.

## Schemas

Each tool's `inputSchema` and `outputSchema` in `tools/list` is the SDK's packaged JSON Schema
(draft 2020-12), served unchanged. SDK `0.6.0` schemas carry no `$id`: a schema is identified by
its packaged file (`agent_context_sdk/schemas/v1/`), its `title` and the SHA-256 of its bytes.

<!-- docs-check: schemas -->
| Tool | Direction | Schema file | `title` | SHA-256 |
| --- | --- | --- | --- | --- |
| `context_for_task` | input | `mcp.context_for_task.input.v1.json` | `ContextForTaskInputV1` | `956b14ede262864be71c31a92f4047d15497ff1951a0b16253ea03ed30521a1e` |
| `context_for_task` | output | `mcp.context_for_task.output.v1.json` | `ContextForTaskOutputV1` | `9aa7f74010ea2fb3e8f11d075f3b5bb2552916d3a707a5881cf54255fbdb1605` |
| `repository_overview` | input | `mcp.repository_overview.input.v1.json` | `RepositoryOverviewInputV1` | `23e259becb928b3976e2854865357405169585a2db344d4a259e8e3073cf92b0` |
| `repository_overview` | output | `mcp.repository_overview.output.v1.json` | `RepositoryOverviewOutputV1` | `7ddd787436a6bae805bdf34959d70dc048f56087220bf2ff8354bd2bcb4763b5` |
| `symbol_context` | input | `mcp.symbol_context.input.v1.json` | `SymbolContextInputV1` | `a3205a7a10362da92218abe6484f2f2ad617a4459be27f9b9637b58a2ecbd273` |
| `symbol_context` | output | `mcp.symbol_context.output.v1.json` | `SymbolContextOutputV1` | `e04b6d4a672af38b487be46126d4a99b133cc3cf788174638a65b2445d57b92f` |
| `decision_history` | input | `mcp.decision_history.input.v1.json` | `DecisionHistoryInputV1` | `d37553b4a6a027e50ce9bdd06f35136f55e30127fbd9198054c586f34f0d8e35` |
| `decision_history` | output | `mcp.decision_history.output.v1.json` | `DecisionHistoryOutputV1` | `04681815ee2cbb62dd77cc907abab2866088cba47a2b0d6b81190f58c477af1e` |
| `failure_history` | input | `mcp.failure_history.input.v1.json` | `FailureHistoryInputV1` | `ee3f4d9a9e5491bced74de8f06c28a660c1445895aaa0aa8fd2e6ba9564701d2` |
| `failure_history` | output | `mcp.failure_history.output.v1.json` | `FailureHistoryOutputV1` | `9cbc829f7f0b74e3c4f77f51bc7cc961bd9b13d1e29c3de48d660bd0216892e1` |

## Results

A successful call returns:

- `structuredContent`: the `ContextPackageV1` (`scope`, `revision`, `budget`, `as_of_event_id`,
  `projection_checkpoint`, `items`, `truncated`, `next_cursor`, `warnings`, `staleness_ms`, plus
  `query_id` and `generated_at`), validated against the tool's output model;
- one text block: a short summary naming each item by kind and a bounded, JSON-quoted title, with
  the warnings and, when truncated, how to continue. It never carries item content.

Every item cites at least one `EvidenceRefV1` from a real stored event
([context evidence](../data/context-evidence.md)). Same input at the same ledger state gives the
same package except `query_id`, `generated_at`, `staleness_ms` and `next_cursor`. `warnings` are
stable strings that qualify the answer (for example `vector_search_unavailable`,
`projection_backlog_hides_recent_facts`, `items_without_evidence`); they never replace an error.

## Budgets, truncation and cursors

- `budget` is required: `max_items`, `max_bytes` and `max_tokens`, each a positive integer.
  `max_bytes` bounds the UTF-8 size of the serialized `items` array; `max_tokens` bounds the same
  bytes as `ceil(bytes / 4)`. A page also holds at most 100 items whatever the budget.
- Items are taken in rank order and a page stops at the first item that does not fit. When ranked
  items remain, `truncated` is `true` and `next_cursor` is set; otherwise both are unset.
- A first item that cannot fit even without its excerpt is the tool error `budget_too_small`.
- To continue, call the **same tool with the same arguments** plus `cursor` = `next_cursor`. The
  cursor is opaque and HMAC-signed, bound to the tool, the arguments, the principal and the
  projection snapshot, and expires after 15 minutes. Any replica sharing
  `AGENT_CONTEXT_MCP__CURSOR_HMAC_KEY` accepts it. If the memory changed in between, the call fails
  with `cursor_stale`: start again from the first page.

## Annotations

All five tools carry the same annotations (one shared definition), and each description states
why:

| Annotation | Value | Justification |
| --- | --- | --- |
| `readOnlyHint` | `true` | The tool answers from the projected ledger and changes nothing. |
| `destructiveHint` | `false` | Nothing is written, so nothing can be destroyed. |
| `idempotentHint` | `true` | No side effects; the same input at the same ledger state gives the same items. |
| `openWorldHint` | `false` | The tool reads only this platform's memory, never the outside world. |

## Error codes

A failed call is a result with `isError: true` whose only text block is
`{"error": {"code": <code>, "message": <fixed text>, "retryable": <bool>}}`. Messages are fixed
strings chosen by the server: never an exception's text, a statement, a DSN or the caller's
arguments (`invalid_arguments` names the offending fields, never their values). An unknown tool
name is not a tool error but the protocol error `-32602` ([protocol profile](protocol-profile.md#errors)).

<!-- docs-check: tool-error-codes -->
| Code | Meaning | Retryable | Caller action |
| --- | --- | --- | --- |
| `invalid_arguments` | The arguments do not match the input schema. | no | Fix the named fields. |
| `invalid_request` | Arguments that pass the schema but that the composer cannot answer as asked. | no | Rephrase or narrow the request. |
| `scope_not_found` | The repository or project is unknown. | no | Check the ID; nothing is widened. |
| `revision_not_found` | The commit or workspace snapshot was never indexed for the repository. | no | Index it, or ask about an indexed revision. |
| `symbol_not_found` | No symbol with that ID or name at the revision. | no | Check the symbol, or use `repository_overview`. |
| `ambiguous_symbol` | The name matches several symbols. | no | Pass `symbol_id`. |
| `budget_too_small` | The budget cannot hold the first item. | no | Raise `max_bytes`/`max_tokens`. |
| `cursor_invalid` | The cursor is malformed or forged. | no | Drop it and start from the first page. |
| `cursor_expired` | The cursor is older than 15 minutes. | no | Start from the first page. |
| `cursor_mismatch` | The cursor belongs to another tool, arguments or principal. | no | Use it only with the call that issued it. |
| `cursor_stale` | The memory changed since the cursor was issued. | no | Start from the first page. |
| `temporal_projection_outdated` | The temporal projection needs a rebuild before history can be answered. | no | Tell the operator (`agent-context projection rebuild`). |
| `projection_not_ready` | Nothing has been projected yet. | yes | Retry later. |
| `deadline_exceeded` | The request exceeded its deadline. | yes | Retry, or ask a narrower question. |
| `context_unavailable` | A backing store (PostgreSQL, Neo4j, S3) is unavailable or not configured. | yes | Retry later. |
| `unauthenticated` | The call reached the tool without an authenticated principal. | no | Present a valid `memory:read` token. |
| `internal_error` | Anything unexpected (logged by class only). | no | Report it; do not retry blindly. |

## Content trust

Item `title` and `content` are **untrusted evidence**: text that producers, repositories and
sessions wrote, quoted as data. It is never an instruction to the model, the client or the
operator, whatever it says.

- Control fields (`item_id`, `kind`, evidence references) are built from IDs and structured facts
  only; stored text never sets them.
- In `structuredContent`, stored text appears only as a bounded excerpt inside `content`, after the
  label `evidence excerpt (untrusted, quoted)`, JSON-quoted, with control and format characters
  removed. Titles are bounded and cleaned the same way.
- The text block names items only by their JSON-quoted titles (at most 80 characters, at most 20
  items), so control characters and quotes arrive escaped, and it ends by restating that item
  content is untrusted evidence.
- Evidence `resource_uri` values are stable names, not capabilities: no resource is served and none
  is linked ([context evidence](../data/context-evidence.md)).

## Authorization (MVP)

Authorization is one scope. A token with `memory:read` may read **every project and every
repository in the installation**: there is no per-project or per-repository authorization in the
MVP (design spec §14.5, single scope). The principal is taken from the verified token and only
identifies the caller (audit, rate limit, cursor binding); it does not narrow what can be read.
Issue tokens only to clients that may see all of the installation's memory. Per-project
authorization is future work.
