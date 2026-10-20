# Context evidence (PLATFORM-043)

Every item of a `ContextPackageV1` cites at least one SDK `EvidenceRefV1`. This page fixes how the
platform builds those references inside SDK 0.6.0 (no SDK change), and in particular the one
metadata-only form used for events that carry no content. The code is
`src/agent_context_platform/retrieval/evidence.py`.

## Which events can be cited

A reference is built only from a stored event that:

- exists in `ledger.events` and is reconstructed through `LedgerRepository.get_event` (the
  ledger's single reconstruction path);
- passes the SDK integrity check (`verify_event`) and has a UUIDv7 `event_id`
  (`EvidenceRefV1.source_event_id` is typed `UUID7`);
- belongs to the request scope by its envelope `context` (the repository, or the project for a
  project-scoped `decision_history`);
- for revision-scoped tools (`context_for_task`, `repository_overview`, `symbol_context`) and
  `failure_history`, occurred strictly before the end of the requested window (the start of the
  repository's next index run, or `valid_to`), so no later fact is ever cited.

An event that fails any rule is not cited. An item left with no reference is dropped and the
package carries the `items_without_evidence` warning.

## `content_ref`

`EvidenceRefV1.content_ref` is required. Two forms exist.

### 1. The event's own content

When the cited event carries content (a `ledger.event_content_refs` row) that is `sanitized` and
whose `content_id` is named by no `content.purged` event, the reference cites that row's
`ContentRefV1` unchanged: the content the item is about (a search hit's `content_id`), otherwise
the event's first content reference by `content_id`. Purged, `metadata_only` and
`dropped_redaction_failure` content is never cited this way.

### 2. Metadata-only: the event itself

Code-index, CI/test-run and failure events carry no content, and purged content must never be
pointed at. Such an event is cited by ONE metadata-only form that names a real, verifiable byte
string: the event's canonical metadata.

| Field | Value |
| --- | --- |
| `content_id` | `event:<event_id>` |
| `disposition` | `metadata_only` |
| `storage` | `inline` |
| `inline_id` | `<event_id>` |
| `media_type` | `application/vnd.agent-context.event-metadata+json` |
| `content_sha256` | the stored event's `integrity.event_sha256` |
| `uncompressed_bytes` | the length of that digest's preimage: the SDK canonical JSON (`canonical_json_bytes`) of the event without `integrity.event_sha256` |

A consumer verifies it by reconstructing the event (for example through a later
`resource_uri` resolver), serializing it canonically without `event_sha256`, and comparing length
and SHA-256. The form is distinguishable from real content by its `content_id` prefix, its media
type and its disposition, so it can never be mistaken for sanitized content.

## Other fields

| Field | Value |
| --- | --- |
| `evidence_kind` | Code facts: the stored kind (`scip`, `tree_sitter`, `git`, `test`); file membership and file changes are `git`; symbol definitions and their history are `scip` or `tree_sitter`. Decisions, failure observations and session content: `agent`. CI and test runs and their output: `test`. |
| `confidence` | The stored confidence for assertions (SCIP and git `1.0`, Tree-sitter syntactic `0.9`, heuristic `0.5`); symbol definitions `1.0` (SCIP) or `0.9` (Tree-sitter); records of what happened (a decision recorded, a run completed, content captured) `1.0`. |
| `valid_from` / `valid_to` | The fact's own validity when known (assertion `valid_from`, symbol interval start, decision validity, run completion); otherwise the event's `occurred_at`. An empty interval is reported as open. |
| `revision_id` | The file or symbol revision the fact belongs to, when there is one. |
| `resource_uri` | `agent-context://events/<event_id>` (metadata-only) or `agent-context://events/<event_id>/contents/<sha256 of content_id>` (content). Opaque: no path, host, DSN or raw content ID. |

Each item cites at most 8 references, in a stable order (their canonical JSON).

## Item content and trust

Item `content` is `untrusted_evidence`. The platform writes the structured part of it from IDs and
projected facts only; stored text (a decision, a session message, tool output, failure details)
appears only as a quoted excerpt after the label `evidence excerpt (untrusted, quoted)`, at most
480 characters with control and format characters removed. Titles are built from IDs and names,
cleaned the same way and bounded to 160 characters.

Content is read for an excerpt only when the event is citable (above), its reference for that
content is sanitized and not purged (checked against the ledger's `content.purged` events, not a
projection), the media type is `text/plain` or `application/json`, and the content is at most
64 KiB. Larger evidence is never read: the item carries the `resource_uri` instead.

A relation whose confidence is below 1 is rendered as `heuristic (inferred, not verified)` with
its evidence kind, confidence and extractor. A CI pass after a failure is described by what the
runs show: a re-run that passed on the same commit says that no code change is evidenced.
