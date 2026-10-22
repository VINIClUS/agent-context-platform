# Context evidence (PLATFORM-043)

Every item of a `ContextPackageV1` cites at least one SDK `EvidenceRefV1`. This page fixes how the
platform builds those references inside SDK 0.6.0 (no SDK change), and in particular the one
metadata-only form used for events that carry no content. The code is
`src/agent_context_platform/retrieval/evidence.py`.

## Which events can be cited

A reference is built only from a stored event that:

- exists in `ledger.events` and is reconstructed through `LedgerRepository.get_event` (the
  ledger's single reconstruction path);
- has been delivered by the projection runner, at or before the package's `as_of_event_id` in
  outbox order (its `projection.outbox` row is `delivered`), so a
  pending or dead-lettered event, which never reached the graph or the search material, is never
  cited, even when a search row still names it;
- passes the SDK integrity check (`verify_event`) and has a UUIDv7 `event_id`
  (`EvidenceRefV1.source_event_id` is typed `UUID7`);
- belongs to the request scope by its envelope `context` (the repository, or the project for a
  project-scoped `decision_history`);
- for revision-scoped tools (`context_for_task`, `repository_overview`, `symbol_context`),
  occurred AND was observed strictly before the end of the requested window (the start of the
  repository's next index run), so no later fact or later knowledge is ever cited; for
  `failure_history`, occurred before `valid_to`;
- for revision-scoped tools, if it is an execution event (`quality.*`: CI and test runs,
  findings), names no workspace snapshot in its payload other than the requested one (for a
  commit: none), and if it is a failure observation, was not made in such a dirty run. CI or test
  output of a dirty workspace never backs a commit answer. Code events are exempt: a code claim
  names the run that first observed it, so a commit that adopts code first indexed in a dirty
  snapshot cites that replayed claim; whether a code fact belongs to the revision is decided by
  the revision's membership (`retrieval/revision.py`).

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
| `resource_uri` | `agent-context://events/<event_id>` (metadata-only) or `agent-context://events/<event_id>/contents/<sha256 of content_id>` (content). Opaque: no path, host, DSN or raw content ID. It is a name, not a capability: the event ID is in the reference anyway and content IDs are often guessable, so any resolver (PLATFORM-046) must re-check the caller's scope, delivery and purge state exactly as above before returning anything. |

Each item cites at most 8 references, in a stable order (their canonical JSON).

## Item content and trust

Item `content` is `untrusted_evidence`. The platform writes the structured part of it from IDs and
projected facts only; stored text (a decision, a session message, tool output, failure details)
appears only as a quoted excerpt after the label `evidence excerpt (untrusted, quoted)`, at most
480 characters with control and format characters removed. Titles name what an item is about, so
they carry producer-supplied names (paths, qualified names, session IDs): they are untrusted as
well, cleaned the same way and bounded to 160 characters, and never carry stored free text.

Content is read for an excerpt only when the event is citable (above), its reference for that
content is sanitized and not purged (checked against the ledger's `content.purged` events, not a
projection, and re-checked in the transaction that reads it), the media type is `text/plain` or `application/json`, and the content is at most
64 KiB. Larger evidence is never read: the item carries the `resource_uri` instead.

A relation whose confidence is below 1 is rendered as `heuristic (inferred, not verified)` with
its evidence kind, confidence and extractor. A CI pass after a failure is described by what the
runs show: a re-run that passed on the same commit says that no code change is evidenced.
