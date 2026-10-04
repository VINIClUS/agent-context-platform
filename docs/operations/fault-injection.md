# Crash-test fault injection

Documented crash points for end-to-end failure-matrix scenarios (integration plan, Task E2E-004).
A scenario runs the platform as a black box (the API process or `agent-context projection run`),
arms one label through the environment, observes the process die with exit code 137, restarts it
without the label and checks recovery.

The hooks are inert by default. A disabled hook costs one `None` check and never changes behaviour.

## Environment

| Variable | Meaning |
| --- | --- |
| `AGENT_CONTEXT_FAULT_INJECTION__ENABLED` | `true` arms fault injection. Default `false`. |
| `AGENT_CONTEXT_FAULT_INJECTION__CRASH_AT` | One label from the table below. An unknown label fails settings load. |
| `AGENT_CONTEXT_FAULT_INJECTION__AFTER` | Fire on the n-th hit of that label (default `1`, minimum `1`). The count is per process. |

Both `ENABLED=true` and `CRASH_AT` are needed to arm a label. When the chosen hit arrives the
process writes `fault_injected label=<label>` (no content) to stderr and calls `os._exit(137)`:
no `finally` blocks, no cleanup, no response, like SIGKILL.

## Production refusal

Settings refuse to load when `AGENT_CONTEXT_ENVIRONMENT=production` and
`AGENT_CONTEXT_FAULT_INJECTION__ENABLED=true`. The API and the CLI therefore refuse to start.
Setting `CRASH_AT` alone, with `ENABLED` false, does nothing.

## Labels

Hits are counted per process and per label.

| Label | Process | Boundary | State if the process dies here |
| --- | --- | --- | --- |
| `content.before_s3_put` | API | `ContentService.prepare`, just before `put_verified` uploads an object-storage item. | Nothing stored anywhere. |
| `content.after_s3_put` | API | `S3BlobStore.put_verified`, after the S3 PUT returned, before the HEAD verification. | Orphan object in S3; no database row. |
| `content.after_head_verification` | API | `ContentService.prepare`, after `put_verified` returned a HEAD-verified object. | Orphan object in S3; no database row. |
| `ledger.before_db_transaction` | API | `IngestionService`, after `prepare` finished and before the database transaction opens. | Orphan object; no database row. |
| `ledger.after_event_before_outbox` | API | `LedgerRepository` append, after the event and its content-ref rows are flushed, before the redaction-report and outbox rows. | Transaction never commits: nothing persisted. |
| `ledger.before_commit` | API | `IngestionService`, after `attach` and `append` succeeded, before the transaction commits. | Transaction never commits: nothing persisted. |
| `ledger.after_commit_before_response` | API | `IngestionService`, after the commit, before the HTTP response is built. | Events, refs and outbox rows committed; the producer never sees the 200. A retry reports `existing`. |
| `projection.during_mutation` | projector | `ProjectionRunner`, inside the Neo4j write transaction after every projector ran, before it commits. | Graph unchanged; the outbox row stays leased until the lease expires. |
| `projection.before_checkpoint` | projector | `ProjectionRunner._finalize_success`, after the Neo4j commit and the fenced outbox update, before the checkpoints advance. | Graph written; outbox still leased and no checkpoint advanced; the replay re-merges idempotently. |

"After response before spool ack" is adapter-side: the scenario kills the uploader instead.

## Recovery expectations

After restarting without the label and replaying the same batch (or draining the outbox):

- no committed `ledger.event_content_refs` row references a missing object;
- no duplicate `(stream_id, stream_sequence)`, and the stream's hash chain is contiguous;
- each event has exactly one outbox row; replaying a committed batch answers `existing`;
- projection converges: the outbox row is `delivered` once, the checkpoint advances and the
  graph holds one node per event.

`tests/integration/ledger/test_fault_points.py` and `tests/integration/projection/test_fault_points.py`
run every label against the real processes.
