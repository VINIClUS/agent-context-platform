# MCP conformance

How the platform's MCP server (`/mcp`, stateless protocol `2026-07-28`) is held to the official
conformance suite, what is not applicable and why, and how to run it. PLATFORM-047; the
implementation is `scripts/run-mcp-conformance.sh`, `tests/mcp/conformance/` and
`.github/workflows/mcp-conformance.yml`.

## Pin

The suite is pinned in one place, `tests/mcp/conformance/conformance.lock`:
`@modelcontextprotocol/conformance` `0.2.0-alpha.11`, server requirements `2026-07-28`, run with
`npx --yes <package>@<version> server --requirements 2026-07-28`. It supersedes the plan's
`0.1.11`. The pin must equal the `mcp-conformance` entry of the infra lock
(`agent-context-infra/integration/fixtures/versions-under-test.lock`), quoted here verbatim; a test
(`test_the_pin_lives_in_one_place_and_matches_the_infra_lock`) compares the two:

```json
{
  "name": "mcp-conformance",
  "kind": "external-cli",
  "status": "resolved",
  "constraint": "exact",
  "version": "0.2.0-alpha.11",
  "source": {
    "package": "@modelcontextprotocol/conformance",
    "protocol_requirements": "2026-07-28"
  }
}
```

To move the pin, change `conformance.lock` and this quote together with the infra lock, run the
suite, and update the manifest: stale or unknown entries fail the run.

## Profiles

The suite connects as an anonymous MCP client and cannot send a bearer token, so both profiles are
**auth-isolated loopback profiles** (`tests/mcp/conformance/loopback.py`, test-only):

- the server binds `127.0.0.1` on an ephemeral port only;
- `LoopbackPrincipal` adds one bearer token, minted per run, to requests that carry no credential.
  That token then goes through the real authenticator, principal cache, rate limiter and
  `memory:read` scope gate. A request that brings its own credential is left untouched, so a wrong
  token is still refused (401). The protocol guard (Host, Origin, version, body) runs before any of
  it. The shim and the profiles are never importable from `src/` (a test parses every module).

| Profile | Application | Tools | Purpose |
| --- | --- | --- | --- |
| `shipping` | the shipping `create_app`: its `/mcp` mount, protocol guard, served surface and the five memory tools, bound to `SeededComposer` (a deterministic test double of the composer that signs cursors with the real `CursorCodec`) | exactly the five memory tools | every scenario that applies to the served surface must pass |
| `fixture` | the same transport stack (`build_mcp_mount`, protocol guard, stateless JSON-response handler, served surface and error mapping) serving `fixture_server.py` | the suite's fixture tools (content kinds, tool error, progress, `-32021` capability check, logging, streaming diagnostic, JSON Schema 2020-12, `x-mcp-header`) | protocol-layer coverage of the scenarios that need the suite's fixture tools |

## Running it

```bash
scripts/run-mcp-conformance.sh [OUTPUT_DIR]     # default build/mcp-conformance
```

It needs Python (`uvx uv@0.11.28 run --frozen python`, or `PYTHON=...`), Node with `npx`, and
network access for `npx` only; no database, graph or object store. For each profile it starts the
server, runs the suite into `OUTPUT_DIR/<profile>/results/`, and stops the server; then it writes
`OUTPUT_DIR/<profile>/checks.json` (all checks, by scenario), `OUTPUT_DIR/summary.json` (counts)
and scans the whole directory for credentials (`secret_scan.py`: the run's own token plus
credential shapes). The servers are always torn down. It exits non-zero on any unexpected failure,
stale or missing manifest entry, unknown scenario, bad citation or scan finding.

CI (`.github/workflows/mcp-conformance.yml`) runs it on pull requests and pushes to `main` with
pinned actions and a checksum-verified Node `22.22.0`. It scans the output again and uploads
`summary.json`, `checks.json`, the suite output and the application logs as the `mcp-conformance`
artifact only after that scan passes.

## Applicability policy

No expected-failure baseline may mask a required check. `tests/mcp/conformance/applicability.toml`
lists only scored checks, and `evaluate.py` enforces:

1. **A failed scored check must be listed** for that profile, else the run fails. Checks reported
   as `FAILURE` or `ERROR` count as failed; `WARNING`, `SKIPPED` and `INFO` do not.
2. **Capability exclusions** (`[[exclusion]]`). The check exercises only a capability the platform
   does not advertise. Each capability names the methods that would serve it. Tests verify the
   exclusion and never assume it (`test_conformance_profiles.py`):
   - the shipping `server/discover` advertises only `tools`;
   - every listed method answers `-32601` with HTTP 404;
   - for `input_requests`, no memory tool returns `InputRequiredResult`;
   - for `response_stream`, a tool call is answered with one `application/json` body even when the
     client accepts `text/event-stream`.

   A protected scenario (stateless/session behaviour, transport security, headers, authorization;
   `evaluate.PROTECTED`) can never be excluded.
3. **Fixture coverage** (`[[fixture]]`, shipping profile only). The check needs one of the suite's
   fixture tools, which the shipping server must never list (tested). It is not a waiver: the same
   check must **pass** on the fixture profile, or the run fails.
4. **The manifest stays current.** These all fail the run:
   - a listed check that passes (stale);
   - a listed check that did not run (missing);
   - an unknown scenario, in the results or the manifest;
   - a scored scenario that did not run.
5. **Citations are checked.** Every entry cites the pinned package's bundle (`dist/index.js`). The
   runner checks that the cited scenario class and literal occur in it.
6. **Unscored scenarios** (the requirement set's `not_scored`: the tasks extension and pending
   scenarios) are reported and never gate. Listing one is an error.

"Conditional on a capability" means that the check's only interaction is a method gated by that
capability. The pinned package's own schema text states the rule: a server returns `-32601` "when
a client invokes a method the server does not implement — either a genuinely unknown method, or
one gated behind a server capability the server did not advertise (e.g., calling `prompts/list`
when the `prompts` capability was not advertised)" (`dist/index.js`, the `MethodNotFoundError`
description).

`response_stream` covers one required scenario, `tools-call-with-progress`. Its fixture tool
reports progress, but the platform's stateless transport answers every POST with a single
`application/json` body (`json_response=True`). The SDK documents that this mode removes the
request-scoped back-channel and drops its notifications (`mcp/server/streamable_http.py`).
Streamable HTTP lets a server answer `application/json`. This classification is a coordinator
decision recorded for PLATFORM-047.

## Manifest

Capability exclusions (both profiles unless stated):

| Scenario | Checks | Profiles | Capability | Justification | Source |
| --- | --- | --- | --- | --- | --- |
| `resources-list` | `resources-list` | shipping, fixture | `resources` | Calls only resources/list; resources are not served (-32601). | dist/index.js scenario `resources-list`: requests `resources/list` and checks its result only |
| `resources-read-text` | `resources-read-text` | shipping, fixture | `resources` | Calls only resources/read; resources are not served (-32601). | dist/index.js scenario `resources-read-text`: requests `resources/read` and checks its result only |
| `resources-read-binary` | `resources-read-binary` | shipping, fixture | `resources` | Calls only resources/read; resources are not served (-32601). | dist/index.js scenario `resources-read-binary`: requests `resources/read` and checks its result only |
| `resources-templates-read` | `resources-templates-read` | shipping, fixture | `resources` | Calls only resources/read; resources are not served (-32601). | dist/index.js scenario `resources-templates-read`: requests `resources/read` and checks its result only |
| `prompts-list` | `prompts-list` | shipping, fixture | `prompts` | Calls only prompts/list; prompts are not served (-32601). | dist/index.js scenario `prompts-list`: requests `prompts/list` and checks its result only |
| `prompts-get-simple` | `prompts-get-simple` | shipping, fixture | `prompts` | Calls only prompts/get; prompts are not served (-32601). | dist/index.js scenario `prompts-get-simple`: requests `prompts/get` and checks its result only |
| `prompts-get-with-args` | `prompts-get-with-args` | shipping, fixture | `prompts` | Calls only prompts/get; prompts are not served (-32601). | dist/index.js scenario `prompts-get-with-args`: requests `prompts/get` and checks its result only |
| `prompts-get-embedded-resource` | `prompts-get-embedded-resource` | shipping, fixture | `prompts` | Calls only prompts/get; prompts are not served (-32601). | dist/index.js scenario `prompts-get-embedded-resource`: requests `prompts/get` and checks its result only |
| `prompts-get-with-image` | `prompts-get-with-image` | shipping, fixture | `prompts` | Calls only prompts/get; prompts are not served (-32601). | dist/index.js scenario `prompts-get-with-image`: requests `prompts/get` and checks its result only |
| `completion-complete` | `completion-complete` | shipping, fixture | `completions` | Calls only completion/complete for a prompt argument; completions are not served (-32601). | dist/index.js scenario `completion-complete`: requests `completion/complete` with ref/prompt `test_prompt_with_arguments` |
| `caching` | `sep-2549-prompts-list-caching-hints` | shipping, fixture | `prompts` | Checks cache hints on prompts/list, which is not served; the tools/list hints in the same scenario pass. | dist/index.js scenario `caching`: check `sep-2549-prompts-list-caching-hints` requests `prompts/list` |
| `caching` | `sep-2549-resources-list-caching-hints`, `sep-2549-resources-templates-list-caching-hints` | shipping, fixture | `resources` | Checks cache hints on resources/list and resources/templates/list, which are not served. | dist/index.js scenario `caching`: checks request `resources/list` and `resources/templates/list` |
| `input-required-result-basic-elicitation` | `sep-2322-elicitation-incomplete` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with an elicitation/create input request; the platform never asks the client for input. | dist/index.js scenario `input-required-result-basic-elicitation`: calls `test_input_required_result_elicitation` and requires an InputRequiredResult |
| `input-required-result-basic-sampling` | `sep-2322-sampling-incomplete` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with a sampling/createMessage input request; the platform never asks the client for input. | dist/index.js scenario `input-required-result-basic-sampling`: calls `test_input_required_result_sampling` and requires an InputRequiredResult |
| `input-required-result-basic-list-roots` | `sep-2322-list-roots-incomplete` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with a roots/list input request; the platform never asks the client for input. | dist/index.js scenario `input-required-result-basic-list-roots`: calls `test_input_required_result_list_roots` and requires an InputRequiredResult |
| `input-required-result-request-state` | `sep-2322-request-state-incomplete` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with an input request with requestState; the platform never asks the client for input. | dist/index.js scenario `input-required-result-request-state`: calls `test_input_required_result_request_state` and requires an InputRequiredResult |
| `input-required-result-multiple-input-requests` | `sep-2322-multiple-inputs-incomplete` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with several input requests; the platform never asks the client for input. | dist/index.js scenario `input-required-result-multiple-input-requests`: calls `test_input_required_result_multiple_inputs` and requires an InputRequiredResult |
| `input-required-result-multi-round` | `sep-2322-multi-round-r1` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with input requests over several rounds; the platform never asks the client for input. | dist/index.js scenario `input-required-result-multi-round`: calls `test_input_required_result_multi_round` and requires an InputRequiredResult |
| `input-required-result-non-tool-request` | `sep-2322-non-tool-incomplete` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with an input request from prompts/get; the platform never asks the client for input. | dist/index.js scenario `input-required-result-non-tool-request`: calls `test_input_required_result_prompt` and requires an InputRequiredResult |
| `input-required-result-result-type` | `sep-2322-result-type-included` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with resultType input_required; the platform never asks the client for input. | dist/index.js scenario `input-required-result-result-type`: calls `test_input_required_result_elicitation` and requires an InputRequiredResult |
| `input-required-result-tampered-state` | `sep-2322-reject-tampered-state` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with a signed requestState it can verify; the platform never asks the client for input. | dist/index.js scenario `input-required-result-tampered-state`: calls `test_input_required_result_tampered_state` and requires an InputRequiredResult |
| `input-required-result-capability-check` | `sep-2322-respect-client-capabilities` | shipping, fixture | `input_requests` | Needs a tool that returns InputRequiredResult with an input request gated on client capabilities; the platform never asks the client for input. | dist/index.js scenario `input-required-result-capability-check`: calls `test_input_required_result_capabilities` and requires an InputRequiredResult |
| `tools-call-with-progress` | `tools-call-with-progress` | shipping, fixture | `response_stream` | Requires notifications/progress on the tools/call response stream; the JSON-response transport has no request-scoped notification wire (the fixture tool reports progress and the SDK drops it). | dist/index.js scenario `tools-call-with-progress`: requires at least three `notifications/progress` for `test_tool_with_progress` |

Fixture coverage (shipping profile; each check passes on the fixture profile):

| Scenario | Checks | Fixture tool | Justification | Source |
| --- | --- | --- | --- | --- |
| `tools-call-simple-text` | `tools-call-simple-text` | `test_simple_text` | A fixture tool the shipping server must not list; passes on the fixture profile. | dist/index.js scenario `tools-call-simple-text`: calls the fixture tool `test_simple_text` |
| `tools-call-image` | `tools-call-image` | `test_image_content` | A fixture tool the shipping server must not list; passes on the fixture profile. | dist/index.js scenario `tools-call-image`: calls the fixture tool `test_image_content` |
| `tools-call-audio` | `tools-call-audio` | `test_audio_content` | A fixture tool the shipping server must not list; passes on the fixture profile. | dist/index.js scenario `tools-call-audio`: calls the fixture tool `test_audio_content` |
| `tools-call-embedded-resource` | `tools-call-embedded-resource` | `test_embedded_resource` | A fixture tool the shipping server must not list; passes on the fixture profile. | dist/index.js scenario `tools-call-embedded-resource`: calls the fixture tool `test_embedded_resource` |
| `tools-call-mixed-content` | `tools-call-mixed-content` | `test_multiple_content_types` | A fixture tool the shipping server must not list; passes on the fixture profile. | dist/index.js scenario `tools-call-mixed-content`: calls the fixture tool `test_multiple_content_types` |
| `tools-call-error` | `tools-call-error` | `test_error_handling` | A fixture tool the shipping server must not list; passes on the fixture profile. | dist/index.js scenario `tools-call-error`: calls the fixture tool `test_error_handling` |
| `server-stateless` | `sep-2575-server-rejects-undeclared-capability`, `sep-2575-missing-capability-http-400` | `test_missing_capability` | Diagnostic tool needing the client's sampling capability (-32021, HTTP 400); passes on the fixture profile. | dist/index.js scenario `server-stateless`: calls the fixture tool `test_missing_capability` |
| `server-stateless` | `sep-2575-http-server-no-independent-requests-on-stream` | `test_streaming_elicitation` | Diagnostic tool for the response stream; passes on the fixture profile. | dist/index.js scenario `server-stateless`: calls the fixture tool `test_streaming_elicitation` |
| `server-stateless` | `sep-2575-server-no-log-without-loglevel` | `test_logging_tool` | Diagnostic tool that logs without a requested log level; passes on the fixture profile. | dist/index.js scenario `server-stateless`: calls the fixture tool `test_logging_tool` |

## Last run

Run on 2026-10-24, pin `0.2.0-alpha.11`, requirements `2026-07-28`, against branch
`claude/keen-shannon-danae0-platform-047`:

| Profile | Scored scenarios | Checks passed | Excluded | Covered by fixture | Other (warning, skipped, info) | Unexpected failures | Unscored scenarios (failing, not gating) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| shipping | 37 | 65 | 24 | 10 | 11 | 0 | 13 (11) |
| fixture | 37 | 75 | 24 | 0 | 11 | 0 | 13 (9) |

The manifest has 32 entries covering 34 checks: 23 capability exclusions (24 checks per profile)
and 9 fixture-coverage entries (10 checks). The secret scan found nothing.
