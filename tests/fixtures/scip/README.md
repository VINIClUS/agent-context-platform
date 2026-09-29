# SCIP fixtures

`scip-python-basic.scip` is a real index, produced once and committed as bytes. Sources are in
`scip-python-basic-src/pkg/`.

- Tool: `@sourcegraph/scip-python` 0.6.6 (reports `scip-python` / `0.6.6`), run through npm's `npx`.
- Toolchain: Node.js v24.18.0, npm-managed; it indexes Python and reports the stdlib as
  `python-stdlib 3.11`.
- Command (from the copied `scip-python-basic-src` directory, made a Git repo, package dir `pkg/`):
  `npx --yes @sourcegraph/scip-python@0.6.6 index . --project-name=fixturepkg --project-version=0.0.1`
- The tool records no `arguments` and no position encoding; it writes deprecated `range`
  arrays. `project_root` inside the file is the temp directory used at generation time and
  is treated as untrusted, informational text.

Hand-crafted (built once with the generated bindings, protobuf 7.36.2):

- `handcrafted-invalid.scip`: one valid definition and reference plus a line beyond the
  document, an end column past the line, start after end, a negative value, wrong arity, an
  empty symbol, and a second document with an unsafe path (`../escape.py`).
- `malformed-truncated.scip`: the first 200 bytes of the real index.
- `malformed-garbage.scip`: bytes that are not a protobuf message.
- `malformed-bad-utf8.scip`: invalid UTF-8 in `Metadata.tool_info.name`.
