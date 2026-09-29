# SCIP protobuf bindings

Generated, never hand-edited. Regenerate with `scripts/generate-scip-bindings.sh`.

- Source: `scip.proto` from https://github.com/scip-code/scip, tag `v0.10.0`,
  commit `1c2b6db7e560d5233c944f36e4ac1377cc6963fc`.
- Compiler: `grpcio-tools==1.84.0` (protoc for protobuf 7.35.1 gencode).
- Runtime: `protobuf==7.36.2` (exact pin in `pyproject.toml`); the gencode guard requires
  runtime 7.35.1 or newer within major 7.
- No official Python binding exists (the PyPI `scip` package is unrelated), hence generation.
