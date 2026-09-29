#!/usr/bin/env bash
# Regenerate the SCIP protobuf bindings from the vendored scip.proto.
#
# scip.proto is scip-code/scip tag v0.10.0 (commit 1c2b6db7e560d5233c944f36e4ac1377cc6963fc).
# To upgrade: replace scip.proto from the new tag, update the tag/commit above and in
# src/agent_context_platform/indexing/scip_pb/README.md, run this script, and keep the
# generated runtime guard in step with the exact-pinned `protobuf` in pyproject.toml.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="$root/src/agent_context_platform/indexing/scip_pb"

uvx --from grpcio-tools==1.84.0 python -m grpc_tools.protoc \
  --proto_path="$out" \
  --python_out="$out" \
  --pyi_out="$out" \
  "$out/scip.proto"
