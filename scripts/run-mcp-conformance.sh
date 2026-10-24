#!/usr/bin/env bash
# Run the pinned MCP conformance suite against both loopback profiles (PLATFORM-047).
#
#   scripts/run-mcp-conformance.sh [OUTPUT_DIR]     (default: build/mcp-conformance)
#
# For each profile (shipping, fixture) it starts tests/mcp/conformance/loopback.py on an
# ephemeral 127.0.0.1 port, runs the suite pinned in tests/mcp/conformance/conformance.lock with
# `npx --yes` at its exact version, and stops the server. It then evaluates both result sets
# against tests/mcp/conformance/applicability.toml (writing <out>/<profile>/checks.json and
# <out>/summary.json) and scans the whole output directory for credentials. No external service
# is needed. It always tears the servers down, and exits non-zero on any unexpected failure,
# stale manifest entry or finding.
#
# PYTHON overrides the interpreter command (default: uvx uv@0.11.28 run --frozen python).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
conformance_dir="tests/mcp/conformance"
out="${1:-build/mcp-conformance}"

if [[ -n "${PYTHON:-}" ]]; then
  read -r -a python <<<"$PYTHON"
else
  python=(uvx uv@0.11.28 run --frozen python)
fi

pin() {
  sed -n "s/^$1 = \"\(.*\)\"\$/\1/p" "$conformance_dir/conformance.lock"
}
package="$(pin package)"
version="$(pin version)"
requirements="$(pin requirements)"
if [[ -z "$package" || -z "$version" || -z "$requirements" ]]; then
  echo "conformance: incomplete pin in $conformance_dir/conformance.lock" >&2
  exit 2
fi

pids=()
# shellcheck disable=SC2317  # reached through the trap below
cleanup() {
  local pid
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

rm -rf "$out"
mkdir -p "$out"

# One bearer token per run, known only to the servers and the scanner (never written to disk).
AGENT_CONTEXT_CONFORMANCE_TOKEN="mcp_conform01.$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
export AGENT_CONTEXT_CONFORMANCE_TOKEN

wait_for_port() {
  local port_file="$1" pid="$2" attempt
  for attempt in $(seq 1 120); do
    if [[ -s "$port_file" ]]; then
      cat "$port_file"
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    if ((attempt % 20 == 0)); then
      echo "conformance: still waiting for the server ($attempt)" >&2
    fi
    sleep 0.5
  done
  return 1
}

for profile in shipping fixture; do
  mkdir -p "$out/$profile"
  "${python[@]}" "$conformance_dir/loopback.py" --profile "$profile" \
    --port-file "$out/$profile/port" >"$out/$profile/app.log" 2>&1 &
  server=$!
  pids+=("$server")
  if ! port="$(wait_for_port "$out/$profile/port" "$server")"; then
    echo "conformance: the $profile server did not start; see $out/$profile/app.log" >&2
    exit 1
  fi
  echo "conformance: $profile profile on 127.0.0.1:$port, suite $package@$version ($requirements)"
  suite_status=0
  npx --yes "$package@$version" server \
    --url "http://127.0.0.1:$port/mcp" \
    --requirements "$requirements" \
    --output-dir "$out/$profile/results" >"$out/$profile/suite.txt" 2>&1 || suite_status=$?
  # The suite exits non-zero for every failing scenario, excluded ones included: the evaluator
  # decides. A run that produced no results fails there ("scored scenario did not run").
  echo "conformance: $profile suite exit status $suite_status"
  kill "$server" 2>/dev/null || true
  wait "$server" 2>/dev/null || true
  rm -f "$out/$profile/port"
done

# The package directory holds the frozen requirement sets and the bundle the manifest cites.
# shellcheck disable=SC2016  # expanded by the shell npx starts, not by this one
suite_bin="$(npx --yes -p "$package@$version" -c 'readlink -f "$(command -v conformance)"')"
package_dir="$(dirname "$(dirname "$suite_bin")")"

status=0
"${python[@]}" "$conformance_dir/evaluate.py" \
  --manifest "$conformance_dir/applicability.toml" \
  --requirements "$package_dir/requirements/$requirements.yaml" \
  --package-source "$package_dir/dist/index.js" \
  --shipping "$out/shipping/results" \
  --fixture "$out/fixture/results" \
  --out "$out" || status=1
"${python[@]}" "$conformance_dir/secret_scan.py" "$out" || status=1
exit "$status"
