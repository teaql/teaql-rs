#!/usr/bin/env bash
set -euo pipefail

example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
trace_evidence_dir="${TEAQL_TRACE_CHAIN_EVIDENCE_DIR:-$(mktemp -d /tmp/teaql-trace-chain-evidence-XXXXXX)}"
mkdir -p "$trace_evidence_dir"
export TEAQL_TRACE_CHAIN_DATABASE="${TEAQL_TRACE_CHAIN_DATABASE:-sqlite:file:$trace_evidence_dir/trace-chain.sqlite}"
if [[ "$TEAQL_TRACE_CHAIN_DATABASE" != sqlite:file:* ]]; then
  printf 'FAIL: use one persistent sqlite:file: database for both replays\n' >&2
  exit 1
fi

generated_hash() {
  (cd "$example_dir/rust-lib-core" && find . -type f -exec sha256sum {} \; | sort | sha256sum | cut -d ' ' -f 1)
}
trace_library_before="$(generated_hash)"
for trace_pass in 1 2; do
  trace_log="$trace_evidence_dir/run-$trace_pass.log"
  trace_status=0
  cargo run --quiet --locked --manifest-path "$example_dir/Cargo.toml" >"$trace_log" 2>&1 || trace_status=$?
  if (( trace_status != 0 )); then
    sed -n '1,240p' "$trace_log" >&2
    printf 'FAIL generated Trace Chain replay %s: exit %s; evidence %s\n' "$trace_pass" "$trace_status" "$trace_log" >&2
    exit "$trace_status"
  fi
  for trace_marker in 'TC-MUT-15 PASSED' 'TC-SQL-07 PASSED' 'TC-MUT-09 PASSED' 'TC-MUT-12 PASSED' 'TC-MUT-13 PASSED' 'TC-MUT-14 PASSED'; do
    if ! rg -Fq "$trace_marker" "$trace_log"; then
      printf 'FAIL missing marker %s in %s\n' "$trace_marker" "$trace_log" >&2
      exit 1
    fi
  done
  printf 'PASS generated Trace Chain replay %s (same database, no cleanup)\n' "$trace_pass"
done
trace_library_after="$(generated_hash)"
if [[ "$trace_library_before" != "$trace_library_after" ]]; then
  printf 'FAIL generated library changed during application verification\n' >&2
  exit 1
fi
printf 'PASS generated Trace Chain: normative graph, three relation levels, same-type batches, concurrent saves, provider failure, readback failure; library hash %s; database %s; evidence %s\n' \
  "$trace_library_after" "$TEAQL_TRACE_CHAIN_DATABASE" "$trace_evidence_dir"
