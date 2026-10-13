#!/usr/bin/env bash
set -euo pipefail
example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
run_dir="$(mktemp -d -t teaql-rust-standard-telemetry.XXXXXXXX)"
manifest="$example_dir/Cargo.toml"
names=(TEAQL_TELEMETRY_ENABLED TEAQL_TELEMETRY_SERVICE_NAME TEAQL_TELEMETRY_OTLP_ENDPOINT)
disabled=(env -u "${names[0]}" -u "${names[1]}" -u "${names[2]}")
library_manifest() {
  (cd "$example_dir" && find rust-lib-core/lib -type f ! -path '*/target/*' -print0 |
    sort -z | xargs -0 sha256sum)
}
library_manifest > "$run_dir/library-before.sha256"
cargo build --manifest-path "$manifest" --features standard-telemetry --bin standard-telemetry --bin standard-telemetry-stream --bin standard-telemetry-graph \
  --locked --offline > "$run_dir/build.log" 2>&1
args=(run --manifest-path "$manifest" --features standard-telemetry --bin standard-telemetry --locked --offline --quiet)
stream_args=(run --manifest-path "$manifest" --features standard-telemetry --bin standard-telemetry-stream --locked --offline --quiet)
graph_args=(run --manifest-path "$manifest" --features standard-telemetry --bin standard-telemetry-graph --locked --offline --quiet)
for repetition in 1 2; do
  "${disabled[@]}" cargo "${args[@]}" > "$run_dir/disabled-$repetition.log" 2>&1
  grep -Fq 'RUST_STANDARD_TELEMETRY_EXAMPLE PASSED' "$run_dir/disabled-$repetition.log"
  grep -Fq 'TeaQL telemetry: supported, disabled' "$run_dir/disabled-$repetition.log"
  for name in "${names[@]}"; do grep -Fq "$name" "$run_dir/disabled-$repetition.log"; done
  "${disabled[@]}" cargo "${stream_args[@]}" > "$run_dir/stream-disabled-$repetition.log" 2>&1
  grep -Fq 'RUST_STANDARD_STREAM PASSED completeRows=3 cancelledRows=1 independentRows=1 committedAudits=4 ordinarySqlLogs=0' "$run_dir/stream-disabled-$repetition.log"
  grep -Fq 'TeaQL telemetry: supported, disabled' "$run_dir/stream-disabled-$repetition.log"
  for name in "${names[@]}"; do grep -Fq "$name" "$run_dir/stream-disabled-$repetition.log"; done
  "${disabled[@]}" cargo "${graph_args[@]}" > "$run_dir/graph-disabled-$repetition.log" 2>&1
  grep -Fq 'RUST_STANDARD_GRAPH PASSED savedEntities=3 committedAudits=3 inheritedGrandchild=true loadedDepth=2 ordinarySqlLogs=0' "$run_dir/graph-disabled-$repetition.log"
  grep -Fq 'TeaQL telemetry: supported, disabled' "$run_dir/graph-disabled-$repetition.log"
  for name in "${names[@]}"; do grep -Fq "$name" "$run_dir/graph-disabled-$repetition.log"; done
done
if [[ ${TEAQL_TELEMETRY_ENABLED:-false} == true ]]; then
  for repetition in 1 2; do
    cargo "${args[@]}" > "$run_dir/enabled-$repetition.log" 2>&1
    grep -Fq 'RUST_STANDARD_TELEMETRY_EXAMPLE PASSED' "$run_dir/enabled-$repetition.log"
    grep -Fq 'TeaQL telemetry: enabled' "$run_dir/enabled-$repetition.log"
    cargo "${stream_args[@]}" > "$run_dir/stream-enabled-$repetition.log" 2>&1
    grep -Fq 'RUST_STANDARD_STREAM PASSED' "$run_dir/stream-enabled-$repetition.log"
    grep -Fq 'TeaQL telemetry: enabled' "$run_dir/stream-enabled-$repetition.log"
    cargo "${graph_args[@]}" > "$run_dir/graph-enabled-$repetition.log" 2>&1
    grep -Fq 'RUST_STANDARD_GRAPH PASSED savedEntities=3 committedAudits=3 inheritedGrandchild=true loadedDepth=2 ordinarySqlLogs=0' "$run_dir/graph-enabled-$repetition.log"
    grep -Fq 'TeaQL telemetry: enabled' "$run_dir/graph-enabled-$repetition.log"
  done
fi
library_manifest > "$run_dir/library-after.sha256"
cmp "$run_dir/library-before.sha256" "$run_dir/library-after.sha256"
printf 'PASS Rust standard telemetry example; evidence=%s\n' "$run_dir"
