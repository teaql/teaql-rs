#!/usr/bin/env bash
set -euo pipefail
runtime_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
wide=false
case "${1:-}" in
  "") ;;
  --wide) wide=true ;;
  *) printf 'Usage: bash verify-allocations.sh [--wide]\n' >&2; exit 2 ;;
esac
if (( $# > 1 )); then printf 'Unexpected arguments\n' >&2; exit 2; fi
if [[ "$wide" == true ]]; then
  mode=""
  IFS= read -r mode <"$runtime_dir/examples/shared-load-state/target/generated/fixture-mode.txt"
  [[ "$mode" == wide ]] || { printf 'Generate a wide fixture before wide allocation verification\n' >&2; exit 1; }
fi
run_dir="$(mktemp -d -t teaql-rust-state-allocations.XXXXXXXX)"
printf 'Evidence retained at %s\n' "$run_dir"
rustc --version >"$run_dir/toolchain.txt"
cargo --version >>"$run_dir/toolchain.txt"
cargo test --manifest-path "$runtime_dir/Cargo.toml" -p teaql-core --release \
  --test load_state_allocations -- --nocapture >"$run_dir/probe.log" 2>&1 || {
    tail -80 "$run_dir/probe.log" >&2
    exit 1
  }
printf 'case,width,iterations,allocation_calls,requested_bytes,elapsed_ns\n' >"$run_dir/allocations.csv"
rg '^(cached_projection|loaded_|reference_)' "$run_dir/probe.log" >>"$run_dir/allocations.csv"
cargo test --manifest-path "$runtime_dir/Cargo.toml" -p teaql-runtime --release \
  --test hydration_allocations -- --nocapture >"$run_dir/hydration.log" 2>&1 || {
    tail -80 "$run_dir/hydration.log" >&2
    exit 1
  }
printf 'case,projection,rows,allocation_calls,requested_bytes,elapsed_ns\n' >"$run_dir/hydration.csv"
rg '^(typed_hydration|context_hydration|plain_hydration),' "$run_dir/hydration.log" >>"$run_dir/hydration.csv"
if [[ "$wide" == true ]]; then
  rg --files --hidden --no-ignore "$runtime_dir/examples/shared-load-state/target/generated/lib/src" -0 |
    sort -z | xargs -0 sha256sum >"$run_dir/generated-source.sha256"
  cargo test --manifest-path "$runtime_dir/examples/shared-load-state/Cargo.toml" --release \
    --test wide_hydration_allocations -- --nocapture >"$run_dir/wide-hydration.log" 2>&1 || {
      tail -80 "$run_dir/wide-hydration.log" >&2; exit 1;
    }
  printf 'case,selected_fields,rows,allocation_calls,requested_bytes,elapsed_ns\n' >"$run_dir/wide-hydration.csv"
  rg '^generated_wide_hydration,' "$run_dir/wide-hydration.log" >>"$run_dir/wide-hydration.csv"
  rg -F 'PASS generated Rust wide hydration shares one overflow snapshot per shape' "$run_dir/wide-hydration.log"
  sha256sum --check --quiet "$run_dir/generated-source.sha256"
fi
printf 'PASS Rust warmed load-state allocation probe\n'
