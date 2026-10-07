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
rustc -Vv >"$run_dir/toolchain.txt"
cargo --version >>"$run_dir/toolchain.txt"
git -C "$runtime_dir" rev-parse HEAD >"$run_dir/base-commit.txt"
git -C "$runtime_dir" diff --exit-code -- teaql-core teaql-macros teaql-runtime teaql-data-service teaql-sql examples/shared-load-state
(
  cd "$runtime_dir"
  git ls-files -z teaql-core/src teaql-core/tests teaql-macros/src teaql-runtime/src teaql-runtime/tests teaql-data-service/src teaql-sql/src examples/shared-load-state/tests examples/shared-load-state/verify-allocations.sh Cargo.lock |
    xargs -0 sha256sum
) >"$run_dir/source.sha256"
cargo test --manifest-path "$runtime_dir/Cargo.toml" -p teaql-core --release \
  --test load_state_allocations -- --nocapture >"$run_dir/probe.log" 2>&1 || {
    tail -80 "$run_dir/probe.log" >&2
    exit 1
  }
printf 'case,width,iterations,allocation_calls,requested_bytes,elapsed_ns\n' >"$run_dir/allocations.csv"
rg '^(cold_projection|cached_projection|loaded_|reference_)' "$run_dir/probe.log" >>"$run_dir/allocations.csv"
printf 'case,width,rows,allocation_calls,requested_bytes,elapsed_ns\n' >"$run_dir/relation-shape.csv"
rg '^flat_edge_shape,' "$run_dir/probe.log" >>"$run_dir/relation-shape.csv"
rg '^COMPACT_SAME_SHAPE_MERGE,' "$run_dir/probe.log" >"$run_dir/compact-merge.log"
cargo test --manifest-path "$runtime_dir/Cargo.toml" -p teaql-runtime --release \
  --test hydration_allocations -- --nocapture >"$run_dir/hydration.log" 2>&1 || {
    tail -80 "$run_dir/hydration.log" >&2
    exit 1
  }
printf 'case,projection,rows,allocation_calls,requested_bytes,elapsed_ns\n' >"$run_dir/hydration.csv"
rg '^(typed_hydration|context_hydration|plain_hydration|identity_batch),' "$run_dir/hydration.log" >>"$run_dir/hydration.csv"
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
(cd "$runtime_dir" && sha256sum --check --quiet "$run_dir/source.sha256")
printf 'PASS Rust warmed load-state allocation probe\n'
