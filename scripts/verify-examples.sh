#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verification_dir="$(mktemp -d)"
trap 'rm -rf -- "$verification_dir"' EXIT
expected=(conformance order-management school-management)
mapfile -t actual < <(find "$repo/examples" -mindepth 1 -maxdepth 1 -type d ! -name src -printf '%f\n' | sort)
if [[ "${actual[*]}" != "${expected[*]}" ]]; then
  echo "example inventory changed; update scripts/verify-examples.sh: ${actual[*]}" >&2
  exit 1
fi

cd "$repo"
cargo test -p teaql-examples --all-targets
cargo run --quiet --manifest-path examples/conformance/Cargo.toml
cargo run --quiet --manifest-path examples/school-management/Cargo.toml
TEAQL_EXAMPLE_DATABASE="$verification_dir/order.db" \
  cargo run --quiet --manifest-path examples/order-management/rust-app-console/Cargo.toml
for graph_pass in 1 2; do
  TEAQL_NESTED_PROBE_DATABASE="sqlite:file:$verification_dir/graph.db" \
    cargo run --quiet --manifest-path examples/order-management/rust-app-console/Cargo.toml --bin nested_graph_probe
done
TEAQL_SAVE_LOAD_STATE_DATABASE="sqlite:file:$verification_dir/graph.db" \
  cargo run --quiet --manifest-path examples/order-management/rust-app-console/Cargo.toml --bin save_loaded_relation_probe
TEAQL_SAVE_LOAD_STATE_DATABASE="sqlite:file:$verification_dir/graph.db" \
  cargo run --quiet --manifest-path examples/order-management/rust-app-console/Cargo.toml --bin save_forward_fk_probe
cargo test -p teaql-tfp-endpoint --examples
echo "PASS: all Rust examples"
