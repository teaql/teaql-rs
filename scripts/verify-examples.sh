#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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
cargo run --quiet --manifest-path examples/order-management/rust-app-console/Cargo.toml
cargo test -p teaql-tfp-endpoint --examples
echo "PASS: all Rust examples"
