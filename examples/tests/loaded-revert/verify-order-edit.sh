#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../../.." && pwd)"
manifest="$example/rust-app-console/Cargo.toml"
scratch="$(mktemp -d -t teaql-order-line-edit.XXXXXXXX)"
trap 'status=$?; trap - EXIT; rm -rf -- "$scratch"; exit "$status"' EXIT
evidence="${TEAQL_ORDER_EDIT_EVIDENCE_DIR:-$scratch/evidence}"
[[ "$evidence" == /* ]]
mkdir -- "$evidence"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
export TEAQL_ORDER_EDIT_DATABASE="$evidence/dispatch.sqlite"
cargo metadata --offline --locked --manifest-path "$manifest" --format-version 1 > "$evidence/dependencies.json"
jq -e --arg root "$repo/" --arg lib "$example/rust-lib-core/lib/Cargo.toml" '
  ([.packages[] | select(.name | startswith("teaql-"))] |
    length > 0 and all(.[]; .source == null and (.manifest_path | startswith($root))))
  and ([.packages[] | select(.name == "order-line-restoration-core" and .source == null and .manifest_path == $lib)] | length == 1)
' "$evidence/dependencies.json" >"$evidence/dependency-origins.txt"
cargo build --offline --locked --manifest-path "$manifest" > "$evidence/build.log" 2>&1 || {
  tail -90 "$evidence/build.log" >&2
  exit 1
}
for round in 1 2; do
  TEAQL_SHOWCASE_ROUND="$round" "$CARGO_TARGET_DIR/debug/order-line-restoration-core-workspace" \
    >"$evidence/round-$round.log" 2>&1 || {
      tail -60 "$evidence/round-$round.log" >&2
      exit 1
    }
  rg '^ORDER_LINE_RESTORATION=' "$evidence/round-$round.log"
done
