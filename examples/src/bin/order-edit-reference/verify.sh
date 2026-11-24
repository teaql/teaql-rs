#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../../../.." && pwd)"
manifest="$example/rust-app-console/Cargo.toml"
scratch="$(mktemp -d -t teaql-rust-reference.XXXXXXXX)"
trap 'status=$?; trap - EXIT; rm -rf -- "$scratch"; exit "$status"' EXIT
evidence="${TEAQL_REFERENCE_EVIDENCE_DIR:-$scratch/evidence}"
[[ "$evidence" == /* ]]
mkdir -- "$evidence"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
export TEAQL_REFERENCE_DATABASE="$evidence/orders.sqlite"
cargo metadata --offline --locked --manifest-path "$manifest" --format-version 1 > "$evidence/dependencies.json"
jq -e --arg root "$repo/" --arg lib "$example/rust-lib-core/lib/Cargo.toml" '
  ([.packages[] | select(.name | startswith("teaql-"))] |
    length > 0 and all(.[]; .source == null and (.manifest_path | startswith($root))))
  and ([.packages[] | select(.name == "order-edit-reference-core" and .source == null and .manifest_path == $lib)] | length == 1)
' "$evidence/dependencies.json" > "$evidence/dependency-origins.txt"
cargo build --offline --locked --manifest-path "$manifest" --bins > "$evidence/build.log" 2>&1 || {
  tail -80 "$evidence/build.log" >&2
  exit 1
}
binary="$CARGO_TARGET_DIR/debug/round-trip-reference-calibration"
env -u TEAQL_UNSAFE_EXPOSE_RAW_ENTITY_IDS TEAQL_REFERENCE_PROFILE=test \
  "$binary" > "$evidence/governed.log" 2>&1
rg -Fx 'PASS Rust round-trip reference governed mode' "$evidence/governed.log"
TEAQL_REFERENCE_PROFILE=development \
  TEAQL_UNSAFE_EXPOSE_RAW_ENTITY_IDS=I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY \
  "$binary" > "$evidence/raw-local-diagnostic.log" 2>&1
rg -Fx 'PASS Rust round-trip reference raw diagnostic mode' "$evidence/raw-local-diagnostic.log"
for round in 1 2; do
  TEAQL_SHOWCASE_ROUND="$round" "$CARGO_TARGET_DIR/debug/order-edit-reference-core-workspace" \
    > "$evidence/round-$round.log" 2>&1 || {
      tail -65 "$evidence/round-$round.log" >&2
      exit 1
    }
  rg '^ORDER_REFERENCE=' "$evidence/round-$round.log"
done
cd "$repo"
cargo test --offline --locked -p teaql-runtime --lib round_trip_reference::tests \
  > "$evidence/calibration.log" 2>&1 || {
    tail -80 "$evidence/calibration.log" >&2
    exit 1
  }
rg '^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;' "$evidence/calibration.log"
