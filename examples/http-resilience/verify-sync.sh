#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../.." && pwd)"
manifest="$example/rust-app-console/Cargo.toml"
scratch="$(mktemp -d -t teaql-rust-supplier-sync.XXXXXXXX)"
trap 'status=$?; trap - EXIT; rm -rf -- "$scratch"; exit "$status"' EXIT
evidence="${TEAQL_SUPPLIER_SYNC_EVIDENCE_DIR:-$scratch/evidence}"
[[ "$evidence" == /* ]]
mkdir -- "$evidence"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
export TEAQL_SUPPLIER_SYNC_DATABASE="$evidence/supplier-orders.sqlite"
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
export NO_PROXY='*' no_proxy='*'
if [[ -n "${TEAQL_SUPPLIER_SYNC_OTLP_ENDPOINT:-}" ]]; then
  [[ "$TEAQL_SUPPLIER_SYNC_OTLP_ENDPOINT" =~ ^http://127\.0\.0\.1:[0-9]+$ ]]
  export TEAQL_TELEMETRY_ENABLED=true
  export TEAQL_TELEMETRY_SERVICE_NAME=teaql-rust-supplier-order-sync
  export TEAQL_TELEMETRY_OTLP_ENDPOINT="$TEAQL_SUPPLIER_SYNC_OTLP_ENDPOINT"
else
  export TEAQL_TELEMETRY_ENABLED=false
fi
cargo metadata --offline --locked --features standard-telemetry --manifest-path "$manifest" --format-version 1 > "$evidence/dependencies.json"
jq -e --arg root "$repo/" --arg lib "$example/rust-lib-core/lib/Cargo.toml" '
  ([.packages[] | select(.name | startswith("teaql-"))] |
    length > 0 and all(.[]; .source == null and (.manifest_path | startswith($root))))
  and ([.packages[] | select(.name == "supplier-order-sync-service-core" and .source == null and .manifest_path == $lib)] | length == 1)
' "$evidence/dependencies.json" >"$evidence/dependency-origins.txt"
cargo build --offline --locked --manifest-path "$manifest" --features standard-telemetry > "$evidence/build.log" 2>&1 || {
  tail -80 "$evidence/build.log" >&2
  exit 1
}
for round in 1 2; do
  if ! TEAQL_SHOWCASE_ROUND="$round" "$CARGO_TARGET_DIR/debug/supplier-order-sync-service-core-workspace" \
    >"$evidence/round-$round.log" 2>&1; then
    tail -80 "$evidence/round-$round.log" | sed -E 's/PRIVATE_[[:alnum:]_]+/[REDACTED]/g' >&2
    exit 1
  fi
  rg '^SUPPLIER_ORDER_SYNC=' "$evidence/round-$round.log"
done
