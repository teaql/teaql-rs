#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../../../.." && pwd)"
evidence="${TEAQL_SHIPPING_LOG_EVIDENCE_DIR:-}"
if [[ -z "$evidence" ]]; then
  evidence="$(mktemp -d -t teaql-rust-shipping-logs.XXXXXXXX)"
else
  mkdir "$evidence"
fi
owned_target=''
cleanup() {
  status=$?
  trap - EXIT
  if [[ -n "$owned_target" ]]; then rm -rf -- "$owned_target" || { [[ "$status" != 0 ]] || status=1; }; fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  owned_target="$(mktemp -d -t teaql-rust-shipping-target.XXXXXXXX)"
  export CARGO_TARGET_DIR="$owned_target"
fi
cd "$repo"
unset TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS TEAQL_SQL_DEBUG_ENDPOINT TEAQL_AUDIT_DEBUG_ENDPOINT
export TEAQL_DOMAIN=shipping-order-logging TEAQL_TELEMETRY_ENABLED=false
manifest="$example/Cargo.toml"
cargo metadata --locked --format-version 1 --manifest-path "$manifest" > "$evidence/dependencies.json"
for format in json human; do
  directory="$evidence/$format"
  mkdir "$directory"
  TEAQL_LOG_FORMAT="$format" TEAQL_LOG_ENDPOINT="$directory/ordinary.log" \
    cargo run --locked --manifest-path "$manifest" -- "$directory/shipment.sqlite" "$directory/safe-audit.json" \
    | tee "$directory/application.log"
done
TEAQL_LOG_FORMAT=json TEAQL_LOG_ENDPOINT="$evidence/calibration-runtime.log" \
  cargo run --locked -p teaql-examples --bin test_default_log | tee "$evidence/calibration.log"
echo "PASS Shipping order logging and original formatter calibration. Evidence at $evidence"
