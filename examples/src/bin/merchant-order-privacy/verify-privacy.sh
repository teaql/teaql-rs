#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
manifest="$example/rust-app-console/Cargo.toml"
repo="$(cd "$example/../../../.." && pwd)"
scratch="$(mktemp -d -t teaql-rust-order-privacy.XXXXXXXX)"
cleanup() {
  status=$?
  trap - EXIT
  python3 -B -c 'import pathlib,shutil,sys; p=pathlib.Path(sys.argv[1]); assert p.name.startswith("teaql-rust-order-privacy."); shutil.rmtree(p)' "$scratch"
  exit "$status"
}
trap cleanup EXIT
evidence="${TEAQL_ORDER_PRIVACY_EVIDENCE_DIR:-$scratch/evidence}"
[[ "$evidence" == /* ]]
mkdir -- "$evidence"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
export TEAQL_ORDER_PRIVACY_DATABASE="$evidence/orders.sqlite" TEAQL_LOG_FORMAT=json TEAQL_TELEMETRY_ENABLED=false
unset TEAQL_AUDIT_DEBUG_ENDPOINT TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS
cargo metadata --offline --locked --manifest-path "$manifest" --format-version 1 >"$evidence/dependencies.json"
jq -e --arg root "$repo/" --arg lib "$example/rust-lib-core/lib/Cargo.toml" '
  ([.packages[] | select(.name | startswith("teaql-"))] |
    length > 0 and all(.[]; .source == null and (.manifest_path | startswith($root))))
  and ([.packages[] | select(.name == "merchant-order-privacy-service-core" and .source == null and .manifest_path == $lib)] | length == 1)
' "$evidence/dependencies.json" >"$evidence/dependency-origins.txt"
cargo build --offline --locked --manifest-path "$manifest" >"$evidence/build.log" 2>&1 || { tail -65 "$evidence/build.log" >&2; exit 1; }
for round in 1 2; do
  if [[ "$round" == 2 ]]; then
    export TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS=I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK
  fi
  TEAQL_SHOWCASE_ROUND="$round" TEAQL_LOG_ENDPOINT="$evidence/ordinary-$round.log" \
    TEAQL_SQL_DEBUG_ENDPOINT="$evidence/debug-$round.log" \
    "$CARGO_TARGET_DIR/debug/merchant-order-privacy-service-core-workspace" >"$evidence/round-$round.log" 2>&1 || {
      tail -45 "$evidence/round-$round.log" | sed -E 's/[[:alnum:]._-]+@example\.test/[REDACTED]/g' >&2
      exit 1
    }
  rg '^ORDER_PRIVACY=' "$evidence/round-$round.log"
done
