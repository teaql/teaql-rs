#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verification_dir="$(mktemp -d)"
trap 'rm -rf -- "$verification_dir"' EXIT
expected=(business-id-runtime conformance order-management school-management tests)
mapfile -t actual < <(find "$repo/examples" -mindepth 1 -maxdepth 1 -type d ! -name src -printf '%f\n' | sort)
if [[ "${actual[*]}" != "${expected[*]}" ]]; then
  echo "example inventory changed; update scripts/verify-examples.sh: ${actual[*]}" >&2
  exit 1
fi

cd "$repo"
cargo test -p teaql-examples --all-targets
cargo run --quiet --manifest-path examples/conformance/Cargo.toml
cargo run --quiet --manifest-path examples/school-management/Cargo.toml
SCHOOL_MANAGEMENT_SERVICE_CORE_DATABASE_URL="$verification_dir/env-helper.db" \
  TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS=I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK \
  TEAQL_LOG_ENDPOINT="$verification_dir/env-helper-safe.log" \
  TEAQL_SQL_DEBUG_ENDPOINT="$verification_dir/env-helper-sensitive.log" \
  TEAQL_AUDIT_DEBUG_ENDPOINT="$verification_dir/env-helper-audit-sensitive.log" \
  TEAQL_AUDIT_LOG=_full_with_payload \
  cargo run --quiet --manifest-path examples/school-management/Cargo.toml --bin env_runtime_save_probe
grep -E -q 'Parameterized SQL:' "$verification_dir/env-helper-safe.log"
if grep -E -q 'Debug SQL:|Env Helper School' "$verification_dir/env-helper-safe.log"; then
  echo 'ordinary SQL log leaked copy-paste SQL or a bound School name' >&2
  exit 1
fi
grep -E -q 'Debug SQL:.*Env Helper School' "$verification_dir/env-helper-sensitive.log"
grep -E -q '\[AUDIT\].*Env Helper School' "$verification_dir/env-helper-audit-sensitive.log"
if grep -Fq 'Debug SQL:' "$verification_dir/env-helper-audit-sensitive.log"; then
  echo 'sensitive audit sink received SQL diagnostics' >&2
  exit 1
fi
echo 'PASS: explicit SQL and audit debug sinks separated from ordinary log'
env -u TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS \
  SCHOOL_MANAGEMENT_SERVICE_CORE_DATABASE_URL="$verification_dir/privacy.db" \
  TEAQL_LOG_ENDPOINT="$verification_dir/privacy-safe.log" \
  TEAQL_SQL_DEBUG_ENDPOINT="$verification_dir/privacy-debug.log" \
  TEAQL_AUDIT_DEBUG_ENDPOINT="$verification_dir/privacy-audit.log" \
  TEAQL_AUDIT_LOG=_full_with_payload \
  cargo run --quiet --manifest-path examples/school-management/Cargo.toml --bin env_runtime_save_probe
test -s "$verification_dir/privacy-safe.log"
for log_file in "$verification_dir/privacy-safe.log" "$verification_dir/privacy-debug.log" "$verification_dir/privacy-audit.log"; do
  # Sensitive-only endpoints are disabled entirely without acknowledgement.
  # If a file is nevertheless created, it must not contain private payloads.
  if [[ ! -e "$log_file" ]]; then continue; fi
  if grep -E -q 'Env Helper School|PRIVATE-FAILURE-CANARY' "$log_file"; then
    echo 'default runtime file endpoint leaked a CRUD privacy marker' >&2
    exit 1
  fi
done
echo 'PASS: default file endpoints redact real SQLite CRUD and failure payloads'
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
