#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verification_dir="${TEAQL_EXAMPLES_EVIDENCE_DIR:-$(mktemp -d -t teaql-rust-examples-evidence.XXXXXXXX)}/runtime"
mkdir -p "$verification_dir"
echo "Retained compact example logs and databases: $verification_dir"
expected=(boolean-boundaries business-id-runtime conformance context-bound-order-document context-derived equipment-purchase-graph expression-profiles facet-trace fix-e-alignment http-resilience inheritance-save maintenance-work-items natural-messages object-storage order-management request-admission school-management shared-load-state tests trace-chain)
mapfile -t actual < <(find "$repo/examples" -mindepth 1 -maxdepth 1 -type d ! -name src -printf '%f\n' | sort)
if [[ "${actual[*]}" != "${expected[*]}" ]]; then
  echo "example inventory changed; update scripts/verify-examples.sh: ${actual[*]}" >&2
  exit 1
fi

cd "$repo"
node scripts/test-sqlite-probe-adapter.cjs
TEAQL_FIX_E_EVIDENCE_DIR="${verification_dir%/runtime}/fix-e" \
  node examples/fix-e-alignment/verify-local.cjs
node examples/school-management/verify-grouped-mapping.cjs --output "${verification_dir%/runtime}/grouped-mapping"
node examples/inheritance-save/verify.cjs --output "${verification_dir%/runtime}/inheritance-save"
python3 examples/context-derived/verify-local.py --output "${verification_dir%/runtime}/context-derived"
python3 scripts/verify-natural-messages.py --example --output "${verification_dir%/runtime}/natural-messages"
TEAQL_REQUEST_ADMISSION_EVIDENCE_DIR="${verification_dir%/runtime}/request-admission" \
  node examples/request-admission/verify-local.cjs
TEAQL_PURCHASE_APPROVAL_EVIDENCE_DIR="${verification_dir%/runtime}/purchase-approval" \
  bash examples/boolean-boundaries/verify-approval.sh
# Isolated non-Git copies must name their hash-bound Git origin explicitly.
TEAQL_DISPATCH_PROFILE_EVIDENCE_DIR="${verification_dir%/runtime}/expression-profiles" bash "examples/expression-profiles/verify-dispatch.sh"
node examples/tests/loaded-revert/verify-local.cjs
node examples/tests/loaded-revert/verify-views.cjs
bash examples/http-resilience/verify.sh
bash examples/object-storage/verify.sh
bash examples/trace-chain/verify-standard-telemetry.sh
TEAQL_SQLITE_EXAMPLE_EVIDENCE_DIR="$verification_dir/equipment-purchase-graph" \
  bash scripts/verify-sqlite-business-example.sh equipment-purchase-graph
TEAQL_SQLITE_EXAMPLE_EVIDENCE_DIR="$verification_dir/maintenance-work-items" \
  bash scripts/verify-sqlite-business-example.sh maintenance-work-items
cargo test -p teaql-provider-sqlite --test relation_membership -- --nocapture
cargo test -p teaql-examples --all-targets
TEAQL_RUST_ENROLLMENT_EVIDENCE_DIR="$verification_dir/school-enrollment" \
  bash examples/src/bin/school_enrollment/verify.sh
TEAQL_SHIPPING_LOG_EVIDENCE_DIR="$verification_dir/order-logging" \
  bash examples/src/bin/order_logging/verify.sh
TEAQL_BUSINESS_ID_KEY_V1_HEX=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
TEAQL_PURCHASE_NUMBERING_ROUND=inventory \
PURCHASE_ORDER_NUMBERING_CORE_DATABASE_URL="sqlite:file:$verification_dir/business-id.sqlite" \
cargo run --locked --manifest-path examples/business-id-runtime/Cargo.toml
TEAQL_CONFORMANCE_EVIDENCE_DIR="$verification_dir/conformance" bash examples/conformance/verify.sh
cargo run --quiet --manifest-path examples/school-management/Cargo.toml
bash scripts/verify-school-bootstrap-example.sh
SCHOOL_MANAGEMENT_SERVICE_CORE_DATABASE_URL="$verification_dir/env-helper.db" \
  TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS=I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK \
  TEAQL_LOG_ENDPOINT="$verification_dir/env-helper-safe.log" \
  TEAQL_SQL_DEBUG_ENDPOINT="$verification_dir/env-helper-sensitive.log" \
  TEAQL_AUDIT_DEBUG_ENDPOINT="$verification_dir/env-helper-audit-sensitive.log" \
  TEAQL_AUDIT_LOG=_full_with_payload \
  cargo run --quiet --manifest-path examples/school-management/Cargo.toml --bin env_runtime_save_probe
python3 - "$verification_dir/env-helper-safe.log" <<'PY'
import json, sys
from pathlib import Path
records = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
sql = [row for row in records if row.get('category') == 'SQL']
assert sql, 'ordinary runtime file must contain structured SQL events'
assert any('-- TeaQL SAFE' in (row.get('sql') or '') for row in sql), 'ordinary SQL lost its safe rendering marker'
assert all(all(key in row for key in ('timestamp', 'level', 'log_name', 'runtime', 'event', 'message')) for row in records)
print('PASS: JSON runtime SQL retains safe rendering and typed event fields')
PY
grep -F -q "'En*************ol'" "$verification_dir/env-helper-safe.log"
grep -F -q "'1 Runtime Road'" "$verification_dir/env-helper-safe.log"
if grep -E -q 'Parameterized SQL:|SQL omitted:|Env Helper School|PRIVATE-FAILURE-CANARY' "$verification_dir/env-helper-safe.log"; then
  echo 'ordinary SQL log lost safe expansion or leaked a rejected mutation payload' >&2
  exit 1
fi
grep -F -q 'DEBUG PLAINTEXT; EXPLICIT OPT-IN' "$verification_dir/env-helper-sensitive.log"
grep -F -q "'Env Helper School'" "$verification_dir/env-helper-sensitive.log"
grep -E -q '\[AUDIT\].*Env Helper School' "$verification_dir/env-helper-audit-sensitive.log"
if grep -Fq 'SqlLogEntry' "$verification_dir/env-helper-audit-sensitive.log"; then
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
test ! -s "$verification_dir/privacy-debug.log"
test ! -s "$verification_dir/privacy-audit.log"
echo 'PASS: default SQL preserves ordinary fields; sensitive names, rejected payloads and unapproved debug sinks stay private'
TEAQL_ORDER_EVIDENCE_DIR="${verification_dir%/runtime}/order-management" bash examples/order-management/verify.sh
cargo test --offline --locked -p teaql-examples --test context_bound_order_document
cargo test -p teaql-tfp-endpoint --examples
bash examples/trace-chain/verify.sh
bash examples/facet-trace/verify.sh
TEAQL_LOAD_STATE_EVIDENCE_DIR="${verification_dir%/runtime}/shared-load-state" bash examples/shared-load-state/verify.sh --workspace
TEAQL_MAINSTREAM_EVIDENCE_DIR="${verification_dir%/runtime}/mainstream-reference" \
  node examples/shared-load-state/verify-mainstream-reference.cjs --workspace
echo "PASS: all Rust examples"
