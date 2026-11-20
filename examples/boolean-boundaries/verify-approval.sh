#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../.." && pwd)"
if [[ -n "${TEAQL_PURCHASE_APPROVAL_EVIDENCE_DIR:-}" ]]; then
  evidence="$TEAQL_PURCHASE_APPROVAL_EVIDENCE_DIR"
  mkdir "$evidence"
else
  evidence="$(mktemp -d -t teaql-rust-purchase-approval.XXXXXXXX)"
fi
evidence="$(cd "$evidence" && pwd)"
scratch="$(mktemp -d -t teaql-rust-approval-build.XXXXXXXX)"
cleanup() {
  status=$?
  trap - EXIT
  rm -rf -- "$scratch" || { [[ "$status" != 0 ]] || status=1; }
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cp -a "$example/rust-app-console" "$example/rust-lib-core" "$scratch"
app="$scratch/rust-app-console"
manifest="$app/Cargo.toml"
cp "$repo/Cargo.lock" "$app/Cargo.lock"
python3 -B - "$manifest" "$repo" <<'PY'
import json
from pathlib import Path
import sys
manifest, repo = map(Path, sys.argv[1:])
with manifest.open('a') as output:
    output.write('\n[workspace]\n\n[patch.crates-io]\n')
    for name in ('teaql-core', 'teaql-macros', 'teaql-runtime', 'teaql-sql', 'teaql-data-service', 'teaql-provider-sqlite'):
        output.write(f'{name} = {{ path = {json.dumps(str(repo / name))} }}\n')
PY
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 TEAQL_TELEMETRY_ENABLED=false
cargo metadata --offline --format-version 1 --manifest-path "$manifest" > "$evidence/dependencies.json"
cp "$app/Cargo.lock" "$evidence/Cargo.lock"
cargo build --offline --locked --manifest-path "$manifest" --bin purchase-approval-queue-core-workspace > "$evidence/build.log" 2>&1 || {
  tail -80 "$evidence/build.log"
  exit 1
}
for phase in first restart; do
  "$CARGO_TARGET_DIR/debug/purchase-approval-queue-core-workspace" "$evidence/approval.sqlite" "$phase" | tee "$evidence/$phase.log"
done
diff -qr "$example/rust-lib-core" "$scratch/rust-lib-core"
cmp "$example/rust-app-console/src/main.rs" "$app/src/main.rs"
TEAQL_BOOLEAN_EVIDENCE_DIR="${TEAQL_BOOLEAN_EVIDENCE_DIR:-$evidence/calibration}" node "$example/verify-local.cjs" | tee "$evidence/calibration.log"
echo "PASS Purchase approval queue and original Boolean calibration. Evidence at $evidence"
