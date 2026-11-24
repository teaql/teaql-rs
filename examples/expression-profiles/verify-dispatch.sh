#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../.." && pwd)"
if [[ -n "${TEAQL_DISPATCH_PROFILE_EVIDENCE_DIR:-}" ]]; then
  evidence="$TEAQL_DISPATCH_PROFILE_EVIDENCE_DIR"
  mkdir "$evidence"
else
  evidence="$(mktemp -d -t teaql-rust-dispatch-profile.XXXXXXXX)"
fi
evidence="$(cd "$evidence" && pwd)"
scratch="$(mktemp -d -t teaql-rust-dispatch-build.XXXXXXXX)"
cleanup() {
  status=$?
  trap - EXIT
  rm -rf -- "$scratch" || { [[ "$status" != 0 ]] || status=1; }
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 TEAQL_TELEMETRY_ENABLED=false

for profile in camelCase snake_case PascalCase; do
  source="$example"
  if [[ "$profile" != camelCase ]]; then source="$example/profiles/$profile"; fi
  folder="$evidence/$profile"
  build="$scratch/$profile"
  mkdir "$folder" "$build"
  cp -a "$source/rust-app-console" "$source/rust-lib-core" "$build"
  app="$build/rust-app-console"
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
  cargo metadata --offline --format-version 1 --manifest-path "$manifest" > "$folder/dependencies.json"
  cp "$app/Cargo.lock" "$folder/Cargo.lock"
  cargo build --offline --locked --manifest-path "$manifest" --bin dispatch-document-core-workspace > "$folder/build.log" 2>&1 || {
    tail -80 "$folder/build.log"
    exit 1
  }
  for phase in first restart; do
    "$CARGO_TARGET_DIR/debug/dispatch-document-core-workspace" "$folder/dispatch.sqlite" "$profile" "$phase" | tee "$folder/$phase.log"
  done
  diff -qr "$source/rust-lib-core" "$build/rust-lib-core"
done
TEAQL_GENERATED_E_PROFILE_EVIDENCE_DIR="${TEAQL_GENERATED_E_PROFILE_EVIDENCE_DIR:-$evidence/calibration}" node "$example/verify-local.cjs" | tee "$evidence/calibration.log"
