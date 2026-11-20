#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
case "${1:-}" in
  equipment-purchase-graph)
    binary=equipment-procurement-core-workspace
    calibration=sqlite_relations_graph
    phases=(create reject restart)
    ;;
  maintenance-work-items)
    binary=maintenance-work-items-core-workspace
    calibration=sqlite_schema_crud
    phases=(create amend close restart)
    ;;
  *)
    echo "Usage: $0 <equipment-purchase-graph|maintenance-work-items>" >&2
    exit 2
    ;;
esac
example="$repo/examples/$1"
if [[ -n "${TEAQL_SQLITE_EXAMPLE_EVIDENCE_DIR:-}" ]]; then
  evidence="$TEAQL_SQLITE_EXAMPLE_EVIDENCE_DIR"
  mkdir "$evidence"
else
  evidence="$(mktemp -d -t teaql-rust-sqlite-example-evidence.XXXXXXXX)"
fi
evidence="$(cd "$evidence" && pwd)"
echo "Retained logs and SQLite evidence: $evidence"
scratch="$(mktemp -d -t teaql-rust-sqlite-example-build.XXXXXXXX)"
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

cp -a "$example/rust-app-console" "$example/rust-lib-core" "$scratch"
manifest="$scratch/rust-app-console/Cargo.toml"
cp "$repo/Cargo.lock" "$scratch/rust-app-console/Cargo.lock"
python3 -B - "$manifest" "$repo" <<'PY'
import json
from pathlib import Path
import sys

manifest, repo = map(Path, sys.argv[1:])
with manifest.open('a') as output:
    output.write('\n[workspace]\n\n[patch.crates-io]\n')
    for name in ('teaql-core', 'teaql-macros', 'teaql-runtime', 'teaql-sql',
                 'teaql-data-service', 'teaql-provider-sqlite'):
        output.write(f'{name} = {{ path = {json.dumps(str(repo / name))} }}\n')
PY
cargo metadata --offline --format-version 1 --manifest-path "$manifest" > "$scratch/dependencies.json"
python3 -B - "$scratch/dependencies.json" "$evidence/dependencies.json" <<'PY'
import json
from pathlib import Path
import sys

source, destination = map(Path, sys.argv[1:])
metadata = json.loads(source.read_bytes())
packages = [pkg for pkg in metadata['packages'] if pkg['source'] is None]
with destination.open('x') as output:
    json.dump(dict(workspaceRoot=metadata['workspace_root'], localPackages=[
        {key: pkg[key] for key in ('name', 'version', 'manifest_path')}
        for pkg in sorted(packages, key=lambda pkg: pkg['name'])
    ]), output, indent=2)
    output.write('\n')
PY
cp "$scratch/rust-app-console/Cargo.lock" "$evidence/Cargo.lock"
cargo build --offline --locked --manifest-path "$manifest" --bin "$binary" > "$evidence/build.log" 2>&1 || {
  tail -80 "$evidence/build.log"
  exit 1
}
database="$evidence/business.sqlite"
for phase in "${phases[@]}"; do
  if [[ "$phase" == reject ]]; then
    python3 -B "$example/reject-unapproved-dock.py" "$database"
  fi
  "$CARGO_TARGET_DIR/debug/$binary" "$database" "$phase" 2>&1 | tee "$evidence/$phase.log"
  python3 -B - "$database" "$evidence/$phase.sqlite" <<'PY'
from contextlib import closing
from pathlib import Path
import sqlite3
import sys

source, snapshot = map(Path, sys.argv[1:])
assert not snapshot.exists()
with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as reader:
    with closing(sqlite3.connect(snapshot)) as copy:
        reader.backup(copy)
PY
done
diff -qr "$example/rust-lib-core" "$scratch/rust-lib-core"

cd "$repo"
cargo run --offline --locked -p teaql-examples --bin "$calibration" 2>&1 | tee "$evidence/calibration.log"
