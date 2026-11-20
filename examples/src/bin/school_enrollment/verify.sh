#!/usr/bin/env bash
set -euo pipefail
case_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$case_dir/../../../.." && pwd)"
evidence="${TEAQL_RUST_ENROLLMENT_EVIDENCE_DIR:-}"
if [[ -n "$evidence" ]]; then
  mkdir "$evidence"
else
  evidence="$(mktemp -d -t teaql-rust-school-enrollment-evidence.XXXXXXXX)"
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
  owned_target="$(mktemp -d -t teaql-rust-school-enrollment-target.XXXXXXXX)"
  export CARGO_TARGET_DIR="$owned_target"
fi
cd "$repo"
manifest="$case_dir/Cargo.toml"
program=(cargo run --locked --manifest-path "$manifest" --)
cargo metadata --format-version 1 --locked --manifest-path "$manifest" > "$evidence/dependencies.json"
"${program[@]}" "$evidence/install.sqlite" install | tee "$evidence/install.log"
for round in first restart; do
  "${program[@]}" "$evidence/enrollment.sqlite" "$round" | tee "$evidence/$round.log"
  python3 -B "$case_dir/verify-sqlite.py" "$evidence" "$round" > "$evidence/rows-$round.json"
done
cargo run --locked -p teaql-examples --bin school_example | tee "$evidence/calibration.log"
python3 -B "$case_dir/verify-sqlite.py" "$evidence" "$repo"
echo "PASS Rust school enrollment workspace and original relation-loading calibration"
