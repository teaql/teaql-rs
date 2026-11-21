#!/usr/bin/env bash
set -euo pipefail

example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../.." && pwd)"
runtime="${TEAQL_RUST_EXAMPLE_SOURCE_ROOT:-$repo}"
scratch="$(mktemp -d -t teaql-supplier-rust.XXXXXXXX)"
cleanup() {
  local status=$?
  trap - EXIT
  python3 -c 'import shutil, sys; shutil.rmtree(sys.argv[1])' "$scratch"
  exit "$status"
}
trap cleanup EXIT
cp -R -- "$example/rust-app-console" "$example/rust-lib-core" "$scratch/"
export CARGO_TARGET_DIR="$scratch/target"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
patches=()
for crate in teaql-core teaql-macros teaql-runtime teaql-sql teaql-data-service teaql-provider-sqlite; do
  patches+=(--config "patch.crates-io.$crate.path=\"$runtime/$crate\"")
done
cd -- "$scratch/rust-app-console"
cargo build "${patches[@]}" >&2
driver=(python3 "$example/run-example.py" --catalog "$example/rust-app-console/teaql-i18n.json")
binary="$scratch/target/debug/supplier-onboarding-service-core-workspace"
if [[ "${1:-}" == --verify ]]; then
  "${driver[@]}" --all --repeat 2 -- "$binary"
  "${driver[@]}" --edge-cases -- "$binary"
else
  "${driver[@]}" "$@" -- "$binary"
fi
