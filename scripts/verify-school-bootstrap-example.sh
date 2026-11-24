#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
example="$repo/examples/school-management"
evidence="$(mktemp -d -t teaql-rust-bootstrap.XXXXXXXX)"
owned_target=''
cleanup() {
  status=$?
  trap - EXIT
  if [[ -n "$owned_target" ]]; then rm -rf -- "$owned_target"; fi
  if (( status != 0 )); then echo "FAILED: bootstrap evidence retained at $evidence" >&2; fi
  exit "$status"
}
trap cleanup EXIT
if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  owned_target="$(mktemp -d -t teaql-rust-bootstrap-build.XXXXXXXX)"
  export CARGO_TARGET_DIR="$owned_target"
fi
fingerprint() {
  (cd "$example/rust-lib-core/lib" && rg --files -g '*.rs' -g '*.toml' -g '!target/**' | LC_ALL=C sort | xargs -d '\n' sha256sum)
}
fingerprint > "$evidence/library-before.sha256"
for logging in on off; do
  for round in 1 2; do
    TEAQL_SCHOOL_BOOTSTRAP_DB="$evidence/$logging.sqlite" TEAQL_SCHOOL_BOOTSTRAP_LOGGING="$logging" \
      cargo run --quiet --locked --manifest-path "$example/Cargo.toml" --bin bootstrap_trace_probe 2>&1 | tee "$evidence/$logging-$round.log"
    expected_logging=true; [[ "$logging" == off ]] && expected_logging=false
    fresh=true; version=1
    if [[ "$round" == 2 ]]; then fresh=false; version=3; fi
    rg -Fq "PASS Rust generated bootstrap trace logging=$expected_logging fresh=$fresh originalVersion=$version" "$evidence/$logging-$round.log"
    fingerprint > "$evidence/library-after.sha256"
    cmp "$evidence/library-before.sha256" "$evidence/library-after.sha256"
  done
done
echo "PASS Rust generated bootstrap twice per logging mode; retained evidence: $evidence"
