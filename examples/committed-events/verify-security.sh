#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
df -h "$repo" "${TMPDIR:-/tmp}"
scratch="$(mktemp -d -t teaql-rust-cmr13.XXXXXXXX)"
task_pid=""
cleanup() {
  status=$?; trap - EXIT
  if [[ -n "$task_pid" ]]; then
    kill -TERM -- "-$task_pid" 2>/dev/null || true
    kill -KILL -- "-$task_pid" 2>/dev/null || true
    wait "$task_pid" 2>/dev/null || true
  fi
  rm -rf -- "$scratch"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
command -v setsid >/dev/null
case_root="$scratch"
if [[ -n "${TEAQL_CMR13_EVIDENCE_ROOT:-}" ]]; then
  if [[ "$TEAQL_CMR13_EVIDENCE_ROOT" != /* || -e "$TEAQL_CMR13_EVIDENCE_ROOT" ]]; then
    echo 'CMR-13 evidence root must be an absent absolute path; never reset databases' >&2; exit 1
  fi
  mkdir "$TEAQL_CMR13_EVIDENCE_ROOT"
  case_root="$TEAQL_CMR13_EVIDENCE_ROOT"
fi
export CARGO_TARGET_DIR="$scratch/target" CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cd "$repo"
setsid cargo build --offline --locked -p teaql-provider-sqlite --example committed_security \
  > "$scratch/build.log" 2>&1 &
task_pid=$!
if wait "$task_pid"; then result=0; else result=$?; fi
task_pid=""
cat "$scratch/build.log"
if [[ -n "${TEAQL_CMR13_EVIDENCE_ROOT:-}" ]]; then cp "$scratch/build.log" "$case_root/build.log"; fi
if [[ "$result" != 0 ]]; then exit "$result"; fi
if [[ -n "${TEAQL_CMR13_EVIDENCE_ROOT:-}" ]]; then
  cargo metadata --offline --locked --format-version=1 --no-deps > "$case_root/cargo-metadata.json"
  sha256sum "$CARGO_TARGET_DIR/debug/examples/committed_security" > "$case_root/binary.sha256"
fi
for round in 1 2; do
  TEAQL_CMR13_EVIDENCE_DIR="$case_root/run-$round" setsid \
    "$CARGO_TARGET_DIR/debug/examples/committed_security" > "$scratch/round-$round.log" 2>&1 &
  task_pid=$!
  if wait "$task_pid"; then result=0; else result=$?; fi
  task_pid=""
  cat "$scratch/round-$round.log"
  if [[ -n "${TEAQL_CMR13_EVIDENCE_ROOT:-}" ]]; then cp "$scratch/round-$round.log" "$case_root/round-$round.log"; fi
  if [[ "$result" != 0 ]]; then exit "$result"; fi
  rg -Fq 'CMR13_SECURITY_EXAMPLE_PASS language=rust snapshot=1 tail=2 audited_saves=2 rejected=5 lost_ack=1 duplicate_no_rewind=1 reopen=1' "$scratch/round-$round.log"
  rg -Fq 'CMR14_OBSERVABILITY_EXAMPLE_PASS language=rust phases=9 attempts=6 retries=4 failures=4 source_rejections=2 target_rejections=5 dead_letters=0' "$scratch/round-$round.log"
  echo "PASS Rust authenticated committed receiver example round $round"
done
