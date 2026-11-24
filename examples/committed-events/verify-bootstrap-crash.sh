#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
df -h "$repo" "${TMPDIR:-/tmp}"
scratch="$(mktemp -d -t teaql-rust-bootstrap-crash.XXXXXXXX)"
task_pid=""
stop_owned_command() {
  if [[ -n "$task_pid" ]]; then
    kill -TERM -- "-$task_pid" 2>/dev/null || true
    kill -KILL -- "-$task_pid" 2>/dev/null || true
    wait "$task_pid" 2>/dev/null || true
    task_pid=""
  fi
}
cleanup() { status=$?; trap - EXIT; stop_owned_command; rm -rf -- "$scratch"; exit "$status"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
command -v setsid >/dev/null
export CARGO_TARGET_DIR="$scratch/target" CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
if [[ -n "${TEAQL_CMR12_EVIDENCE_ROOT:-}" ]]; then
  if [[ "$TEAQL_CMR12_EVIDENCE_ROOT" != /* || -e "$TEAQL_CMR12_EVIDENCE_ROOT" ]]; then
    echo 'CMR-12 evidence root must be an absent absolute path; never reset prior databases' >&2
    exit 1
  fi
  mkdir -p "$TEAQL_CMR12_EVIDENCE_ROOT"
fi
cd "$repo"
# Compile once in an owned target; both native invocations use the same binary.
setsid cargo build --offline --locked -p teaql-provider-sqlite --example committed_bootstrap_crash \
  > "$scratch/build.log" 2>&1 &
task_pid=$!
if wait "$task_pid"; then result=0; else result=$?; fi
stop_owned_command
cat "$scratch/build.log"
if [[ "$result" != 0 ]]; then exit "$result"; fi
binary="$CARGO_TARGET_DIR/debug/examples/committed_bootstrap_crash"
if [[ -n "${TEAQL_CMR12_EVIDENCE_ROOT:-}" ]]; then
  "$binary" --build-receipt > "$TEAQL_CMR12_EVIDENCE_ROOT/build-origin.json"
  cargo metadata --offline --locked --format-version=1 --no-deps \
    > "$TEAQL_CMR12_EVIDENCE_ROOT/cargo-metadata.json"
fi
for run in 1 2; do
  if [[ -n "${TEAQL_CMR12_EVIDENCE_ROOT:-}" ]]; then
    export TEAQL_CMR12_EVIDENCE_DIR="$TEAQL_CMR12_EVIDENCE_ROOT/run-$run"
  else
    unset TEAQL_CMR12_EVIDENCE_DIR
  fi
  setsid "$binary" > "$scratch/run-$run.log" 2>&1 &
  task_pid=$!
  if wait "$task_pid"; then result=0; else result=$?; fi
  stop_owned_command
  cat "$scratch/run-$run.log"
  if [[ "$result" != 0 ]]; then exit "$result"; fi
  rg -Fq 'CMR12_NEGATIVE_CONTROL_PASS language=rust' "$scratch/run-$run.log"
  for round in 1 2; do
    rg -Fq "CMR12_PROCESS_PASS language=rust round=$round baseline=$((2 * round - 1)) tail=$((2 * round)) killed=7 same_source_database=1 original_bytes=1" "$scratch/run-$run.log"
  done
  echo "PASS Rust managed bootstrap process example run $run rounds=2 killed=14"
done
