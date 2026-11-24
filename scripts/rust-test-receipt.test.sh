#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/rust-test-receipt.sh"
expect_pass() {
  native_case_passed "$1" /dev/stdin <<<"$2"
}
expect_missing() {
  if native_case_passed "$1" /dev/stdin <<<"$2"; then
    printf 'FAIL false positive for %s\n' "$1" >&2
    exit 1
  fi
}
expect_pass probe 'test probe ... TeaQL telemetry: supported, disabled.
ok

successes:
    probe
test result: ok. 1 passed; 0 failed; 0 ignored'
expect_pass module::probe 'successes:
    module::probe
test result: ok. 1 passed; 0 failed; 0 ignored'
expect_missing probe 'successes:
    another_probe
test result: ok. 1 passed; 0 failed; 0 ignored'
expect_missing probe 'test probe ... ok
successes:
---- another_probe stdout ----
    probe
successes:
    another_probe
test result: ok. 1 passed; 0 failed; 0 ignored'
expect_missing probe 'failures:
    probe
test result: FAILED. 0 passed; 1 failed'
expect_missing probe 'test probe ... ignored
test result: ok. 0 passed; 0 failed; 1 ignored'
printf 'PASS Rust named test receipt parser (six positive/negative cases)\n'
