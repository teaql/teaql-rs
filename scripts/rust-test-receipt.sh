#!/usr/bin/env bash
# Named libtest receipts for cargo test -- --show-output.
native_case_passed() {
  awk -v expected="$1" '
    /^successes:$/ { in_successes = 1; next }
    /^---- / || /^failures:$/ || /^test result:/ { in_successes = 0 }
    in_successes && $0 == "    " expected { found = 1 }
    END { exit !found }
  ' "$2"
}
