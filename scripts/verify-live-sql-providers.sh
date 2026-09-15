#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

verify_expected_output() {
  local package="$1"
  local expected="$2"
  local output="$3"
  local observed
  observed="$(printf '%s\n' "$output" \
    | sed -nE 's/^test result: ok\. ([0-9]+) passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;.*/\1/p' \
    | paste -sd, -)"
  if [[ "$observed" != "$expected" ]]; then
    printf 'FAIL %s executed test counts: observed=%s expected=%s (zero failed/ignored required)\n' \
      "$package" "${observed:-none}" "$expected" >&2
    return 1
  fi
}

if [[ "${1:-}" == "--self-test" ]]; then
  green='test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s'
  partial='test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s'
  ignored='test result: ok. 29 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 1.00s'
  verify_expected_output postgres 29 "$green"
  if verify_expected_output postgres 29 "$partial" >/dev/null 2>&1; then
    printf 'FAIL self-test: partial execution was accepted\n' >&2
    exit 1
  fi
  if verify_expected_output postgres 29 "$ignored" >/dev/null 2>&1; then
    printf 'FAIL self-test: ignored test was accepted\n' >&2
    exit 1
  fi
  printf 'PASS live-provider output validator rejects partial and ignored suites\n'
  exit 0
fi

require_isolated_url() {
  local variable="$1"
  local scheme="$2"
  local example_scheme="$3"
  local url="${!variable:-}"
  if [[ -z "$url" ]]; then
    printf '%s is required: create a dedicated teaql_rust_provider_* database first\n' "$variable" >&2
    return 1
  fi
  if [[ ! "$url" =~ ^${scheme}://[^/]+/teaql_rust_provider_[A-Za-z0-9_]+([?].*)?$ ]]; then
    printf '%s must use %s://.../teaql_rust_provider_* (a dedicated disposable test database)\n' \
      "$variable" "$example_scheme" >&2
    return 1
  fi
}

require_isolated_url TEAQL_TEST_POSTGRES_URL 'postgres(ql)?' 'postgresql'
require_isolated_url TEAQL_TEST_MYSQL_URL 'mysql' 'mysql'

cd "$repo_dir"
run_expected_suite() {
  local package="$1"
  local expected="$2"
  shift 2
  local output
  if ! output="$(cargo test -p "$package" "$@" --quiet 2>&1)"; then
    printf '%s\n' "$output" >&2
    return 1
  fi
  printf '%s\n' "$output"
  verify_expected_output "$package" "$expected" "$output"
}

run_expected_suite teaql-provider-sqlite '42,1' --lib --tests
run_expected_suite teaql-provider-postgres '29' --lib
run_expected_suite teaql-provider-mysql '23' --lib
printf 'PASS live SQLite/PostgreSQL/MySQL provider suites (isolated database URLs supplied)\n'
