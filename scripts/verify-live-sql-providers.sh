#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
cargo test -p teaql-provider-sqlite --lib --tests --quiet
cargo test -p teaql-provider-postgres --lib --quiet
cargo test -p teaql-provider-mysql --lib --quiet
printf 'PASS live SQLite/PostgreSQL/MySQL provider suites (isolated database URLs supplied)\n'
