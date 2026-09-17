#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
expected="$({
    printf '%s\n' \
        teaql-macros \
        teaql-core \
        teaql-data-service \
        teaql-sql \
        teaql-runtime \
        teaql-provider-postgres \
        teaql-provider-sqlite \
        teaql-provider-mysql \
        teaql-web-integration-axum
})"
actual="$($repo_dir/scripts/verify-release-provenance.sh --list-crates)"

if [[ "$actual" != "$expected" ]]; then
    printf 'FAIL public release crate contract\nexpected:\n%s\nactual:\n%s\n' \
        "$expected" "$actual" >&2
    exit 1
fi

if [[ "$(wc -l <<<"$actual")" -ne 9 ]]; then
    printf 'FAIL expected exactly nine public release crates\n' >&2
    exit 1
fi

printf 'PASS release provenance contract: 9/9 public crates\n'
