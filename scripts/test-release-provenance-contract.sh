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
        teaql-tfp-endpoint \
        teaql-web-integration-axum
})"
actual="$($repo_dir/scripts/verify-release-provenance.sh --list-crates)"

if [[ "$actual" != "$expected" ]]; then
    printf 'FAIL public release crate contract\nexpected:\n%s\nactual:\n%s\n' \
        "$expected" "$actual" >&2
    exit 1
fi

if [[ "$(wc -l <<<"$actual")" -ne 10 ]]; then
    printf 'FAIL expected exactly ten public release crates\n' >&2
    exit 1
fi

declare -A release_position=()
position=0
while IFS= read -r crate_name; do
    release_position["$crate_name"]="$position"
    ((position += 1))
done <<<"$actual"

metadata="$(cargo metadata --manifest-path "$repo_dir/Cargo.toml" --locked --no-deps --format-version 1)"
while IFS=$'\t' read -r consumer dependency; do
    [[ -n "${release_position[$consumer]+present}" ]] || continue
    [[ -n "${release_position[$dependency]+present}" ]] || continue
    if (( release_position[$dependency] >= release_position[$consumer] )); then
        printf 'FAIL release order places dependency %s at/after consumer %s\n' \
            "$dependency" "$consumer" >&2
        exit 1
    fi
done < <(jq -r '
    .packages[]
    | .name as $consumer
    | .dependencies[]
    | select(.path != null)
    | [$consumer, .name]
    | @tsv
' <<<"$metadata")

printf 'PASS release provenance contract: 10/10 public crates in dependency order\n'
