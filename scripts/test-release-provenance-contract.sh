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
        teaql-web-integration-axum \
        teaql-blob-contracts \
        teaql-blob-store-file \
        teaql-blob-content-native \
        teaql-object-storage-s3 \
        teaql-secure-document-reference \
        teaql-blob-reference
})"
actual="$($repo_dir/scripts/verify-release-provenance.sh --list-crates)"

if [[ "$actual" != "$expected" ]]; then
    printf 'FAIL public release crate contract\nexpected:\n%s\nactual:\n%s\n' \
        "$expected" "$actual" >&2
    exit 1
fi

if [[ "$(wc -l <<<"$actual")" -ne 16 ]]; then
    printf 'FAIL expected exactly sixteen public release crates\n' >&2
    exit 1
fi

declare -A release_position=()
position=0
while IFS= read -r crate_name; do
    release_position["$crate_name"]="$position"
    ((position += 1))
done <<<"$actual"

metadata="$(cargo metadata --manifest-path "$repo_dir/Cargo.toml" --locked --offline --no-deps --format-version 1)"
while IFS=$'\t' read -r consumer dependency; do
    [[ -n "${release_position[$consumer]+present}" ]] || continue
    if [[ -z "${release_position[$dependency]+present}" ]]; then
        printf 'FAIL release dependency %s required by %s is absent from the crate contract\n' \
            "$dependency" "$consumer" >&2
        exit 1
    fi
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

printf 'PASS release provenance contract: 16/16 public crates in dependency order and complete closure\n'
