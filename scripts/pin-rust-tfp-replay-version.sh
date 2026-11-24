#!/usr/bin/env bash
set -euo pipefail

version="${1:-}"
manifest="${2:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ || -z "$manifest" ]]; then
    printf 'Usage: %s <x.y.z> <Cargo.toml>\n' "$0" >&2
    exit 2
fi
if [[ ! -f "$manifest" ]]; then
    printf 'Missing replay manifest: %s\n' "$manifest" >&2
    exit 1
fi

crates=(
    teaql-core
    teaql-data-service
    teaql-runtime
    teaql-tfp-endpoint
)

for crate_name in "${crates[@]}"; do
    if [[ "$(grep -Ec "^${crate_name} = \"=?[0-9]+\.[0-9]+\.[0-9]+\"$" "$manifest")" -ne 1 ]]; then
        printf 'Expected one numeric %s dependency in %s\n' "$crate_name" "$manifest" >&2
        exit 1
    fi
    sed -i -E \
        "s/^(${crate_name} = \"?)[=]?[0-9]+\.[0-9]+\.[0-9]+(\"$)/\1=${version}\2/" \
        "$manifest"
    grep -Fx "${crate_name} = \"=${version}\"" "$manifest"
done
