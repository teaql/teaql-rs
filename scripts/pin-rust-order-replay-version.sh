#!/usr/bin/env bash
# teaql-rs#153: pin both layers of the generated Order archive replay.
set -euo pipefail

version="${1:-}"
shift || true

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'Usage: %s <x.y.z> <Cargo.toml>...\n' "$0" >&2
    exit 2
fi
if [[ "$#" -eq 0 ]]; then
    printf 'At least one Cargo.toml must be supplied\n' >&2
    exit 2
fi

crates=(
    teaql-core
    teaql-macros
    teaql-runtime
    teaql-sql
    teaql-data-service
    teaql-provider-sqlite
)

for manifest in "$@"; do
    if [[ ! -f "$manifest" ]]; then
        printf 'Missing replay manifest: %s\n' "$manifest" >&2
        exit 1
    fi
    for crate_name in "${crates[@]}"; do
        if [[ "$(grep -Ec "^${crate_name} = \"=?[0-9]+\\.[0-9]+\\.[0-9]+\"$" "$manifest")" -ne 1 ]]; then
            printf 'Expected one numeric %s dependency in %s\n' "$crate_name" "$manifest" >&2
            exit 1
        fi
        sed -i -E \
            "s/^(${crate_name} = \"?)[=]?[0-9]+\.[0-9]+\.[0-9]+(\"$)/\1=${version}\2/" \
            "$manifest"
        grep -Fx "${crate_name} = \"=${version}\"" "$manifest"
    done
done
