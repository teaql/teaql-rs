#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir="$(mktemp -d)"
trap 'rm -r -- "$fixture_dir"' EXIT

outer="$fixture_dir/outer.toml"
generated="$fixture_dir/generated.toml"
cp "$repo_dir/examples/order-management/published-replay/Cargo.toml" "$outer"
cp "$repo_dir/examples/order-management/rust-lib-core/lib/Cargo.toml" "$generated"

"$repo_dir/scripts/pin-rust-order-replay-version.sh" 5.0.0 "$outer" "$generated" >/dev/null

for manifest in "$outer" "$generated"; do
    for crate_name in \
        teaql-core teaql-macros teaql-runtime teaql-sql teaql-data-service teaql-provider-sqlite
    do
        grep -Fxq "${crate_name} = \"=5.0.0\"" "$manifest"
    done
done

broken="$fixture_dir/broken.toml"
cp "$generated" "$broken"
sed -i '/^teaql-macros = /d' "$broken"
if "$repo_dir/scripts/pin-rust-order-replay-version.sh" 5.0.0 "$broken" \
    >"$fixture_dir/broken.stdout" 2>"$fixture_dir/broken.stderr"
then
    printf 'Pinning unexpectedly accepted a missing generated dependency\n' >&2
    exit 1
fi
grep -Fq 'Expected one numeric teaql-macros dependency' "$fixture_dir/broken.stderr"

printf 'PASS: outer and generated Order manifests share exact replay version\n'
