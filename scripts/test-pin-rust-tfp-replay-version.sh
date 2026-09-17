#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir="$(mktemp -d)"
trap 'rm -r -- "$fixture_dir"' EXIT
manifest="$fixture_dir/Cargo.toml"
cp "$repo_dir/release-fixtures/tfp-wire-profile/Cargo.toml" "$manifest"

"$repo_dir/scripts/pin-rust-tfp-replay-version.sh" 5.0.0 "$manifest" >/dev/null
for crate_name in teaql-core teaql-data-service teaql-runtime teaql-tfp-endpoint; do
    grep -Fxq "${crate_name} = \"=5.0.0\"" "$manifest"
done

sed -i '/^teaql-tfp-endpoint = /d' "$manifest"
if "$repo_dir/scripts/pin-rust-tfp-replay-version.sh" 5.0.0 "$manifest" \
    >"$fixture_dir/broken.stdout" 2>"$fixture_dir/broken.stderr"
then
    printf 'Pinning unexpectedly accepted a missing endpoint dependency\n' >&2
    exit 1
fi
grep -Fq 'Expected one numeric teaql-tfp-endpoint dependency' "$fixture_dir/broken.stderr"

printf 'PASS published TFP replay pins one exact official version\n'
