#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
publisher="$repo_dir/publish.sh"

bash -n "$publisher"

if grep -Eq -- '--allow-dirty|--no-verify' "$publisher"; then
    printf 'FAIL publisher bypasses Cargo source or package verification\n' >&2
    exit 1
fi

required_fragments=(
    'verify-release-provenance.sh" --list-crates'
    'status --porcelain --untracked-files=normal'
    'tag -v "$tag"'
    'ls-remote origin "refs/tags/$tag^{}"'
    'ls-remote origin refs/heads/main'
    'https://index.crates.io/$index_path'
    'Refusing resume:'
    'cargo publish --manifest-path "$repo_dir/Cargo.toml" --locked -p "$crate_name"'
    'verify-release-provenance.sh" "$version"'
)

for fragment in "${required_fragments[@]}"; do
    if ! grep -Fq -- "$fragment" "$publisher"; then
        printf 'FAIL publisher is missing contract fragment: %s\n' "$fragment" >&2
        exit 1
    fi
done

printf 'PASS fail-closed publisher contract\n'
