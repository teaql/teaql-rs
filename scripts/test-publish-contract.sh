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
    'expected 16'
    'status --porcelain --untracked-files=normal'
    'tag -v "$tag"'
    'ls-remote origin "refs/tags/$tag^{}"'
    'ls-remote origin refs/heads/main'
    'https://index.crates.io/$index_path'
    'Refusing resume:'
    'declare -A release_states=()'
    'release-version-status.sh" "$crate_name" "$version"'
    'cargo publish --manifest-path "$repo_dir/Cargo.toml" --locked -p "$crate_name"'
    'verify-release-provenance.sh" "$version"'
)

for fragment in "${required_fragments[@]}"; do
    if ! grep -Fq -- "$fragment" "$publisher"; then
        printf 'FAIL publisher is missing contract fragment: %s\n' "$fragment" >&2
        exit 1
    fi
done

if grep -Fq 'public_version_exists' "$publisher"; then
    printf 'FAIL publisher treats registry lookup errors as version absence\n' >&2
    exit 1
fi

preflight_line="$(grep -nF 'declare -A release_states=()' "$publisher" | cut -d: -f1)"
publish_line="$(grep -nF 'cargo publish --manifest-path' "$publisher" | cut -d: -f1)"
if ((preflight_line >= publish_line)); then
    printf 'FAIL publisher must classify all crate versions before publishing\n' >&2
    exit 1
fi

printf 'PASS fail-closed publisher contract\n'
