#!/usr/bin/env bash
# teaql-rs#135: a published release is qualified only by official archive bytes,
# one source commit, and a valid signed tag on that exact commit.
set -euo pipefail

version="${1:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'Usage: %s <published-semver>\n' "$0" >&2
    exit 2
fi

for command_name in curl jq sha256sum tar git mktemp; do
    command -v "$command_name" >/dev/null || {
        printf 'Missing required command: %s\n' "$command_name" >&2
        exit 2
    }
done

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
tag="v$version"
archive_dir="$(mktemp -d)"
trap 'rm -r -- "$archive_dir"' EXIT
user_agent='teaql-release-provenance/1.0 (info@teaql.io)'
crates=(
    teaql-macros teaql-core teaql-data-service teaql-sql teaql-runtime
    teaql-provider-sqlite teaql-provider-postgres teaql-provider-mysql
)
source_commit=''

for crate_name in "${crates[@]}"; do
    api_url="https://crates.io/api/v1/crates/$crate_name/$version"
    archive_url="https://static.crates.io/crates/$crate_name/$crate_name-$version.crate"
    archive_path="$archive_dir/$crate_name-$version.crate"
    official_checksum="$(curl -fsSL --retry 2 --max-time 20 -H "User-Agent: $user_agent" "$api_url" \
        | jq -er '.version.checksum | select(test("^[0-9a-f]{64}$"))')"
    curl -fsSL --retry 2 --max-time 30 -H "User-Agent: $user_agent" \
        -o "$archive_path" "$archive_url"
    actual_checksum="$(sha256sum "$archive_path" | cut -d' ' -f1)"
    if [[ "$actual_checksum" != "$official_checksum" ]]; then
        printf 'FAIL %s %s checksum: archive=%s crates.io=%s\n' \
            "$crate_name" "$version" "$actual_checksum" "$official_checksum" >&2
        exit 1
    fi

    vcs_info="$(tar -xOzf "$archive_path" \
        "$crate_name-$version/.cargo_vcs_info.json")"
    crate_commit="$(jq -er '.git.sha1 | select(test("^[0-9a-f]{40}$"))' <<<"$vcs_info")"
    dirty="$(jq -r '.git.dirty // false' <<<"$vcs_info")"
    if [[ "$dirty" != false ]]; then
        printf 'FAIL %s %s archive came from a dirty source tree\n' \
            "$crate_name" "$version" >&2
        exit 1
    fi
    if [[ -n "$source_commit" && "$crate_commit" != "$source_commit" ]]; then
        printf 'FAIL %s %s source=%s differs from prior source=%s\n' \
            "$crate_name" "$version" "$crate_commit" "$source_commit" >&2
        exit 1
    fi
    source_commit="$crate_commit"
    printf 'PASS archive %-24s %s %s\n' "$crate_name" "$actual_checksum" "$crate_commit"
done

git -C "$repo_dir" cat-file -e "$source_commit^{commit}" || {
    printf 'FAIL release source commit %s is absent from this checkout\n' \
        "$source_commit" >&2
    exit 1
}

if ! git -C "$repo_dir" rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    printf 'RED release provenance: eight archives match crates.io and source %s, but signed tag %s is absent\n' \
        "$source_commit" "$tag" >&2
    exit 1
fi
tag_commit="$(git -C "$repo_dir" rev-list -n 1 "$tag")"
if [[ "$tag_commit" != "$source_commit" ]]; then
    printf 'FAIL tag %s points to %s, archive source is %s\n' \
        "$tag" "$tag_commit" "$source_commit" >&2
    exit 1
fi
if ! git -C "$repo_dir" tag -v "$tag" >/dev/null 2>&1; then
    printf 'FAIL tag %s is not verifiably signed by a trusted local key\n' \
        "$tag" >&2
    exit 1
fi

printf 'PASS signed published release: version=%s crates=%s source=%s tag=%s\n' \
    "$version" "${#crates[@]}" "$source_commit" "$tag"
