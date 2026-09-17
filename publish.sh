#!/usr/bin/env bash
# Publish the retained public TeaQL Rust crate boundary from one signed source.
#
# The script is deliberately fail-closed. It does not publish dirty source,
# bypass Cargo package verification, infer a version, or maintain a second
# crate list independent of the provenance verifier.
set -euo pipefail

usage() {
    printf 'Usage: %s <signed-semver> [--check-only]\n' "$0" >&2
}

version="${1:-}"
mode="${2:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    usage
    exit 2
fi
if [[ -n "$mode" && "$mode" != "--check-only" ]]; then
    usage
    exit 2
fi

for command_name in awk cargo curl cut git jq mktemp sha256sum sleep tar; do
    command -v "$command_name" >/dev/null || {
        printf 'Missing required command: %s\n' "$command_name" >&2
        exit 2
    }
done

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
tag="v$version"
source_commit="$(git -C "$repo_dir" rev-parse HEAD)"
user_agent='teaql-release-publisher/1.0 (info@teaql.io)'
wait_attempts="${TEAQL_PUBLISH_WAIT_ATTEMPTS:-60}"
wait_seconds="${TEAQL_PUBLISH_WAIT_SECONDS:-5}"

if [[ ! "$wait_attempts" =~ ^[1-9][0-9]*$ || ! "$wait_seconds" =~ ^[1-9][0-9]*$ ]]; then
    printf 'TEAQL_PUBLISH_WAIT_ATTEMPTS and TEAQL_PUBLISH_WAIT_SECONDS must be positive integers\n' >&2
    exit 2
fi

mapfile -t crates < <("$repo_dir/scripts/verify-release-provenance.sh" --list-crates)
if [[ "${#crates[@]}" -ne 9 ]]; then
    printf 'Refusing release: retained public crate contract contains %s entries, expected 9\n' \
        "${#crates[@]}" >&2
    exit 1
fi

if [[ -n "$(git -C "$repo_dir" status --porcelain --untracked-files=normal)" ]]; then
    printf 'Refusing release from a dirty source tree\n' >&2
    exit 1
fi

tag_commit="$(git -C "$repo_dir" rev-list -n 1 "$tag" 2>/dev/null || true)"
if [[ -z "$tag_commit" || "$tag_commit" != "$source_commit" ]]; then
    printf 'Refusing release: signed tag %s must point to HEAD %s\n' \
        "$tag" "$source_commit" >&2
    exit 1
fi
if ! git -C "$repo_dir" tag -v "$tag" >/dev/null 2>&1; then
    printf 'Refusing release: tag %s is not verifiably signed by a trusted local key\n' \
        "$tag" >&2
    exit 1
fi

remote_tag_commit="$(git -C "$repo_dir" ls-remote origin "refs/tags/$tag^{}" | awk 'NR == 1 { print $1 }')"
if [[ "$remote_tag_commit" != "$source_commit" ]]; then
    printf 'Refusing release: pushed annotated tag %s resolves to %s, expected %s\n' \
        "$tag" "${remote_tag_commit:-<absent>}" "$source_commit" >&2
    exit 1
fi
remote_main_commit="$(git -C "$repo_dir" ls-remote origin refs/heads/main | awk 'NR == 1 { print $1 }')"
if [[ "$remote_main_commit" != "$source_commit" ]]; then
    printf 'Refusing release: origin/main is %s, expected release source %s\n' \
        "${remote_main_commit:-<absent>}" "$source_commit" >&2
    exit 1
fi

metadata="$(cargo metadata --manifest-path "$repo_dir/Cargo.toml" --locked --no-deps --format-version 1)"
for crate_name in "${crates[@]}"; do
    crate_version="$(jq -er --arg name "$crate_name" \
        '.packages[] | select(.name == $name) | .version' <<<"$metadata")"
    if [[ "$crate_version" != "$version" ]]; then
        printf 'Refusing release: %s has manifest version %s, expected %s\n' \
            "$crate_name" "$crate_version" "$version" >&2
        exit 1
    fi
done

archive_dir="$(mktemp -d)"
trap 'rm -r -- "$archive_dir"' EXIT

verify_public_archive() {
    local crate_name="$1"
    local api_url="https://crates.io/api/v1/crates/$crate_name/$version"
    local archive_url="https://static.crates.io/crates/$crate_name/$crate_name-$version.crate"
    local archive_path="$archive_dir/$crate_name-$version.crate"
    local version_json official_checksum actual_checksum vcs_info crate_commit dirty

    version_json="$(curl -fsSL --retry 2 --max-time 20 \
        -H "User-Agent: $user_agent" "$api_url")" || return 1
    official_checksum="$(jq -er \
        '.version.checksum | select(test("^[0-9a-f]{64}$"))' <<<"$version_json")" || return 1
    curl -fsSL --retry 2 --max-time 30 -H "User-Agent: $user_agent" \
        -o "$archive_path" "$archive_url" || return 1
    actual_checksum="$(sha256sum "$archive_path" | cut -d' ' -f1)"
    [[ "$actual_checksum" == "$official_checksum" ]] || return 1
    vcs_info="$(tar -xOzf "$archive_path" \
        "$crate_name-$version/.cargo_vcs_info.json")" || return 1
    crate_commit="$(jq -er \
        '.git.sha1 | select(test("^[0-9a-f]{40}$"))' <<<"$vcs_info")" || return 1
    dirty="$(jq -r '.git.dirty // false' <<<"$vcs_info")"
    [[ "$crate_commit" == "$source_commit" && "$dirty" == false ]]
}

public_version_exists() {
    local crate_name="$1"
    curl -fsSL --retry 2 --max-time 20 -H "User-Agent: $user_agent" \
        "https://crates.io/api/v1/crates/$crate_name/$version" >/dev/null 2>&1
}

sparse_index_has_version() {
    local crate_name="$1"
    local normalized_name="${crate_name,,}"
    local crate_length="${#normalized_name}"
    local index_path index_records
    case "$crate_length" in
        1) index_path="1/$normalized_name" ;;
        2) index_path="2/$normalized_name" ;;
        3) index_path="3/${normalized_name:0:1}/$normalized_name" ;;
        *) index_path="${normalized_name:0:2}/${normalized_name:2:2}/$normalized_name" ;;
    esac
    index_records="$(curl -fsSL --retry 2 --max-time 20 \
        -H "User-Agent: $user_agent" "https://index.crates.io/$index_path")" || return 1
    jq -se --arg version "$version" \
        'any(.vers == $version and .yanked == false)' <<<"$index_records" >/dev/null
}

wait_for_public_archive() {
    local crate_name="$1"
    local attempt
    for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
        if verify_public_archive "$crate_name" && sparse_index_has_version "$crate_name"; then
            printf 'PASS public archive %-24s version=%s source=%s\n' \
                "$crate_name" "$version" "$source_commit"
            return 0
        fi
        if ((attempt < wait_attempts)); then
            sleep "$wait_seconds"
        fi
    done
    printf 'FAIL %s %s did not expose a checksum-valid clean archive from %s\n' \
        "$crate_name" "$version" "$source_commit" >&2
    return 1
}

printf 'PASS release preflight: version=%s crates=%s source=%s tag=%s\n' \
    "$version" "${#crates[@]}" "$source_commit" "$tag"
if [[ "$mode" == "--check-only" ]]; then
    exit 0
fi

for crate_name in "${crates[@]}"; do
    if public_version_exists "$crate_name"; then
        if verify_public_archive "$crate_name"; then
            wait_for_public_archive "$crate_name"
            printf 'SKIP already published and provenance-matched: %s %s\n' \
                "$crate_name" "$version"
            continue
        fi
        printf 'Refusing resume: %s %s already exists but does not match source %s\n' \
            "$crate_name" "$version" "$source_commit" >&2
        exit 1
    fi
    printf 'Publishing %s %s from %s\n' "$crate_name" "$version" "$source_commit"
    cargo publish --manifest-path "$repo_dir/Cargo.toml" --locked -p "$crate_name"
    wait_for_public_archive "$crate_name"
done

"$repo_dir/scripts/verify-release-provenance.sh" "$version"
