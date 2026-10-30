#!/usr/bin/env bash
# Return an exact crates.io version state; transport and server failures are not absence.
set -euo pipefail

crate_name="${1:-}"
version="${2:-}"
if [[ ! "$crate_name" =~ ^teaql-[a-z0-9-]+$ ||
      ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'Usage: %s <teaql-crate> <semver>\n' "$0" >&2
    exit 2
fi

url="https://crates.io/api/v1/crates/$crate_name/$version"
if ! status="$(curl --silent --show-error --location --retry 2 --max-time 20 \
    --header 'User-Agent: teaql-release-publisher/1.0 (info@teaql.io)' \
    --output /dev/null --write-out '%{http_code}' "$url")"; then
    printf 'Refusing release: crates.io version lookup failed for %s %s\n' \
        "$crate_name" "$version" >&2
    exit 1
fi

case "$status" in
    200) printf 'present\n' ;;
    404) printf 'absent\n' ;;
    *) printf 'Refusing release: crates.io returned HTTP %s for %s %s\n' \
           "$status" "$crate_name" "$version" >&2
       exit 1 ;;
esac
