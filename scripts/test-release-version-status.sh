#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
helper="$repo_dir/scripts/release-version-status.sh"
scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT

cat > "$scratch/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
expected='https://crates.io/api/v1/crates/teaql-blob-reference/5.0.8'
if [[ "${TEAQL_RELEASE_TEST_LATE_FAILURE:-0}" == 1 ]]; then
    [[ "${!#}" == https://crates.io/api/v1/crates/teaql-*/5.0.8 ]] || exit 91
    if [[ "${!#}" == "$expected" ]]; then printf '500'; else printf '404'; fi
    exit 0
fi
[[ "${!#}" == "$expected" ]] || exit 91
printf '%s' "${TEAQL_RELEASE_TEST_HTTP_STATUS:-000}"
exit "${TEAQL_RELEASE_TEST_CURL_EXIT:-0}"
EOF
chmod +x "$scratch/curl"

for row in '200 present' '404 absent'; do
    read -r status expected <<< "$row"
    actual="$(PATH="$scratch:$PATH" TEAQL_RELEASE_TEST_HTTP_STATUS="$status" \
        "$helper" teaql-blob-reference 5.0.8)"
    [[ "$actual" == "$expected" ]]
done
for status in 000 403 429 500 503 '200 404'; do
    if PATH="$scratch:$PATH" TEAQL_RELEASE_TEST_HTTP_STATUS="$status" \
        "$helper" teaql-blob-reference 5.0.8 > "$scratch/output" 2> "$scratch/error"; then
        printf 'FAIL HTTP %s was accepted as a version state\n' "$status" >&2
        exit 1
    fi
    [[ ! -s "$scratch/output" ]]
done
if PATH="$scratch:$PATH" TEAQL_RELEASE_TEST_HTTP_STATUS=200 \
    TEAQL_RELEASE_TEST_CURL_EXIT=7 "$helper" teaql-blob-reference 5.0.8 \
    > "$scratch/output" 2> "$scratch/error"; then
    printf 'FAIL curl transport error was accepted\n' >&2
    exit 1
fi
[[ ! -s "$scratch/output" ]]
if "$helper" '../invalid' 5.0.8 > "$scratch/output" 2> "$scratch/error"; then
    printf 'FAIL invalid crate name was accepted\n' >&2
    exit 1
fi

# A late collision/registry failure must stop the whole 16-crate release
# before the first cargo publish call, even when the first 15 versions are free.
mkdir -p "$scratch/repo/scripts" "$scratch/fake-bin"
cp "$repo_dir/publish.sh" "$scratch/repo/publish.sh"
cp "$helper" "$scratch/repo/scripts/release-version-status.sh"
"$repo_dir/scripts/verify-release-provenance.sh" --list-crates > "$scratch/crates"
cat > "$scratch/repo/scripts/verify-release-provenance.sh" <<'EOF'
#!/usr/bin/env bash
[[ "${1:-}" == --list-crates ]] || exit 90
cat "$TEAQL_RELEASE_TEST_CRATES"
EOF
cat > "$scratch/fake-bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == -C ]]; then shift 2; fi
case "${1:-}" in
    rev-parse|rev-list|ls-remote) printf '%s\n' "$TEAQL_RELEASE_TEST_COMMIT" ;;
    status|tag) ;;
    *) exit 92 ;;
esac
EOF
cat > "$scratch/fake-bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
    metadata) cat "$TEAQL_RELEASE_TEST_METADATA" ;;
    publish) : > "$TEAQL_RELEASE_TEST_PUBLISHED" ;;
    *) exit 93 ;;
esac
EOF
cp "$scratch/curl" "$scratch/fake-bin/curl"
chmod +x "$scratch/repo/publish.sh" "$scratch/repo/scripts/"*.sh "$scratch/fake-bin/"*
python3 - "$scratch/crates" "$scratch/metadata.json" <<'PY'
import json, sys
names = open(sys.argv[1]).read().splitlines()
assert len(names) == 16 and names[-1] == 'teaql-blob-reference'
json.dump({'packages': [{'name': name, 'version': '5.0.8'} for name in names]},
          open(sys.argv[2], 'w'))
PY
if PATH="$scratch/fake-bin:$PATH" TEAQL_RELEASE_TEST_LATE_FAILURE=1 \
    TEAQL_RELEASE_TEST_COMMIT=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    TEAQL_RELEASE_TEST_CRATES="$scratch/crates" \
    TEAQL_RELEASE_TEST_METADATA="$scratch/metadata.json" \
    TEAQL_RELEASE_TEST_PUBLISHED="$scratch/published" \
    "$scratch/repo/publish.sh" 5.0.8 > "$scratch/output" 2> "$scratch/error"; then
    printf 'FAIL publisher accepted a late registry failure\n' >&2
    exit 1
fi
[[ ! -e "$scratch/published" ]]
! grep -Fq 'Publishing ' "$scratch/output"
grep -Fq 'HTTP 500' "$scratch/error"
printf 'PASS crates.io version state: present, absent, HTTP/transport errors fail closed\n'
