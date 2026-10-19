#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"
# The fixture is loopback-only. Preserve exclusions for the contract tests;
# the example process below uses a proxy-free environment.
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost"
export no_proxy="$NO_PROXY"
proxy_vars=()
for name in HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy; do
  if [[ -n "${!name:-}" ]]; then
    proxy_vars+=("$name")
  fi
done
if (( ${#proxy_vars[@]} > 0 )); then
  printf 'HTTP fixture: proxy variables present (%s); loopback example will not inherit them\n' "${proxy_vars[*]}"
fi
cargo test --quiet -p teaql-http-reqwest
cargo build --quiet -p teaql-examples --bin http_resilience
loopback_env=(
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY
  -u http_proxy -u https_proxy -u all_proxy
  NO_PROXY='*' no_proxy='*' TEAQL_HTTP_LOCAL_FIXTURE_DIAGNOSTICS=1
)
safe_diagnostic() {
  sed -E 's/PRIVATE_(QUERY|HEADER|BODY)[_[:alnum:]]*/[REDACTED]/g'
}
for round in 1 2; do
  # Build with the runner's network settings; execute without ambient proxies.
  if output="$("${loopback_env[@]}" cargo run --offline --quiet -p teaql-examples --bin http_resilience 2>&1)"; then
    :
  else
    status=$?
    printf 'Rust HTTP example round %s exited %s\n' "$round" "$status" >&2
    printf '%s\n' "$output" | safe_diagnostic >&2
    exit "$status"
  fi
  if [[ "$output" == *"PRIVATE_QUERY"* || "$output" == *"PRIVATE_HEADER"* || "$output" == *"PRIVATE_BODY"* ]]; then
    printf 'Rust HTTP example round %s leaked a private request canary\n' "$round" >&2
    exit 1
  fi
  if [[ "$output" != *"HTTP_RESILIENCE_PASS status=200 attempts=1 requests=2 trace=2 lineage=3"* ]]; then
    printf 'Rust HTTP example round %s lacked acceptance marker\n' "$round" >&2
    printf '%s\n' "$output" | safe_diagnostic >&2
    exit 1
  fi
  printf 'PASS Rust HTTP example round %s\n' "$round"
done
