#!/usr/bin/env bash
set -euo pipefail
command -v openssl >/dev/null || { echo 'OpenSSL CLI is required for the local TLS fixture' >&2; exit 1; }
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_dir"
evidence_dir="${TEAQL_SECRET_EVIDENCE_DIR:-}"
if [[ -n "$evidence_dir" ]]; then
  [[ "$evidence_dir" == /* && ! -e "$evidence_dir" && ! -L "$evidence_dir" ]] || {
    echo 'Use a new absolute Secret Provider evidence directory' >&2; exit 1;
  }
  mkdir -p "$evidence_dir"
fi
for round in 1 2; do
  native_status=0
  output="$(cargo run --quiet -p teaql-examples --bin secret_provider 2>&1)" || native_status=$?
  if [[ -n "$evidence_dir" ]]; then
    printf '%s\n' "$output" >"$evidence_dir/provider-native-$round.log"
  fi
  if (( native_status != 0 )); then
    printf 'Rust Secret Provider example round %s failed\n%s\n' "$round" "$output" >&2
    exit "$native_status"
  fi
  if [[ "$output" != *"SECRET_PROVIDER_PASS binding=1 rotation=1 masking=1 operational_expiry=1 shared_custom_spi=1 ledger_isolation=1 vault_request_isolation=1 rotation_ledger_unchanged=1"* ]] ||
     [[ "$output" == *"private-example-key"* ]]; then
    printf 'Rust Secret Provider example round %s lacked evidence or leaked a canary\n' "$round" >&2
    exit 1
  fi
  printf 'PASS Rust Secret Provider example round %s\n' "$round"
  native_status=0
  output="$(cargo run --quiet -p teaql-examples --bin vault_kv2 2>&1)" || native_status=$?
  if [[ -n "$evidence_dir" ]]; then
    printf '%s\n' "$output" >"$evidence_dir/vault-native-$round.log"
  fi
  if (( native_status != 0 )); then
    printf 'Rust Vault KV v2 example round %s failed\n%s\n' "$round" "$output" >&2
    exit "$native_status"
  fi
  if [[ "$output" != *"VAULT_KV2_PASS path=1 token=1 version=1 timeout=1 masking=1 tls=1 diagnostics=1"* ]] ||
     [[ "$output" == *"PRIVATE-VAULT"* ]]; then
    printf 'Rust Vault KV v2 example round %s lacked evidence or leaked a canary\n' "$round" >&2
    exit 1
  fi
  printf 'PASS Rust Vault KV v2 example round %s\n' "$round"
done
