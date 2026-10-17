#!/usr/bin/env bash
set -euo pipefail
command -v openssl >/dev/null || { echo 'OpenSSL CLI is required for the local TLS fixture' >&2; exit 1; }
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_dir"
for round in 1 2; do
  if ! output="$(cargo run --quiet -p teaql-examples --bin secret_provider 2>&1)"; then
    printf 'Rust Secret Provider example round %s failed\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"SECRET_PROVIDER_PASS binding=1 rotation=1 masking=1 operational_expiry=1 shared_custom_spi=1"* ]] ||
     [[ "$output" == *"private-example-key"* ]]; then
    printf 'Rust Secret Provider example round %s lacked evidence or leaked a canary\n' "$round" >&2
    exit 1
  fi
  printf 'PASS Rust Secret Provider example round %s\n' "$round"
  if ! output="$(cargo run --quiet -p teaql-examples --bin vault_kv2 2>&1)"; then
    printf 'Rust Vault KV v2 example round %s failed\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"VAULT_KV2_PASS path=1 token=1 version=1 timeout=1 masking=1 tls=1 diagnostics=1"* ]] ||
     [[ "$output" == *"PRIVATE-VAULT"* ]]; then
    printf 'Rust Vault KV v2 example round %s lacked evidence or leaked a canary\n' "$round" >&2
    exit 1
  fi
  printf 'PASS Rust Vault KV v2 example round %s\n' "$round"
done
