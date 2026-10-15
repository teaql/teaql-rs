#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_dir"
for round in 1 2; do
  if ! output="$(cargo run --quiet -p teaql-examples --bin secret_provider 2>&1)"; then
    printf 'Rust Secret Provider example round %s failed\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"SECRET_PROVIDER_PASS binding=1 rotation=1 masking=1"* ]] ||
     [[ "$output" == *"private-example-key"* ]]; then
    printf 'Rust Secret Provider example round %s lacked evidence or leaked a canary\n' "$round" >&2
    exit 1
  fi
  printf 'PASS Rust Secret Provider example round %s\n' "$round"
done
