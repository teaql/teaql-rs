#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_dir"
for round in 1 2; do
  if ! output="$(cargo run --quiet -p teaql-examples --bin committed_events 2>&1)"; then
    printf 'Rust committed-events example round %s failed\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"COMMITTED_EVENTS_PASS authoritative=database accepted outbox=1 target=1 rollback=1"* ]]; then
    printf 'Rust committed-events example round %s failed or lacked evidence\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  printf 'PASS Rust committed-events example round %s\n' "$round"
done

school_example="$repo_dir/examples/school-management"
school_evidence="$(mktemp -d -t teaql-committed-school.XXXXXXXX)"
school_library_hash() {
  (cd "$school_example/lib" && rg --files -g '*.rs' -g '*.toml' -g '!target/**' \
    | LC_ALL=C sort | xargs -d '\n' sha256sum)
}
school_library_hash > "$school_evidence/generated-library-before.sha256"
for round in 1 2; do
  if ! output="$(TEAQL_COMMITTED_SCHOOL_SOURCE_DB="$school_evidence/source-$round.sqlite" \
    TEAQL_COMMITTED_SCHOOL_TARGET_DB="$school_evidence/target-$round.sqlite" \
    cargo run --quiet --locked --manifest-path "$school_example/Cargo.toml" \
      --bin committed_q_readback 2>&1)"; then
    printf 'Rust generated committed Q example round %s failed\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"COMMITTED_GENERATED_Q_PASS"* ]]; then
    printf 'Rust generated committed Q example round %s lacked its PASS marker\n%s\n' \
      "$round" "$output" >&2
    exit 1
  fi
  school_library_hash > "$school_evidence/generated-library-after.sha256"
  cmp "$school_evidence/generated-library-before.sha256" \
    "$school_evidence/generated-library-after.sha256"
  printf 'PASS Rust generated committed Q example round %s\n' "$round"
done
printf 'Rust generated committed Q evidence: %s\n' "$school_evidence"
