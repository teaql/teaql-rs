#!/usr/bin/env bash
set -euo pipefail
example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$example_dir/../../scripts/rust-test-receipt.sh"
workspace_dir="$example_dir/rust-app-console"
owned_build=""
cleanup() {
  result=$?
  trap - EXIT
  if [[ -n "$owned_build" ]]; then rm -rf -- "$owned_build"; fi
  exit "$result"
}
trap cleanup EXIT
if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  owned_build="$(mktemp -d -t teaql-rust-facet-build.XXXXXXXX)"
  export CARGO_TARGET_DIR="$owned_build/target"
  export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
fi
if [[ -n "${TEAQL_FACET_EVIDENCE_DIR:-}" ]]; then
  evidence_dir="$TEAQL_FACET_EVIDENCE_DIR"
  [[ ! -e "$evidence_dir" ]] || { echo "Use a new Facet evidence directory" >&2; exit 1; }
  mkdir -p "$evidence_dir"
elif [[ -n "${TEAQL_EXAMPLES_EVIDENCE_DIR:-}" ]]; then
  evidence_dir="$TEAQL_EXAMPLES_EVIDENCE_DIR/facet-trace"
  [[ ! -e "$evidence_dir" ]] || { echo "Use a new Facet evidence directory" >&2; exit 1; }
  mkdir -p "$evidence_dir"
else
  evidence_dir="$(mktemp -d -t teaql-rust-facet-example.XXXXXXXX)"
fi
export TEAQL_FACET_DATABASE="$evidence_dir/school.db"
unset TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS
hash_library() {
  (cd "$example_dir" && find rust-lib-core -type f -exec sha256sum {} \; | sort)
}
hash_library > "$evidence_dir/library-before.sha256"
sha256sum "$example_dir/Cargo.lock" > "$evidence_dir/lock-before.sha256"
# Verification consumes the committed dependency graph; an offline cache must
# not silently select newer packages and rewrite the application's lockfile.
cargo metadata --offline --locked --no-deps --format-version 1 \
  --manifest-path "$workspace_dir/Cargo.toml" > "$evidence_dir/lock.log" 2>&1
for round in 1 2; do
  if ! cargo run --offline --locked --manifest-path "$workspace_dir/Cargo.toml" \
      -- "$evidence_dir/showcase.sqlite" > "$evidence_dir/showcase-$round.log" 2>&1; then
    tail -100 "$evidence_dir/showcase-$round.log" >&2
    printf 'FAIL: school directory evidence %s\n' "$evidence_dir" >&2
    exit 1
  fi
  if ! cargo test --offline --locked --manifest-path "$workspace_dir/Cargo.toml" \
      --test request_intent -- --show-output --test-threads=1 > "$evidence_dir/run-$round.log" 2>&1; then
    tail -100 "$evidence_dir/run-$round.log" >&2
    printf 'FAIL: retained Facet evidence %s\n' "$evidence_dir" >&2
    exit 1
  fi
  for name in generated_facet_inherits_explicit_root_comment_and_purpose \
      future_facet_binding_masks_the_first_root_statement \
      loaded_typed_relation_retains_facets_even_when_empty \
      loaded_relation_future_binding_masks_root_sql; do
    native_case_passed "$name" "$evidence_dir/run-$round.log"
  done
  if ! cargo test --offline --locked --manifest-path "$workspace_dir/Cargo.toml" \
      --test counted_facets -- --show-output --test-threads=1 > "$evidence_dir/count-$round.log" 2>&1; then
    tail -100 "$evidence_dir/count-$round.log" >&2
    printf 'FAIL: retained counted Facet evidence %s\n' "$evidence_dir" >&2
    exit 1
  fi
  for name in root_count_uses_full_filtered_membership_not_the_visible_page \
      nested_count_retains_original_root_and_empty_parent_metadata \
      loaded_relation_count_retains_ancestor_and_empty_collection \
      future_count_binding_masks_first_statement_but_not_the_next_request; do
    native_case_passed "$name" "$evidence_dir/count-$round.log"
  done
  if ! cargo test --offline --locked --manifest-path "$workspace_dir/Cargo.toml" \
      --test web_response -- --show-output --test-threads=1 > "$evidence_dir/response-$round.log" 2>&1; then
    tail -100 "$evidence_dir/response-$round.log" >&2
    printf 'FAIL: retained response projection evidence %s\n' "$evidence_dir" >&2
    exit 1
  fi
  native_case_passed generated_query_response_omits_unselected_defaults_and_editing_state \
    "$evidence_dir/response-$round.log"
  printf 'PASS generated Rust Facet round %s: same database, no cleanup\n' "$round"
done
python3 - "$evidence_dir" <<'PY'
import json
from pathlib import Path
import sys
root = Path(sys.argv[1])
runs = []
for round_number in [1, 2]:
    lines = (root / f'showcase-{round_number}.log').read_text().splitlines()
    matches = [json.loads(line.removeprefix('SHOWCASE_RESULT '))
               for line in lines if line.startswith('SHOWCASE_RESULT ')]
    assert len(matches) == 1
    result = matches[0]
    assert result['scenario'] == 'school-directory-facets' and result['schools'] == 3
    assert result['created'] == (3 if round_number == 1 else 0)
    assert result['missingAuditRejectedWithoutWrite']
    assert result['generatedLibraryScope'] == 'rust-lib-core'
    assert result['applicationScope'] == 'rust-app-console'
    assert [case['membershipCounts'] for case in result['cases']] == [
        {'PRIMARY': 2, 'SECONDARY': 1}, {'PRIMARY': 2}, {'PRIMARY': 2, 'SECONDARY': 0}]
    assert all(case['visibleSchools'] == 1 for case in result['cases'])
    runs.append(result)
with (root / 'business-receipt.json').open('x') as stream:
    json.dump({'passed': True, 'runs': runs, 'sameDatabase': True,
               'originalRegressionTestsRetained': 9}, stream, indent=2)
    stream.write('\n')
print('PASS: school directory workspace; real rows, facets, counts, audit rejection and idempotent repeat')
PY
hash_library > "$evidence_dir/library-after.sha256"
cmp "$evidence_dir/library-before.sha256" "$evidence_dir/library-after.sha256"
sha256sum "$example_dir/Cargo.lock" > "$evidence_dir/lock-after.sha256"
cmp "$evidence_dir/lock-before.sha256" "$evidence_dir/lock-after.sha256"
printf 'PASS: Rust generated Facet; unchanged library; database %s; evidence %s\n' \
  "$TEAQL_FACET_DATABASE" "$evidence_dir"
