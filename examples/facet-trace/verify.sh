#!/usr/bin/env bash
set -euo pipefail
example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
evidence_dir="$(mktemp -d /tmp/teaql-rust-facet-example-XXXXXX)"
export TEAQL_FACET_DATABASE="$evidence_dir/school.db"
unset TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS
hash_library() {
  (cd "$example_dir/lib" && find . -type f -exec sha256sum {} \; | sort)
}
hash_library > "$evidence_dir/library-before.sha256"
cargo generate-lockfile --offline --manifest-path "$example_dir/Cargo.toml" > "$evidence_dir/lock.log" 2>&1
for round in 1 2; do
  if ! cargo test --offline --locked --manifest-path "$example_dir/Cargo.toml" \
      --test request_intent -- --show-output --test-threads=1 > "$evidence_dir/run-$round.log" 2>&1; then
    tail -100 "$evidence_dir/run-$round.log" >&2
    printf 'FAIL: retained Facet evidence %s\n' "$evidence_dir" >&2
    exit 1
  fi
  for name in generated_facet_inherits_explicit_root_comment_and_purpose \
      future_facet_binding_masks_the_first_root_statement \
      loaded_typed_relation_retains_facets_even_when_empty \
      loaded_relation_future_binding_masks_root_sql; do
    rg -q "test $name .*ok" "$evidence_dir/run-$round.log"
  done
  if ! cargo test --offline --locked --manifest-path "$example_dir/Cargo.toml" \
      --test counted_facets -- --show-output --test-threads=1 > "$evidence_dir/count-$round.log" 2>&1; then
    tail -100 "$evidence_dir/count-$round.log" >&2
    printf 'FAIL: retained counted Facet evidence %s\n' "$evidence_dir" >&2
    exit 1
  fi
  for name in root_count_uses_full_filtered_membership_not_the_visible_page \
      nested_count_retains_original_root_and_empty_parent_metadata \
      loaded_relation_count_retains_ancestor_and_empty_collection \
      future_count_binding_masks_first_statement_but_not_the_next_request; do
    rg -q "test $name .*ok" "$evidence_dir/count-$round.log"
  done
  printf 'PASS generated Rust Facet round %s: same database, no cleanup\n' "$round"
done
hash_library > "$evidence_dir/library-after.sha256"
cmp "$evidence_dir/library-before.sha256" "$evidence_dir/library-after.sha256"
printf 'PASS: Rust generated Facet; unchanged library; database %s; evidence %s\n' \
  "$TEAQL_FACET_DATABASE" "$evidence_dir"
