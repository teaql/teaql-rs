#!/usr/bin/env bash
set -euo pipefail

example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
trace_evidence_dir="${TEAQL_TRACE_CHAIN_EVIDENCE_DIR:-$(mktemp -d /tmp/teaql-trace-chain-evidence-XXXXXX)}"
mkdir -p "$trace_evidence_dir"
export TEAQL_TRACE_CHAIN_DATABASE="${TEAQL_TRACE_CHAIN_DATABASE:-sqlite:file:$trace_evidence_dir/trace-chain.sqlite}"
if [[ "$TEAQL_TRACE_CHAIN_DATABASE" != sqlite:file:* ]]; then
  printf 'FAIL: use one persistent sqlite:file: database for both replays\n' >&2
  exit 1
fi

generated_hash() {
  (cd "$example_dir/rust-lib-core" && find . -type f -exec sha256sum {} \; | sort | sha256sum | cut -d ' ' -f 1)
}
# Database allocation must be exercised, not inferred from the generated early
# allocation path. Each retained native run includes real transaction/rollback
# probes and both directions of declared relation metadata.
for allocation_pass in 1 2; do
  allocation_log="$trace_evidence_dir/native-allocation-$allocation_pass.log"
  allocation_status=0
  timeout 180s cargo test --locked --manifest-path "$example_dir/../../Cargo.toml" \
    -p teaql-provider-sqlite --test ledger_allocated_trace -- --nocapture \
    >"$allocation_log" 2>&1 || allocation_status=$?
  if (( allocation_status != 0 )); then
    sed -n '1,240p' "$allocation_log" >&2
    printf 'FAIL native database allocation replay %s: exit %s; evidence %s\n' "$allocation_pass" "$allocation_status" "$allocation_log" >&2
    exit "$allocation_status"
  fi
  for allocation_case in \
    root_allocated_during_ledger_planning_reaches_command_sql_and_committed_audit \
    child_allocated_during_ledger_planning_retains_its_own_and_parent_identity \
    database_root_allocation_inside_transaction_reaches_all_trace_boundaries \
    database_child_allocation_preserves_existing_parent_and_local_reason \
    database_allocated_root_and_child_form_one_persisted_graph \
    database_allocated_graph_resolves_forward_only_relation_metadata \
    database_allocated_graph_resolves_both_relation_directions_without_duplicate_audit \
    database_allocation_rolls_back_and_retry_uses_new_identity_and_intent \
    database_id_failure_cannot_fall_back_to_an_in_process_identity \
    database_graph_failure_retains_assigned_trace_but_rolls_back_rows_and_retryable_ids \
    database_allocated_explicit_transaction_delivers_audit_only_after_commit; do
    if ! rg -Fq "test $allocation_case ... ok" "$allocation_log"; then
      printf 'FAIL missing native allocation case %s in %s\n' "$allocation_case" "$allocation_log" >&2
      exit 1
    fi
  done
  printf 'PASS native database allocation replay %s (11 cases, real SQLite)\n' "$allocation_pass"
  reference_log="$trace_evidence_dir/native-shared-reference-$allocation_pass.log"
  reference_status=0
  timeout 180s cargo test --locked --manifest-path "$example_dir/../../Cargo.toml" \
    -p teaql-runtime --test flat_identity_graph \
    >"$reference_log" 2>&1 || reference_status=$?
  if (( reference_status != 0 )); then
    sed -n '1,240p' "$reference_log" >&2
    printf 'FAIL native shared-reference replay %s: exit %s; evidence %s\n' "$allocation_pass" "$reference_status" "$reference_log" >&2
    exit "$reference_status"
  fi
  for reference_case in \
    macro_hydration_shares_identity_graph_without_reusing_mutation_ownership \
    shared_read_only_reference_preserves_snapshot_version_and_root_local_intent; do
    if ! rg -Fq "test $reference_case ... ok" "$reference_log"; then
      printf 'FAIL missing native reference case %s in %s\n' "$reference_case" "$reference_log" >&2
      exit 1
    fi
  done
  printf 'PASS native shared-reference replay %s (6 identity-graph cases)\n' "$allocation_pass"
  partition_log="$trace_evidence_dir/native-numeric-partition-$allocation_pass.log"
  partition_status=0
  timeout 180s cargo test --locked --manifest-path "$example_dir/../../Cargo.toml" \
    -p teaql-provider-sqlite --test numeric_partition_trace -- --nocapture \
    >"$partition_log" 2>&1 || partition_status=$?
  if (( partition_status != 0 )); then
    sed -n '1,240p' "$partition_log" >&2
    printf 'FAIL native numeric partition replay %s: exit %s; evidence %s\n' "$allocation_pass" "$partition_status" "$partition_log" >&2
    exit "$partition_status"
  fi
  for partition_case in \
    scalar_partition_keeps_groups_and_does_not_invent_relation_edges \
    loaded_scalar_groups_preserve_only_the_real_relation_edge_window \
    loaded_scalar_groups_preserve_only_the_real_relation_edge_probes \
    cached_partition_having_rebinds_real_sqlite_results; do
    if ! rg -Fq "test $partition_case ... ok" "$partition_log"; then
      printf 'FAIL missing native numeric partition case %s in %s\n' "$partition_case" "$partition_log" >&2
      exit 1
    fi
  done
  printf 'PASS native numeric partition replay %s (4 cases, including actual warm-cache execution)\n' "$allocation_pass"
done
trace_library_before="$(generated_hash)"
for trace_pass in 1 2; do
  trace_log="$trace_evidence_dir/run-$trace_pass.log"
  trace_status=0
  timeout 180s cargo run --quiet --locked --manifest-path "$example_dir/Cargo.toml" >"$trace_log" 2>&1 || trace_status=$?
  if (( trace_status != 0 )); then
    sed -n '1,240p' "$trace_log" >&2
    printf 'FAIL generated Trace Chain replay %s: exit %s; evidence %s\n' "$trace_pass" "$trace_status" "$trace_log" >&2
    exit "$trace_status"
  fi
  for trace_marker in 'TC-MUT-15 PASSED' 'TC-SQL-07 PASSED' 'TC-MUT-09 PASSED' 'TC-MUT-12 PASSED' 'TC-MUT-13 PASSED' 'TC-MUT-14 PASSED' 'TC-MUT-12 SHARED READONLY PASSED' 'TC-MUT-07 GENERATED DATABASE IDS PASSED' 'TC-REQ-10 SUCCESSFUL READBACK PASSED' 'TC-REQ-06 GENERATED SCALAR STREAM PASSED' 'TC-REQ-10 GENERATED PAGE COUNT PASSED'; do
    if ! rg -Fq "$trace_marker" "$trace_log"; then
      printf 'FAIL missing marker %s in %s\n' "$trace_marker" "$trace_log" >&2
      exit 1
    fi
  done
  if ! rg -Fq 'TC-REQ-16 LOADED GRAPH PRIVACY PASSED' "$trace_log"; then
    printf 'FAIL missing loaded graph privacy acceptance in %s\n' "$trace_log" >&2
    exit 1
  fi
  rg -Fq 'PASS FORWARD_NOTLOADED: generated Q/E retains known identity and independent load boundaries' "$trace_log"
  rg -Fq 'TC-MUT-15 SAFE TARGET PASSED unannotated child deletion retains independent typed identity' "$trace_log"
  if ! rg -Fq 'TC-MUT-12 GENERATED CHECKER OVERLAP PASSED logging=false' "$trace_log" ||
     ! rg -Fq 'TC-MUT-12 GENERATED CHECKER OVERLAP PASSED logging=true' "$trace_log"; then
    printf 'FAIL missing generated Checker overlap in both logging modes in %s\n' "$trace_log" >&2
    exit 1
  fi
  if ! rg -Fq 'TC-REQ-16 GRAPH PRIVACY ROLLBACK PASSED' "$trace_log"; then
    printf 'FAIL missing loaded graph privacy rollback acceptance in %s\n' "$trace_log" >&2
    exit 1
  fi
  for ledger_logging in false true; do
    if ! rg -Fq "TC-MUT-10 GENERATED LEDGER PRECEDENCE PASSED logging=$ledger_logging" "$trace_log"; then
      printf 'FAIL missing generated ledger precedence in %s\n' "$trace_log" >&2
      exit 1
    fi
  done
  printf 'PASS generated Trace Chain replay %s (same database, no cleanup)\n' "$trace_pass"
done
trace_library_after="$(generated_hash)"
if [[ "$trace_library_before" != "$trace_library_after" ]]; then
  printf 'FAIL generated library changed during application verification\n' >&2
  exit 1
fi
printf 'PASS generated Trace Chain: normative graph, three relation levels, same-type batches, concurrent saves, shared read-only references, provider failure, readback failure; library hash %s; database %s; evidence %s\n' \
  "$trace_library_after" "$TEAQL_TRACE_CHAIN_DATABASE" "$trace_evidence_dir"
