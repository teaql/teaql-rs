#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
manifest="$example/rust-app-console/Cargo.toml"
if [[ -n "${TEAQL_ORDER_EVIDENCE_DIR:-}" ]]; then
  evidence="$TEAQL_ORDER_EVIDENCE_DIR"
  mkdir "$evidence"
else
  evidence="$(mktemp -d -t teaql-rust-order-evidence.XXXXXXXX)"
fi
owned_target=''
cleanup() {
  status=$?
  trap - EXIT
  if [[ -n "$owned_target" ]]; then rm -rf -- "$owned_target"; fi
  exit "$status"
}
trap cleanup EXIT
if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  owned_target="$(mktemp -d -t teaql-rust-order-build.XXXXXXXX)"
  export CARGO_TARGET_DIR="$owned_target"
fi
fingerprint() {
  (cd "$example" && find rust-lib-core rust-app-console -type f \
    ! -path '*/target/*' ! -path '*/.local/*' -print0 | sort -z | xargs -0 sha256sum)
}
fingerprint > "$evidence/source-before.sha256"
cargo metadata --offline --locked --format-version=1 --manifest-path "$manifest" > "$evidence/metadata.json" 2> "$evidence/metadata.stderr"
cargo build --offline --locked --manifest-path "$manifest" --all-targets > "$evidence/build.log" 2>&1
# The shared-descriptor Order library has no private roll-up test module.
# Run its original six generator-template tests through the peer Order model.
rollup_example="$example/../src/bin/order-edit-reference"
sha256sum "$rollup_example/rust-lib-core/lib/src/rollup_support.rs" > "$evidence/rollup-source.sha256"
cargo test --offline --locked --manifest-path "$rollup_example/rust-app-console/Cargo.toml" \
  -p order-edit-reference-core --lib rollup_support::tests > "$evidence/library-tests.log" 2>&1
binary="$CARGO_TARGET_DIR/debug"
for round in 1 2; do
  TEAQL_EXAMPLE_DATABASE="$evidence/order.sqlite" \
    "$binary/order-management-example" > "$evidence/terminal-$round.log" 2>&1
  TEAQL_NESTED_PROBE_DATABASE="sqlite:file:$evidence/graph.sqlite" \
    "$binary/nested_graph_probe" > "$evidence/graph-$round.log" 2>&1
  TEAQL_EXAMPLE_DATABASE="$evidence/policy.sqlite" \
    "$binary/mutation_policy_probe" > "$evidence/policy-$round.log" 2>&1
  TEAQL_CONTEXT_DOCUMENT_DATABASE="sqlite:file:$evidence/document.sqlite" \
    "$binary/examples/context_bound_document_sqlite" > "$evidence/document-$round.log" 2>&1
done
TEAQL_SAVE_LOAD_STATE_DATABASE="sqlite:file:$evidence/graph.sqlite" \
  "$binary/save_loaded_relation_probe" > "$evidence/loaded-relation.log" 2>&1
TEAQL_SAVE_LOAD_STATE_DATABASE="sqlite:file:$evidence/graph.sqlite" \
  "$binary/save_forward_fk_probe" > "$evidence/forward-fk.log" 2>&1
fingerprint > "$evidence/source-after.sha256"
cmp "$evidence/source-before.sha256" "$evidence/source-after.sha256"
python3 -B - "$evidence" "$example" <<'PY'
import decimal, hashlib, json, pathlib, re, sqlite3, sys
root, example = map(pathlib.Path, sys.argv[1:])
metadata = json.loads((root / 'metadata.json').read_text())
runtime = example.parents[1]
for name in ['teaql-core', 'teaql-macros', 'teaql-runtime', 'teaql-sql', 'teaql-data-service', 'teaql-provider-sqlite']:
    packages = [row for row in metadata['packages'] if row['name'] == name]
    assert len(packages) == 1 and packages[0]['source'] is None, name
    assert pathlib.Path(packages[0]['manifest_path']).resolve() == (runtime / name / 'Cargo.toml').resolve()
for round in [1, 2]:
    text = (root / f'terminal-{round}.log').read_text()
    assert '[query] matched 1 order(s)' in text and 'WEB-2026-001  2026-08-12  129.95' in text
    expected = '[seed] inserted deterministic customer and order' if round == 1 else '[seed] deterministic data already exists; no duplicate rows added'
    assert expected in text
    assert 'MUTATION_POLICY_PASS' in (root / f'policy-{round}.log').read_text()
    assert 'CONTEXT_BOUND_SQLITE_QE_PASS' in (root / f'document-{round}.log').read_text()
    for marker in ['MULTILEVEL_MUTATION_TRACE_PASS', 'HIDDEN_CONFLICT:', 'CANCELLED:', 'TRACE_LINEAGE:']:
        assert marker in (root / f'graph-{round}.log').read_text(), marker
for marker in ['TRANSACTION_SAVE_LOADED_RELATION_PASS', 'SAVE_CHANGED_RELATION_INVALIDATED', 'SAVE_EMPTY_RELATION_PASS', 'SAVE_SCALAR_STATE_PASS']:
    assert marker in (root / 'loaded-relation.log').read_text(), marker
assert 'SAVE_FORWARD_FK_PASS' in (root / 'forward-fk.log').read_text()
rollup_tests = {
    'ordinary_selection_has_no_shared_state_requirement',
    'repeated_target_is_frozen_once_per_resolution',
    'cycles_fail_before_query_execution',
    'grouped_parent_is_never_used_as_an_identity_subquery',
    'mismatched_parent_is_rejected',
    'independent_relation_filter_metadata_survives_navigation',
}
library_log = (root / 'library-tests.log').read_text()
observed_tests = set(re.findall(r'^test rollup_support::tests::(\w+) \.\.\. ok$', library_log, re.M))
assert observed_tests == rollup_tests, (observed_tests, rollup_tests)
assert 'test result: ok. 6 passed; 0 failed; 0 ignored;' in library_log
database = root / 'order.sqlite'
with sqlite3.connect(database.resolve().as_uri() + '?mode=ro&immutable=1', uri=True) as connection:
    orders = connection.execute('SELECT order_number,order_date,total_amount,version FROM customer_order_data').fetchall()
    assert len(orders) == 1 and orders[0][:2] == ('WEB-2026-001', '2026-08-12'), orders
    assert decimal.Decimal(str(orders[0][2])) == decimal.Decimal('129.95') and orders[0][3] == 1
    presets = connection.execute('SELECT request_id,version FROM order_search_preset_data').fetchall()
    assert presets == [('quick-start-pending-orders', 1)], presets
    assert connection.execute('SELECT count(*) FROM customer_data').fetchone() == (1,)
    assert connection.execute('SELECT id,version FROM order_status_data ORDER BY id').fetchall() == [(1001, 1), (1002, 1)]
    assert connection.execute('PRAGMA quick_check').fetchone() == ('ok',)
receipt = dict(schema='teaql.rust-order-business/v1', passed=True, sameTerminalDatabase=True,
    terminalRuns=2, graphRuns=2, policyRuns=2, documentRuns=2, generatedHelperTests=len(observed_tests),
    generatedHelperSourceSHA256=(root / 'rollup-source.sha256').read_text().split()[0],
    generatedScopesUnchanged=True, orders=orders, presets=presets, currentRuntimeSourceVerified=True,
    historicalOrPublishedArtifactIdentityClaimed=False, databaseSHA256=hashlib.sha256(database.read_bytes()).hexdigest())
(root / 'business-receipt.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
print(json.dumps(dict(passed=True, terminalRuns=2, graphRuns=2, policyRuns=2, documentRuns=2)))
PY
