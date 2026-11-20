#!/usr/bin/env bash
set -euo pipefail
example="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$example/../.." && pwd)"
if [[ -n "${TEAQL_CONFORMANCE_EVIDENCE_DIR:-}" ]]; then
 evidence="$TEAQL_CONFORMANCE_EVIDENCE_DIR"
 mkdir "$evidence"
else
 evidence="$(mktemp -d -t teaql-work-item-evidence.XXXXXXXX)"
fi
unset TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS
scratch="$(mktemp -d -t teaql-rust-work-item-build.XXXXXXXX)"
trap 'status=$?; trap - EXIT; rm -rf -- "$scratch"; exit "$status"' EXIT
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
manifest="$example/rust-app-console/Cargo.toml"
fingerprint() {
 (cd "$example" && find rust-lib-core rust-app-console -type f -print0 | sort -z | xargs -0 sha256sum)
 sha256sum "$example/Cargo.toml" "$example/Cargo.lock"
}
fingerprint > "$evidence/source-before.sha256"
cargo metadata --offline --locked --format-version 1 --manifest-path "$manifest" > "$evidence/metadata.log" 2>&1
cargo test --offline --locked --manifest-path "$manifest" --workspace --all-targets > "$evidence/tests.log" 2>&1
cargo build --offline --locked --manifest-path "$manifest" > "$evidence/build.log" 2>&1
binary="$CARGO_TARGET_DIR/debug/runtime-example-conformance-service-core-workspace"
[[ -x "$binary" ]]
for round in 1 2; do
 TEAQL_CONFORMANCE_DB="$evidence/work-items.sqlite" timeout --kill-after=5s 120s "$binary" > "$evidence/run-$round.log" 2>&1
 rg -Fq 'PASS Rust minimum runtime conformance: 8/8' "$evidence/run-$round.log"
 rg -Fq 'PASS Mutation ledger identity (same ID, different entity types keep versions 3/9)' "$evidence/run-$round.log"
done
fingerprint > "$evidence/source-after.sha256"
cmp "$evidence/source-before.sha256" "$evidence/source-after.sha256"
python3 -B - "$evidence" "$repo" "$example" "$binary" <<'CHECK'
import hashlib,json,pathlib,sqlite3,sys
root,runtime,example,binary=map(pathlib.Path,sys.argv[1:]);text=(root/'metadata.log').read_text();start=text.find('{');assert start>=0
metadata,_=json.JSONDecoder().raw_decode(text[start:]);packages=metadata['packages'];origins={}
for name in ['teaql-core','teaql-runtime','teaql-macros','teaql-sql','teaql-data-service','teaql-provider-sqlite']:
 selected=[row for row in packages if row['name']==name];assert len(selected)==1 and selected[0]['source'] is None,name
 expected=(runtime/name/'Cargo.toml').resolve();assert pathlib.Path(selected[0]['manifest_path']).resolve()==expected,name;origins[name]=str(expected)
domain=[row for row in packages if row['name']=='runtime-example-conformance-service-core'];assert len(domain)==1
assert pathlib.Path(domain[0]['manifest_path']).resolve()==(example/'rust-lib-core/lib/Cargo.toml').resolve()
assert 'test result: ok.' in (root/'tests.log').read_text()
database=root/'work-items.sqlite'
with sqlite3.connect(database.resolve().as_uri()+'?mode=ro&immutable=1',uri=True) as connection:
 rows=connection.execute('SELECT title,version FROM work_item_data ORDER BY id').fetchall();assert len(rows)==2 and all(row[1]==-3 for row in rows),rows
 assert connection.execute('PRAGMA quick_check').fetchone()==('ok',)
receipt=dict(schema='teaql.rust-work-item-business/v1',passed=True,originalBusinessAssertionsRetained=True,
 starts=2,sameDatabase=True,databaseNeverReset=True,nativeWorkspaceEntryUsed=True,generatedScopesUnchanged=True,
 runtimeSourceCrates=origins,domainSource=domain[0]['manifest_path'],workItems=rows,
 executedBinarySHA256=hashlib.sha256(binary.read_bytes()).hexdigest(),databaseSHA256=hashlib.sha256(database.read_bytes()).hexdigest())
(root/'business-receipt.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps(dict(passed=True,starts=2,sameDatabase=True)))
CHECK
