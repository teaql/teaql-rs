#!/usr/bin/env bash
set -euo pipefail
example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runtime_dir="$(cd "$example_dir/../.." && pwd)"
generate=false
wide=false
inheritance=false
for argument in "$@"; do
  case "$argument" in
    --generate) generate=true ;;
    --wide) wide=true ;;
    --inheritance) inheritance=true ;;
    *) printf 'Usage: bash verify.sh [--generate] [--wide] [--inheritance]\n' >&2; exit 2 ;;
  esac
done
if [[ "$generate" == true ]]; then
  : "${TEAQL_CODEGEN_DIR:?Set TEAQL_CODEGEN_DIR to the local generator checkout}"
  mvn -q -f "$TEAQL_CODEGEN_DIR/pom.xml" -pl generator -am \
    '-Dtest=RustGeneratedCrateCompileTest#generatedRustSharedLoadStateRuntimeExample' \
    -Dsurefire.failIfNoSpecifiedTests=false -Dteaql.loadState.generateOnly=true "-Dteaql.loadState.wide=$wide" "-Dteaql.loadState.inheritance=$inheritance" \
    "-Dteaql.rs.dir=$runtime_dir" test
fi
fixture_mode=narrow
if [[ -f "$example_dir/target/generated/fixture-mode.txt" ]]; then
  IFS= read -r fixture_mode <"$example_dir/target/generated/fixture-mode.txt"
fi
if [[ "$wide" == true && "$fixture_mode" != wide ]]; then
  printf 'Wide verification requires a newly generated wide fixture.\n' >&2
  exit 1
fi
case "$fixture_mode" in
  wide) export TEAQL_LOAD_STATE_WIDE=true ;;
  narrow) export TEAQL_LOAD_STATE_WIDE=false ;;
  *) printf 'Invalid generated fixture mode\n' >&2; exit 1 ;;
esac
fixture_inheritance=false
if [[ -f "$example_dir/target/generated/fixture-inheritance.txt" ]]; then
  IFS= read -r fixture_inheritance <"$example_dir/target/generated/fixture-inheritance.txt"
fi
if [[ "$inheritance" == true && "$fixture_inheritance" != true ]]; then
  printf 'Inheritance verification requires a newly generated inherited fixture.\n' >&2
  exit 1
fi
test_targets=(--test generated_school)
case "$fixture_inheritance" in
  true) test_targets+=(--features inheritance --test inherited_school) ;;
  false) ;;
  *) printf 'Invalid generated inheritance mode\n' >&2; exit 1 ;;
esac
if [[ ! -f "$example_dir/target/generated/lib/Cargo.toml" ]]; then
  printf 'Generate first: see the generator test generatedRustSharedLoadStateRuntimeExample with -Dteaql.rs.dir=<this repository>\n' >&2
  exit 1
fi
run_dir="$(mktemp -d -t teaql-generated-load-state.XXXXXXXX)"
printf 'Evidence retained at %s\n' "$run_dir"
cargo metadata --manifest-path "$example_dir/Cargo.toml" --format-version 1 >"$run_dir/dependencies.json"
jq -e --arg root "$runtime_dir/" '
  [.packages[] | select(.name | startswith("teaql-"))] |
  length > 0 and all(.[]; .source == null and (.manifest_path | startswith($root)))
' "$run_dir/dependencies.json" >"$run_dir/local-dependencies-check.txt"
rg --files --hidden --no-ignore "$example_dir/target/generated/lib/src" -0 |
  sort -z | xargs -0 sha256sum >"$run_dir/generated-source.sha256"
export TEAQL_LOAD_STATE_DATABASE="$run_dir/school.sqlite"
for round in first second; do
  export TEAQL_LOAD_STATE_ROUND="$round"
  cargo test --manifest-path "$example_dir/Cargo.toml" "${test_targets[@]}" -- --nocapture \
    >"$run_dir/$round.log" 2>&1 || {
      tail -80 "$run_dir/$round.log" >&2
      exit 1
    }
  rg -F "PASS generated indexed Q/E/Checker/create/update/delete and snapshot sharing $round" "$run_dir/$round.log"
  rg -F "PASS generated Rust typed native JSON roundtrip and snapshot sharing $round" "$run_dir/$round.log"
  rg -F "PASS generated Rust typed JSON graph roundtrip and Empty/NotLoaded isolation" "$run_dir/$round.log"
  rg -F "PASS generated Rust same-identity JSON projection isolation through E API" "$run_dir/$round.log"
  rg -F "PASS generated Rust sparse Checker rejects before provider entry" "$run_dir/$round.log"
  rg -F "PASS generated Rust Q selection and real SQLite column-order invariance" "$run_dir/$round.log"
  rg -F "PASS generated Rust page and chunked stream shared load state" "$run_dir/$round.log"
  rg -F "PASS generated Rust typed Checker reconstruction shares snapshot without widening values" "$run_dir/$round.log"
  rg -F "PASS generated Rust dynamic property borrowed null/missing/zero reads with zero allocation and provider entry" "$run_dir/$round.log"
  rg -F "PASS generated Rust dynamic storage provenance and retry" "$run_dir/$round.log"
  rg -F "PASS generated Rust mixed dynamic Value/Null/NotLoaded list lifetime readback rollback and retry" "$run_dir/$round.log"
  rg -F "PASS generated Rust stored unselected extension survives rollback retry and readonly property is not persisted" "$run_dir/$round.log"
  rg -F "PASS generated Rust namespace serialization and NotLoaded boundary" "$run_dir/$round.log"
  rg -F "PASS generated Rust nested/reverse graph Q/E/JSON and Empty/NotLoaded isolation" "$run_dir/$round.log"
  rg -F "PASS generated Rust nested dynamic Value/NULL/NotLoaded and shared snapshots" "$run_dir/$round.log"
  rg -F "PASS generated Rust LF08 loaded FK and excluded forward details stay distinct" "$run_dir/$round.log"
  rg -F "PASS generated Rust LF09 reverse Loaded/Empty/NotLoaded through Q/E/JSON" "$run_dir/$round.log"
  rg -F "PASS generated Rust LF17 dynamic metadata shared without fixed slots" "$run_dir/$round.log"
  rg -F "PASS generated Rust LF19 fixed derived and persistent same-name namespace isolation" "$run_dir/$round.log"
  rg -F "PASS generated Rust LF23 dynamic availability detaches only one view" "$run_dir/$round.log"
  rg -F "PASS generated Rust LF20 readonly total persists only through modeled materialization" "$run_dir/$round.log"
  rg -F "PASS generated Rust LF11 native readback rollback retains loaded state and retry intent" "$run_dir/$round.log"
  rg -F "PASS generated Rust authoritative missing column rolls back without turning absence into null" "$run_dir/$round.log"
  rg -F "PASS generated Rust original baseline repeat clones allocate zero and preserve loaded state" "$run_dir/$round.log"
  if [[ "$fixture_inheritance" == true ]]; then
    rg -F "PASS generated Rust inherited indexes Q/E/save and snapshot isolation $round" "$run_dir/$round.log"
  fi
done
sha256sum --check --quiet "$run_dir/generated-source.sha256"
printf 'PASS fresh generated Rust shared-load-state twice\n'
