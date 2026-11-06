#!/usr/bin/env node
"use strict";
// Offline source gate. All compiler output lives in one owned scratch tree.
const fs = require("node:fs"), path = require("node:path"), os = require("node:os"), cp = require("node:child_process"), crypto = require("node:crypto"), assert = require("node:assert/strict");
const example = __dirname, repo = path.resolve(example, "../.."), sha = bytes => crypto.createHash("sha256").update(bytes).digest("hex");
function fingerprint() {
  const m = JSON.parse(fs.readFileSync(path.join(example, "fixture-manifest.json")));
  assert.equal(m.language, "rust"); assert.equal(m.evaluationErrors, 0);
  assert.equal(sha(fs.readFileSync(path.join(example, "model.xml"))), m.modelSha256);
  const files = [];
  function visit(dir, prefix = "") { for (const e of fs.readdirSync(dir, {withFileTypes: true})) {
    const name = path.join(prefix, e.name), file = path.join(dir, e.name);
    assert(!e.isSymbolicLink()); if (e.isDirectory()) visit(file, name); else { assert(e.isFile()); files.push(name); }
  } }
  visit(path.join(example, "generated"));
  assert.deepEqual(files.sort(), Object.keys(m.files).sort());
  for (const [name, expected] of Object.entries(m.files)) assert.equal(sha(fs.readFileSync(path.join(example, "generated", name))), expected, name);
  return m;
}
const requested = process.env.TEAQL_FIX_E_EVIDENCE_DIR;
const evidence = requested ? path.resolve(requested) : fs.mkdtempSync(path.join(os.tmpdir(), "teaql-rust-fix-e-evidence-"));
if (requested) { assert(path.isAbsolute(requested) && !fs.existsSync(evidence)); fs.mkdirSync(evidence, {recursive: true}); }
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "teaql-rust-fix-e-build-"));
const report = {schema: "teaql.rust-fix-e-example/v1", passed: false, rounds: [], sameDatabase: true,
  localRuntime: repo, databaseRetained: true, sharedCacheRemoved: false};
const quote = JSON.stringify;
const {createOwnedRunner, ownedBytes} = require('./owned-command.cjs'), runner = createOwnedRunner();
async function main() {
try {
  assert(fs.statfsSync(scratch).bavail * fs.statfsSync(scratch).bsize > 8 * 1024 ** 3, "8 GiB free build space required");
  const m = fingerprint(); report.modelSha256 = m.modelSha256;
  fs.mkdirSync(path.join(scratch, "src"));
  for (const file of fs.readdirSync(example).filter(file => file.endsWith(".rs"))) fs.copyFileSync(path.join(example, file), path.join(scratch, "src", file));
  const local = name => quote(path.join(repo, name));
  const manifest = ['[package]', 'name = "teaql-fix-e-example"', 'version = "0.0.0"', 'edition = "2024"', 'publish = false',
    '[[bin]]', 'name = "teaql-expression-boundaries"', 'path = "src/expression_boundary_probe.rs"',
    '[dependencies]', 'checker-fix-advanced-core = { path = ' + quote(path.join(example, "generated/lib")) + ' }',
    'teaql-core = { path = ' + local('teaql-core') + ' }', 'teaql-runtime = { path = ' + local('teaql-runtime') + ' }',
    'teaql-tfp-endpoint = { path = ' + local('teaql-tfp-endpoint') + ' }',
    'chrono = "0.4"', 'serde_json = "1"', 'futures-util = "0.3"', 'rusqlite = { version = "0.40", features = ["bundled", "trace", "hooks"] }',
    'tokio = { version = "1", features = ["macros", "rt-multi-thread"] }', '[patch.crates-io]',
    ...['teaql-core', 'teaql-macros', 'teaql-runtime', 'teaql-sql', 'teaql-data-service', 'teaql-provider-sqlite'].map(name => name + ' = { path = ' + local(name) + ' }'), ''].join('\n');
  fs.writeFileSync(path.join(scratch, "Cargo.toml"), manifest);
  fs.writeFileSync(path.join(evidence, "consumer-manifest.toml"), manifest);
  const env = {...process.env, CARGO_TARGET_DIR: path.join(scratch, "target"), CARGO_INCREMENTAL: "0",
    CARGO_PROFILE_DEV_DEBUG: "0", CARGO_PROFILE_TEST_DEBUG: "0", CARGO_BUILD_JOBS: "2",
    TEAQL_FIX_E_DB: path.join(evidence, "fixture.sqlite")};
  async function run(command, log, timeout) {
    const fd = fs.openSync(path.join(evidence, log), "w"); let result;
    try { result = await runner.run(command, {cwd: scratch, env, timeout, stdio: ["ignore", fd, fd]}); }
    finally { fs.closeSync(fd); }
    const bytes = fs.readFileSync(path.join(evidence, log));
    report.rounds.push({command, exitCode: result.status, signal: result.signal, log, logSha256: sha(bytes)});
    if (result.status !== 0) process.exitCode = result.status || 1;
    assert.equal(result.status, 0, bytes.toString().slice(-12000));
    return bytes.toString();
  }
  await run(['cargo', 'build', '--offline', '--manifest-path', path.join(scratch, 'Cargo.toml')], 'build.log', 540000);
  fs.copyFileSync(path.join(scratch, 'Cargo.lock'), path.join(evidence, 'consumer-Cargo.lock'));
  const binary = path.join(scratch, 'target/debug/teaql-fix-e-example');
  report.binarySha256 = sha(fs.readFileSync(binary));
  for (const round of [1, 2]) {
    env.TEAQL_MIXED_GRAPH_ROUND = String(round);
    env.TEAQL_CHECKER_PIPELINE_DB = path.join(evidence, 'fixture.sqlite.checker-pipeline.sqlite');
    env.TEAQL_CHECKER_PIPELINE_ROUND = String(round);
    env.TEAQL_COMPLETENESS_DB = path.join(evidence, 'fixture.sqlite.nonroot-completeness.sqlite');
    env.TEAQL_COMPLETENESS_ROUND = String(round);
    env.TEAQL_CHECKER_WIRE_DB = path.join(evidence, 'fixture.sqlite.checker-wire.sqlite');
    env.TEAQL_CHECKER_WIRE_ROUND = String(round);
    const log = await run([binary], 'round-' + round + '.log', 90000);
    const concurrent = log.split(/\r?\n/).filter(line => line.startsWith('RUST_CONCURRENT_QUERY='))
      .map(line => JSON.parse(line.slice('RUST_CONCURRENT_QUERY='.length)));
    assert.equal(concurrent.length, 1); assert(concurrent[0].passed && concurrent[0].round === round);
    assert.equal(concurrent[0].overlapEntered, 2); assert.equal(concurrent[0].events.length, 6);
    for (const marker of ['FIX_E_EXAMPLE=', 'CORE_LOAD_EXAMPLE=', 'CORE_MUTATION_OWNERSHIP=', 'CORE_MUTATION_PAGING=', 'RUST_AUTHORITATIVE_TOMBSTONES=', 'RUST_VERSION_BOUNDARIES=', 'RUST_GENERATED_CHECKER_TYPES=', 'RUST_GENERATED_CHECKER_RANGES=', 'RUST_GENERATED_CLOCK_UPDATES=', 'RUST_CONTEXT_ROOT_BOUNDARY=', 'RUST_CANCELLED_CONTEXT_ROOT=', 'RUST_PARTIAL_ANCESTORS_RESULT=', 'RUST_PARTIAL_ANCESTOR_ROOT_RESULT=']) {
      const values = log.split(/\r?\n/).filter(line => line.startsWith(marker)).map(line => JSON.parse(line.slice(marker.length)));
      assert.equal(values.length, 1, marker); assert.equal(values[0].passed, true, marker);
    }
    for (const [marker, count] of [['RUST_MIXED_GRAPH_CASE=', 3], ['RUST_CHECKER_PIPELINE_CASE=', 10], ['RUST_NONROOT_COMPLETENESS_CASE=', 5], ['RUST_CHECKER_WIRE_CASE=', 21], ['RUST_CHECKER_REQUIRED_FIX_CASE=', 2]]) {
      const values = log.split(/\r?\n/).filter(line => line.startsWith(marker)).map(line => JSON.parse(line.slice(marker.length)));
      assert.equal(values.length, count, marker); assert(values.every(row => row.passed && row.round === round), marker);
    }
    for (const marker of ['RUST_CHECKER_PIPELINE_RESULT=', 'RUST_NONROOT_COMPLETENESS_RESULT=', 'RUST_CHECKER_WIRE_RESULT=', 'RUST_CHECKER_REQUIRED_FIX_RESULT=']) {
      const values = log.split(/\r?\n/).filter(line => line.startsWith(marker)).map(line => JSON.parse(line.slice(marker.length)));
      assert.equal(values.length, 1, marker); assert.equal(values[0].passed, true, marker); assert.equal(values[0].round, round, marker);
    }
    assert.deepEqual(fingerprint(), m);
  }
  report.databaseSha256 = sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite')));
  report.mixedGraphDatabase = {file: 'fixture.sqlite.mixed-graph.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.mixed-graph.sqlite')))};
  report.mixedGraphApplicationSha256 = sha(fs.readFileSync(path.join(example, 'mixed_graph_probe.rs')));
  report.concurrentQueryApplicationSha256 = sha(fs.readFileSync(path.join(example, 'concurrent_query_probe.rs')));
  report.concurrentQueryDatabase = {file: 'fixture.sqlite.concurrent-query.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.concurrent-query.sqlite')))};
  report.checkerTypeDatabase = {file: 'fixture.sqlite.checker-types.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.checker-types.sqlite')))};
  report.checkerRangeDatabase = {file: 'fixture.sqlite.checker-ranges.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.checker-ranges.sqlite')))};
  report.rangeApplicationSha256 = sha(fs.readFileSync(path.join(example, 'checker_range_probe.rs')));
  report.clockUpdateDatabase = {file: 'fixture.sqlite.clock-updates.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.clock-updates.sqlite')))};
  report.clockUpdateApplicationSha256 = sha(fs.readFileSync(path.join(example, 'clock_update_probe.rs')));
  report.contextRootDatabase = {file: 'fixture.sqlite.context-root.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.context-root.sqlite')))};
  report.contextRootApplicationSha256 = sha(fs.readFileSync(path.join(example, 'context_root_probe.rs')));
  report.cancelledRootDatabase = {file: 'fixture.sqlite.cancelled-root.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.cancelled-root.sqlite')))};
  report.cancelledRootApplicationSha256 = sha(fs.readFileSync(path.join(example, 'cancelled_root_probe.rs')));
  report.partialAncestorDatabase = {file: 'fixture.sqlite.partial-ancestors.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.partial-ancestors.sqlite')))};
  report.partialAncestorApplicationSha256 = sha(fs.readFileSync(path.join(example, 'partial_ancestor_probe.rs')));
  report.partialAncestorRootDatabase = {file: 'fixture.sqlite.partial-ancestor-root.sqlite',
    sha256: sha(fs.readFileSync(path.join(evidence, 'fixture.sqlite.partial-ancestor-root.sqlite')))};
  report.checkerBoundaryApplications = Object.fromEntries(['checker_pipeline_probe.rs', 'nonroot_completeness_probe.rs', 'checker_wire_probe.rs']
    .map(file => [file, sha(fs.readFileSync(path.join(example, file)))]));
  report.checkerBoundaryDatabases = ['fixture.sqlite.checker-pipeline.sqlite', 'fixture.sqlite.checker-pipeline.sqlite.no-schema.sqlite',
    'fixture.sqlite.nonroot-completeness.sqlite', 'fixture.sqlite.checker-wire.sqlite'].map(file => ({file, sha256: sha(fs.readFileSync(path.join(evidence, file)))}));
  const expressionBinary = path.join(scratch, 'target/debug/teaql-expression-boundaries');
  report.expressionBoundaryBinarySha256 = sha(fs.readFileSync(expressionBinary));
  report.expressionBoundaryApplicationSha256 = sha(fs.readFileSync(path.join(example, 'expression_boundary_probe.rs')));
  env.TEAQL_EXPRESSION_DB = path.join(evidence, 'fixture.sqlite.expression-boundaries.sqlite');
  report.expressionBoundaryRounds = [];
  for (const round of [1, 2]) {
    assert.equal(fs.existsSync(env.TEAQL_EXPRESSION_DB), round === 2, 'expression rounds must share one unreset database');
    env.TEAQL_EXPRESSION_ROUND = String(round);
    const log = await run([expressionBinary], 'expression-boundaries-round-' + round + '.log', 90000);
    const values = log.split(/\r?\n/).filter(line => line.startsWith('RUST_E_BOUNDARY_RESULT='))
      .map(line => JSON.parse(line.slice('RUST_E_BOUNDARY_RESULT='.length)));
    assert.equal(values.length, 1); assert.equal(values[0].passed, true); assert.equal(values[0].round, round);
    assert.equal(values[0].cases.length, 10);
    report.expressionBoundaryRounds.push(values[0]);
    assert.deepEqual(fingerprint(), m);
  }
  report.expressionBoundaryDatabase = {file: 'fixture.sqlite.expression-boundaries.sqlite',
    sha256: sha(fs.readFileSync(env.TEAQL_EXPRESSION_DB))};
  report.passed = true;
  console.log('PASS Rust retained Fix/E/loading/ownership example twice on one database');
} catch (error) { console.error(error.stack); process.exitCode ||= 1; }
finally {
  // Every spawnSync child has exited before this exact owned build is removed.
  report.disposedOwnedBytes = runner.safeToClean ? ownedBytes(scratch) : 0;
  if (runner.safeToClean) fs.rmSync(scratch, {recursive: true});
  else report.unsafeBuildRetention = {path: scratch, processGroup: runner.unsafeGroup};
  report.scratchRemoved = !fs.existsSync(scratch); runner.dispose();
  fs.writeFileSync(path.join(evidence, 'receipt.json'), JSON.stringify(report, null, 2) + '\n');
  console.log('Retained compact example evidence: ' + evidence);
}
}
main().catch(error => { console.error(error.stack); process.exitCode ||= 1; });
