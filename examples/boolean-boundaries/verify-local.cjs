#!/usr/bin/env node
'use strict';
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const cp = require('node:child_process'), crypto = require('node:crypto'), assert = require('node:assert/strict');
const fixture = require('../tests/loaded-revert/verify-local.cjs');
const {sourceIdentity} = require('../expression-profiles/verify-local.cjs');
const {createOwnedRunner, ownedBytes} = require('../tests/loaded-revert/owned-command.cjs');
const {prepareOwnedSubreaper} = require('../tests/loaded-revert/subreaper.cjs');
const EXAMPLE = __dirname, ROOT = path.resolve(EXAMPLE, '../..');
const FIXTURE = path.resolve(EXAMPLE, '../tests/loaded-revert');
const CRATES = ['teaql-core', 'teaql-macros', 'teaql-runtime', 'teaql-sql', 'teaql-data-service', 'teaql-provider-sqlite'];
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
async function main() {
  const requested = process.env.TEAQL_BOOLEAN_EVIDENCE_DIR;
  const output = requested ? path.resolve(requested) : fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-boolean-evidence-'));
  if (requested) { assert(path.isAbsolute(requested) && !fs.existsSync(output)); fs.mkdirSync(output, {recursive: true}); }
  const report = {schema: 'teaql.rust-generated-boolean-example/v1', passed: false, sourceRoot: ROOT,
    localRuntimeOnly: true, sameDatabase: true, completeExampleInventoryAccepted: false,
    installedArtifactAccepted: false, publicPublication: false, commands: [], rounds: []};
  const save = () => fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report, null, 2) + '\n');
  let scratch, runner;
  try {
    const space = fs.statfsSync(os.tmpdir()); assert(space.bavail * space.bsize > 6 * 1024 ** 3);
    report.fixture = fixture.fingerprint();
    // An inventory may run in a byte-identical disposable source copy. The
    // shared identity check binds that copy to an explicit real Git origin;
    // never copy or manufacture .git metadata just to satisfy this example.
    report.sourceBefore = sourceIdentity();
    report.sourceCommit = report.sourceBefore.commit;
    report.applicationSha256 = sha(fs.readFileSync(path.join(EXAMPLE, 'main.rs')));
    report.controllerSha256 = sha(fs.readFileSync(__filename));
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-boolean-build-'));
    report.scratch = scratch;
    runner = createOwnedRunner({supervisor: prepareOwnedSubreaper(scratch)});
    fs.cpSync(path.join(FIXTURE, 'generated/lib'), path.join(scratch, 'lib'), {recursive: true});
    report.libraryMetadata = require('../../scripts/pin-local-example-library.cjs').pin(path.join(scratch, 'lib'), ROOT, scratch);
    fs.copyFileSync(path.join(FIXTURE, 'rust-integral-generated.rs'), path.join(scratch, 'prepared.rs'));
    report.applicationDriverAdaptation = require('../../scripts/adapt-sqlite-probe.cjs').adapt(path.join(scratch, 'prepared.rs'), scratch);
    fs.copyFileSync(path.join(EXAMPLE, 'main.rs'), path.join(scratch, 'main.rs'));
    for (const name of ['main.rs', 'prepared.rs']) fs.copyFileSync(path.join(scratch, name), path.join(output, name));
    const cargo = ['[package]', 'name="teaql-boolean-generated-example"', 'version="0.0.0"', 'edition="2024"',
      'publish=false', '[workspace]', '[[bin]]', 'name="boolean-generated"', 'path="main.rs"', '[dependencies]',
      'scalar-type-conformance-service-core={path="lib"}',
      ...CRATES.map(n => n + '={path=' + JSON.stringify(path.join(ROOT, n)) + '}'),
      'chrono={version="0.4",features=["serde"]}', 'serde_json="1"',
      'rust_decimal={version="=1.42.1",features=["serde"]}',
      'rusqlite={version="0.41.1",features=["bundled","trace","hooks"]}',
      'tokio={version="1",features=["macros","rt-multi-thread"]}',
      '[patch.crates-io]', ...CRATES.map(n => n + '={path=' + JSON.stringify(path.join(ROOT, n)) + '}'), ''].join('\n');
    fs.writeFileSync(path.join(scratch, 'Cargo.toml'), cargo);
    fs.writeFileSync(path.join(output, 'Cargo.toml'), cargo);
    fs.copyFileSync(path.join(ROOT, 'Cargo.lock'), path.join(scratch, 'Cargo.lock'));
    const env = {...process.env, TMPDIR: scratch, CARGO_TARGET_DIR: path.join(scratch, 'target'),
      CARGO_INCREMENTAL: '0', CARGO_BUILD_JOBS: '2', CARGO_PROFILE_DEV_DEBUG: '0', CARGO_PROFILE_TEST_DEBUG: '0',
      TEAQL_TRACE_ENABLED: 'false', TEAQL_SQL_LOG_ENABLED: 'false', TEAQL_TELEMETRY_ENABLED: 'false',
      TEAQL_BOOLEAN_DB: path.join(output, 'boolean.sqlite')};
    delete env.TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS;
    report.environment = {target: env.CARGO_TARGET_DIR, offline: true, jobs: 2, incremental: false,
      dependencyDownloadsShared: true, executableOutputsShared: false};
    async function run(name, command) {
      const out = fs.openSync(path.join(output, name + '.stdout.log'), 'wx');
      const err = fs.openSync(path.join(output, name + '.stderr.log'), 'wx'); let result;
      try { result = await runner.run(command, {cwd: scratch, env, timeout: 900000, stdio: ['ignore', out, err]}); }
      finally { fs.closeSync(out); fs.closeSync(err); }
      const stdout = fs.readFileSync(path.join(output, name + '.stdout.log'));
      const stderr = fs.readFileSync(path.join(output, name + '.stderr.log'));
      report.commands.push({name, command, exitCode: result.status, signal: result.signal,
        descendantProof: result.descendantProof, stdoutSha256: sha(stdout), stderrSha256: sha(stderr)}); save();
      assert(result.descendantProof?.safe); assert.equal(result.signal, null);
      assert.equal(result.status, 0, (stdout.toString() + stderr.toString()).slice(-5000));
      console.log(JSON.stringify({phase: name, exitCode: result.status})); return stdout.toString();
    }
    const metadata = JSON.parse(await run('resolution', ['cargo', 'metadata', '--offline', '--format-version=1']));
    report.origins = CRATES.map(name => {
      const found = metadata.packages.filter(p => p.name === name); assert.equal(found.length, 1);
      const p = found[0]; assert.equal(p.source, null); assert.equal(p.manifest_path, path.join(ROOT, name, 'Cargo.toml'));
      return {name, manifestPath: p.manifest_path, version: p.version};
    });
    fs.copyFileSync(path.join(scratch, 'Cargo.lock'), path.join(output, 'Cargo.lock'));
    report.lockSha256 = sha(fs.readFileSync(path.join(output, 'Cargo.lock')));
    const compiled = await run('compile', ['cargo', 'build', '--offline', '--locked', '--bin', 'boolean-generated', '--message-format=json']);
    const messages = compiled.split(/\r?\n/).filter(l => l.startsWith('{')).map(JSON.parse);
    assert.deepEqual(messages.filter(m => m.reason === 'build-finished').map(m => m.success), [true]);
    const binaries = messages.filter(m => m.reason === 'compiler-artifact' && m.target.name === 'boolean-generated' && m.executable);
    assert.equal(binaries.length, 1); const binary = binaries[0];
    assert(binary.profile.debug_assertions && binary.profile.overflow_checks);
    assert.equal(binary.target.src_path, path.join(scratch, 'main.rs'));
    assert(binary.executable.startsWith(env.CARGO_TARGET_DIR + '/'));
    report.binarySha256 = sha(fs.readFileSync(binary.executable)); report.compiled = binary;
    for (const round of [1, 2]) {
      env.TEAQL_BOOLEAN_ROUND = String(round);
      const stdout = await run('round-' + round, [binary.executable]);
      const rows = [...stdout.matchAll(/^RUST_BOOLEAN_GENERATED=(.+)$/gm)].map(m => JSON.parse(m[1]));
      assert.equal(rows.length, 1); const row = rows[0]; assert.equal(row.passed, true);
      assert.equal(row.round, String(round)); assert.equal(row.version, 3); assert.equal(row.postFixRefusals, 8);
      for (const flag of ['typedQAndE', 'trueFalseUnknown', 'notLoadedIsNotNull', 'rejectedFixLedgerUnchanged', 'legacyIntegerRejected'])
        assert.equal(row[flag], true, flag);
      assert.equal(row.rejectedFixProviderEntries, 0); assert.equal(row.physicalBoolean, 0); assert.equal(row.physicalNull, null);
      report.rounds.push(row); assert.equal(sha(fs.readFileSync(binary.executable)), report.binarySha256);
    }
    assert.deepEqual(fixture.fingerprint(), report.fixture);
    assert.equal(sha(fs.readFileSync(path.join(EXAMPLE, 'main.rs'))), report.applicationSha256);
    report.sourceAfter = sourceIdentity();
    assert.deepEqual(report.sourceAfter, report.sourceBefore);
    report.passed = true;
  } catch (error) { report.error = error.stack; process.exitCode = 1; }
  finally {
    const database = path.join(output, 'boolean.sqlite');
    if (fs.existsSync(database)) report.databaseSha256 = sha(fs.readFileSync(database));
    report.cleanup = {scratchRemoved: false, sharedCachesDeleted: false, databasesDeleted: false};
    try {
      assert(!runner || runner.safeToClean);
      if (scratch) { report.cleanup.disposedOwnedBytes = ownedBytes(scratch); fs.rmSync(scratch, {recursive: true}); }
      report.cleanup.scratchRemoved = true;
    } catch (error) { report.cleanup.error = error.stack; report.passed = false; process.exitCode = 1; }
    runner?.dispose(); save(); console.log(JSON.stringify({output, passed: report.passed, error: report.error, cleanup: report.cleanup}));
  }
}
module.exports = {main};
if (require.main === module) main().catch(e => {console.error(e.stack); process.exitCode = 1;});
