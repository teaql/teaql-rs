#!/usr/bin/env node
'use strict';
const fs = require('node:fs'), path = require('node:path'), os = require('node:os'), assert = require('node:assert/strict'), crypto = require('node:crypto');
const {createOwnedRunner, ownedBytes} = require('../tests/loaded-revert/owned-command.cjs');
const {prepareOwnedSubreaper} = require('../tests/loaded-revert/subreaper.cjs');
const root = path.resolve(__dirname, '../..'), sha = b => crypto.createHash('sha256').update(b).digest('hex');
async function main() {
  const release = process.argv.slice(2).includes('--release'); assert(process.argv.slice(2).every(x => x === '--release'));
  const requested = process.env.TEAQL_MAINSTREAM_EVIDENCE_DIR;
  const output = requested ? path.resolve(requested) : fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-mainstream-evidence-'));
  if (requested) {assert(path.isAbsolute(requested) && !fs.existsSync(output)); fs.mkdirSync(output, {recursive: true});}
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-mainstream-build-'));
  const runner = createOwnedRunner({supervisor: prepareOwnedSubreaper(scratch)});
  const report = {schema: 'teaql.mainstream-isolated-reference/v1', passed: false, samples: release ? 31 : 3,
    processIsolation: true, timedScope: 'query-only; IPC, serialization and connection setup excluded', commands: []};
  const save = () => fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report, null, 2) + '\n');
  const env = {...process.env, CARGO_INCREMENTAL: '0', CARGO_BUILD_JOBS: '2', CARGO_PROFILE_DEV_DEBUG: '0', CARGO_PROFILE_TEST_DEBUG: '0', TMPDIR: scratch};
  delete env.RUST_MIN_STACK;
  async function run(name, command, reference = false) {
    env.CARGO_TARGET_DIR = path.join(scratch, reference ? 'reference-target' : 'candidate-target');
    const file = path.join(output, name + '.log'), fd = fs.openSync(file, 'wx'); let r;
    try {r = await runner.run(command, {cwd: root, env, timeout: 900000, stdio: ['ignore', fd, fd]});} finally {fs.closeSync(fd);}
    const bytes = fs.readFileSync(file); report.commands.push({name, command, exitCode: r.status, signal: r.signal, descendantProof: r.descendantProof, sha256: sha(bytes)}); save();
    assert(r.descendantProof?.safe && !r.signal && !r.error); assert.equal(r.status, 0, name + ': see retained log'); return bytes.toString();
  }
  try {
    const reference = path.join(__dirname, 'diesel-reference/Cargo.toml'), candidate = path.join(__dirname, 'Cargo.toml');
    // Explicit preparation is separate from offline, locked execution.
    await run('fetch-reference', ['cargo', 'fetch', '--locked', '--manifest-path', reference], true);
    await run('fetch-candidate', ['cargo', 'fetch', '--locked', '--manifest-path', candidate]);
    for (const [name, manifest, isReference] of [['reference', reference, true], ['candidate', candidate, false]]) {
      const metadata = JSON.parse(await run(name + '-metadata', ['cargo', 'metadata', '--locked', '--offline', '--format-version=1', '--manifest-path', manifest], isReference));
      const sqlite = metadata.packages.filter(p => p.name === 'libsqlite3-sys'); assert.equal(sqlite.length, 1);
      if (isReference) assert.equal(sqlite[0].version, '0.38.1');
      else assert(/^0\.39\.[0-9]+$/.test(sqlite[0].version));
      if (isReference) assert(!metadata.packages.some(p => ['teaql-core', 'teaql-runtime', 'rusqlite'].includes(p.name)));
      else assert(!metadata.packages.some(p => p.name === 'diesel'));
      report[name + 'SqliteBinding'] = sqlite[0].version;
    }
    const profile = release ? ['--release'] : [];
    await run('build-reference', ['cargo', 'build', '--locked', '--offline', '--manifest-path', reference, ...profile], true);
    env.TEAQL_DIESEL_REFERENCE_BIN = path.join(scratch, 'reference-target', release ? 'release' : 'debug', 'teaql-diesel-test-reference');
    env.TEAQL_ORM_SAMPLES = String(report.samples);
    const result = await run('agreement', ['cargo', 'test', '--locked', '--offline', '--manifest-path', candidate, '--test', 'mainstream_orm_benchmark', ...profile, '--', '--ignored', '--nocapture', '--test-threads=1']);
    assert(result.includes('PASS mainstream Diesel and TeaQL bounded typed results with independent version and prefix filters'));
    assert(result.includes('1 passed; 0 failed; 0 ignored')); assert(result.includes('ENGINE_VERSION,diesel-sqlite,'));
    report.legacyBindingScope = 'Trusted-fixture test reference only; not a production dependency or runtime artifact';
    report.passed = true;
  } catch (error) {report.error = error.stack; process.exitCode = 1;}
  finally {
    report.cleanup = {scratchRemoved: false, sharedCacheRemoved: false};
    try {assert(runner.safeToClean);
      report.databases = [];
      for (const entry of fs.readdirSync(scratch, {withFileTypes: true})) if (entry.isDirectory() && entry.name.startsWith('teaql-diesel-')) {
        const dir = path.join(output, 'databases', entry.name); fs.mkdirSync(dir, {recursive: true});
        for (const name of fs.readdirSync(path.join(scratch, entry.name))) {
          assert(/^probe\.sqlite(?:-wal|-shm)?$/.test(name)); const bytes = fs.readFileSync(path.join(scratch, entry.name, name));
          fs.writeFileSync(path.join(dir, name), bytes); report.databases.push({file: path.relative(output, path.join(dir, name)), sha256: sha(bytes)});
        }
      }
      report.cleanup.removedBytes = ownedBytes(scratch); fs.rmSync(scratch, {recursive: true}); report.cleanup.scratchRemoved = true;}
    catch (error) {report.cleanup.error = error.stack; report.passed = false; process.exitCode = 1;}
    runner.dispose(); save(); console.log(JSON.stringify({output, passed: report.passed, error: report.error?.split('\n')[0]}));
  }
}
main().catch(error => {console.error(error.stack); process.exitCode = 1;});
