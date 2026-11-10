'use strict';
const {test} = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const {pin} = require('./pin-local-example-library.cjs');
function fixture(run) {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-local-pin-test-'));
  try {
    const runtime = path.join(scratch, 'runtime'), owned = path.join(scratch, 'consumer');
    const library = path.join(owned, 'lib');
    fs.mkdirSync(runtime); fs.mkdirSync(path.join(library, 'src'), {recursive: true});
    fs.writeFileSync(path.join(runtime, 'Cargo.toml'), '[workspace.package]\nversion = "6.0.0"\n');
    const cargo = '[package]\nname="generated-example"\nversion="0.1.0"\n[dependencies]\n' +
      ['teaql-core','teaql-macros','teaql-runtime','teaql-sql','teaql-data-service','teaql-provider-sqlite'].map(name => name + ' = "5.0.7"').join('\n') + '\n';
    fs.writeFileSync(path.join(library, 'Cargo.toml'), cargo);
    fs.writeFileSync(path.join(library, 'src/lib.rs'), 'pub const ORIGINAL: &str = "5.0.7";\n');
    run({runtime, owned, library, cargo});
  } finally {fs.rmSync(scratch, {recursive: true});}
}
test('pins only six numeric runtime coordinates in the disposable consumer', () => fixture(({runtime, owned, library}) => {
  const result = pin(library, runtime, owned);
  assert.equal(result.changes.length, 6); assert.equal(result.version, '6.0.0');
  assert.equal(result.sourceFixtureEdited, false); assert.equal(result.implementationFilesUnchanged, true);
  assert.equal(result.implementationFiles, 1);
  assert.equal(fs.readFileSync(path.join(library, 'src/lib.rs'), 'utf8'), 'pub const ORIGINAL: &str = "5.0.7";\n');
  assert.equal((fs.readFileSync(path.join(library, 'Cargo.toml'), 'utf8').match(/"=6\.0\.0"/g) || []).length, 6);
}));
test('rejects an undeclared owned root without changing metadata', () => fixture(({runtime, library, cargo}) => {
  assert.throws(() => pin(library, runtime, runtime), /explicit owned/);
  assert.equal(fs.readFileSync(path.join(library, 'Cargo.toml'), 'utf8'), cargo);
}));
test('rejects source fixtures even if the caller supplies their parent as owner', () => fixture(({runtime, library}) => {
  const source = path.join(runtime, 'fixtures/lib'); fs.mkdirSync(path.dirname(source)); fs.cpSync(library, source, {recursive: true});
  assert.throws(() => pin(source, runtime, path.dirname(source)), /source fixture/);
}));
test('rejects incomplete dependency inventories atomically', () => fixture(({runtime, owned, library, cargo}) => {
  const invalid = cargo.replace('teaql-macros = "5.0.7"\n', ''); fs.writeFileSync(path.join(library, 'Cargo.toml'), invalid);
  assert.throws(() => pin(library, runtime, owned), /teaql-macros/);
  assert.equal(fs.readFileSync(path.join(library, 'Cargo.toml'), 'utf8'), invalid);
}));
test('rejects symlinked generated implementation before writing', () => fixture(({runtime, owned, library, cargo}) => {
  fs.symlinkSync(path.join(library, 'src/lib.rs'), path.join(library, 'linked.rs'));
  assert.throws(() => pin(library, runtime, owned));
  assert.equal(fs.readFileSync(path.join(library, 'Cargo.toml'), 'utf8'), cargo);
}));
