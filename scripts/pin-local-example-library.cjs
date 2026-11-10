'use strict';
// Only dependency coordinates in an owned, disposable consumer are adapted.
// Frozen source fixtures and their hashes stay unchanged. No Rust source edit,
// package-source override, or public/artifact acceptance is implied.
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict');
const crypto = require('node:crypto');
const CRATES = ['teaql-core', 'teaql-macros', 'teaql-runtime', 'teaql-sql', 'teaql-data-service', 'teaql-provider-sqlite'];
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
function pin(library, runtime, ownedRoot) {
  for (const dir of [library, runtime, ownedRoot]) assert.equal(fs.realpathSync(dir), dir, 'Symlink/relative roots are not admitted');
  assert(library.startsWith(ownedRoot + path.sep), 'Library must be inside the explicit owned consumer');
  const relative = path.relative(runtime, library);
  assert(relative.startsWith('..' + path.sep) || relative.split(path.sep).includes('target'), 'Do not adapt a source fixture');
  const rootCargo = fs.readFileSync(path.join(runtime, 'Cargo.toml'), 'utf8');
  const version = rootCargo.match(/\[workspace\.package\]\s*\nversion\s*=\s*"(\d+\.\d+\.\d+)"/)[1];
  const file = path.join(library, 'Cargo.toml'), before = fs.readFileSync(file, 'utf8');
  let after = before; const changes = [];
  for (const name of CRATES) {
    const pattern = new RegExp('^(' + name + '\\s*=\\s*)"(=?\\d+\\.\\d+\\.\\d+)"$', 'gm');
    const matches = [...before.matchAll(pattern)]; assert.equal(matches.length, 1, 'Expected one numeric dependency: ' + name);
    changes.push({name, from: matches[0][2], to: '=' + version});
    after = after.replace(pattern, (_, start) => start + '"=' + version + '"');
  }
  const implementation = [];
  function visit(dir) {for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name); assert(!entry.isSymbolicLink());
    if (entry.isDirectory()) visit(full); else if (full !== file) implementation.push([full, sha(fs.readFileSync(full))]);
  }}
  visit(library); fs.writeFileSync(file, after);
  for (const [full, digest] of implementation) assert.equal(sha(fs.readFileSync(full)), digest);
  return {version, changes, originalManifestSha256: sha(before), consumerManifestSha256: sha(after),
    implementationFilesUnchanged: true, implementationFiles: implementation.length, sourceFixtureEdited: false};
}
module.exports = {pin};
if (require.main === module) console.log(JSON.stringify(pin(...process.argv.slice(2))));
