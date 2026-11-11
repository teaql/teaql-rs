'use strict';
const fs = require('node:fs'), path = require('node:path'), os = require('node:os'), assert = require('node:assert/strict');
const {adapt} = require('./adapt-sqlite-probe.cjs');
require('../examples/tests/loaded-revert/verify-local.cjs').fingerprint();
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-probe-adapter-test-'));
try {
  const file = path.join(temp, 'probe.rs'); fs.writeFileSync(file, 'pool.trace(Some(observe)); assert_eq!(count, 2); pool.trace(None);');
  const receipt = adapt(file, temp), text = fs.readFileSync(file, 'utf8'); assert.equal(receipt.replacements, 2);
  assert(text.includes('assert_eq!(count, 2)')); assert(text.includes('expanded_sql()')); assert(text.includes('sql.starts_with("--")'));
  assert.throws(() => adapt(path.resolve(__filename), path.resolve(__dirname)));
  fs.writeFileSync(file, 'pool.trace(Some(|sql| unexpected(sql)));');
  assert.throws(() => adapt(file, temp));
  console.log('PASS bounded owned-copy trace adapter, assertions and source guards');
} finally {fs.rmSync(temp, {recursive: true});}
