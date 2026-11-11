'use strict';
// Application-probe compatibility in an explicitly owned disposable consumer.
// Never rewrite a frozen source fixture or generated domain-library code.
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict'), crypto = require('node:crypto');
const runtime = path.resolve(__dirname, '..');
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
function adapt(file, ownedRoot) {
  assert.equal(fs.realpathSync(file), file); assert.equal(fs.realpathSync(ownedRoot), ownedRoot);
  assert(file.startsWith(ownedRoot + path.sep)); assert(!file.startsWith(runtime + path.sep));
  const before = fs.readFileSync(file, 'utf8'); let changed = 0;
  const after = before.replace(/\.trace\(Some\((\w+)\)\)/g, (_, callback) => {
    changed++; return '.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(|event: rusqlite::trace::TraceEvent<\'_>| { if let rusqlite::trace::TraceEvent::Stmt(statement, sql) = event { if sql.starts_with("--") { ' + callback + '(sql); } else { ' + callback + '(&statement.expanded_sql().expect("expanded SQLite trace SQL")); } } })).unwrap()';
  }).replace(/\.trace\(None\)/g, () => {changed++; return '.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None::<fn(rusqlite::trace::TraceEvent<\'_>)>).unwrap()';});
  assert(changed > 0); assert.equal((after.match(/\.trace\(/g) || []).length, 0, 'Unknown legacy trace syntax');
  fs.writeFileSync(file, after);
  return {originalSha256: sha(before), consumerSha256: sha(after), replacements: changed, scope: 'SQLite statement observer API only; source fixture unchanged'};
}
module.exports = {adapt};
