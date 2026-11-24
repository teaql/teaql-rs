"use strict";
const fs = require('node:fs'), path = require('node:path'), cp = require('node:child_process');
const assert = require('node:assert/strict'), crypto = require('node:crypto');
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
function prepareOwnedSubreaper(scratch, stdio = 'pipe') {
  assert.equal(process.platform, 'linux');
  const directory = path.join(scratch, 'owned-supervisor');
  assert(!fs.existsSync(directory)); fs.mkdirSync(directory);
  const source = path.join(__dirname, 'owned-subreaper.c'), executable = path.join(directory, 'supervisor');
  const command = ['cc', '-std=c11', '-O2', '-Wall', '-Wextra', '-Werror', source, '-o', executable];
  const result = cp.spawnSync(command[0], command.slice(1), {stdio, timeout: 15000});
  assert.equal(result.status, 0, 'owned subreaper build failed: ' + (result.stderr || result.error || ''));
  return {directory, executable, sourceSha256: sha(fs.readFileSync(source)),
    executableSha256: sha(fs.readFileSync(executable)), compilerCommand: command, compilerExit: result.status};
}
module.exports = {prepareOwnedSubreaper};
