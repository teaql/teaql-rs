"use strict";
// POSIX compiler/test runner. The detached group belongs only to this command;
// no service, shared cache or other session is a termination target.
const cp = require('node:child_process');
const fs = require('node:fs'), path = require('node:path');
let runnerSequence = 0;
const delay = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
function alive(pid) {
  try { process.kill(-pid, 0); return true; }
  catch (error) { if (error.code === 'ESRCH') return false; throw error; }
}
function signal(pid, name) {
  if (!pid) return;
  try { process.kill(-pid, name); }
  catch (error) { if (error.code !== 'ESRCH') throw error; }
}
function createOwnedRunner({supervisor} = {}) {
  let active, interrupted, unsafeGroup, killer;
  let sequence = 0;
  const runnerId = process.pid + '-' + (++runnerSequence);
  const terminateActive = () => {
    if (!active?.pid) return;
    signal(active.pid, 'SIGTERM');
    killer ||= setTimeout(() => { if (active?.pid) signal(active.pid, 'SIGKILL'); }, supervisor ? 6000 : 500);
  };
  const cancel = name => {
    interrupted ||= name;
    terminateActive();
  };
  const onInt = () => cancel('SIGINT'), onTerm = () => cancel('SIGTERM');
  process.on('SIGINT', onInt); process.on('SIGTERM', onTerm);
  return {
    get interrupted() { return interrupted; },
    get unsafeGroup() { return unsafeGroup; },
    get safeToClean() { return !active && !unsafeGroup; },
    cancel,
    async run(command, options) {
      if (active) throw new Error('owned commands must be awaited, not overlapped');
      if (unsafeGroup) return {status: 125, signal: null, error: 'cannot start another command after incomplete descendant reaping'};
      if (interrupted) return {status: interrupted === 'SIGINT' ? 130 : 143, signal: interrupted};
      let timedOut = false, timer;
      const {timeout, ...spawnOptions} = options;
      const marker = supervisor ? path.join(supervisor.directory, 'command-' + runnerId + '-' + (++sequence) + '.json') : null;
      const actual = supervisor ? [supervisor.executable, marker, '--', ...command] : command;
      const child = cp.spawn(actual[0], actual.slice(1), {...spawnOptions, detached: true});
      active = child;
      try {
        const result = await new Promise(resolve => {
          child.once('error', error => resolve({status: null, signal: null, error: error.message}));
          child.once('close', (status, childSignal) => resolve({status, signal: childSignal}));
          timer = setTimeout(() => { timedOut = true; terminateActive(); }, timeout);
        });
        clearTimeout(timer);
        // Reap the direct child and stop any compiler descendants which outlive
        // it. Keep signal handlers installed until the complete owned group ends.
        if (child.pid && alive(child.pid)) {
          signal(child.pid, 'SIGTERM');
          for (let attempt = 0; attempt < 100 && alive(child.pid); attempt++) {
            if (attempt === 10) signal(child.pid, 'SIGKILL');
            await delay(25);
          }
          if (alive(child.pid)) { unsafeGroup = child.pid; throw new Error('owned process group remains live: ' + child.pid); }
        }
        if (supervisor) {
          let proof;
          try { proof = JSON.parse(fs.readFileSync(marker)); }
          catch { unsafeGroup = child.pid; return {...result, error: 'owned descendant-reaping proof missing'}; }
          const expectedSignal = proof.primarySignal ? require('node:os').constants.signals[result.signal] : 0;
          const exitMatches = proof.primarySignal ? expectedSignal === proof.primarySignal : result.status === proof.primaryExit;
          if (proof.schema !== 'teaql.owned-subreaper/v1' || proof.safe !== true || proof.primaryDone !== true || !exitMatches) {
            unsafeGroup = child.pid;
            return {...result, error: 'owned descendants were not fully reaped', descendantProof: proof};
          }
          result.descendantProof = proof;
        }
        if (timedOut) return {...result, status: 124};
        if (interrupted) return {...result, status: interrupted === 'SIGINT' ? 130 : 143};
        return result;
      } finally { clearTimeout(timer); clearTimeout(killer); killer = undefined; active = undefined; }
    },
    dispose() {
      if (active) throw new Error('cannot dispose a live owned command');
      process.off('SIGINT', onInt); process.off('SIGTERM', onTerm);
    },
  };
}
function ownedBytes(directory) {
  const seen = new Set();
  function visit(file) {
    const stat = fs.lstatSync(file), key = stat.dev + ':' + stat.ino;
    if (seen.has(key)) return 0;
    seen.add(key);
    return stat.size + (stat.isDirectory() ? fs.readdirSync(file).reduce((sum, name) => sum + visit(path.join(file, name)), 0) : 0);
  }
  return visit(directory);
}
module.exports = {createOwnedRunner, alive, ownedBytes};
