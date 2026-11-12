'use strict';
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const cp = require('node:child_process'), crypto = require('node:crypto'), assert = require('node:assert/strict');
const {createOwnedRunner, ownedBytes} = require('./owned-command.cjs');
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
async function main() {
    const repo = path.resolve(__dirname, '../..'), output = process.argv[2];
    assert(output && path.isAbsolute(output) && !fs.existsSync(output), 'Supply a fresh absolute evidence path');
    assert(fs.statfsSync(repo).bavail * fs.statfsSync(repo).bsize > 4 * 1024 ** 3);
    fs.mkdirSync(output, {recursive: true});
    const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-rollup-plan-'));
    const runner = createOwnedRunner();
    const receipt = {schema: 'teaql.rust-rollup-plan/v1', passed: false, publication: false,
        commit: cp.execFileSync('git', ['rev-parse', 'HEAD'], {cwd: repo, encoding: 'utf8'}).trim()};
    try {
        receipt.sourceHashes = {};
        for (const name of ['teaql-core/src/aggregation_plan.rs', 'teaql-core/src/lib.rs',
            'teaql-sql/src/statistical.rs', 'teaql-sql/src/lib.rs', 'teaql-provider-sqlite/src/statistical_tests.rs',
            'teaql-provider-sqlite/src/lib.rs', 'examples/fix-e-alignment/verify-rollup-plan.cjs']) {
            const bytes = fs.readFileSync(path.join(repo, name)); receipt.sourceHashes[name] = hash(bytes);
            const target = path.join(output, 'sources', name); fs.mkdirSync(path.dirname(target), {recursive: true});
            fs.writeFileSync(target, bytes);
        }
        const env = {...process.env, CARGO_TARGET_DIR: path.join(scratch, 'target'), CARGO_INCREMENTAL: '0',
            CARGO_BUILD_JOBS: '4', CARGO_PROFILE_TEST_DEBUG: '0', CARGO_PROFILE_DEV_DEBUG: '0'};
        receipt.commands = [];
        for (const [crate, filter] of [['teaql-core', 'aggregation_plan::tests::'],
            ['teaql-provider-sqlite', 'statistical_tests::layered_rollup_']]) {
            const command = [process.env.CARGO || 'cargo', 'test', '--offline', '--locked', '-p', crate, filter];
            const logPath = path.join(output, crate + '.log'), fd = fs.openSync(logPath, 'wx'); let result;
            try { result = await runner.run(command, {cwd: repo, env, timeout: 900000, stdio: ['ignore', fd, fd]}); }
            finally { fs.closeSync(fd); }
            const log = fs.readFileSync(logPath);
            const testsPassed = (log.toString().match(/test (?:aggregation_plan::tests::|statistical_tests::layered_rollup_)[^\n]+ \.\.\. ok/g) || []).length;
            receipt.commands.push({command, result, testsPassed, logSha256: hash(log)});
            assert(result.status === 0 && testsPassed >= 6, log.toString().slice(-6000));
        }
        receipt.passed = true;
    } catch (error) { receipt.error = error.message; process.exitCode = 1; }
    finally {
        if (runner.safeToClean) {
            receipt.cleanup = {path: scratch, bytes: ownedBytes(scratch)};
            fs.rmSync(scratch, {recursive: true});
        } else receipt.unsafeRetention = {scratch, processGroup: runner.unsafeGroup};
        runner.dispose();
        fs.writeFileSync(path.join(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
        process.stdout.write(JSON.stringify(receipt, null, 2) + '\n');
    }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
