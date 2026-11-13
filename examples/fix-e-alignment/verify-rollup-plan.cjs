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
        for (const name of ['Cargo.toml', 'Cargo.lock', 'teaql-core/src/aggregation_plan.rs', 'teaql-core/src/lib.rs',
            'teaql-core/src/query.rs', 'teaql-core/src/expr.rs',
            'teaql-core/src/request.rs', 'teaql-core/tests/rollup_navigation.rs',
            'teaql-runtime/src/inmemory_engine.rs', 'teaql-runtime/src/memory.rs',
            'teaql-runtime/src/data_service/helpers.rs',
            'teaql-sql/src/executor.rs', 'teaql-sql/src/execution_tests.rs',
            'teaql-sql/src/dialect.rs', 'teaql-sql/src/having.rs', 'teaql-sql/src/bindings.rs',
            'teaql-runtime/src/data_service/mod.rs', 'teaql-runtime/src/data_service/resolved.rs',
            'teaql-runtime/src/data_service/statistics.rs',
            'teaql-sql/src/statistical.rs', 'teaql-sql/src/lib.rs', 'teaql-provider-sqlite/src/statistical_tests.rs',
            'teaql-provider-sqlite/src/lib.rs', 'teaql-provider-sqlite/src/statistical_functions.rs',
            'teaql-provider-sqlite/src/decimal_functions.rs', 'teaql-provider-sqlite/Cargo.toml',
            'examples/fix-e-alignment/verify-rollup-plan.cjs']) {
            const bytes = fs.readFileSync(path.join(repo, name)); receipt.sourceHashes[name] = hash(bytes);
            const target = path.join(output, 'sources', name); fs.mkdirSync(path.dirname(target), {recursive: true});
            fs.writeFileSync(target, bytes);
        }
        const env = {...process.env, CARGO_TARGET_DIR: path.join(scratch, 'target'), CARGO_INCREMENTAL: '0',
            CARGO_BUILD_JOBS: '4', CARGO_PROFILE_TEST_DEBUG: '0', CARGO_PROFILE_DEV_DEBUG: '0'};
        receipt.commands = [];
        for (const [crate, filter, minimum] of [['teaql-core', 'aggregation_plan::tests::', 19],
            ['teaql-provider-sqlite', 'statistical_tests::', 60],
            ['teaql-provider-sqlite', 'decimal_functions::tests::', 4],
            ['teaql-provider-sqlite', 'statistical_functions::tests::', 7]]) {
            const command = [process.env.CARGO || 'cargo', 'test', '--offline', '--locked', '-p', crate, filter];
            const suffix = filter.startsWith('statistical_functions::') ? '-functions'
                : filter.startsWith('decimal_functions::') ? '-decimal-functions' : '';
            const logPath = path.join(output, crate + suffix + '.log'), fd = fs.openSync(logPath, 'wx'); let result;
            try { result = await runner.run(command, {cwd: repo, env, timeout: 900000, stdio: ['ignore', fd, fd]}); }
            finally { fs.closeSync(fd); }
            const log = fs.readFileSync(logPath);
            const testsPassed = (log.toString().match(/test (?:aggregation_plan::tests::|statistical_tests::|statistical_functions::tests::|decimal_functions::tests::)[^\n]+ \.\.\. ok/g) || []).length;
            receipt.commands.push({command, result, testsPassed, logSha256: hash(log)});
            assert(result.status === 0 && testsPassed >= minimum, log.toString().slice(-6000));
        }
        const command = [process.env.CARGO || 'cargo', 'test', '--offline', '--locked',
            '-p', 'teaql-core', '-p', 'teaql-sql', '-p', 'teaql-runtime', '-p', 'teaql-provider-sqlite', '--all-targets'];
        const fd = fs.openSync(path.join(output, 'native-regression.log'), 'wx'); let result;
        try { result = await runner.run(command, {cwd: repo, env, timeout: 900000, stdio: ['ignore', fd, fd]}); }
        finally { fs.closeSync(fd); }
        const bytes = fs.readFileSync(path.join(output, 'native-regression.log')), log = bytes.toString();
        const testsPassed = [...log.matchAll(/test result: ok\. (\d+) passed;/g)]
            .reduce((count, match) => count + Number(match[1]), 0);
        receipt.commands.push({command, result, testsPassed, logSha256: hash(bytes)});
        assert(result.status === 0 && testsPassed >= 1135, log.slice(-6000));
        for (const [name, expected] of Object.entries(receipt.sourceHashes)) {
            assert.equal(hash(fs.readFileSync(path.join(repo, name))), expected, 'Source changed during verification: ' + name);
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
        process.stdout.write(JSON.stringify({passed: receipt.passed, evidence: output, error: receipt.error?.slice(-700),
            commands: receipt.commands?.map(command => ({exitCode: command.result.status, testsPassed: command.testsPassed})),
            disposableBytesRemoved: receipt.cleanup?.bytes}) + '\n');
    }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
