'use strict';
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const cp = require('node:child_process'), crypto = require('node:crypto'), assert = require('node:assert/strict');
const {createOwnedRunner, ownedBytes} = require('./owned-command.cjs');
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
async function main() {
    const repo = path.resolve(__dirname, '../..'), output = process.argv[2];
    assert(output && path.isAbsolute(output) && !fs.existsSync(output), 'Use a fresh absolute evidence directory');
    const space = fs.statfsSync(repo); assert(space.bavail * space.bsize > 4 * 1024 ** 3);
    fs.mkdirSync(output, {recursive: true});
    const runner = createOwnedRunner(); let scratch;
    const receipt = {schema: 'teaql.binary-results/v1', passed: false, publication: false,
        commit: cp.execFileSync('git', ['rev-parse', 'HEAD'], {cwd: repo, encoding: 'utf8'}).trim(), commands: [], sourceHashes: {}};
    try {
        scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-binary-results-'));
        const sources = cp.execFileSync('git', ['ls-files', '--cached', '--others', '--exclude-standard', '-z'], {cwd: repo, encoding: 'utf8'})
            .split('\0').filter(file => file === 'Cargo.toml' || file === 'Cargo.lock'
                || (/^teaql-[^/]+\/(src|tests)\//.test(file) && file.endsWith('.rs'))
                || (/^teaql-[^/]+\/Cargo.toml$/.test(file))
                || file === 'examples/fix-e-alignment/verify-binary-results.cjs'
                || file === 'examples/fix-e-alignment/owned-command.cjs');
        for (const file of new Set(sources)) {
            const bytes = fs.readFileSync(path.join(repo, file)); receipt.sourceHashes[file] = sha(bytes);
            const target = path.join(output, 'sources', file); fs.mkdirSync(path.dirname(target), {recursive: true}); fs.writeFileSync(target, bytes);
        }
        const env = {...process.env, CARGO_TARGET_DIR: path.join(scratch, 'target'), CARGO_INCREMENTAL: '0',
            CARGO_BUILD_JOBS: '4', CARGO_PROFILE_DEV_DEBUG: '0', CARGO_PROFILE_TEST_DEBUG: '0', TEAQL_TELEMETRY_ENABLED: 'false'};
        // This gate is intentionally offline and owns no live external database.
        delete env.TEAQL_TEST_POSTGRES_URL; delete env.TEAQL_REQUIRE_LIVE_DB;
        async function run(args, log, minimum) {
            const command = [process.env.CARGO || 'cargo', ...args], fd = fs.openSync(path.join(output, log), 'wx'); let result;
            try { result = await runner.run(command, {cwd: repo, env, timeout: 900000, stdio: ['ignore', fd, fd]}); }
            finally { fs.closeSync(fd); }
            const bytes = fs.readFileSync(path.join(output, log));
            const testsPassed = [...bytes.toString().matchAll(/test result: ok\. (\d+) passed;/g)].reduce((n, m) => n + Number(m[1]), 0);
            receipt.commands.push({command, result, log, logSha256: sha(bytes), testsPassed});
            assert.equal(result.status, 0, bytes.toString().slice(-6500));
            if (minimum !== undefined) assert(testsPassed >= minimum, `Expected >= ${minimum} tests, got ${testsPassed}`);
        }
        await run(['test', '--offline', '--locked', '-p', 'teaql-core', '--test', 'binary_results'], 'binary-core.log', 4);
        await run(['test', '--offline', '--locked', '-p', 'teaql-provider-sqlite', 'binary_results'], 'binary-sqlite.log', 3);
        await run(['check', '--offline', '--locked', '--workspace', '--all-targets'], 'workspace-check.log');
        await run(['test', '--offline', '--locked', '-p', 'teaql-core', '-p', 'teaql-sql', '-p', 'teaql-runtime', '-p', 'teaql-provider-sqlite', '--all-targets'], 'native-regression.log', 1205);
        for (const [file, expected] of Object.entries(receipt.sourceHashes)) assert.equal(sha(fs.readFileSync(path.join(repo, file))), expected, 'Source changed during gate: ' + file);
        receipt.passed = true;
    } catch (error) { receipt.error = error.message; process.exitCode = 1; }
    finally {
        if (scratch && runner.safeToClean) { receipt.cleanup = {path: scratch, bytes: ownedBytes(scratch)}; fs.rmSync(scratch, {recursive: true}); }
        else if (scratch) receipt.unsafeRetention = {scratch, processGroup: runner.unsafeGroup};
        runner.dispose(); fs.writeFileSync(path.join(output, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
        console.log(JSON.stringify({passed: receipt.passed, output, error: receipt.error?.slice(-1200), commands: receipt.commands.map(x => ({exitCode: x.result.status, testsPassed: x.testsPassed})), removedBytes: receipt.cleanup?.bytes}));
    }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
