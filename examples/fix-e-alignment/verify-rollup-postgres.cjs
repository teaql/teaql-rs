'use strict';
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const cp = require('node:child_process'), crypto = require('node:crypto'), assert = require('node:assert/strict');
const {createOwnedRunner, ownedBytes} = require('./owned-command.cjs');
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
async function main() {
  const output = process.argv[2], container = process.argv[3], port = process.argv[4];
  assert(output && path.isAbsolute(output) && !fs.existsSync(output), 'Use a fresh absolute evidence directory');
  assert(container && /^[a-zA-Z0-9_-]+$/.test(container));
  assert(port && /^\d{1,5}$/.test(port) && Number(port) > 0 && Number(port) < 65536);
  const repo = path.resolve(__dirname, '../..');
  assert(fs.statfsSync(repo).bavail * fs.statfsSync(repo).bsize > 4 * 1024 ** 3);
  fs.mkdirSync(output, {recursive: true});
  const runner = createOwnedRunner(), scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-rollup-postgres-'));
  const lock = path.join(os.tmpdir(), 'teaql-rollup-postgres-326.lock'); let ownsLock = false;
  const receipt = {schema:'teaql.rust-rollup-postgres/v1', passed:false, publication:false,
    commit:cp.execFileSync('git', ['rev-parse','HEAD'], {cwd:repo,encoding:'utf8'}).trim(), commands:[]};
  let secret = '', connectionUrl = '';
  const psql = sql => cp.execFileSync('docker', ['exec','-i','-u','postgres',container,'psql','-X','-v','ON_ERROR_STOP=1','-d','postgres','-At'],
    {input:sql,encoding:'utf8',stdio:['pipe','pipe','pipe']}).trim();
  try {
    const fd = fs.openSync(lock, 'wx'); ownsLock = true;
    try { fs.writeFileSync(fd, JSON.stringify({pid:process.pid, repo, output})); } finally { fs.closeSync(fd); }
    const database = 'teaql_rollup_326', marker = 'TeaQL #326 owned layered statistics fixture';
    const existing = psql("SELECT COALESCE(shobj_description(oid,'pg_database'),'') FROM pg_database WHERE datname='teaql_rollup_326';");
    const exists = psql("SELECT COUNT(*) FROM pg_database WHERE datname='teaql_rollup_326';") === '1';
    if (exists) assert.equal(existing, marker, 'Refuse to use a database without this fixture ownership marker');
    else {
      psql('CREATE DATABASE teaql_rollup_326;');
      psql("COMMENT ON DATABASE teaql_rollup_326 IS 'TeaQL #326 owned layered statistics fixture';");
    }
    receipt.database = {name:database, container, port:Number(port), created:!exists, retained:true, ownership:marker,
      server:psql('SELECT version();')};
    const fixtureSql = sql => cp.execFileSync('docker', ['exec','-i','-u','postgres',container,'psql','-X','-v','ON_ERROR_STOP=1','-d',database,'-At'],
      {input:sql,encoding:'utf8',stdio:['pipe','pipe','pipe']}).trim();
    // Provision only this marked fixture database, never an application query.
    fixtureSql('CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;');
    receipt.database.extensions = {fuzzystrmatch:fixtureSql("SELECT extversion FROM pg_extension WHERE extname='fuzzystrmatch';")};
    assert(receipt.database.extensions.fuzzystrmatch);
    // Read only the password needed for this local fixture. Never put it in a
    // command argument, source snapshot, receipt, log, or printed environment.
    secret = cp.execFileSync('docker', ['exec',container,'sh','-c','printf %s "$POSTGRES_PASSWORD"'], {encoding:'utf8',stdio:['ignore','pipe','pipe']});
    assert(secret, 'Existing local PostgreSQL container needs POSTGRES_PASSWORD');
    connectionUrl = 'postgresql://postgres:' + encodeURIComponent(secret) + '@127.0.0.1:' + port + '/' + database;
    receipt.sourceHashes = {};
    for (const file of ['Cargo.toml','Cargo.lock','teaql-core/src/query.rs','teaql-core/src/expr.rs','teaql-core/src/value.rs','teaql-core/src/aggregation_plan.rs',
      'teaql-runtime/src/inmemory_engine.rs','teaql-runtime/src/memory.rs','teaql-runtime/src/data_service/helpers.rs',
      'teaql-sql/src/dialect.rs','teaql-sql/src/having.rs','teaql-sql/src/statistical.rs','teaql-sql/src/executor.rs','teaql-sql/src/bindings.rs',
      'teaql-provider-sqlite/Cargo.toml','teaql-provider-sqlite/src/lib.rs','teaql-provider-sqlite/src/decimal_functions.rs',
      'teaql-provider-sqlite/src/statistical_tests.rs',
      'teaql-runtime/src/data_service/resolved.rs','teaql-runtime/src/data_service/statistics.rs',
      'teaql-sql/src/integer_operand.rs','teaql-sql/src/lib.rs',
      'teaql-provider-postgres/Cargo.toml','teaql-provider-postgres/src/lib.rs','teaql-provider-postgres/src/statistical_binding_tests.rs',
      'teaql-provider-postgres/tests/layered_statistics.rs',
      'teaql-provider-postgres/tests/layered_statistics/gbk_order.rs',
      'teaql-provider-postgres/tests/layered_statistics/integer_operand.rs',
      'examples/fix-e-alignment/verify-rollup-postgres.cjs']) {
      const bytes = fs.readFileSync(path.join(repo,file)); receipt.sourceHashes[file] = hash(bytes);
      const target = path.join(output,'sources',file); fs.mkdirSync(path.dirname(target),{recursive:true}); fs.writeFileSync(target,bytes);
    }
    const env = {...process.env, CARGO_TARGET_DIR:path.join(scratch,'target'), CARGO_INCREMENTAL:'0', CARGO_BUILD_JOBS:'4',
      CARGO_PROFILE_TEST_DEBUG:'0', CARGO_PROFILE_DEV_DEBUG:'0', TEAQL_TEST_POSTGRES_URL:connectionUrl, TEAQL_REQUIRE_LIVE_DB:'true',
      TEAQL_TELEMETRY_ENABLED:'false'};
    async function run(command, name, runEnv = env) {
      const fd = fs.openSync(path.join(output,name),'wx'); let result;
      try { result = await runner.run(command,{cwd:repo,env:runEnv,timeout:900000,stdio:['ignore',fd,fd]}); }
      finally { fs.closeSync(fd); }
      const bytes = fs.readFileSync(path.join(output,name));
      const testsPassed = [...bytes.toString().matchAll(/test result: ok\. (\d+) passed;/g)].reduce((n,m)=>n+Number(m[1]),0);
      receipt.commands.push({command,result,log:name,logSha256:hash(bytes),testsPassed});
      assert.equal(result.status,0,bytes.toString().slice(-5000)); return bytes.toString();
    }
    await run([process.env.CARGO || 'cargo','test','--offline','--locked','-p','teaql-provider-postgres','--lib','statistical_binding_tests::'],
      'statistical-bindings.log');
    assert.equal(receipt.commands.at(-1).testsPassed,8);
    const unitLog = await run([process.env.CARGO || 'cargo','test','--offline','--locked','-p','teaql-provider-postgres','--lib','--','--skip','streaming_tests::'],
      'postgres-unit-regression.log');
    for (const name of ['postgres_relation_index_name_is_stable_and_within_identifier_limit',
      'postgres_dialect_compiles_schema_and_large_in_array_binds', 'schema_compatibility_covers_boolean_and_temporal_storage_contracts',
      'topn_012_postgres_schema_adds_full_foreign_key_id_desc_index', 'postgres_dialect_compiles_mutations_with_numbered_placeholders']) {
      assert(unitLog.includes('test tests::' + name + ' ... ok'), 'Missing provider regression: '+name);
    }
    assert(receipt.commands.at(-1).testsPassed >= 9);
    receipt.rounds = [];
    for (const round of [1,2]) {
      const command = [process.env.CARGO || 'cargo','test','--offline','--locked','-p','teaql-provider-postgres','--test','layered_statistics',
        '--','--test-threads=1','--nocapture'];
      const log = await run(command,'postgres-round-'+round+'.log');
      const observations = log.split('\n').filter(line=>line.startsWith('ROLLUP_POSTGRES_CASE='))
        .map(line=>JSON.parse(line.slice('ROLLUP_POSTGRES_CASE='.length)));
      assert.deepEqual(observations.map(o=>o.case).sort(), ['extended-owner-grain','parent-cell-page-and-having','ordinary-having',
        'empty-scalar','all-null-source-and-singleton-parent','distinct-overlap','distinct-empty','distinct-null','ordinary-distinct-having',
        'computed-parent-group','computed-multihop-group','computed-ordinary-group',
        'identity-ordinary-group','identity-parent-group','identity-multihop-group',
        'nullable-raw-foreign-keys','nullable-parent-membership','nullable-grandparent-membership','nullable-empty-scalar',
        'independent-grouped-membership','independent-grouped-empty','independent-grouped-alias-page',
        'independent-grouped-null','independent-grouped-policy',
        'multilayer-membership-positive','multilayer-membership-negative',
        'multilayer-membership-page-and-parameters','multilayer-membership-ordinary-policy',
        ...['positive','negative','neighboring'].flatMap(kind=>['ordinary','parent','nested'].map(layer=>'numeric-'+layer+'-'+kind)),
        'gbk-ordinary','gbk-parent','gbk-nested','bitwise-expression-fields','bitwise-expression-parameters'].sort());
      assert.equal(receipt.commands.at(-1).testsPassed,12);
      receipt.rounds.push({round,observations});
    }
    const oracleSql = "SELECT json_build_object('schools',COUNT(*),'nonnull',COUNT(\"Legacy Capacity\"),'capacity',SUM(\"Legacy Capacity\"),'types',(SELECT COUNT(*) FROM legacy_kind),'platforms',(SELECT COUNT(*) FROM legacy_platform)) FROM legacy_school;";
    const oracle = cp.execFileSync('docker',['exec','-i','-u','postgres',container,'psql','-X','-v','ON_ERROR_STOP=1','-d',database,'-At'],
      {input:oracleSql,encoding:'utf8',stdio:['pipe','pipe','pipe']}).trim();
    receipt.independentOracle = JSON.parse(oracle);
    assert.deepEqual(receipt.independentOracle,{schools:7,nonnull:6,capacity:4100,types:2,platforms:1});
    fs.writeFileSync(path.join(output,'oracle.sql'),oracleSql+'\n');
    fs.writeFileSync(path.join(output,'oracle.json'),oracle+'\n');
    const numericSql="SELECT jsonb_build_object('schools',(SELECT COUNT(*) FROM numeric_school_326),'ranks',jsonb_agg(ordinal::text ORDER BY id)) FROM numeric_kind_326;";
    receipt.numericOracle=JSON.parse(fixtureSql(numericSql));
    assert.deepEqual(receipt.numericOracle,{schools:5,ranks:['9007199254740992.000000001','9007199254740992.000000002']});
    fs.writeFileSync(path.join(output,'numeric-oracle.sql'),numericSql+'\n');
    fs.writeFileSync(path.join(output,'numeric-oracle.json'),JSON.stringify(receipt.numericOracle,null,2)+'\n');
    const nativeEnv = {...env}; delete nativeEnv.TEAQL_TEST_POSTGRES_URL; delete nativeEnv.TEAQL_REQUIRE_LIVE_DB;
    await run([process.env.CARGO || 'cargo','test','--offline','--locked','-p','teaql-core','-p','teaql-sql','-p','teaql-runtime','-p','teaql-provider-sqlite','--all-targets'],
      'native-regression.log', nativeEnv);
    assert(receipt.commands.at(-1).testsPassed >= 1135);
    for(const [name, expected] of Object.entries(receipt.sourceHashes)) assert.equal(hash(fs.readFileSync(path.join(repo,name))),expected,'Source changed: '+name);
    receipt.sameDatabase = true; receipt.passed = true;
  } catch(error) {
    const safe = value => { let text = String(value); for(const token of [connectionUrl,secret].filter(Boolean)) text = text.split(token).join('[REDACTED]'); return text; };
    receipt.error = safe(error.message); process.exitCode = 1;
  } finally {
    if(runner.safeToClean) {
      receipt.cleanup = {path:scratch,bytes:ownedBytes(scratch)}; fs.rmSync(scratch,{recursive:true});
      if(ownsLock) fs.unlinkSync(lock);
    } else receipt.unsafeRetention = {scratch,processGroup:runner.unsafeGroup,lock:ownsLock ? lock : undefined};
    runner.dispose();
    fs.writeFileSync(path.join(output,'receipt.json'),JSON.stringify(receipt,null,2)+'\n');
    process.stdout.write(JSON.stringify({passed:receipt.passed,evidence:output,error:receipt.error?.slice(-900),
      commands:receipt.commands.map(x=>({exitCode:x.result.status,testsPassed:x.testsPassed})),disposableBytesRemoved:receipt.cleanup?.bytes,
      retainedDatabase:receipt.database?.name})+'\n');
  }
}
main().catch(error=>{console.error(error.message);process.exitCode=1;});
