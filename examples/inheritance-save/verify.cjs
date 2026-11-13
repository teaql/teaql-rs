'use strict';
// Local-source example gate. No service, shared cache or other session is owned.
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const cp = require('node:child_process'), crypto = require('node:crypto'), assert = require('node:assert/strict');
const {createOwnedRunner, ownedBytes} = require('../fix-e-alignment/owned-command.cjs');
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const repo = path.resolve(__dirname, '../..');
function inventory(directory, prefix = '', result = {}) {
  for (const entry of fs.readdirSync(directory, {withFileTypes: true})) {
    const name = path.join(prefix, entry.name), file = path.join(directory, entry.name);
    assert(!entry.isSymbolicLink(), 'Do not follow symlinks: ' + file);
    if (entry.isDirectory()) inventory(file, name, result);
    else {assert(entry.isFile()); result[name] = sha(fs.readFileSync(file));}
  }
  return result;
}
async function main() {
  const args = process.argv.slice(2);
  assert(args.length === 2 && args[0] === '--output', 'usage: node verify.cjs --output /new/evidence');
  const output = args[1];
  assert(path.isAbsolute(output) && !fs.existsSync(output), 'Use a fresh absolute evidence directory');
  assert(fs.statfsSync(repo).bavail * fs.statfsSync(repo).bsize > 4 * 1024 ** 3);
  fs.mkdirSync(output, {recursive: true});
  const runner = createOwnedRunner();
  let scratch;
  const receipt = {schema:'teaql.runtime-inheritance-example/v1',passed:false,publication:false,commands:[],rounds:[]};
  try {
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'teaql-inheritance-example-'));
    const target = path.join(scratch, 'target');
    const env = {...process.env,CARGO_TARGET_DIR:target,CARGO_INCREMENTAL:'0',CARGO_BUILD_JOBS:'4',
      CARGO_PROFILE_DEV_DEBUG:'0',CARGO_PROFILE_TEST_DEBUG:'0',TEAQL_TELEMETRY_ENABLED:'false',
      INHERITANCE_MATCHING_SERVICE_CORE_DATABASE_URL:path.join(output,'inheritance.sqlite')};
    receipt.sourceCommit = cp.execFileSync('git',['rev-parse','HEAD'],{cwd:repo,encoding:'utf8'}).trim();
    const sourceNames = ['Cargo.toml','Cargo.lock','examples/fix-e-alignment/owned-command.cjs'];
    const crates = ['core','macros','data-service','sql','runtime','provider-sqlite','provider-postgres'];
    for (const name of crates) {
      sourceNames.push('teaql-'+name+'/Cargo.toml');
      for (const area of ['src','tests']) {
        const base = path.join(repo,'teaql-'+name,area);
        if(fs.existsSync(base)) for(const file of Object.keys(inventory(base))) sourceNames.push('teaql-'+name+'/'+area+'/'+file);
      }
    }
    for (const file of ['Cargo.toml','Cargo.lock','model.xml','generated-files.json','generation-origin.json','verify.cjs']) sourceNames.push('examples/inheritance-save/'+file);
    for (const area of ['src','tests','assist']) for(const file of Object.keys(inventory(path.join(__dirname,area)))) sourceNames.push('examples/inheritance-save/'+area+'/'+file);
    const sources = () => Object.fromEntries(sourceNames.map(name=>[name,sha(fs.readFileSync(path.join(repo,name)))]));
    receipt.sourceHashes = sources();
    const expected = JSON.parse(fs.readFileSync(path.join(__dirname,'generated-files.json')));
    const generated = () => ({...Object.fromEntries(Object.entries(inventory(path.join(__dirname,'lib'))).map(([name,hash])=>['lib/'+name,hash])),
      ...Object.fromEntries(Object.entries(inventory(path.join(__dirname,'teaql'))).map(([name,hash])=>['teaql/'+name,hash]))});
    assert.deepEqual(generated(),expected); receipt.generatedFileHashes = expected;
    const cargo = process.env.CARGO || 'cargo', manifest = path.join(__dirname,'Cargo.toml');
    async function run(command, log) {
      const fd = fs.openSync(path.join(output,log),'wx'), errorLog = log+'.stderr';
      const errorFd = fs.openSync(path.join(output,errorLog),'wx'); let result;
      try {result=await runner.run(command,{cwd:repo,env,timeout:900000,stdio:['ignore',fd,errorFd]});}
      finally {fs.closeSync(fd);fs.closeSync(errorFd);}
      const bytes = fs.readFileSync(path.join(output,log));
      const diagnostics = fs.readFileSync(path.join(output,errorLog));
      receipt.commands.push({command,log,errorLog,exitCode:result.status,signal:result.signal,logSha256:sha(bytes),errorLogSha256:sha(diagnostics)});
      assert.equal(result.status,0,diagnostics.toString().slice(-5000)+'\n'+bytes.toString().slice(-5000)); return bytes.toString();
    }
    const metadata = JSON.parse(await run([cargo,'metadata','--offline','--locked','--format-version','1','--manifest-path',manifest],'origins.json'));
    receipt.origins = {};
    for(const name of crates) {
      const pkg = metadata.packages.find(pkg=>pkg.name==='teaql-'+name);
      if(name==='provider-postgres' && !pkg) continue; // SQLite-only generated fixture.
      assert(pkg && pkg.source===null, 'Published package cannot stand in for local '+name);
      assert.equal(fs.realpathSync(pkg.manifest_path),path.join(repo,'teaql-'+name,'Cargo.toml'));
      receipt.origins[pkg.name]=pkg.manifest_path;
    }
    const library = metadata.packages.find(pkg=>pkg.name==='inheritance-matching-service-core');
    assert(library && library.source===null);assert.equal(fs.realpathSync(library.manifest_path),path.join(__dirname,'lib/Cargo.toml'));
    for(const round of [1,2]) {
      const log = await run([cargo,'test','--offline','--locked','--manifest-path',manifest,'--all-targets'],'tests-'+round+'.log');
      assert(log.includes('test generated_lifecycle_twice_with_one_root_version_and_no_manual_parent_seed ... ok'));
    }
    await run([cargo,'build','--offline','--locked','--manifest-path',manifest],'build.log');
    for(const round of [1,2]) {
      const log = await run([path.join(target,'debug/inheritance-save-example')],'round-'+round+'.log');
      const lines = log.split('\n').filter(line=>line.startsWith('NATURAL_INHERITANCE_LIFECYCLE='));assert.equal(lines.length,1);
      const proof = JSON.parse(lines[0].slice('NATURAL_INHERITANCE_LIFECYCLE='.length));
      for(const key of ['passed','generatedQAndE','auditedMutation','partialWriteRejected','staleWriteRejected','parentAndChildHidden','retainedDeletedRow']) assert.equal(proof[key],true,key);
      assert.equal(proof.manualParentSeed,false);assert.equal(proof.caseCount,8);assert.equal(proof.noopVersion,1);assert.deepEqual(proof.versions,[1,2,3,-4]);
      receipt.rounds.push(proof);
    }
    assert.notEqual(receipt.rounds[0].id,receipt.rounds[1].id);
    const sqlOracle = "import sqlite3,sys,pathlib,json; c=sqlite3.connect(pathlib.Path(sys.argv[1]).resolve().as_uri()+'?mode=ro',uri=True); r={'rows':c.execute('SELECT s.id,s.version,a.\"Legacy Campus Code\" FROM school_data s JOIN academy_data a ON a.id=s.id ORDER BY s.id').fetchall(),'columns':[r[1] for r in c.execute('PRAGMA table_info(academy_data)')],'rootCount':c.execute('SELECT count(*) FROM school_data').fetchone()[0],'platformCount':c.execute('SELECT count(*) FROM platform_data').fetchone()[0],'constantCount':c.execute('SELECT count(*) FROM school_type_data WHERE id IN (1001,1002)').fetchone()[0]};c.close();print(json.dumps(r))";
    receipt.oracle = JSON.parse(await run(['python3','-B','-c',sqlOracle,env.INHERITANCE_MATCHING_SERVICE_CORE_DATABASE_URL],'oracle.json'));
    assert.deepEqual(receipt.oracle.rows,receipt.rounds.map(p=>[p.id,-4,'SOUTH']).sort((a,b)=>a[0]-b[0]));
    assert.deepEqual(receipt.oracle.columns,['id','Legacy Campus Code']);assert.equal(receipt.oracle.rootCount,2);assert.equal(receipt.oracle.platformCount,1);assert.equal(receipt.oracle.constantCount,2);
    assert.deepEqual(generated(),expected);receipt.generatedLibraryUnchanged=true;
    assert.deepEqual(sources(),receipt.sourceHashes);receipt.sameDatabase=true;receipt.passed=true;
  } catch(error) {receipt.error=String(error);process.exitCode=1;}
  finally {
    if(runner.safeToClean && scratch) {receipt.disposableBytesRemoved=ownedBytes(scratch);fs.rmSync(scratch,{recursive:true});}
    else if(scratch) receipt.unsafeRetention={scratch,processGroup:runner.unsafeGroup};
    runner.dispose();receipt.scratchRemoved=!scratch || !fs.existsSync(scratch);
    fs.writeFileSync(path.join(output,'receipt.json'),JSON.stringify(receipt,null,2)+'\n');
    console.log(JSON.stringify({passed:receipt.passed,evidence:output,error:receipt.error,scratchRemoved:receipt.scratchRemoved,disposableBytesRemoved:receipt.disposableBytesRemoved}));
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
