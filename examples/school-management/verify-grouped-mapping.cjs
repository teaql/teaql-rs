'use strict';
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),cp=require('node:child_process'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const repo=path.resolve(__dirname,'../..'),sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const {createOwnedRunner,ownedBytes}=require('../fix-e-alignment/owned-command.cjs');
const {verifySources,runAdditionalProbes}=require('./native-verification.cjs');
function files(root){const result={};function visit(dir,prefix=''){for(const entry of fs.readdirSync(dir,{withFileTypes:true})){
  assert(!entry.isSymbolicLink());const name=path.join(prefix,entry.name);if(entry.isDirectory())visit(path.join(dir,entry.name),name);else{assert(entry.isFile());result[name]=sha(fs.readFileSync(path.join(dir,entry.name)));}
}}visit(root);return result;}
async function main(){
  const args=process.argv.slice(2);let output,native=false;
  for(let i=0;i<args.length;i++)if(args[i]==='--output'&&args[i+1])output=args[++i];else if(args[i]==='--native')native=true;else throw new Error('usage: node verify-grouped-mapping.cjs [--native] --output /new/evidence');
  assert(output&&path.isAbsolute(output)&&!fs.existsSync(output));fs.mkdirSync(output,{recursive:true});
  const scratch=fs.mkdtempSync(path.join(os.tmpdir(),'teaql-grouped-mapping-')),runner=createOwnedRunner();
  const receipt={schema:'teaql.rust-grouped-mapping-example/v1',passed:false,publication:false,commands:[],rounds:[],scratch};
  try{
    assert(fs.statfsSync(scratch).bavail*fs.statfsSync(scratch).bsize>4*1024**3);
    receipt.sourceCommit=cp.execFileSync('git',['rev-parse','HEAD'],{cwd:repo,encoding:'utf8'}).trim();
    const sources=['teaql-core/src/meta.rs','teaql-sql/src/dialect.rs','teaql-sql/src/lib.rs',
      'teaql-runtime/src/data_service/resolved.rs','examples/school-management/verify-grouped-mapping.cjs',
      'examples/school-management/src/bin/grouped_mapping_probe.rs','examples/school-management/tests/grouped_field_mapping.rs',
      'examples/school-management/native-verification.cjs','examples/school-management/src/bin/rollup_statistics_probe.rs',
      'examples/school-management/src/bin/numeric_rollup_probe.rs',
      'examples/school-management/src/bin/gbk_rollup_cases/mod.rs',
      'examples/school-management/src/bin/bitwise_rollup_cases/mod.rs',
      'examples/school-management/src/bin/reverse_rollup_cases/mod.rs',
      'examples/school-management/src/bin/expression_measure_cases/mod.rs',
      'examples/school-management/models/school-model.xml','examples/school-management/Cargo.toml','examples/school-management/Cargo.lock',
      'Cargo.toml','Cargo.lock','teaql-core/src/aggregation_plan.rs','teaql-core/src/aggregation_plan/measure_expression.rs','teaql-sql/src/statistical.rs',
      'teaql-core/src/query.rs','teaql-core/src/request.rs','teaql-core/tests/rollup_navigation.rs'];
    sources.push('teaql-sql/src/integer_operand.rs','teaql-provider-sqlite/src/lib.rs');
    const sourceHashes=()=>Object.fromEntries(sources.map(name=>[name,sha(fs.readFileSync(path.join(repo,name)))]));
    receipt.sourceHashes=sourceHashes();
    receipt.generatedFiles=files(path.join(__dirname,'lib'));
    receipt.applicationSha256=sha(fs.readFileSync(path.join(__dirname,'src/bin/grouped_mapping_probe.rs')));
    receipt.legacyTestSha256=sha(fs.readFileSync(path.join(__dirname,'tests/grouped_field_mapping.rs')));
    const cargo=process.env.CARGO||'cargo',target=path.join(scratch,'target'),env={...process.env,
      CARGO_TARGET_DIR:target,CARGO_INCREMENTAL:'0',CARGO_BUILD_JOBS:'4',CARGO_PROFILE_DEV_DEBUG:'0',CARGO_PROFILE_TEST_DEBUG:'0',TEAQL_TELEMETRY_ENABLED:'false',
      TEAQL_GROUPED_MAPPING_DATABASE:path.join(output,'fixture.sqlite')};
    async function run(command,log,cwd=repo){const fd=fs.openSync(path.join(output,log),'wx');let result;
      try{result=await runner.run(command,{cwd,env,timeout:900000,stdio:['ignore',fd,fd]});}finally{fs.closeSync(fd);}
      const bytes=fs.readFileSync(path.join(output,log));receipt.commands.push({command,log,exitCode:result.status,signal:result.signal,logSha256:sha(bytes)});
      assert.equal(result.status,0,bytes.toString().slice(-6000));return bytes.toString();
    }
    receipt.origins=await verifySources(run,cargo,repo,__dirname);
    await run([cargo,'test','--offline','--locked','--manifest-path',path.join(__dirname,'Cargo.toml'),'--test','grouped_field_mapping','--','--nocapture'],'legacy-tests.log');
    await run([cargo,'build','--offline','--locked','--manifest-path',path.join(__dirname,'Cargo.toml'),'--bin','grouped_mapping_probe'],'build.log');
    const binary=path.join(target,'debug/grouped_mapping_probe');assert(fs.statSync(binary).isFile());
    for(const round of [1,2]){env.TEAQL_GROUPED_MAPPING_ROUND=String(round);
      const log=await run([binary],'round-'+round+'.log',__dirname);
      const rows=log.split('\n').filter(line=>line.startsWith('RUST_GROUPED_MAPPING=')).map(line=>JSON.parse(line.slice('RUST_GROUPED_MAPPING='.length)));
      assert.equal(rows.length,1);assert(rows[0].passed&&rows[0].originalGeneratedLibrary&&rows[0].round===round);assert.equal(rows[0].observations.length,5);receipt.rounds.push(rows[0]);
    }
    receipt.additional=await runAdditionalProbes(run,cargo,repo,__dirname,target,output,env);
    if(native)await run([cargo,'test','--offline','--locked','-p','teaql-core','-p','teaql-sql','-p','teaql-runtime','-p','teaql-provider-sqlite','--all-targets'],'native-tests.log');
    receipt.completeNativeTargets=native;
    const oracle='import sqlite3,json,sys,pathlib; c=sqlite3.connect(pathlib.Path(sys.argv[1]).resolve().as_uri()+"?mode=ro&immutable=1",uri=True); rows=c.execute("SELECT school_type,count(*),sum(student_capacity) FROM school_data GROUP BY school_type").fetchall(); assert rows==[(1001,4,3200),(1002,1,100)],rows; names=[x[1] for x in c.execute("PRAGMA table_info(school_data)")]; assert "school_type" in names and "school_type_id" not in names; assert c.execute("pragma quick_check").fetchone()==("ok",); c.close(); print("SQLITE_GROUPED_MAPPING_ORACLE="+json.dumps({"rows":rows,"physicalColumns":names}))';
    await run(['python3','-B','-c',oracle,env.TEAQL_GROUPED_MAPPING_DATABASE],'oracle.log');
    const rankOracle='import sqlite3,json,sys,pathlib,decimal; c=sqlite3.connect(pathlib.Path(sys.argv[1]).resolve().as_uri()+"?mode=ro&immutable=1",uri=True); rows=[(r[0],str(decimal.Decimal(str(r[1])).normalize())) for r in c.execute("select id,display_order from school_type_data order by id")]; assert rows==[(1001,"1"),(1002,"2")],rows; c.close(); print("RESTORED_RANK_ORACLE="+json.dumps(rows))';
    await run(['python3','-B','-c',rankOracle,env.TEAQL_GROUPED_MAPPING_DATABASE],'rank-oracle.log');
    assert.deepEqual(files(path.join(__dirname,'lib')),receipt.generatedFiles);receipt.generatedLibraryUnchanged=true;receipt.sameDatabase=true;
    receipt.databaseSha256=sha(fs.readFileSync(env.TEAQL_GROUPED_MAPPING_DATABASE));
    assert.deepEqual(sourceHashes(),receipt.sourceHashes);
    receipt.passed=true;
  }catch(error){receipt.error=String(error);process.exitCode=1;console.error(error);}
  finally{receipt.disposableBytesRemoved=runner.safeToClean?ownedBytes(scratch):0;
    if(runner.safeToClean)fs.rmSync(scratch,{recursive:true});else receipt.unsafeBuildRetention={path:scratch,processGroup:runner.unsafeGroup};
    runner.dispose();receipt.scratchRemoved=!fs.existsSync(scratch);fs.writeFileSync(path.join(output,'receipt.json'),JSON.stringify(receipt,null,2)+'\n');console.log(JSON.stringify({passed:receipt.passed,evidence:output,scratchRemoved:receipt.scratchRemoved}));}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
