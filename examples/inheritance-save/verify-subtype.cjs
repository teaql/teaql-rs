'use strict';
// Focused source-only gate; each process group and build directory is task-owned.
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),cp=require('node:child_process');
const crypto=require('node:crypto'),assert=require('node:assert/strict');
const {createOwnedRunner,ownedBytes}=require('../fix-e-alignment/owned-command.cjs');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
async function main(){
  const output=process.argv[2],repo=path.resolve(__dirname,'../..');
  assert(output&&path.isAbsolute(output)&&!fs.existsSync(output));
  assert(fs.statfsSync(repo).bavail*fs.statfsSync(repo).bsize>4*1024**3);
  fs.mkdirSync(output,{recursive:true});const runner=createOwnedRunner();let scratch;
  const receipt={schema:'teaql.inherited-subtype-native/v1',passed:false,publication:false,commands:[]};
  try{
    scratch=fs.mkdtempSync(path.join(os.tmpdir(),'teaql-subtype-native-'));
    const names=['Cargo.toml','Cargo.lock','teaql-core/src/meta.rs','teaql-core/src/entity.rs','teaql-core/src/lib.rs',
      'teaql-core/tests/inherited_runtime_type.rs','teaql-macros/src/attr.rs','teaql-macros/src/derive_impl.rs',
      'teaql-runtime/src/entity_runtime.rs','teaql-runtime/src/entity_save.rs','teaql-runtime/tests/inherited_type_identity.rs',
      'teaql-core/src/mutation.rs','teaql-runtime/src/mutation_version.rs','teaql-runtime/src/mutation_policy.rs',
      'teaql-runtime/src/data_service/graph.rs','teaql-runtime/src/data_service/resolved.rs',
      'teaql-runtime/src/event.rs','teaql-runtime/src/graph.rs','teaql-runtime/src/entity_save/context_roots.rs',
      'teaql-runtime/src/memory.rs','teaql-sql/src/mutation_execution.rs',
      'teaql-sql/src/inheritance.rs','teaql-sql/src/dialect.rs','teaql-provider-sqlite/src/inheritance_tests.rs',
      'examples/inheritance-save/verify-subtype.cjs'];
    const sourceHashes=()=>Object.fromEntries(names.map(name=>[name,sha(fs.readFileSync(path.join(repo,name)))]));
    receipt.sourceHashes=sourceHashes();receipt.runtimeCommit=cp.execFileSync('git',['rev-parse','HEAD'],{cwd:repo,encoding:'utf8'}).trim();
    const env={...process.env,CARGO_TARGET_DIR:path.join(scratch,'target'),CARGO_INCREMENTAL:'0',CARGO_BUILD_JOBS:'4',
      CARGO_PROFILE_TEST_DEBUG:'0',CARGO_PROFILE_DEV_DEBUG:'0',TEAQL_TELEMETRY_ENABLED:'false'};
    const cargo=process.env.CARGO||'cargo';
    for(const [args,log] of [
      [['-p','teaql-macros','--lib'],'macros.log'],
      [['-p','teaql-core','--test','inherited_runtime_type'],'core.log'],
      [['-p','teaql-runtime','--test','inherited_type_identity'],'runtime.log'],
      [['-p','teaql-runtime','--lib','recovery'],'recovery-unit.log'],
      [['-p','teaql-sql','--lib','inheritance::tests::'],'sql.log'],
      [['-p','teaql-provider-sqlite','--lib','inheritance_tests::'],'sqlite.log'],
    ]){
      const command=[cargo,'test','--offline','--locked',...args,'--','--nocapture'];
      const fd=fs.openSync(path.join(output,log),'wx');let result;
      try{result=await runner.run(command,{cwd:repo,env,timeout:900000,stdio:['ignore',fd,fd]});}finally{fs.closeSync(fd);}
      const bytes=fs.readFileSync(path.join(output,log));const testsPassed=[...bytes.toString().matchAll(/test result: ok\. (\d+) passed;/g)].reduce((n,m)=>n+Number(m[1]),0);
      receipt.commands.push({command,log,exitCode:result.status,signal:result.signal,logSha256:sha(bytes),testsPassed});
      assert.equal(result.status,0,bytes.toString().slice(-6000));assert(testsPassed>0);
    }
    assert.deepEqual(sourceHashes(),receipt.sourceHashes);receipt.passed=true;
  }catch(error){receipt.error=String(error);process.exitCode=1;}
  finally{
    if(runner.safeToClean&&scratch){receipt.disposableBytesRemoved=ownedBytes(scratch);fs.rmSync(scratch,{recursive:true});}
    else if(scratch)receipt.unsafeRetention={scratch,processGroup:runner.unsafeGroup};
    runner.dispose();receipt.scratchRemoved=!scratch||!fs.existsSync(scratch);
    fs.writeFileSync(path.join(output,'receipt.json'),JSON.stringify(receipt,null,2)+'\n');
    console.log(JSON.stringify({passed:receipt.passed,evidence:output,error:receipt.error,scratchRemoved:receipt.scratchRemoved}));
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
