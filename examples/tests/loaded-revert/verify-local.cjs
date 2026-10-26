#!/usr/bin/env node
'use strict';
// Offline consumer of the repository's runtime, never a published runtime.
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),cp=require('node:child_process'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const {createOwnedRunner,ownedBytes}=require('./owned-command.cjs');const {prepareOwnedSubreaper}=require('./subreaper.cjs');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const example=__dirname,repo=path.resolve(example,'../../..');
const CRATES=['teaql-core','teaql-macros','teaql-runtime','teaql-sql','teaql-data-service','teaql-provider-sqlite'];
const FLAGS=['knownValueRestored','knownNullRestored','zeroProviderOnNoop','loadedRelationRetained','unchangedPartialAccepted','modifiedPartialRejected','rawAliasRejected','remainingEditSaved','unrelatedPendingRetained'];
function fingerprint(){
 const bytes=fs.readFileSync(path.join(example,'provenance.json')),p=JSON.parse(bytes);assert.equal(p.schema,'teaql.rust-loaded-revert-fixture/v1');assert.equal(p.language,'rust');assert.equal(p.evaluationErrors,0);
 assert.equal(p.modelSha256,'8d3017392ffd00abbc06be2555ced27ac6360e2d05fabc4299e27a5ef8f1c762');
 for(const[file,hash]of Object.entries(p.files)){assert(!path.isAbsolute(file)&&!file.split('/').includes('..'));const absolute=path.join(example,file);assert.equal(fs.realpathSync(absolute),absolute);assert.equal(sha(fs.readFileSync(absolute)),hash,file);}
 assert.equal(p.files['model.xml'],p.modelSha256);return{provenanceSha256:sha(bytes),...p};
}
async function main(){
 const requested=process.env.TEAQL_RESTORATION_EVIDENCE_DIR||(process.env.TEAQL_EXAMPLES_EVIDENCE_DIR&&path.join(process.env.TEAQL_EXAMPLES_EVIDENCE_DIR,'loaded-revert'));
 const output=requested?path.resolve(requested):fs.mkdtempSync(path.join(os.tmpdir(),'teaql-restoration-evidence-'));
 if(requested){assert(path.isAbsolute(requested)&&!fs.existsSync(output));fs.mkdirSync(output,{recursive:true});}
 const report={schema:'teaql.rust-loaded-revert-example/v1',passed:false,localRuntime:repo,localSourceOnly:true,publicPublication:false,sameDatabase:true,commands:[],rounds:[],completeExampleInventoryAccepted:false,internalArtifactAccepted:false};
 let scratch,runner;
 try{
  assert(fs.statfsSync(os.tmpdir()).bavail*fs.statfsSync(os.tmpdir()).bsize>8*1024**3,'8 GiB free build space required');
  report.fixture=fingerprint();scratch=fs.mkdtempSync(path.join(os.tmpdir(),'teaql-restoration-build-'));runner=createOwnedRunner({supervisor:prepareOwnedSubreaper(scratch)});
  for(const file of Object.keys(report.fixture.files).filter(file=>file.startsWith('generated/'))){const relative=file.slice('generated/'.length),to=path.join(scratch,relative);fs.mkdirSync(path.dirname(to),{recursive:true});fs.copyFileSync(path.join(example,file),to);}
  for(const file of ['rust-loaded-revert.rs','rust-integral-generated.rs'])fs.copyFileSync(path.join(example,file),path.join(scratch,file));
  const cargo=['[package]','name="teaql-loaded-revert-example"','version="0.0.0"','edition="2024"','publish=false','[workspace]',
   '[[test]]','name="restoration"','path="rust-loaded-revert.rs"','[dependencies]','scalar-type-conformance-service-core={path="lib"}',
   ...CRATES.map(name=>name+'={path='+JSON.stringify(path.join(repo,name))+'}'),'chrono={version="0.4",features=["serde"]}','serde_json="1"','rust_decimal={version="=1.42.1",features=["serde"]}',
   'rusqlite={version="0.40",features=["bundled","trace","hooks"]}','tokio={version="1",features=["macros","rt-multi-thread"]}',
   '[patch.crates-io]',...CRATES.map(name=>name+'={path='+JSON.stringify(path.join(repo,name))+'}'),''].join('\n');
  fs.writeFileSync(path.join(scratch,'Cargo.toml'),cargo);fs.writeFileSync(path.join(output,'consumer-Cargo.toml'),cargo);fs.copyFileSync(path.join(repo,'Cargo.lock'),path.join(scratch,'Cargo.lock'));
  const env={...process.env,TMPDIR:scratch,CARGO_TARGET_DIR:path.join(scratch,'target'),CARGO_INCREMENTAL:'0',CARGO_BUILD_JOBS:'2',CARGO_PROFILE_DEV_DEBUG:'0',CARGO_PROFILE_TEST_DEBUG:'0',TEAQL_REVERT_DB:path.join(output,'restoration.sqlite')};delete env.TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS;
  async function run(name,command){const file=path.join(output,name+'.log'),fd=fs.openSync(file,'wx');let result;
   try{result=await runner.run(command,{cwd:scratch,env,timeout:600000,stdio:['ignore',fd,fd]});}finally{fs.closeSync(fd);}
   const b=fs.readFileSync(file);report.commands.push({name,command,exitCode:result.status,signal:result.signal,descendantProof:result.descendantProof,logSha256:sha(b)});
   assert(result.descendantProof?.safe);assert.equal(result.signal,null);assert.equal(result.status,0,b.toString().slice(-6000));return b.toString();
  }
  const metadata=JSON.parse((await run('resolution',['cargo','metadata','--offline','--format-version=1'])).split('\n').find(line=>line.startsWith('{"packages"')));
  report.origins=CRATES.map(name=>{const matches=metadata.packages.filter(p=>p.name===name);assert.equal(matches.length,1);const p=matches[0];assert.equal(p.source,null);assert.equal(p.manifest_path,path.join(repo,name,'Cargo.toml'));return{name,manifestPath:p.manifest_path,version:p.version};});
  fs.copyFileSync(path.join(scratch,'Cargo.lock'),path.join(output,'consumer-Cargo.lock'));report.lockSha256=sha(fs.readFileSync(path.join(output,'consumer-Cargo.lock')));
  await run('build',['cargo','test','--locked','--offline','--test','restoration','--no-run']);
  for(const round of ['1','2']){env.TEAQL_PROBE_ROUND=round;const log=await run('round-'+round,['cargo','test','--locked','--offline','--test','restoration','generated_known_baseline_restore','--','--nocapture','--test-threads=1']);
   assert.match(log,/test result: ok\. 1 passed; 0 failed;/);const matches=[...log.matchAll(/^RUST_LOADED_REVERT=(.+)$/gm)];assert.equal(matches.length,1);const value=JSON.parse(matches[0][1]);assert.equal(value.passed,true);assert.equal(value.round,round);assert.equal(value.finalVersion,2);
   for(const flag of FLAGS)assert.equal(value[flag],true,flag);assert.equal(value.completeExampleInventoryAccepted,false);assert.equal(value.internalArtifactAccepted,false);report.rounds.push(value);
  }
  assert.deepEqual(fingerprint(),report.fixture);report.passed=true;
 }catch(error){report.error=error.stack;console.error(error.stack);process.exitCode=1;}
 finally{
  const database=path.join(output,'restoration.sqlite');if(fs.existsSync(database))report.databaseSha256AtExit=sha(fs.readFileSync(database));
  const safe=!runner||runner.safeToClean;report.cleanup={scratchRemoved:false,disposedOwnedBytes:safe&&scratch?ownedBytes(scratch):0,sharedCachesDeleted:false,databasesDeleted:false};
  if(safe&&scratch){fs.rmSync(scratch,{recursive:true});report.cleanup.scratchRemoved=true;}else if(scratch)report.unsafeRetention=scratch;
  runner?.dispose();fs.writeFileSync(path.join(output,'report.json'),JSON.stringify(report,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify({output,passed:report.passed,rounds:report.rounds.length,cleanup:report.cleanup}));
 }
}
module.exports={FLAGS,fingerprint};if(require.main===module)main().catch(error=>{console.error(error.stack);process.exitCode=1;});
