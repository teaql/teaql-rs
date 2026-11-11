#!/usr/bin/env node
'use strict';
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),cp=require('node:child_process'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const {createOwnedRunner,ownedBytes}=require('../tests/loaded-revert/owned-command.cjs');
const {prepareOwnedSubreaper}=require('../tests/loaded-revert/subreaper.cjs');
const EXAMPLE=__dirname,RUNTIME=path.resolve(EXAMPLE,'../..'),CRATES=['teaql-core','teaql-macros','teaql-runtime','teaql-sql','teaql-data-service','teaql-provider-sqlite'];
// A full-inventory runner may execute a disposable byte-identical source copy.
// Its Git origin must be explicit; never manufacture Git metadata in that copy.
const ORIGIN=process.env.TEAQL_RUST_EXAMPLE_SOURCE_ROOT?path.resolve(process.env.TEAQL_RUST_EXAMPLE_SOURCE_ROOT):RUNTIME;
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),digest=f=>sha(fs.readFileSync(f));
function fingerprint(source=RUNTIME){
 assert.equal(fs.realpathSync(ORIGIN),ORIGIN);assert.equal(fs.realpathSync(source),source);
 const files=[...new Set(cp.execFileSync('git',['-C',ORIGIN,'ls-files','-z','--cached','--others','--exclude-standard']).toString().split('\0').filter(Boolean))].sort(),hash=crypto.createHash('sha256');
 for(const n of files){hash.update(n+'\0');const f=path.join(source,n);if(!fs.existsSync(f)){hash.update('deleted\0');continue;}const s=fs.lstatSync(f);hash.update(String(s.mode)+'\0');if(s.isSymbolicLink())hash.update(fs.readlinkSync(f));else if(s.isFile())hash.update(fs.readFileSync(f));hash.update('\0');}return hash.digest('hex');
}
function sourceIdentity(){
 const sourceContentsSha256=fingerprint();
 assert.equal(fingerprint(ORIGIN),sourceContentsSha256,'source copy must exactly match its real Git origin');
 return{commit:cp.execFileSync('git',['-C',ORIGIN,'rev-parse','HEAD'],{encoding:'utf8'}).trim(),sourceContentsSha256};
}
function fixture(){
 const root=path.join(EXAMPLE,'generated'),manifest=JSON.parse(fs.readFileSync(path.join(root,'manifest.json'))),p=JSON.parse(fs.readFileSync(path.join(EXAMPLE,'provenance.json')));
 assert.equal(manifest.schema,'teaql.generated-expression-model-profiles/v1');assert.equal(manifest.language,'rust');assert.equal(manifest.localSourceOnly,true);
 assert.equal(digest(path.join(root,'manifest.json')),p.manifestSha256);assert.deepEqual(Object.keys(manifest.profiles).sort(),['PascalCase','camelCase','snake_case']);
 for(const[profile,v]of Object.entries(manifest.profiles)){assert.deepEqual(v.evaluation.errors,[]);assert.equal(digest(path.join(root,profile,'model.xml')),v.modelSha256);
  for(const[n,h]of Object.entries(v.files)){const f=path.resolve(root,profile,n);assert(f.startsWith(path.join(root,profile)+path.sep));assert.equal(fs.realpathSync(f),f);assert.equal(digest(f),h);}}
 return{manifest,provenance:p,provenanceSha256:digest(path.join(EXAMPLE,'provenance.json'))};
}
async function main(){
 const requested=process.env.TEAQL_GENERATED_E_PROFILE_EVIDENCE_DIR;
 const output=requested?path.resolve(requested):fs.mkdtempSync(path.join(os.tmpdir(),'teaql-generated-e-profile-evidence-'));
 if(requested){assert(path.isAbsolute(requested)&&!fs.existsSync(output));fs.mkdirSync(output,{recursive:true});}
 const report={schema:'teaql.rust-generated-e-model-profiles/v1',passed:false,localSourceOnly:true,publicPublication:false,
  currentInstalledArtifactAccepted:false,completeExampleInventoryAccepted:false,allP0P1Complete:false,commands:[],profiles:{}};
 let scratch,runner;
 try{
  const disk=fs.statfsSync(os.tmpdir());assert(disk.bavail*disk.bsize>8*1024**3);
  report.sourceBefore=sourceIdentity();
  if(ORIGIN!==RUNTIME)report.runtimeSourceCopy={path:RUNTIME,origin:ORIGIN,sourceContentsSha256:report.sourceBefore.sourceContentsSha256};
  report.fixture=fixture();report.applicationSha256=digest(path.join(EXAMPLE,'main.rs'));report.controllerSha256=digest(__filename);
  scratch=fs.mkdtempSync(path.join(os.tmpdir(),'teaql-generated-e-profile-build-'));runner=createOwnedRunner({supervisor:prepareOwnedSubreaper(scratch)});
  const env={...process.env,TMPDIR:scratch,CARGO_TARGET_DIR:path.join(scratch,'target'),CARGO_INCREMENTAL:'0',CARGO_BUILD_JOBS:'2',CARGO_PROFILE_DEV_DEBUG:'0',CARGO_PROFILE_TEST_DEBUG:'0'};
  async function run(profile,name,command,cwd){const folder=path.join(output,profile);fs.mkdirSync(folder,{recursive:true});const file=path.join(folder,name+'.log'),fd=fs.openSync(file,'wx');let result;
   try{result=await runner.run(command,{cwd,env,timeout:600000,stdio:['ignore',fd,fd]});}finally{fs.closeSync(fd);}
   const log=fs.readFileSync(file,'utf8');report.commands.push({profile,name,command,exitCode:result.status,signal:result.signal,descendantProof:result.descendantProof,logSha256:digest(file)});
   assert(result.descendantProof?.safe);assert.equal(result.signal,null);assert.equal(result.status,0,log.slice(-6000));return log;
  }
  for(const profile of ['camelCase','snake_case','PascalCase']){
   const work=path.join(scratch,profile);fs.mkdirSync(work);fs.cpSync(path.join(EXAMPLE,'generated',profile,'lib'),path.join(work,'lib'),{recursive:true});fs.copyFileSync(path.join(EXAMPLE,'main.rs'),path.join(work,'main.rs'));
   report.libraryMetadata??={};report.libraryMetadata[profile]=require('../../scripts/pin-local-example-library.cjs').pin(path.join(work,'lib'),RUNTIME,scratch);
   const cargo=['[package]','name="teaql-generated-expression-profile-example"','version="0.0.0"','edition="2024"','publish=false','[workspace]',
    '[[bin]]','name="expression-profile"','path="main.rs"','[dependencies]','scalar-type-conformance-service-core={path="lib"}',
    ...CRATES.map(n=>n+'={path='+JSON.stringify(path.join(RUNTIME,n))+'}'),'serde_json="1"','rusqlite={version="0.41.1",features=["bundled","trace"]}',
    'tokio={version="1",features=["macros","rt-multi-thread"]}','[patch.crates-io]',...CRATES.map(n=>n+'={path='+JSON.stringify(path.join(RUNTIME,n))+'}'),''].join('\n');
   fs.writeFileSync(path.join(work,'Cargo.toml'),cargo);fs.copyFileSync(path.join(RUNTIME,'Cargo.lock'),path.join(work,'Cargo.lock'));
   const metadata=JSON.parse((await run(profile,'resolution',['cargo','metadata','--offline','--format-version=1'],work)).split('\n').find(l=>l.startsWith('{"packages"')));
   const origins=CRATES.map(n=>{const p=metadata.packages.filter(p=>p.name===n);assert.equal(p.length,1);assert.equal(p[0].source,null);assert.equal(p[0].manifest_path,path.join(RUNTIME,n,'Cargo.toml'));return{name:n,version:p[0].version,manifestPath:p[0].manifest_path};});
   fs.copyFileSync(path.join(work,'Cargo.toml'),path.join(output,profile,'consumer-Cargo.toml'));fs.copyFileSync(path.join(work,'Cargo.lock'),path.join(output,profile,'consumer-Cargo.lock'));
   const state={origins,rounds:[],sameDatabase:true};report.profiles[profile]=state;
   await run(profile,'build',['cargo','build','--offline','--locked','--bin','expression-profile'],work);
   env.TEAQL_EXPRESSION_PROFILE=profile;env.TEAQL_EXPRESSION_DB=path.join(output,profile,'fixture.sqlite');
   for(const round of [1,2]){env.TEAQL_EXPRESSION_ROUND=String(round);const log=await run(profile,'round-'+round,['cargo','run','--offline','--locked','--bin','expression-profile'],work);
    const values=[...log.matchAll(/^GENERATED_E_MODEL_PROFILE=(.+)$/gm)].map(m=>JSON.parse(m[1]));assert.equal(values.length,1);const value=values[0];
    assert.equal(value.passed,true);assert.equal(value.profile,profile);assert.equal(value.round,round);assert.equal(value.providerEntries,0);assert(value.queryCalibration>0);state.rounds.push(value);
   }
   state.databaseSha256=digest(env.TEAQL_EXPRESSION_DB);assert.deepEqual(fixture(),report.fixture);assert.equal(fingerprint(),report.sourceBefore.sourceContentsSha256);
   assert.equal(fingerprint(ORIGIN),report.sourceBefore.sourceContentsSha256,'Git origin changed during the isolated run');
  }
  report.passed=true;
 }catch(error){report.error=error.stack;console.error(error.stack);process.exitCode=1;}
 finally{
  const safe=!runner||runner.safeToClean;report.cleanup={scratchRemoved:false,disposedOwnedBytes:safe&&scratch?ownedBytes(scratch):0,sharedCachesDeleted:false,databasesDeleted:false};
  if(safe&&scratch){fs.rmSync(scratch,{recursive:true});report.cleanup.scratchRemoved=true;}else if(scratch)report.unsafeRetention=scratch;
  runner?.dispose();report.sourceAfter=fingerprint();fs.writeFileSync(path.join(output,'report.json'),JSON.stringify(report,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify({output,passed:report.passed,cleanup:report.cleanup}));
 }
}
module.exports={fingerprint,sourceIdentity,fixture};if(require.main===module)main().catch(error=>{console.error(error.stack);process.exitCode=1;});
