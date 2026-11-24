#!/usr/bin/env node
'use strict';
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const {createOwnedRunner,ownedBytes}=require('./verify/owned-command.cjs');
const {prepareOwnedSubreaper}=require('./verify/subreaper.cjs');
const {sourceIdentity}=require('./verify/source-identity.cjs');
const example=__dirname,repo=path.resolve(example,'../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function files(dir,prefix=''){const out={};for(const n of fs.readdirSync(dir).sort()){const f=path.join(dir,n),s=fs.lstatSync(f);assert(!s.isSymbolicLink());if(s.isDirectory())Object.assign(out,files(f,prefix+n+'/'));else out[prefix+n]=sha(fs.readFileSync(f));}return out;}
function fixture(){const m=JSON.parse(fs.readFileSync(path.join(example,'fixture-manifest.json')));assert.equal(m.evaluationErrors,0);assert.equal(sha(fs.readFileSync(path.join(example,'model.xml'))),m.modelSha256);assert.deepEqual(files(path.join(example,'generated')),m.files);return m;}
async function main(){
 const requested=process.env.TEAQL_REQUEST_ADMISSION_EVIDENCE_DIR;
 const output=requested?path.resolve(requested):fs.mkdtempSync(path.join(os.tmpdir(),'teaql-request-admission-evidence-'));
 if(requested){assert(path.isAbsolute(requested)&&!fs.existsSync(output));fs.mkdirSync(output,{recursive:true});}
 const write=(n,b)=>{const f=path.join(output,n);fs.mkdirSync(path.dirname(f),{recursive:true});fs.writeFileSync(f,typeof b==='string'||Buffer.isBuffer(b)?b:JSON.stringify(b,null,2)+'\n',{flag:'wx'});};
 const r={schema:'teaql.rust-request-admission-example/v2',passed:false,commands:[],rounds:[],sameDatabase:true,
  providerMetadataIsEntryCounter:false,providerBoundaryStartsInstrumented:true,fullTraceContractAccepted:false,installedSdkAccepted:false,publicPublication:false,allP0P1Complete:false};
 let scratch,runner;
 try{
  assert(fs.statfsSync(os.tmpdir()).bavail*fs.statfsSync(os.tmpdir()).bsize>8*1024**3);
  r.source=sourceIdentity(repo);
  r.fixture=fixture();r.inputs=Object.fromEntries(['main.rs','missing_purpose.rs','missing_audit.rs','model.xml','fixture-manifest.json','verify-local.cjs','verify/owned-command.cjs','verify/subreaper.cjs','verify/owned-subreaper.c','verify/source-identity.cjs'].map(n=>[n,sha(fs.readFileSync(path.join(example,n)))]));
  for(const n of Object.keys(r.inputs))write('inputs/'+n,fs.readFileSync(path.join(example,n)));
  scratch=fs.mkdtempSync(path.join(os.tmpdir(),'teaql-request-admission-build-'));r.scratch=scratch;runner=createOwnedRunner({supervisor:prepareOwnedSubreaper(scratch)});fs.mkdirSync(path.join(scratch,'src'));
  for(const n of ['main.rs','missing_purpose.rs','missing_audit.rs'])fs.copyFileSync(path.join(example,n),path.join(scratch,'src',n));
  fs.cpSync(path.join(example,'generated/lib'),path.join(scratch,'lib'),{recursive:true});
  r.libraryMetadata=require('../../scripts/pin-local-example-library.cjs').pin(path.join(scratch,'lib'),repo,scratch);
  const q=JSON.stringify,crates=['teaql-core','teaql-runtime','teaql-macros','teaql-sql','teaql-data-service','teaql-provider-sqlite'];
  const manifest=['[package]','name="teaql-request-admission-example"','version="0.0.0"','edition="2024"','publish=false',
   ...[['request-admission','main.rs'],['missing-purpose','missing_purpose.rs'],['missing-audit','missing_audit.rs']].flatMap(([n,f])=>['[[bin]]','name='+q(n),'path='+q('src/'+f)]),
   '[dependencies]','checker-fix-advanced-core={path='+q(path.join(scratch,'lib'))+'}',
   'teaql-core={path='+q(path.join(repo,'teaql-core'))+'}','teaql-runtime={path='+q(path.join(repo,'teaql-runtime'))+'}',
   'chrono="0.4"','serde_json="1"','futures-util="0.3"','tokio={version="1",features=["macros","rt-multi-thread"]}',
   'rusqlite={version="0.41.1",features=["bundled","trace"]}',
   '[patch.crates-io]',...crates.map(n=>n+'={path='+q(path.join(repo,n))+'}'),''].join('\n');
  fs.writeFileSync(path.join(scratch,'Cargo.toml'),manifest);write('Cargo.toml',manifest);
  const env={...process.env};for(const n of Object.keys(env))if(/^(TEAQL_|CORE236_|OTEL_)/.test(n)||/TOKEN|PASSWORD|SECRET|AUTH|API_KEY/.test(n)||['CARGO_TARGET_DIR','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER'].includes(n))delete env[n];
  Object.assign(env,{PATH:['/home/philip/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin',path.dirname(process.execPath),env.PATH].join(path.delimiter),
   TMPDIR:scratch,CARGO_TARGET_DIR:path.join(scratch,'target'),CARGO_INCREMENTAL:'0',CARGO_BUILD_JOBS:'2',CARGO_PROFILE_DEV_DEBUG:'0',
   CARGO_PROFILE_DEV_DEBUG_ASSERTIONS:'true',CARGO_PROFILE_DEV_OVERFLOW_CHECKS:'true',TEAQL_TELEMETRY_ENABLED:'false',TEAQL_REQUEST_ADMISSION_DB:path.join(output,'admission.sqlite')});
  async function run(name,command,expected=0){const fd=fs.openSync(path.join(output,name+'.log'),'wx');let v;console.log(JSON.stringify({phase:name,state:'started'}));try{v=await runner.run(command,{cwd:scratch,env,timeout:600000,stdio:['ignore',fd,fd]});}finally{fs.closeSync(fd);}const b=fs.readFileSync(path.join(output,name+'.log'));r.commands.push({name,command,cwd:scratch,exitCode:v.status,signal:v.signal,descendantProof:v.descendantProof,logSha256:sha(b)});assert(!v.error&&!v.signal&&v.descendantProof?.safe&&v.descendantProof.primaryDone);assert.equal(v.status,expected,b.toString().slice(-9000));return b.toString();}
  const build=await run('build',['cargo','build','--offline','--bin','request-admission','--message-format=json']);write('Cargo.lock',fs.readFileSync(path.join(scratch,'Cargo.lock')));
  const messages=build.split(/\r?\n/).filter(n=>n.startsWith('{')).map(n=>JSON.parse(n)),artifacts=messages.filter(m=>m.reason==='compiler-artifact');
  r.compiledRuntime=artifacts.filter(m=>crates.includes(m.target.name.replaceAll('_','-'))).map(m=>({name:m.target.name,source:m.target.src_path}));assert.equal(new Set(r.compiledRuntime.map(m=>m.name)).size,6);
  for(const m of r.compiledRuntime)assert(m.source.startsWith(repo+'/'));
  const binary=artifacts.find(m=>m.executable&&m.target.name==='request-admission').executable;assert(binary.startsWith(scratch+'/target/'));r.binarySha256=sha(fs.readFileSync(binary));
  for(const round of[1,2]){env.TEAQL_REQUEST_ADMISSION_ROUND=String(round);const log=await run('round-'+round,[binary]);
   const rows=key=>log.split(/\r?\n/).filter(n=>n.startsWith(key+'=')).map(n=>JSON.parse(n.slice(key.length+1)));
   const frame={round,cases:rows('RUST_REQUEST_ADMISSION_CASE'),controls:rows('RUST_REQUEST_ADMISSION_CONTROL'),results:rows('RUST_REQUEST_ADMISSION_RESULT')};
   assert.equal(frame.cases.length,120);assert(frame.cases.every(c=>c.passed&&c.round===round&&c.identitiesUnchanged&&c.childReasonRetained&&c.privateValuesNotEchoed&&Object.values(c.effects).every(v=>v===0)));assert.equal(frame.controls.length,2);assert.deepEqual(frame.results,[{round,cases:120,controls:2,passed:true}]);r.rounds.push(frame);assert.equal(sha(fs.readFileSync(binary)),r.binarySha256);
  }
  r.compileFail=[];
  for(const[bin,method]of[['missing-purpose','execute_for_list'],['missing-audit','save']]){
   const log=await run(bin,['cargo','check','--offline','--bin',bin,'--message-format=json'],101);
   const diagnostics=log.split(/\r?\n/).filter(n=>n.startsWith('{')).map(n=>JSON.parse(n)).filter(m=>m.reason==='compiler-message'&&m.message.level==='error').map(m=>m.message);
   assert.equal(diagnostics.length,1);assert.equal(diagnostics[0].code?.code,'E0599');assert(diagnostics[0].message.includes(method));assert(diagnostics[0].spans.some(s=>s.is_primary&&s.file_name.endsWith(bin.replaceAll('-','_')+'.rs')));
   r.compileFail.push({bin,method,code:'E0599',diagnostics});
  }
  assert.deepEqual(fixture(),r.fixture);for(const[n,h]of Object.entries(r.inputs))assert.equal(sha(fs.readFileSync(path.join(example,n))),h);
  r.sourceAfter=sourceIdentity(repo);assert.deepEqual(r.sourceAfter,r.source);
  r.database={file:'admission.sqlite',sha256:sha(fs.readFileSync(path.join(output,'admission.sqlite'))),bytes:fs.statSync(path.join(output,'admission.sqlite')).size};r.passed=true;
 }catch(error){r.error=error.stack;console.error(r.error);process.exitCode=1;}
 finally{r.cleanup={scratchRemoved:false,sharedCachesDeleted:false,sourceOrDatabasesDeleted:false};try{assert(!runner||runner.safeToClean);r.cleanup.disposedOwnedBytes=scratch?ownedBytes(scratch):0;if(scratch)fs.rmSync(scratch,{recursive:true});r.cleanup.scratchRemoved=true;}catch(error){r.cleanup.error=error.stack;r.passed=false;process.exitCode=1;}finally{runner?.dispose();write('report.json',r);console.log(JSON.stringify({output,passed:r.passed,error:r.error,cleanup:r.cleanup}));}}
 return r;
}
module.exports={main};if(require.main===module)main().catch(e=>{console.error(e.stack);process.exitCode=1;});
