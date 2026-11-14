'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),fs=require('node:fs');
const {verifySources}=require('./native-verification.cjs');
const example=__dirname,repo=path.resolve(example,'../..');
const names=['teaql-core','teaql-macros','teaql-data-service','teaql-runtime','teaql-sql','teaql-provider-sqlite'];
const selected=fs.readFileSync(path.join(example,'models/school-model.xml'),'utf8').match(/teaql_rs_version="(\d+\.\d+\.\d+)"/)[1];
function fixture(){return {packages:[...names.map(name=>({name,version:selected,source:null,manifest_path:path.join(repo,name,'Cargo.toml')})),
  {name:'school-management-service-core',source:null,manifest_path:path.join(example,'lib/Cargo.toml')}]};}
function runner(metadata,version=selected){return async(_command,log)=>JSON.stringify(log==='runtime-metadata.json'
  ?{packages:[{name:'teaql-runtime',version}]}:metadata);}
test('all six runtime components and the generated library must resolve to this checkout',async()=>{
  const origins=await verifySources(runner(fixture()),'cargo',repo,example);
  assert.deepEqual(Object.keys(origins),names);assert(Object.values(origins).every(p=>p.source===null&&p.version===selected));
});
test('an old runtime dependency cannot be accepted just because its source is local',async()=>{
  const metadata=fixture();metadata.packages[0].version='7.0.0';
  await assert.rejects(verifySources(runner(metadata),'cargo',repo,example));
});
test('a registry artifact with the right version must be rejected',async()=>{
  const metadata=fixture();metadata.packages[0].source='registry+https://github.com/rust-lang/crates.io-index';
  await assert.rejects(verifySources(runner(metadata),'cargo',repo,example));
});
test('mixed versions must be rejected even if one component is the candidate',async()=>{
  const metadata=fixture();metadata.packages.push({...metadata.packages[0],version:'7.0.0'});
  await assert.rejects(verifySources(runner(metadata),'cargo',repo,example),/No mixed\/public runtime versions/);
});
test('a different source directory must not impersonate the current component',async()=>{
  const metadata=fixture();metadata.packages[0].manifest_path=path.join(repo,'teaql-runtime/Cargo.toml');
  await assert.rejects(verifySources(runner(metadata),'cargo',repo,example));
});
test('the generated domain library must not resolve to an unrelated manifest',async()=>{
  const metadata=fixture();metadata.packages.at(-1).manifest_path=path.join(repo,'teaql-core/Cargo.toml');
  await assert.rejects(verifySources(runner(metadata),'cargo',repo,example));
});
test('the candidate baseline itself must be explicit',async()=>{
  await assert.rejects(verifySources(runner(fixture(),'7.0.0'),'cargo',repo,example),/model-selected runtime/);
});
