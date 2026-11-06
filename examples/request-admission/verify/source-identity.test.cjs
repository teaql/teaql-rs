'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),cp=require('node:child_process');
const {sourceIdentity}=require('./source-identity.cjs');
function fixture(run){const scratch=fs.mkdtempSync(path.join(os.tmpdir(),'teaql-request-origin-test-'));try{
 const origin=path.join(scratch,'origin'),copy=path.join(scratch,'copy');fs.mkdirSync(origin);fs.mkdirSync(copy);
 const git=args=>cp.execFileSync('git',['-C',origin,...args],{stdio:'pipe',timeout:30000});git(['init','-q']);git(['config','user.name','Fixture']);git(['config','user.email','fixture@example.invalid']);
 fs.writeFileSync(path.join(origin,'source.rs'),'fn main() {}\n');fs.writeFileSync(path.join(origin,'verify.sh'),'#!/bin/sh\nexit 0\n');fs.chmodSync(path.join(origin,'verify.sh'),0o755);
 git(['add','.']);git(['-c','commit.gpgsign=false','commit','-q','-m','fixture']);for(const n of['source.rs','verify.sh'])fs.cpSync(path.join(origin,n),path.join(copy,n),{preserveTimestamps:true});
 return run({origin,copy,git});
}finally{fs.rmSync(scratch,{recursive:true});}}
test('direct Git and explicitly sourced copy retain identical fingerprints',()=>fixture(({origin,copy})=>{const a=sourceIdentity(origin,origin),b=sourceIdentity(copy,origin);assert.equal(a.contentsSha256,b.contentsSha256);assert.deepEqual(a.files,b.files);assert.equal(b.originDirectory,origin);assert.equal(b.directory,copy);}));
test('non-Git execution without an origin fails closed',()=>fixture(({copy})=>assert.throws(()=>sourceIdentity(copy,copy))));
test('changed copy content is rejected',()=>fixture(({origin,copy})=>{fs.writeFileSync(path.join(copy,'source.rs'),'changed\n');assert.throws(()=>sourceIdentity(copy,origin));}));
test('a missing tracked copy file is rejected',()=>fixture(({origin,copy})=>{fs.unlinkSync(path.join(copy,'source.rs'));assert.throws(()=>sourceIdentity(copy,origin));}));
test('changed executable permissions are rejected',()=>fixture(({origin,copy})=>{fs.chmodSync(path.join(copy,'verify.sh'),0o644);assert.throws(()=>sourceIdentity(copy,origin));}));
test('a file symlink cannot substitute for the matching copy',()=>fixture(({origin,copy})=>{fs.unlinkSync(path.join(copy,'source.rs'));fs.symlinkSync(path.join(origin,'source.rs'),path.join(copy,'source.rs'));assert.throws(()=>sourceIdentity(copy,origin));}));
test('a dirty origin cannot authorize copied execution',()=>fixture(({origin,copy})=>{fs.writeFileSync(path.join(origin,'untracked.rs'),'untracked\n');assert.throws(()=>sourceIdentity(copy,origin));}));
test('relative origins are rejected',()=>fixture(({copy})=>assert.throws(()=>sourceIdentity(copy,'origin'))));
