'use strict';
// A disposable non-Git view is trusted only by matching its explicit Git origin.
const fs=require('node:fs'),path=require('node:path'),cp=require('node:child_process'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function sourceIdentity(directory,origin=process.env.TEAQL_RUST_EXAMPLE_SOURCE_ROOT||directory){
 for(const p of[directory,origin]){assert(path.isAbsolute(p));assert.equal(fs.realpathSync(p),p,'source aliases are not allowed');}
 const git=args=>cp.execFileSync('git',['-C',origin,...args],{encoding:'utf8',timeout:30000,stdio:['ignore','pipe','pipe']}).trim();
 assert.equal(git(['rev-parse','--show-toplevel']),origin,'origin must be its Git root');
 const status=git(['status','--porcelain=v1','--untracked-files=all']);if(origin!==directory)assert.equal(status,'','copied execution requires a clean origin');
 const paths=git(['ls-files','-z']).split('\0').filter(Boolean).sort(),inputs={},h=crypto.createHash('sha256');assert(paths.length);
 for(const n of paths){
  assert(!path.isAbsolute(n)&&!n.split('/').some(v=>!v||v==='.'||v==='..'));
  const original=path.join(origin,n),copy=path.join(directory,n);
  for(const p of[original,copy])assert.equal(fs.realpathSync(path.dirname(p)),path.dirname(p),'source directory symlink');
  const s=fs.lstatSync(copy),o=fs.lstatSync(original);assert(s.isFile()&&o.isFile()&&!s.isSymbolicLink()&&!o.isSymbolicLink());
  const b=fs.readFileSync(copy);assert.equal(sha(b),sha(fs.readFileSync(original)),'copied source differs: '+n);assert.equal(s.mode,o.mode,'copied permissions differ: '+n);
  inputs[n]=sha(b);h.update(n+'\0'+s.mode+'\0');h.update(b);h.update('\0');
 }
 return {directory,originDirectory:origin,commit:git(['rev-parse','HEAD']),status,contentsSha256:h.digest('hex'),files:inputs};
}
module.exports={sourceIdentity};
