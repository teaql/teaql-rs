'use strict';
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');

async function verifySources(run,cargo,repo,example){
  const root=JSON.parse(await run([cargo,'metadata','--offline','--locked','--no-deps','--format-version=1','--manifest-path',path.join(repo,'Cargo.toml')],'runtime-metadata.json'));
  const version=root.packages.find(p=>p.name==='teaql-runtime').version;
  assert.equal(version,'7.1.0','This statistics example targets the 7.1 candidate');
  const metadata=JSON.parse(await run([cargo,'metadata','--offline','--locked','--format-version=1','--manifest-path',path.join(example,'Cargo.toml')],'consumer-metadata.json'));
  const origins={};
  for(const name of ['teaql-core','teaql-macros','teaql-data-service','teaql-runtime','teaql-sql','teaql-provider-sqlite']){
    const packages=metadata.packages.filter(p=>p.name===name);assert.equal(packages.length,1,'No mixed/public runtime versions: '+name);
    const p=packages[0];assert.equal(p.version,version);assert.equal(p.source,null);
    assert.equal(fs.realpathSync(p.manifest_path),fs.realpathSync(path.join(repo,name,'Cargo.toml')));
    origins[name]={version:p.version,manifest:p.manifest_path,source:p.source};
  }
  const library=metadata.packages.find(p=>p.name==='school-management-service-core');assert(library);
  assert.equal(library.source,null);assert.equal(fs.realpathSync(library.manifest_path),fs.realpathSync(path.join(example,'lib/Cargo.toml')));
  return origins;
}

async function runAdditionalProbes(run,cargo,repo,example,target,output,env){
  const manifest=path.join(example,'Cargo.toml');
  const build=await run([cargo,'test','--offline','--locked','--manifest-path',manifest,'--all-targets'],'example-tests.log');
  for(const name of ['mapping_metadata_does_not_expose_legacy_columns_as_logical_api_fields',
    'grouping_without_aggregates_returns_only_logical_dimensions','grouped_legacy_foreign_keys_preserve_logical_names_and_aliases',
    'grouped_legacy_smart_list_keeps_explicit_relation_enhancements','grouped_legacy_relations_load_details_using_the_logical_foreign_key'])
    assert(build.includes('test '+name+' ... ok'),'Missing executed legacy mapping case: '+name);
  const unit=await run([cargo,'test','--offline','--locked','--manifest-path',manifest,'-p','school-management-service-core','--lib'],'generated-helper-tests.log');
  assert(unit.includes('6 passed; 0 failed'),'Generated navigation/copy guards must execute');
  assert(unit.includes('independent_relation_filter_metadata_survives_navigation ... ok'));
  await run([cargo,'build','--offline','--locked','--manifest-path',manifest,'--bins'],'all-binaries.log');
  const statistics=[];
  for(const round of [1,2]){
    const log=await run([path.join(target,'debug/rollup_statistics_probe')],'statistics-round-'+round+'.log',example);
    const lines=log.split('\n').filter(l=>l.startsWith('LAYERED_ROLLUP='));assert.equal(lines.length,1);
    const proof=JSON.parse(lines[0].slice('LAYERED_ROLLUP='.length));assert(proof.passed&&proof.javaShaped);assert.equal(proof.caseCount,31);
    assert.equal(proof.observations[0].row.schools,5);assert.equal(Number(proof.observations[0].row.capacity),3300);
    const aliases=proof.observations.filter(item=>item.case.startsWith('alias-'));assert.equal(aliases.length,5);
    assert.deepEqual(aliases[0].rows.map(row=>row.kindKey),[1001,1002]);
    for(const name of ['typed-independent-statistical-membership','typed-independent-layered-membership',
      'renavigate-grouped-copy','renavigate-entity-copy']){
      const item=proof.observations.find(item=>item.case===name);assert(item,'Missing '+name);
      assert.equal(item.row.schools,1);assert.equal(Number(item.row.capacity),100);
    }
    assert.equal(proof.observations.find(item=>item.case==='renavigate-keeps-independent-filter')?.rows,0);
    statistics.push(proof);
  }
  const numeric=[];
  for(const round of [1,2]){
    const log=await run([path.join(target,'debug/numeric_rollup_probe')],'numeric-round-'+round+'.log',example);
    const lines=log.split('\n').filter(l=>l.startsWith('GENERATED_NUMERIC_ROLLUP='));assert.equal(lines.length,1);
    const proof=JSON.parse(lines[0].slice('GENERATED_NUMERIC_ROLLUP='.length));
    assert(proof.passed&&proof.generatedQAndE&&proof.auditedMutation);assert.equal(proof.caseCount,7);
    assert.deepEqual(proof.cases,['numeric-ordinary-order','numeric-ordinary-having','numeric-parent-page',
      'numeric-nested-page','numeric-parent-having','numeric-nested-having','numeric-unselected-order-rejected']);
    assert.deepEqual(proof.numericRanks,['2','10']);assert.deepEqual(proof.restoredRanks,['1','2']);numeric.push(proof);
  }
  env.TEAQL_SCHOOL_EXAMPLE_DATABASE=path.join(output,'main.sqlite');
  assert((await run([path.join(target,'debug/school-management-bootstrap-example')],'main-smoke.log',example)).includes('PASS Rust School bootstrap'));
  const runs=[];
  for(const round of [1,2]){
    env.TEAQL_SCHOOL_DELETE_PROBE_DATABASE=path.join(output,'delete.sqlite');
    await run([path.join(target,'debug/soft_delete_return_probe')],'delete-round-'+round+'.log',example);runs.push('delete-'+round);
    env.SCHOOL_MANAGEMENT_SERVICE_CORE_DATABASE_URL=path.join(output,'environment.sqlite');
    await run([path.join(target,'debug/env_runtime_save_probe')],'environment-round-'+round+'.log',example);runs.push('environment-'+round);
  }
  for(const logging of ['on','off'])for(const round of [1,2]){
    env.TEAQL_SCHOOL_BOOTSTRAP_DB=path.join(output,'bootstrap-'+logging+'.sqlite');env.TEAQL_SCHOOL_BOOTSTRAP_LOGGING=logging;
    const log=await run([path.join(target,'debug/bootstrap_trace_probe')],'bootstrap-'+logging+'-'+round+'.log',example);
    assert(log.includes('PASS Rust generated bootstrap trace logging='+(logging==='on')));runs.push('bootstrap-'+logging+'-'+round);
  }
  return {statistics,numeric,runs,allTargets:true,mainSmoke:true,generatedHelperTests:6};
}
module.exports={verifySources,runAdditionalProbes};
