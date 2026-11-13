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
  const statistics=[],expressionMeasures=[],rollupTraceScopes=[];
  for(const round of [1,2]){
    const log=await run([path.join(target,'debug/rollup_statistics_probe')],'statistics-round-'+round+'.log',example);
    const traceLines=log.split('\n').filter(line=>line.startsWith('GENERATED_ROLLUP_TRACE_SCOPES='));assert.equal(traceLines.length,1);
    const trace=JSON.parse(traceLines[0].slice('GENERATED_ROLLUP_TRACE_SCOPES='.length));
    assert(trace.passed&&trace.generatedQ);assert.equal(trace.caseCount,5);
    assert.deepEqual(trace.observations.map(x=>x.method),['roll_up_to_school_type','roll_up_to_school_type_with',
      'group_by_school_type_with','group_by_school_type_with_details_from','roll_up_to_school_type']);
    assert.equal(trace.observations[4].requests,3);rollupTraceScopes.push(trace);
    const lines=log.split('\n').filter(l=>l.startsWith('LAYERED_ROLLUP='));assert.equal(lines.length,1);
    const proof=JSON.parse(lines[0].slice('LAYERED_ROLLUP='.length));assert(proof.passed&&proof.javaShaped);assert.equal(proof.caseCount,36);
    const expressionLines=log.split('\n').filter(line=>line.startsWith('GENERATED_EXPRESSION_MEASURES='));assert.equal(expressionLines.length,1);
    const expression=JSON.parse(expressionLines[0].slice('GENERATED_EXPRESSION_MEASURES='.length));
    assert(expression.passed&&expression.generatedQ);assert.equal(expression.caseCount,6);
    assert.deepEqual(expression.observations.map(x=>x.case),['expression-ordinary','expression-owner-grain','expression-operators',
      'expression-transforms','expression-empty-null','expression-parent-only']);
    assert.equal(expression.observations[1].row.perSchool,15);assert.equal(expression.observations[1].row.school_type.perType,14);
    assert.equal(expression.observations[2].checked,13);assert.equal(expression.observations[4].checked,13);
    assert.equal(expression.observations[5].row.school_type.owners,2);expressionMeasures.push(expression);
    assert.equal(proof.observations[0].row.schools,5);assert.equal(Number(proof.observations[0].row.capacity),3300);
    const aliases=proof.observations.filter(item=>item.case.startsWith('alias-'));assert.equal(aliases.length,5);
    assert.deepEqual(aliases[0].rows.map(row=>row.kindKey),[1001,1002]);
    for(const name of ['typed-independent-statistical-membership','typed-independent-layered-membership',
      'renavigate-grouped-copy','renavigate-entity-copy']){
      const item=proof.observations.find(item=>item.case===name);assert(item,'Missing '+name);
      assert.equal(item.row.schools,1);assert.equal(Number(item.row.capacity),100);
    }
    assert.equal(proof.observations.find(item=>item.case==='renavigate-keeps-independent-filter')?.rows,0);
    const reverse=proof.observations.slice(-5);
    assert.deepEqual(reverse.map(x=>x.case),['reverse-layered-empty-membership','reverse-layered-empty-exclusion',
      'reverse-layered-having','reverse-layered-cell-page','same-entity-layered-membership']);
    assert.equal(reverse[0].types,0);assert.equal(reverse[1].types,2);assert.equal(reverse[4].types,0);
    for(const cell of reverse.slice(2,4)){assert.equal(cell.primary,1);assert.equal(cell.secondary,1);}
    statistics.push(proof);
  }
  const numeric=[],gbk=[],bitwise=[];
  for(const round of [1,2]){
    const log=await run([path.join(target,'debug/numeric_rollup_probe')],'numeric-round-'+round+'.log',example);
    const lines=log.split('\n').filter(l=>l.startsWith('GENERATED_NUMERIC_ROLLUP='));assert.equal(lines.length,1);
    const proof=JSON.parse(lines[0].slice('GENERATED_NUMERIC_ROLLUP='.length));
    assert(proof.passed&&proof.generatedQAndE&&proof.auditedMutation);assert.equal(proof.caseCount,8);
    assert.deepEqual(proof.cases,['numeric-ordinary-order','numeric-ordinary-having','numeric-parent-page',
      'numeric-nested-page','numeric-parent-having','numeric-nested-having','numeric-unselected-order-rejected',
      'numeric-ordinary-order-rejected']);
    assert.deepEqual(proof.numericRanks,['2','10']);assert.deepEqual(proof.restoredRanks,['1','2']);numeric.push(proof);
    const gbkLines=log.split('\n').filter(l=>l.startsWith('GENERATED_GBK_ROLLUP='));assert.equal(gbkLines.length,1);
    const capability=JSON.parse(gbkLines[0].slice('GENERATED_GBK_ROLLUP='.length));
    assert(capability.passed&&capability.generatedQAndE&&capability.auditedMutation);
    assert.equal(capability.supported,false);assert.equal(capability.caseCount,4);
    assert.deepEqual(capability.restoredNames,['Primary','Secondary']);
    assert.deepEqual(capability.observations.map(x=>x.case),['gbk-ordinary-asc','gbk-ordinary-desc','gbk-parent','gbk-nested']);
    assert(capability.observations.every(x=>x.supported===false));gbk.push(capability);
    const bitLines=log.split('\n').filter(l=>l.startsWith('GENERATED_BITWISE_ROLLUP='));assert.equal(bitLines.length,1);
    const bits=JSON.parse(bitLines[0].slice('GENERATED_BITWISE_ROLLUP='.length));
    assert(bits.passed&&bits.generatedQ&&bits.integerResults);assert.equal(bits.caseCount,9);
    assert.deepEqual(bits.observations.map(x=>x.case),['bitwise-ordinary','bitwise-grouped','bitwise-parent',
      'bitwise-two-hop','bitwise-having-page','bitwise-empty-scalar','bitwise-empty-layered','bitwise-empty-grouped',
      'bitwise-invalid-field-rejected']);
    assert.equal(bits.observations[8].rejected,true);assert.equal(bits.observations[8].checks,6);
    bitwise.push(bits);
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
  return {statistics,expressionMeasures,rollupTraceScopes,numeric,gbk,bitwise,runs,allTargets:true,mainSmoke:true,generatedHelperTests:6};
}
module.exports={verifySources,runAdditionalProbes};
