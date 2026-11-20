#!/usr/bin/env python3
"""Exercise the loopback federation workspace with two starts on one SQLite store."""
import argparse
from contextlib import closing
import hashlib
from http import HTTPStatus
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

CASE = Path(__file__).resolve().parent
REPO = CASE.parents[1] if CASE.parent.name == 'example' else CASE.parents[2]
ORDER = 'FEDERATION-042'


def command(args, log):
    with log.open('xb') as file:
        subprocess.run(args, cwd=REPO, stdout=file, stderr=subprocess.STDOUT, check=True, timeout=1200)


def build(runtime, scratch, evidence):
    if runtime == 'rust':
        manifest = CASE / 'rust-web-axum/Cargo.toml'
        args = ['--offline', '--locked', '--manifest-path', str(manifest)]
        metadata = evidence / 'dependency-origins.json'
        command(['cargo', 'metadata', *args, '--format-version=1'], metadata)
        content = metadata.read_text()
        packages = json.loads(content[content.index('{'):])['packages']
        teaql = [row for row in packages if row['name'].startswith('teaql-')]
        assert all(row['source'] is None and Path(row['manifest_path']) == REPO / row['name'] / 'Cargo.toml'
                   for row in teaql)
        assert {'teaql-runtime', 'teaql-sql', 'teaql-provider-sqlite', 'teaql-tfp-endpoint'} <= {r['name'] for r in teaql}
        domain, = [row for row in packages if row['name'] == 'order-federation-service-core']
        assert domain['source'] is None and Path(domain['manifest_path']) == CASE / 'rust-lib-core/lib/Cargo.toml'
        target = Path(os.environ.get('CARGO_TARGET_DIR', str(scratch / 'cargo')))
        compile_args = ['--target-dir', str(target)]
        command(['cargo', 'build', *args, *compile_args], evidence / 'build.log')
        return [str(target / 'debug/order-federation-service-core-web-axum')]
    manifest = CASE / 'dotnet-web-aspnet/order-federation-service-web.csproj'
    artifacts = scratch / 'dotnet'
    command(['dotnet', 'build', str(manifest), f'--property:TeaQLRuntimeSourceRoot={REPO}',
             '--property:UseArtifactsOutput=true', f'--property:ArtifactsPath={artifacts}',
             '--property:UseSharedCompilation=false'], evidence / 'build.log')
    restored = {}
    for path in artifacts.rglob('project.assets.json'):
        asset = json.loads(path.read_text())
        restored[Path(asset['project']['restore']['projectPath'])] = asset
    app = restored[manifest]
    lib = CASE / 'dotnet-lib-core/order-federation-service-lib.csproj'
    def references(asset):
        return {Path(name) for name in asset['project']['restore']['frameworks']['net8.0']['projectReferences']}
    names = ['TeaQL.Core', 'TeaQL.DataService', 'TeaQL.Runtime', 'TeaQL.Sql', 'TeaQL.Provider.Sqlite']
    projects = {REPO / 'src' / name / f'{name}.csproj' for name in names}
    tfp = REPO / 'src/TeaQL.TfpEndpoint/TeaQL.TfpEndpoint.csproj'
    assert references(app) == {lib, tfp} and references(restored[lib]) == projects
    for name in [*names, 'TeaQL.TfpEndpoint']:
        selected, = [value for key, value in app['libraries'].items() if key.split('/', 1)[0] == name]
        assert selected['type'] == 'project'
    (evidence / 'dependency-origins.json').write_text(json.dumps(
        {'domain': str(lib), 'nativeProjects': sorted(str(p) for p in {*projects, tfp})}, indent=2) + '\n')
    return ['dotnet', str(artifacts / 'bin/order-federation-service-web/debug/order-federation-service-web.dll')]


def request(address, path, payload, status):
    opener = build_opener(ProxyHandler({}))
    request = Request(address + path, json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
    try:
        response = opener.open(request, timeout=15)
    except HTTPError as error:
        response = error
    with closing(response):
        body = json.load(response)
        assert response.code == status, (path, response.code, body)
        return body


def stored(database):
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as connection:
        assert connection.execute('PRAGMA quick_check').fetchone() == ('ok',)
        return connection.execute('''
            SELECT id, merchant, order_number, fulfillment_status, version
            FROM federated_order_data ORDER BY id
        ''').fetchall()


def verify_requests(runtime, address, database, round_no, previous, current):
    baseline = stored(database)
    home, foreign = baseline
    assert home == (1, 1, ORDER, previous, round_no)
    assert foreign == (2, 2, 'FEDERATION-OTHER', 'ready', 1)
    keys = ('orderNumber', 'fulfillmentStatus') if runtime == 'rust' else ('order_number', 'fulfillment_status')
    def expected(state, version):
        return {'id': 1, keys[0]: ORDER, keys[1]: state, 'version': version}

    query = {'entity': 'FederatedOrder', 'selectItems': ['id', 'orderNumber', 'fulfillmentStatus', 'version'],
             'limitValue': 10, 'commentText': 'Load the merchant dispatch order', 'purposeText': 'review-dispatch'}
    mutation = {'entity': 'FederatedOrder', 'action': 'Update', 'id': 1, 'expectedVersion': round_no,
                'payload': {'fulfillmentStatus': current}, 'comment': 'Advance the merchant dispatch order'}
    evidence = {}
    evidence['before'] = request(address, '/query', query, HTTPStatus.OK)
    assert evidence['before']['data'] == [expected(previous, round_no)]
    evidence['foreignLookup'] = request(address, '/query',
        {**query, 'filterCondition': {'id': {'$eq': 2}}}, HTTPStatus.OK)
    assert evidence['foreignLookup']['data'] == []

    evidence['foreignMutation'] = request(address, '/mutate',
        {**mutation, 'id': 2, 'expectedVersion': 1}, HTTPStatus.CONFLICT)
    blocked = 'TFP_MUTATION_TARGET_UNAVAILABLE' if runtime == 'rust' else 'TFP_UNAVAILABLE'
    assert evidence['foreignMutation']['code'] == blocked
    evidence['forgedOwnership'] = request(address, '/mutate',
        {**mutation, 'payload': {'merchant': 2}}, HTTPStatus.FORBIDDEN)
    assert evidence['forgedOwnership']['code'] in {'TFP_FORBIDDEN_FIELD', 'TFP_POLICY_VIOLATION'}
    assert stored(database) == baseline

    evidence['update'] = request(address, '/mutate', mutation, HTTPStatus.OK)
    assert evidence['update']['affectedRows'] == 1
    evidence['after'] = request(address, '/query', query, HTTPStatus.OK)
    assert evidence['after']['data'] == [expected(current, round_no + 1)]
    evidence['stale'] = request(address, '/mutate', mutation, HTTPStatus.CONFLICT)
    assert evidence['stale'] == evidence['foreignMutation']
    persisted = [(1, 1, ORDER, current, round_no + 1), foreign]
    assert stored(database) == persisted
    return {'round': round_no, 'responses': evidence, 'persistedRows': persisted}


def run_workspace(runtime, evidence, scratch):
    run = build(runtime, scratch, evidence)
    db = evidence / 'orders.sqlite'
    results = []
    for round_no, previous, current in [(1, 'ready', 'packed'), (2, 'packed', 'shipped')]:
        log = evidence / f'host-{round_no}.log'
        with log.open('xb') as output:
            env = dict(os.environ, TEAQL_FEDERATION_DATABASE=str(db), TEAQL_TELEMETRY_ENABLED='false')
            proc = subprocess.Popen(run, cwd=REPO, stdout=output, stderr=subprocess.STDOUT, env=env)
            try:
                deadline = time.monotonic() + 30
                endpoint = None
                while time.monotonic() < deadline and proc.poll() is None:
                    endpoint = re.search(r'^FEDERATION_READY=(http://127\.0\.0\.1:\d+)$', log.read_text(), re.MULTILINE)
                    if endpoint:
                        break
                    time.sleep(.05)
                assert endpoint and proc.poll() is None, f'{log.name} did not announce a ready loopback host'
                results.append(verify_requests(runtime, endpoint[1], db, round_no, previous, current))
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
    assert not list(evidence.glob('orders.sqlite-*'))
    with (evidence / 'business.json').open('x') as file:
        json.dump({'runtime': runtime, 'rounds': results, 'sameDatabase': True,
                   'sourceSQLiteSHA256': hashlib.sha256(db.read_bytes()).hexdigest()}, file, indent=2)
        file.write('\n')
    print(f'PASS {runtime} order federation: loopback query, guarded update and restart; two merchants and two versions checked')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', required=True, choices=['dotnet', 'rust'])
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix='teaql-federation-workspace-') as owned:
        scratch = Path(owned).resolve()
        evidence = args.output.resolve() if args.output is not None else scratch / 'evidence'
        evidence.mkdir()
        run_workspace(args.runtime, evidence, scratch)


if __name__ == '__main__':
    main()
