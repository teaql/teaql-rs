"""Independently read the enrollment database and bind the native receipts."""

import hashlib
import json
from pathlib import Path
import sqlite3
import sys


def readonly(path):
    return sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)


def rows(directory):
    with readonly(directory / 'enrollment.sqlite') as db:
        assert db.execute('PRAGMA quick_check').fetchone() == ('ok',)
        result = {
            'platforms': db.execute('SELECT id,version,name FROM platform_data').fetchall(),
            'schoolTypes': db.execute('SELECT id,version,code FROM school_type_data ORDER BY id').fetchall(),
            'schools': db.execute('SELECT id,version,platform,school_type,name FROM school_data').fetchall(),
            'students': db.execute('SELECT id,version,school,name FROM student_data ORDER BY id').fetchall(),
        }
    assert result == {
        'platforms': [(1, 1, 'District Enrollment')],
        'schoolTypes': [(1001, 1, 'PRIMARY'), (1002, 1, 'SECONDARY')],
        'schools': [(1, 1, 1, 1001, 'Riverside Primary')],
        'students': [(1, 1, 1, 'Alice'), (2, 1, 1, 'Bob')],
    }, result
    return result


def qualify(directory, runtime):
    installation = directory / 'install.sqlite'
    if installation.exists():
        with readonly(installation) as db:
            assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []
    assert (directory / 'install.log').read_text().splitlines().count('ENROLLMENT_INSTALL_READY') == 1
    metadata = json.loads((directory / 'dependencies.json').read_text())
    runtime_metadata = json.loads((directory / 'runtime-dependencies.json').read_text())
    runtime_packages = {package['name']: package for package in runtime_metadata['packages']}
    local_modules = {}
    for package in metadata['packages']:
        name = package['name']
        if name.startswith('teaql-'):
            manifest = Path(package['manifest_path']).resolve()
            expected = runtime_packages[name]
            assert package['source'] is None and expected['source'] is None
            assert package['version'] == expected['version']
            assert manifest == Path(expected['manifest_path']).resolve() == runtime / name / 'Cargo.toml'
            local_modules[name] = str(manifest.relative_to(runtime))
        elif name == 'school-enrollment-core':
            assert Path(package['manifest_path']).resolve() == (
                runtime / 'examples/src/bin/school_enrollment/rust-lib-core/lib/Cargo.toml'
            )
    assert {'teaql-core', 'teaql-runtime', 'teaql-sql', 'teaql-provider-sqlite'} <= local_modules.keys()
    results = []
    for round_name, audits in [('first', 3), ('restart', 0)]:
        log = (directory / (round_name + '.log')).read_text().splitlines()
        frames = [json.loads(line.split('=', 1)[1]) for line in log if line.startswith('ENROLLMENT_RESULT=')]
        assert frames == [dict(round=round_name, bootstrapAudits=audits, businessAudits=audits,
                               schoolId=1, students=['Alice', 'Bob'], invalidPupilRejected=bool(audits))]
        results.append(frames[0])
    assert (directory / 'rows-first.json').read_bytes() == (directory / 'rows-restart.json').read_bytes()
    calibration = (directory / 'calibration.log').read_text()
    assert all(text in calibration for text in ['save(&context) succeeded', 'My School', 'Alice', 'Bob'])
    names = ['install.log', 'first.log', 'restart.log', 'enrollment.sqlite',
             'rows-first.json', 'rows-restart.json', 'calibration.log', 'dependencies.json', 'runtime-dependencies.json']
    result = dict(schema='teaql.rust-enrollment-business/v1', passed=True, sameDatabase=True, passiveInstall=True,
                  rounds=results, persisted=rows(directory), localRuntimeModules=local_modules,
                  evidenceSHA256={name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in names})
    with (directory / 'business-receipt.json').open('x') as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write('\n')


if __name__ == '__main__':
    directory = Path(sys.argv[1])
    if sys.argv[2] in {'first', 'restart'}:
        print(json.dumps(rows(directory), sort_keys=True))
    else:
        qualify(directory, Path(sys.argv[2]).resolve())
