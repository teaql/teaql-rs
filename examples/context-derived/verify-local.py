"""Verify the complete generated library against this repository's six crates.

Build outputs are disposable; reports and the twice-used SQLite DB are retained.
This is a source qualification gate, not a published-package acceptance test.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import tempfile

EXAMPLE = Path(__file__).resolve().parent
REPO = EXAMPLE.parents[1]
CRATES = ("teaql-core", "teaql-runtime", "teaql-macros", "teaql-data-service", "teaql-sql", "teaql-provider-sqlite")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(root=None):
    root = root or EXAMPLE / "fixture"
    manifest = json.loads((root / "generated-files.json").read_text())
    expected = set(manifest) | {"model.xml", "generated-files.json"}
    actual = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise AssertionError(f"generated fixture contains symlink: {path}")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    assert actual == expected, (actual - expected, expected - actual)
    for name, digest in manifest.items():
        assert sha(root / name) == digest, f"generated file changed: {name}"
    return {name: sha(root / name) for name in sorted(expected)}


def sources():
    paths = [EXAMPLE / "Cargo.toml", EXAMPLE / "Cargo.lock", EXAMPLE / "src/main.rs", Path(__file__).resolve(), REPO / "scripts/pin-local-example-library.cjs"]
    for crate in CRATES:
        paths.append(REPO / crate / "Cargo.toml")
        paths.extend((REPO / crate / "src").rglob("*.rs"))
    return {path.relative_to(REPO).as_posix(): sha(path) for path in sorted(paths)}


def stage(scratch, output, env):
    """Adapt only owned dependency metadata; preserve the sealed source fixture."""
    library, consumer = scratch / "generated-lib", scratch / "consumer"
    shutil.copytree(EXAMPLE / "fixture/lib", library)
    consumer.mkdir()
    shutil.copytree(EXAMPLE / "src", consumer / "src")
    shutil.copyfile(EXAMPLE / "Cargo.lock", consumer / "Cargo.lock")
    manifest = (EXAMPLE / "Cargo.toml").read_text()
    assert manifest.count('path = "fixture/lib"') == 1
    manifest = manifest.replace('path = "fixture/lib"', 'path = "../generated-lib"')
    for crate in CRATES:
        original = f'{crate} = {{ path = "../../{crate}" }}'
        assert manifest.count(original) == 1, crate
        manifest = manifest.replace(original, f'{crate} = {{ path = {json.dumps(str(REPO / crate))} }}')
    (consumer / "Cargo.toml").write_text(manifest)
    command = [shutil.which("node") or "/home/philip/.nvm/versions/node/v24.19.0/bin/node",
               str(REPO / "scripts/pin-local-example-library.cjs"), str(library), str(REPO), str(scratch)]
    log = output / "pin-metadata.log"
    assert run(command, env, log) == 0, "local metadata pin failed"
    pin = json.loads(log.read_text())
    assert pin["implementationFilesUnchanged"] and not pin["sourceFixtureEdited"]
    assert not pin["sqliteDriverChanged"], "fresh fixture must use the selected SQLite ABI"
    return consumer, pin


def run(command, env, log):
    with log.open("w") as output:
        child = subprocess.Popen(command, cwd=EXAMPLE, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return child.wait(timeout=900)
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--unit-tests", action="store_true", help="Also run core/runtime/macro all-target tests with disposable outputs")
    parser.add_argument("--prepare-lock", action="store_true", help="Prepare only the selected-version consumer lock; does not qualify tests")
    args = parser.parse_args()
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="teaql-context-evidence-"))
    output.mkdir(parents=True, exist_ok=True)
    assert not any(output.iterdir()), "Use a new evidence directory; never overwrite a previous run"
    report = dict(passed=False, sourceQualification=True, rounds=[])
    scratch = Path(tempfile.mkdtemp(prefix="teaql-context-build-"))
    status = 1
    try:
        report.update(fixture=fixture(), sourceHashes=sources(), revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip())
        cargo = shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo")
        env = dict(os.environ, CARGO_TARGET_DIR=str(scratch / "target"), CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="4", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0", TEAQL_CONTEXT_FIX_DB=str(output / "fixture.sqlite"))
        report["buildOutput"] = str(scratch)
        consumer, report["metadataOverride"] = stage(scratch, output, env)
        manifest = consumer / "Cargo.toml"
        if args.prepare_lock:
            command = [cargo, "generate-lockfile", "--offline", "--manifest-path", str(manifest)]
            report["prepareLockCommand"] = command
            assert run(command, env, output / "prepare-lock.log") == 0, "lock preparation failed"
            assert sources() == report["sourceHashes"] and fixture() == report["fixture"]
            shutil.copyfile(consumer / "Cargo.lock", EXAMPLE / "Cargo.lock")
            report.update(prepared=True, sourceQualification=False, lockSha256=sha(EXAMPLE / "Cargo.lock"))
            return 0
        if args.unit_tests:
            command = [cargo, "test", "--locked", "--manifest-path", str(REPO / "Cargo.toml"), "-p", "teaql-runtime", "-p", "teaql-core", "-p", "teaql-macros", "--all-targets"]
            code = run(command, env, output / "unit.log")
            summaries = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored", (output / "unit.log").read_text())
            report["unit"] = dict(command=command, exitCode=code, passed=sum(int(s[0]) for s in summaries), failed=sum(int(s[1]) for s in summaries), ignored=sum(int(s[2]) for s in summaries), targets=len(summaries))
            assert code == 0 and report["unit"]["passed"] >= 750 and report["unit"]["failed"] == 0, "unit tests failed; see unit.log"
        command = [cargo, "build", "--offline", "--locked", "--manifest-path", str(manifest)]
        report["buildCommand"] = command
        assert run(command, env, output / "build.log") == 0, "build failed; see build.log"
        metadata = json.loads(subprocess.check_output([cargo, "metadata", "--offline", "--locked", "--format-version", "1", "--manifest-path", str(manifest)], cwd=EXAMPLE, env=env, text=True))
        assert (consumer / "Cargo.lock").read_bytes() == (EXAMPLE / "Cargo.lock").read_bytes()
        report["dependencies"] = {}
        for crate in CRATES:
            packages = [p for p in metadata["packages"] if p["name"] == crate]
            assert len(packages) == 1 and Path(packages[0]["manifest_path"]).resolve() == REPO / crate / "Cargo.toml", f"not local source: {crate}"
            report["dependencies"][crate] = packages[0]["manifest_path"]
        for attempt in (1, 2):
            log = output / f"round-{attempt}.log"
            code = run([str(scratch / "target/debug/context-derived-example")], env, log)
            report["rounds"].append(dict(round=attempt, exitCode=code, log=log.name))
            assert code == 0 and "RUST_CONTEXT_FIX_SQLITE_PASS" in log.read_text(), f"round {attempt} failed"
            assert "RUST_CONTEXT_FIX_EXTENSIONS_PASS dynamic_only=true readonly_noop=true equal_dynamic_noop=true" in log.read_text(), "missing extension acceptance cases"
            with sqlite3.connect(f"file:{output / 'fixture.sqlite'}?mode=ro", uri=True) as connection:
                rows = connection.execute("SELECT name,last_ip,source_ip,changed_by,opened_by,version FROM entry_data ORDER BY id").fetchall()
                assert any(r[:5] == ("accepted-first", "ip-D", "ip-C", "actor-D", "actor-C") for r in rows)
                report["rounds"][-1]["sqliteRows"] = len(rows)
            assert fixture() == report["fixture"] and sources() == report["sourceHashes"], "source/fixture changed during verification"
        report.update(passed=True, sameDatabase=True, databaseSha256=sha(output / "fixture.sqlite"))
        status = 0
    except BaseException as error:
        report["error"] = str(error)
        print(f"FAIL: {error}")
    finally:
        report["removedBuildBytes"] = sum(p.stat().st_size for p in scratch.rglob("*") if p.is_file())
        shutil.rmtree(scratch)
        report["buildOutputRemoved"] = not scratch.exists()
        (output / "receipt.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(dict(passed=report["passed"], evidence=str(output), removedBuildBytes=report["removedBuildBytes"])))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
