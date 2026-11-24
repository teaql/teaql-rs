"""Automatic generated consumer. Only owned dependency manifests are adapted."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "examples/natural-messages"
CRATES = ("teaql-core", "teaql-runtime", "teaql-macros", "teaql-data-service", "teaql-sql", "teaql-provider-sqlite")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixture_hashes():
    root = EXAMPLE / "automatic-fixture"
    manifest = json.loads((root / "generated-files.json").read_text())
    assists = json.loads((root / "assist-files.json").read_text())
    assert len(manifest) == 21 and len(assists) == 5
    expected = set(manifest) | set(assists) | {"model.xml", "evaluation.json", "generated-files.json", "assist-files.json", "generation.json"}
    actual = set()
    for file in root.rglob("*"):
        assert not file.is_symlink(), "generated fixture contains a link"
        if file.is_file():
            actual.add(file.relative_to(root).as_posix())
    assert actual == expected, "unexpected generated fixture inventory"
    for name, digest in {**manifest, **assists}.items():
        assert not Path(name).is_absolute() and ".." not in Path(name).parts
        file = root / name
        assert file.resolve() == file and sha(file) == digest, name
    origin = json.loads((root / "generation.json").read_text())
    assert sha(root / "model.xml") == origin["modelSha256"]
    assert not origin["implementationHandEdited"] and not origin["publicPublication"]
    assert not json.loads((root / "evaluation.json").read_text())["errors"]
    return {name: sha(root / name) for name in sorted(expected)}


def stage(owned, output, env, run):
    owned.mkdir(parents=True, exist_ok=False)
    fixture = EXAMPLE / "automatic-fixture"
    before = fixture_hashes()
    library, consumer = owned / "generated-lib", owned / "consumer"
    shutil.copytree(fixture / "lib", library)
    consumer.mkdir()
    shutil.copytree(EXAMPLE / "src", consumer / "src")
    shutil.copyfile(EXAMPLE / "Cargo.lock", consumer / "Cargo.lock")
    manifest = (EXAMPLE / "Cargo.toml").read_text()
    assert manifest.count('path = "automatic-fixture/lib"') == 1
    manifest = manifest.replace('path = "automatic-fixture/lib"', 'path = "../generated-lib"')
    for crate in CRATES:
        original = f'{crate} = {{ path = "../../{crate}" }}'
        assert manifest.count(original) == 1, crate
        manifest = manifest.replace(original, f'{crate} = {{ path = {json.dumps(str(REPO / crate))} }}')
    (consumer / "Cargo.toml").write_text(manifest)
    command = [shutil.which("node") or "/home/philip/.nvm/versions/node/v24.19.0/bin/node",
               str(REPO / "scripts/pin-local-example-library.cjs"), str(library), str(REPO), str(owned)]
    log = output / "pin-metadata.log"
    assert run(command, env, log) == 0, "local metadata pin failed"
    pin = json.loads(log.read_text())
    assert pin["implementationFilesUnchanged"] and not pin["sourceFixtureEdited"]
    # Fresh output must already target the correct SQLite ABI; no historical-driver escape.
    assert not pin["sqliteDriverChanged"], "fix the upstream generator's fresh SQLite ABI instead"
    assert before == fixture_hashes()
    return consumer, pin, before


def verify_consumer(owned, output, env, run):
    application = (EXAMPLE / "src/main.rs").read_text()
    for forbidden in ["set_checker_presentation_model(", "CheckerPresentationModel::", "set_checker_registry(", "EntryCheckerLogic", "TypedEntityChecker"]:
        assert forbidden not in application, f"automatic composition cannot be replaced in the application: {forbidden}"
    consumer, pin, before = stage(owned, output, env, run)
    cargo = str(Path.home() / ".cargo/bin/cargo")
    command = [cargo, "build", "--offline", "--locked", "--manifest-path", str(consumer / "Cargo.toml")]
    assert run(command, env, output / "example-build.log") == 0, "automatic example compilation failed"
    metadata = json.loads(subprocess.check_output([cargo, "metadata", "--offline", "--locked", "--format-version", "1",
        "--manifest-path", str(consumer / "Cargo.toml")], cwd=REPO, env=env, text=True, timeout=120))
    dependencies = {}
    for crate in CRATES:
        paths = [package["manifest_path"] for package in metadata["packages"] if package["name"] == crate]
        assert paths == [str(REPO / crate / "Cargo.toml")], f"not local source: {crate}"
        dependencies[crate] = paths[0]
    library = next(package for package in metadata["packages"] if package["name"] == "checker-natural-message-service-core")
    assert Path(library["manifest_path"]) == owned / "generated-lib/Cargo.toml"
    assert (consumer / "Cargo.lock").read_bytes() == (EXAMPLE / "Cargo.lock").read_bytes(), "committed consumer lock drift"
    database = output / "example.sqlite"
    run_env = dict(env, TEAQL_NATURAL_MESSAGES_DB=str(database), TEAQL_TELEMETRY_ENABLED="false",
                   TEAQL_LOG_ENDPOINT=str(output / "ordinary.log"))
    for key in ["TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS", "TEAQL_SQL_DEBUG_ENDPOINT", "TEAQL_AUDIT_DEBUG_ENDPOINT"]:
        run_env.pop(key, None)
    rounds = []
    messages = ["The name of the 2nd entry is required.", "请填写第2项明细的名称。",
                "The name of the 2nd entry must not exceed 100 Unicode scalar values; its current length is 101.", "Private [REDACTED]"]
    for attempt in [1, 2]:
        log = output / f"example-{attempt}.log"
        code = run([str(Path(env["CARGO_TARGET_DIR"]) / "debug/natural-messages-example")], run_env, log)
        assert code == 0, f"automatic example round {attempt} failed"
        text = log.read_text()
        responses = [json.loads(line.split("=", 1)[1]) for line in text.splitlines() if line.startswith("RUST_NATURAL_CHECKER_RESPONSE=")]
        facts = [json.loads(line.split("=", 1)[1]) for line in text.splitlines() if line.startswith("RUST_NATURAL_CHECKER_SQLITE=")]
        assert len(responses) == 4 and len(facts) == 1, "missing acceptance frames"
        for response, message in zip(responses, messages):
            assert len(response["violations"]) == 1
            result = response["violations"][0]
            assert result["entityType"] == "Entry" and result["instancePath"] == "/entryList/1/name"
            assert result["message"] == message
        assert "密钥🔑" + "x" * 98 not in json.dumps(responses, ensure_ascii=False)
        fact = facts[0]
        for name in ["passed", "typedQ", "typedE", "automaticMetadata", "originalGeneratedChecker", "unchangedParent"]:
            assert fact[name] is True, name
        for name, expected in {"responses": 4, "customerRendererCalls": 1, "updatedAudits": 1,
                               "providerCallsOnRejection": 0, "transactionCallsOnRejection": 0,
                               "version": 2, "originalScalarLength": 101}.items():
            assert fact[name] == expected, name
        (output / f"responses-{attempt}.json").write_text(json.dumps(responses, ensure_ascii=False, indent=2) + "\n")
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            assert connection.execute("pragma quick_check").fetchone()[0] == "ok"
            rows = connection.execute("SELECT name,version FROM entry_data ORDER BY id").fetchall()
        assert rows == [("accepted", 2), ("second", 1)] * attempt, rows
        rounds.append(dict(round=attempt, exitCode=code, log=log.name, logSha256=sha(log), facts=fact, sqliteRows=rows))
        assert fixture_hashes() == before
    return dict(generatedFixture=json.loads((EXAMPLE / "automatic-fixture/generated-files.json").read_text()),
                fixtureInputHashes=before, generation=json.loads((EXAMPLE / "automatic-fixture/generation.json").read_text()),
                metadataOverride=pin, exampleBuild=dict(command=command, exitCode=0), localDependencies=dependencies,
                exampleRounds=rounds, databaseSha256=sha(database), sameDatabase=True, automaticComposition=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare-lock", action="store_true", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    spec = importlib.util.spec_from_file_location("natural_gate", REPO / "scripts/verify-natural-messages.py")
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    scratch = Path(tempfile.mkdtemp(prefix="teaql-natural-lock-"))
    report = dict(prepared=False, sourceQualification=False, publicPublication=False)
    status = 1
    try:
        assert shutil.disk_usage(scratch).free > 2 * 1024**3
        hashes = gate.source_hashes()
        env = dict(os.environ, CARGO_TARGET_DIR=str(scratch / "target"), CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="4")
        consumer, pin, _ = stage(scratch / "work", output, env, gate.run)
        command = [str(Path.home() / ".cargo/bin/cargo"), "generate-lockfile", "--offline", "--manifest-path", str(consumer / "Cargo.toml")]
        code = gate.run(command, env, output / "prepare-lock.log")
        assert code == 0, "lock preparation failed"
        assert gate.source_hashes() == hashes, "source changed while preparing the lock"
        shutil.copyfile(consumer / "Cargo.lock", EXAMPLE / "Cargo.lock")
        after = gate.source_hashes()
        assert [name for name in after if after[name] != hashes.get(name)] in [[], ["examples/natural-messages/Cargo.lock"]]
        report.update(prepared=True, command=command, exitCode=code, metadataOverride=pin,
                      lockSha256=sha(EXAMPLE / "Cargo.lock"), logSha256=sha(output / "prepare-lock.log"))
        status = 0
    except Exception as error:
        report["error"] = str(error)
    finally:
        report["removedBuildBytes"] = sum(file.stat().st_size for file in scratch.rglob("*") if file.is_file())
        if gate.UNSAFE_CHILDREN:
            report["unsafeBuildRetention"] = str(scratch)
        else:
            shutil.rmtree(scratch)
        report["buildOutputRemoved"] = not scratch.exists()
        (output / "receipt.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
