#!/usr/bin/env python3
"""Source-only Checker qualification; retain receipts, never build trees."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parents[1]
UNSAFE_CHILDREN = []


def source_hashes():
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=REPO
    ).decode().split("\0")
    hashes = {}
    for name in sorted(set(names) - {""}):
        source = REPO / name
        assert source.is_file() and not source.is_symlink(), f"missing or non-regular source: {name}"
        assert source.resolve() == source, f"source traverses a link: {name}"
        hashes[name] = hashlib.sha256(source.read_bytes()).hexdigest()
    return hashes


def run(command, env, log):
    global UNSAFE_CHILDREN
    marker = "TEAQL_NATURAL_PROCESS_OWNER=" + str(uuid.uuid4())
    owned_env = dict(env, TEAQL_NATURAL_PROCESS_OWNER=marker.split("=",1)[1])
    with log.open("w") as output:
        child = subprocess.Popen(command, cwd=REPO, env=owned_env, stdout=output,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        def owned_members():
            members = []
            for entry in Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    fields = (entry / "stat").read_text().rsplit(") ",1)[1].split()
                    if fields[0] == "Z" or int(fields[2]) != child.pid or int(fields[3]) != child.pid:
                        continue
                    if marker.encode() in (entry / "environ").read_bytes().split(b"\0"):
                        members.append(int(entry.name))
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    continue
            return members
        try:
            return child.wait(timeout=1800)
        finally:
            # Do not delete outputs while a failed command's grandchildren
            # still use them. Only this command's marked, private group is owned.
            for sig in [signal.SIGTERM, signal.SIGKILL]:
                members = owned_members()
                if not members:
                    break
                for pid in members:
                    try:
                        os.kill(pid,sig)
                    except ProcessLookupError:
                        pass
                deadline = time.monotonic() + 10
                while owned_members() and time.monotonic() < deadline:
                    time.sleep(0.05)
            child.wait()
            UNSAFE_CHILDREN = owned_members()
            assert not UNSAFE_CHILDREN, "owned children remain; keep their outputs"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--all-targets", action="store_true")
    parser.add_argument("--example", action="store_true", help="Also run generated SQLite Q/E/Save twice")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix="teaql-natural-rust-build-"))
    status = 1
    report = {"passed": False, "scope": "local-source", "runs": [], "buildOutput": str(scratch)}
    try:
        report["revision"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
        report["sourceHashes"] = source_hashes()
        report["rustc"] = subprocess.check_output([str(Path.home() / ".cargo/bin/rustc"), "--version"], text=True).strip()
        report["freeBytesBefore"] = shutil.disk_usage(scratch).free
        assert report["freeBytesBefore"] > 8 * 1024**3, "insufficient owned build space"
        env = dict(os.environ, CARGO_TARGET_DIR=str(scratch / "target"),
                   CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="4",
                   CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
        report["buildEnvironment"] = {key: env[key] for key in
            ["CARGO_TARGET_DIR", "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_TEST_DEBUG"]}
        cargo = str(Path.home() / ".cargo/bin/cargo")
        commands = [[cargo, "test", "--locked", "-p", "teaql-runtime", "--test", "natural_messages", "--", "--nocapture"]]
        if args.all_targets:
            commands.append([cargo, "test", "--locked", "-p", "teaql-runtime", "-p", "teaql-core", "-p", "teaql-macros", "--all-targets", "--no-fail-fast"])
        for index, command in enumerate(commands, 1):
            log = output / f"native-{index}.log"
            code = run(command, env, log)
            summaries = re.findall(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored", log.read_text())
            report["runs"].append(dict(command=command, exitCode=code, log=log.name,
                logSha256=hashlib.sha256(log.read_bytes()).hexdigest(),
                passed=sum(int(s[0]) for s in summaries), failed=sum(int(s[1]) for s in summaries),
                ignored=sum(int(s[2]) for s in summaries), targets=len(summaries)))
            assert code == 0 and summaries, f"native gate failed: {log.name}"
        if args.example:
            pin_command = [shutil.which("node") or "/home/philip/.nvm/versions/node/v24.19.0/bin/node", "--test", str(REPO / "scripts/pin-local-example-library.test.cjs")]
            pin_code = run(pin_command, env, output / "pin-tests.log")
            assert pin_code == 0, "local manifest pin guards failed"
            report["metadataPinTests"] = dict(command=pin_command, exitCode=pin_code, tests=7, logSha256=hashlib.sha256((output / "pin-tests.log").read_bytes()).hexdigest())
            from natural_message_consumer import verify_consumer
            report.update(verify_consumer(scratch / "automatic-consumer", output, env, run))
        assert source_hashes() == report["sourceHashes"], "source changed during qualification"
        report["passed"] = True
        status = 0
    except BaseException as error:
        report["failure"] = f"{type(error).__name__}: {error}"
    finally:
        report["removedBuildBytes"] = sum(p.stat().st_size for p in scratch.rglob("*") if p.is_file() and not p.is_symlink())
        if UNSAFE_CHILDREN:
            report["unsafeBuildRetention"] = dict(children=UNSAFE_CHILDREN, reason="owned children have not exited")
        else:
            shutil.rmtree(scratch)
        report["buildOutputRemoved"] = not scratch.exists()
        report["retainedFileHashes"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in output.iterdir() if p.is_file()}
        (output / "receipt.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "sourceHashes"}, indent=2))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
