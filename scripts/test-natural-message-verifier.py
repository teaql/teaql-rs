#!/usr/bin/env python3
"""Exercise owned-child cleanup independently of Cargo or a database."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("natural_gate", Path(__file__).with_name("verify-natural-messages.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class Lifecycle(unittest.TestCase):
    def test_success_status_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="teaql-natural-runner-test-") as directory:
            self.assertEqual(gate.run([sys.executable,"-c","print('success')"],os.environ,Path(directory)/"run.log"),0)
            self.assertEqual((Path(directory)/"run.log").read_text(),"success\n")

    def test_failure_status_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="teaql-natural-runner-test-") as directory:
            self.assertEqual(gate.run([sys.executable,"-c","raise SystemExit(7)"],os.environ,Path(directory)/"run.log"),7)

    def test_orphaned_owned_grandchild_stops_before_run_returns(self):
        with tempfile.TemporaryDirectory(prefix="teaql-natural-runner-test-") as directory:
            log = Path(directory)/"run.log"
            code = "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(300)']); print(p.pid,flush=True)"
            self.assertEqual(gate.run([sys.executable,"-c",code],os.environ,log),0)
            pid = int(log.read_text().strip())
            stat = Path(f"/proc/{pid}/stat")
            self.assertTrue(not stat.exists() or stat.read_text().rsplit(") ",1)[1].split()[0] == "Z")
            self.assertEqual(gate.UNSAFE_CHILDREN,[])


if __name__ == "__main__":
    unittest.main()
