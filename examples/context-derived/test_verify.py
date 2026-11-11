import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("verify", Path(__file__).with_name("verify-local.py"))
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class FixtureGuard(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="context-guard-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "model.xml").write_text("<root/>")
        (self.root / "lib").mkdir()
        (self.root / "lib/Cargo.toml").write_text("[package]\n")
        (self.root / "generated-files.json").write_text(json.dumps({"lib/Cargo.toml": verify.sha(self.root / "lib/Cargo.toml")}))

    def test_exact_fixture_is_accepted(self):
        self.assertEqual(len(verify.fixture(self.root)), 3)

    def test_extra_file_is_rejected(self):
        (self.root / "lib/hidden-runtime.rs").touch()
        with self.assertRaises(AssertionError): verify.fixture(self.root)

    def test_missing_file_is_rejected(self):
        (self.root / "lib/Cargo.toml").unlink()
        with self.assertRaises(AssertionError): verify.fixture(self.root)

    def test_edited_file_is_rejected(self):
        (self.root / "lib/Cargo.toml").write_text("edited")
        with self.assertRaises(AssertionError): verify.fixture(self.root)

    def test_symlink_is_rejected(self):
        (self.root / "linked").symlink_to(self.root / "model.xml")
        with self.assertRaises(AssertionError): verify.fixture(self.root)


if __name__ == "__main__": unittest.main()
