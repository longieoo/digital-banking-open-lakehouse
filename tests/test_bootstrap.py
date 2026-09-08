import unittest
from pathlib import Path
import tomllib


class BootstrapTest(unittest.TestCase):
    def test_package_metadata(self):
        config = tomllib.loads(Path("pyproject.toml").read_text())
        self.assertEqual(config["project"]["name"], "digital-banking-open-lakehouse")
