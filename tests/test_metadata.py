import json
from pathlib import Path
import tempfile
import unittest
from bank.pipeline import load_config
from bank.adapters import TABLES


class MetadataTest(unittest.TestCase):
    def test_complete_explicit_contracts(self):
        config = load_config()
        self.assertEqual([r['source_table'] for r in config], list(TABLES))
        self.assertTrue(all(r['enabled'] for r in config))

    def test_unknown_table_rejected(self):
        config = load_config()
        config[0]['source_table'] = 'transactions; DROP SCHEMA core'
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'bad.json'
            path.write_text(json.dumps(config))
            with self.assertRaises(ValueError):
                load_config(path)
