import tempfile
from pathlib import Path
import unittest

from bank.exporter import collect, prometheus, ReadOnlyState
from bank.ingestion import State


class ExporterTest(unittest.TestCase):
    def test_missing_state_stays_missing_and_reports_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = collect(root/'missing.db',root/'releases')
            self.assertFalse(data['ready'])
            self.assertEqual(data['ingestion']['available'],0)
            self.assertFalse((root/'missing.db').exists())
            self.assertIn('bank_ingestion_available 0',prometheus(data))

    def test_reads_without_migrations_and_rejects_pointer_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root/'control.db'
            State(path)
            self.assertEqual(ReadOnlyState(path).manifests(),[])
            releases = root/'releases'
            releases.mkdir()
            (releases/'current.json').write_text('{"release_id":"../secret","manifest":"../secret"}')
            data = collect(path,releases)
            self.assertEqual(data['ingestion']['available'],1)
            self.assertEqual(data['release']['available'],0)
            self.assertNotIn('secret',str(data))
