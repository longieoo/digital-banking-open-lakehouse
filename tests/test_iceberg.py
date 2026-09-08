"""Real Iceberg format tests with isolated persistent SQL catalog, not REST acceptance."""
import importlib.util
from pathlib import Path
import tempfile
import os
from contextlib import ExitStack
import unittest


@unittest.skipUnless(importlib.util.find_spec('pyiceberg'), 'PyIceberg test dependencies unavailable')
class IcebergTest(unittest.TestCase):
    def test_snapshots_retry_evolution_time_travel_and_restart(self):
        from pyiceberg.catalog import load_catalog
        from pyiceberg.types import StringType
        from bank.adapters import IcebergBronze
        with ExitStack() as stack:
            tmp = stack.enter_context(tempfile.TemporaryDirectory())
            root = Path(tmp)
            props = {'type': 'sql', 'uri': 'sqlite:///' + (root/'catalog.db').as_posix(),
                     'warehouse': Path(os.path.relpath(root/'warehouse')).as_posix()}
            catalog = load_catalog('isolated_test', **props)
            stack.callback(catalog.engine.dispose)
            bronze = IcebergBronze(catalog, 'transactions')
            row = dict(record_id='SYN-T1', source_updated_at='2026-01-01T00:00:00Z',
                business_date='2026-01-01', payload='{"amount":"2.00"}', batch_id='batch1',
                source_system='core', source_table='transactions')
            first = bronze.commit([row])
            self.assertEqual(bronze.commit([row]), first)
            self.assertEqual(bronze.validate([row]), 1)
            second = {**row, 'source_updated_at':'2026-01-02T00:00:00Z', 'payload':'{"amount":"3.00"}'}
            bronze.commit([second])
            self.assertEqual(bronze.table.scan().to_arrow().num_rows, 2)
            self.assertEqual(bronze.table.scan(snapshot_id=int(first)).to_arrow().num_rows, 1)
            with self.assertRaises(ValueError):
                bronze.commit([{**row, 'payload':'conflicting'}])
            with bronze.table.update_schema() as update:
                update.add_column('source_note', StringType())
            self.assertIn('source_note', bronze.table.schema().column_names)
            self.assertEqual(len(bronze.table.metadata.snapshots), 2)
            self.assertGreater(bronze.table.inspect.files().num_rows, 0)
            self.assertGreater(bronze.table.inspect.manifests().num_rows, 0)
            restarted = load_catalog('isolated_test', **props)
            self.assertEqual(restarted.load_table('bronze.transactions').scan().to_arrow().num_rows, 2)
            restarted.engine.dispose()
            catalog.engine.dispose()
