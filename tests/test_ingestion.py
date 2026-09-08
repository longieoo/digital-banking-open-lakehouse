import json
from pathlib import Path
import tempfile
import unittest
from bank.ingestion import FileRaw, MemorySource, SQLiteBronze, State, Watermark, ingest, transaction


class IngestionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.state = State(root/'state.db')
        self.raw = FileRaw(root/'raw')
        self.bronze = SQLiteBronze(root/'bronze.db')
        self.rows = [dict(transaction_id=f'T{i}', updated_at='2026-01-02T00:00:00+00:00',
                          business_date='2026-01-01', amount='2.00') for i in range(5)]
        self.source = MemorySource(self.rows)

    def run_batch(self, **kw):
        return ingest(self.source, self.raw, self.bronze, self.state, **kw)

    def count(self):
        with transaction(self.bronze.path) as db:
            return db.execute('SELECT count(*) FROM versions').fetchone()[0]

    def test_timestamp_ties_pagination_and_noop(self):
        self.assertEqual([self.run_batch(limit=2)['count'] for _ in range(4)], [2, 2, 1, 0])
        self.assertEqual(self.count(), 5)
        self.assertEqual(self.state.watermark('core.transactions').record_id, 'T4')

    def test_failure_retry_raw_reuse(self):
        with self.assertRaises(IOError):
            self.run_batch(fail_at='bronze')
        self.assertEqual(self.state.watermark('core.transactions'), Watermark())
        manifest = self.state.manifests()[0]
        self.assertEqual((manifest['status'], manifest['raw_written_count']), ('FAILED', 5))
        file = next(self.raw.root.rglob('*.json'))
        modified = file.stat().st_mtime_ns
        self.run_batch()
        self.assertEqual(modified, file.stat().st_mtime_ns)
        self.assertEqual(self.count(), 5)
        self.assertEqual(self.state.manifests()[0]['attempts'], 2)

    def test_post_commit_failure_is_idempotent(self):
        with self.assertRaises(IOError):
            self.run_batch(fail_at='after_commit')
        self.assertEqual(self.count(), 5)
        self.assertEqual(self.state.watermark('core.transactions'), Watermark())
        self.run_batch()
        self.assertEqual(self.count(), 5)

    def test_backfill_preserves_forward_state(self):
        self.run_batch(limit=2)
        before = self.state.watermark('core.transactions')
        self.run_batch(dates=('2026-01-01', '2026-01-01'))
        self.run_batch(dates=('2026-01-01', '2026-01-01'))
        self.assertEqual(self.state.watermark('core.transactions'), before)
        self.assertEqual(self.count(), 5)

    def test_raw_and_extract_failure_sanitized(self):
        with self.assertRaises(IOError):
            self.run_batch(fail_at='raw')
        self.assertEqual(self.count(), 0)
        class Broken:
            def extract(self, *args):
                raise ConnectionError('password=NEVER_PRINT')
        self.source = Broken()
        with self.assertRaises(ConnectionError):
            self.run_batch()
        self.assertNotIn('NEVER_PRINT', json.dumps(self.state.manifests()))
        self.assertEqual(self.state.watermark('core.transactions'), Watermark())

    def test_versions_duplicates_and_conflicts(self):
        self.rows.append(dict(self.rows[0]))
        self.run_batch()
        self.assertEqual(self.count(), 5)
        self.rows[0] = {**self.rows[0], 'amount': '3.00', 'updated_at': '2026-01-03T00:00:00Z'}
        self.assertEqual(self.run_batch()['count'], 1)
        self.assertEqual(self.count(), 6)
        self.rows[0]['amount'] = '9.00'
        with self.assertRaises(ValueError):
            self.run_batch(dates=('2026-01-01', '2026-01-01'))

    def test_corrupt_raw_and_invalid_range(self):
        with self.assertRaises(IOError):
            self.run_batch(fail_at='bronze')
        next(self.raw.root.rglob('*.json')).write_text('corrupt')
        with self.assertRaises(ValueError):
            self.run_batch()
        with self.assertRaises(ValueError):
            self.run_batch(dates=('2026-01-02','2026-01-01'))
