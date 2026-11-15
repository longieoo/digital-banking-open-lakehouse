from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from bank.maintenance import retention_plan
from bank.release import atomic_json
from tests.test_backup import make_release


class MaintenanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'releases'
        self.now = datetime(2026, 4, 1, tzinfo=timezone.utc)

    def test_current_inprogress_newest_and_currency_history_are_protected(self):
        current, older, eur, active, newest = [letter * 32 for letter in 'abcde']
        make_release(self.root, current, '2026-01-30')
        make_release(self.root, older, '2026-01-29')
        make_release(self.root, eur, '2026-01-25', 'EUR')
        make_release(self.root, active, '2026-01-31', status='SOURCE_READY')
        make_release(self.root, newest, '2026-01-31')
        atomic_json(self.root / 'current.json', {'release_id': current,
                    'manifest': f'{current}/release.json', 'as_of_date': '2026-01-30'})
        before = {str(p.relative_to(self.root)): p.read_bytes()
                  for p in self.root.rglob('*') if p.is_file()}
        plan = retention_plan(self.root, keep_latest=1, now=self.now)
        items = {item['release_id']: item for item in plan['releases']}
        self.assertEqual(plan['deletions_performed'], 0)
        self.assertEqual(plan['protected_bronze_snapshots'], {'transactions': [123]})
        self.assertIn('current_publication', items[current]['protected_reasons'])
        self.assertIn('latest_per_currency', items[newest]['protected_reasons'])
        self.assertIn('latest_per_currency', items[eur]['protected_reasons'])
        self.assertIn('in_progress_release', items[active]['protected_reasons'])
        self.assertTrue(items[older]['export_retention_candidate'])
        after = {str(p.relative_to(self.root)): p.read_bytes()
                 for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(before, after)

    def test_corrupt_unknown_and_recent_releases_never_become_candidates(self):
        values = [letter * 32 for letter in 'abc']
        for value in values:
            make_release(self.root, value)
        (self.root / values[0] / 'exports/fct_transactions.csv').write_text('corrupt')
        manifest = self.root / values[1] / 'release.json'
        manifest.write_text('invalid json')
        manifest = self.root / values[2] / 'release.json'
        document = json.loads(manifest.read_text())
        document['certified_at'] = '2026-03-31T00:00:00+00:00'
        atomic_json(manifest, document)
        plan = retention_plan(self.root, keep_latest=1, now=self.now)
        for item in plan['releases']:
            self.assertFalse(item['export_retention_candidate'])

    def test_invalid_publication_or_arguments_fail_closed(self):
        make_release(self.root)
        atomic_json(self.root / 'current.json', {'release_id': '../outside'})
        with self.assertRaises(ValueError):
            retention_plan(self.root, now=self.now)
        for keep, age in [(0, 7), (True, 7), (1, -1)]:
            with self.assertRaises(ValueError):
                retention_plan(self.root, keep_latest=keep, minimum_age_days=age, now=self.now)
