import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bank.fx import ingest_live_fx, version_observations
from bank.ingestion import FileRaw, SQLiteBronze, State, Watermark, transaction


class FxRestatementTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = State(self.root/'state.db')
        self.bronze = SQLiteBronze(self.root/'bronze.db')
        self.raw = FileRaw(self.root/'raw')

    def fetch(self,value,bronze=None):
        data = json.dumps({'date':'2026-01-30','base':'USD','quote':'EUR','rate':value}).encode()
        with patch('bank.external.urlopen',return_value=io.BytesIO(data)):
            return ingest_live_fx('2026-01-31',state=self.state,raw=self.raw,bronze=bronze or self.bronze)

    def saved(self):
        with transaction(self.root/'bronze.db') as db:
            return [json.loads(r[0]) for r in db.execute('SELECT payload FROM versions ORDER BY ts')]

    def test_provider_correction_and_reversion_preserve_observed_versions(self):
        first = self.fetch('0.886')
        corrected = self.fetch('0.887')
        replay = self.fetch('0.88700')
        reverted = self.fetch('0.886')
        rows = self.saved()
        self.assertEqual([r['rate'] for r in rows],['0.886','0.887','0.886'])
        self.assertLess(first['observed_at'],corrected['observed_at'])
        self.assertLess(corrected['observed_at'],reverted['observed_at'])
        self.assertEqual(corrected['batch_id'],replay['batch_id'])
        self.assertEqual(self.state.watermark('external.fx_rates_live'),Watermark())
        self.assertTrue(all(r['business_date']=='2026-01-30' and r['origin']=='LIVE' for r in rows))

    def test_retry_after_bronze_commit_reuses_observation_identity(self):
        parent = self.bronze
        class FailedAfterCommit:
            def commit(self,rows):
                parent.commit(rows)
                raise IOError('Injected interruption after durable commit')
            def validate(self,rows):
                return parent.validate(rows)
        with self.assertRaises(IOError):
            self.fetch('0.887',FailedAfterCommit())
        with self.state.connect() as db:
            observation = db.execute('SELECT observed_at FROM fx_observations').fetchone()[0]
        recovered = self.fetch('0.887')
        self.assertEqual(recovered['observed_at'],observation)
        self.assertEqual(len(self.saved()),1)
        self.assertEqual(self.state.manifests()[0]['attempts'],2)

    def test_clock_regression_does_not_reorder_content_revisions(self):
        row = dict(rate_id='2026-01-30-USD-EUR',business_date='2026-01-30',base_currency='USD',
                   quote_currency='EUR',rate='100.00',origin='LIVE',updated_at='2026-01-30T00:00:00Z')
        with self.state.exclusive():
            first = version_observations([row],self.state,'2026-02-01T00:00:00Z')[0]
            second = version_observations([{**row,'rate':'101.00'}],self.state,'2026-01-31T00:00:00Z')[0]
        self.assertEqual(first['rate'],'100')
        self.assertEqual(second['rate'],'101')
        self.assertGreater(second['updated_at'],first['updated_at'])

    def test_unbounded_numeric_input_is_rejected_before_writes(self):
        row = dict(rate_id='bad',rate='1e999999999',origin='LIVE')
        with self.assertRaises(ValueError),self.state.exclusive():
            version_observations([row],self.state)
        with self.state.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM fx_observations').fetchone()[0],0)
