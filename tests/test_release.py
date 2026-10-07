import csv
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bank.ingestion import FileRaw, MemorySource, SQLiteBronze, State, Watermark, ingest
from bank.pipeline import PagedBackfill
from bank.release import atomic_json, certify, check_build, dbt_vars, release_id, verify


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.environ = patch.dict(os.environ,{'RELEASE_ROOT':str(self.root/'releases'),
                                              'CONTROL_PATH':str(self.root/'control.db')})
        self.environ.start()
        self.addCleanup(self.environ.stop)
        self.value = 'a'*32
        self.document = dict(release_id=self.value,start_date='2026-01-01',as_of_date='2026-01-31',
            reporting_currency='USD',gold_schema='gold_r_'+self.value,silver_schema='silver_r_'+self.value,
            bronze_schema='bronze_r_'+self.value,status='SILVER_READY',silver_quality={'rejected':0})
        self.release = self.root/'releases'/self.value
        atomic_json(self.release/'release.json',self.document)
        self.artifacts = self.root/'artifacts'
        self.artifacts.mkdir()
        self.nodes = {'model.banking.mart_reconciliation_daily':{
            'resource_type':'model','package_name':'banking','name':'mart_reconciliation_daily',
            'schema':self.document['gold_schema']},
            'test.banking.reconciliation_pass':{'resource_type':'test','package_name':'banking'}}
        self.results = {'args':{'vars':dbt_vars(self.document)},'results':[
            {'unique_id':key,'status':'success' if node['resource_type']=='model' else 'pass'}
            for key,node in self.nodes.items()]}
        self.write_artifacts()

    def write_artifacts(self):
        (self.artifacts/'manifest.json').write_text(json.dumps({'nodes':self.nodes,'sources':{}}))
        (self.artifacts/'run_results.json').write_text(json.dumps(self.results))

    def test_certification_is_complete_and_publication_is_idempotent(self):
        class Client:
            def scalar(self,sql):
                return 0 if 'WHERE' in sql else 1
            def pages(self,sql):
                yield {'columns':[{'name':'business_date'},{'name':'reconciliation_status'}],
                       'data':[['2026-01-01','PASS']]}
        result = certify(self.value,self.artifacts,Client())
        self.assertEqual(result['status'],'CERTIFIED')
        pointer = self.root/'releases/current.json'
        before = pointer.read_bytes()
        self.assertEqual(certify(self.value,self.artifacts)['status'],'CERTIFIED')
        self.assertEqual(before,pointer.read_bytes())
        with (self.release/'exports/mart_reconciliation_daily.csv').open() as handle:
            self.assertEqual(list(csv.DictReader(handle))[0]['reconciliation_status'],'PASS')
        (self.release/'exports/mart_reconciliation_daily.csv').write_text('corrupt')
        with self.assertRaises(ValueError):
            verify(self.value)

    def test_failed_or_incomplete_build_cannot_publish(self):
        pointer = self.root/'releases/current.json'
        atomic_json(pointer,{'release_id':'b'*32,'as_of_date':'2026-01-31'})
        before = pointer.read_bytes()
        self.results['results'][1]['status']='fail'
        self.write_artifacts()
        with self.assertRaises(ValueError):
            certify(self.value,self.artifacts)
        self.assertEqual(before,pointer.read_bytes())
        self.results['results'].pop()
        self.write_artifacts()
        with self.assertRaises(ValueError):
            check_build(self.document,self.artifacts)

    def test_wrong_day_and_wrong_schema_cannot_certify(self):
        self.results['args']['vars']['as_of_date']='2026-02-01'
        self.write_artifacts()
        with self.assertRaises(ValueError):
            check_build(self.document,self.artifacts)
        self.results['args']['vars']=dbt_vars(self.document)
        self.nodes['model.banking.mart_reconciliation_daily']['schema']='gold'
        self.write_artifacts()
        with self.assertRaises(ValueError):
            check_build(self.document,self.artifacts)

    def test_mid_export_failure_keeps_previous_release(self):
        pointer = self.root/'releases/current.json'
        atomic_json(pointer,{'release_id':'b'*32,'as_of_date':'2026-01-31'})
        before = pointer.read_bytes()
        class Client:
            def scalar(self,sql):
                return 0 if 'WHERE' in sql else 1
            def pages(self,sql):
                yield {'columns':[{'name':'status'}],'data':[['PASS']]}
                raise IOError('Interrupted export')
        with self.assertRaises(IOError):
            certify(self.value,self.artifacts,Client())
        self.assertEqual(before,pointer.read_bytes())
        self.assertEqual(json.loads((self.release/'release.json').read_text())['status'],'SILVER_READY')

    def test_backfill_is_bounded_and_preserves_forward_cursor(self):
        rows = [dict(transaction_id=f'T{i}',updated_at='2026-01-02T00:00:00+00:00',
                     business_date='2026-01-01') for i in range(7)]
        state, raw = State(self.root/'state.db'),FileRaw(self.root/'raw')
        bronze = SQLiteBronze(self.root/'bronze.db')
        source = PagedBackfill(MemorySource(rows),('2026-01-01','2026-01-01'))
        counts = []
        while True:
            result = ingest(source,raw,bronze,state,limit=2,dates=('2026-01-01','2026-01-01'),cursor=source.cursor)
            source.advance(result['end_watermark'])
            counts.append(result['count'])
            if result['count']<2:
                break
        self.assertEqual(counts,[2,2,2,1])
        self.assertEqual(state.watermark('core.transactions'),Watermark())
        self.assertEqual(len(state.manifests()),4)

    def test_identifiers_reject_path_and_sql_injection(self):
        for value in ('../other','a'*31,'a'*32+';DROP TABLE x'):
            with self.assertRaises(ValueError):
                release_id(value)

    def test_reentrant_writer_lock_keeps_outer_lock(self):
        state = State(self.root/'state.db')
        with state.exclusive():
            with state.exclusive():
                pass
            self.assertIsNotNone(state._lock_owner)
        self.assertIsNone(state._lock_owner)

    def test_source_snapshot_identity_preserves_raw_provenance(self):
        source = MemorySource([dict(transaction_id='T',business_date='2026-01-01',
                                    updated_at='2026-01-02T00:00:00+00:00')])
        state, raw = State(self.root/'state.db'),FileRaw(self.root/'raw')
        bronze = SQLiteBronze(self.root/'bronze.db')
        for token in ('snapshot-one','snapshot-two'):
            ingest(source,raw,bronze,state,dates=('2026-01-01','2026-01-01'),cursor=Watermark(),request_token=token)
        manifests = state.manifests()
        self.assertEqual(len(manifests),2)
        self.assertNotEqual(manifests[0]['batch_id'],manifests[1]['batch_id'])
        self.assertEqual(manifests[0]['raw_sha256'],manifests[1]['raw_sha256'])
        self.assertTrue((raw.root/manifests[0]['raw_key']).is_file())
