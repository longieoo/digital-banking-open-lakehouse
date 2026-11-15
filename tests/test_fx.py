import ast
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bank.fx import ingest_live_fx
from bank.ingestion import FileRaw, SQLiteBronze, State, transaction


class ScheduledFxTest(unittest.TestCase):
    def test_replay_and_historical_capture_do_not_move_watermark(self):
        payload=b'{"date":"2026-01-30","base":"USD","quote":"EUR","rate":0.886}'
        with tempfile.TemporaryDirectory() as root:
            root=Path(root)
            state=State(root/'control.db')
            bronze=SQLiteBronze(root/'bronze.db')
            raw=FileRaw(root/'raw')
            with patch('bank.external.urlopen',side_effect=lambda *a,**k:io.BytesIO(payload)):
                first=ingest_live_fx('2026-01-31',state=state,raw=raw,bronze=bronze)
                second=ingest_live_fx('2026-01-31',state=state,raw=raw,bronze=bronze)
            self.assertEqual(first['batch_id'],second['batch_id'])
            self.assertEqual(first['origin'],'LIVE')
            self.assertEqual(first['age_days'],1)
            self.assertFalse(first['used_for_gold_conversion'])
            self.assertEqual(state.watermark('external.fx_rates_live').updated_at,'1970-01-01T00:00:00.000000+00:00')
            with transaction(root/'bronze.db') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM versions').fetchone()[0],1)
            self.assertEqual(len(list((root/'raw').rglob('*.json'))),1)
            revised=payload.replace(b'0.886',b'0.887')
            with patch('bank.external.urlopen',return_value=io.BytesIO(revised)):
                with self.assertRaisesRegex(ValueError,'Source changed'):
                    ingest_live_fx('2026-01-31',state=state,raw=raw,bronze=bronze)
            self.assertEqual(state.manifests()[-1]['status'],'FAILED')

    def test_stale_fetch_does_not_write(self):
        payload=b'{"date":"2026-01-20","base":"USD","quote":"EUR","rate":0.886}'
        with patch('bank.external.urlopen',return_value=io.BytesIO(payload)), patch('bank.fx.ingest') as write:
            with self.assertRaisesRegex(ValueError,'stale'):
                ingest_live_fx('2026-01-31')
            write.assert_not_called()

    def test_dag_optional_fx_and_business_date(self):
        # Execute only the thin callables, without substituting proof of Airflow runtime.
        tree=ast.parse(Path('orchestration/dags/banking.py').read_text())
        funcs=[node for node in tree.body if isinstance(node,ast.FunctionDef)
               and node.name in {'refresh_reference_fx','daily_reference_fx'}]
        from datetime import datetime, timedelta, timezone
        namespace={'timedelta':timedelta}
        exec(compile(ast.Module(body=funcs,type_ignores=[]),'banking.py','exec'),namespace)
        context={'params':{'fetch_live_fx':False,'as_of_date':'','fx_max_age_days':7},
                 'data_interval_end':datetime(2026,1,31,2,tzinfo=timezone.utc)}
        with patch('bank.fx.ingest_live_fx') as fetch:
            self.assertEqual(namespace['refresh_reference_fx'](**context)['status'],'DISABLED')
            fetch.assert_not_called()
            context['params']['fetch_live_fx']=True
            namespace['refresh_reference_fx'](**context)
            fetch.assert_called_once_with('2026-01-30',max_age_days=7)
        context['params']['as_of_date']='2026-01-15'
        with patch('bank.fx.ingest_live_fx',side_effect=ValueError('stale')):
            with self.assertRaises(ValueError):
                namespace['refresh_reference_fx'](**context)
