import importlib.util
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from bank.generator import generate
from streaming.producer import event
from streaming.rules import risk_sql


class EventTest(unittest.TestCase):
    def test_manual_alert_reconciliation_does_not_require_kafka_or_mutate_source(self):
        from streaming.consumer import main
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ,{'CONTROL_PATH':str(Path(tmp)/'control.db')}):
                with patch('sys.argv',['consumer.py','--reconcile-alerts']), patch('streaming.consumer.session') as session:
                    session.return_value.sql.return_value.first.return_value.snapshot_id = 123
                    with patch('streaming.consumer.reconcile_current_alerts',return_value={'state_changes':0}) as reconcile:
                        with patch('sys.stdout',new_callable=io.StringIO):
                            main()
                        reconcile.assert_called_once_with(session.return_value,None,'manual-silver-snapshot:123')
                        session.return_value.sql.assert_called_once_with(
                            "SELECT snapshot_id FROM lake.silver.transactions.refs WHERE name='main'")
                    session.return_value.stop.assert_called_once()

    def test_deterministic_version_identity(self):
        row = generate()['transactions'][1]
        self.assertEqual(event(row),event(dict(row)))
        self.assertNotEqual(event(row)['event_id'],event({**row,'updated_at':'2026-02-01T00:00:00Z'})['event_id'])

    def test_threshold_validation(self):
        for value in ['NaN','-1','Infinity','0','invalid','0.001','100000000000000000000.00']:
            with self.assertRaises(ValueError):
                risk_sql(value)

    @unittest.skipUnless(importlib.util.find_spec('duckdb'), 'SQL test dependencies unavailable')
    def test_rules_window_threshold_and_boundary(self):
        import duckdb
        db = duckdb.connect()
        self.addCleanup(db.close)
        db.execute('CREATE TABLE risk_events (transaction_id varchar,account_id varchar,event_time timestamp,'
                   'amount decimal(22,2),status varchar,transaction_type varchar)')
        for i in range(3):
            db.execute("INSERT INTO risk_events VALUES (?, 'A', ?, 100, 'SUCCESS', 'TRANSFER_OUT')",
                       (f'T{i}',f'2026-01-01 00:0{i}:00'))
        db.execute("INSERT INTO risk_events VALUES ('L','A',timestamp '2026-01-01 00:10:00',10001,'SUCCESS','DEPOSIT')")
        db.execute("INSERT INTO risk_events VALUES ('E','B',timestamp '2026-01-01 00:10:00',10000,'SUCCESS','DEPOSIT')")
        rows = db.execute(risk_sql()).fetchall()
        self.assertEqual({(r[0],r[3]) for r in rows},{('T2','TRANSFER_VELOCITY'),('L','AMOUNT')})

    @unittest.skipUnless(importlib.util.find_spec('duckdb'), 'SQL test dependencies unavailable')
    def test_velocity_and_failure_alerts_only_apply_to_matching_event_types(self):
        import duckdb
        db = duckdb.connect()
        self.addCleanup(db.close)
        db.execute('''CREATE TABLE risk_events AS SELECT transaction_id,account_id,
          cast(event_time AS timestamp) event_time,cast(amount AS decimal(22,2)) amount,status,transaction_type
          FROM (VALUES
          ('T0','A','2026-01-01 00:00:00',100,'SUCCESS','TRANSFER_OUT'),
          ('T1','A','2026-01-01 00:01:00',100,'SUCCESS','TRANSFER_OUT'),
          ('T2','A','2026-01-01 00:05:00',100,'SUCCESS','TRANSFER_OUT'),
          ('DEPOSIT','A','2026-01-01 00:05:00',100,'SUCCESS','DEPOSIT'),
          ('T3','A','2026-01-01 00:10:01',100,'SUCCESS','TRANSFER_OUT'),
          ('F0','B','2026-01-01 00:00:00',100,'FAILED','WITHDRAWAL'),
          ('F1','B','2026-01-01 00:01:00',100,'FAILED','WITHDRAWAL'),
          ('F2','B','2026-01-01 00:05:00',100,'FAILED','WITHDRAWAL'),
          ('RECOVERED','B','2026-01-01 00:05:00',100,'SUCCESS','WITHDRAWAL'),
          ('F3','B','2026-01-01 00:10:01',100,'FAILED','WITHDRAWAL'))
          AS events(transaction_id,account_id,event_time,amount,status,transaction_type)''')
        rows = db.execute(risk_sql('10000.000')).fetchall()
        self.assertEqual({(r[0],r[3]) for r in rows},
                         {('T2','TRANSFER_VELOCITY'),('F2','REPEATED_FAILURES')})
