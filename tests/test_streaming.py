import importlib.util
import unittest
from bank.generator import generate
from streaming.producer import event
from streaming.rules import risk_sql


class EventTest(unittest.TestCase):
    def test_deterministic_version_identity(self):
        row = generate()['transactions'][1]
        self.assertEqual(event(row),event(dict(row)))
        self.assertNotEqual(event(row)['event_id'],event({**row,'updated_at':'2026-02-01T00:00:00Z'})['event_id'])

    def test_threshold_validation(self):
        for value in ['NaN','-1','Infinity','0']:
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
