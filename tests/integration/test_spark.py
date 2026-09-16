import os
import unittest


@unittest.skipUnless(os.environ.get('RUN_SPARK') == '1', 'Set RUN_SPARK=1 with Spark/Iceberg runtime')
class SparkTest(unittest.TestCase):
    def test_typing_and_quarantine(self):
        from processing.silver import session, parse_frame
        from bank.generator import generate
        import json
        spark = session()
        try:
            rows = generate(customers=2, transactions=3)['transactions']
            rows = [rows[0], {**rows[1], 'amount':'-5.00'}, {**rows[2], 'amount':'bad'}]
            df = spark.createDataFrame([(json.dumps(r), 'test') for r in rows], ['payload','batch_id'])
            parsed = parse_frame(df, 'transactions')
            self.assertEqual(parsed.filter("reject_reason = ''").count(), 1)
            self.assertEqual(parsed.filter("reject_reason <> ''").count(), 2)
            self.assertEqual(str(parsed.schema['amount'].dataType), 'DecimalType(22,2)')
            # Exercise the actual streaming rule SQL on Spark, including thresholds
            # and both five-minute windows (the small source demo has no alerts).
            from streaming.rules import risk_sql
            spark.sql("""SELECT transaction_id, account_id, cast(event_time as timestamp) event_time,
                cast(amount as decimal(22,2)) amount, status, transaction_type FROM VALUES
                ('T0','A','2026-01-01 00:00:00',100,'SUCCESS','TRANSFER_OUT'),
                ('T1','A','2026-01-01 00:01:00',100,'SUCCESS','TRANSFER_OUT'),
                ('T2','A','2026-01-01 00:02:00',100,'SUCCESS','TRANSFER_OUT'),
                ('L','A','2026-01-01 00:10:00',10001,'SUCCESS','DEPOSIT'),
                ('E','B','2026-01-01 00:10:00',10000,'SUCCESS','DEPOSIT'),
                ('F0','C','2026-01-01 00:00:00',100,'FAILED','WITHDRAWAL'),
                ('F1','C','2026-01-01 00:01:00',100,'FAILED','WITHDRAWAL'),
                ('F2','C','2026-01-01 00:02:00',100,'FAILED','WITHDRAWAL')
                AS events(transaction_id,account_id,event_time,amount,status,transaction_type)
                """).createOrReplaceTempView('risk_events')
            self.assertEqual({(r.transaction_id, r.rule) for r in spark.sql(risk_sql()).collect()},
                {('T2', 'TRANSFER_VELOCITY'), ('L', 'AMOUNT'), ('F2', 'REPEATED_FAILURES')})
        finally:
            spark.stop()


if __name__ == '__main__':
    unittest.main()
