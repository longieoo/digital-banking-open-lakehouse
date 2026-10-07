import os
import unittest


@unittest.skipUnless(os.environ.get('RUN_SPARK') == '1', 'Set RUN_SPARK=1 with Spark/Iceberg runtime')
class SparkTest(unittest.TestCase):
    def test_control_epochs_remove_disappeared_groups(self):
        import json
        import uuid
        from processing.silver import session, process_table
        spark = session()
        prefix = 'quality_' + uuid.uuid4().hex
        namespaces = [prefix + suffix for suffix in ('_b','_s','_q')]
        try:
            for namespace in namespaces:
                spark.sql(f'CREATE NAMESPACE lake.{namespace}')
            def frame(currency,stamp):
                payload = dict(total_id=f'2026-01-01-{currency}',business_date='2026-01-01',
                               currency=currency,source_count=1,source_amount='1.00',updated_at=stamp)
                return spark.createDataFrame([(json.dumps(payload),'test',payload['total_id'],stamp,'2026-01-01')],
                    ['payload','batch_id','record_id','source_updated_at','business_date'])
            frame('EUR','2026-01-30T00:00:00+00:00').union(
                frame('USD','2026-01-31T00:00:00+00:00')).writeTo(f'lake.{namespaces[0]}.source_totals').create()
            process_table(spark,'source_totals',*namespaces,strict=True)
            self.assertEqual([row.currency for row in spark.table(f'lake.{namespaces[1]}.source_totals').collect()],['USD'])
            frame('EUR','2026-02-01T00:00:00+00:00').writeTo(f'lake.{namespaces[0]}.source_totals').append()
            process_table(spark,'source_totals',*namespaces,strict=True)
            self.assertEqual([row.currency for row in spark.table(f'lake.{namespaces[1]}.source_totals').collect()],['EUR'])
        finally:
            for namespace in namespaces:
                for table in spark.sql(f'SHOW TABLES IN lake.{namespace}').collect():
                    spark.sql(f'DROP TABLE lake.{namespace}.{table.tableName}')
                spark.sql(f'DROP NAMESPACE IF EXISTS lake.{namespace}')
            spark.stop()

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
