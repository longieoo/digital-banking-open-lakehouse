import os
import unittest


@unittest.skipUnless(os.environ.get('RUN_SPARK') == '1', 'Set RUN_SPARK=1 with Spark/Iceberg runtime')
class SparkTest(unittest.TestCase):
    def test_current_alert_corrections_retract_atomically_and_retry_audit(self):
        import json
        import uuid
        from unittest.mock import patch
        from processing.silver import session
        from streaming.consumer import affected_accounts, reconcile_current_alerts
        spark = session()
        namespace = 'risk_' + uuid.uuid4().hex
        spark.sql(f'CREATE NAMESPACE lake.{namespace}')
        transactions = [
            ('T0','A','2026-01-01 00:00:00','100','SUCCESS','TRANSFER_OUT'),
            ('T1','A','2026-01-01 00:01:00','100','SUCCESS','TRANSFER_OUT'),
            ('T2','A','2026-01-01 00:02:00','100','SUCCESS','TRANSFER_OUT'),
            ('F0','B','2026-01-01 00:00:00','100','FAILED','WITHDRAWAL'),
            ('F1','B','2026-01-01 00:01:00','100','FAILED','WITHDRAWAL'),
            ('F2','B','2026-01-01 00:02:00','100','FAILED','WITHDRAWAL'),
            ('L','C','2026-01-01 00:03:00','10001','SUCCESS','DEPOSIT'),
            ('U','UNTOUCHED','2026-01-01 00:03:00','10002','SUCCESS','DEPOSIT')]
        def write(rows):
            frame = spark.createDataFrame(rows,['transaction_id','account_id','event_time','amount','status','transaction_type'])
            (frame.selectExpr('transaction_id','account_id','cast(event_time as timestamp) event_time',
                              'cast(amount as decimal(22,2)) amount','status','transaction_type')
             .writeTo(f'lake.{namespace}.transactions').using('iceberg').createOrReplace())
        def state():
            return {(row.transaction_id,row.rule) for row in spark.table(f'lake.{namespace}.risk_alerts').collect()}
        def audit_count():
            return spark.table(f'lake.{namespace}.risk_alert_observations').count()
        try:
            write(transactions)
            # Migrate a stale alert retained by the previous append-only implementation.
            legacy = spark.sql("SELECT 'OLD' transaction_id,'FORMER' account_id,"
                "timestamp '2026-01-01 00:00:00' event_time,'AMOUNT' rule,'legacy' alert_id")
            legacy.writeTo(f'lake.{namespace}.risk_alerts').using('iceberg').tableProperty('format-version','2').create()
            accounts = spark.createDataFrame([('A',)],['account_id'])
            initial = reconcile_current_alerts(spark,accounts,'initial',namespace)
            self.assertTrue(initial['bootstrap'])
            self.assertEqual(state(),{('T2','TRANSFER_VELOCITY'),('F2','REPEATED_FAILURES'),('L','AMOUNT'),('U','AMOUNT')})
            self.assertEqual(audit_count(),5)
            self.assertEqual(reconcile_current_alerts(spark,accounts,'initial',namespace)['state_changes'],0)
            self.assertEqual(audit_count(),5)
            corrected = [tuple(['T0','D',*row[2:]]) if row[0]=='T0' else
                         tuple([*row[:4],'SUCCESS',row[5]]) if row[0]=='F0' else
                         tuple([*row[:4],'FAILED',row[5]]) if row[0]=='L' else row for row in transactions]
            write(corrected)
            history = spark.createDataFrame([
                ('T0',json.dumps({'account_id':'A'})),('T0',json.dumps({'account_id':'D'})),
                ('F0',json.dumps({'account_id':'B'})),('L',json.dumps({'account_id':'C'})),
                ('U',json.dumps({'account_id':'UNTOUCHED'}))],['record_id','payload'])
            ids = spark.createDataFrame([('T0',),('F0',),('L',)],['record_id'])
            changed = affected_accounts(history,ids)
            self.assertEqual({r.account_id for r in changed.collect()},{'A','B','C','D'})
            # The observation commit succeeds, then the current-state commit fails.
            # Retrying must neither lose the retraction nor duplicate its audit event.
            with patch('streaming.consumer._merge_current_alerts',side_effect=RuntimeError('injected before current commit')):
                with self.assertRaises(RuntimeError):
                    reconcile_current_alerts(spark,changed,'correction',namespace)
            self.assertEqual(audit_count(),8)
            self.assertEqual(len(state()),4)
            reconcile_current_alerts(spark,changed,'correction',namespace)
            self.assertEqual(state(),{('U','AMOUNT')})
            self.assertEqual(audit_count(),8)
            reconcile_current_alerts(spark,changed,'correction',namespace)
            self.assertEqual(audit_count(),8)
            # Correcting one account does not retract alerts belonging to untouched accounts.
            restored = [tuple([*row[:4],'SUCCESS',row[5]]) if row[0]=='L' else row for row in corrected]
            write(restored)
            reconcile_current_alerts(spark,changed,'restored',namespace)
            self.assertEqual(state(),{('L','AMOUNT'),('U','AMOUNT')})
            self.assertEqual(audit_count(),9)
            # A changed rule threshold must trigger global policy reconciliation,
            # even if this batch only touches a different account.
            result = reconcile_current_alerts(spark,accounts,'policy-up',namespace,threshold='20000.00')
            self.assertTrue(result['bootstrap'])
            self.assertEqual(state(),set())
            self.assertEqual(audit_count(),11)
            reconcile_current_alerts(spark,accounts,'policy-up',namespace,threshold='20000.00')
            self.assertEqual(audit_count(),11)
        finally:
            for table in spark.sql(f'SHOW TABLES IN lake.{namespace}').collect():
                spark.sql(f'DROP TABLE lake.{namespace}.{table.tableName}')
            spark.sql(f'DROP NAMESPACE IF EXISTS lake.{namespace}')
            spark.stop()

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
