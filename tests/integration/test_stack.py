"""Run only against the dedicated disposable Compose demo, not unrelated databases."""
import os
from pathlib import Path
import tempfile
import unittest


@unittest.skipUnless(os.environ.get('RUN_STACK')=='1','Set RUN_STACK=1 with disposable Docker services')
class RealStackTest(unittest.TestCase):
    def test_shared_read_snapshot_excludes_concurrent_commit(self):
        import psycopg
        from bank.adapters import PostgresSource, seed_postgres
        from bank.generator import generate
        from bank.ingestion import Watermark
        seed_postgres(generate())
        with psycopg.connect() as snapshot:
            snapshot.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            source = PostgresSource('transactions',snapshot)
            before = source.extract(Watermark(),1,None)[0]
            try:
                with psycopg.connect() as writer:
                    writer.execute('UPDATE core.transactions SET channel=%s WHERE transaction_id=%s',
                                   ('ATM' if before['channel']!='ATM' else 'BRANCH',before['transaction_id']))
                self.assertEqual(source.extract(Watermark(),1,None)[0],before)
                current = next(row for row in PostgresSource('transactions').extract(Watermark(),10000,None)
                               if row['transaction_id']==before['transaction_id'])
                self.assertNotEqual(current['channel'],before['channel'])
            finally:
                with psycopg.connect() as writer:
                    writer.execute('UPDATE core.transactions SET channel=%s WHERE transaction_id=%s',
                                   (before['channel'],before['transaction_id']))

    def test_postgres_minio_rest_recovery_backfill(self):
        from bank.adapters import PostgresSource,IcebergBronze,iceberg_catalog,raw_store,seed_postgres
        from bank.generator import generate
        from bank.ingestion import State,ingest,Watermark
        seed_postgres(generate())
        source=PostgresSource('transactions')
        bronze=IcebergBronze(iceberg_catalog(),'transactions')
        raw=raw_store()
        with tempfile.TemporaryDirectory() as tmp:
            state=State(Path(tmp)/'control.db')
            def run(**kwargs):
                return ingest(source,raw,bronze,state,**kwargs)
            with self.assertRaises(IOError):
                run(fail_at='bronze')
            self.assertEqual(state.watermark('core.transactions'),Watermark())
            self.assertEqual(state.manifests()[0]['status'],'FAILED')
            first=run()
            self.assertGreater(first['count'],0)
            snapshot=bronze.table.current_snapshot().snapshot_id
            run()
            self.assertEqual(bronze.table.current_snapshot().snapshot_id,snapshot)
            import psycopg
            with psycopg.connect() as db:
                db.execute("UPDATE core.transactions SET channel='ATM', updated_at=updated_at + interval '45 days' "
                           "WHERE transaction_id='SYN-T000000001'")
            self.assertEqual(run()['count'],1)
            before=state.watermark('core.transactions')
            run(dates=('2026-01-01','2026-01-07'))
            self.assertEqual(state.watermark('core.transactions'),before)
            self.assertGreater(bronze.table.scan(snapshot_id=snapshot).to_arrow().num_rows,0)
