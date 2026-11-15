import os
import unittest
import uuid


@unittest.skipUnless(os.environ.get('RUN_STACK') == '1', 'Requires disposable PostgreSQL and raw storage')
class OutboxTest(unittest.TestCase):
    def test_delete_rollback_out_of_order_commit_and_retry(self):
        import psycopg
        from bank.adapters import raw_store
        from bank.cdc import install, drain
        identifier = uuid.uuid4().hex
        store = raw_store()
        with psycopg.connect(autocommit=True) as db, psycopg.connect() as late:
            with db.transaction():
                install(db)
            while drain(db, store):
                pass
            insert = 'INSERT INTO gl.gl_accounts VALUES (%s, %s, now())'
            late.execute(insert, (identifier, 'late'))
            db.execute(insert, (identifier + '2', 'early'))
            db.execute('UPDATE gl.gl_accounts SET account_name=%s WHERE gl_account_id=%s', ('changed',identifier+'2'))
            db.execute('DELETE FROM gl.gl_accounts WHERE gl_account_id=%s', (identifier+'2',))
            class Interrupted:
                def put_verified(self, key, content):
                    store.put_verified(key, content)
                    raise RuntimeError('interrupted after write')
            with self.assertRaises(RuntimeError):
                drain(db, Interrupted())
            self.assertEqual(drain(db, store), 3)
            late.commit()
            self.assertEqual(drain(db, store), 1)
            late.execute('DELETE FROM gl.gl_accounts WHERE gl_account_id=%s', (identifier,))
            late.rollback()
            self.assertEqual(drain(db, store), 0)
            db.execute('DELETE FROM gl.gl_accounts WHERE gl_account_id=%s', (identifier,))
            self.assertEqual(drain(db, store), 1)
