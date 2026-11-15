from decimal import Decimal
import importlib.util
import unittest
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec('duckdb') and importlib.util.find_spec('sqlglot'),
                     'Install SQL test dependencies')
class GoldTest(unittest.TestCase):
    def setUp(self):
        from tests.gold_harness import GoldHarness
        self.h = GoldHarness()
        self.addCleanup(self.h.close)
        self.h.build()

    def test_daily_incremental_matches_full_after_delete_and_move(self):
        db = self.h.db
        db.execute("ATTACH ':memory:' AS lake")
        db.execute('CREATE SCHEMA lake.prior')
        for model in ('fct_transactions','mart_transaction_daily'):
            db.execute(f'CREATE TABLE lake.prior.{model} AS SELECT * FROM gold.{model}')
        db.execute("DELETE FROM gold.fct_transactions WHERE business_date = (SELECT min(business_date) FROM gold.fct_transactions)")
        db.execute("UPDATE gold.fct_transactions SET business_date = DATE '2026-02-01' WHERE status = 'FAILED'")
        path = Path('dbt/models/mart_transaction_daily.sql')
        db.execute('CREATE TABLE expected AS ' + self.h.render(path))
        self.h.variables['baseline_schema'] = 'prior'
        db.execute('CREATE TABLE observed AS ' + self.h.render(path))
        for a, b in [('expected','observed'),('observed','expected')]:
            self.assertEqual(db.execute(f'SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b}').fetchall(), [])

    def test_scd_history_and_historical_join(self):
        self.assertGreater(self.h.scalar('SELECT count(*) FROM gold.dim_customer'), 8)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM gold.dim_customer WHERE is_current'), 8)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM gold.fct_transactions WHERE customer_sk IS NULL'),0)
        self.assertEqual(self.h.scalar("SELECT count(*) FROM gold.fct_transactions WHERE customer_id='SYN-C000000' "
                                      "AND event_time < timestamp '2026-01-16' AND segment <> 'Standard'"),0)
        # A non-business source update does not create another SCD2 interval.
        self.h.db.execute("INSERT INTO silver.customer_history SELECT customer_id,full_name,email,phone,"
            "date_of_birth,segment,branch_id,status,created_at,timestamp '2026-01-20' "
            "FROM silver.customers WHERE customer_id='SYN-C000000'")
        self.h.build()
        self.assertEqual(self.h.scalar("SELECT count(*) FROM gold.dim_customer WHERE customer_id='SYN-C000000'"),2)

    def test_dense_balances_exact_totals_and_idempotency(self):
        self.assertEqual(self.h.scalar('SELECT count(*) FROM gold.fct_account_balance_daily'), 8*31)
        expected = self.h.scalar('SELECT sum(signed_amount) FROM gold.fct_transactions')
        actual = self.h.scalar("SELECT sum(closing_balance) FROM gold.fct_account_balance_daily "
                               "WHERE business_date=date '2026-01-31'")
        self.assertIsInstance(actual, Decimal)
        self.assertEqual(expected, actual)
        before = self.h.db.execute('SELECT * FROM gold.mart_customer_360 ORDER BY customer_id').fetchall()
        self.h.build()
        self.assertEqual(before, self.h.db.execute('SELECT * FROM gold.mart_customer_360 ORDER BY customer_id').fetchall())

    def test_customer_grain_and_pii_exclusion(self):
        self.assertEqual(self.h.scalar('SELECT count(*) FROM gold.mart_customer_360'),8)
        columns = {r[0] for r in self.h.db.execute('DESCRIBE gold.mart_customer_360').fetchall()}
        self.assertFalse(columns & {'email','phone','full_name','date_of_birth'})
        self.assertEqual(self.h.scalar('SELECT sum(txn_count_30d) FROM gold.mart_customer_360'),
            self.h.scalar("SELECT count(*) FROM gold.fct_transactions WHERE business_date BETWEEN "
                          "date '2026-01-02' AND date '2026-01-31'"))

    def test_dbt_singular_assertions(self):
        for path in Path('dbt/tests').glob('*.sql'):
            with self.subTest(test=path.stem):
                self.assertEqual(self.h.db.execute(self.h.render(path)).fetchall(), [])
