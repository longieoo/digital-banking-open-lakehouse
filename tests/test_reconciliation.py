import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec('duckdb') and importlib.util.find_spec('sqlglot'),
                     'Install SQL test dependencies')
class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        from tests.gold_harness import GoldHarness
        self.h = GoldHarness()
        self.addCleanup(self.h.close)
        self.h.build()

    def failures(self):
        return self.h.scalar("SELECT count(*) FROM gold.mart_reconciliation_daily WHERE reconciliation_status='FAIL'")

    def test_good_passes_and_missing_bronze_fails(self):
        self.assertEqual(self.failures(),0)
        self.h.db.execute("DELETE FROM bronze.transactions WHERE record_id='SYN-T000000001'")
        self.h.build()
        self.assertGreater(self.failures(),0)

    def test_balanced_wrong_gl_amount_fails(self):
        self.h.db.execute("UPDATE silver.journal_lines SET debit=debit*2,credit=credit*2 "
                          "WHERE entry_id='SYN-J000000001'")
        self.h.build()
        self.assertGreater(self.failures(),0)
        self.assertEqual(self.h.scalar('SELECT sum(gl_debit-gl_credit) FROM gold.mart_reconciliation_daily'),0)

    def test_unbalanced_gl_and_duplicate_bronze_fail(self):
        self.h.db.execute("UPDATE silver.journal_lines SET debit=debit+1 WHERE line_id='SYN-J000000001-debit'")
        self.h.db.execute("INSERT INTO bronze.transactions SELECT * FROM bronze.transactions LIMIT 1")
        self.h.build()
        self.assertGreater(self.failures(),0)
        self.assertEqual(self.h.scalar('SELECT sum(duplicate_transactions) FROM gold.mart_reconciliation_daily'),1)

    def test_missing_gold_or_wrong_control_account_fails(self):
        self.h.db.execute("DELETE FROM silver.transactions WHERE transaction_id='SYN-T000000001'")
        self.h.build()
        self.assertGreater(self.failures(),0)

    def test_balanced_misallocation_fails(self):
        self.h.db.execute("UPDATE silver.journal_lines SET gl_account_id='SYN-GL-CASH' WHERE entry_id='SYN-J000000001'")
        self.h.build()
        self.assertGreater(self.failures(),0)
