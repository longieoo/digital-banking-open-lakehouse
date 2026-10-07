import unittest
from tests.gold_harness import GoldHarness


class ReportingDateTest(unittest.TestCase):
    def test_reporting_date_changes_calendar_and_customer_snapshot(self):
        harness = GoldHarness(variables={'as_of_date':'2026-01-15'})
        try:
            harness.build()
            self.assertEqual(harness.scalar('SELECT count(*) FROM gold.dim_date'),15)
            self.assertEqual(str(harness.scalar('SELECT max(as_of_date) FROM gold.mart_customer_360')),'2026-01-15')
            self.assertEqual(harness.scalar('SELECT count(*) FROM gold.fct_account_balance_daily'),8*15)
        finally:
            harness.close()
