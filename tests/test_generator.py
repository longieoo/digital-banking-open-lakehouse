from collections import defaultdict
from decimal import Decimal
import unittest
from bank.generator import generate, failure_cases


class GeneratorTest(unittest.TestCase):
    def setUp(self):
        self.data = generate(customers=8, transactions=100)

    def test_determinism_and_override(self):
        self.assertEqual(self.data, generate(customers=8, transactions=100))
        self.assertNotEqual(self.data, generate(seed=2, customers=8, transactions=100))
        self.assertEqual(len(generate('large', customers=2, transactions=2)['customers']), 2)

    def test_references_and_decimal(self):
        accounts = {a['account_id'] for a in self.data['accounts']}
        txns = {t['transaction_id']: t for t in self.data['transactions']}
        self.assertEqual(len(txns), len(self.data['transactions']))
        for t in txns.values():
            self.assertIn(t['account_id'], accounts)
            self.assertGreater(Decimal(t['amount']), 0)
            if t['original_transaction_id']:
                original = txns[t['original_transaction_id']]
                self.assertEqual(original['amount'], t['amount'])
                self.assertEqual(original['status'], 'SUCCESS')
        self.assertTrue(all(c['email'].endswith('@example.invalid') for c in self.data['customers']))

    def test_gl_balance_and_transfer_legs(self):
        balances = defaultdict(Decimal)
        for row in self.data['journal_lines']:
            balances[row['entry_id']] += Decimal(row['debit']) - Decimal(row['credit'])
        self.assertTrue(all(v == 0 for v in balances.values()))
        for transfer in self.data['transfers']:
            legs = [t for t in self.data['transactions'] if t['transfer_id'] == transfer['transfer_id']]
            self.assertEqual({t['transaction_type'] for t in legs}, {'TRANSFER_IN', 'TRANSFER_OUT'})
            self.assertEqual(len(legs), 2)
            self.assertNotEqual(transfer['source_account_id'], transfer['destination_account_id'])

    def test_history_and_failure_fixtures(self):
        self.assertGreater(len(self.data['customer_history']), len(self.data['customers']))
        cases = failure_cases(self.data)
        self.assertEqual(cases['duplicate'][0], cases['duplicate'][1])
        self.assertLess(Decimal(cases['invalid'][0]['amount']), 0)
