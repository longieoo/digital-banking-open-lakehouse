import csv
import hashlib
from http.server import ThreadingHTTPServer
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

from bank.release import atomic_json, verify
from bank.serving import handler, report, snapshot


class ServingTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.value = 'a'*32
        self.bundle = self.root/self.value
        self.document = dict(release_id=self.value, start_date='2026-01-01', as_of_date='2026-01-31',
            reporting_currency='USD', bronze_schema='bronze_r_'+self.value,
            silver_schema='silver_r_'+self.value, gold_schema='gold_r_'+self.value,
            status='CERTIFIED', exports={})
        base = dict(business_date='2026-01-01', event_time='2026-01-01T00:00:00Z',
                    currency='USD', status='SUCCESS', channel='MOBILE', transaction_type='TRANSFER_OUT')
        self.tables = {
            'fct_transactions': [{**base, 'transaction_id':'T1', 'amount':'10000000000000000.01',
                                  'signed_amount':'-0.01'},
                                 {**base, 'transaction_id':'T2', 'amount':'0.02', 'signed_amount':'0.02'},
                                 {**base, 'currency':'EUR', 'transaction_id':'T3', 'amount':'99.00',
                                  'signed_amount':'99.00'}],
            'fct_transfers': [{**base, 'amount':'0.01', 'transfer_id':'X1'}],
            'fct_account_balance_daily': [dict(currency='USD', business_date='2026-01-01',
                                            closing_balance='10.00'),
                                          dict(currency='USD', business_date='2026-01-02',
                                               closing_balance='20.00')],
            'mart_customer_360': [dict(customer_id='C1', reporting_currency='USD', total_balance='20.00')],
            'mart_reconciliation_daily': [dict(currency='USD', business_date='2026-01-01',
                                               reconciliation_status='PASS')],
            'dim_account': [dict(account_id='A1')]}
        for name, rows in self.tables.items():
            output = io.StringIO(newline='')
            writer = csv.DictWriter(output, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
            data = output.getvalue().encode('utf-8')
            path = self.bundle/'exports'/f'{name}.csv'
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            self.document['exports'][name] = dict(file=f'exports/{name}.csv', rows=len(rows),
                                                 sha256=hashlib.sha256(data).hexdigest())
        self.document['certified_models'] = list(self.tables)
        atomic_json(self.bundle/'release.json', self.document)
        atomic_json(self.root/'current.json', dict(release_id=self.value,
            manifest=f'{self.value}/release.json', as_of_date='2026-01-31'))

    def test_decimal_currency_transfer_and_balance_semantics(self):
        document, tables = snapshot(self.root)
        result = report(document, tables)
        summary = result['summary']
        self.assertEqual(summary['transactions'], 2)
        self.assertEqual(summary['posted_value'], '10000000000000000.03')
        self.assertEqual(summary['net_posted_movement'], '0.01')
        self.assertEqual(summary['closing_balance'], '20.00')
        self.assertEqual(summary['balance_date'], '2026-01-02')
        self.assertEqual(summary['transferred_value'], '0.01')
        self.assertEqual(report(document, tables, end='2026-01-01')['summary']['closing_balance'], '10.00')
        self.assertEqual(report(document, tables, currency='EUR')['summary']['posted_value'], '99.00')
        self.assertEqual(result['customer_as_of_date'], '2026-01-31')

    def test_tampered_export_and_invalid_pointer_are_rejected(self):
        (self.bundle/'exports/fct_transactions.csv').write_text('corrupt', encoding='utf-8')
        with self.assertRaises(ValueError):
            snapshot(self.root)
        atomic_json(self.root/'current.json', dict(release_id='../secret', manifest='../secret/release.json'))
        with self.assertRaises(ValueError):
            snapshot(self.root)

    def test_exact_parsed_bytes_are_hashed_again_after_verification(self):
        def race(value, root):
            document = verify(value, root=root)
            (self.bundle/'exports/fct_transactions.csv').write_text('changed', encoding='utf-8')
            return document
        with patch('bank.serving.verify', side_effect=race), self.assertRaises(ValueError):
            snapshot(self.root)

    def test_publication_pointer_is_resolved_only_once(self):
        def switch(value, root):
            atomic_json(self.root/'current.json', dict(release_id='b'*32))
            return verify(value, root=root)
        with patch('bank.serving.verify', side_effect=switch):
            document, tables = snapshot(self.root)
        self.assertEqual(document['release_id'], self.value)
        self.assertEqual(len(tables['fct_transactions']), 3)

    def test_http_readonly_boundary_filters_and_unavailable(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler(self.root))
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f'http://127.0.0.1:{server.server_port}'
        with urlopen(base+'/api/report') as response:
            self.assertEqual(json.load(response)['summary']['transactions'], 2)
            self.assertEqual(response.headers['Cache-Control'], 'no-store')
        for route, expected in [('/../.env', 404), ('/api/report?currency=USD&currency=EUR', 400),
                                ('/api/report?start=2027-01-01', 400)]:
            with self.assertRaises(HTTPError) as caught:
                urlopen(base+route)
            self.assertEqual(caught.exception.code, expected)
        (self.bundle/'exports/fct_transactions.csv').write_text('corrupt', encoding='utf-8')
        with self.assertRaises(HTTPError) as caught:
            urlopen(base+'/health')
        self.assertEqual(caught.exception.code, 503)
        self.assertNotIn(str(self.root), caught.exception.read().decode())


if __name__ == '__main__':
    unittest.main()
