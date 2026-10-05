import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from bank.external import URL, rates


class LiveRatesTest(unittest.TestCase):
    def test_live_v2_preserves_decimal_and_provenance(self):
        payload = b'{"date":"2026-10-05","base":"USD","quote":"EUR","rate":0.886010123456789}'
        with patch('bank.external.urlopen',return_value=io.BytesIO(payload)) as fetch:
            row = rates(live=True)[0]
        self.assertEqual(row['rate'],'0.886010123456789')
        self.assertEqual(row['origin'],'LIVE')
        self.assertEqual(row['rate_id'],'2026-10-05-USD-EUR')
        self.assertEqual(fetch.call_args.args[0].full_url,URL)

    def test_invalid_live_payload_is_rejected(self):
        for pair, rate in [('EUR',1),('USD',0),('USD',-1),('USD','NaN')]:
            payload = ('{"date":"2026-10-05","base":"%s","quote":"EUR","rate":"%s"}' % (pair,rate)).encode()
            with self.subTest(pair=pair,rate=rate), patch('bank.external.urlopen',return_value=io.BytesIO(payload)):
                with self.assertRaises(ValueError):
                    rates(live=True)

    def test_http_failure_does_not_substitute_fixture(self):
        with patch('bank.external.urlopen',side_effect=HTTPError(URL,403,'Forbidden',{},None)):
            with self.assertRaises(HTTPError):
                rates(live=True)
