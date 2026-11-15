import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from datetime import date, datetime, timezone
from email.utils import format_datetime

from bank.external import URL, fetch_content, rates


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

    def test_retry_after_and_transient_errors(self):
        payload = b'{"date":"2026-10-05","base":"USD","quote":"EUR","rate":0.886}'
        failures=[HTTPError(URL,429,'Limit',{'Retry-After':'3'},None),
                  HTTPError(URL,503,'Unavailable',{},None),io.BytesIO(payload)]
        with patch('bank.external.urlopen',side_effect=failures) as fetch, patch('bank.external.time.sleep') as sleep:
            self.assertEqual(fetch_content(URL),payload)
        self.assertEqual(fetch.call_count,3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list],[3,2])

    def test_retry_budget_and_retry_after_date(self):
        response_time = datetime(2026,11,15,tzinfo=timezone.utc)
        header = format_datetime(datetime(2026,11,15,0,0,4,tzinfo=timezone.utc),usegmt=True)
        with patch('bank.external.datetime') as clock, patch('bank.external.time.sleep') as sleep:
            clock.now.return_value=response_time
            with patch('bank.external.urlopen',side_effect=[
                HTTPError(URL,429,'Limit',{'Retry-After':header},None),io.BytesIO(b'{}')]):
                self.assertEqual(fetch_content(URL),b'{}')
            sleep.assert_called_once_with(4)
        with patch('bank.external.urlopen',side_effect=TimeoutError), patch('bank.external.time.sleep') as sleep:
            with self.assertRaises(TimeoutError):
                fetch_content(URL,attempts=2)
            sleep.assert_called_once_with(1)
        with patch('bank.external.urlopen',side_effect=HTTPError(URL,429,'Limit',{'Retry-After':'3600'},None)), \
             patch('bank.external.time.sleep') as sleep:
            with self.assertRaises(HTTPError):
                fetch_content(URL)
            sleep.assert_not_called()

    def test_historical_freshness_and_future_dates(self):
        payload=b'{"date":"2026-11-13","base":"USD","quote":"EUR","rate":0.886}'
        with patch('bank.external.urlopen',return_value=io.BytesIO(payload)) as fetch:
            row=rates(live=True,as_of_date='2026-11-15',max_age_days=3,today=date(2026,11,15))[0]
            self.assertEqual(row['business_date'],'2026-11-13')
            self.assertEqual(fetch.call_args.args[0].full_url,URL+'?date=2026-11-15')
        for as_of,age in [('2026-11-15',1),('2026-11-12',7)]:
            with patch('bank.external.urlopen',return_value=io.BytesIO(payload)):
                with self.assertRaises(ValueError):
                    rates(live=True,as_of_date=as_of,max_age_days=age,today=date(2026,11,15))
        with patch('bank.external.urlopen') as fetch:
            with self.assertRaises(ValueError):
                rates(live=True,as_of_date='2026-11-16',today=date(2026,11,15))
            fetch.assert_not_called()

    def test_invalid_policy_and_oversized_response(self):
        for policy in [{'timeout':0},{'attempts':0},{'attempts':6},{'max_delay':121}]:
            with self.subTest(policy=policy),self.assertRaises(ValueError):
                fetch_content(URL,**policy)
        with patch('bank.external.urlopen',return_value=io.BytesIO(b'x'*(1024*1024+1))):
            with self.assertRaises(ValueError):
                fetch_content(URL)
