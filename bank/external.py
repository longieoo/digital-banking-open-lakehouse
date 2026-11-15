"""Public reference rates with an explicitly labeled deterministic offline fixture."""
from datetime import date, datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
import json
import os
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from bank.ingestion import MemorySource, State, ingest

URL = 'https://api.frankfurter.dev/v2/rate/USD/EUR'


def fetch_content(url, *, timeout=20, attempts=3, backoff=1, max_delay=60):
    """Retry only transient failures, with bounded work and Retry-After support."""
    if type(attempts) is not int or not 0 < timeout <= 120 or not 1 <= attempts <= 5 or not 0 <= backoff <= max_delay <= 120:
        raise ValueError('Invalid FX HTTP retry policy')
    request = Request(url,headers={'User-Agent':'bank-lakehouse/0.2','Accept':'application/json'})
    for attempt in range(attempts):
        try:
            with urlopen(request,timeout=timeout) as response:
                content = response.read(1024*1024+1)
                if len(content) > 1024*1024:
                    raise ValueError('FX response too large')
                return content
        except (HTTPError, URLError, TimeoutError, ConnectionError) as error:
            if isinstance(error,HTTPError) and error.code not in {408,429,500,502,503,504}:
                error.close()
                raise
            if attempt == attempts-1:
                if isinstance(error,HTTPError):
                    error.close()
                raise
            delay = min(backoff*2**attempt,max_delay)
            if isinstance(error,HTTPError):
                retry_after = error.headers.get('Retry-After') if error.headers else None
                error.close()
                if retry_after:
                    try:
                        delay = max(delay,float(retry_after))
                    except ValueError:
                        try:
                            delay = max(delay,(parsedate_to_datetime(retry_after)-
                                datetime.now(timezone.utc)).total_seconds())
                        except (ValueError,TypeError,OverflowError):
                            pass
                    # Never retry earlier than the server's explicit requirement.
                    if delay > max_delay:
                        raise
            time.sleep(max(0,delay))


def rates(live=False, *, as_of_date=None, max_age_days=None, today=None,
          timeout=20, attempts=3):
    reference = date.fromisoformat(as_of_date) if as_of_date else (today or datetime.now(timezone.utc).date())
    if max_age_days is not None and (type(max_age_days) is not int or not 0 <= max_age_days <= 30):
        raise ValueError('FX maximum age must be between 0 and 30 days')
    if live:
        if reference > (today or datetime.now(timezone.utc).date()):
            raise ValueError('Future FX requests are not allowed')
        url = URL + ('?'+urlencode({'date':reference.isoformat()}) if as_of_date else '')
        content = fetch_content(url,timeout=timeout,attempts=attempts)
        origin = 'LIVE'
    else:
        content = Path('sources/external/frankfurter-fixture.json').read_bytes()
        origin = 'OFFLINE_FIXTURE'
    document = json.loads(content,parse_float=Decimal)
    if live:
        if document['base'] != 'USD' or document['quote'] != 'EUR':
            raise ValueError('Unexpected FX currency pair')
        document = dict(date=document['date'],base=document['base'],amount=1,
            rates={document['quote']:document['rate']})
    day = date.fromisoformat(document['date']).isoformat()
    if live and date.fromisoformat(day) > reference:
        raise ValueError('FX response is later than requested date')
    if max_age_days is not None and (reference-date.fromisoformat(day)).days > max_age_days:
        raise ValueError('FX response is stale')
    if document['base'] != 'USD' or Decimal(str(document['amount'])) != 1:
        raise ValueError('Unexpected FX base or amount')
    rows = []
    for quote, rate in sorted(document['rates'].items()):
        value = Decimal(str(rate))
        if not value.is_finite() or value <= 0 or not re.fullmatch('[A-Z]{3}',quote):
            raise ValueError('Invalid FX quote')
        rows.append(dict(rate_id=f'{day}-USD-{quote}',business_date=day,base_currency='USD',
            quote_currency=quote,rate=str(value),origin=origin,updated_at=f'{day}T00:00:00+00:00'))
    return rows


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--live',action='store_true')
    parser.add_argument('--fetch-only',action='store_true',help='Print validated rows without lakehouse ingestion')
    parser.add_argument('--as-of-date',help='Fetch a historical rate, allowing the preceding published rate')
    parser.add_argument('--max-age-days',type=int,help='Maximum calendar days before the requested date')
    args = parser.parse_args()
    if args.live and not args.fetch_only:
        from bank.fx import ingest_live_fx
        print(json.dumps(ingest_live_fx(args.as_of_date,max_age_days=args.max_age_days if args.max_age_days is not None else 7)))
        raise SystemExit(0)
    rows = rates(args.live,as_of_date=args.as_of_date,max_age_days=args.max_age_days)
    if args.fetch_only:
        print(json.dumps(rows))
        raise SystemExit(0)
    from bank.adapters import IcebergBronze,iceberg_catalog,raw_store
    # Keep fixture and live versions in different tables: provenance is part of meaning.
    table = 'fx_rates_live' if args.live else 'fx_rates_fixture'
    result = ingest(MemorySource(rows,'rate_id'),raw_store(),IcebergBronze(iceberg_catalog(),table),
        State(os.environ.get('CONTROL_PATH','runtime/control.db')),source_name=f'external.{table}',key='rate_id')
    print(json.dumps(result))
