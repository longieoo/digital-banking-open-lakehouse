"""Public reference rates with an explicitly labeled deterministic offline fixture."""
from datetime import date
from decimal import Decimal
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
from bank.ingestion import MemorySource, State, ingest

URL = 'https://api.frankfurter.dev/v2/rate/USD/EUR'


def rates(live=False):
    if live:
        request = Request(URL,headers={'User-Agent':'bank-lakehouse/0.1','Accept':'application/json'})
        with urlopen(request,timeout=20) as response:
            content = response.read()
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
    if document['base'] != 'USD' or Decimal(str(document['amount'])) != 1:
        raise ValueError('Unexpected FX base or amount')
    rows = []
    for quote, rate in sorted(document['rates'].items()):
        value = Decimal(str(rate))
        if not value.is_finite() or value <= 0 or len(quote) != 3:
            raise ValueError('Invalid FX quote')
        rows.append(dict(rate_id=f'{day}-USD-{quote}',business_date=day,base_currency='USD',
            quote_currency=quote,rate=str(value),origin=origin,updated_at=f'{day}T00:00:00+00:00'))
    return rows


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--live',action='store_true')
    parser.add_argument('--fetch-only',action='store_true',help='Print validated rows without lakehouse ingestion')
    args = parser.parse_args()
    rows = rates(args.live)
    if args.fetch_only:
        print(json.dumps(rows))
        raise SystemExit(0)
    from bank.adapters import IcebergBronze,iceberg_catalog,raw_store
    # Keep fixture and live versions in different tables: provenance is part of meaning.
    table = 'fx_rates_live' if args.live else 'fx_rates_fixture'
    result = ingest(MemorySource(rows,'rate_id'),raw_store(),IcebergBronze(iceberg_catalog(),table),
        State(os.environ.get('CONTROL_PATH','runtime/control.db')),source_name=f'external.{table}',key='rate_id')
    print(json.dumps(result))
