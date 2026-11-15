"""Scheduled reference FX ingestion, separate from native-currency Gold measures."""
from datetime import date, datetime, timezone
import hashlib
import json
import os

from bank.external import URL, rates
from bank.ingestion import MemorySource, State, canonical, ingest


def ingest_live_fx(as_of_date=None, *, max_age_days=7, state=None, raw=None, bronze=None):
    """Fetch one published day; replays verify the same observation without moving W."""
    requested = date.fromisoformat(as_of_date) if as_of_date else datetime.now(timezone.utc).date()
    rows = rates(live=True,as_of_date=requested.isoformat(),max_age_days=max_age_days)
    if raw is None or bronze is None:
        from bank.adapters import IcebergBronze, iceberg_catalog, raw_store
        raw = raw if raw is not None else raw_store()
        bronze = bronze if bronze is not None else IcebergBronze(iceberg_catalog(),'fx_rates_live')
    state = state if state is not None else State(os.environ.get('CONTROL_PATH','runtime/control.db'))
    day = rows[0]['business_date']
    token = json.dumps({'provider':'Frankfurter v2','endpoint':URL,
        'requested_date':requested.isoformat(),'rate_date':day,
        'payload_sha256':hashlib.sha256(canonical(rows)).hexdigest()},sort_keys=True)
    result = ingest(MemorySource(rows,'rate_id'),raw,bronze,state,
        source_name='external.fx_rates_live',key='rate_id',dates=(day,day),request_token=token)
    return dict(result,origin='LIVE',provider='Frankfurter v2',endpoint=URL,
        requested_date=requested.isoformat(),rate_date=day,
        age_days=(requested-date.fromisoformat(day)).days,used_for_gold_conversion=False)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--as-of-date')
    parser.add_argument('--max-age-days',type=int,default=7)
    args = parser.parse_args()
    try:
        print(json.dumps(ingest_live_fx(args.as_of_date,max_age_days=args.max_age_days)))
    except Exception as error:
        print(json.dumps({'status':'FAILED','error_type':type(error).__name__}))
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
