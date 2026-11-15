"""Scheduled reference FX ingestion, separate from native-currency Gold measures."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os

from bank.external import URL, rates
from bank.ingestion import MemorySource, State, canonical, ingest, utc


def version_observations(rows, state, observed_at=None):
    """Version provider content changes, preserving observed order and replay identity.

    Frankfurter publishes a rate date, not a revision timestamp. updated_at therefore
    represents a locally observed content version, never a claimed provider revision.
    The caller holds state.exclusive through the following durable ingestion.
    """
    clock = datetime.fromisoformat(utc(observed_at or datetime.now(timezone.utc).isoformat()))
    if not rows or len({row['rate_id'] for row in rows})!=len(rows):
        raise ValueError('Expected unique FX observations')
    versioned = []
    with state.connect() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS fx_observations (
            rate_id TEXT NOT NULL, observed_at TEXT NOT NULL, content_sha256 TEXT NOT NULL,
            payload TEXT NOT NULL, PRIMARY KEY(rate_id,observed_at))''')
        for source in rows:
            row = {key:value for key,value in source.items() if key not in {'updated_at','observed_at'}}
            rate = Decimal(str(row['rate']))
            if not rate.is_finite() or rate<=0 or abs(rate.adjusted())>24 or len(rate.as_tuple().digits)>64:
                raise ValueError('FX rate outside supported numeric bounds')
            text = format(rate,'f')
            row['rate'] = text.rstrip('0').rstrip('.') if '.' in text else text
            digest = hashlib.sha256(canonical(row)).hexdigest()
            previous = db.execute('SELECT * FROM fx_observations WHERE rate_id=? '
                'ORDER BY observed_at DESC LIMIT 1',(row['rate_id'],)).fetchone()
            if previous and previous['content_sha256']==digest:
                versioned.append(json.loads(previous['payload']))
                continue
            stamp = max(clock,datetime.fromisoformat(previous['observed_at'])+timedelta(microseconds=1)) if previous else clock
            row.update(observed_at=utc(stamp.isoformat()),updated_at=utc(stamp.isoformat()))
            payload = canonical(row).decode()
            db.execute('INSERT INTO fx_observations VALUES(?,?,?,?)',
                (row['rate_id'],row['observed_at'],digest,payload))
            versioned.append(row)
    return versioned


def ingest_live_fx(as_of_date=None, *, max_age_days=7, state=None, raw=None, bronze=None):
    """Fetch one published day; replays verify the same observation without moving W."""
    requested = date.fromisoformat(as_of_date) if as_of_date else datetime.now(timezone.utc).date()
    rows = rates(live=True,as_of_date=requested.isoformat(),max_age_days=max_age_days)
    if raw is None or bronze is None:
        from bank.adapters import IcebergBronze, iceberg_catalog, raw_store
        raw = raw if raw is not None else raw_store()
        bronze = bronze if bronze is not None else IcebergBronze(iceberg_catalog(),'fx_rates_live')
    state = state if state is not None else State(os.environ.get('CONTROL_PATH','runtime/control.db'))
    with state.exclusive():
        rows = version_observations(rows,state)
        day = rows[0]['business_date']
        token = json.dumps({'provider':'Frankfurter v2','endpoint':URL,
            'requested_date':requested.isoformat(),'rate_date':day,
            'observation_policy':'content-change-v1',
            'payload_sha256':hashlib.sha256(canonical(rows)).hexdigest()},sort_keys=True)
        result = ingest(MemorySource(rows,'rate_id'),raw,bronze,state,
            source_name='external.fx_rates_live',key='rate_id',dates=(day,day),request_token=token)
    return dict(result,origin='LIVE',provider='Frankfurter v2',endpoint=URL,
        requested_date=requested.isoformat(),rate_date=day,
        age_days=(requested-date.fromisoformat(day)).days,used_for_gold_conversion=False,
        observed_at=rows[0]['observed_at'],version_policy='content-change-v1')


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
