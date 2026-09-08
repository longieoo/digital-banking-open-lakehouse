"""Bounded metadata-driven runner after explicit source adapters are established."""
from datetime import date
import json
import os
from pathlib import Path
from bank.adapters import TABLES, PostgresSource, IcebergBronze, iceberg_catalog, raw_store
from bank.ingestion import State, ingest


def load_config(path='config/ingestion.json'):
    config = json.loads(Path(path).read_text())
    names = [r['source_table'] for r in config]
    if len(set(names)) != len(names):
        raise ValueError('Duplicate configured table')
    for item in config:
        table = item['source_table']
        if table not in TABLES or item['primary_key'] != TABLES[table][1]:
            raise ValueError('Unsupported source contract')
        if item['source_system'] != TABLES[table][0] or item['target_table'] != f'bronze.{table}':
            raise ValueError('Unsupported routing')
        if item['incremental_key'] != 'updated_at' or item['load_strategy'] != 'versioned_incremental':
            raise ValueError('Unsupported load strategy')
    return config


def refresh_controls():
    import psycopg
    with psycopg.connect(connect_timeout=10) as db:
        db.execute('''INSERT INTO core.source_totals
          SELECT business_date::text || '-' || currency, business_date, currency, count(*),
          sum(CASE WHEN status='SUCCESS' THEN amount ELSE 0 END), clock_timestamp()
          FROM core.transactions GROUP BY business_date,currency
          ON CONFLICT(total_id) DO UPDATE SET source_count=excluded.source_count,
          source_amount=excluded.source_amount,updated_at=excluded.updated_at''')


def run_all(config_path='config/ingestion.json', dates=None):
    if dates:
        dates = tuple(date.fromisoformat(d).isoformat() for d in dates)
    config = load_config(config_path)
    refresh_controls()
    catalog, raw = iceberg_catalog(), raw_store()
    state = State(os.environ.get('CONTROL_PATH','runtime/control.db'))
    results = []
    for item in config:
        if not item['enabled']:
            continue
        table = item['source_table']
        if dates and table != 'transactions':
            continue
        source = PostgresSource(table)
        bronze = IcebergBronze(catalog,table)
        while True:
            result = ingest(source,raw,bronze,state,source_name=f'{source.schema}.{table}',key=source.key,
                            dates=dates,limit=item['batch_size'])
            print(json.dumps({'table':table, **result}))
            results.append(result)
            if dates or result['count'] < item['batch_size']:
                break
    return results


if __name__ == '__main__':
    run_all()
