"""Bounded metadata-driven runner after explicit source adapters are established."""
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from bank.adapters import TABLES, PostgresSource, IcebergBronze, iceberg_catalog, raw_store
from bank.ingestion import State, Watermark, ingest


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


def run_all(config_path='config/ingestion.json', dates=None, state=None, snapshot_info=None):
    if dates:
        dates = tuple(date.fromisoformat(d).isoformat() for d in dates)
    config = load_config(config_path)
    import psycopg
    catalog, raw = iceberg_catalog(), raw_store()
    state = state or State(os.environ.get('CONTROL_PATH','runtime/control.db'))
    results = []
    # One read snapshot for every page and table; cooperating stream writers wait.
    with state.exclusive(), psycopg.connect(connect_timeout=10) as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        snapshot, captured_at = connection.execute('SELECT pg_current_snapshot()::text, clock_timestamp()').fetchone()
        if snapshot_info is not None:
            snapshot_info.update(postgres_snapshot=snapshot,captured_at=captured_at.isoformat())
            minimum = connection.execute('SELECT min(business_date) FROM core.transactions').fetchone()[0]
            if dates and minimum and dates[0] > minimum.isoformat():
                raise ValueError('Zero-opening releases must include the complete ledger history')
        for item in config:
            if not item['enabled']:
                continue
            table = item['source_table']
            source = PostgresSource(table, connection)
            if table == 'source_totals':
                source = snapshot_controls(connection,captured_at.isoformat())
            bronze = IcebergBronze(catalog,table)
            if dates:
                # Related facts and all dimensions are replayed together, in pages.
                source = PagedBackfill(source, dates if table in DATED_TABLES or table in RELATED_TABLES else None)
            while True:
                result = ingest(source,raw,bronze,state,source_name=f'{TABLES[table][0]}.{table}',
                                key=TABLES[table][1],dates=dates,limit=item['batch_size'],
                                cursor=source.cursor if dates else None,
                                request_token=f'{snapshot}@{captured_at.isoformat()}')
                if dates:
                    source.advance(result['end_watermark'])
                print(json.dumps({'table':table, **result}))
                results.append(result)
                if result['count'] < item['batch_size']:
                    break
    return results


DATED_TABLES = {'transactions', 'journal_entries', 'source_totals'}
RELATED_TABLES = {
    'transfers': ('transfer_id','transactions','transfer_id'),
    'card_transactions': ('transaction_id','transactions','record_id'),
    'journal_lines': ('entry_id','journal_entries','record_id'),
}


class PagedBackfill:
    """Keyset pagination without changing durable forward watermarks."""
    def __init__(self, source, dates):
        from bank.ingestion import MemorySource
        if isinstance(source, MemorySource) and dates:
            source = MemorySource([r for r in source.rows if dates[0] <= r['business_date'] <= dates[1]],
                                  source.key)
        self.source, self.dates, self.cursor = source, dates, Watermark()

    def extract(self, start, limit, dates):
        # Postgres uses a bounded cursor even during a historical replay.
        if isinstance(self.source, PostgresSource):
            from psycopg import sql
            from psycopg.rows import dict_row
            from bank.adapters import json_value
            predicate = sql.SQL('(updated_at, {key}) > (%s::timestamptz, %s)').format(
                key=sql.Identifier(self.source.key))
            params = [self.cursor.updated_at, self.cursor.record_id]
            if self.dates:
                related = RELATED_TABLES.get(self.source.table)
                if related:
                    child_key,parent,parent_key = related
                    parent_key = TABLES[parent][1] if parent_key=='record_id' else parent_key
                    predicate += sql.SQL(' AND {} IN (SELECT {} FROM {}.{} WHERE business_date BETWEEN %s AND %s)').format(
                        sql.Identifier(child_key),sql.Identifier(parent_key),sql.Identifier(TABLES[parent][0]),
                        sql.Identifier(parent))
                else:
                    predicate += sql.SQL(' AND business_date BETWEEN %s AND %s')
                params.extend(self.dates)
            query = sql.SQL('SELECT * FROM {}.{} WHERE {} ORDER BY updated_at, {} LIMIT %s').format(
                sql.Identifier(self.source.schema),sql.Identifier(self.source.table),predicate,
                sql.Identifier(self.source.key))
            with self.source.connection.cursor(row_factory=dict_row) as cursor:
                rows = [{k:json_value(v) for k,v in row.items()}
                        for row in cursor.execute(query,(*params,limit)).fetchall()]
        else:
            rows = self.source.extract(self.cursor, limit, None)
            if self.dates:
                rows = [r for r in rows if self.dates[0] <= r['business_date'] <= self.dates[1]]
        # Advance only when ingest returns, so a failed call keeps the same page.
        return rows

    def advance(self, result):
        self.cursor = Watermark(**result)


def snapshot_controls(connection, stamp=None):
    from psycopg.rows import dict_row
    from bank.adapters import json_value
    from bank.ingestion import MemorySource
    with connection.cursor(row_factory=dict_row) as cursor:
        rows = cursor.execute('''SELECT business_date::text || '-' || currency AS total_id,
            business_date, currency, count(*) AS source_count,
            sum(CASE WHEN status='SUCCESS' THEN amount ELSE 0 END) AS source_amount
            FROM core.transactions GROUP BY business_date,currency ORDER BY business_date,currency''').fetchall()
    stamp = stamp or datetime.now(timezone.utc).isoformat()
    return MemorySource([{**{k:json_value(v) for k,v in row.items()},'updated_at':stamp}
                         for row in rows],'total_id')


if __name__ == '__main__':
    run_all()
