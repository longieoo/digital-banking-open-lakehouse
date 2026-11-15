"""Compact one live Bronze table during a quiet ingestion window."""
import argparse
import json
import os

from bank.adapters import TABLES
from bank.ingestion import State
from processing.silver import session


def compact(spark, table):
    if table not in TABLES:
        raise ValueError('Unknown Bronze table')
    target = f'lake.bronze.{table}'
    snapshot = spark.sql(f"SELECT snapshot_id FROM {target}.refs WHERE name='main'").first()
    if snapshot is None:
        return {'table': table, 'files': 0}
    previous = spark.read.format('iceberg').option('snapshot-id', str(snapshot.snapshot_id)).load(target)
    before = spark.table(f'{target}.files').count()
    result = spark.sql(f"""CALL lake.system.rewrite_data_files(
        table => 'bronze.{table}', options => map('min-input-files','2'))""").first()
    current = spark.table(target)
    if previous.exceptAll(current).take(1) or current.exceptAll(previous).take(1):
        raise ValueError('Compaction changed the row multiset or a concurrent write occurred')
    return {'table': table, 'before_files': before, 'after_files': spark.table(f'{target}.files').count(),
            'preserved_snapshot': snapshot.snapshot_id, 'rewrite': result.asDict()}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table', choices=sorted(TABLES), required=True)
    args = parser.parse_args()
    spark = session()
    try:
        with State(os.environ.get('CONTROL_PATH','runtime/control.db')).exclusive():
            print(json.dumps(compact(spark, args.table)))
    finally:
        spark.stop()
