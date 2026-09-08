"""Measured local comparison. Does not claim distributed/storage performance."""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import platform
import tempfile
from time import perf_counter


def run(count):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.catalog import load_catalog
    from bank.adapters import IcebergBronze
    with ExitStack() as stack:
        root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        catalog = load_catalog('bench', type='sql', uri='sqlite:///' + (root/'catalog.db').as_posix(),
                               warehouse=Path(os.path.relpath(root/'warehouse')).as_posix())
        stack.callback(catalog.engine.dispose)
        bronze = IcebergBronze(catalog,'comparison')
        rows = [dict(record_id=f'SYN-T{i}',source_updated_at='2026-01-01T00:00:00Z',
                     business_date='2026-01-01',payload=json.dumps({'amount':f'{i % 100}.00'}),
                     batch_id='benchmark',source_system='core',source_table='transactions') for i in range(count)]
        arrow = pa.Table.from_pylist(rows, schema=bronze.table.schema().as_arrow())
        start = perf_counter()
        pq.write_to_dataset(arrow, root_path=str(root/'unmanaged'), partition_cols=['business_date'],compression='zstd')
        parquet_write = perf_counter() - start
        start = perf_counter()
        bronze.commit(rows)
        iceberg_write = perf_counter() - start
        start = perf_counter()
        parquet_count = pq.ParquetDataset(root/'unmanaged').read().num_rows
        parquet_read = perf_counter() - start
        start = perf_counter()
        iceberg_count = bronze.table.scan().to_arrow().num_rows
        iceberg_read = perf_counter() - start
        assert parquet_count == iceberg_count == count
        return dict(rows=count, parquet_write_seconds=parquet_write, iceberg_write_seconds=iceberg_write,
            parquet_read_seconds=parquet_read, iceberg_read_seconds=iceberg_read,
            iceberg_data_files=bronze.table.inspect.files().num_rows,
            snapshots=len(bronze.table.metadata.snapshots), checks='PASS')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows',nargs='+',type=int,default=[1000,10000])
    parser.add_argument('--output',default='runtime/iceberg-parquet.json')
    args = parser.parse_args()
    result = dict(environment=platform.platform(),python=platform.python_version(),cpu_count=os.cpu_count(),
        scope='single local process, SQL catalog, filesystem, cold-ish single samples; not a statistical benchmark',
        measurements=[run(n) for n in args.rows])
    path = Path(args.output)
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(result,indent=2))
    print(json.dumps(result))
