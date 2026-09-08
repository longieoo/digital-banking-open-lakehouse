import argparse
import json
import os
from pathlib import Path
from bank.generator import generate
from bank.ingestion import State, ingest


def main():
    parser = argparse.ArgumentParser(description='Synthetic banking lakehouse')
    sub = parser.add_subparsers(dest='command', required=True)
    seed = sub.add_parser('seed')
    seed.add_argument('--profile', choices=['small','medium','large'], default='small')
    seed.add_argument('--customers', type=int)
    seed.add_argument('--transactions', type=int)
    seed.add_argument('--seed', type=int, default=42)
    seed.add_argument('--output', help='Write JSON fixture instead of PostgreSQL')
    extract = sub.add_parser('ingest')
    extract.add_argument('--table', default='transactions')
    extract.add_argument('--limit', type=int, default=10000)
    extract.add_argument('--start-date')
    extract.add_argument('--end-date')
    extract.add_argument('--fail-at', choices=['raw','bronze','after_commit'])
    sub.add_parser('manifests')
    args = parser.parse_args()
    try:
        if args.command == 'seed':
            data = generate(args.profile, args.seed, args.customers, args.transactions)
            if args.output:
                path = Path(args.output)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(data, indent=2))
            else:
                from bank.adapters import seed_postgres
                seed_postgres(data)
            result = {'status': 'COMPLETED', 'counts': {k: len(v) for k, v in data.items()}}
        elif args.command == 'ingest':
            from bank.adapters import PostgresSource, IcebergBronze, iceberg_catalog, raw_store
            if bool(args.start_date) != bool(args.end_date):
                raise ValueError('Both backfill dates required')
            source = PostgresSource(args.table)
            state = State(os.environ.get('CONTROL_PATH','runtime/control.db'))
            result = ingest(source, raw_store(), IcebergBronze(iceberg_catalog(), args.table), state,
                source_name=f'{source.schema}.{args.table}', key=source.key, limit=args.limit,
                dates=(args.start_date,args.end_date) if args.start_date else None, fail_at=args.fail_at)
        else:
            result = State(os.environ.get('CONTROL_PATH','runtime/control.db')).manifests()
        print(json.dumps(result))
    except Exception as exc:
        print(json.dumps({'status': 'FAILED', 'error_type': type(exc).__name__}))
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
