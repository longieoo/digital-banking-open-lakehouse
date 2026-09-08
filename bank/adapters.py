"""Real operational and object-storage adapters; imports are lazy for contract tests."""
from datetime import date, datetime
from decimal import Decimal
import os
from pathlib import Path
from urllib.parse import urlparse
from bank.ingestion import utc

TABLES = {
    'branches': ('core', 'branch_id'), 'customers': ('core', 'customer_id'),
    'customer_history': ('core', 'customer_id'), 'accounts': ('core', 'account_id'),
    'transfers': ('core', 'transfer_id'), 'transactions': ('core', 'transaction_id'),
    'cards': ('card', 'card_id'), 'card_transactions': ('card', 'card_transaction_id'),
    'gl_accounts': ('gl', 'gl_account_id'), 'journal_entries': ('gl', 'entry_id'),
    'journal_lines': ('gl', 'line_id'), 'source_totals': ('core', 'total_id'),
}


def json_value(value):
    if isinstance(value, datetime):
        return utc(value)
    if isinstance(value, (date, Decimal)):
        return str(value)
    return value


class PostgresSource:
    def __init__(self, table):
        self.table = table
        self.schema, self.key = TABLES[table]  # identifier allowlist

    def extract(self, start, limit, dates):
        import psycopg
        from psycopg import sql
        from psycopg.rows import dict_row
        where = sql.SQL("business_date BETWEEN %s AND %s") if dates else sql.SQL(
            "(updated_at, {key}) > (%s::timestamptz, %s)").format(key=sql.Identifier(self.key))
        query = sql.SQL("SELECT * FROM {}.{} WHERE {} ORDER BY updated_at, {}").format(
            sql.Identifier(self.schema), sql.Identifier(self.table), where, sql.Identifier(self.key))
        params = dates if dates else (start.updated_at, start.record_id, limit)
        if not dates:
            query += sql.SQL(" LIMIT %s")
        with psycopg.connect(row_factory=dict_row, connect_timeout=10) as db:
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            rows = db.execute(query, params).fetchall()
        return [{k: json_value(v) for k, v in row.items()} for row in rows]


def seed_postgres(data):
    import psycopg
    from psycopg import sql
    with psycopg.connect(connect_timeout=10) as db:
        db.execute(Path('sources/schema.sql').read_text())
        db.execute(Path('sources/security.sql').read_text())
        for table, (schema, key) in TABLES.items():
            rows = data[table]
            if not rows:
                continue
            columns = list(rows[0])
            conflict = [key, 'updated_at'] if table == 'customer_history' else [key]
            updates = [c for c in columns if c not in conflict]
            query = sql.SQL('INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}').format(
                sql.Identifier(schema), sql.Identifier(table),
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.SQL(',').join(sql.Placeholder() for _ in columns),
                sql.SQL(',').join(map(sql.Identifier, conflict)),
                sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(c), sql.Identifier(c))
                                 for c in updates))
            with db.cursor() as cur:
                cur.executemany(query, [tuple(row[c] for c in columns) for row in rows])


class S3Raw:
    def __init__(self, uri):
        import boto3
        from botocore.config import Config
        parsed = urlparse(uri)
        self.bucket, self.prefix = parsed.netloc, parsed.path.strip('/')
        self.client = boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT'),
                                  config=Config(s3={'addressing_style': 'path'}))

    def put_verified(self, key, content):
        from botocore.exceptions import ClientError
        target = '/'.join(p for p in [self.prefix, key] if p)
        try:
            previous = self.client.get_object(Bucket=self.bucket, Key=target)['Body'].read()
        except ClientError as exc:
            if exc.response['Error']['Code'] not in {'NoSuchKey', '404'}:
                raise
            self.client.put_object(Bucket=self.bucket, Key=target, Body=content,
                                   ContentType='application/json', IfNoneMatch='*')
            previous = self.client.get_object(Bucket=self.bucket, Key=target)['Body'].read()
        if previous != content:
            raise ValueError('Object checksum/content conflict')


class AzureRaw:
    def __init__(self, uri):
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
        parsed = urlparse(uri)
        self.container = parsed.netloc.split('@')[0]
        self.prefix = parsed.path.strip('/')
        account = os.environ['AZURE_STORAGE_ACCOUNT_NAME']
        self.client = BlobServiceClient(f'https://{account}.blob.core.windows.net',
                                        credential=DefaultAzureCredential())

    def put_verified(self, key, content):
        from azure.core.exceptions import ResourceExistsError
        blob = self.client.get_blob_client(self.container, '/'.join(p for p in [self.prefix, key] if p))
        try:
            blob.upload_blob(content, overwrite=False)
        except ResourceExistsError:
            pass
        if blob.download_blob().readall() != content:
            raise ValueError('Azure raw checksum/content conflict')


def raw_store():
    from bank.ingestion import FileRaw
    uri = os.environ.get('RAW_URI', 'runtime/raw')
    if uri.startswith('s3://'):
        return S3Raw(uri)
    if uri.startswith('abfss://'):
        return AzureRaw(uri)
    return FileRaw(uri)


def iceberg_catalog():
    from pyiceberg.catalog import load_catalog
    properties = {'type': 'rest', 'uri': os.environ['CATALOG_URI'],
                  'warehouse': os.environ.get('WAREHOUSE_URI', 's3://warehouse')}
    if os.environ.get('STORAGE_BACKEND', 's3') == 's3':
        properties.update({'s3.endpoint': os.environ['S3_ENDPOINT'],
            's3.access-key-id': os.environ['AWS_ACCESS_KEY_ID'],
            's3.secret-access-key': os.environ['AWS_SECRET_ACCESS_KEY'],
            's3.region': os.environ.get('AWS_REGION', 'us-east-1'), 's3.force-virtual-addressing': 'false'})
    else:
        properties.update({'py-io-impl': 'pyiceberg.io.fsspec.FsspecFileIO',
                           'adls.account-name': os.environ['AZURE_STORAGE_ACCOUNT_NAME']})
    return load_catalog('bank', **properties)


class IcebergBronze:
    def __init__(self, catalog, table):
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType
        from pyiceberg.partitioning import PartitionSpec, PartitionField
        from pyiceberg.transforms import IdentityTransform
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        self.catalog, self.identifier = catalog, f'bronze.{table}'
        try:
            catalog.create_namespace('bronze')
        except NamespaceAlreadyExistsError:
            pass
        fields = ['record_id', 'source_updated_at', 'business_date', 'payload', 'batch_id',
                  'source_system', 'source_table']
        schema = Schema(*[NestedField(i + 1, name, StringType(), required=False)
                          for i, name in enumerate(fields)])
        if not catalog.table_exists(self.identifier):
            catalog.create_table(self.identifier, schema=schema,
                partition_spec=PartitionSpec(PartitionField(source_id=3, field_id=1000,
                    transform=IdentityTransform(), name='business_day')),
                properties={'format-version': '2', 'write.parquet.compression-codec': 'zstd'})
        self.table = catalog.load_table(self.identifier)

    def existing(self, rows):
        from pyiceberg.expressions import In
        self.table.refresh()
        if not rows:
            return {}
        found = self.table.scan(row_filter=In('record_id', {r['record_id'] for r in rows})).to_arrow()
        return {(r['record_id'], r['source_updated_at']): r['payload'] for r in found.to_pylist()}

    def commit(self, rows):
        import pyarrow as pa
        old = self.existing(rows)
        new = []
        for row in rows:
            identity = (row['record_id'], row['source_updated_at'])
            if identity in old:
                if old[identity] != row['payload']:
                    raise ValueError('Conflicting immutable source version')
            else:
                new.append(row)
                old[identity] = row['payload']
        if new:
            self.table.append(pa.Table.from_pylist(new, schema=self.table.schema().as_arrow()))
        snapshot = self.table.current_snapshot()
        return str(snapshot.snapshot_id) if snapshot else None

    def validate(self, rows):
        existing = self.existing(rows)
        for row in rows:
            if existing.get((row['record_id'], row['source_updated_at'])) != row['payload']:
                raise ValueError('Iceberg Bronze verification failed')
        return len(rows)
