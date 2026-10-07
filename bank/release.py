"""Snapshot-pinned data releases and atomic publication of validated BI exports."""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import uuid

from bank.ingestion import State


def release_id(value):
    if not re.fullmatch('[a-f0-9]{32}',value):
        raise ValueError('Invalid release ID')
    return value


def directory(value):
    return Path(os.environ.get('RELEASE_ROOT','runtime/releases')) / release_id(value)


def atomic_json(path, document):
    path = Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=path.parent,delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(document,handle,indent=2,sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary,path)
    if os.name != 'nt':
        descriptor = os.open(path.parent,os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def code_digest():
    root = Path(__file__).resolve().parent.parent
    digest = hashlib.sha256()
    for folder in ('bank','processing','dbt','orchestration','streaming'):
        for path in sorted((root/folder).rglob('*')):
            if path.suffix in {'.py','.sql','.yml'} and path.name != '.user.yml' and not {'target','logs','__pycache__'} & set(path.parts):
                digest.update(str(path.relative_to(root)).replace('\\','/').encode())
                digest.update(path.read_bytes())
    return digest.hexdigest()


def load(value):
    document = json.loads((directory(value)/'release.json').read_text())
    if document['release_id']!=value or any(document[key]!=f'{layer}_r_{value}'
            for key,layer in [('bronze_schema','bronze'),('silver_schema','silver'),('gold_schema','gold')]):
        raise ValueError('Invalid release routing')
    return document


def prepare(value, start_date, as_of_date, currency='USD'):
    from bank.adapters import TABLES, iceberg_catalog
    from bank.pipeline import run_all
    value = release_id(value)
    start, end = date.fromisoformat(start_date),date.fromisoformat(as_of_date)
    if start > end or currency not in {'USD','EUR','VND'}:
        raise ValueError('Invalid release bounds or currency')
    path = directory(value)/'release.json'
    state = State(os.environ.get('CONTROL_PATH','runtime/control.db'))
    with state.exclusive():
        if path.exists():
            existing = load(value)
            if (existing['start_date'],existing['as_of_date'],existing['reporting_currency']) != (
                    start.isoformat(),end.isoformat(),currency):
                raise ValueError('Release ID already belongs to another request')
            return existing
        info = {}
        results = run_all(dates=(start.isoformat(),end.isoformat()),state=state,snapshot_info=info)
        catalog = iceberg_catalog()
        snapshots = {}
        for table in TABLES:
            snapshot = catalog.load_table(f'bronze.{table}').current_snapshot()
            snapshots[table] = snapshot.snapshot_id if snapshot else None
        document = dict(release_id=value,start_date=start.isoformat(),as_of_date=end.isoformat(),
            reporting_currency=currency,source_cutoff=info,bronze_snapshots=snapshots,
            bronze_schema=f'bronze_r_{value}',silver_schema=f'silver_r_{value}',gold_schema=f'gold_r_{value}',
            status='SOURCE_READY',code_revision=os.environ.get('CODE_REVISION','unrecorded'),
            prepare_code_digest=code_digest(),
            source_batches=[r['batch_id'] for r in results])
        atomic_json(path,document)
        return document


def dbt_vars(document):
    return {key:document[key] for key in ('start_date','as_of_date','reporting_currency',
                                         'bronze_schema','silver_schema')}


def check_build(document, artifact_root):
    root = Path(artifact_root)
    manifest = json.loads((root/'manifest.json').read_text())
    results = json.loads((root/'run_results.json').read_text())
    required = {key for key,node in manifest['nodes'].items()
                if node['resource_type'] in {'model','test'} and node['package_name']=='banking'}
    actual = {row['unique_id']:row for row in results['results']}
    if not required or required != set(actual) or len(actual) != len(results['results']):
        raise ValueError('Incomplete dbt build')
    if any(row['status'] not in {'success','pass'} for row in actual.values()):
        raise ValueError('Failed or skipped dbt quality gate')
    models = [node for node in manifest['nodes'].values() if node['resource_type']=='model']
    if any(node['schema'] != document['gold_schema'] for node in models):
        raise ValueError('Build does not belong to this release')
    if results['args'].get('vars') != dbt_vars(document):
        raise ValueError('Build used different reporting parameters')
    expected_sources = {'bronze':document['bronze_schema'],'silver':document['silver_schema']}
    if any(node['schema'] != expected_sources[node['source_name']]
           for node in manifest['sources'].values()):
        raise ValueError('Build did not read pinned sources')
    return sorted(node['name'] for node in models)


class Trino:
    """Local release service client; shared deployments require external authentication."""
    def __init__(self):
        self.base = os.environ.get('TRINO_URL',f'http://{os.environ.get("TRINO_HOST","trino")}:'
                                   f'{os.environ.get("TRINO_PORT","8080")}')

    def pages(self, sql):
        request = Request(self.base+'/v1/statement',data=sql.encode(),headers={
            'X-Trino-User':os.environ.get('TRINO_USER','transformer'),'Content-Type':'text/plain'})
        while True:
            with urlopen(request,timeout=60) as response:
                page = json.loads(response.read(),parse_float=Decimal)
            if 'error' in page:
                raise RuntimeError('Trino query failed')
            yield page
            next_uri = page.get('nextUri')
            if not next_uri:
                return
            if urlparse(next_uri).netloc != urlparse(self.base).netloc:
                raise ValueError('Unexpected Trino next URI')
            request = Request(next_uri,headers={'X-Trino-User':os.environ.get('TRINO_USER','transformer')})

    def scalar(self, sql):
        rows = [row for page in self.pages(sql) for row in page.get('data',[])]
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ValueError('Expected one query result')
        return rows[0][0]


def certify(value, artifact_root, client=None):
    document = load(value)
    if document['status']=='CERTIFIED':
        verify(value)
        publish(document)
        return document
    if document['status'] != 'SILVER_READY' or document['silver_quality']['rejected']:
        raise ValueError('Silver quality gate has not passed')
    models = check_build(document,artifact_root)
    client = client or Trino()
    schema = document['gold_schema']
    if client.scalar(f"SELECT count(*) FROM lake.{schema}.mart_reconciliation_daily "
                     "WHERE reconciliation_status <> 'PASS'"):
        raise ValueError('Reconciliation failed')
    if not client.scalar(f'SELECT count(*) FROM lake.{schema}.mart_reconciliation_daily'):
        raise ValueError('No reconciliation evidence')
    exports = directory(value)/'exports'
    exports.mkdir(exist_ok=True)
    files = {}
    for model in models:
        if not re.fullmatch('[a-z][a-z0-9_]*',model):
            raise ValueError('Invalid export identifier')
        path = exports/f'{model}.csv'
        count = 0
        with path.open('w',newline='',encoding='utf-8') as handle:
            writer = csv.writer(handle)
            header_written = False
            for page in client.pages(f'SELECT * FROM lake.{schema}.{model}'):
                if not header_written and page.get('columns'):
                    writer.writerow([column['name'] for column in page['columns']])
                    header_written = True
                for row in page.get('data',[]):
                    writer.writerow(['' if cell is None else cell for cell in row])
                    count += 1
            if not header_written:
                raise ValueError('Missing export schema')
            handle.flush()
            os.fsync(handle.fileno())
        digest = hashlib.sha256()
        with path.open('rb') as handle:
            for chunk in iter(lambda:handle.read(1024*1024),b''):
                digest.update(chunk)
        files[model] = dict(rows=count,sha256=digest.hexdigest(),file=f'exports/{model}.csv')
    document.update(status='CERTIFIED',certified_at=datetime.now(timezone.utc).isoformat(),exports=files,
                    validation_code_digest=code_digest())
    publish(document)
    return document


def publish(document):
    value = release_id(document['release_id'])
    # Readers resolve current.json once and consume only immutable certified files.
    state = State(os.environ.get('CONTROL_PATH','runtime/control.db'))
    with state.exclusive():
        atomic_json(directory(value)/'release.json',document)
        current = directory(value).parent/'current.json'
        previous = json.loads(current.read_text()) if current.exists() else None
        if previous and (previous['as_of_date'] > document['as_of_date']):
            # Historical certification never moves the current reporting date backwards.
            return document
        if previous and previous['as_of_date']==document['as_of_date']:
            prior = load(previous['release_id'])
            if prior.get('source_cutoff',{}).get('captured_at','') > document.get('source_cutoff',{}).get('captured_at',''):
                return document
        atomic_json(current,{'release_id':value,'as_of_date':document['as_of_date'],
                             'manifest':f'{value}/release.json'})
    return document


def verify(value):
    """Verify a certified export after copying it to another machine or restoring it."""
    document = load(value)
    if document['status'] != 'CERTIFIED':
        raise ValueError('Release is not certified')
    for model,metadata in document['exports'].items():
        if not re.fullmatch('[a-z][a-z0-9_]*',model) or metadata['file'] != f'exports/{model}.csv':
            raise ValueError('Invalid export path')
        digest = hashlib.sha256()
        with (directory(value)/metadata['file']).open('rb') as handle:
            for chunk in iter(lambda:handle.read(1024*1024),b''):
                digest.update(chunk)
        if digest.hexdigest()!=metadata['sha256']:
            raise ValueError('Corrupt certified export')
    return document


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('action',choices=['prepare','build','certify','verify','show'])
    parser.add_argument('--release-id',default=uuid.uuid4().hex)
    parser.add_argument('--start-date',default='2026-01-01')
    parser.add_argument('--as-of-date',default='2026-01-31')
    parser.add_argument('--currency',default='USD')
    parser.add_argument('--artifacts')
    parser.add_argument('--dbt-executable',default='dbt')
    args = parser.parse_args()
    if args.action=='prepare':
        result = prepare(args.release_id,args.start_date,args.as_of_date,args.currency)
    elif args.action=='build':
        import subprocess
        result = load(args.release_id)
        root = directory(args.release_id).parent.parent/'dbt-target'/args.release_id
        if result['status']!='CERTIFIED':
            subprocess.run([args.dbt_executable,'build','--project-dir','/opt/bank/dbt',
                '--profiles-dir','/opt/bank/dbt','--target-path',str(root.resolve()),
                '--vars',json.dumps(dbt_vars(result))],check=True,
                env={**os.environ,'DBT_SCHEMA':result['gold_schema']})
        result = certify(args.release_id,root)
    elif args.action=='certify':
        result = certify(args.release_id,args.artifacts)
    elif args.action=='verify':
        result = verify(args.release_id)
    else:
        result = load(args.release_id)
    print(json.dumps(result))


if __name__=='__main__':
    try:
        main()
    except Exception as exc:
        raise SystemExit(type(exc).__name__)
