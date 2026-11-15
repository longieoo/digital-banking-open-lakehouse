"""Snapshot-pinned data releases and atomic publication of validated BI exports."""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
import uuid

from bank.ingestion import State
from bank.locking import file_lock


def release_id(value):
    if not re.fullmatch('[a-f0-9]{32}',value):
        raise ValueError('Invalid release ID')
    return value


def directory(value, root=None):
    base = Path(root if root is not None else os.environ.get('RELEASE_ROOT','runtime/releases'))
    path = base / release_id(value)
    if path.is_symlink() or path.resolve().parent != base.resolve():
        raise ValueError('Release path escapes configured root')
    return path


def release_lock(value, root=None, timeout=600):
    """Always acquire before the global publication lock; readers need neither."""
    folder = directory(value,root)
    return file_lock(folder.parent/'.release-locks'/f'{release_id(value)}.db',timeout=timeout)


def _sync_directory(path):
    if os.name != 'nt':
        descriptor = os.open(path,os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def atomic_json(path, document):
    path = Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=path.parent,delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(document,handle,indent=2,sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary,path)
        _sync_directory(path.parent)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def code_digest():
    root = Path(__file__).resolve().parent.parent
    digest = hashlib.sha256()
    for folder in ('bank','processing','dbt','orchestration','streaming','config','query','sources','docker','scripts'):
        for path in sorted((root/folder).rglob('*')):
            if path.suffix in {'.py','.sql','.yml','.json','.html','.css','.js','.properties','.config','.Dockerfile','.example'} and path.name != '.user.yml' and not {'target','logs','__pycache__'} & set(path.parts):
                digest.update(str(path.relative_to(root)).replace('\\','/').encode())
                digest.update(path.read_bytes())
    for name in ('pyproject.toml','requirements-test.txt','docker-compose.yml','docker-compose.ci.yml'):
        path = root/name
        if path.is_file():
            digest.update(name.encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


def load(value, root=None):
    path = directory(value,root)/'release.json'
    if path.is_symlink():
        raise ValueError('Release manifest must not be a symbolic link')
    document = json.loads(path.read_text(encoding='utf-8'))
    if document['release_id']!=value or any(document[key]!=f'{layer}_r_{value}'
            for key,layer in [('bronze_schema','bronze'),('silver_schema','silver'),('gold_schema','gold')]):
        raise ValueError('Invalid release routing')
    return document


def prepare(value, start_date, as_of_date, currency='USD'):
    with release_lock(value):
        return _prepare(value,start_date,as_of_date,currency)


def _prepare(value, start_date, as_of_date, currency='USD'):
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
        results = run_all(dates=(start.isoformat(),end.isoformat()),state=state,snapshot_info=info,
                          membership_root=path.parent/'source-membership')
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
    variables = {key:document[key] for key in ('start_date','as_of_date','reporting_currency',
                                         'bronze_schema','silver_schema')}
    if document.get('gold_baseline'):
        variables['baseline_schema'] = 'gold_r_' + release_id(document['gold_baseline'])
    return variables


def project_models():
    models = {path.stem for path in (Path(__file__).resolve().parent.parent/'dbt/models').glob('*.sql')}
    if not models:
        raise ValueError('Project model definitions are missing')
    return models


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
    models = [node for node in manifest['nodes'].values()
              if node['resource_type']=='model' and node['package_name']=='banking']
    if len(models)!=len({node['name'] for node in models}) or {node['name'] for node in models}!=project_models():
        raise ValueError('Build model inventory does not match the project')
    if any(node['schema'] != document['gold_schema'] for node in models):
        raise ValueError('Build does not belong to this release')
    if results['args'].get('vars') != dbt_vars(document):
        raise ValueError('Build used different reporting parameters')
    expected_sources = {'bronze':document['bronze_schema'],'silver':document['silver_schema']}
    if any(node['schema'] != expected_sources[node['source_name']]
           for node in manifest['sources'].values()):
        raise ValueError('Build did not read pinned sources')
    return sorted(node['name'] for node in models)


def _http_origin(value):
    parsed = urlparse(value)
    if parsed.scheme not in {'http','https'} or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise ValueError('Invalid Trino HTTP route')
    try:
        port = parsed.port
    except ValueError:
        raise ValueError('Invalid Trino HTTP port') from None
    return parsed.scheme,parsed.hostname.lower(),port or (443 if parsed.scheme=='https' else 80)


class _TrinoRedirect(HTTPRedirectHandler):
    def __init__(self, origin):
        self.origin = origin

    def redirect_request(self, request, fp, code, message, headers, newurl):
        if _http_origin(newurl)!=self.origin:
            raise ValueError('Unexpected Trino redirect origin')
        return super().redirect_request(request,fp,code,message,headers,newurl)


class Trino:
    """Local release service client; shared deployments require external authentication."""
    def __init__(self):
        self.base = os.environ.get('TRINO_URL',f'http://{os.environ.get("TRINO_HOST","trino")}:'
                                   f'{os.environ.get("TRINO_PORT","8080")}')
        self.origin = _http_origin(self.base)
        parsed = urlparse(self.base)
        if parsed.path not in {'','/'} or parsed.query:
            raise ValueError('Trino base must be an origin URL')
        self.base = self.base.rstrip('/')
        self._open = build_opener(_TrinoRedirect(self.origin)).open

    def pages(self, sql):
        request = Request(self.base+'/v1/statement',data=sql.encode(),headers={
            'X-Trino-User':os.environ.get('TRINO_USER','transformer'),'Content-Type':'text/plain'})
        while True:
            with self._open(request,timeout=60) as response:
                page = json.loads(response.read(),parse_float=Decimal)
            if 'error' in page:
                raise RuntimeError('Trino query failed')
            yield page
            next_uri = page.get('nextUri')
            if not next_uri:
                return
            if _http_origin(next_uri) != self.origin:
                raise ValueError('Unexpected Trino next URI')
            request = Request(next_uri,headers={'X-Trino-User':os.environ.get('TRINO_USER','transformer')})

    def scalar(self, sql):
        rows = [row for page in self.pages(sql) for row in page.get('data',[])]
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ValueError('Expected one query result')
        return rows[0][0]


def certify(value, artifact_root, client=None):
    with release_lock(value):
        return _certify(value,artifact_root,client)


def _certify(value, artifact_root, client=None):
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
    folder = directory(value)
    pointer = folder.parent/'current.json'
    if pointer.exists() and json.loads(pointer.read_text(encoding='utf-8')).get('release_id')==value:
        raise ValueError('Uncertified release is already published; prepare a new release')
    with tempfile.TemporaryDirectory(prefix='.export-build-',dir=folder) as temporary:
        exports = Path(temporary)
        files = _export_models(client,schema,models,exports)
        document.update(status='CERTIFIED',certified_at=datetime.now(timezone.utc).isoformat(),exports=files,
                        certified_models=models,validation_code_digest=code_digest())
        _verify_exports(document,export_root=exports)
        destination = folder/'exports'
        if destination.is_symlink():
            raise ValueError('Invalid export directory')
        # Old pre-certification output can exist after an interrupted legacy run.
        # Preserve it for diagnosis instead of overwriting files or deleting history.
        if destination.exists():
            abandoned = folder/f'.uncertified-exports-{uuid.uuid4().hex}'
            if destination.resolve().parent!=folder.resolve() or abandoned.resolve().parent!=folder.resolve():
                raise ValueError('Unsafe export recovery path')
            os.replace(destination,abandoned)
        _sync_directory(exports)
        os.replace(exports,destination)
        _sync_directory(folder)
        publish(document)
    return document


def _export_models(client, schema, models, exports):
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
    return files


def publish(document):
    value = release_id(document['release_id'])
    with release_lock(value):
        return _publish(document)


def _publish(document):
    value = release_id(document['release_id'])
    previous_document = load(value)
    if previous_document['status']=='CERTIFIED' and document!=previous_document:
        raise ValueError('Certified release manifest is immutable')
    mutable = {'status','certified_at','exports','certified_models','validation_code_digest'}
    if any(document.get(key)!=item for key,item in previous_document.items() if key not in mutable):
        raise ValueError('Release request cannot change during publication')
    _verify_exports(document)
    # Readers resolve current.json once and consume only immutable certified files.
    state = State(os.environ.get('CONTROL_PATH','runtime/control.db'))
    with state.exclusive():
        if previous_document['status']!='CERTIFIED':
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


def verify(value, root=None):
    """Verify a certified export after copying it to another machine or restoring it."""
    document = load(value,root)
    _verify_exports(document,root=root)
    return document


class _DigestReader(io.RawIOBase):
    """Hash exactly the binary bytes consumed by the CSV parser, in one pass."""
    def __init__(self, handle, digest):
        self.handle, self.digest = handle,digest

    def readable(self):
        return True

    def readinto(self, buffer):
        size = self.handle.readinto(buffer)
        if size:
            self.digest.update(memoryview(buffer)[:size])
        return size


def _verify_exports(document, root=None, export_root=None):
    value = release_id(document['release_id'])
    if document['status'] != 'CERTIFIED':
        raise ValueError('Release is not certified')
    if not isinstance(document.get('exports'),dict) or not document['exports']:
        raise ValueError('Certified exports are missing')
    if 'certified_models' in document and (len(document['certified_models'])!=len(set(document['certified_models'])) or
            set(document['certified_models'])!=set(document['exports'])):
        raise ValueError('Certified model inventory mismatch')
    for model,metadata in document['exports'].items():
        if not re.fullmatch('[a-z][a-z0-9_]*',model) or metadata['file'] != f'exports/{model}.csv':
            raise ValueError('Invalid export path')
        folder = Path(export_root) if export_root is not None else directory(value,root)/'exports'
        path = folder/f'{model}.csv'
        if path.is_symlink() or folder.is_symlink() or path.resolve().parent != folder.resolve():
            raise ValueError('Export path escapes certified bundle')
        if type(metadata.get('rows')) is not int or metadata['rows'] < 0:
            raise ValueError('Invalid export row count')
        digest = hashlib.sha256()
        with path.open('rb') as handle:
            before = os.fstat(handle.fileno())
            try:
                with io.TextIOWrapper(io.BufferedReader(_DigestReader(handle,digest)),encoding='utf-8',newline='') as text:
                    reader = csv.reader(text,strict=True)
                    header = next(reader,None)
                    if not header or len(set(header)) != len(header) or any(not name for name in header):
                        raise ValueError('Invalid certified export header')
                    count = 0
                    for row in reader:
                        if len(row) != len(header):
                            raise ValueError('Malformed certified export row')
                        count += 1
            except (csv.Error,UnicodeError):
                raise ValueError('Malformed certified export CSV') from None
            after = os.fstat(handle.fileno())
            def identity(stat):
                # Windows fstat/Path.stat disagree on ctime semantics; inode,
                # size and modification time are comparable on both platforms.
                return stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns
            if identity(before)!=identity(after) or identity(after)!=identity(path.stat()):
                raise ValueError('Certified export changed during verification')
        if digest.hexdigest()!=metadata['sha256']:
            raise ValueError('Corrupt certified export')
        if count != metadata['rows']:
            raise ValueError('Certified export row count mismatch')


def build(value, dbt_executable='dbt'):
    """One lock covers dbt writes and certification; certified schemas are not rebuilt."""
    import subprocess
    with release_lock(value):
        result = load(value)
        root = directory(value).parent.parent/'dbt-target'/value
        if result['status']=='CERTIFIED':
            return certify(value,root)
        if result['status']!='SILVER_READY' or result['silver_quality']['rejected']:
            raise ValueError('Silver quality gate has not passed')
        baseline_id = result.get('silver_baseline')
        if baseline_id:
            prior = load(baseline_id)
            compatible = all(prior[key] == result[key] for key in ('start_date','as_of_date','reporting_currency'))
            if compatible and prior['status'] == 'CERTIFIED' and prior['validation_code_digest'] == code_digest():
                result['gold_baseline'] = baseline_id
                atomic_json(directory(value)/'release.json',result)
        subprocess.run([dbt_executable,'build','--project-dir','/opt/bank/dbt',
            '--profiles-dir','/opt/bank/dbt','--target-path',str(root.resolve()),
            '--vars',json.dumps(dbt_vars(result))],check=True,
            env={**os.environ,'DBT_SCHEMA':result['gold_schema']})
        return certify(value,root)


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
        result = build(args.release_id,args.dbt_executable)
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
