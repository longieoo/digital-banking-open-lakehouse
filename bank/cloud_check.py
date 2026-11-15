"""Offline Azure readiness and an explicit synthetic raw-storage I/O probe.

No Azure resources, identities or role assignments are created. The probe only
creates one unique blob in the configured raw filesystem and conditionally
deletes that same blob. Passing it is not cloud lakehouse acceptance.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
from urllib.parse import unquote, urlparse
import uuid


def _dependency(module):
    try:
        return importlib.util.find_spec(module) is not None
    except (ModuleNotFoundError, ValueError, ImportError):
        return False


def _storage_uri(value, account):
    try:
        parsed = urlparse(value)
    except ValueError:
        return None
    if parsed.scheme != 'abfss' or parsed.query or parsed.fragment:
        return None
    if parsed.netloc.count('@') != 1:
        return None
    container, host = parsed.netloc.split('@')
    if host != f'{account}.dfs.core.windows.net':
        return None
    if not re.fullmatch('[a-z0-9](?:[a-z0-9-]{1,61})[a-z0-9]', container) or '--' in container:
        return None
    path = unquote(parsed.path).strip('/')
    if '%' in path or '\\' in path or any(part in {'.', '..', ''} for part in path.split('/')):
        if path:
            return None
    if path and not re.fullmatch('[A-Za-z0-9_./-]+', path):
        return None
    return {'container': container, 'prefix': path}


def _catalog_uri(value):
    try:
        parsed = urlparse(value)
        return bool(parsed.scheme == 'https' and parsed.hostname and not parsed.username
                    and not parsed.password and not parsed.query and not parsed.fragment
                    and not parsed.hostname.startswith('replace')
                    and not parsed.hostname.endswith('.invalid'))
    except ValueError:
        return False


def preflight(config=None, dependency_checker=None):
    """Inspect process configuration and installed modules without authentication/network calls."""
    config = os.environ if config is None else config
    dependency_checker = dependency_checker or _dependency
    account = config.get('AZURE_STORAGE_ACCOUNT_NAME', '')
    account_valid = bool(re.fullmatch('[a-z0-9]{3,24}', account) and not account.startswith('replace'))
    raw = _storage_uri(config.get('RAW_URI', ''), account) if account_valid else None
    warehouse = _storage_uri(config.get('WAREHOUSE_URI', ''), account) if account_valid else None
    checks = {
        'azure_backend_selected': config.get('STORAGE_BACKEND') == 'azure',
        'storage_account_configured': account_valid,
        'raw_abfss_same_account': raw is not None,
        'warehouse_abfss_same_account': warehouse is not None,
        'separate_raw_warehouse_filesystems': bool(raw and warehouse and raw['container'] != warehouse['container']),
        'https_catalog_configured': _catalog_uri(config.get('CATALOG_URI', '')),
        'azure_identity_dependency': bool(dependency_checker('azure.identity')),
        'azure_blob_dependency': bool(dependency_checker('azure.storage.blob')),
    }
    token_file = config.get('AZURE_FEDERATED_TOKEN_FILE', '')
    return {
        'status': 'READY_FOR_PROBE' if all(checks.values()) else 'NOT_READY',
        'mode': 'OFFLINE_PREFLIGHT', 'cloud_validation': 'NOT_RUN',
        'checks': {name: 'PASS' if passed else 'MISSING_OR_INVALID' for name, passed in checks.items()},
        'credential_hints': {
            'environment_credential_fields_present': all(bool(config.get(name)) for name in (
                'AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET')),
            'workload_identity_fields_present': bool(config.get('AZURE_TENANT_ID')
                and config.get('AZURE_CLIENT_ID') and token_file and Path(token_file).is_file()),
            'azure_cli_installed': shutil.which('az') is not None,
            'managed_identity_availability': 'NOT_CHECKED',
        },
        'boundary': 'Configuration/dependency checks do not prove authentication, RBAC, network access, '
                    'ADLS I/O, catalog compatibility or a cloud release.'}


def _azure_clients(account):
    from azure.core import MatchConditions
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    try:
        service = BlobServiceClient(f'https://{account}.blob.core.windows.net', credential=credential,
                                    connection_timeout=10, read_timeout=30, retry_total=2)
    except Exception:
        credential.close()
        raise
    return credential, service, MatchConditions.IfNotModified


def probe(config=None, client_factory=None, dependency_checker=None):
    """Opt-in one-blob roundtrip with ETag-scoped cleanup and sanitized error reporting."""
    config = os.environ if config is None else config
    readiness = preflight(config, dependency_checker)
    if readiness['status'] != 'READY_FOR_PROBE':
        return {'status': 'UNAVAILABLE', 'cloud_validation': 'NOT_RUN', 'preflight': readiness}
    target = _storage_uri(config['RAW_URI'], config['AZURE_STORAGE_ACCOUNT_NAME'])
    token = uuid.uuid4().hex
    probe_name = f'_acceptance/{token}.json'
    key = '/'.join(part for part in (target['prefix'], probe_name) if part)
    content = json.dumps({'purpose': 'synthetic-storage-acceptance', 'probe_id': token}, sort_keys=True).encode()
    result = {'status': 'FAIL', 'scope': 'azure_raw_blob_roundtrip', 'cloud_validation': 'ATTEMPTED',
              'probe_id': token, 'uploaded': False, 'read_verified': False, 'deleted': False,
              'cleanup': 'NOT_NEEDED_NO_CONFIRMED_UPLOAD'}
    credential = service = blob = None
    etag = None
    stage = 'client_initialization'
    try:
        credential, service, match_condition = (client_factory or _azure_clients)(
            config['AZURE_STORAGE_ACCOUNT_NAME'])
        blob = service.get_blob_client(container=target['container'], blob=key)
        stage = 'upload'
        response = blob.upload_blob(content, overwrite=False, timeout=30,
                                    metadata={'purpose': 'bank-acceptance', 'probe_id': token})
        result['uploaded'] = True
        etag = response.get('etag')
        if not etag:
            raise ValueError('Missing upload ETag')
        stage = 'download'
        actual = blob.download_blob(etag=etag, match_condition=match_condition, timeout=30).readall()
        if actual != content:
            raise ValueError('Synthetic probe content mismatch')
        result['read_verified'] = True
        result['bytes'] = len(content)
        result['sha256'] = hashlib.sha256(content).hexdigest()
    except Exception as exc:
        # Never return exception messages, response headers, account URLs,
        # tenant/client IDs, credentials or token content.
        result['error'] = {'stage': stage, 'type': type(exc).__name__}
    finally:
        if result['uploaded'] and etag:
            try:
                blob.delete_blob(etag=etag, match_condition=match_condition, timeout=30)
                result.update(deleted=True, cleanup='DELETED_OWN_CONFIRMED_ETAG')
            except Exception as exc:
                result['cleanup'] = 'FAILED_OWN_PROBE_MAY_REMAIN'
                result['cleanup_error'] = type(exc).__name__
        elif result['uploaded']:
            result['cleanup'] = 'NOT_ATTEMPTED_WITHOUT_ETAG'
        elif stage == 'upload':
            result['cleanup'] = 'NOT_ATTEMPTED_UPLOAD_OWNERSHIP_UNCONFIRMED'
        for resource in (service, credential):
            if resource is not None:
                try:
                    resource.close()
                except Exception as exc:
                    result.setdefault('transport_close_errors', []).append(type(exc).__name__)
    if result['read_verified'] and result['deleted']:
        result['status'] = 'PASS'
        result['cloud_validation'] = 'RAW_IO_ONLY'
    result['boundary'] = ('One configured raw blob only; no deployment, filesystem ACL, denied-identity, '
                          'Iceberg catalog, Spark/Trino/dbt cloud release or full platform acceptance.')
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', action='store_true', help='Contact Azure and write/read/delete one synthetic blob')
    args = parser.parse_args(argv)
    result = probe() if args.probe else preflight()
    print(json.dumps(result, indent=2))
    if result['status'] not in {'READY_FOR_PROBE', 'PASS'}:
        raise SystemExit(2)


if __name__ == '__main__':
    main()
