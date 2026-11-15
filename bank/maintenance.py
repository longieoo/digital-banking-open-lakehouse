"""Read-only release retention inventory; this module never deletes any data."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import re

from bank.backup import _current, _root
from bank.release import load, verify


def retention_plan(release_root, keep_latest=3, minimum_age_days=7, now=None):
    """Conservative planning only. Snapshot references are all protected.

    An eligible export directory still requires a verified backup and a separate
    maintenance policy; it does not authorize dropping Iceberg schemas/snapshots.
    Invalid manifests and every in-progress release remain protected.
    """
    if isinstance(keep_latest, bool) or not isinstance(keep_latest, int) or keep_latest < 1:
        raise ValueError('Keep at least one release per reporting currency')
    if isinstance(minimum_age_days, bool) or not isinstance(minimum_age_days, int) or minimum_age_days < 0:
        raise ValueError('Minimum age must be nonnegative')
    root = _root(release_root)
    if not root.is_dir():
        raise FileNotFoundError('Release root does not exist')
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Planning time requires a timezone')
    current = _current(root) if (root / 'current.json').exists() else None
    # A broken current pointer is never silently ignored.
    if (root / 'current.json').is_symlink():
        raise ValueError('Symlink publication pointer')
    results, snapshots, ignored = [], {}, []
    for path in sorted(root.iterdir()):
        if not re.fullmatch('[a-f0-9]{32}', path.name):
            if path.name != 'current.json':
                ignored.append(path.name)
            continue
        item = {'release_id': path.name, 'protected_reasons': [], 'export_retention_candidate': False}
        results.append(item)
        if path.name == current:
            item['protected_reasons'].append('current_publication')
        try:
            document = load(path.name, root=root)
            item.update(status=document['status'], as_of_date=document['as_of_date'],
                        reporting_currency=document['reporting_currency'])
            for table, snapshot in document.get('bronze_snapshots', {}).items():
                if snapshot is not None:
                    snapshots.setdefault(table, set()).add(snapshot)
            if document['status'] != 'CERTIFIED':
                item['protected_reasons'].append('in_progress_release')
                continue
            verify(path.name, root=root)
            certified = datetime.fromisoformat(document['certified_at'].replace('Z', '+00:00'))
            if certified.tzinfo is None:
                raise ValueError('Certification time requires timezone')
            item['certified_at'] = certified.isoformat()
            if certified > now - timedelta(days=minimum_age_days):
                item['protected_reasons'].append('minimum_age')
        except (ValueError, KeyError, TypeError, OSError, json.JSONDecodeError):
            item['protected_reasons'].append('invalid_or_unreadable_release')
    currencies = {item.get('reporting_currency') for item in results}
    for currency in currencies:
        certified = sorted((item for item in results if item.get('reporting_currency') == currency
                            and 'certified_at' in item),
                           key=lambda item: (item['as_of_date'], item['certified_at'], item['release_id']),
                           reverse=True)
        for item in certified[:keep_latest]:
            item['protected_reasons'].append('latest_per_currency')
    for item in results:
        item['export_retention_candidate'] = not item['protected_reasons']
    return {'mode': 'DRY_RUN_ONLY', 'current_release': current, 'keep_latest_per_currency': keep_latest,
            'minimum_age_days': minimum_age_days, 'releases': results, 'ignored_entries': ignored,
            'protected_bronze_snapshots': {table: sorted(values) for table, values in snapshots.items()},
            'deletions_performed': 0,
            'boundary': 'No raw, snapshot, catalog, schema, control or checkpoint deletion is authorized. '
                        'Export candidates require verified backup and a separate reviewed cleanup procedure.'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release-root', default=os.environ.get('RELEASE_ROOT', 'runtime/releases'))
    parser.add_argument('--keep-latest', type=int, default=3)
    parser.add_argument('--minimum-age-days', type=int, default=7)
    args = parser.parse_args(argv)
    print(json.dumps(retention_plan(args.release_root, args.keep_latest, args.minimum_age_days), indent=2))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        raise SystemExit(type(exc).__name__) from None
