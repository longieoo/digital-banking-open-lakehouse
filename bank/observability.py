"""Sanitized local metrics, with explicit synthetic-data freshness anchors."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from bank.ingestion import State


def metrics(state: State, as_of=None):
    now = datetime.fromisoformat(as_of) if as_of else datetime.now(timezone.utc)
    rows = state.manifests()
    completed = [r for r in rows if r['status']=='COMPLETED']
    duration = sum((datetime.fromisoformat(r['completed_at']) - datetime.fromisoformat(r['started_at'])).total_seconds()
                   for r in completed)
    return dict(completed_batches=len(completed),failed_batches=sum(r['status']=='FAILED' for r in rows),
        started_batches=sum(r['status']=='STARTED' for r in rows),
        rows_extracted=sum(r['extracted_count'] for r in completed),
        rows_written=sum(r['bronze_written_count'] for r in completed),
        retry_count=sum(r['attempts']-1 for r in rows),total_attempt_elapsed_seconds=duration,
        measured_at=now.isoformat(),
        manifest_count_mismatches=sum(r['raw_written_count'] != r['bronze_written_count'] for r in completed))


def release_metrics(root='runtime/releases'):
    root = Path(root)
    failures = len(list(root.glob('*/failure.json')))
    pointer = root/'current.json'
    if not pointer.exists():
        return dict(certified_current=0,releases_with_recorded_failures=failures)
    from bank.release import verify, release_id
    if pointer.is_symlink():
        raise ValueError('Invalid publication pointer')
    current = json.loads(pointer.read_text(encoding='utf-8'))
    value = release_id(current['release_id'])
    if current.get('manifest')!=f'{value}/release.json':
        raise ValueError('Invalid manifest route')
    document = verify(value,root=root)
    age = (datetime.now(timezone.utc)-datetime.fromisoformat(document['certified_at'])).total_seconds()
    return dict(certified_current=int(document['status']=='CERTIFIED'),releases_with_recorded_failures=failures,
                current_release_id=document['release_id'],business_date=document['as_of_date'],
                publication_age_seconds=max(0,age),rejected_rows=document['silver_quality']['rejected'],
                exported_rows={name:item['rows'] for name,item in document['exports'].items()})


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--prometheus',action='store_true')
    args = parser.parse_args()
    operational = metrics(State(os.environ.get('CONTROL_PATH','runtime/control.db')))
    releases = release_metrics(os.environ.get('RELEASE_ROOT','runtime/releases'))
    if args.prometheus:
        for prefix,values in [('bank_ingestion',operational),('bank_release',releases)]:
            for key,value in values.items():
                if isinstance(value,(int,float)):
                    print(f'{prefix}_{key} {value}')
    else:
        print(json.dumps(dict(ingestion=operational,releases=releases)))
