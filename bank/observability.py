"""Sanitized local metrics, with explicit synthetic-data freshness anchors."""
from datetime import datetime, timezone
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


if __name__ == '__main__':
    import json
    import os
    print(json.dumps(metrics(State(os.environ.get('CONTROL_PATH','runtime/control.db')))))
