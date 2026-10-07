"""Real acceptance on a dedicated Compose environment; no teardown or volume deletion."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from bank.release import atomic_json, directory, verify


def run(*args):
    subprocess.run(args,check=True)


def main():
    runtime = Path(os.environ.get('BANK_RUNTIME_DIR','runtime'))
    os.environ.setdefault('RELEASE_ROOT',str(runtime/'releases'))
    run('docker','compose','--profile','core','--profile','lakehouse','up','-d','--wait','postgres','catalog')
    run('docker','compose','build','bank','spark','dbt')
    run('docker','compose','run','--rm','bank','seed')
    run('docker','compose','--profile','query','up','-d','--wait','trino')
    first, historical = uuid.uuid4().hex,uuid.uuid4().hex
    run(sys.executable,'-m','scripts.run_release','--release-id',first)
    pointer = runtime/'releases/current.json'
    before = pointer.read_bytes()
    run(sys.executable,'-m','scripts.run_release','--release-id',first)
    assert pointer.read_bytes()==before
    run(sys.executable,'-m','scripts.run_release','--release-id',historical,'--as-of-date','2026-01-30')
    assert pointer.read_bytes()==before
    assert verify(first)['as_of_date']=='2026-01-31'
    assert verify(historical)['as_of_date']=='2026-01-30'
    # Simulate an incomplete dbt run after successful Silver, on a separate candidate.
    failed = uuid.uuid4().hex
    document = json.loads((directory(first)/'release.json').read_text())
    document.update(release_id=failed,status='SILVER_READY')
    document.update(bronze_schema=f'bronze_r_{failed}',silver_schema=f'silver_r_{failed}',gold_schema=f'gold_r_{failed}')
    atomic_json(directory(failed)/'release.json',document)
    artifacts = runtime/'dbt-target'/first
    bad_artifacts = runtime/'dbt-target'/failed
    bad_artifacts.mkdir(parents=True,exist_ok=True)
    (bad_artifacts/'manifest.json').write_bytes((artifacts/'manifest.json').read_bytes())
    results = json.loads((artifacts/'run_results.json').read_text())
    results['results'][0]['status']='error'
    atomic_json(bad_artifacts/'run_results.json',results)
    failure = subprocess.run(['docker','compose','run','--rm','--entrypoint','python','bank',
        '-m','bank.release','certify','--release-id',failed,'--artifacts',f'/opt/bank/runtime/dbt-target/{failed}'])
    assert failure.returncode!=0
    assert pointer.read_bytes()==before
    atomic_json(runtime/'release-acceptance.json',{'status':'PASS','current_release':first,
        'historical_release':historical,'retry':'PASS','historical_does_not_publish_backwards':'PASS',
        'injected_incomplete_build_does_not_publish':'PASS',
        'failure_scope':'injected dbt artifact failure; not a network outage'})


if __name__=='__main__':
    main()
