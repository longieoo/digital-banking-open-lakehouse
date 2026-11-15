"""One-command certified demo release; COMPOSE_FILE/PROJECT_NAME select the environment."""
import argparse
import json
import os
from pathlib import Path
import subprocess
from time import perf_counter
import uuid

from bank.release import directory, load, dbt_vars, atomic_json, release_id, code_digest


def code_revision(root=None):
    """Source archives remain runnable without Git or a .git history directory."""
    root = Path(root or Path(__file__).resolve().parent.parent)
    if not (root/'.git').exists():
        return 'source-sha256:'+code_digest()
    try:
        git = ['git','-c',f'safe.directory={root.as_posix()}','-C',str(root)]
        revision = subprocess.check_output([*git,'rev-parse','HEAD'],text=True,stderr=subprocess.DEVNULL).strip()
        if subprocess.check_output([*git,'status','--porcelain'],text=True,stderr=subprocess.DEVNULL).strip():
            revision += '-dirty'
        return revision
    except (OSError,subprocess.CalledProcessError):
        return 'source-sha256:'+code_digest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-id',default=uuid.uuid4().hex)
    parser.add_argument('--start-date',default='2026-01-01')
    parser.add_argument('--as-of-date',default='2026-01-31')
    parser.add_argument('--currency',default='USD')
    args = parser.parse_args()
    os.environ.setdefault('RELEASE_ROOT',str(Path(os.environ.get('BANK_RUNTIME_DIR','runtime'))/'releases'))
    value = release_id(args.release_id)
    revision = code_revision()
    compose = ['docker','compose']
    measurements = []

    def run(stage,*command):
        start = perf_counter()
        try:
            subprocess.run([*compose,*command],check=True)
        except subprocess.CalledProcessError:
            atomic_json(directory(value)/'failure.json',{'stage':stage,'error':'CommandFailed'})
            raise
        measurements.append({'stage':stage,'seconds':perf_counter()-start})

    run('source','run','--rm','-e',f'CODE_REVISION={revision}','--entrypoint','python','bank',
        '-m','bank.release','prepare','--release-id',value,'--start-date',args.start_date,
        '--as-of-date',args.as_of_date,'--currency',args.currency)
    document = load(value)
    if document['status']=='CERTIFIED':
        run('verify','run','--rm','--entrypoint','python','bank','-m','bank.release','certify',
            '--release-id',value,'--artifacts',f'/opt/bank/runtime/dbt-target/{value}')
        print(json.dumps({'release_id':value,'status':'CERTIFIED','reused':True}))
        return
    run('silver','run','--rm','spark','processing/silver.py','--release-id',value)
    root = f'/opt/bank/runtime/dbt-target/{value}'
    run('gold','run','--rm','-e',f'DBT_SCHEMA={document["gold_schema"]}','dbt',
        'build','--profiles-dir','.','--target-path',root,'--vars',json.dumps(dbt_vars(document)))
    run('publish','run','--rm','--entrypoint','python','bank','-m','bank.release','certify',
        '--release-id',value,'--artifacts',root)
    atomic_json(directory(value)/'timings.json',{'stages':measurements,'code_revision':revision,
                                                'compose_project':os.environ.get('COMPOSE_PROJECT_NAME')})
    print(json.dumps({'release_id':value,'status':load(value)['status'],'stages':measurements}))


if __name__=='__main__':
    main()
