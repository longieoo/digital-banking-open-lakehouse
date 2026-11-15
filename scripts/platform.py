"""Repeatable local full-platform lifecycle without deleting volumes or overwriting .env."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys


def doctor():
    checks = {'python':sys.version_info >= (3,11),
              'source_configuration':all(Path(p).is_file() for p in
                 ('docker-compose.yml','dbt/dbt_project.yml','sources/schema.sql')),
              'local_env':Path('.env').is_file()}
    for name in ('pyiceberg','duckdb','yaml'):
        checks['dependency_'+name] = importlib.util.find_spec(name) is not None
    try:
        checks['docker_engine'] = subprocess.run(['docker','info','--format','{{.ServerVersion}}'],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30).returncode==0
    except (OSError,subprocess.TimeoutExpired):
        checks['docker_engine'] = False
    return {'checks':checks,'status':'PASS' if all(checks.values()) else 'INCOMPLETE'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['doctor','up','status','stop'])
    parser.add_argument('--project',default='bank-full')
    parser.add_argument('--runtime',default='runtime/full-platform')
    parser.add_argument('--internal',action='store_true',help='Remove core host ports using CI Compose override')
    parser.add_argument('--all',action='store_true',help='Also start Airflow and the Redpanda broker')
    parser.add_argument('--enable-schedules',action='store_true',help='With --all, unpause daily release and live FX DAGs')
    parser.add_argument('--seed',action='store_true',help='Explicitly replace ONLY this demo project source with synthetic data')
    parser.add_argument('--no-release',action='store_true',help='Start services without building a new certified release')
    parser.add_argument('--bi-port',type=int,default=8090)
    parser.add_argument('--metrics-port',type=int,default=9091)
    parser.add_argument('--as-of-date',default='2026-01-31')
    parser.add_argument('--auth-token-file',help='Private host file enabling protected BI and metrics')
    args = parser.parse_args()
    if args.enable_schedules and not args.all:
        parser.error('--enable-schedules requires --all')
    if args.action=='doctor':
        result = doctor()
        print(json.dumps(result))
        return 0 if result['status']=='PASS' else 1
    os.environ['COMPOSE_PROJECT_NAME'] = args.project
    os.environ['BANK_RUNTIME_DIR'] = str(Path(args.runtime).resolve()).replace('\\','/')
    os.environ['RELEASE_ROOT'] = str(Path(args.runtime).resolve()/'releases')
    os.environ['BANK_BI_PORT'] = str(args.bi_port)
    os.environ['BANK_METRICS_PORT'] = str(args.metrics_port)
    files = ['docker-compose.yml']+(['docker-compose.ci.yml'] if args.internal else [])
    if args.auth_token_file:
        from bank.http_security import TokenAuth
        try:
            auth = TokenAuth(token_file=args.auth_token_file)
            os.environ['BANK_HTTP_TOKEN'] = auth.secret()
        except (ValueError,OSError):
            raise SystemExit('HTTP token file is missing or invalid') from None
        files.append('docker-compose.auth.yml')
    os.environ['COMPOSE_FILE'] = os.pathsep.join(files)
    compose = ['docker','compose']

    def run(*command):
        subprocess.run(command,check=True)

    if args.action=='status':
        run(*compose,'--profile','*','ps','--all')
    elif args.action=='stop':
        run(*compose,'--profile','*','stop')
    else:
        if not Path('.env').exists():
            run(sys.executable,'scripts/create_demo_env.py')
        Path(args.runtime).mkdir(parents=True,exist_ok=True)
        run(*compose,'config','--quiet')
        run(*compose,'build','bank','spark','dbt',*(['airflow-init'] if args.all else []))
        run(*compose,'--profile','core','--profile','lakehouse','up','-d','--wait','postgres','catalog')
        if args.seed:
            run(*compose,'run','--rm','bank','seed')
        run(*compose,'--profile','query','up','-d','--wait','trino')
        if not args.no_release:
            run(sys.executable,'-m','scripts.run_release','--as-of-date',args.as_of_date)
        run(*compose,'--profile','bi','--profile','monitoring','up','-d','--wait','dashboard','metrics')
        if args.all:
            run(*compose,'--profile','streaming','--profile','orchestration','up','-d','--wait',
                'redpanda','airflow-scheduler','airflow-webserver')
            topic = subprocess.run([*compose,'exec','-T','redpanda','rpk','topic','describe','bank.transactions'],
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            if topic.returncode:
                run(*compose,'exec','-T','redpanda','rpk','topic','create','bank.transactions','--partitions','3')
            # The shared Silver dimensions must exist before stream conformance.
            run(*compose,'run','--rm','spark','processing/silver.py')
            run(*compose,'run','--rm','spark','streaming/consumer.py','--reconcile-alerts')
            run(*compose,'run','--rm','--entrypoint','python','bank','-m','streaming.producer')
            run(*compose,'--profile','streaming-worker','up','-d','stream-consumer')
            if args.enable_schedules:
                for name in ('bank_release','bank_fx_daily'):
                    run(*compose,'exec','-T','airflow-scheduler','airflow','dags','unpause',name)
        print(json.dumps({'status':'STARTED','project':args.project,
            'dashboard':f'http://127.0.0.1:{args.bi_port}',
            'metrics':f'http://127.0.0.1:{args.metrics_port}/metrics',
            'streaming':'broker, deterministic producer and continuous consumer started' if args.all else 'available via --all',
            'runtime':os.environ['BANK_RUNTIME_DIR']}))
    return 0


if __name__=='__main__':
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f'Platform command failed with exit code {exc.returncode}') from None
