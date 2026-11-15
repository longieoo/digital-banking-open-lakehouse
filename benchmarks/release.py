"""End-to-end benchmark on a fresh dedicated Compose stack with synthetic data."""
import argparse
import json
import ipaddress
import os
from pathlib import Path
import subprocess
import sys
from time import perf_counter
import uuid

from bank.release import atomic_json, verify


def run(*args):
    subprocess.run(args, check=True)


def database_stats():
    sql = ("SELECT json_build_object('blocks_read',blks_read,'buffer_hits',blks_hit,"
           "'transactions',xact_commit+xact_rollback,'database_bytes',pg_database_size(datname)) "
           "FROM pg_stat_database WHERE datname=current_database()")
    return json.loads(subprocess.check_output(['docker','compose','exec','-T','postgres','psql','-Atc',sql]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--customers', type=int, default=400)
    parser.add_argument('--transactions', type=int, default=10000)
    parser.add_argument('--subnet', help='Optional unused private Docker IPv4 CIDR')
    args = parser.parse_args()
    if min(args.customers,args.transactions) < 1:
        parser.error('Positive fixture sizes required')
    project = 'bank-bench-' + uuid.uuid4().hex[:12]
    runtime = (Path('runtime') / project).resolve()
    runtime.mkdir(parents=True,exist_ok=False)
    os.environ.update(COMPOSE_PROJECT_NAME=project, BANK_RUNTIME_DIR=str(runtime),
                      COMPOSE_FILE=os.pathsep.join(['docker-compose.yml','docker-compose.ci.yml']),
                      RELEASE_ROOT=str(runtime/'releases'))
    if args.subnet:
        network = ipaddress.IPv4Network(args.subnet)
        if not network.is_private:
            parser.error('Use a private Docker subnet')
        override = runtime / 'network.json'
        override.write_text(json.dumps({'networks':{'default':{'ipam':{'config':[{'subnet':str(network)}]}}}}))
        os.environ['COMPOSE_FILE'] += os.pathsep + str(override)
    try:
        run('docker','compose','up','-d','--wait','postgres','catalog','trino')
        run('docker','compose','build','bank','spark','dbt')
        run('docker','compose','run','--rm','bank','seed','--customers',str(args.customers),
            '--transactions',str(args.transactions))
        before = database_stats()
        value = uuid.uuid4().hex
        start = perf_counter()
        run(sys.executable,'-m','scripts.run_release','--full-refresh','--release-id',value)
        elapsed = perf_counter() - start
        after = database_stats()
        doc = verify(value)
        transactions = doc['exports']['fct_transactions']['rows']
        atomic_json(runtime/'benchmark.json', {
            'project':project,'release_id':value,'fixture':vars(args),'transactions':transactions,
            'seconds':elapsed,'transactions_per_second':transactions/elapsed,
            'docker':json.loads(subprocess.check_output(['docker','info','--format',
                '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}}}'])),
            'database_before':before,'database_after':after,
            'timings':json.loads((runtime/'releases'/value/'timings.json').read_text())})
        print(runtime/'benchmark.json')
    finally:
        run('docker','compose','--profile','*','stop')


if __name__ == '__main__':
    main()
