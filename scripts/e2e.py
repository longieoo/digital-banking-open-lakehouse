"""Real service acceptance, opt-in. Captures no credentials and asserts each gate."""
import subprocess
from pathlib import Path


def run(*args):
    subprocess.run(args,check=True)


def query(sql):
    result=subprocess.run(['docker','compose','exec','-T','trino','trino','--user','transformer',
        '--output-format','CSV','--execute',sql],capture_output=True,text=True,check=True)
    return result.stdout.strip().strip('"')


def main():
    if not Path('.env').exists():
        raise SystemExit('Create local .env from .env.example first')
    run('docker','compose','--profile','core','--profile','lakehouse','up','-d','--wait','postgres','catalog')
    run('docker','compose','build','bank','spark','dbt')
    run('docker','compose','run','--rm','bank','seed')
    run('docker','compose','run','--rm','--entrypoint','python','bank','-m','bank.pipeline')
    run('docker','compose','run','--rm','-e','RUN_STACK=1','--entrypoint','python','bank',
        '-m','unittest','tests.integration.test_stack','-v')
    run('docker','compose','--profile','processing','run','--rm','spark')
    run('docker','compose','--profile','query','up','-d','--wait','trino')
    run('docker','compose','run','--rm','dbt','build','--profiles-dir','.')
    assert query("SELECT count(*) FROM lake.gold.mart_reconciliation_daily WHERE reconciliation_status='FAIL'")=='0'
    assert int(query('SELECT count(*) FROM lake.gold.mart_customer_360'))>0
    count=query('SELECT count(*) FROM lake.bronze.transactions')
    run('docker','compose','restart','catalog','minio')
    run('docker','compose','--profile','lakehouse','up','-d','--wait','catalog','minio')
    assert query('SELECT count(*) FROM lake.bronze.transactions')==count
    # Reading after restart is insufficient: SQLite JDBC locks can block writes
    # while all health checks and SELECT queries still succeed.
    run('docker','compose','run','--rm','dbt','build','--profiles-dir','.')
    assert query("SELECT count(*) FROM lake.gold.mart_reconciliation_daily WHERE reconciliation_status='FAIL'")=='0'
    print('PASS: real source -> Bronze -> Spark Silver -> dbt Gold, recovery, reconciliation, restart persistence')


if __name__ == '__main__':
    main()
