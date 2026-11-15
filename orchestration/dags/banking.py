"""Airflow 2.10 Dataset dependencies; all business code remains outside this file."""
from datetime import datetime, timedelta, timezone
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

BRONZE = Dataset('iceberg://lake/bronze/banking')
SILVER = Dataset('iceberg://lake/silver/banking')
DEFAULTS = {'retries':2,'retry_delay':timedelta(minutes=1),'execution_timeout':timedelta(hours=1)}


def batch(**context):
    from bank.pipeline import run_all
    params = context['params']
    start, end = params['start_date'], params['end_date']
    if bool(start) != bool(end):
        raise ValueError('Both backfill dates are required')
    run_all(dates=(start,end) if start else None)


with DAG('bank_bronze',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule=None,
         catchup=False,max_active_runs=1,default_args=DEFAULTS,
         params={'start_date':Param('',type='string'),'end_date':Param('',type='string')},
         tags=['banking','batch']) as bronze_dag:
    PythonOperator(task_id='ingest_sources',python_callable=batch,outlets=[BRONZE])

with DAG('bank_silver',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule=[BRONZE],
         catchup=False,max_active_runs=1,default_args=DEFAULTS,tags=['banking']) as silver_dag:
    BashOperator(task_id='spark_conformance',outlets=[SILVER],cwd='/opt/bank',bash_command=
        'spark-submit --master local[2] --packages '
        'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,'
        'org.apache.iceberg:iceberg-aws-bundle:1.9.1,'
        'org.apache.iceberg:iceberg-azure-bundle:1.9.1 processing/silver.py')

with DAG('bank_gold',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule=[SILVER],
         catchup=False,max_active_runs=1,default_args=DEFAULTS,tags=['banking']) as gold_dag:
    BashOperator(task_id='dbt_build_and_quality',cwd='/opt/bank/dbt',
                 bash_command='/opt/bank/dbt-venv/bin/dbt build --profiles-dir .')


def prepare_release(**context):
    import hashlib
    import os
    from bank.release import prepare
    params = context['params']
    value = hashlib.sha256(context['run_id'].encode()).hexdigest()[:32]
    day = params['as_of_date'] or (context['data_interval_end'].date()-timedelta(days=1)).isoformat()
    os.chdir('/opt/bank')
    prepare(value,params['start_date'],day,params['currency'])
    return value


def refresh_reference_fx(**context):
    """Optional release prerequisite; no implicit fixture fallback or Gold conversion."""
    from bank.fx import ingest_live_fx
    params = context['params']
    if not params['fetch_live_fx']:
        return {'status':'DISABLED','used_for_gold_conversion':False}
    day = params['as_of_date'] or (context['data_interval_end'].date()-timedelta(days=1)).isoformat()
    return ingest_live_fx(day,max_age_days=params['fx_max_age_days'])


def daily_reference_fx(**context):
    from bank.fx import ingest_live_fx
    day = (context['data_interval_end'].date()-timedelta(days=1)).isoformat()
    return ingest_live_fx(day,max_age_days=context['params']['fx_max_age_days'])


with DAG('bank_fx_daily',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule='0 4 * * *',
         catchup=False,max_active_runs=1,default_args=DEFAULTS,
         params={'fx_max_age_days':Param(7,type='integer',minimum=0,maximum=30)},
         tags=['banking','fx','reference']) as fx_dag:
    PythonOperator(task_id='ingest_live_reference_rates',python_callable=daily_reference_fx)


with DAG('bank_release',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule='0 2 * * *',
         catchup=False,max_active_runs=1,default_args=DEFAULTS,
         params={'start_date':Param('2026-01-01',type='string'),
                 'as_of_date':Param('',type='string'),
                 'currency':Param('USD',enum=['USD','EUR','VND']),
                 'fetch_live_fx':Param(False,type='boolean'),
                 'fx_max_age_days':Param(7,type='integer',minimum=0,maximum=30)},
         tags=['banking','certified']) as release_dag:
    fx_task = PythonOperator(task_id='refresh_reference_fx',python_callable=refresh_reference_fx)
    prepare_task = PythonOperator(task_id='prepare_snapshot',python_callable=prepare_release)
    silver_task = BashOperator(task_id='validate_silver',cwd='/opt/bank',bash_command=
        'spark-submit --master local[2] --conf spark.jars.ivy=/opt/spark/.ivy2 --packages '
        'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,'
        'org.apache.iceberg:iceberg-aws-bundle:1.9.1,'
        'org.apache.iceberg:iceberg-azure-bundle:1.9.1 processing/silver.py --release-id '
        '"{{ ti.xcom_pull(task_ids=\'prepare_snapshot\') }}"')
    publish_task = BashOperator(task_id='build_and_publish',cwd='/opt/bank',bash_command=
        'python -m bank.release build --dbt-executable /opt/bank/dbt-venv/bin/dbt --release-id '
        '"{{ ti.xcom_pull(task_ids=\'prepare_snapshot\') }}"')
    fx_task >> prepare_task >> silver_task >> publish_task


def archive_postgres_changes():
    import psycopg
    from bank.cdc import drain
    from bank.adapters import raw_store
    store = raw_store()
    with psycopg.connect(autocommit=True, connect_timeout=10) as db:
        total = 0
        # Bound each task's work. A later scheduled run drains the remaining backlog.
        for _ in range(20):
            count = drain(db, store, 500)
            total += count
            if count < 500:
                break
        return total


with DAG('bank_cdc_archive', start_date=datetime(2026,1,1,tzinfo=timezone.utc),
         schedule='*/15 * * * *', catchup=False, max_active_runs=1,
         default_args=DEFAULTS, tags=['banking','cdc']) as cdc_dag:
    PythonOperator(task_id='archive_committed_changes', python_callable=archive_postgres_changes)
