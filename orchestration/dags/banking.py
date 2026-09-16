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


with DAG('bank_bronze',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule='0 2 * * *',
         catchup=False,max_active_runs=1,default_args=DEFAULTS,
         params={'start_date':Param('',type='string'),'end_date':Param('',type='string')},
         tags=['banking','batch']) as bronze_dag:
    PythonOperator(task_id='ingest_sources',python_callable=batch,outlets=[BRONZE])

with DAG('bank_silver',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule=[BRONZE],
         catchup=False,max_active_runs=1,default_args=DEFAULTS,tags=['banking']) as silver_dag:
    BashOperator(task_id='spark_conformance',outlets=[SILVER],cwd='/opt/bank',bash_command=
        'spark-submit --master local[2] --packages '
        'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,'
        'org.apache.iceberg:iceberg-aws-bundle:1.9.1 processing/silver.py')

with DAG('bank_gold',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule=[SILVER],
         catchup=False,max_active_runs=1,default_args=DEFAULTS,tags=['banking']) as gold_dag:
    BashOperator(task_id='dbt_build_and_quality',cwd='/opt/bank/dbt',
                 bash_command='/opt/bank/dbt-venv/bin/dbt build --profiles-dir .')
