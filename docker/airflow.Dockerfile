FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/bank
COPY . .
RUN mkdir -p /opt/bank/runtime && chown -R airflow:0 /opt/bank
USER airflow
RUN pip install --no-cache-dir apache-airflow==2.10.5 '.[runtime]' pyspark==3.5.5
RUN pip install --no-cache-dir aiobotocore==2.23.1 && pip check
RUN python -m venv /opt/bank/dbt-venv && /opt/bank/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.14 dbt-trino==1.9.3
ENV PYTHONPATH=/opt/bank AIRFLOW__CORE__LOAD_EXAMPLES=false AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/bank/orchestration/dags
