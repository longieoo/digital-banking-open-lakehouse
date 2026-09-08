# Clean-clone and demonstration guide

Prerequisites: Git, Python 3.11+, Docker Compose v2 with 16 GB available RAM for the
staged distributed demo, and network access to images/Maven/PyPI. Use Linux containers
on Docker Desktop. Java and Spark are supplied by the Spark image. Terraform is
needed only for the cloud demonstration. All commands start at repository root.

## Local correctness demonstration (no Docker)

```sh
python -m venv .venv
# Activate .venv: source .venv/bin/activate (Linux); .venv\Scripts\Activate.ps1 (PowerShell)
python -m pip install -r requirements-test.txt
python -m unittest discover -s tests -v
python -m scripts.local_demo
```

The local demo uses real Iceberg files but typed fixture Silver and translated dbt
SQL. It demonstrates logic and recovery, not the complete deployed stack.

## Distributed batch

```sh
python scripts/create_demo_env.py
docker compose --profile core --profile lakehouse up -d --wait postgres catalog
docker compose build bank spark dbt
docker compose run --rm bank seed --profile small
docker compose run --rm --entrypoint python bank -m bank.pipeline
docker compose --profile processing run --rm spark
docker compose --profile query up -d --wait trino
docker compose run --rm dbt build --profiles-dir .
docker compose exec trino trino --user analyst --execute "SELECT * FROM lake.gold.mart_reconciliation_daily"
```

Bucket initialization runs through catalog dependencies. Keep .env AWS and MinIO
credentials aligned; the helper does that without printing credentials. The bank
container does not depend on every service so it can run standalone utilities.
Wait for PostgreSQL and catalog health before seeding/ingestion. Maven artifacts
download on first Spark run and persist in the Ivy volume.

Profiles: core = source/Python utilities; lakehouse = MinIO/bucket init/REST;
processing = on-demand Spark; query = Trino/on-demand dbt; streaming = Redpanda;
orchestration = Airflow init/scheduler/webserver. Run only needed named services.
MinIO, catalog, PostgreSQL, Airflow and broker use persistent volumes; pipeline state
and checkpoints use ./runtime. A one-shot runtime-init sets this directory to
UID 50000, shared by Python/Spark/Airflow/dbt writers, with mode 0770. It does not
recursively change unrelated files. Trino is stateless. Services bind host ports to loopback.

## Failure, retry and backfill

```sh
# Use a new CONTROL_PATH to observe initial failure after previous successful loads.
docker compose run --rm -e CONTROL_PATH=/opt/bank/runtime/failure-demo.db bank ingest --fail-at bronze
docker compose run --rm -e CONTROL_PATH=/opt/bank/runtime/failure-demo.db bank manifests
docker compose run --rm -e CONTROL_PATH=/opt/bank/runtime/failure-demo.db bank ingest
docker compose run --rm bank ingest --start-date 2026-01-01 --end-date 2026-01-07
docker compose run --rm --entrypoint python bank -m bank.observability
```

The intentional first command exits nonzero. Inspect FAILED and unchanged watermark;
retry reuses raw and existing Bronze versions. `python scripts/e2e.py` automates
the real service gate including Spark/dbt and catalog/MinIO restart persistence.

## Iceberg exploration

Run query/validation.sql through Trino. Copy a real snapshot ID from
`lake.bronze."transactions$snapshots"`, then query
`SELECT * FROM lake.bronze.transactions FOR VERSION AS OF <actual_snapshot_id>`.
Use `ALTER TABLE lake.bronze.transactions ADD COLUMN source_note varchar` to demonstrate
evolution after completing streaming tests. Evolving the envelope requires updating
the stream's explicit projection before resuming it. Snapshot retention is deliberate;
do not delete files manually. Local tests already demonstrate schema change/time travel.

## Streaming

```sh
docker compose --profile streaming up -d --wait redpanda
docker compose exec redpanda rpk topic create bank.transactions --partitions 3
docker compose run --rm --entrypoint python bank -m streaming.producer
docker compose --profile processing run --rm spark streaming/consumer.py
```

Stop the Spark command, rerun it with the same checkpoint and compare distinct
event/alert counts. Replay the producer; no new business versions should appear.
Run dbt again to serve changed Silver; the standalone stream does not emit an
Airflow Dataset event. New stream-only transactions need corresponding GL postings
and source audit totals before reconciliation can PASS.

## Orchestration, FX and BI

`docker compose --profile orchestration up -d` starts Airflow on localhost:8081.
Read credentials from local .env. Check `airflow dags list-import-errors`, unpause
bank_bronze and trigger it; Dataset success schedules Silver then Gold. Startup is
one-time initialization; preserve airflow-data for subsequent starts.

`docker compose run --rm --entrypoint python bank -m bank.external` loads the labeled
offline FX fixture. Add --live only when endpoint access is available. BI serving
instructions and DAX are in bi/README.md. No PBIX or dashboard screenshots are claimed.

## Stop / reset

`docker compose --profile core --profile lakehouse --profile query --profile streaming --profile orchestration down`
stops services but preserves data. Only when the demo data is no longer needed,
add `--volumes` to remove named volumes and separately remove ./runtime. Never reset
the catalog while keeping warehouse data and assume tables will rediscover themselves.
