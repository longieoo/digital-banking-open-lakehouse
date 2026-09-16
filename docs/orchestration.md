# Metadata and data-aware orchestration

`config/ingestion.json` versions the existing explicit contracts. It contains source,
target, primary key, incremental column, partition column, strategy, batch size and
enabled flag. JSON is chosen for a stdlib parser; it is also valid YAML 1.2. Unknown
tables/strategies are rejected rather than generating arbitrary SQL. This runner
was introduced after explicit transaction/card/GL adapters and SQL tests.

Run `python -m bank.pipeline` inside the bank container (override entrypoint to
python). It refreshes independent PostgreSQL control totals, ingests sources in FK
order and pages to exhaustion. Source writers should be paused for a consistent
cross-table audit. Table deletions/tombstones and multi-host ingestion are unsupported.

Airflow's three thin DAGs form Bronze Dataset → Silver Dataset → Gold build/tests.
Each publishes only on task success. Retries use the same control state and
idempotent target writes. Manual `bank_bronze` parameters start_date/end_date invoke
transaction-only historical backfill. Run the normal pipeline to refresh all
controls and dimensions before a full reconciliation after backfill.

The resource-aware local Airflow demo uses SequentialExecutor and persistent SQLite
metadata. It is intentionally single-host; use a separate metadata PostgreSQL for
multi-worker deployment. No Docker socket is exposed to Airflow. Its image includes
Spark and dbt clients; it starts jobs rather than hosting business transformations.
Airflow image build, DAG import and Dataset-triggered Bronze → Silver → Gold were
validated locally on 2026-09-16/17. dbt runs in `/opt/bank/dbt-venv` to isolate its
dependencies from Airflow. Unpause all three DAGs; downstream Dataset DAGs are
initially paused too. Run `airflow dags list-import-errors` before enabling schedules.
