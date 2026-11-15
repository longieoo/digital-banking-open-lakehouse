# Metadata and data-aware orchestration

`config/ingestion.json` versions the existing explicit contracts. It contains source,
target, primary key, incremental column, partition column, strategy, batch size and
enabled flag. JSON is chosen for a stdlib parser; it is also valid YAML 1.2. Unknown
tables/strategies are rejected rather than generating arbitrary SQL. This runner
was introduced after explicit transaction/card/GL adapters and SQL tests.

Run `python -m bank.pipeline` inside the bank container (override entrypoint to
python). It reads all pages and tables from one PostgreSQL REPEATABLE READ snapshot,
derives independent source control totals from that same view, and ingests in FK
order. Table deletions/tombstones and multi-host ingestion are unsupported.

The primary daily UTC DAG is `bank_release`: one pinned source capture, isolated
Spark Silver, then dbt build/tests and certification. It derives the business date
from the data interval, supports explicit manual dates/currency, and allows one
active run. Related facts, dimensions and controls accompany historical replay;
forward ingestion watermarks do not move. See [release runbook](releases.md).

Version 0.2 adds `refresh_reference_fx` before those three release tasks. Its
`fetch_live_fx` parameter defaults false for reproducible offline historical runs;
when enabled, a failed LIVE reference fetch blocks that release. `bank_fx_daily`
is the separate daily04:00UTC FX pipeline, with rate-date freshness checks, bounded
HTTP retry and immutable raw/Bronze replay. Reference FX is not Gold conversion.
The full-platform lifecycle can explicitly unpause both schedules with
`--all --enable-schedules`. New DAGs remain paused until that choice is made.

The three older Dataset DAGs remain a manual compatibility demo. bank_bronze has
no automatic schedule. Their mutable Gold tables are separate from the certified
CSV publication boundary. Existing task retries retain idempotent ingestion.

The resource-aware local Airflow demo uses SequentialExecutor and persistent SQLite
metadata. It is intentionally single-host; use a separate metadata PostgreSQL for
multi-worker deployment. No Docker socket is exposed to Airflow. Its image includes
Spark and dbt clients; it starts jobs rather than hosting business transformations.
Airflow image build, DAG import and Dataset-triggered Bronze → Silver → Gold were
validated locally on 2026-09-16/17. dbt runs in `/opt/bank/dbt-venv` to isolate its
dependencies from Airflow. Enable bank_release for the new workflow; its daily
schedule does not imply synthetic January data has become current. Run
`airflow dags list-import-errors` before enabling schedules.
