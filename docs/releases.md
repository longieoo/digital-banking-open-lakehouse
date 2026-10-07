# Certified reporting releases

The primary operational DAG is bank_release. The former three-stage Dataset demo
is retained for manual compatibility; bank_bronze no longer schedules automatically.
The new daily UTC DAG derives its default as-of date from the data interval, passes
it to dbt, and publishes only after all quality gates pass. Manual parameters allow
an explicit as_of_date, start_date and reporting currency.

## Local run

From the repository root, using the existing ignored .env:

```sh
docker compose --profile core --profile lakehouse up -d --wait postgres catalog
docker compose build bank spark dbt
docker compose run --rm bank seed
docker compose --profile query up -d --wait trino
python -m scripts.run_release --start-date 2026-01-01 --as-of-date 2026-01-31
```

Use the activated virtualenv's Python. Windows may require
`.venv\Scripts\python.exe` if the global python command is a Store alias.
For the already-validated local environment on Windows, select
`$env:COMPOSE_PROJECT_NAME='bank-de-upgrade'` and
`$env:COMPOSE_FILE='docker-compose.yml;docker-compose.ci.yml'` before running the
release command. Its core services use internal ports, avoiding host collisions.
The printed release_id selects runtime/releases/<id>/release.json and exports/.
runtime/releases/current.json is the only publication pointer. Resolve it once per
BI refresh, then load all CSVs from that one exports directory. Monetary CSV fields
are exact text; configure the BI importer with decimal types and one currency.
Candidate SQL tables are not the certified BI boundary.

To retry, use `python -m scripts.run_release --release-id <id>` with the original
dates/currency. IDs are 32 lowercase hexadecimal characters. A retry after prepare
has stored its snapshots uses those inputs. A preparation failure before the
manifest exists starts a new source read transaction. Partial raw/Bronze commits
remain recoverable and are not published.

The source has zero opening balances: the release start must cover its complete
ledger history. For related facts without business_date, selection follows the
transaction/journal parent. Customer history is versioned; account/card dimensions
remain current-state. Hard deletes and arbitrary-source CDC remain outside this
source contract; a resulting mismatch blocks certification.

## Gates and recovery

SOURCE_READY → SILVER_READY → CERTIFIED. Inspect failure.json for the sanitized
failed stage and secured local logs for details. A latest invalid source version
goes to a release-specific quarantine and blocks Silver. Missing/skipped/failed dbt
results, wrong business parameters or failed reconciliation block publication.
Files are exported before the pointer changes. The pointer is protected against
business-date regression and same-date older source cutoffs.

Copy a complete release directory, including release.json and exports, to a backup
location. With RELEASE_ROOT set to its parent, run:

```sh
python -m bank.release verify --release-id <id>
```

This verifies certified BI data hashes after restore. It does not restore PostgreSQL,
catalog/control databases, object-storage metadata or streaming checkpoints. Those
require a coordinated infrastructure backup procedure and a separate restore drill.

## Monitoring and performance

`python -m bank.observability` reports ingestion and current-release signals.
`--prometheus` emits numeric text metrics. Publication age is elapsed wall-clock
time since certification; business_date is a separate reporting period, especially
for historical synthetic fixtures. Recorded failures count release directories
with a failure record, including recovered runs, not every failed attempt.

New CLI builds record stage timings. Certified replay verifies the existing files.
The Spark microbenchmark adds one warm-up,
three repetitions, median timings, exact result comparisons and executed plans:

```sh
docker compose run --rm spark benchmarks/spark_experiments.py --rows 100000 1000000 --repeats 3
```

The result is runtime/spark-benchmark.json. It measures synthetic joins, not a
million-transaction banking pipeline or distributed cluster throughput. Full
incremental Silver/Gold, bounded streaming and retention/compaction remain later
optimizations against the correctness reference provided by certified releases.

## Acceptance and CI

`python -m scripts.release_acceptance` runs actual two-date releases, retries and
an injected incomplete dbt artifact failure. Use only a dedicated synthetic demo
environment: it seeds sources. `RUN_STACK=1` tests real recovery/backfill and a shared
read snapshot under concurrent commits; `RUN_SPARK=1` tests typing, all risk rules
and control-group removal.

On Linux the shared runtime is owned by container UID 50000. The host runner must
be able to write beside those artifacts; the dedicated CI runner invokes acceptance
with sudo and preserves only the two Compose selection variables. Windows Docker
Desktop bind mounts were used for the local acceptance run.

GitHub workflow_dispatch has a distributed boolean input for a dedicated 16GB+
self-hosted Linux runner labelled bank-lakehouse. It stores release evidence and
stops its own Compose services. Authored workflow is not proof of a GitHub run.
Internal-only docker-compose.ci.yml avoids host-port collisions (Compose with
!reset support, version 2.24.4+). Windows COMPOSE_FILE uses semicolons; Linux uses
colons. Use a unique COMPOSE_PROJECT_NAME; do not mix control files between stacks.
For simultaneous stacks also set BANK_RUNTIME_DIR to a different ignored directory
per environment; the CLI resolves its local release root from that directory.

For shared/cloud deployments, retain the external Azure/FX setup and add the
appropriate authenticated Trino/catalog, distributed control state and access
policies before exposing services. The local release procedure uses existing demo
identities and a filesystem publication boundary.
