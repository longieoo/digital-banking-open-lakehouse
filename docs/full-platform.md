# Full platform operation version 0.3

This repository retains all source, batch/stream processing, dbt models, tests,
infrastructure definitions, BI measures and operational tools. The certified
release is the reporting contract; an individual successful job is not sufficient.
Only synthetic banking data belongs in this project.

## Start from the complete source archive or repository

Use Python 3.11+, Docker Desktop Linux containers and network access for images,
Python packages and Spark Maven artifacts. A 16 GB or larger Docker allocation is recommended
for concurrent full-platform use. On smaller hosts, run Spark, Airflow acceptance
and streaming tests sequentially. Existing unrelated Docker projects are not stopped.

```sh
python -m venv .venv
# Activate .venv for your shell before the following commands.
python -m pip install -r requirements-test.txt
python -m scripts.platform doctor
python -m scripts.platform up --project bank-full --internal --all --seed
```

`doctor` reports missing local `.env` before first setup. `up` creates it from
the template only when absent. It never replaces existing credentials. `--seed`
explicitly prepares/replaces the selected demo database with deterministic synthetic
data; omit it to preserve an existing source. `--internal` removes core host ports,
while services use Docker DNS. BI and metrics retain loopback-only host ports.
`up` builds images, starts storage/source/catalog/Trino, certifies Gold, starts BI
and metrics, and with `--all` initializes Airflow, Redpanda, shared Silver and the
event consumer. It sends one deterministic demo event batch; checkpoints persist.

Use a separate runtime for every project with `--runtime runtime/my-demo`.
Override BI/metrics ports using `--bi-port 18090 --metrics-port 19091` when occupied.
Airflow's host UI remains port 8081; choose a Compose override if that port is occupied.

```sh
python -m scripts.platform status --project bank-full --internal
python -m scripts.platform stop --project bank-full --internal
```

Stopping preserves volumes, releases, checkpoints and credentials. These commands
must run from the repository root. A source ZIP does not need `.git` to run releases:
the runner falls back to a SHA-256 source revision. The installed Python wheel
includes CLI modules and dashboard assets, while Compose/SQL/configuration still
come from the complete source directory.

New Airflow DAGs remain paused by default so opening a historical demo does not
silently advance its reporting date or call an external API. To activate the daily
release and LIVE FX schedules, add `--all --enable-schedules` to `up`. Their logical
interval sets the business day; the local fixture itself is still January2026 data.

## Reporting and operational endpoints

- `http://127.0.0.1:8090`: interactive read-only certified-data dashboard.
- `http://127.0.0.1:9091/metrics`: numeric Prometheus exposition.
- `http://127.0.0.1:9091/status`: sanitized operational JSON.
- `http://127.0.0.1:9091/health`: process liveness; no release metadata is exposed.
- `http://127.0.0.1:9091/ready`: 200 only with readable control state and a
  certified bundle whose CSV hashes and row counts validate; otherwise 503.
- `http://127.0.0.1:8081`: Airflow when orchestration is started; local `.env`
  contains the generated demo administrator password.

BI resolves one release, verifies exported files, uses exact Decimal calculations,
requires a single currency and chooses the closing balance at the end of the selected
period. Transaction posting totals and transfer-once totals have different grains.
Never sum balances across dates or add USD/EUR/VND values without a documented FX
conversion. The dashboard is implemented; the separate Power BI DAX specification
is retained, and a native PBIX is not implied by this web UI.

The exporter mounts runtime read-only and opens SQLite using mode=ro. It does not
create/migrate operational databases. Missing/corrupt state reports unavailability,
not fabricated zeros or private exception content. Metrics are gauges over persisted
state, not monotonic process counters. Age of publication is measured from
certified_at; historical fixture business dates are not an availability SLA.

## Live reference FX

```sh
docker compose run --rm --entrypoint python bank -m bank.fx --as-of-date 2026-01-30
```

The same command replays the date and verifies content. Retry is bounded for
transient failures and honors a bounded Retry-After; no fixture is substituted for
an HTTP failure. Both requested date and actual published rate date are recorded.
`bank_fx_daily` runs at 04:00 UTC. `bank_release` optionally gates on reference FX
when its `fetch_live_fx` parameter is true; default false permits offline historical
reproduction. Provider restatements receive locally observed versions; identical
replay reuses its identity and earlier raw/Bronze observations remain available.
These are reference rates; native Gold monetary measures are not silently converted.

Protected serving, token rotation, concurrent-build guarantees and current-alert
correction semantics are described in [the hardening guide](hardening.md).

## Backup restore and lifecycle planning

```sh
python -m bank.backup create --release-root runtime/full-platform/releases --archive runtime/backups/certified.zip
python -m bank.backup verify --archive runtime/backups/certified.zip
python -m bank.backup restore --archive runtime/backups/certified.zip --release-root runtime/restored/releases
python -m bank.maintenance --release-root runtime/full-platform/releases --keep-latest 3
```

Backup/restore validate member paths, limits, exact CSV hashes/counts and reconciliation.
Restore refuses overwrite and does not publish current.json. Verify the restored
release before deliberately choosing it for serving. This is a certified data-product
backup, not PostgreSQL/MinIO/catalog/control/checkpoint infrastructure recovery.
Maintenance emits a dry-run inventory and referenced snapshot IDs, protects current,
in-progress, corrupt, recent and per-currency releases, and never deletes objects.
Iceberg snapshot expiration needs a separate coordinated policy and approval.

## Cloud acceptance

`python -m bank.cloud_check` performs offline readiness inspection. An explicitly
configured `--probe` checks only a unique synthetic raw object and removes only its
own verified version. See [Azure acceptance](azure-acceptance.md). Full Azure
acceptance additionally requires deployed authenticated catalog/compute, ADLS
identities/ACLs, secrets/network configuration and a certified cloud release. Terraform
source, init/validate or a local test do not prove those outcomes.

## Validation and handover

```sh
python -m unittest discover -s tests -v
ruff check .
python -m scripts.release_acceptance
python -m scripts.package_project --output runtime/distributions/bank-full.zip
```

Set COMPOSE_PROJECT_NAME, COMPOSE_FILE and BANK_RUNTIME_DIR consistently before
direct Docker/script commands; lifecycle flags only persist within that command.
Real integration tests need disposable services and RUN_STACK=1 or RUN_SPARK=1.
The source packager retains complete implementation files, protects existing ZIPs,
rejects secret artifacts and writes a per-file SHA-256 manifest. Runtime and actual
credentials are deliberately separate from source distribution.
