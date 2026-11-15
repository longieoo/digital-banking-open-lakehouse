# Implementation and validation ledger

Statuses: PASS = executed successfully; FAIL = executed and failed; SKIPPED = an
available procedure was not run; UNAVAILABLE = prerequisite absent. Code existence
alone does not establish runtime acceptance.

| Phase | Scope | Validation |
|---|---|---|
| 0 | Bootstrap | PASS: package test; commit a27208a |
| 1 | Sources/generator | PASS: 4 deterministic/invariant tests. PostgreSQL startup UNAVAILABLE (Docker absent). |
| 2 | Incremental/recovery/backfill | PASS: 7 failure/state tests using explicit SQLite Bronze contract double. |
| 3 / 3.5 | MinIO/REST/Iceberg and real infrastructure | PASS: real Iceberg snapshot, replay, evolution, time-travel, metadata and persistent SQL-catalog reopen test. REST/MinIO Docker integration UNAVAILABLE. |
| 4 | Spark Silver | PASS: Python compilation. Runtime test UNAVAILABLE (Java/Docker absent). |
| 5 | Trino | PASS: YAML/config parse. Live query UNAVAILABLE. |
| 6 / 7 | dbt Gold and data products | PASS: 4 business SQL tests executing actual models through DuckDB translation; Trino materialization UNAVAILABLE. |
| 8 | GL/reconciliation | PASS: 5 actual SQL mismatch tests, including balanced GL misallocation; good data PASS. |
| 9 | Performance | PASS: measured 1K/10K local Iceberg/Parquet. Spark/JDBC experiments implemented, runtime UNAVAILABLE. |
| 10 / 11 | Metadata/Airflow | PASS: metadata routing tests and DAG syntax. Airflow import/runtime UNAVAILABLE. |
| 12 | Streaming | PASS: event identity and SQL rule tests. Broker/Structured Streaming restart UNAVAILABLE. |
| 13 / 14 | Quality/security | PASS: 33 executed tests, lint, masking/rules and fixture checks. 2 service tests SKIPPED for unavailable runtime. |
| 15 | Azure/Terraform | PASS: Terraform 1.11.4 init, fmt and validate with AzureRM 4.26.0. Azure plan/apply UNAVAILABLE (no credentials). |
| 16 / 17 | CI/observability/BI | PASS: workflow YAML and monitoring code checks. CI hosted run / Power BI GUI UNAVAILABLE; DAX and semantic specification supplied. |
| 18 | Portfolio freeze | PASS: clean-clone 33 local tests, lint, hygiene, demo, dbt parse. Terraform fresh-clone init FAIL (local checksum file-access error); original validate PASS. Distributed gates remain UNAVAILABLE. |

Environment discovery: Python 3.12 and Git available; Docker daemon/Java absent.
Terraform was subsequently downloaded and verified; init/fmt/validate PASS.
No live infrastructure or Azure deployment is claimed. See validation/local-tests.json.

## Local Windows rerun — 2026-09-16

- PASS: Python 3.12.14 virtual environment installed from requirements-test.txt;
  33 tests passed, 2 service tests skipped because their runtimes were unavailable.
- PASS: local demo returned 113 source/Iceberg/Gold events, 8 Customer 360 rows,
  248 daily balances and zero reconciliation failures. Existing local-demo state
  was preserved; this was a rerun, not fresh-clone acceptance.
- PASS: Ruff, Python compilation, tracked-file hygiene, dependency consistency,
  all-profile Compose configuration validation and dbt 1.11.14 / Trino adapter
  1.9.3 parsing without a live server.
- UNAVAILABLE: distributed acceptance. Docker Desktop 4.85.0 failed during
  startup while initializing its Inference manager: the local dockerInference
  socket could not be accessed (Windows error 1920). Restarting reproduced the
  error; a non-destructive socket rename was also refused. The documented Compose
  startup command timed out waiting for the unavailable engine. No stack runtime
  success, Spark execution or live dbt build is claimed.
- Changes: created ignored .venv and .env using the documented helper, installed
  test/dbt dependencies, and refreshed ignored runtime/demo and dbt parse outputs.
  No application code, dependency pins or Compose settings required modification.
  Git ownership checks used a command-scoped safe.directory override; global Git
  settings were not changed. No Docker factory reset or volume removal was done.
- Next gate: restore a working Docker Desktop Linux engine, then rerun the
  distributed batch commands in docs/demo.md and scripts/e2e.py.

## Distributed local validation — 2026-09-16/17

This later run supersedes the earlier local Docker UNAVAILABLE status above;
historical artifacts retain their original results.

- PASS: PostgreSQL, MinIO, REST catalog, Trino, Redpanda and Airflow scheduler/webserver
  start with healthy service checks. The Docker engine was available on the resumed
  run; no Docker reset or removal of data volumes was needed.
- PASS: all 12 source tables through raw/Bronze and Spark Silver, with zero rejected
  source rows; 225 transaction business events, 20 Customer 360 rows and 620 balances.
- PASS: dbt on live Trino, 14 table models and 31 data tests. All 30 reconciliation
  day/currency rows pass, covering 225 source and Gold transactions.
- PASS: the final expanded scripts/e2e.py run builds/tests Gold both before and
  after restarting catalog/MinIO (45/45 on each build), verifies unchanged Bronze
  counts and rechecks reconciliation. See validation/docker-tests.json.
- PASS: real PostgreSQL/MinIO/REST failure/retry, no-op replay, version update,
  backfill and snapshot integration test. The Spark integration test verifies exact
  decimal typing, quarantine and all three risk rules, including threshold boundaries.
- PASS: Redpanda delivers 230 messages representing 225 unique events. Restarting
  the consumer with the same checkpoint and replaying the producer preserves 225
  unique events and 226 Bronze versions (one additional version is intentional in
  the recovery integration test). Default demo risk alerts remain zero.
- PASS: Airflow Bronze succeeded and triggered Silver through a Dataset; Silver
  succeeded and triggered Gold. Gold initially failed on a SQLite catalog lock,
  then passed all 45 dbt steps after the catalog fix and retry.
- PASS: local unit/contract checks, Ruff, compilation, Compose configuration,
  Airflow DAG import, both Airflow/dbt dependency checks and analyst Gold-only access.
- PASS: Terraform 1.11.4 fmt/init/validate with AzureRM 4.26.0 in a Linux container;
  the lock file now includes the verified Linux/provider registry checksums.
- External boundaries: FX fixture ingestion passes; the live API still returns
  HTTP 403. Azure plan/apply/identity and an actual Power BI Desktop artifact remain
  unvalidated and are not claimed as local-stack completion.

Fixes are documented in [ADR 007](adr/007-local-runtime-compatibility.md): catalog
volume permissions and a single JDBC client, Spark's writable persistent Ivy cache,
deferred shared annotations for the pinned Spark image, isolated Airflow/dbt
dependencies and matching aiobotocore, and health/write-after-restart E2E gates.
No financial transformations or dependency versions of standalone dbt were changed.

## Live FX follow-up, 2026-10-05

ADR 008 replaces the failing fixed-date v1 route with Frankfurter v2 latest USD/EUR
and adds --fetch-only. PASS: real HTTP extraction on Windows and in Docker, raw/
Iceberg Bronze ingestion of one LIVE row, direct persisted-table read, and replay
with zero new rows. PASS: 36 local checks and Ruff; two opt-in infrastructure tests
were skipped in this run. The prior full-stack validation remains historical.

Docker was initially paused. After it became available, catalog startup encountered
a host port 8181 collision. Validation used ignored runtime/fx-compose-override.yml
with catalog ports reset to an empty list, preserving internal Docker connectivity.
No unrelated process was stopped. Repeat these FX integration commands with
`docker compose -f docker-compose.yml -f runtime/fx-compose-override.yml` while the
host port remains occupied. Azure, Power BI and scheduled live FX remain outstanding.

## Data Engineering upgrade — 2026-10-07

The upgrade keeps the frozen architecture and prioritizes reproducible reporting,
source consistency, recovery, quality and measured execution. ADR 009 and
docs/releases.md define the new primary path. Azure/FX connectivity is treated as
already complete for this scope; this run does not re-certify Azure deployment.

- PASS: 49 tests discovered; 45 execute locally and four opt-in tests skip there.
  Those four then pass against real infrastructure: two PostgreSQL/MinIO/REST
  tests (recovery and a shared read snapshot under a concurrent commit), and two
  Spark tests (typing/quarantine/risk rules and disappearing control groups).
- PASS: full legacy scripts/e2e.py pipeline and write after catalog/MinIO restart.
  Each Gold build now runs 15 models and 33 tests, 48/48 successful steps.
- PASS: scripts.release_acceptance using actual PostgreSQL → raw/Bronze → Spark
  Silver → Trino/dbt Gold. January 31 has 225 transactions, 20 Customer 360 rows,
  620 balances and 30 passing reconciliation groups. January 30 has 600 balances.
  Existing-ID replay verifies exports without rebuilding; historical certification
  does not replace the current January 31 pointer. An injected failed dbt artifact
  blocks certification and preserves the pointer. This is not a network-outage test.
- PASS: a separate real invalid-currency source test blocks strict Silver and
  preserves the last certified pointer. The synthetic source was restored afterward.
- PASS: Airflow dags test bank_release executes all three tasks successfully;
  its January 31 UTC interval correctly derives January 30 as the reporting day.
  This proves DAG execution, not a long-running daily scheduling history.
- PASS: copied certified BI bundle verifies all 15 export SHA-256 hashes and
  30/30 reconciliation results. This is a file-bundle restore check, not recovery
  of PostgreSQL, object storage, catalog/control databases or stream checkpoints.
- PASS: measured Spark local[2] joins at 100K and 1M rows, 12 configurations,
  one warm-up and three repetitions per case. All exact-money totals agree.
  This microbenchmark is not end-to-end banking or multi-node throughput.
- PASS: Ruff, compilation, dbt parse, Compose/workflow configuration and numeric
  monitoring output. GitHub CI execution, JDBC benchmarking and a native Power BI
  Desktop/DAX execution remain UNAVAILABLE in this validation scope.

Resolved during integration: date filtering on related facts without business_date;
export identifiers containing digits (Customer 360); stale control groups after
source correction; and explicit Spark test-table cleanup because Iceberg REST
does not cascade-drop a nonempty namespace. The concurrent-view test now finds its
record by ID because source version triggers legitimately change sort order.
Airflow's primary Compose volume set now also mounts the shared Ivy cache; its
image creates that path with the Airflow UID so standalone runs have write access.

Evidence: docs/validation/de-upgrade-2026-10-07.json. Detailed secured local logs,
release manifests, CSVs and benchmark plans remain under ignored runtime/.
Docker resources use a dedicated bank-de-upgrade Compose project; validation used
the existing host runtime directory, so filesystem control state was not isolated
from earlier local runs. Forward watermarks stay unchanged during release replay.
Full incremental Silver/Gold, general CDC/deletes, distributed locking, retention/
compaction and shared-service authentication remain explicitly bounded follow-ups.

## Full local platform completion 2026 11 15

Version0.2 retains all existing batch/streaming/modeling/infrastructure modules and
adds executable certified BI, HTTP readiness/metrics, scheduled/retryable LIVE FX,
verified product backup/restore, safe lifecycle planning, cloud preflight/probe,
source distribution and full-platform startup/status/stop commands. ADR010 records
the operating and external acceptance boundaries.

- PASS: 96 tests discovered;91 run locally and5 skip. The four opted-in real
  infrastructure tests pass separately; the Windows-unavailable symlink test passes
  on Linux as part of three integrity tests. No skip is represented as a local pass.
- PASS: dedicated, filesystem-isolated Compose release acceptance;225 transactions,
  20 Customer360 rows,620 balances,30 reconciliation groups and15 verified exports.
  Live Trino/dbt completes15 models plus33 tests; retry, historical publication
  protection and injected failed-artifact rejection pass.
- PASS: the upgraded four-task bank_release and bank_fx_daily execute in Airflow.
  Scheduler/webserver startup health also passes. This is not a long-running
  scheduling history; enabling daily schedules is an explicit lifecycle option.
- PASS: real HTTP FX → raw/MinIO → Iceberg Bronze; replay retains one version and
  one snapshot, with unchanged forward watermark and explicit LIVE provenance.
- PASS: real Redpanda producer230messages plus230replay; two availableNow Spark
  drains reuse the checkpoint and retain225 unique events and228 Bronze versions.
  Extra versions come from actual source-update integration checks; alert count0.
- PASS: real dashboard HTTP data, filters/error boundaries and exporter readiness;
  monetary aggregation uses Decimal strings, a single currency and date-end balances.
  Five serving tests cover tampering and publication/read consistency. Browser visual
  QA remains NOT_VERIFIED; HTTP and data-contract acceptance are recorded separately.
- PASS: current15-export certified-bundle backup/verify/restore without publication
  or overwrite; retention inventory protects referenced/current/in-progress releases.
- PASS: Ruff, compilation, all Compose profiles, JavaScript syntax and complete wheel
  module/assets inventory. Docker contexts now exclude Terraform/provider caches
  and private env variants; source archives no longer require Git history to run.
- PASS: Terraform1.11.4/AzureRM4.26 fmt/init/validate using ephemeral TF_DATA_DIR and
  read-only infra/lockfile. Initial shared-memory failure and deliberately cancelled
  retry remain recorded. No Azure authentication/plan/apply was attempted.

Evidence: [full-platform-2026-11-15.json](validation/full-platform-2026-11-15.json).
Detailed logs stay in ignored runtime/completion-2026-11-15. Remote GitHub CI,
authenticated Azure release, native PBIX/DAX execution and whole-infrastructure DR
remain distinct unvalidated/unimplemented gates; no production-completion claim.
