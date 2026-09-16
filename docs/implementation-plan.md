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
