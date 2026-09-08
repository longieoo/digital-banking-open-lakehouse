# Final implementation report

## Final status

The requested architecture and mandatory modules are implemented with coherent
milestone commits. Available local validation passes. Distributed runtime acceptance
remains open because Docker/Java were unavailable; this is not a claim of a fully
validated production deployment. The project is a production-style educational
platform with synthetic banking data only.

## Architecture implemented

PostgreSQL core/card/GL sources, deterministic Python generator, durable ingestion
control, immutable raw, MinIO and persistent Iceberg REST catalog configuration,
real Iceberg Bronze adapter, Spark Silver, Trino, dbt Gold, Redpanda and Structured
Streaming, Terraform ADLS/Key Vault, Airflow Datasets, CI, metrics and BI guidance.
See [architecture and ER diagrams](architecture.md).

## Data flow

Batch: sources → Python/Airflow → raw → Bronze Iceberg → Spark conformance/quarantine
→ Silver Iceberg → Trino/dbt → Gold. Stream: synthetic events → Redpanda → Spark
microbatch → shared immutable Bronze → shared Silver → rule alerts. MinIO and ADLS
are environment alternatives. External FX is a separate explicitly labeled fixture
ingestion because the live endpoint returned HTTP 403.

## Business capabilities

Six dimensions, four facts and four marts provide Customer 360, transaction trends,
dense daily balances, branch/card analytics and financial reconciliation. Customer
SCD2 preserves segment/branch/status intervals and historical transaction joins.
Core postings include transfer legs; the transfer fact counts each transfer once.
Failed events do not move balances; reversals remain separate linked opposite events.

The local demo actually produced **113 source/Iceberg/Gold events, 8 customer rows,
248 daily balance rows and 0 reconciliation failures**. Silver is a typed fixture in
that demonstration; the Gold SQL is the real model SQL, translated for DuckDB.

## Technologies and responsibilities

PostgreSQL enforces operational integrity. Python extracts/generates/validates;
Airflow orchestrates. Iceberg manages table state over object storage. Spark owns
conformance, Trino query access and dbt analytical models. Redpanda transports events;
Structured Streaming coordinates microbatch processing. Terraform owns Azure
infrastructure. Docker profiles and GitHub Actions provide reproducibility/CI.
Power BI is specified as the consumer, not fabricated as a binary artifact.

## Reliability guarantees

Composite timestamp/ID cursor prevents timestamp ties being skipped. Canonical raw
content hashes and immutable Bronze business-version keys enable safe replay.
Raw and Bronze are read back before completion. Completed manifest and cursor update
share a durable SQLite transaction. Failure leaves the cursor unchanged, including
failure after Bronze commit; retry reuses prior work. Date-range backfill has its own
manifest and does not mutate forward state. Cooperating writers use a single-host
SQLite lock. These guarantees require monotonic source versions, no hard deletes,
and the documented writer/cutoff contract; they are not distributed exactly-once.

## Testing

**35 tests discovered: 33 PASS, 2 SKIPPED, 0 failures/errors.** Skips are the real
Spark and PostgreSQL/MinIO/REST service gates. Tests cover generator integrity,
composite watermarks, duplicate/conflict handling, raw failure/corruption, commit
failure, post-commit recovery, backfill, SQL business behavior, masking, routing,
metrics and configuration. Actual local Iceberg tests cover snapshots, time travel,
schema and partition evolution, manifests/data files, retry and catalog reopen.

Lint, Python compilation and dbt 1.11.14/trino adapter 1.9.3 parsing pass. Gold/risk
business SQL executes through DuckDB; it does not prove Trino/Spark interoperability.
See [machine-readable local results](validation/local-tests.json).

## Data quality

Conformance rejects invalid fields, references, currencies, monetary scale and
reversal links. dbt checks uniqueness, required keys, SCD interval overlap, one
current customer, transfer legs, card/core agreement, dense balance continuity,
missing balance activity and reconciliation. Mismatch tests detect missing Bronze,
missing Gold, duplicated versions, unbalanced journals, balanced wrong amounts and
balanced GL misallocation. FAIL is visible in both the mart and dbt gate.

## Security

All identities are SYN identifiers; email uses .invalid and no card/PIN/CVV data
exists. Gold excludes direct PII and uses masked display labels. PostgreSQL has
masked views/NOLOGIN roles; Trino has Gold-only analyst rules. Runtime writers share
a dedicated container UID and persistent directory. Secrets, runtime files, keys,
Terraform state and plans are ignored. Manifest/CLI errors are sanitized.
Local Trino usernames are unauthenticated and REST is a local fixture: authenticated
shared deployment is an explicit remaining security gate.

## Streaming

Version-derived event IDs, account partition keys, persistent offsets/checkpoints,
durable deduplication, late-arrival audit and deterministic SQL rules are implemented.
Tests validate identity, amount boundaries and transfer-window behavior. Broker and
actual Structured Streaming restart are UNAVAILABLE. Retained history is recomputed
per batch; alerts are observations rather than automatically retracted decisions.

## Cloud / Terraform

Terraform 1.11.4 and AzureRM 4.26.0 **init/fmt/validate PASS** using verified release
archives. Resources include Resource Group, HNS-enabled storage, raw/warehouse,
scoped pipeline RBAC, restricted network access and RBAC Key Vault. Windows/Linux
provider archive hashes are in the lock file. No Azure plan/apply, ADLS I/O, identity
test or cost measurement occurred. Cloud catalog hosting/identity remains manual.

## Performance experiments

Actual single-process Windows local-file measurements at 1K and 10K rows are saved
in [local-measurements.json](performance/local-measurements.json). These are single
samples with cache/setup noise, not a statistical or distributed benchmark. The
Iceberg path includes key verification and metadata commit. Spark/JDBC experiment
scripts exist but no timing numbers are claimed without their runtime.

## Validation matrix

| Component | Status | Evidence / limit |
|---|---|---|
| Generator/business invariants | PASS | Deterministic/reference/GL/transfer tests |
| PostgreSQL service/constraints | UNAVAILABLE | DDL and real gate supplied; no Docker |
| Ingestion/recovery/backfill | PASS | Contract tests and actual local Iceberg demo |
| MinIO + REST deployment | UNAVAILABLE | Persistent Compose config; real gate supplied |
| Real local Iceberg format | PASS | Snapshots, time travel, evolution, metadata/reopen |
| Spark runtime | UNAVAILABLE | Compiled implementation; runtime test SKIPPED |
| Trino runtime/dbt materialization | UNAVAILABLE | SQL models supplied; no query service |
| dbt parsing / Gold business SQL | PASS | Adapter parse and actual SQL contract tests |
| Reconciliation | PASS | Good data and five controlled mismatch tests |
| Airflow runtime | UNAVAILABLE | Thin Dataset DAGs; syntax checks only |
| Rule SQL/event identity | PASS | Deterministic local tests |
| Redpanda/checkpoint restart | UNAVAILABLE | Broker/client code; no running runtime |
| External live rates | UNAVAILABLE | HTTP 403; fixture explicitly labeled |
| Security hygiene/masking | PASS | Code/config tests; shared identity enforcement pending |
| Terraform configuration | PASS | init, fmt, validate |
| Azure deployment/I/O | UNAVAILABLE | No credentials; no resources deployed |
| Local benchmark | PASS | Only saved measured samples |
| Spark/JDBC benchmark | UNAVAILABLE | Executable experiment; no fabricated results |
| GitHub Actions hosted run | UNAVAILABLE | Workflow configuration exists; not published/run |
| Power BI artifact | UNAVAILABLE | Semantic guide/DAX supplied; no PBIX |
| Clean-clone local gate | Pending | Final repository audit in progress |

## Repository structure

bank/ contains generators, ingestion, adapters, metadata runner and metrics; sources/
contains PostgreSQL and external contracts. processing/, query/, dbt/, streaming/
and orchestration/ implement the data path. infra/terraform/, docker/ and workflows
configure runtime. tests/, benchmarks/, monitoring/, bi/ and docs/ provide acceptance,
measurement, consumption and explanation. No empty placeholder modules were created.

## Git milestones

See [the milestone history](validation/git-milestones.txt). Commits progress through
bootstrap, sources, reliable ingestion, Iceberg, Spark, Trino, dbt, reconciliation,
performance, orchestration, streaming, quality/security, Terraform, CI/BI and final
correctness/documentation review. No remote was created or pushed.

## Known limitations

The [limitations register](limitations.md) distinguishes implementation from runtime
acceptance: single-host control, full Silver/Gold scans, memory-resident generation/
backfill, simplified banking assumptions, stable audit cutoffs, unbounded retained
stream history, unauthenticated local identities, missing cloud catalog deployment
and unavailable BI execution. Nothing is represented as production banking or ML fraud.

## Manual validations still required

Run the real Docker E2E and Spark tests, test broker restart/duplicate replay, inspect
Airflow import errors and Dataset runs, verify analyst deny rules under real auth,
measure Spark/JDBC workloads, deploy Azure only with an authorized account, and build
a genuine Power BI model. These are concrete runtime gates, not claims of completed tests.

## How to run locally

Install requirements-test.txt and run `python -m unittest discover -s tests -v`, then
`python -m scripts.local_demo`. For the full stack, generate ignored .env and follow
[docs/demo.md](demo.md). `python scripts/e2e.py` runs the dedicated Docker acceptance.

## How to deploy Azure resources

Follow [Azure steps](azure.md): authenticate, fill demo.tfvars from the example,
init/fmt/validate, review plan, apply, validate identities/I/O and destroy the demo
afterward. [Cost controls](cloud-cost.md) explain persistence and soft deletion.

## Portfolio highlights

The strongest demonstrated evidence is failure-safe ingestion, real Iceberg table
state, historical customer modeling, exact dense balances, and reconciliation that
detects balanced-but-wrong journals. The documentation explains both guarantees
and failure boundaries without inflated claims.

## Interview defense topics

[Twenty-five grounded answers](interview-defense.md) cover cursors, replay, backfill,
Iceberg, Spark shuffles, Trino/dbt boundaries, fact grains, SCD2, balances, GL, Kafka,
late events, ADLS, Terraform, masking and 10x-volume trade-offs. Coursework mappings
reuse the same implementation rather than maintaining duplicate projects.

## Next steps

Execute the outstanding distributed acceptance gates first. Then replace measured
full-history bottlenecks, validate cloud identity and build BI artifacts. Keep the
architecture frozen until those results justify a specific change.
