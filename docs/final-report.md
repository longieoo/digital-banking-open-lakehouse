# Full implementation report version 0.3

The educational banking platform implements PostgreSQL/file/API → Python/Airflow
→ raw → Iceberg Bronze → Spark Silver → Trino/dbt Gold → certified BI consumption,
with a Redpanda/Structured Streaming branch. Banking data is entirely synthetic.
Source, tests, infrastructure and documentation remain in the full repository.

Version 0.3 closes concrete defects in concurrent release builds, partial export
installation, current streaming alert corrections, same-day FX restatements and
unauthenticated HTTP consumption. The [hardening guide](hardening.md) describes the
changes and [its acceptance record](validation/hardening-2026-11-15.json) separates
actual infrastructure checks from local tests and external deployment gates.

## Completed operating and consumption path

Version 0.2 adds a browser dashboard over verified certified CSV exports, read-only
HTTP/Prometheus endpoints, bounded FX retries/provenance, a daily FX DAG and optional
release prerequisite, verified bundle backup/restore, lifecycle inventory and cloud
readiness/probe tooling. Source archives run releases without Git history; Python
distributions include processing/streaming/scripts and dashboard assets. Docker
contexts exclude runtime, private environment variants and Terraform caches/state.

Financial semantics remain explicit: customer SCD2 joins by event time; transfers
have two posting legs and a separate once-per-transfer fact; failed events do not
move balances; reversals are linked opposite postings. Decimal/NUMERIC and UTC are
used. Measures require one currency; balances are evaluated at a date rather than
summed through time. FX is reference data, not implicit conversion of Gold facts.

## Validation and result provenance

The 2026-11-15 dedicated Compose acceptance builds actual PostgreSQL, MinIO, REST,
Spark and Trino/dbt. The January 31 fixture has 225 transactions, 20 Customer 360 rows,
620 daily balances, 30 passing reconciliation groups and 15 hashed exports. Retry
preserves results; January 30 has 600 balances without moving current backward.
An injected failed dbt artifact blocks publication; this is not a network outage.

Live USD/EUR reference FX for January 30 was extracted over HTTP and persisted in
raw/real Iceberg. Replay retained one Bronze version and the same snapshot and
forward watermark. Rates are dated reference observations, not current trading
quotes. [Completion evidence](validation/full-platform-2026-11-15.json) records
final verification results and skipped/unavailable gates.

Historical evidence retains original dates in docs/validation. Earlier Docker
unavailability or fixture-only results do not supersede newer acceptance. The
Vietnamese Word report produced on 2026-10-08 describes the prior release; this
guide and ADR 010 document later operating additions.

## Remaining production and external gates

This is an executable educational local platform within its documented contract,
not a production banking deployment. Single-host coordination, full Silver/Gold
builds, retained stream history and simplified banking remain explicit boundaries.
General CDC/hard deletes, multi-node HA and incremental late-correction equivalence
require separate designs and load/failure evidence.

Azure deployment, cloud catalog/compute authentication and ADLS identity enforcement
need configured infrastructure/account access. A raw probe does not prove a cloud
release. The web dashboard runs; native Power BI PBIX/DAX execution is separately
unvalidated. Product-bundle restore is not full infrastructure recovery. Authored
GitHub workflow source does not establish a remote CI execution.

See [full lifecycle commands](full-platform.md), [limitations](limitations.md),
[ADR 010](adr/010-full-local-operation.md) and [release contract](releases.md).
