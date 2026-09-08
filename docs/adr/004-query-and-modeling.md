# ADR 004: Trino queries Iceberg; dbt owns Gold

## Context
Analysts need interactive SQL and reviewable dimensional logic without launching Spark jobs.
## Decision
Use the Trino Iceberg REST connector against the same catalog and object store.
dbt-trino creates Gold Iceberg tables. Spark remains responsible for conformance.
## Alternatives
Spark SQL for every query adds job/session overhead; duplicating Gold in Python
would create a second implementation to reconcile.
## Consequences
No data is copied to a Trino database. Trino stores/query-plans metadata, not table
files. Gold refresh uses dbt table materialization initially for deterministic late
correction handling. Customer history drives deterministic SCD2 reconstruction.
This trades incremental Gold speed for clear correctness on portfolio volumes.

Start `docker compose --profile query up -d trino`; then
`docker compose exec -T trino trino --user transformer < query/validation.sql`.
Windows PowerShell can pipe `Get-Content query/validation.sql` into that command.
Runtime result is UNAVAILABLE until the Docker gate runs.

References: [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html),
[dbt-trino](https://github.com/starburstdata/dbt-trino).
