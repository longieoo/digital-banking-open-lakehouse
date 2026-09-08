# Engineering rules

Build a production-style educational banking data platform using synthetic data only.
Frozen flow: PostgreSQL/file/API → Python/Airflow → raw → Iceberg Bronze → Spark
Silver → Trino/dbt Gold → BI. Redpanda transports events to Structured Streaming.
MinIO is local storage; ADLS is the cloud alternative. Terraform owns Azure resources.

Use Decimal/NUMERIC for money, UTC timestamps, explicit types and deterministic seeds.
Keep ingestion outside DAGs and analytical business transformations in dbt.
Never advance a watermark until raw and Bronze validate durably. Preserve source versions.
Never log secrets or commit .env, runtime data, Terraform state or customer information.
Run `python -m unittest discover -s tests -v`, `ruff check .` and relevant integration
checks. Report PASS, FAIL, SKIPPED or UNAVAILABLE accurately; substitutes are not
proof that distributed infrastructure works. Before committing inspect `git diff`,
`git diff --check`, and `git status`.

Done means implementation, configuration, meaningful tests, error handling and docs
exist; available checks pass; boundaries and limitations are explained. Update
docs/implementation-plan.md and ADRs for meaningful decisions. Commit coherent changes.
