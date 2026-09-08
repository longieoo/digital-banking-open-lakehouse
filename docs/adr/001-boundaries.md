# ADR 001: Separate source, processing and analytical responsibilities

## Context
The project must be understandable and finishable while demonstrating an open lakehouse.
## Decision
PostgreSQL models operational integrity. Python extracts and validates. Airflow
orchestrates. Iceberg provides table state over MinIO locally or ADLS in Azure.
Spark owns Bronze-to-Silver typing and conformance. Trino exposes tables and dbt
owns dimensional logic. Redpanda is the sole event broker.
## Alternatives
A single Python database demo is easier but does not demonstrate the frozen
architecture. Multiple overlapping engines add maintenance without distinct benefit.
## Consequences
Local contract tests run without services; distributed acceptance needs Docker.
No local substitute is described as a real Iceberg, Spark or Trino integration.
