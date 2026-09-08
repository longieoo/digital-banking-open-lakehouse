# Digital Banking Open Lakehouse Platform

A production-style **educational** banking data platform with a simulated digital
banking workload. All identifiers and customer data are synthetic. This is a
portfolio project, not a production banking system.

The platform demonstrates recoverable incremental ingestion, versioned Iceberg
tables, conformed Spark processing, dbt banking marts and financial reconciliation.

See [implementation status](docs/implementation-plan.md). The repository is built
in tested milestones; runtime validation is reported separately from code availability.

## Development

Python 3.11+; `python -m pip install -e '.[dev]'` then
`python -m unittest discover -s tests -v`. Distributed services require Docker Compose.
