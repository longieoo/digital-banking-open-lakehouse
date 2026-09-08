# ADR 003: Iceberg v2 on MinIO via a persistent REST catalog

## Context
Spark, Trino and ingestion need a common atomic table state and recoverable history.
## Decision
Use Iceberg v2, Zstandard Parquet and Apache's REST fixture backed by persistent
SQLite for the single-host demo. Persist catalog and object-store volumes together.
Bronze is a source-aligned JSON envelope with version keys and ingestion metadata;
Silver exposes typed columns. Bronze daily identity partitions use business_date.
## Alternatives
Unmanaged Parquet needs external file manifests and custom transaction semantics.
A Python-only catalog prevents deployed cross-engine validation. A production REST
service adds operational overhead to a local demo.
## Consequences
The fixture is educational, unauthenticated and single-host. Only cooperating
ingestion processes using the shared State lock may write Bronze. Direct competing
writers can violate business-key uniqueness; Iceberg commits alone do not enforce
unique constraints. Snapshots are table-level, not transactions across layers.
Use environment-specific catalogs and warehouses for ADLS, never mix warehouses.

References: [Apache REST fixture](https://github.com/apache/iceberg/tree/main/docker/iceberg-rest-fixture),
[PyIceberg API](https://py.iceberg.apache.org/api/),
[Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/).
