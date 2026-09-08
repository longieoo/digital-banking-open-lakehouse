# ADR 002: Composite watermark and durable replay

## Context
Rows share timestamps. Crashes may occur before or after an Iceberg commit.
## Decision
Use `(updated_at, business key)`, ordered ascending, with strict tuple comparison.
Raw is canonical JSON with a content-addressed immutable path. Bronze preserves
`(record_id, source_updated_at)` and rejects conflicting payloads at the same version.
SQLite stores durable manifests and watermarks on a persistent local volume. A
separate SQLite write lock serializes ingestion across cooperating local processes.
Completion plus watermark update is a single transaction after target verification.
## Alternatives
Timestamp-only loses ties; advancing first loses data; rebuilding tables hides failure.
## Consequences
This is a single-host control store, not a multi-host coordinator. PostgreSQL writers
must use increasing updated_at per record. A transaction committed later with an old
timestamp can fall behind a forward watermark: use explicit business-date backfill.
Deletes require tombstones (not implemented). Backfill reads current source rows, not
unavailable historical versions; raw/Bronze preserve versions already observed.

Retries re-extract the bounded request and reuse identical raw by checksum. Source
changes between retries can produce another raw object; both are retained. A crash
after Bronze but before completion replays a safe upsert. STARTED manifests after a
crash are diagnostic, and a retry restarts the same request. Orphan raw is harmless.
Full backfills are bounded by date, but currently materialized in memory: split large
ranges. No claim of distributed exactly-once execution is made.
