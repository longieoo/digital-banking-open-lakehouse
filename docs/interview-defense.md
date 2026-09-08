# Interview defense grounded in this repository

1. **Why PostgreSQL?** Operational PK/FK/CHECK integrity, exact NUMERIC and indexed
   source queries. Source domains are schemas in one demo instance.
2. **Why incremental?** Re-extracting every transaction wastes I/O. Strict composite
   cursors page only newly updated rows; Silver/Gold currently favor full logical rebuilds.
3. **Why composite watermark?** Many events share updated_at. The ID breaks ties,
   as the two-row-page test demonstrates.
4. **How are retries idempotent?** Content-addressed raw and immutable business-key/
   source-version Bronze identities; conflicting payloads at one version fail.
5. **What happens on failure?** Manifest FAILED, cursor unchanged. A post-commit crash
   replays existing versions safely. Completion and cursor update share one SQLite transaction.
6. **Backfill?** Inclusive business-date bounds, own manifest, no forward-state mutation.
   It recovers current source rows and observed raw history, not nonexistent CDC history.
7. **Why object storage?** Separate storage from engines and keep durable replay/table files.
8. **Why Iceberg over Parquet?** Parquet is a file format; Iceberg tracks atomic table
   state, versions, schemas and partitions. It is not universally faster.
9. **Snapshot?** A committed view of an Iceberg table's file set. Local tests read an
   earlier snapshot after another commit and reopen the persisted catalog.
10. **Why Spark?** Typed conformance, joins, deduplication and MERGE at larger volume;
    source control metadata stays in Python.
11. **Shuffle?** Window/group/join/repartition operations redistribute keys. Inspect
    benchmark plans; broadcast small dimensions and avoid excessive tiny partitions.
12. **Why Trino?** Interactive SQL over the same table files, without starting a
    Spark job for each analyst query.
13. **Why dbt?** Reviewable Gold SQL, dependencies, materializations and data assertions.
14. **Fact grain?** Transaction event; transfer (two legs); card purchase; account/date
    balance. Never sum purchase facts again into core posting balances.
15. **SCD2?** Customer source history is collapsed to segment/branch/status change
    intervals. `[valid_from,valid_to)` joins historical transactions exactly once.
16. **Balance?** Zero opening contract, signed successful postings, dense account/date
    scaffold, cumulative balance. Failed events contribute zero; reversals offset originals.
17. **Customer 360?** Preaggregate accounts/cards/balances/30-day activity separately
    before joining to one customer, avoiding many-to-many metric multiplication.
18. **Core versus GL?** Independent source totals, latest Bronze count, Gold gross
    successful amount, and per-event journal amount/date/currency/control-account agreement.
19. **Debit differs from credit?** FAIL mart row and failing dbt test. Balanced wrong
    amounts/control-account allocations also fail.
20. **Why Kafka/Redpanda?** Durable partitioned transport with offsets/replay. Only
    Redpanda is deployed; Kafka refers to its protocol.
21. **Duplicate streams?** Version-based event IDs and durable merges. Checkpoint
    replay is at-least-once; common Bronze/Silver prevents double-counting batch overlap.
22. **Why MinIO and ADLS?** Environment alternatives. Changing URIs/FileIO does not
    migrate existing data/catalog metadata automatically.
23. **Terraform?** Repeatable Azure resources/RBAC/network configuration and teardown.
    Validation passed; no cloud deployment is claimed.
24. **PII?** Raw/Silver restricted, Gold excludes direct PII, display labels masked,
    role rules provided. Local usernames are unauthenticated, an explicit limitation.
25. **10x volume?** Measure source throughput, shuffle and file sizing; split backfills,
    compact small files, and implement safe incremental Silver/stream state before
    adding technologies. Current benchmark results do not prove distributed scale.

Demonstrate tests and failure manifests first. Clearly distinguish local Iceberg/
SQL validation from outstanding Docker, cloud and BI acceptance. No production-bank,
exactly-once transport or ML-fraud claims are supported by this implementation.
