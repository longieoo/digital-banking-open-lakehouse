# Iceberg and unmanaged Parquet

Run `python -m benchmarks.iceberg_parquet --rows 1000 10000` with test dependencies.
Both paths write identical envelopes, Zstandard compression and a business-date
partition. Iceberg additionally verifies existing keys and commits table metadata;
the experiment intentionally measures that complete operation, not only encoder speed.
Single samples include filesystem/cache noise and are not statistical evidence.

| Capability | Unmanaged Parquet directory | Iceberg |
|---|---|---|
| Table state | File listing, custom convention | Atomic catalog metadata pointer |
| Append visibility | Individual files | One snapshot commit |
| Time travel | Keep and track files manually | Snapshot ID |
| Schema evolution | Reader conventions | Tracked schema/field IDs |
| Metadata | Per-file footer | Manifests, snapshots, schema, partitions |
| Retry | Application file tracking | Still needs business-key idempotency |
| Partition evolution | Rewrite/reorganize convention | Multiple tracked partition specs |

Iceberg is chosen for table-state management, not a promise that every query is
faster. See `local-measurements.json` for measurements actually executed. Full MinIO,
Spark, JDBC and remote-query timings remain unmeasured until their runtimes run.
