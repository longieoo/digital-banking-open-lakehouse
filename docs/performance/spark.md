# Spark experiment protocol

Run `docker compose --profile processing run --rm spark benchmarks/spark_experiments.py --rows 100000 1000000 --jdbc`.
Increase to 5M/10M only after inspecting local RAM and duration. Record hardware,
Docker limits, Spark version, input count, source index and cache state with results.
The script measures actual actions, prints physical plans and records aggregate
checksums for JDBC. `numPartitions` controls source connections; more partitions
can overload PostgreSQL. Bounds divide the read, not filter the source.

Compare 1/4/8 shuffle partitions and broadcast versus merge hints. AQE can override
expectations; inspect plans. Repartition shuffles; coalesce reduces partitions but
can concentrate work. Daily Iceberg partitions help date predicates; tiny files
eventually require compaction. At 10x volume, tune batch size, parallel source load,
file sizing and incremental Silver change capture before adding technologies.

The join benchmark now uses one warm-up and three timed repetitions per case,
reports medians and executed plans, and verifies exact Decimal totals across all
configurations. Measured locally on 2026-10-07: 100K and 1M rows, 12 cases, all
result comparisons PASS. These are warm synthetic joins on Spark local[2], not
end-to-end banking ingestion throughput. Evidence is runtime/spark-benchmark.json.
JDBC and a distributed multi-node cluster were not measured in that run. The
local Iceberg/Parquet experiment remains a separate small-data exercise.
