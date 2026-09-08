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

Status: Spark/JDBC benchmarks UNAVAILABLE in the authoring environment (no Java or
Docker). No synthetic timing numbers are supplied. The local Iceberg/Parquet
experiment is a separate, measured small-data exercise.
