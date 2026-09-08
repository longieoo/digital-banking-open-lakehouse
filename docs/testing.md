# Validation boundaries and commands

```sh
python -m pip install -r requirements-test.txt
ruff check .
python -m compileall -q bank processing streaming orchestration tests scripts
python -m unittest discover -s tests -v
```

Unit/contract coverage includes determinism, references, GL balance, timestamp ties,
immutable versions, duplicate replay, raw corruption, source/raw/commit failures,
post-commit crashes, backfills and sanitized errors. Real local Iceberg tests use
the actual Iceberg format/Parquet files with an isolated persistent SQL catalog.
They verify snapshots, history, schema change and reopen; they do not verify REST.

Gold and risk SQL tests execute the actual SQL files/queries with DuckDB, translating
Trino syntax where necessary. Typed fixture Silver is deliberately substituted.
They establish business behavior, not distributed interoperability. `dbt parse`
checks adapter/project parsing without contacting Trino. `dbt build` is still required.

`python scripts/e2e.py` is the real Docker acceptance gate against the disposable
demo, including source → Bronze → Spark Silver → dbt Gold, recovery, reconciliation
and storage/catalog restart. This seeds SYN records: use the dedicated Compose
project, not an unrelated database. It leaves services running for inspection;
`docker compose --profile core --profile lakehouse --profile query down` stops them
without removing volumes. Docker gate was UNAVAILABLE in authoring.

Spark integration: run `tests/integration/test_spark.py` with RUN_SPARK=1 through
spark-submit or in a configured Java/Spark environment. PostgreSQL/MinIO/REST:
RUN_STACK=1 inside the bank container. CI lightweight jobs avoid live Azure; the
optional full-stack workflow needs a sufficiently sized Docker runner.

Reconciliation FAIL is a visible test failure. Quarantine is retained evidence,
not silently discarded records. Freshness is relative to synthetic January 2026
data: compare against an explicit demo date, never pretend historical fixtures are
live. No successful remote validation is inferred from passing local substitutes.
