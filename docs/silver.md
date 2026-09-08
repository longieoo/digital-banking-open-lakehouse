# Spark conformance contract

`processing/silver.py` reads real Bronze Iceberg envelopes, parses explicit fields,
casts decimals/dates/UTC timestamps, normalizes currency, checks references and
routes invalid rows to persistent quarantine tables. MERGE retains the newest
valid source version by business key. Customer history retains all version keys.
Reversals reference an equal opposite original; they never erase the original.

All Bronze versions are currently scanned on each run, deliberately avoiding unsafe
incremental reads across updates/deletes. A late source update is selected by its
updated_at even if its business date is old. Invalid updates preserve the previous
valid Silver row and remain visible in quarantine. Remediation reruns the same job;
quarantine retains audit evidence and is not an unresolved-error count.

Window dedup and reference joins cause shuffles. Four shuffle partitions suit the
small demo; benchmark before changing. Transformations are lazy; count and MERGE
are actions. Small dimensions may broadcast. Partition business_date for transaction
pruning; do not partition by high-cardinality account IDs. AQE may change plans.

Run `docker compose --profile processing run --rm spark` after all Bronze tables
exist. Run integration tests with `RUN_SPARK=1`; absent Java/Docker is UNAVAILABLE,
not successful Spark validation. Silver owns conformance, dbt owns signed financial
measures, SCD2, balances and all Gold marts.
