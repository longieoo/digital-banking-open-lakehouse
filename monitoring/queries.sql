-- Run as transformer. Synthetic data uses an explicit freshness anchor.
SELECT max(updated_at) AS last_source_update,
       date_diff('hour', max(updated_at), timestamp '2026-02-02 00:00:00') AS demo_age_hours
FROM lake.silver.transactions;
SELECT reconciliation_status,count(*) AS day_currency_groups
FROM lake.gold.mart_reconciliation_daily GROUP BY reconciliation_status;
SELECT rule,count(*) AS alerts FROM lake.silver.risk_alerts GROUP BY rule;
SELECT count(*) AS retained_late_events FROM lake.silver.late_events;
-- Spark's query progress JSON exposes numInputRows, inputRowsPerSecond and offsets.
-- Compare Kafka log end offsets with checkpoint/query offsets to measure lag.
