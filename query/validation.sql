SELECT count(*) AS bronze_versions FROM lake.bronze.transactions;
SELECT count(*) AS silver_events FROM lake.silver.transactions;
SELECT snapshot_id, committed_at, operation FROM lake.bronze."transactions$snapshots";
SELECT * FROM lake.bronze."transactions$files";
SELECT * FROM lake.bronze."transactions$manifests";
SELECT transaction_id, amount, currency, status FROM lake.silver.transactions LIMIT 10;
