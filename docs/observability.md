# Operational signals

`python -m bank.observability` prints sanitized JSON totals from durable manifests:
completed/failed/started batches, extracted/written rows, retries, elapsed time and
count mismatches. Counts are operational attempt totals, not unique financial-event
counts. A manifest reused on retry retains first start time, so duration includes
retry delays. Use Iceberg/Gold to count unique events.

Inspect STARTED manifests after a crash; retry the matching source request. Any
FAILED manifest needs the corresponding sanitized error type and local secure logs.
Never dump environment variables or raw payloads into CI logs. Quarantine tables
are retained evidence, so old rejects can remain after correction. Count new rejects
by batch_id when assessing a fresh run.

`monitoring/queries.sql` covers freshness, reconciliation, alert counts and lateness.
Synthetic freshness is explicitly anchored to the fixture period; wall-clock checks
would correctly report it stale. Streaming progress exposes offsets and rates; Kafka
lag needs comparison with log-end offsets, not event-time age alone. Alert on a
failed reconciliation, repeated batch failure or stalled checkpoint. Datadog is
deliberately omitted; no paid monitoring dependency is needed.

GitHub Actions checks lint, contracts, local Iceberg, SQL, dbt parsing, Compose
configuration and Terraform validation. It was authored locally, not run on GitHub.
Manual distributed acceptance is restricted to a dedicated disposable Docker runner.
