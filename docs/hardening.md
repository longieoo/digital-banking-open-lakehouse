# Release safety and corrected observations

Version 0.3 addresses defects found while reviewing the operating boundaries of
the local platform. It retains the source contract and existing financial models.

## Concurrent builds and certification

Each release has a separate process-safe lock. Prepare, Spark Silver, dbt build,
certification and publication participate in the same lock order. A waiting runner
rechecks status and does not rebuild an already certified Gold schema. The dbt
container runs `bank.release build`, rather than an unguarded dbt command.

Exports are written to a staging directory, checked, then renamed into place on
the same filesystem. Failed candidate exports cannot overwrite a certified bundle.
Certification checks the actual dbt model inventory as well as run results; leaving
models out of both JSON artifacts is not accepted as a successful complete build.
Reader verification hashes the bytes used by the CSV parser and rejects malformed
rows, count/hash mismatch and a file that changes during verification.

Locks are local SQLite transactions with crash release and a bounded wait. They
coordinate cooperating processes on one host, not arbitrary distributed writers.
The SQL engines still do not share a multi-table transaction. BI's publication
boundary remains the immutable certified bundle and its current pointer.

## Streaming alert corrections

`silver.risk_alerts` now represents current rule results. When a transaction is
corrected, alerts that no longer hold are removed in the same Iceberg MERGE that
updates the affected current state. `silver.risk_alert_observations` retains ACTIVE
and RETRACTED observations with deterministic identities for retry deduplication.
Audit is committed before current state, allowing a retry to repair an interrupted
update. This ordering is recoverable, not a cross-table atomicity claim.

Velocity alerts are emitted only on successful outgoing transfers; repeated-failure
alerts are emitted only on failed transactions. A later deposit or successful event
does not inherit another event type's alert merely because the preceding window
contains enough qualifying events.

Normal risk evaluation considers affected old and new accounts, using retained
Bronze history to recover prior account ownership even after a partial retry.
First startup and rule-policy/threshold changes reconcile globally. Silver
conformance still scans retained history; this is not bounded streaming state.
After an independent batch refresh, explicitly reconcile current alerts:

```sh
docker compose run --rm spark streaming/consumer.py --reconcile-alerts
```

## Provider restatements of reference FX

The FX provider publishes a rate date, not a revision timestamp. The control store
now assigns a monotonic locally observed version when content changes. Business
date remains the provider's rate date. A repeated equivalent rate reuses its
observation identity; a correction and a later return to the old rate each create
a new observed version. Previous raw and Bronze observations are retained.

The observation ledger is persisted before ingestion. A retry after raw/Bronze
failure reuses the recorded identity. If the clock moves backward, the local
version increases by a microsecond; it is not represented as a provider revision.
Backfill does not advance the forward watermark. Existing pre-0.3 observations
remain readable, and the first new-policy fetch records a new observation.
This change still does not convert native-currency Gold facts automatically.

## Protected HTTP serving

Loopback-only anonymous serving remains available for a trusted local demo.
To protect BI and metrics, create a private token under ignored runtime:

```sh
python -m scripts.http_token
python -m scripts.platform up --internal --all --no-release --auth-token-file runtime/secrets/http-token
python -m scripts.serving_acceptance --token-file runtime/secrets/http-token
```

Use the token from that private file in the dashboard sign-in form. The browser
keeps it in memory only; reload/sign-out requires authentication again. The CLI
never prints the generated token or puts it in process arguments. The Compose
override passes it as a container environment secret, visible to trusted Docker
administrators just like local database credentials.

Direct deployments may instead set `BANK_HTTP_TOKEN_FILE` to a mounted secret
readable by the service UID. Changes to that file take effect on the next request;
an unreadable/missing file fails closed. An environment token requires a container
recreation for rotation. `--require-auth` refuses startup without valid credentials.
Optional `BANK_HTTP_TOKEN_EXPIRES_AT` is an aware UTC timestamp. Configure
`BANK_HTTP_ALLOWED_HOSTS` explicitly when using a domain or an internal scraper DNS
name; defaults permit localhost and loopback addresses only.

`/health` exposes process liveness only. BI `/ready`, `/api/report` and operational
`/ready`, `/status`, `/metrics` require Bearer authentication in protected mode.
Host, Origin and cross-site checks apply even in anonymous local mode. Tokens in
query parameters are not accepted, and access logs do not record credentials.
Shared deployments still require HTTPS and request limits at a trusted reverse
proxy. HTTP token authentication does not secure the Trino/catalog data plane.

## Acceptance boundaries

See the dated validation records for local tests, real Spark correction/replay,
concurrent certification and protected HTTP probes. General CDC/hard deletes,
distributed HA, incremental Gold, Azure deployment, native Power BI execution and
full infrastructure recovery remain separate engineering/deployment scopes.
