# Known limitations and remaining acceptance gates

1. Docker/Java were absent: PostgreSQL constraints/startup, MinIO/REST persistence,
   Spark transformations, Trino materialization, Airflow and broker/checkpoint restart
   are implemented but not executed in authoring. Run scripts/e2e.py and service tests.
2. Local Iceberg uses a persistent SQL catalog test setup; deployment uses REST.
   Passing the local test does not validate the REST image, credentials or Docker networking.
3. Azure Terraform init/fmt/validate pass; no account, plan, apply, ADLS I/O or identity
   validation occurred. Cloud REST hosting/authentication is a manual prerequisite.
4. Full Silver and Gold refreshes favor correctness/explainability over scale.
   Large generator/backfill batches are memory-resident. Spark benchmark scripts
   exist but no distributed timing is claimed.
5. The SQLite control lock is single-host. All cooperating Bronze writers must use
   it. Direct external writers can violate unique business-version assumptions.
   No cross-layer atomicity, exactly-once transport or arbitrary-source CDC is claimed.
6. Forward ingestion requires monotonic updated_at and no hard deletes. Delayed
   commits behind the watermark need explicit backfill. Source historical versions
   never observed are not recoverable from a mutable transaction source.
7. Reconciliation needs a stable source cutoff. Control totals are refreshed before
   ingestion, not atomically with every later engine read. Freeze writes during audit.
8. USD fixture only; amounts stay native-currency. Zero opening balances, unrestricted
   overdrafts, descriptive account status and simplified cash/deposit GL are educational
   assumptions. Account/card dimensions are current-state, customer is SCD2.
9. Streaming recomputes retained history per batch; it accepts/audits late events
   instead of maintaining bounded event-time state. Alert observations are retained
   after corrections. Standalone streaming requires an explicit downstream dbt refresh.
10. FX live request returned HTTP 403. The rate file is explicitly invented test data.
11. Trino HTTP usernames are unauthenticated locally; role rules and masking are a
    demonstration, not a shared-network security perimeter. Catalog fixture is local-only.
12. No real Power BI artifact/screenshots, GitHub CI run, Datadog deployment or paid
    service was fabricated. No production banking or large-scale performance claim.
