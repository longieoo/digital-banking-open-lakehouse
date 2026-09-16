# Known limitations and remaining acceptance gates

1. Local Docker validation now covers PostgreSQL/MinIO/REST, Spark Silver, Trino/dbt,
   Airflow and broker/checkpoint restart (2026-09-16/17). This is a small single-host
   demonstration, not production or scale acceptance. The SQLite-backed REST fixture
   uses one JDBC client to avoid metadata write locks after restart.
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

Fresh-clone Terraform init previously encountered a local provider-checksum
file-access error. A subsequent Linux-container run of Terraform 1.11.4 passed
fmt, init and validate with AzureRM 4.26.0. Historical validation/clean-clone.json
retains its original result. No Azure resources were deployed.
