# Validation boundaries and production extensions

For version 0.4, see [source membership and operations](adr/012-source-membership-and-incremental-reporting.md). The items below describe the preceding 0.3 architecture and historical acceptance.

See [full operation](full-platform.md) and [completion evidence](validation/full-platform-2026-11-15.json).
Historical validation JSON files retain their original dates.

1. Actual local storage/catalog, Spark, Trino/dbt, recovery and release publication
   are tested on synthetic data. This is single-host acceptance, not production
   load/HA. The REST fixture serializes SQLite JDBC connections.
2. Certified releases capture source tables/controls in one PostgreSQL REPEATABLE
   READ view and pin Iceberg snapshots. Legacy manual paths are not certification
   evidence. No cross-table distributed transaction or exactly-once transport claim.
3. Forward loading requires monotonic updated_at and no general hard deletes.
   Explicit paged backfill handles documented corrections. Unobserved overwritten
   source versions cannot be reconstructed. General CDC/delete needs its own contract.
4. Silver/Gold fully rebuild; coordination and release schemas are single-host.
   Incremental recomputation, HA, compaction/expiration and large end-to-end benchmarks
   remain extensions. The lifecycle planner never deletes snapshots or raw history.
5. Streaming supports finite draining/checkpoint replay and retracts current alerts
   after corrections. Risk computation scopes affected accounts, but Silver still
   scans retained history. maxOffsetsPerTrigger bounds input, not retained state.
   Current alerts and their audit are recoverable commits, not one cross-table
   transaction. Certified Gold remains a separate release job.
6. Values stay native-currency. Live and fixture FX are separated; same-day provider
   restatements preserve monotonic locally observed versions. Those timestamps are
   not provider-authored revisions. Weekend reference dates can precede the request
   within the freshness bound. No live trading-rate claim.
7. Zero opening balances, unrestricted demo overdrafts, descriptive account status,
   simplified cash/deposit GL and current-state account/card dimensions are educational
   assumptions. Customer is SCD2. This is not regulatory accounting.
8. BI/metrics bind to localhost via Compose and support required Bearer authentication,
   rotating secret files, expiry and host/origin guards. Shared deployment still
   requires TLS, request limits and network controls. Trino HTTP usernames and the
   REST fixture do not provide a shared-network identity boundary.
9. Backup verifies certified files/reconciliation, not PostgreSQL, object versions,
   catalog/control databases or checkpoints. Hashes provide integrity, not signatures.
10. Cloud preflight/probe does not replace actual Azure deployment, ADLS role/ACL
    checks, catalog/compute authentication and a certified cloud release. Terraform
    validate is not deployment. See [Azure acceptance](azure-acceptance.md).
11. The browser dashboard is implemented; Power BI DAX/model guidance is retained,
    but native PBIX execution and visual browser QA remain separate gates. GitHub-hosted
    contracts and Terraform passed for v0.2; distributed CI requires a dedicated runner.
12. Third-party deprecation notices may appear. Versions remain pinned to the tested
    interoperable stack; this upgrade does not claim every dependency is newest.
