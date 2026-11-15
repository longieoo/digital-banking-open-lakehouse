# Validation boundaries and production extensions

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
5. Streaming supports finite draining/checkpoint replay but recomputes retained
   history. Alerts do not retract automatically. maxOffsetsPerTrigger bounds input,
   not retained state. Certified Gold still requires a separate release job.
6. Values stay native-currency. Live and fixture FX are separated; same-day provider
   restatements yield explicit version conflicts. Weekend reference dates can precede
   the requested date within the freshness bound. No live trading-rate claim.
7. Zero opening balances, unrestricted demo overdrafts, descriptive account status,
   simplified cash/deposit GL and current-state account/card dimensions are educational
   assumptions. Customer is SCD2. This is not regulatory accounting.
8. BI/metrics bind to localhost via Compose. Shared deployment requires authentication,
   TLS and network controls. Trino HTTP usernames and the REST fixture do not provide
   a shared-network security boundary.
9. Backup verifies certified files/reconciliation, not PostgreSQL, object versions,
   catalog/control databases or checkpoints. Hashes provide integrity, not signatures.
10. Cloud preflight/probe does not replace actual Azure deployment, ADLS role/ACL
    checks, catalog/compute authentication and a certified cloud release. Terraform
    validate is not deployment. See [Azure acceptance](azure-acceptance.md).
11. The browser dashboard is implemented; Power BI DAX/model guidance is retained,
    but no executed native PBIX is fabricated. Remote GitHub CI needs separate evidence.
12. Third-party deprecation notices may appear. Versions remain pinned to the tested
    interoperable stack; this upgrade does not claim every dependency is newest.
