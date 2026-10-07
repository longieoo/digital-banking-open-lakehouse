# ADR 009: Snapshot-pinned releases and certified exports

Date: 2026-10-07. Status: accepted.

## Problem

Independent source reads, latest-table processing and fixed dbt dates did not define
one consistent reporting run. A failed dbt build could leave some candidate tables
updated. Historical backfill only covered transactions and was memory-resident.

## Decision

- Read all source pages/tables and derive control totals in one PostgreSQL
  REPEATABLE READ, READ ONLY transaction. Store its snapshot description and capture
  timestamp. Do not confuse the timestamp with a sufficient commit-order watermark.
- Replay the complete zero-opening ledger window and dependent facts in keyset pages;
  load dimensions together. Keep forward watermarks unchanged during this replay.
  A shared reentrant local writer lock prevents cooperating Bronze stream writers
  from changing the captured snapshot set during preparation.
- Store the Bronze snapshot IDs and release/business parameters before processing.
  Materialize isolated Bronze/Silver/Gold schemas for each release. Check the latest
  version before accepting a row; current invalid data blocks publication.
- Treat source_totals as a complete control epoch, so a disappeared currency/day
  group does not survive after a correction.
- Require every dbt model/test, matching parameters/routing, successful Silver and
  nonempty passing reconciliation before writing certified Gold CSV exports. Keep
  Decimal values during HTTP extraction. Hash every exported file.
- Atomically publish one small current.json pointer under the local writer lock.
  BI resolves it once and reads only that release's exports. Historical runs do not
  move the current business date backwards. Interrupted exports keep the last good
  pointer. Repeating a certified run verifies its files and repairs publication.

## Consequences and boundaries

This is atomic publication of a local file bundle, not a distributed transaction
across Iceberg tables. Candidate SQL schemas remain transformer-only under existing
Trino rules. The legacy Gold schema remains a manual compatibility/demo path.
Source snapshots survive only while their read transaction is open: a failed prepare
re-reads the source; once SOURCE_READY is durable, retry uses stored Iceberg inputs.

The canonical release is correctness-first full conformance/build, with bounded
ingestion memory. It duplicates candidate data and requires a retention policy.
No general CDC/delete handling, multi-host SQLite lock, incremental Gold speedup,
global exactly-once guarantee or authenticated shared deployment is claimed.
Keep all required snapshots until their releases finish; do not run broad orphan
cleanup against active jobs. A future incremental implementation must match this
full-rebuild reference for late postings and historical corrections.
