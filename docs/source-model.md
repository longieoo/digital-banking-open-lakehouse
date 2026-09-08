# Source semantics

Three PostgreSQL schemas isolate operational domains: core, card and gl. Numeric
money has scale 2; JSON uses strings. Identifiers start SYN; email uses .invalid;
no PAN, PIN, CVV or valid-looking phone is generated. All timestamps are UTC.

The seeded demo is USD only. Each transfer has two account postings, each card
purchase one core posting, each successful posting one journal with equal debit
and credit. Failed events have no journal. Reversals are separate opposite signed
postings; the original remains counted. Customers have explicit source history so
SCD2 can be reconstructed even on the first extract. Accounts start at zero;
overdraft is allowed for this simulation, and account status is descriptive.

Profiles small/medium/large default to 200/100K/1M base transactions. Customer and
transaction counts are overridable. Extra transfers and reversals increase output
size. Generator memory is O(n); large runs need adequate RAM.

PostgreSQL enforces keys, FKs, positive amounts, enums and transfer inequality.
Cross-row GL balancing is tested and reconciled downstream; it is not a SQL CHECK.
Controlled duplicates, negative amounts and late updates are separate fixtures.

PostgreSQL triggers advance transaction/customer updated_at on changed rows and
capture customer inserts/updates into source history. These triggers are included
in the real source schema; their execution remains part of the Docker gate.
