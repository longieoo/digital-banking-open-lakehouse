# Certified BI dashboard and Power BI specification

## Runnable local dashboard

The built-in dashboard is a read-only web application for the actual certified
CSV bundle. It does not require Power BI or any third-party frontend CDN.

```sh
python -m bank.serving --root runtime/releases --port 8090
```

Open http://127.0.0.1:8090 after a release has been certified. The four pages cover
overview, Customer 360, transactions and financial reconciliation. Currency and
transaction date filters use only Gold exports. A release ID, business date and
SHA-256 verification status are visible. API `GET /api/report` returns the same
source-backed values; optional query keys are `currency`, `start`, `end`.
`GET /health` returns 200 only when the current certified bundle verifies.
Missing or corrupt certified data returns 503, with no fallback to fixtures.

Each request resolves current.json once, verifies every manifest export, then
hashes the exact bytes parsed for serving. Thus a pointer change cannot mix two
releases. Monetary JSON values are Decimal strings; display formatting avoids
conversion to JavaScript floating-point numbers. Each monetary result has one
native currency, without implicit FX conversion. Closing balance uses the last
available date at or before the selected end, never a sum across dates. Customer
360 stays at the certified as-of date and its fixed 30-day window, independently
of the transaction range. The recent-transaction table is limited to 100 rows;
summary totals include all selected events. Counts and chart heights are numeric.

The server binds loopback by default and exposes only three static assets and
two read APIs. It cannot browse the filesystem or serve arbitrary export paths.
Use the Docker Compose dashboard profile for the packaged service. This local
HTTP server has no user authentication and is not a public internet endpoint.
Shared deployments need an authenticated TLS reverse proxy and restricted storage
access. The entire dashboard dataset is synthetic and Gold excludes raw PII.

## Power BI Desktop boundary

No PBIX was generated. Power BI Desktop/driver/GUI validation was UNAVAILABLE.
Install a supported Trino/Starburst ODBC driver on the BI workstation, configure a
DSN to localhost:8080 (or the authenticated remote endpoint), and connect Power BI
through Get Data → ODBC. Use analyst identity and import the Gold tables. Driver
availability/licensing depends on the distribution; confirm it locally. CSV export
of query results is a fallback for a static portfolio demo.

## Semantic model

The certified local serving option is the CSV bundle selected once by
runtime/releases/current.json; see [release runbook](../docs/releases.md). Load all
tables from the same release and display its business date. Avoid directly reading
an in-progress candidate schema. Hash verification supports an exported-data restore.

Add dim_currency 1:* to currency on each monetary fact, using single-direction
filters. DAX monetary measures now return BLANK without exactly one currency.
Transaction Value means requested value across statuses; Posted Transaction Value
includes SUCCESS only; Net Posted Movement is signed. These DAX changes are source
specification changes, not a claim of validated Power BI Desktop execution.

Use dim_date 1:* business_date to each fact; dim_account 1:* account_id to transaction
and balance facts; dim_card 1:* card_id to card facts; dim_branch 1:* branch_id on
dim_account; dim_customer 1:* customer_sk on fct_transactions for historical segment.
Use a separate current-customer view from mart_customer_360 for the customer page;
do not join customer_id directly to SCD2 facts through a many-to-many relationship.
Keep cross-filter direction single. Transfer source/destination need role-playing
account dimensions or inactive relationships with USERELATIONSHIP. Reconciliation
has day/currency grain and relates to date, not individual transaction rows.

Load `measures.dax` as individual measures. Require a single currency selection for
every monetary card/visual. Customer 360 uses USD by default; native balance facts
must not sum USD/EUR/VND together. Balances are semi-additive: use the last visible
date, never sum balances across dates. Gross transaction value counts core posting
legs; use fct_transfers for transferred value counted once.

## Four-page wireframe

1. **Executive Overview:** date/currency filters; customers, accounts, closing
   balance, transaction count/value and success rate; daily volume line and channel
   distribution; visible synthetic-data and refresh-date labels.
2. **Customer 360:** customer selector and masked name; segment/branch, accounts,
   cards, USD balance, 30-day volume/card spend/failure ratio; recent activity table.
3. **Transaction Analytics:** daily/monthly trend, channel/type breakdown, branch
   matrix, historical segment, failures. Keep amount and net posted value distinct.
4. **Financial Reconciliation:** PASS/FAIL status cards; source/Bronze/Gold counts;
   source/Gold amounts; GL debit/credit and unmatched count by day/currency. Red
   for FAIL, neutral green for PASS, explicit unavailable/no-data state.

Acceptance: verify filters against Trino SQL, compare final-date balance to signed
postings, test a controlled mismatch, inspect relationship ambiguity, and confirm
no raw PII columns. Capture screenshots only from an actual working dashboard.
