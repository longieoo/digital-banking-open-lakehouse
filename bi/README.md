# Power BI implementation specification

No PBIX was generated. Power BI Desktop/driver/GUI validation was UNAVAILABLE.
Install a supported Trino/Starburst ODBC driver on the BI workstation, configure a
DSN to localhost:8080 (or the authenticated remote endpoint), and connect Power BI
through Get Data → ODBC. Use analyst identity and import the Gold tables. Driver
availability/licensing depends on the distribution; confirm it locally. CSV export
of query results is a fallback for a static portfolio demo.

## Semantic model

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
