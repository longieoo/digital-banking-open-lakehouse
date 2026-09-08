# Financial reconciliation

`mart_reconciliation_daily` has one row per business date and currency. Technical
checks compare a separately extracted operational control total with latest Bronze
event count and Gold event count. Financial checks compare successful gross source
amount with Gold. Accounting checks match each successful core posting to exactly
one journal with equal debit/credit/amount, correct date/currency, and correct signed
movement in the deposit-control GL account. Failed core events must not have GL.

Gross core posting value counts both transfer legs; the transfer fact counts once.
Reversals are additional opposite postings; their gross value is included and their
net effect offsets originals. This is intentional, not double-counted balance.

Missing control dates, missing records, extra journals, duplicate Bronze versions,
unbalanced journals and balanced-but-misallocated GL cause FAIL. The dbt test returns
all FAIL rows so a DAG cannot quietly report success. Snapshot isolation across
all engines is not provided: pause source mutation for an audit or align extraction
cutoffs. The seed's source_totals must be refreshed after operational changes before
claiming a fresh reconciliation. The metadata runner performs that refresh.

Run `python -m unittest tests.test_reconciliation -v` for actual model SQL tests
using a clearly labeled DuckDB dialect translation. Real Trino acceptance remains
a separate gate. Test fixtures never alter the user's running operational database.
