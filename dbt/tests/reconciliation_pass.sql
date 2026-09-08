select business_date, currency from {{ ref('mart_reconciliation_daily') }} where reconciliation_status <> 'PASS'
