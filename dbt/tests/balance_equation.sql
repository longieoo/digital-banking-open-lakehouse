select account_id, business_date from {{ ref('fct_account_balance_daily') }}
where opening_balance + credit_amount - debit_amount <> closing_balance
