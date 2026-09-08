-- Grain: one card purchase event; do not add its amount again to core balances.
select t.card_transaction_id, t.card_id, c.account_id, a.customer_id,
    t.transaction_id, t.amount, t.currency, t.status, t.merchant, t.event_time,
    cast(t.event_time as date) as business_date
from {{ source('silver', 'card_transactions') }} t
join {{ ref('dim_card') }} c on t.card_id = c.card_id
join {{ ref('dim_account') }} a on c.account_id = a.account_id
