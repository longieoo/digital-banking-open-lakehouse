-- Grain: one transfer, not one transfer leg. Avoid doubling transferred value.
select t.transfer_id, t.source_account_id, t.destination_account_id,
    a.customer_id as source_customer_id, t.amount, t.currency, t.status, t.created_at,
    cast(t.created_at as date) as business_date
from {{ source('silver', 'transfers') }} t
join {{ ref('dim_account') }} a on t.source_account_id = a.account_id
