-- Grain: one latest valid core transaction event. Reversals are separate events.
select t.transaction_id, t.account_id, a.customer_id, c.customer_sk,
    a.branch_id as account_branch_id, c.branch_id as customer_branch_id, c.segment,
    t.transaction_type, t.currency, t.status, t.channel, t.amount,
    case when t.status = 'SUCCESS' then t.amount * ty.direction else cast(0 as decimal(22,2)) end
        as signed_amount,
    t.business_date, t.event_time, t.updated_at, t.original_transaction_id, t.transfer_id
from {{ source('silver', 'transactions') }} t
join {{ ref('dim_account') }} a on t.account_id = a.account_id
join {{ ref('dim_transaction_type') }} ty on t.transaction_type = ty.transaction_type
left join {{ ref('dim_customer') }} c on a.customer_id = c.customer_id
    and t.event_time >= c.valid_from and (t.event_time < c.valid_to or c.valid_to is null)
