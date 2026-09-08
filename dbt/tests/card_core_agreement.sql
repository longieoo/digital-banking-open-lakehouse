select c.card_transaction_id from {{ ref('fct_card_transactions') }} c
left join {{ ref('fct_transactions') }} t on c.transaction_id=t.transaction_id
where t.transaction_id is null or c.amount<>t.amount or c.currency<>t.currency
   or c.status<>t.status or c.account_id<>t.account_id
