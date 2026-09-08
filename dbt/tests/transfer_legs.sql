select x.transfer_id from {{ ref('fct_transfers') }} x
left join {{ ref('fct_transactions') }} t on x.transfer_id = t.transfer_id and t.status = 'SUCCESS'
where x.status = 'SUCCESS' group by x.transfer_id, x.amount
having count(t.transaction_id) <> 2 or sum(t.signed_amount) <> 0
    or sum(abs(t.signed_amount)) <> 2 * x.amount
