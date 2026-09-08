select t.transaction_id from {{ ref('fct_transactions') }} t
left join {{ ref('fct_account_balance_daily') }} b
on t.account_id=b.account_id and t.business_date=b.business_date
where t.status='SUCCESS' and t.business_date <= date '{{ var("as_of_date") }}' and b.account_id is null
