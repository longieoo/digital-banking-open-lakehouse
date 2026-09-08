select a.customer_id from {{ ref('dim_customer') }} a join {{ ref('dim_customer') }} b
on a.customer_id = b.customer_id and a.customer_sk <> b.customer_sk
and a.valid_from < coalesce(b.valid_to, timestamp '9999-12-31 00:00:00')
and b.valid_from < coalesce(a.valid_to, timestamp '9999-12-31 00:00:00')
