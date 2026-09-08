-- Grain: day/currency/status/channel/type/account branch/historical customer segment.
select business_date, currency, status, channel, transaction_type, account_branch_id, segment,
    count(*) as transaction_count, sum(amount) as transaction_amount,
    sum(signed_amount) as net_posted_amount
from {{ ref('fct_transactions') }}
group by business_date, currency, status, channel, transaction_type, account_branch_id, segment
