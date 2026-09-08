-- Grain: date/branch/currency, card events only.
select t.business_date, a.branch_id, t.currency, count(*) as card_event_count,
    sum(case when t.status = 'SUCCESS' then t.amount else 0 end) as successful_card_spend,
    sum(case when t.status = 'FAILED' then 1 else 0 end) as failed_card_count
from {{ ref('fct_card_transactions') }} t join {{ ref('dim_account') }} a on t.account_id = a.account_id
group by t.business_date, a.branch_id, t.currency
