-- Recompute every affected business day, including the old day of a moved or deleted posting.
{% set baseline = var('baseline_schema', none) %}
{% if baseline %}
with added as (
    select * from {{ ref('fct_transactions') }}
    except select * from lake.{{ baseline }}.fct_transactions
), removed as (
    select * from lake.{{ baseline }}.fct_transactions
    except select * from {{ ref('fct_transactions') }}
), changed_days as (
    select business_date from added union select business_date from removed
)
select * from lake.{{ baseline }}.mart_transaction_daily
where business_date not in (select business_date from changed_days)
union all
{% endif %}
-- Grain: day/currency/status/channel/type/account branch/historical customer segment.
select business_date, currency, status, channel, transaction_type, account_branch_id, segment,
    count(*) as transaction_count, sum(amount) as transaction_amount,
    sum(signed_amount) as net_posted_amount
from {{ ref('fct_transactions') }}
{% if baseline %}
where business_date in (select business_date from changed_days)
{% endif %}
group by business_date, currency, status, channel, transaction_type, account_branch_id, segment
