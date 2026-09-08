-- Grain: one account per calendar business date, including dates with no activity.
with daily as (
    select account_id, business_date,
        sum(case when signed_amount > 0 then signed_amount else 0 end) as credit_amount,
        sum(case when signed_amount < 0 then -signed_amount else 0 end) as debit_amount
    from {{ ref('fct_transactions') }} group by account_id, business_date
), scaffold as (
    select a.account_id, a.customer_id, a.currency, d.business_date,
        coalesce(t.credit_amount, 0) as credit_amount, coalesce(t.debit_amount, 0) as debit_amount
    from {{ ref('dim_account') }} a cross join {{ ref('dim_date') }} d
    left join daily t on a.account_id = t.account_id and d.business_date = t.business_date
    where d.business_date >= cast(a.opened_at as date)
      and (a.closed_at is null or d.business_date <= cast(a.closed_at as date))
)
select account_id, customer_id, currency, business_date,
    coalesce(sum(credit_amount - debit_amount) over (partition by account_id order by business_date
        rows between unbounded preceding and 1 preceding), 0) as opening_balance,
    credit_amount, debit_amount,
    sum(credit_amount - debit_amount) over (partition by account_id order by business_date
        rows between unbounded preceding and current row) as closing_balance
from scaffold
