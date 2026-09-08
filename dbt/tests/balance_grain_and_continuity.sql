with balances as (
    select account_id,business_date,opening_balance,
        lag(closing_balance) over (partition by account_id order by business_date) as previous_close,
        count(*) over (partition by account_id,business_date) as grain_count
    from {{ ref('fct_account_balance_daily') }}
)
select account_id,business_date from balances
where grain_count <> 1 or opening_balance <> coalesce(previous_close,0)
