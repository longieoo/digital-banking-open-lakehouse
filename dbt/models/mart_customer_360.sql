-- Grain: one customer, reporting_currency only for monetary measures; explicit as-of date.
with accounts as (
    select customer_id, count(*) as number_of_accounts,
        sum(case when status = 'ACTIVE' then 1 else 0 end) as active_accounts,
        count(distinct account_type) as account_product_types,
        min(opened_at) as first_account_opened_at
    from {{ ref('dim_account') }} group by customer_id
), cards as (
    select a.customer_id, count(*) as number_of_cards,
        sum(case when c.status = 'ACTIVE' then 1 else 0 end) as active_cards
    from {{ ref('dim_card') }} c join {{ ref('dim_account') }} a on c.account_id = a.account_id
    group by a.customer_id
), balances as (
    select customer_id, sum(closing_balance) as total_balance,
        sum(case when closing_balance < 0 then 1 else 0 end) as overdrawn_accounts
    from {{ ref('fct_account_balance_daily') }}
    where business_date = date '{{ var("as_of_date") }}' and currency = '{{ var("reporting_currency") }}'
    group by customer_id
), transactions as (
    select customer_id, count(*) as txn_count_30d,
        sum(case when status = 'SUCCESS' then amount else 0 end) as txn_amount_30d,
        sum(case when status = 'FAILED' then 1 else 0 end) as failed_txn_count_30d,
        sum(case when signed_amount > 0 then signed_amount else 0 end) as credits_30d,
        sum(case when signed_amount < 0 then -signed_amount else 0 end) as debits_30d,
        count(distinct channel) as channels_used_30d, max(event_time) as last_activity_at,
        sum(case when transaction_type like 'REVERSAL_%' then 1 else 0 end) as reversals_30d
    from {{ ref('fct_transactions') }}
    where business_date between date_add('day', -29, date '{{ var("as_of_date") }}')
        and date '{{ var("as_of_date") }}' and currency = '{{ var("reporting_currency") }}'
    group by customer_id
), card_spend as (
    select customer_id, sum(amount) as card_spend_30d
    from {{ ref('fct_card_transactions') }}
    where status = 'SUCCESS' and currency = '{{ var("reporting_currency") }}'
        and business_date between date_add('day', -29, date '{{ var("as_of_date") }}')
        and date '{{ var("as_of_date") }}' group by customer_id
), transfers as (
    select source_customer_id as customer_id, count(*) as transfer_count_30d
    from {{ ref('fct_transfers') }}
    where status = 'SUCCESS' and currency = '{{ var("reporting_currency") }}'
        and business_date between date_add('day', -29, date '{{ var("as_of_date") }}')
        and date '{{ var("as_of_date") }}' group by source_customer_id
)
select c.customer_id, c.masked_name, c.segment as customer_segment, c.status as customer_status,
    c.branch_id, date '{{ var("as_of_date") }}' as as_of_date,
    '{{ var("reporting_currency") }}' as reporting_currency,
    coalesce(a.number_of_accounts,0) as number_of_accounts, coalesce(a.active_accounts,0) as active_accounts,
    coalesce(a.account_product_types,0) as account_product_types, a.first_account_opened_at,
    coalesce(k.number_of_cards,0) as number_of_cards, coalesce(k.active_cards,0) as active_cards,
    coalesce(b.total_balance,0) as total_balance, coalesce(b.overdrawn_accounts,0) as overdrawn_accounts,
    coalesce(t.txn_count_30d,0) as txn_count_30d, coalesce(t.txn_amount_30d,0) as txn_amount_30d,
    coalesce(t.failed_txn_count_30d,0) as failed_txn_count_30d,
    coalesce(1.0 * t.failed_txn_count_30d / nullif(t.txn_count_30d,0),0) as failed_txn_ratio,
    coalesce(t.credits_30d,0) as credits_30d, coalesce(t.debits_30d,0) as debits_30d,
    coalesce(t.channels_used_30d,0) as channels_used_30d, t.last_activity_at,
    coalesce(t.reversals_30d,0) as reversals_30d,
    coalesce(s.card_spend_30d,0) as card_spend_30d, coalesce(x.transfer_count_30d,0) as transfer_count_30d
from {{ ref('dim_customer') }} c
left join accounts a on c.customer_id = a.customer_id
left join cards k on c.customer_id = k.customer_id
left join balances b on c.customer_id = b.customer_id
left join transactions t on c.customer_id = t.customer_id
left join card_spend s on c.customer_id = s.customer_id
left join transfers x on c.customer_id = x.customer_id
where c.valid_from < cast(date_add('day', 1, date '{{ var("as_of_date") }}') as timestamp)
  and (c.valid_to is null or c.valid_to >= cast(date_add('day', 1, date '{{ var("as_of_date") }}') as timestamp))
