-- Grain: business date / currency. PASS requires technical, financial and per-posting accounting agreement.
with bronze_ranked as (
    select record_id, business_date, payload,
        row_number() over (partition by record_id order by source_updated_at desc) as version_rank
    from {{ source('bronze', 'transactions') }}
), bronze as (
    select cast(business_date as date) as business_date,
        json_extract_scalar(payload, '$.currency') as currency, count(*) as lakehouse_count
    from bronze_ranked where version_rank = 1 group by business_date, json_extract_scalar(payload, '$.currency')
), duplicate_versions as (
    select cast(business_date as date) as business_date, json_extract_scalar(payload, '$.currency') as currency,
        count(*) - count(distinct concat(record_id, '|', source_updated_at)) as duplicate_versions
    from {{ source('bronze', 'transactions') }}
    group by business_date, json_extract_scalar(payload, '$.currency')
), gold as (
    select business_date, currency, count(*) as gold_count,
        sum(case when status = 'SUCCESS' then amount else 0 end) as gold_amount,
        count(*) - count(distinct transaction_id) as duplicate_transactions
    from {{ ref('fct_transactions') }} group by business_date, currency
), journals as (
    select e.transaction_id, e.business_date, e.currency,
        sum(l.debit) as debit, sum(l.credit) as credit,
        sum(case when l.gl_account_id = 'SYN-GL-DEPOSIT' then l.credit - l.debit else 0 end) as deposit_movement,
        count(distinct e.entry_id) as entry_count
    from {{ source('silver', 'journal_entries') }} e
    left join {{ source('silver', 'journal_lines') }} l on e.entry_id = l.entry_id
    group by e.transaction_id, e.business_date, e.currency
), posting_check as (
    select coalesce(t.business_date, j.business_date) as business_date,
        coalesce(t.currency, j.currency) as currency, coalesce(j.debit, 0) as gl_debit,
        coalesce(j.credit, 0) as gl_credit,
        case when t.transaction_id is null or j.transaction_id is null or j.entry_count <> 1
            or t.business_date <> j.business_date or t.currency <> j.currency
            or coalesce(j.debit, 0) <> t.amount or coalesce(j.credit, 0) <> t.amount
            or coalesce(j.deposit_movement, 0) <> t.signed_amount then 1 else 0 end as unmatched
    from (select transaction_id, business_date, currency, amount, signed_amount
          from {{ ref('fct_transactions') }} where status = 'SUCCESS') t
    full outer join journals j on t.transaction_id = j.transaction_id
), accounting as (
    select business_date, currency, sum(gl_debit) as gl_debit, sum(gl_credit) as gl_credit,
        sum(unmatched) as unmatched_transactions
    from posting_check group by business_date, currency
), dates as (
    select business_date, currency from {{ source('silver', 'source_totals') }}
    union select business_date, currency from bronze
    union select business_date, currency from gold
    union select business_date, currency from accounting
), compared as (
    select d.business_date, d.currency,
        coalesce(s.source_count,0) as source_count, coalesce(b.lakehouse_count,0) as lakehouse_count,
        coalesce(g.gold_count,0) as gold_count, coalesce(s.source_amount,0) as source_amount,
        coalesce(g.gold_amount,0) as gold_amount, coalesce(a.gl_debit,0) as gl_debit,
        coalesce(a.gl_credit,0) as gl_credit, coalesce(a.unmatched_transactions,0) as unmatched_transactions,
        coalesce(g.duplicate_transactions,0) + coalesce(v.duplicate_versions,0) as duplicate_transactions,
        s.total_id is null as missing_source_control
    from dates d
    left join {{ source('silver', 'source_totals') }} s on d.business_date = s.business_date and d.currency = s.currency
    left join bronze b on d.business_date = b.business_date and d.currency = b.currency
    left join gold g on d.business_date = g.business_date and d.currency = g.currency
    left join accounting a on d.business_date = a.business_date and d.currency = a.currency
    left join duplicate_versions v on d.business_date = v.business_date and d.currency = v.currency
)
select business_date, currency, source_count, lakehouse_count, gold_count, source_amount, gold_amount,
    gl_debit, gl_credit, unmatched_transactions, duplicate_transactions, missing_source_control,
    case when source_count = lakehouse_count and source_count = gold_count and source_amount = gold_amount
        and gl_debit = gl_credit and gl_debit = gold_amount and unmatched_transactions = 0
        and duplicate_transactions = 0 and not missing_source_control then 'PASS' else 'FAIL' end
        as reconciliation_status
from compared
