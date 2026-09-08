-- Grain: one calendar date. start_date must cover the complete zero-opening ledger.
select business_date, year(business_date) as calendar_year,
    month(business_date) as calendar_month, day(business_date) as day_of_month,
    date_trunc('month', business_date) as month_start
from unnest(sequence(date '{{ var("start_date") }}', date '{{ var("as_of_date") }}', interval '1' day))
    as dates(business_date)
