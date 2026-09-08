-- Grain: one contiguous business-attribute interval per customer, [valid_from, valid_to).
with history as (
    select customer_id, segment, branch_id, status, updated_at,
        lag(segment) over (partition by customer_id order by updated_at) as previous_segment,
        lag(branch_id) over (partition by customer_id order by updated_at) as previous_branch,
        lag(status) over (partition by customer_id order by updated_at) as previous_status
    from {{ source('silver', 'customer_history') }}
), changes as (
    select customer_id, segment, branch_id, status, updated_at as valid_from
    from history
    where previous_segment is null or segment <> previous_segment
        or branch_id <> previous_branch or status <> previous_status
), intervals as (
    select customer_id, segment, branch_id, status, valid_from,
        lead(valid_from) over (partition by customer_id order by valid_from) as valid_to
    from changes
)
select concat(customer_id, '|', cast(valid_from as varchar)) as customer_sk,
    customer_id, segment, branch_id, status, valid_from, valid_to,
    valid_to is null as is_current,
    concat('Customer ***', substr(customer_id, -4)) as masked_name
from intervals
