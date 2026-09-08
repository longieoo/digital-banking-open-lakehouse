-- Grain: one current account; initial balance is zero by source contract.
select account_id, customer_id, branch_id, account_type, currency, status, opened_at, closed_at,
    concat('***', substr(account_id, -4)) as masked_account
from {{ source('silver', 'accounts') }}
