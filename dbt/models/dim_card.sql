-- Grain: one synthetic card identifier; no payment credentials exist.
select card_id, account_id, card_type, status, created_at
from {{ source('silver', 'cards') }}
