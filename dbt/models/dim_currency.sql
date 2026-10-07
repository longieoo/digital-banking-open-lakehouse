-- Grain: one native account currency. Monetary measures require a single selection.
select distinct currency from {{ ref('dim_account') }}
