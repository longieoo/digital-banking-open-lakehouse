-- Grain: one branch.
select branch_id, branch_name, region, city from {{ source('silver', 'branches') }}
