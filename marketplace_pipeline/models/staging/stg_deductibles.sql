{{ config(materialized='view') }}

select
    plan_id,
    network_tier,
    type        as deductible_type,
    cast(amount as decimal(10, 2)) as amount,
    family_cost
from {{ source('raw_data', 'deductibles') }}
