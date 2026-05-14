{{ config(materialized='view') }}

select
    county_fips,
    county_name,
    plan_id,
    network_tier,
    type                           as deductible_type,
    cast(amount as decimal(10, 2)) as amount,
    family_cost
from {{ source('raw_data', 'deductibles') }}
