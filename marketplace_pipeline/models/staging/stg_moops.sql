{{ config(materialized='view') }}

select
    county_fips,
    county_name,
    plan_id,
    network_tier,
    type                           as moop_type,
    cast(amount as decimal(10, 2)) as amount,
    csr,
    family_cost,
    cast(individual as boolean)    as individual,
    cast(family as boolean)        as family
from {{ source('raw_data', 'moops') }}
