{{ config(materialized='table') }}

-- One row per insurance carrier. Aggregates from plan-level stg_issuer.
select
    issuer_id,
    min(carrier_name)    as carrier_name,
    min(state)           as state,
    min(individual_url)  as individual_url,
    min(shop_url)        as shop_url,
    min(toll_free_number) as toll_free_number,
    min(tty_number)      as tty_number,
    count(*)             as plan_count
from {{ ref('stg_issuer') }}
group by issuer_id
