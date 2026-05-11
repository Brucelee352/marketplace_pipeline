{{ config(materialized='view') }}

-- One row per plan_id × issuer_id. GROUP BY absorbs duplicate rows
-- caused by the extraction bug (ratings/issuers were appended inside
-- the moops loop). After the main.py fix, this just passes through cleanly.
select
    plan_id,
    issuer_id,
    min(name)           as carrier_name,
    min(state)          as state,
    min(individual_url) as individual_url,
    min(shop_url)       as shop_url,
    min(toll_free)      as toll_free_number,
    min(tty_number)     as tty_number
from {{ source('raw_data', 'issuer') }}
where issuer_id is not null
group by plan_id, issuer_id
