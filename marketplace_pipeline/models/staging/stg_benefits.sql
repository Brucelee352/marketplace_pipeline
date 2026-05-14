{{ config(materialized='view') }}

-- DISTINCT absorbs exact-duplicate rows caused by the page-accumulation bug
-- (benefits loop was inside while True, so page-1 plans were reprocessed N times).
-- After the main.py fix this is a no-op.
select distinct
    county_fips,
    county_name,
    plan_id,
    benefit_type,
    benefit_name,
    network_tier,
    cast(covered as boolean)                as covered,
    cast(copay as decimal(10, 2))           as copay,
    cast(coinsurance_rate as decimal(5, 4)) as coinsurance_rate,
    benefit_type in (
        'MENTAL_BEHAVIORAL_HEALTH_INPATIENT_SERVICES',
        'MENTAL_BEHAVIORAL_HEALTH_OUTPATIENT_SERVICES'
    )                                       as is_mental_health_benefit
from {{ source('raw_data', 'benefits') }}
