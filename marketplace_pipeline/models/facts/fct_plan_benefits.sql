{{ config(materialized='table') }}

select
    md5(county_fips || '|' || plan_id || '|' || benefit_type || '|' || network_tier) as benefit_key,
    county_fips,
    county_name,
    plan_id,
    benefit_type,
    benefit_name,
    network_tier,
    covered,
    copay,
    coinsurance_rate,
    is_mental_health_benefit
from {{ ref('stg_benefits') }}
