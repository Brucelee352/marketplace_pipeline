{{ config(materialized='table') }}

select
    md5(b.county_fips || '|' || b.plan_id || '|' || b.benefit_type || '|' || b.network_tier) as benefit_key,
    b.county_fips,
    b.county_name,
    b.plan_id,
    p.plan_name,
    p.metal_level,
    b.benefit_type,
    b.benefit_name,
    b.network_tier,
    b.covered,
    b.copay,
    b.coinsurance_rate,
    b.is_mental_health_benefit
from {{ ref('stg_benefits') }} b
left join {{ ref('stg_plans') }} p
    on  b.county_fips = p.county_fips
    and b.plan_id     = p.plan_id
