{{ config(materialized='table') }}

-- One row per county × plan (plan_key is the PK).
-- stg_issuer and stg_rating are plan-level (not county-level), so they join
-- on plan_id alone and their data is replicated across the county rows.
select
    p.plan_key,
    p.county_fips,
    p.county_name,
    p.plan_id,
    p.plan_name,
    p.premium,
    p.metal_level,
    p.plan_type,
    p.design_type,
    p.is_standardized_plan,
    p.hsa_eligible,
    p.has_national_network,
    p.specialist_referral_required,
    p.service_area_id,
    p.state,
    p.disease_mgmt_programs,
    i.issuer_id,
    i.carrier_name,
    r.rating_available,
    r.global_rating,
    r.clinical_quality_mgmt_rating,
    r.enrollee_experience_rating,
    r.plan_efficiency_rating,
    r.global_not_rated_reason
from {{ ref('stg_plans') }}   p
left join {{ ref('stg_issuer') }} i on p.plan_id = i.plan_id
left join {{ ref('stg_rating') }} r on p.plan_id = r.plan_id
