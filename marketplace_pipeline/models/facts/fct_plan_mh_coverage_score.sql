{{ config(materialized='table') }}

with mh_benefits as (
    select
        county_fips,
        plan_id,
        count(distinct case when covered then benefit_type end) as mh_benefits_covered,
        avg(case
            when benefit_type = 'MENTAL_BEHAVIORAL_HEALTH_OUTPATIENT_SERVICES'
             and network_tier = 'In-Network'
            then copay
        end) as avg_mh_copay
    from {{ ref('stg_benefits') }}
    where is_mental_health_benefit
    group by county_fips, plan_id
),

deductibles as (
    select county_fips, plan_id, min(amount) as in_network_deductible
    from {{ ref('stg_deductibles') }}
    where deductible_type = 'Combined Medical and Drug EHB Deductible'
      and network_tier   = 'In-Network'
      and family_cost    = 'Individual'
    group by county_fips, plan_id
),

moops as (
    select county_fips, plan_id, min(amount) as in_network_moop
    from {{ ref('stg_moops') }}
    where moop_type    = 'Maximum Out of Pocket for Medical and Drug EHB Benefits (Total)'
      and network_tier = 'In-Network'
      and family_cost  = 'Individual'
    group by county_fips, plan_id
)

select
    p.plan_key,
    p.county_fips,
    p.county_name,
    p.plan_id,
    coalesce(p.carrier_name, split_part(p.plan_id, 'FL', 1)) as carrier,
    p.metal_level                                             as metal_tier,
    p.plan_type,
    p.premium,
    coalesce(mb.mh_benefits_covered, 0)                       as mh_benefits_covered,
    round(coalesce(mb.avg_mh_copay, 0), 2)                    as avg_mh_copay,
    coalesce(d.in_network_deductible, 9200)                   as in_network_deductible,
    coalesce(mo.in_network_moop, 9200)                        as in_network_moop,
    coalesce(p.global_rating, 0)                              as global_rating,

    -- coverage_score (0-100):
    --   20 pts: MH benefit coverage completeness
    --   40 pts: Outpatient copay efficiency   (max $125 in this market)
    --   25 pts: Deductible access             (max $9,200 in this market)
    --   15 pts: CMS quality rating            (1-5 stars)
    round(
          (coalesce(mb.mh_benefits_covered, 0) / 2.0) * 20
        + greatest(0, 1 - coalesce(mb.avg_mh_copay, 125) / 125.0) * 40
        + greatest(0, 1 - coalesce(d.in_network_deductible, 9200) / 9200.0) * 25
        + coalesce(p.global_rating, 0) / 5.0 * 15
    , 1)                                                      as coverage_score

from {{ ref('dim_plan') }}       p
left join mh_benefits  mb on p.county_fips = mb.county_fips and p.plan_id = mb.plan_id
left join deductibles  d  on p.county_fips = d.county_fips  and p.plan_id = d.plan_id
left join moops        mo on p.county_fips = mo.county_fips and p.plan_id = mo.plan_id
