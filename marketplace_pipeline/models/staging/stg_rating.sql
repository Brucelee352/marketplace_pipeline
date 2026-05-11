{{ config(materialized='view') }}

-- One row per plan_id. Converts 0-star ratings to NULL (0 = not yet rated
-- on the CMS QRS scale, not an actual score). GROUP BY absorbs duplicate
-- rows caused by the extraction bug.
select
    plan_id,
    bool_or(available)                          as rating_available,
    nullif(max(global_rating), 0)               as global_rating,
    nullif(max(clinical_quality_mgmt_rating), 0) as clinical_quality_mgmt_rating,
    nullif(max(enrollee_experience_rating), 0)   as enrollee_experience_rating,
    nullif(max(plan_efficiency_rating), 0)       as plan_efficiency_rating,
    max(global_not_rated_reason)                as global_not_rated_reason
from {{ source('raw_data', 'rating') }}
group by plan_id
