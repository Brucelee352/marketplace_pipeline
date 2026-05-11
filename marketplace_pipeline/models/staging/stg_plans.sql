{{ config(materialized='view') }}

select
    id                                                       as plan_id,
    name                                                     as plan_name,
    cast(premium as decimal(10, 2))                          as premium,
    metal_level,
    type                                                     as plan_type,
    design_type,
    cast(is_standardized_plan as boolean)                    as is_standardized_plan,
    cast(hsa_eligible as boolean)                            as hsa_eligible,
    cast(has_national_network as boolean)                    as has_national_network,
    cast(specialist_referral_required as boolean)            as specialist_referral_required,
    service_area_id,
    state,
    -- already a VARCHAR[] array from pandas; flatten to comma-separated string for BI compatibility
    nullif(array_to_string(disease_mgmt_programs, ', '), '') as disease_mgmt_programs
from {{ source('raw_data', 'plans') }}
