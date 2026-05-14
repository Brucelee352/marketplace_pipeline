{% snapshot snp_plans %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='check',
        check_cols=[
            'premium',
            'metal_level',
            'type',
            'design_type',
            'hsa_eligible',
            'is_standardized_plan'
        ]
    )
}}

-- Deduplicate across counties so the snapshot sees one row per plan_id.
-- The same plan can appear in multiple counties; plan attributes are identical
-- across counties for the same id, so any_value() is safe here.
select
    id,
    any_value(name)                                  as name,
    any_value(premium)                               as premium,
    any_value(metal_level)                           as metal_level,
    any_value(type)                                  as type,
    any_value(design_type)                           as design_type,
    any_value(cast(is_standardized_plan as boolean)) as is_standardized_plan,
    any_value(cast(hsa_eligible as boolean))         as hsa_eligible,
    any_value(service_area_id)                       as service_area_id,
    any_value(state)                                 as state
from {{ source('raw_data', 'plans') }}
group by id

{% endsnapshot %}
