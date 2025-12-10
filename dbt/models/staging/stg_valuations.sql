with source as (
    select * from {{ source('raw_data', 'RAW_VALUATIONS') }}
),

renamed as (
    select
        province,
        municipality as municipality_name,
        avg_value_m2,
        total_appraisals
    from source
)

select *
from renamed
-- Filter out invalid rows (sometimes metadata rows slip through)
where municipality_name is not null
  and avg_value_m2 > 0
