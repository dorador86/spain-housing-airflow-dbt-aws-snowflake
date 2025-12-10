with source as (
    select * from {{ source('raw_data', 'RAW_POPULATION') }}
),

renamed as (
    select
        municipality_residence as municipality_raw,
        -- Extract INE code if present (e.g. "04001 - Abla" -> "04001")
        split_part(municipality_residence, ' ', 1) as municipality_code,
        -- Extract Name
        trim(substr(municipality_residence, position(' ' in municipality_residence))) as municipality_name,
        sex,
        period as year,
        total as population_count
    from source
)

select * 
from renamed
where sex = 'Total' -- We only care about the total population for now
