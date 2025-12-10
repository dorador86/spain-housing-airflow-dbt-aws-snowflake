with source as (
    select * from {{ source('raw_data', 'RAW_INCOME') }}
),

renamed as (
    select
        municipality_code_ine as municipality_code,
        municipality as municipality_name,
        avg_gross_income,
        avg_disposable_income,
        total_declarations
    from source
)

select * 
from renamed
where municipality_code is not null
