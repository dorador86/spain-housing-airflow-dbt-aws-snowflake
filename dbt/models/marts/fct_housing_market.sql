{{ config(materialized='table') }}

with income as (
    select * from {{ ref('stg_income') }}
),

valuations as (
    select * from {{ ref('stg_valuations') }}
),

population as (
    select * from {{ ref('stg_population') }}
),

joined as (
    select
        -- Use Income as base (it has codes)
        i.municipality_code,
        i.municipality_name,
        
        -- Metrics
        i.avg_gross_income,
        i.avg_disposable_income,
        v.avg_value_m2 as housing_price_m2,
        v.total_appraisals,
        p.population_count
        
    from income i
    -- Join Valuations by Name (LOWER to minimalize mismatch)
    left join valuations v 
        on lower(trim(i.municipality_name)) = lower(trim(v.municipality_name))
    
    -- Join Population by Code (Robust)
    left join population p
        on i.municipality_code = p.municipality_code
)

select
    *,
    -- TENSOR INDEX CALCULATION
    -- Formula: (Housing Price / Disposable Income) * 100
    -- A higher index means housing is more expensive relative to income (High Tension)
    case 
        when avg_disposable_income > 0 then 
            (housing_price_m2 / avg_disposable_income) * 100
        else null 
    end as tension_index
from joined
where housing_price_m2 is not null -- Only show rows where we have pricing data
order by tension_index desc
