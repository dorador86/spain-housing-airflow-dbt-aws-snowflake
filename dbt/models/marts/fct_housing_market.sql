{{ config(materialized='table') }}

with income as (
    select 
        *,
        -- Normalize municipality name logic (Reconstruct Natural Name):
        -- 1. Remove Trailing Code (e.g., "-03065")
        -- 2. Move suffix article to prefix (e.g., "Oliva, La" -> "La Oliva")
        -- 3. Remove apostrophes (Handle "L'" vs "L") and trim/lower
        lower(trim(replace(
            regexp_replace(
                regexp_replace(municipality_name, '-[0-9]+$', ''), -- Step 1: Remove Code
                '^(.*),\\s*(El|La|Los|Las|L\'|L|A|Els|Les)$', '\\2 \\1', 1, 0, 'i' -- Step 2: Move Suffix to Prefix
            ), 
            '\'', '' -- Step 3: Remove apostrophes
        ))) as normalized_name
    from {{ ref('stg_income') }}
),

valuations as (
    select 
        *,
        -- Normalize municipality name logic for Valuations:
        -- 1. Move Parenthesized Article to Prefix (e.g., "Ejido (El)" -> "El Ejido")
        -- 2. Move Comma Article to Prefix (e.g., "Oliva, La" -> "La Oliva")
        -- 3. Remove apostrophes and trim/lower
        lower(trim(replace(
            regexp_replace(
                regexp_replace(municipality_name, '^(.*)\\s+\\((El|La|Los|Las|L\'|L|A|Els|Les)\\)$', '\\2 \\1', 1, 0, 'i'), -- Step 1: Move (Art) to Prefix
                '^(.*),\\s*(El|La|Los|Las|L\'|L|A|Els|Les)$', '\\2 \\1', 1, 0, 'i' -- Step 2: Move , Art to Prefix
            ),
            '\'', '' -- Step 3: Remove apostrophes
        ))) as normalized_name
    from {{ ref('stg_valuations') }}
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
    -- Join Population by Code (Robust) - INNER JOIN to keep only full matches
    inner join population p
        on i.municipality_code = p.municipality_code
    
    -- Join Valuations by normalized name - INNER JOIN
    -- Both sides normalized to handle: "Municipio, El", "Municipio (Los)", etc.
    inner join valuations v 
        on i.normalized_name = v.normalized_name
)

select
    *,
    -- TENSION INDEX CALCULATION
    -- Formula: (Housing Price / Disposable Income) * 100
    -- A higher index means housing is more expensive relative to income (High Tension)
    case 
        when avg_disposable_income > 0 then 
            round((housing_price_m2 / avg_disposable_income) * 100, 2)
        else null 
    end as tension_index
from joined
where housing_price_m2 is not null -- Only show rows where we have pricing data
order by tension_index desc
