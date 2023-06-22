with source as (
    select * from {{ source('raw', 'zensus') }}
),

refactored as (
    select
        source."AGS_12" as community_key_12,
        source."Name" as community_name,
        source."Reg_Hier" as regional_level,
        source."DEM_1.1" as number_inhabitants, -- noqa: RF01, RF05
        source."RS_RB_NUTS2"::text as nuts2_in_ags,
        LPAD(source."RS_Land"::text, 2, '0') as federal_state_in_ags,
        LPAD(source."RS_Kreis"::text, 2, '0') as district_in_ags,
        LPAD(source."RS_VB"::text, 4, '0') as municipalities_association_in_ags,
        LPAD(source."RS_Gem"::text, 3, '0') as municipality_in_ags

    from source
)

select * from refactored
