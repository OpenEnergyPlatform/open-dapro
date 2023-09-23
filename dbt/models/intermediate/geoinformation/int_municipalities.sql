with municipalities_area as (
    select * from {{ ref('stg_municipalities__area') }}
),

municipalities_inhabitants as (
    select * from {{ ref('stg_destatis_areas_and_inhabitants') }}
),

final as (
    select
        municipalities_inhabitants.municipality_id,
        municipalities_inhabitants.municipality,
        municipalities_inhabitants.district_id,
        municipalities_inhabitants.district,
        municipalities_inhabitants.number_inhabitants,
        municipalities_area.nuts,
        municipalities_area.legal_effective_date,
        municipalities_area.geometry_array,
        st_centroid(municipalities_area.geometry_array) as center
    from municipalities_inhabitants
    left join
        municipalities_area
        on
            municipalities_inhabitants.municipality_id
            = municipalities_area.municipality_id
)

select * from final
