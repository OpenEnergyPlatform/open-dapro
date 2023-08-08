with municipalities_area as (
    select * from {{ ref('stg_municipalities__area') }}
),

municipalities_zensus as (
    select
        municipality_id,
        number_inhabitants
    from {{ ref('stg_zensus__municipalities') }}
),

join_municipalities as (
    select
        municipalities_area.municipality_id as municipality_id,
        municipalities_area.municipality,
        municipalities_area.nuts,
        municipalities_area.legal_effective_date,
        municipalities_area.geometry_array,
        municipalities_zensus.number_inhabitants
    from municipalities_area
    left join
        municipalities_zensus
        on
            municipalities_area.municipality_id
            = municipalities_zensus.municipality_id
),

final as (
    select
        *,
        st_centroid(geometry_array) as center
    from join_municipalities
)

select * from final
