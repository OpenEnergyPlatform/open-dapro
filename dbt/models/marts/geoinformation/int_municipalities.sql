with municipalities_area as (
    select * from {{ ref('stg_municipalities__area') }}
),

charging_points as (
    select
        amount_charging_points,
        geo_point
    from {{ ref('stg_charging_points') }}
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

join_charging_points as (
    select
        municipality_id as id,
        sum(amount_charging_points) as amount_charging_points
    from join_municipalities left join charging_points
        on st_contains(geometry_array, geo_point)
    group by municipality_id
),


final as (
    select
        *,
        st_centroid(geometry_array) as center
    from join_municipalities
    inner join join_charging_points
        on
            join_municipalities.municipality_id
            = join_charging_points.id

)

select * from final
