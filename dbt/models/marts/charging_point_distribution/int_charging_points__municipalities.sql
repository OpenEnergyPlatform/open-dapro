with municipalities_area as (
    select * from {{ ref('stg_municipalities__area') }}
),

charging_points as (
    select
        amount_charging_points,
        geo_point
    from {{ ref('stg_charging_points') }}
),

final as (
    select
        municipality_id,
        sum(amount_charging_points) as amount_charging_points
    from municipalities_area left join charging_points
        on st_contains(geometry_array, geo_point)
    where amount_charging_points is not null
    group by municipality_id
)

select * from final
