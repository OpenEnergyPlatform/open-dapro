with district_area as (
    select * from {{ ref('stg_districts__area') }}
),

charging_points as (
    select
        amount_charging_points,
        geo_point
    from {{ ref('stg_charging_points') }}
),

final as (
    select
        district_id,
        sum(amount_charging_points) as amount_charging_points
    from district_area left join charging_points
        on st_contains(geometry_array, geo_point)
    group by district_id
)

select * from final
