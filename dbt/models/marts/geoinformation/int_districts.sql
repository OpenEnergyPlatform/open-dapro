with district_area as (
    select * from {{ ref('stg_districts__area') }}
),

districts_zensus as (
    select
        district_id,
        number_inhabitants
    from {{ ref('stg_zensus__districts') }}
),

join_districts as (
    select
        district_area.district_id as district_id,
        district_area.district,
        district_area.nuts,
        district_area.legal_effective_date,
        district_area.geometry_array,
        districts_zensus.number_inhabitants
    from district_area
    left join
        districts_zensus
        on
            district_area.district_id
            = districts_zensus.district_id
),


final as (
    select
        *,
        st_centroid(geometry_array) as center
    from join_districts
)

select * from final
