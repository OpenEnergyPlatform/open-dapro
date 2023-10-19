with source as (
    select * from {{ source('raw', 'districts_geoboundaries') }}
),

renamed as (
    select
        "AGS" as district_id,
        "GEN" as district,
        "NUTS" as nuts,
        "WSK" as legal_effective_date,
        "geometry" as geometry_array
    from source
)

select * from renamed
