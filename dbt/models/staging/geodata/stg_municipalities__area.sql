with source as (
    select * from {{ source('raw', 'municipalities') }}
),

renamed as (
    select
        "AGS" as municipality_id,
        "GEN" as municipality,
        "NUTS" as nuts,
        "WSK" as legal_effective_date,
        "geometry" as geometry_array
    from source
)

select * from renamed
