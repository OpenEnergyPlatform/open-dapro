with source as (
    select * from {{ source('raw_mastr', 'storage_units') }}
),

renamed as (

    select
        "MastrNummer" as mastr_id_storage_unit,
        "VerknuepfteEinheit" as linked_unit_id,
        "NutzbareSpeicherkapazitaet" as storage_capacity,
        "DatumDownload" as download_date
    from source

)

select * from renamed
