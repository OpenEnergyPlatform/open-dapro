with source as (
    select * from {{ source('raw_mastr', 'storage_extended') }}
),

renamed as (

    select
        "EinheitMastrNummer" as mastr_id,
        "Gemeindeschluessel" as municipality_id,
        "Gemeinde" as municipality,
        "Landkreis" as district,
        "Postleitzahl" as zip_code,
        "Inbetriebnahmedatum" as commissioning_date,
        "GeplantesInbetriebnahmedatum" as planned_commissioning_date,
        "Nettonennleistung" as power,
        "DatumDownload" as download_date,
        left("Gemeindeschluessel", 5) as district_id,
        concat(
            date_part('year', "Inbetriebnahmedatum"),
            date_part('year', "GeplantesInbetriebnahmedatum")
        ) as installation_year
    from source

)

select * from renamed
