{{
    config(
        materialized = 'table',
        indexes=[
            {'columns': ['coordinate'], 'type': 'gist'}
        ]
    )
}}

with source as (
    select * from {{ source('raw_mastr', 'wind_extended') }}
),


renamed as (

    select
        "EinheitMastrNummer" as mastr_id,
        "EinheitBetriebsstatus" as operating_status,
        "Gemeindeschluessel" as municipality_id,
        "Gemeinde" as municipality,
        "Landkreis" as district,
        "Postleitzahl" as zip_code,
        "Inbetriebnahmedatum" as commissioning_date,
        "GeplantesInbetriebnahmedatum" as planned_commissioning_date,
        "Nettonennleistung" as power,
        "AnlagenbetreiberMastrNummer" as unit_owner_mastr_id,
        "Hersteller" as manufacturer,
        "DatumDownload" as download_date,
        left("Gemeindeschluessel", 5) as district_id,
        CASE
        WHEN "Inbetriebnahmedatum" IS NOT NULL OR "GeplantesInbetriebnahmedatum" IS NOT NULL THEN
            concat(
            date_part('year', "Inbetriebnahmedatum"),
            date_part('year', "GeplantesInbetriebnahmedatum")
        )::integer
        ELSE
            NULL
        END as installation_year,
        st_setsrid(st_point("Laengengrad", "Breitengrad"), 4326) as coordinate
    from source

)



select * from renamed
