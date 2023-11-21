{{
    config(
        materialized = 'table',
        indexes=[
            {'columns': ['coordinate'], 'type': 'gist'}
        ]
    )
}}

with source as (
    select * from {{ source('raw_mastr', 'solar_extended') }}
),

renamed as (

    select
        --general
        "EinheitMastrNummer" as mastr_id,
        "EinheitBetriebsstatus" as operating_status,
        --location
        "Gemeindeschluessel" as municipality_id,
        "Gemeinde" as municipality,
        left("Gemeindeschluessel", 5) as district_id,
        "Landkreis" as district,
        "Postleitzahl" as zip_code,
        --dates
        "Inbetriebnahmedatum" as commissioning_date,
        "GeplantesInbetriebnahmedatum" as planned_commissioning_date,
        "DatumDownload" as download_date,
        --technical
        "AnlagenbetreiberMastrNummer" as unit_owner_mastr_id,
        "Nettonennleistung" as power,
        "Hauptausrichtung" as orientation,
        "Nebenausrichtung" as orientation_secondary,
        "GemeinsamerWechselrichterMitSpeicher" as combination_with_storage,
        "Nutzungsbereich" as utilization_area,
        concat(
            date_part('year', "Inbetriebnahmedatum"),
            date_part('year', "GeplantesInbetriebnahmedatum")
        ) as installation_year,
        st_setsrid(st_point("Laengengrad", "Breitengrad"), 4326) as coordinate
    from source

)

select * from renamed
