{{
    config(
        materialized = 'table',
        indexes=[
            {'columns': ['coordinate'], 'type': 'gist'}
        ]
    )
}}

with source_extended as (
    select * from {{ source('raw_mastr', 'storage_extended') }}
),

source_units as (
    select * from {{ source('raw_mastr', 'storage_units') }}
),

renamed_storage_units as (

    select
        "VerknuepfteEinheit" as mastr_id,
        "NutzbareSpeicherkapazitaet" as storage_capacity
    from source_units

),

renamed_extended as (

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
        "DatumDownload" as download_date,
        left("Gemeindeschluessel", 5) as district_id,
        concat(
            date_part('year', "Inbetriebnahmedatum"),
            date_part('year', "GeplantesInbetriebnahmedatum")
        ) as installation_year,
        st_setsrid(st_point("Laengengrad", "Breitengrad"), 4326) as coordinate
    from source_extended

),


storage_units as (
    select
        renamed_storage_units.mastr_id as mastr_id,
        renamed_extended.operating_status,
        renamed_storage_units.storage_capacity,
        renamed_extended.municipality_id,
        renamed_extended.district_id,
        renamed_extended.municipality,
        renamed_extended.district,
        renamed_extended.zip_code,
        renamed_extended.commissioning_date,
        renamed_extended.planned_commissioning_date,
        renamed_extended.installation_year,
        renamed_extended.power,
        renamed_extended.download_date,
        renamed_extended.coordinate
    from renamed_storage_units

    left join
        renamed_extended
        on renamed_storage_units.mastr_id = renamed_extended.mastr_id
)

select * from storage_units
