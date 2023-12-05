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
        --general
        "EinheitMastrNummer" as mastr_id,
        "EinheitBetriebsstatus" as operating_status,
        "NetzbetreiberpruefungStatus" as grid_operator_inspection,
        --dates
        CASE
        WHEN "Inbetriebnahmedatum" IS NOT NULL OR "GeplantesInbetriebnahmedatum" IS NOT NULL THEN
            concat(
            date_part('year', "Inbetriebnahmedatum"),
            date_part('year', "GeplantesInbetriebnahmedatum")
        )::integer
        ELSE
            NULL
        END as installation_year,
        "Inbetriebnahmedatum" as commissioning_date,
        "GeplantesInbetriebnahmedatum" as planned_commissioning_date,
        "DatumDownload" as download_date,
        --technical
        "Nettonennleistung" as power_net,
        "Bruttoleistung" as power_gross,
        "ZugeordnenteWirkleistungWechselrichter" as power_inverter,
        "Batterietechnologie" as battery_technology,
        --owner
        "AnlagenbetreiberMastrNummer" as unit_owner_mastr_id,
        --location
        "Gemeindeschluessel" as municipality_id,
        "Gemeinde" as municipality,
        left("Gemeindeschluessel", 5) as district_id,
        "Landkreis" as district,
        "Postleitzahl" as zip_code,
        st_setsrid(st_point("Laengengrad", "Breitengrad"), 4326) as coordinate
    from source_extended

),


storage_units as (
    select
        --general
        ru.mastr_id as mastr_id,
        re.operating_status,
        re.grid_operator_inspection,
        --dates
        re.installation_year,
        re.commissioning_date,
        re.planned_commissioning_date,
        re.download_date,
        --technical
        ru.storage_capacity,
        re.power_net,
        re.power_gross,
        re.power_inverter,
        re.battery_technology,
        --owner
        re.unit_owner_mastr_id,
        --location
        re.municipality_id,
        re.municipality,
        re.district_id,
        re.district,
        re.zip_code,
        re.coordinate
    from renamed_extended as re
    left join
        renamed_storage_units as ru
        on re.mastr_id = ru.mastr_id
)

select * from storage_units
