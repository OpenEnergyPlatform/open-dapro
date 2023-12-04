{{
    config(
        materialized = 'table',
        indexes=[
            {'columns': ['coordinate'], 'type': 'gist'}
        ]
    )
}}

with source as (
    select * from {{ source('raw_mastr', 'biomass_extended') }}
),

final as (

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
        "Nettonennleistung" as power,
        "Hauptbrennstoff" as fuel,
        "Biomasseart" as fuel_type,
        "Technologie" as combustion_technology,
        --owner
        "AnlagenbetreiberMastrNummer" as unit_owner_mastr_id,
        --location
        "Gemeindeschluessel" as municipality_id,
        "Gemeinde" as municipality,
        left("Gemeindeschluessel", 5) as district_id,
        "Landkreis" as district,
        "Postleitzahl" as zip_code,     
        st_setsrid(st_point("Laengengrad", "Breitengrad"), 4326) as coordinate
    from source
)

select * from final
