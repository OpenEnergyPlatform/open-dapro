with source as (
    select * from {{ source('raw', 'charging_points') }}
),

refactored as (
    select
        "Postleitzahl" as zip_code,
        "Ort" as municipality,
        "Straße" as street, -- noqa: RF05
        "Hausnummer" as street_number,
        "Adresszusatz" as adress_addition,
        --"Bundesland" as Bundesland,
        --"Betreiber" as Betreiber,
        --"Anschlussleistung" as anschlussleistung,
        "Art der Ladeeinrichung" as charging_equipment, -- noqa: RF05
        "Anzahl Ladepunkte" as amount_charging_points, -- noqa: RF05
        COALESCE("Steckertypen4" || ' - ' || "P4 [kW]" || ' kW | ', '') -- noqa: RF05
        || COALESCE("Steckertypen3" || ' - ' || "P3 [kW]" || ' kW | ', '') -- noqa: RF05
        || COALESCE("Steckertypen2" || ' - ' || "P2 [kW]" || ' kW | ', '') -- noqa: RF05
        || COALESCE(
            "Steckertypen1" || ' - ' || "P1 [kW]" || ' kW', '' -- noqa: RF05
        ) as connectors_and_power,
        ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT(
            CAST(
                REPLACE(
                    REGEXP_REPLACE(
                        REPLACE(TRIM("Längengrad"), E'\u00A0', ''), '[[:space:]]', '', 'g'
                    ),
                    ',',
                    '.'
                ) as double precision
            ),
            CAST(
                REPLACE(
                    REGEXP_REPLACE(
                        REPLACE(TRIM("Breitengrad"), E'\u00A0', ''), '[[:space:]]', '', 'g'
                    ),
                    ',',
                    '.'
                ) as double precision
            )
        ), 4326), 25832) as geo_point

    from source
)

select * from refactored
